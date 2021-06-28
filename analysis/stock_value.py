#!/usr/bin/env python
# coding: utf-8

import psycopg2
import pandas as pd
import numpy as np
import copy
from sqlalchemy import create_engine, types

import matplotlib.pyplot as plt
import seaborn as sns

plt.rc('font', family='Malgun Gothic')
plt.rcParams['axes.unicode_minus'] = False

# conn = psycopg2.connect(host='localhost',
#                         dbname = 'hadoop',
#                         user = 'hadoop',
#                         port = '5432')
conn = psycopg2.connect("host='localhost' port='5432' user='postgres' password='rootpass'")
cursor = conn.cursor()
engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=rootpass")

def query(query):
    cursor = conn.cursor()
    cursor.execute(query)
    col_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns = col_names)
    cursor.close()
    
    return result

# 테이블 비우기
trunc_queries = ["TRUNCATE TABLE value_analysis_results", "TRUNCATE TABLE value_analysis_results_graph"]
for query in trunc_queries:
    cursor.execute(query)
conn.commit()
print("테이블 비우기")

# ## 데이터 불러오기
company_fs = query("select * from company_fs")
company_fs.head()

stock = query("select * from stock")

opt10001 = query("select * from opt10001")
opt10001 = opt10001[['종목코드', '종목명', '시가총액']]
opt10001 = opt10001.sort_values(by=('시가총액'), ascending=False)
opt10001['RANK'] = opt10001['시가총액'].rank()

# ## 스코어링
temp = copy.deepcopy(opt10001)
temp['WB1'] = np.where(temp['RANK'] > temp.shape[0] * (95/100), 3, 0)
temp['WB1'] = np.where(temp['RANK'] > temp.shape[0] * (97/100), 5, temp['WB1'])
temp['WB1'] = np.where(temp['RANK'] > temp.shape[0] * (99/100), 10, temp['WB1'])
temp['WB1'] = np.where(temp['RANK'] > temp.shape[0] * (99.5/100), 20, temp['WB1'])
temp = temp.drop('RANK', axis = 1)
temp.head()

df = company_fs[['종목코드', '기준년도', 'P/E', 'P/S', "ROE_지배", "EPS증가율_YoY", "순이익률"]]
df.columns = ['종목코드', '기준년도', 'PER', 'PSR', "ROE", "EPS증가율", "순이익률"]
df = pd.merge(temp, df, on = "종목코드", how = "left")
df.head()

mdf = pd.DataFrame()

for code in stock['종목코드']:
    
    temp = df[df['종목코드'] == code]
    temp = temp.sort_values('기준년도') 
    
    #워렌버핏
    temp2 = copy.deepcopy(temp)
    temp2['ROE_y1'] = temp2['ROE'].shift(1)
    temp2['ROE_y2'] = temp2['ROE'].shift(2)
    temp2['WB2'] = np.where(((temp2['ROE']>10) & (temp2['ROE_y1']>10) & (temp2['ROE_y2']>10)), 1, 0)
    temp2['WB2'] = np.where(((temp2['ROE']>15) & (temp2['ROE_y1']>15) & (temp2['ROE_y2']>15)), 3, temp2['WB2'])
    temp2['WB2'] = np.where(((temp2['ROE']>20) & (temp2['ROE_y1']>20) & (temp2['ROE_y2']>20)), 5, temp2['WB2'])

    temp2['EPS증가율_y1'] = temp2['EPS증가율'].shift(1)
    temp2['EPS증가율_y2'] = temp2['EPS증가율'].shift(2)
    temp2['WB3'] = np.where(temp2['EPS증가율'] > 0, 2, 0)
    temp2['WB3'] = np.where(temp2['EPS증가율'] > temp2['EPS증가율_y1'], 6, temp2['WB3'])
    temp2['WB3'] = np.where(((temp2['EPS증가율']>temp2['EPS증가율_y1']) & (temp2['EPS증가율_y1']>temp2['EPS증가율_y2'])), 10, temp2['WB3'])

    #피터린치
    temp2['PEG'] = temp2['PER'] / temp2['EPS증가율']
    temp2['PL1'] = np.where((temp2['PEG'] < 1) & (temp2['PEG'] > 0), 3, 0)
    temp2['PL1'] = np.where(temp2['PEG'] < 0.75, 6, temp2['PL1'])
    temp2['PL1'] = np.where((temp2['PEG'] < 0.5), 9, temp2['PL1'])
    
    temp2['PL2'] = np.where(temp2['PER'] < 40 & (temp2['PER'] > 0), 2, 0)
    temp2['PL2'] = np.where(temp2['PER'] < 30, 4, temp2['PL2'])
    temp2['PL2'] = np.where(temp2['PER'] < 25, 6, temp2['PL2'])
    temp2['PL2'] = np.where(temp2['PER'] < 20, 10, temp2['PL2'])
    
    temp2['PL3'] = np.where(temp2['EPS증가율'] > 15, 3, 0)
    temp2['PL3'] = np.where(temp2['EPS증가율'] > 17, 5, temp2['PL3'])
    temp2['PL3'] = np.where(temp2['EPS증가율'] > 20, 15, temp2['PL3'])
    
    #케네스피셔
    temp2['KF1'] = np.where(temp2['PSR'] < 0.75, 3, 0)
    temp2['KF1'] = np.where(temp2['PSR'] < 0.6, 5, temp2['KF1'])
    temp2['KF1'] = np.where(temp2['PSR'] < 0.5, 9, temp2['KF1'])
    
    temp2['KF2'] = np.where(temp2['EPS증가율'] > 13, 2, 0)
    temp2['KF2'] = np.where(temp2['EPS증가율'] > 14, 4, temp2['KF2'])
    temp2['KF2'] = np.where(temp2['EPS증가율'] > 15, 6, temp2['KF2'])
    
    temp2['3년평균순이익률'] = temp2['순이익률'].rolling(3).mean()
    temp2['KF3'] = np.where(temp2['3년평균순이익률']<0, -20, 0)
    temp2['KF3'] = np.where(temp2['3년평균순이익률']>= 5, 3, temp2['KF3'])
    temp2['KF3'] = np.where(temp2['3년평균순이익률']>= 7 , 5, temp2['KF3'])
    temp2['KF3'] = np.where(temp2['3년평균순이익률']> 10, 15, temp2['KF3'])
    
    mdf = pd.concat([mdf, temp2])

mdf_score = copy.deepcopy(mdf)
mdf_score = mdf_score[mdf_score['기준년도'] == "2020"]
mdf_score['WB'] = mdf_score['WB1'] + mdf_score['WB2'] + mdf_score['WB3']
mdf_score['PL'] = mdf_score['PL1'] + mdf_score['PL2'] + mdf_score['PL3']
mdf_score['KF'] = mdf_score['KF1'] + mdf_score['KF2'] + mdf_score['KF3']
mdf_score['스코어'] = mdf_score['WB'] + mdf_score['PL'] + mdf_score['KF'] 
mdf_score = mdf_score.sort_values(['스코어', '시가총액'], ascending = False)
mdf_score.head()

mdf_score2 = mdf_score[['기준년도','종목코드', '종목명', '스코어', '시가총액', 'ROE', 'EPS증가율', 'PEG', 'PER', 'PSR', '3년평균순이익률']]
mdf_score2.head(20)

opt20002 = query("select * from opt20002")
opt20002.head()

mdf_score3 = pd.merge(mdf_score2, opt20002[['업종명', '업종코드', '종목코드']], on="종목코드", how = "left")
mdf_score3.head()

mdf_score3['업종명'].unique()

mdf_score3[mdf_score3.업종명 == "종합(KOSPI)"].head(20)
mdf_score3[mdf_score3.업종명 == "대형주"].head(30)

try:
    mdf_score3.to_sql("value_analysis_results", engine, if_exists='append', index=False, chunksize=1000)
    print("value_analysis_results 수집/적재 완료")
except Exception as ex:
    print("value_analysis_results 수집/적재 실패")
    print(ex)
# mdf_score3.to_csv('./결과/가치분석결과.csv', index = False, encoding = 'cp949')

mdf_line = copy.deepcopy(mdf)
mdf_line = mdf_line[['기준년도','종목코드', '종목명', '시가총액', 'ROE', 'EPS증가율', 'PEG', 'PER', 'PSR', '3년평균순이익률']]
mdf_line.head()

try:
    mdf_line.to_sql("value_analysis_results_graph", engine, if_exists='append', index=False, chunksize=1000)
    print("value_analysis_results_graph 수집/적재 완료")
except Exception as ex:
    print("value_analysis_results_graph 수집/적재 실패")
    print(ex)
# mdf_line.to_csv('./결과/가치분석결과_그래프용.csv', index = False, encoding = 'cp949')