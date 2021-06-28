#!/usr/bin/env python
# coding: utf-8

import psycopg2
import pandas as pd
import warnings
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib import rc

warnings.filterwarnings(action='ignore')

#폰트 한글 깨짐 처리
mpl.rcParams['axes.unicode_minus'] = False
font_name = fm.FontProperties(fname="c:\\windows\\fonts\\malgun.ttf").get_name()
rc('font', family=font_name)

# postgresql python연동
conn = psycopg2.connect(host='localhost', dbname ='hadoop', user='hadoop', port='5431')
cur = conn.cursor()

# 쿼리 함수
def query(query):
    cursor = conn.cursor()
    cursor.execute(query)
    col_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=col_names)
    cursor.close()

    return result

# 회사 정보 가져오기
company_info = query("SELECT  A.종목명, B.*   FROM stock A JOIN company_fs B ON A.종목코드 =B.종목코드")
company_info.head()

# 가격정보 가져오기져오기
price = query("select * from kospi_200")
price.head()

# In[6]:


# 가격정보 가져오기져오기

price = query("SELECT  A.종목명, B.*   FROM kospi_200 A JOIN opt10081 B ON A.종목코드 =B.종목코드 order by 일자 desc")
price.head()

# In[7]:


price['일자'] = price['일자'].astype(str)

# In[8]:


price_day = price.일자.str[:7].tolist()[0]
price_day

# In[9]:


price['일자'] = pd.to_datetime(price['일자'])
price.dtypes

# In[10]:


# 날짜별로 테이블 생성

price_p = pd.pivot_table(price, values='현재가', columns='종목코드', index = ['일자'], aggfunc=['sum'])['sum']

# In[11]:


# 코스피200 종목들만 남기기

stocks = list(set(company_info['종목코드']))

stocks_list = []

for code in stocks:
    if code in list(set(price['종목코드'])):
        stocks_list.append(code)
        
        
print(stocks_list)

# In[12]:


# 코스피 200 

print('가격데이터 종목코드 갯수 : ',len(set(price['종목코드'])))
print('company info와 겹치는 갯수 : ', len(stocks_list))

# In[13]:


# 회사정보데이터 stocks_list 에서 종목코드 가져오기

company_stock = company_info.set_index('종목코드')
company_stocks = company_stock.loc[stocks_list]

# In[14]:


len(set(pd.isna(company_stocks.ROA).index))

# In[15]:


pd.isna(company_stocks.ROA).sum()

# In[16]:


len(set(company_stocks[company_stocks.ROA.isnull()==True].index))

# In[17]:


# 가격데이터에서 stocks_list 종목코드 가져오기

price_df = price_p[stocks_list]

# In[18]:


len(price_df.columns)

# In[19]:


# company_stock=company_info.set_index('종목코드')
# company_stocks = company_stock[company_stock.index.isin(stocks_list)]
# company_stocks

# ## 테이블 생성

# In[20]:


# 재무비율

fr_df = company_stocks[['기준년도','종목명','ROA','부채비율','영업이익률','유동비율']]
fr_df

# In[21]:


# 재무제표

fs_df = company_stocks[['기준년도','종목명','당기순이익','매출액','부채총계','영업이익','영업활동으로인한현금흐름','자본총계','자산총계']]
fs_df.head()

# In[22]:


fs_df = company_stocks[['기준년도','종목명','당기순이익','매출액','부채총계','영업이익','영업활동으로인한현금흐름','자본총계','자산총계','ROA','부채비율','유동비율','매출총이익','총자산회전율']]
fs_df.head()

# In[23]:


# 투자지표데이터

invest_df = company_stocks[['기준년도','종목명','P/B','P/C','P/E','P/S']]
invest_df.columns = ['기준년도','종목명','PBR','PCR','PER','PSR']
invest_df.head()

# # 저평가 주식

# In[24]:


#  2020 기준으로 PER 작은 순으로 나열

invest_df_2020 = invest_df[invest_df.기준년도 == '2020']
invest_df_2020.sort_values(by=('PER'))

# In[25]:


#  ROA 컬럼을 기준으로 내림차순 정렬 
fr_df_2020 = fr_df[fr_df.기준년도 == '2020']
fr_df_2020.sort_values(by=('ROA'))

# In[26]:


#  PER기준으로 오름차순으로 정렬하여 주는 함수 

def low_per(invest_df, index_date, num):
    invest_date = invest_df[invest_df.기준년도 == index_date]
    per_sorted = invest_date.sort_values(by=('PER'))
    return per_sorted[:num]


# In[27]:


#  ROA기준으로 내림차순으로 정렬하여 주는 함수 

def high_roa(fr_df, index_date, num):
    fr_date = fr_df[fr_df.기준년도 == index_date]
    sorted_roa = fr_date.sort_values(by=('ROA'), ascending=False)
    return sorted_roa[:num]

# In[28]:


#  pbr+roa공식 함수로 만들기 

def magic_formula(fr_df, invest_df, index_date, num):
    per = low_per(invest_df, index_date, None)
    roa = high_roa(fr_df, index_date, None).iloc[:,2:]
    per['pbr순위'] = per['PBR'].rank()
    roa['roa순위'] = roa['ROA'].rank(ascending=False)
    magic = pd.merge(per, roa, how='outer', left_index=True, right_index=True)
    magic['마법공식 순위'] = (magic['pbr순위'] + magic['roa순위']).rank().sort_values()
    magic = magic.sort_values(by='마법공식 순위')
    return magic[:num]

# In[29]:


#  저평가 지수를 기준으로 정렬하여 순위 만들어 주는 함수 

def get_value_rank(invest_df, value_type, index_date, num):
    invest_date = invest_df[invest_df.기준년도 == index_date]
    value_sorted = invest_date.sort_values(by=(value_type))
    value_sorted[  value_type + '순위'] = value_sorted[value_type].rank()
    return value_sorted[[value_type, value_type + '순위']][:num]

# In[30]:


pd.set_option('display.max_rows', None)

# In[31]:


# 저평가지수통합

def get_value_quality(invest_df, fs_df, index_date, num):
    value = make_value_combo(['PER', 'PBR', 'PSR', 'PCR'], invest_df, index_date, None)
    quality = get_fscore(fs_df, index_date, None)
    value_quality = pd.merge(value, quality, how='outer', left_index=True, right_index=True)
    value_quality_filtered = value_quality[value_quality['종합점수'] == 3]
    vq_df = value_quality_filtered.sort_values(by='저평가종합순위')
    vq_df['저평_fs_순위'] = range(1,len(vq_df)+1)
    return vq_df[:num]

# In[32]:


#  저평가 지표 조합 함수 

def make_value_combo(value_list, invest_df, index_date, num):
    
    for i, value in enumerate(value_list):
        temp_df = get_value_rank(invest_df, value, index_date, None)
        if i == 0:
            value_combo_df = temp_df
            rank_combo = temp_df[value + '순위']
        else:
            value_combo_df = pd.merge(value_combo_df, temp_df, how='outer', left_index=True, right_index=True)
            rank_combo = rank_combo + temp_df[value + '순위']
    
    value_combo_df['저평가종합순위'] = rank_combo.rank()
    value_combo_df = value_combo_df.sort_values(by='저평가종합순위')
    
    return value_combo_df[:num]

# In[33]:


#  F-score 함수

def get_fscore_t(fs_df, index_date, num):
    fscore_df = fs_df[fs_df.기준년도 == index_date]
    fscore_df['당기순이익점수'] = fscore_df['당기순이익'] > 0
    fscore_df['영업활동점수'] = fscore_df['영업활동으로인한현금흐름'] > 0
    fscore_df['더큰영업활동점수'] = fscore_df['영업활동으로인한현금흐름'] > fscore_df['당기순이익']
    fscore_df['종합점수'] = fscore_df[['당기순이익점수', '영업활동점수', '더큰영업활동점수']].sum(axis=1)
    fscore_df = fscore_df[fscore_df['종합점수'] == 3]
    return fscore_df[:num]

# In[34]:


#  F-score 함수

def get_fscore(fs_df, index_date, num):
    p_date = str(int(index_date)-1)
    fsco_df = fs_df.copy()
    fscore_df = fs_df[fs_df.기준년도 == index_date]

    fscore_df['당기순이익점수'] = fscore_df['당기순이익'] > 0
    fscore_df['영업활동점수'] = fscore_df['영업활동으로인한현금흐름'] > 0
    fscore_df['더큰영업활동점수'] = fscore_df['영업활동으로인한현금흐름'] > fscore_df['당기순이익']
    fscore_df['ROA점수'] = (pd.DataFrame(fsco_df[fsco_df.기준년도 == index_date].ROA)-pd.DataFrame(fsco_df[fsco_df.기준년도 ==p_date ].ROA))>0
    fscore_df['부채점수'] =  (pd.DataFrame(fsco_df[fsco_df.기준년도 == index_date].부채비율)-pd.DataFrame(fsco_df[fsco_df.기준년도 ==p_date ].부채비율))>0
    fscore_df['유동점수'] = (pd.DataFrame(fsco_df[fsco_df.기준년도 == index_date].유동비율)-pd.DataFrame(fsco_df[fsco_df.기준년도 ==p_date ].유동비율))>0
    fscore_df['매출총이익점수'] = (pd.DataFrame(fsco_df[fsco_df.기준년도 == index_date].매출총이익)-pd.DataFrame(fsco_df[fsco_df.기준년도 ==p_date ].매출총이익))>0
    fscore_df['자산회전율점수'] = (pd.DataFrame(fsco_df[fsco_df.기준년도 == index_date].총자산회전율)-pd.DataFrame(fsco_df[fsco_df.기준년도 ==p_date ].총자산회전율))>0
    fscore_df['종합점수'] = fscore_df[['당기순이익점수', '영업활동점수', '더큰영업활동점수','ROA점수','부채점수','유동점수','매출총이익점수','자산회전율점수']].sum(axis=1)
    fscore_df = fscore_df[fscore_df['종합점수'] == 6]
    return fscore_df[:num]

# ---

# In[35]:


#  모멘텀 데이터프레임 만들기 함수화

def get_momentum_rank(price_df, index_date, date_range, num):
    momentum_df = pd.DataFrame(price_df.pct_change(date_range).loc[index_date])
    momentum_df.columns = ['모멘텀']
    momentum_df['모멘텀순위'] = momentum_df['모멘텀'].rank(ascending=False)
    momentum_df = momentum_df.sort_values(by='모멘텀순위')
    return momentum_df[:num]

# In[36]:


#  저평가 + Fscore 함수화

def get_value_quality(invest_df, fs_df, index_date, num):
    value = make_value_combo(['PER', 'PBR', 'PSR', 'PCR'], invest_df, index_date, None)
    quality = get_fscore(fs_df, index_date, None)
    value_quality = pd.merge(value, quality, how='outer', left_index=True, right_index=True)
    value_quality_filtered = value_quality[value_quality['종합점수'] == 6]
    vq_df = value_quality_filtered.sort_values(by='저평가종합순위')
    vq_df['저평_fs_순위'] = range(1,len(vq_df)+1)
    return vq_df[:num]

# ---

# # 백테스트

# In[37]:


#  필요한 모듈과 데이터를 가져오기 

import python_quant
import pandas as pd

# In[38]:


#  해당 날짜에 가격이 없으면 투자 관련 데이터에서 해당 종목 없애는 함수 

def select_code_by_price(price_df, data_df, start_date):
    new_code_list = list(price_df[start_date].iloc[0].dropna().index)
    selected_df =  data_df.loc[new_code_list]
    return selected_df

# In[39]:


#  백테스트 함수 버젼1 


def backtest_beta(price_df, strategy_df, start_date, end_date, initial_money):

    code_list = list(strategy_df.index)

    strategy_price = price_df[code_list][start_date:end_date]
#     print(strategy_price.isnull().sum())
    pf_stock_num = {}
    stock_amount = 0
    stock_pf = 0
    each_money = initial_money / len(strategy_df)
    for code in strategy_price.columns:
        temp = int( each_money / strategy_price[code][0] )
        pf_stock_num[code] = temp
        stock_amount = stock_amount + temp * strategy_price[code][0]
        stock_pf = stock_pf + strategy_price[code] * pf_stock_num[code]

    cash_amount = initial_money - stock_amount

    backtest_df = pd.DataFrame({'주식포트폴리오':stock_pf})
    backtest_df['현금포트폴리오'] = [cash_amount] * len(backtest_df)
    backtest_df['종합포트폴리오'] = backtest_df['주식포트폴리오'] + backtest_df['현금포트폴리오']
    backtest_df['일변화율'] = backtest_df['종합포트폴리오'].pct_change()
    backtest_df['총변화율'] = backtest_df['종합포트폴리오']/initial_money - 1
    
    return backtest_df

# In[40]:


#  해당 날짜에 가격이 없으면 투자 관련 데이터에서 해당 종목 없애는 함수 

def select_code_by_price(price_df, data_df, start_date):
    new_code_list = list(price_df[start_date].iloc[0].dropna().index)
    selected_df =  data_df.loc[new_code_list]
    return selected_df

# In[41]:


# 백테스트 시작날짜가 주어지면 전략 기준 날짜를 계산하는 함수 

def get_strategy_date(start_date):
    temp_year = int(start_date.split('-')[0])
    temp_month = start_date.split('-')[1]
    if temp_month in '1 2 3 4 5'.split(' '):
        strategy_date = str(temp_year - 2)
    else:
        strategy_date = str(temp_year - 1)
    return strategy_date

# In[42]:



def present_day(date):
    if date < date[:4]+'-6':
        return date
    else:
        return date[:4]+'-6'


# In[43]:


# 리밸런싱 백테스트 함수화 

def backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type=None, value_list=None, date_range=None):
    
    start_year = int(start_date.split('-')[0])
    end_year = int(end_date.split('-')[0])

    total_df = 0
    for temp in range(start_year, end_year):
        if temp != int(price_day[:4])-1:    
            this_term_start = str(temp) + '-' + start_date.split('-')[1]
            this_term_end = str(temp+1) + '-' + start_date.split('-')[1]
                
        elif temp == int(price_day[:4])-1:
            this_term_start = str(temp) + '-' + start_date.split('-')[1]
            this_term_end = str(temp+1) + '-'+ str(int(end_date[-2:]))
            
        strategy_date = get_strategy_date(this_term_start)

        if strategy.__name__ == 'high_roa':
            st_df = strategy(select_code_by_price(price_df, fr_df, this_term_start), strategy_date, num)
        elif strategy.__name__ == 'magic_formula':
            st_df = strategy(fr_df, select_code_by_price(price_df, invest_df, this_term_start), strategy_date, num)
        elif strategy.__name__ == 'get_value_rank':
            st_df = strategy(select_code_by_price(price_df, invest_df, this_term_start), value_type, strategy_date, num)
        elif strategy.__name__ == 'make_value_combo':
            st_df = strategy(value_list, select_code_by_price(price_df, invest_df, this_term_start), strategy_date, num)
        elif strategy.__name__ == 'get_fscore':
            st_df = strategy(select_code_by_price(price_df, fs_df, this_term_start), strategy_date, num)
        elif strategy.__name__ == 'get_momentum_rank':
            st_df = strategy(price_df, price_df[this_term_start].index[0] , date_range, num)
        elif strategy.__name__ == 'get_value_quality':
            st_df = strategy(select_code_by_price(price_df, invest_df, this_term_start), 
                             select_code_by_price(price_df, fs_df, this_term_start), strategy_date, num)
        backtest = backtest_beta(price_df, st_df, this_term_start, this_term_end, initial_money)
        temp_end = backtest[this_term_end].index[0]
        backtest = backtest[:temp_end]
        initial_money =  backtest['종합포트폴리오'][-1]
        if temp == start_year:
            total_df = backtest
        else:
            total_df = pd.concat([total_df[:-1], backtest])

    total_df ['일변화율'] = total_df ['종합포트폴리오'].pct_change()
    total_df ['총변화율'] = total_df ['종합포트폴리오']/ total_df ['종합포트폴리오'][0] - 1
    
    return total_df

# In[45]:


def total_rate(back_result, num):
    back_result['종합변화율'] = round(back_result['종합포트폴리오']/back_result['종합포트폴리오'][0],num)    
    return back_result


# In[48]:


import python_quant

# 전략비교 
start_date = '2013-6'
end_date = present_day(price_day)
initial_money = 100000000
# start_date = '2016-6'
# end_date = '2021-3'
initial_money = 100000000
strategy = python_quant.get_value_rank
num = 10


back_test_result0 = backtest_re(high_roa, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
back_test_result1 = backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PSR')
back_test_result2 = backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PBR')
back_test_result3 = backtest_re(magic_formula, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
# back_test_result4 = backtest_re(get_fscore, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
back_test_result5 = backtest_re(python_quant.get_momentum_rank, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, date_range=500)
back_test_result6 = backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PER','PBR'])
back_test_result7 = backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PER', 'PBR', 'PSR', 'PCR'])
# back_test_result8 = backtest_re(get_value_quality, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)

total_rate(back_test_result5, 3)

plt.figure(figsize=(15,10))
plt.rc('font', size=10)
plt.rc('xtick', labelsize=15) # fontsize of the tick labels
plt.rc('ytick', labelsize=15)
# back_test_result0['총변화율'].plot(label='ROA')
# back_test_result1['종합포트폴리오'].plot(color='#e35f62',label='PSR')
# back_test_result2['종합포트폴리오'].plot(color = '#a593f5',label='PBR')
# back_test_result3['종합포트폴리오'].plot(color = '#f2b277',label='magic')
back_test_result5['종합변화율'].plot(color = '#b46df2',label='momentum')
# back_test_result4['종합포트폴리오'].plot(color = '#5bd459',label='f-score')
# # back_test_result6['총변화율'].plot(label='combo')
# back_test_result7['종합포트폴리오'].plot(color = '#54bcf0',label='Undervalued stock')
# # back_test_result8['총변화율'].plot(label='f-score + Undervalued stock')

plt.legend(loc='best', ncol=1, fontsize=15)
plt.show()

# In[52]:


import python_quant

# 저PER과 저PBR 비교 
start_date = '2014-6'
end_date = present_day(price_day)
initial_money = 100000000
# start_date = '2016-6'
# end_date = '2021-3'
initial_money = 100000000
strategy = python_quant.get_value_rank
num = 10


back_test_result0 = backtest_re(high_roa, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
back_test_result1 = backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PSR')
back_test_result2 = backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PBR')
back_test_result3 = backtest_re(magic_formula, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
back_test_result4 = backtest_re(get_fscore, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
back_test_result5 = backtest_re(python_quant.get_momentum_rank, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, date_range=150)
back_test_result6 = backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PER','PBR'])
back_test_result7 = backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PER', 'PBR', 'PSR', 'PCR'])
back_test_result8 = backtest_re(get_value_quality, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)

# 종합변화율
for i in [back_test_result1,back_test_result2,back_test_result3,back_test_result4, back_test_result7]:
    total_rate(i, 3)

plt.figure(figsize=(15,10))
plt.rc('font', size=10)
plt.rc('xtick', labelsize=15) # fontsize of the tick labels
plt.rc('ytick', labelsize=15)
# back_test_result0['총변화율'].plot(label='ROA')
back_test_result1['종합변화율'].plot(color='#e35f62',label='PSR')
back_test_result2['종합변화율'].plot(color = '#a593f5',label='PBR')
back_test_result3['종합변화율'].plot(color = '#f2b277',label='PBR+ROA')
# back_test_result5['총변화율'].plot(color = '#a955f2', label='momentum')
back_test_result4['종합변화율'].plot(color = '#5bd459',label='f-score')
# back_test_result6['총변화율'].plot(label='combo')
back_test_result7['종합변화율'].plot(color = '#54bcf0',label='Undervalued stock')
# back_test_result8['총변화율'].plot(label='f-score + Undervalued stock')

plt.legend(loc='best', ncol=1, fontsize=15)
plt.show()

# In[53]:



a=[back_test_result0['총변화율'][-1],back_test_result6['총변화율'][-1],
   backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PCR')['총변화율'][-1],
   backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PSR')['총변화율'][-1],
   backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PSR','PBR'])['총변화율'][-1],
   backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PER','PSR'])['총변화율'][-1],
   backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PCR','PBR'])['총변화율'][-1],
   backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PSR','PCR'])['총변화율'][-1],
    back_test_result1['총변화율'][-1],back_test_result2['총변화율'][-1],back_test_result3['총변화율'][-1],
back_test_result5['총변화율'][-1],
back_test_result4['총변화율'][-1],
back_test_result7['총변화율'][-1],
back_test_result8['총변화율'][-1]]

pd.DataFrame(a)

# In[50]:


s_name = ['PSR','PBR','PBR+ROA','f-score','undervalued']
end = [back_test_result1['종합포트폴리오'][-1],back_test_result2['종합포트폴리오'][-1],back_test_result3['종합포트폴리오'][-1],back_test_result4['종합포트폴리오'][-1],back_test_result7['종합포트폴리오'][-1]]

d_end = pd.DataFrame(data=end, index=s_name, columns=['종합포트폴리오'],dtype=None, copy=False)
d_end

# In[54]:


# MDD 함수화 

def get_mdd(back_test_df):
    max_list = [0]
    mdd_list = [0]

    for i in back_test_df.index[1:]:
        max_list.append(back_test_df['총변화율'][:i].max())
        if max_list[-1] > max_list[-2]:
            mdd_list.append(0)
        else:
            mdd_list.append(min(back_test_df['총변화율'][i] - max_list[-1], mdd_list[-1])   )

    back_test_df['max'] = max_list
    back_test_df['MDD'] = mdd_list
    
    return back_test_df

# In[56]:


# MDD 비교하기 

start_date = '2016-6'
# end_date = '2021-3'
initial_money = 100000000
strategy = python_quant.get_value_rank

back_test_result1 = backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, 20, value_type='PSR')
back_test_result2 = backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, 20, value_type='PBR')
back_test_result3 = backtest_re(magic_formula, start_date, end_date, initial_money, price_df, fr_df, fs_df, 20, '2020')
back_test_result4 = backtest_re(get_fscore, start_date, end_date, initial_money, price_df, fr_df, fs_df, 20)
back_test_result5 = backtest_re(get_momentum_rank, start_date, end_date, initial_money, price_df, fr_df, fs_df, 20, date_range=250)

back_test_result1 = get_mdd(back_test_result1)
back_test_result2 = get_mdd(back_test_result2)
back_test_result3 = get_mdd(back_test_result3)
back_test_result4 = get_mdd(back_test_result4)
back_test_result5 = get_mdd(back_test_result5)

plt.figure(figsize=(10, 7))
plt.subplot(2,1,1)
# back_test_result1['총변화율'].plot(label='PER')
# back_test_result1['max'].plot(label='PER')
back_test_result2['총변화율'].plot(label='PBR')
back_test_result2['max'].plot(label='PBR')
back_test_result3['총변화율'].plot(label='magic')
back_test_result3['max'].plot(label='magic')
back_test_result4['총변화율'].plot(label='fscore')
back_test_result4['max'].plot(label='fscore')
back_test_result5['총변화율'].plot(label='momentum')
back_test_result5['max'].plot(label='momentum')
plt.legend(loc='lower left')

plt.subplot(3,1,3)
# back_test_result1['MDD'].plot(label='PER')
back_test_result2['MDD'].plot(label='PBR')
back_test_result3['MDD'].plot(label='magic')
back_test_result4['MDD'].plot(label='fscore')
back_test_result5['MDD'].plot(label='momentum')
plt.legend()

# In[64]:


# MDD 비교하기 

start_date = '2014-6'
# end_date = '2021-3'
initial_money = 100000000
strategy = python_quant.get_value_rank

# 종합변화율
for i in [back_test_result1,back_test_result2,back_test_result3,back_test_result4, back_test_result7]:
    total_rate(i, 3)

# plt.figure(figsize=(15,10))
# plt.rc('font', size=10)
# plt.rc('xtick', labelsize=15) # fontsize of the tick labels
# plt.rc('ytick', labelsize=15)
# # back_test_result0['총변화율'].plot(label='ROA')
# back_test_result1['종합변화율'].plot(color='#e35f62',label='PSR')
# back_test_result2['종합변화율'].plot(color = '#a593f5',label='PBR')
# back_test_result3['종합변화율'].plot(color = '#f2b277',label='PBR+ROA')
# # back_test_result5['총변화율'].plot(color = '#a955f2', label='momentum')
# back_test_result4['종합변화율'].plot(color = '#5bd459',label='f-score')
# # back_test_result6['총변화율'].plot(label='combo')
# back_test_result7['종합변화율'].plot(color = '#54bcf0',label='Undervalued stock')

back_test_result0 = backtest_re(high_roa, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
back_test_result1 = backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PSR')
back_test_result2 = backtest_re(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PBR')
back_test_result3 = backtest_re(magic_formula, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
back_test_result4 = backtest_re(get_fscore, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)
back_test_result5 = backtest_re(python_quant.get_momentum_rank, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, date_range=150)
back_test_result6 = backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PER','PBR'])
back_test_result7 = backtest_re(python_quant.make_value_combo, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_list=['PER', 'PBR', 'PSR', 'PCR'])
back_test_result8 = backtest_re(get_value_quality, start_date, end_date, initial_money, price_df, fr_df, fs_df, num)

back_test_result1 = get_mdd(back_test_result1)
back_test_result2 = get_mdd(back_test_result2)
back_test_result3 = get_mdd(back_test_result3)
back_test_result4 = get_mdd(back_test_result4)
back_test_result7 = get_mdd(back_test_result7)

plt.figure(figsize=(10, 7))
plt.subplot(2,1,1)
back_test_result1['총변화율'].plot(label='PSR')
back_test_result1['max'].plot(label='PSR')
back_test_result2['총변화율'].plot(label='PBR')
back_test_result2['max'].plot(label='PBR')
back_test_result3['총변화율'].plot(label='PBR+ROA')
back_test_result3['max'].plot(label='PBR+ROA')
back_test_result4['총변화율'].plot(label='f-score')
back_test_result4['max'].plot(label='f-score')
back_test_result7['총변화율'].plot(label='Undervalued stock')
back_test_result7['max'].plot(label='Undervalued stock')
plt.legend(loc='lower left')

plt.subplot(3,1,3)
back_test_result1['MDD'].plot(label='PSR')
back_test_result2['MDD'].plot(label='PBR')
back_test_result3['MDD'].plot(label='PBR+ROA')
back_test_result4['MDD'].plot(label='f-score')
back_test_result7['MDD'].plot(label='Undervalued stock')
plt.legend()

# In[66]:


# 종합변화율
for i in [back_test_result1,back_test_result2,back_test_result3,back_test_result4, back_test_result7]:
    total_rate(i, 3)

# In[67]:


# 종합변화율 데이터프레임 만들기
start_date = '2014-6'
end_date = present_day(price_day)

from functools import reduce
dfs = [back_test_result4['종합변화율'], back_test_result2['종합변화율'], back_test_result3['종합변화율'], back_test_result1['종합변화율'], back_test_result7['종합변화율']]
df_total_por= reduce(lambda left, right: pd.merge(left, right,left_index=True, right_index=True), dfs)
df_total_por.columns=['fscore','pbr','pbr_roa','psr','undervalued']
df_total_por

# In[1]:


# 종목수 선택
start_date = '2014-6'
end_date = present_day(price_day)
initial_money = 100000000

kind_total_por = pd.DataFrame(data=None, index=None, columns=['종목수','fscore','pbr','pbr_roa','psr','undervalued'], dtype=None, copy=False)

kinds=[]
for i in [5, 10, 15, 20]:
    num = i
    ee = str(i)
    
    dfs = [back_test_result4['종합변화율'], back_test_result2['종합변화율'], back_test_result3['종합변화율'], back_test_result1['종합변화율'], back_test_result7['종합변화율']]
    df_total_por= reduce(lambda left, right: pd.merge(left, right,left_index=True, right_index=True), dfs)
    back=len(df_total_por)
    kinds.extend(('{},'.format(ee)*back)[:-1].split(','))
    df_total_por.columns=['fscore','pbr','pbr_roa','psr','undervalued']
    kind_total_por = pd.concat([kind_total_por,df_total_por])
    
kind_total_por['종목수']=kinds
kind_total_por['date'] = kind_total_por.index
kind_total_por=kind_total_por[['date','종목수','fscore','pbr','pbr_roa','psr','undervalued']]

# In[ ]:


kind_total_por.to_csv('./1.port_strategy.csv', encoding='utf-8')

# In[ ]:


import python_quant
def result_total(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type=None, value_list=None, date_range=None):
    
    start_year = int(start_date.split('-')[0])
    end_year = int(end_date.split('-')[0])

    total_df = 0
    for temp in range(start_year, end_year):
        if temp != int(price_day[:4])-1:    
            this_term_start = str(temp) + '-' + start_date.split('-')[1]
            this_term_end = str(temp+1) + '-' + start_date.split('-')[1]
                
        elif temp == int(price_day[:4])-1:
            this_term_start = str(temp) + '-' + start_date.split('-')[1]
            this_term_end = str(temp+1) + '-'+ str(int(end_date[-2:]))
            
        strategy_date = get_strategy_date(this_term_start)

        if strategy.__name__ == 'high_roa':
            st_df = strategy(select_code_by_price(price_df, fr_df, this_term_start), strategy_date, num)
        elif strategy.__name__ == 'magic_formula':
            st_df = strategy(fr_df, select_code_by_price(price_df, invest_df, this_term_start), strategy_date, num)
        elif strategy.__name__ == 'get_value_rank':
            st_df = strategy(select_code_by_price(price_df, invest_df, this_term_start), value_type, strategy_date, num)
        elif strategy.__name__ == 'make_value_combo':
            st_df = strategy(value_list, select_code_by_price(price_df, invest_df, this_term_start), strategy_date, num)
        elif strategy.__name__ == 'get_fscore':
            st_df = strategy(select_code_by_price(price_df, fs_df, this_term_start), strategy_date, num)
        elif strategy.__name__ == 'get_momentum_rank':
            st_df = strategy(price_df, price_df[this_term_start].index[0] , date_range, num)
        elif strategy.__name__ == 'get_value_quality':
            st_df = strategy(select_code_by_price(price_df, invest_df, this_term_start), 
                             select_code_by_price(price_df, fs_df, this_term_start), strategy_date, num)
        if temp == start_year:
            total_df = st_df
        else:
            total_df = pd.concat([total_df, st_df])
    return total_df

# In[ ]:


kind=[]
f=[]
p=[]
pr=[]
ps=[]
u=[]
date = []
rank=[]

start_date = '2014-6'
end_date = present_day(price_day)
initial_money = 100000000

start_year = int(start_date.split('-')[0])
end_year = int(end_date.split('-')[0])

for i in [5, 10, 15, 20]:
    num = i
    dd = str(i)
    kind.extend(('{},'.format(dd)*i*(end_year-start_year))[:-1].split(','))
    for y in list(range(start_year+1,end_year+1)):
        st_y = str(y)
        date.extend(('{},'.format(st_y)*i)[:-1].split(','))
        index_date = st_y
        rank.extend(list(range(1,i+1)))
    fscore = list(result_total(get_fscore, start_date, end_date, initial_money, price_df, fr_df, fs_df, num).index)
    f.extend(fscore)
    pbr = list(result_total(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PBR').index)
    p.extend(pbr)
    pbr_roa = list(result_total(magic_formula, start_date, end_date, initial_money, price_df, fr_df, fs_df, num).index)
    pr.extend(pbr_roa)
    psr = list(result_total(strategy, start_date, end_date, initial_money, price_df, fr_df, fs_df, num, value_type='PSR').index)
    ps.extend(psr)
    undervalued = list(result_total(get_value_quality, start_date, end_date, initial_money, price_df, fr_df, fs_df, num).index)
    u.extend(undervalued)
        
    

    

# In[530]:


df_kind = pd.DataFrame(data=None, index=None, columns=['stocks_num','rank','year','fscore','pbr','pbr_roa','psr','undervalued'], dtype=None, copy=False)
df_kind['stocks_num']=kind
df_kind['rank'] = rank
df_kind['year'] = date
df_kind['fscore'] = f
df_kind['pbr'] = p
df_kind['pbr_roa'] = pr
df_kind['psr'] = ps
df_kind['undervalued'] = u
df_kind

# In[532]:


df_kind.to_csv('./2.strategy_stocks.csv', index=False)

# In[522]:


df_mo = pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)
m_name=[]
m_n = [50, 100, 150, 200, 250, 300, 400, 500]

start_date = '2013-6'
end_date = present_day(price_day)
initial_money = 100000000


for i in m_n:  
    m_name.append('momentum_'+str(i))
    momen_c = pd.Series([], name = 'e')
    kinds=[]
    for j in [5, 10, 15, 20]:
        num = j
        ee = str(j)
        
        momen = backtest_re(python_quant.get_momentum_rank, start_date, end_date, initial_money, price_df, fr_df, fs_df, j, date_range=i)
        momen = total_rate(momen, 3)['종합변화율']
        momen_c = pd.concat([momen_c, momen], axis = 0) 
        back = len(momen)
        kinds.extend(('{},'.format(ee)*back)[:-1].split(','))
    df_mo = pd.concat([df_mo,momen_c], axis=1)

    
df_mo.columns=m_name
df_mo['stocks_num']=kinds
df_mo['date'] = df_mo.index
df_mo = df_mo[['date',
               'stocks_num',
               'momentum_50',
               'momentum_100',
               'momentum_150',
               'momentum_200',
               'momentum_250',
               'momentum_300',
               'momentum_400',
               'momentum_500']]
df_mo

# In[495]:


df_mo.to_csv('./3.mo_port_strategy.csv', index=False, encoding='utf-8')

# In[497]:


from pandas import Series

df_momen = pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)
m_name=[]
m_n = [50, 100, 150, 200, 250, 300, 400, 500]

yyy=[list(range(start_year+1,end_year+2))

start_date = '2013-6'
end_date = present_day(price_day)
initial_money = 100000000

for i in m_n:  
    m_name.append('momentum_'+str(i))
    mo_c = pd.Series([], name = 'e')
    kinds=[]
    rank=[]
    rr=[]
    date =[]
    for j in [5, 10, 15, 20]:
        num = j
        ee = str(j)
        kinds.extend(('{},'.format(ee)*len(yyy)*j)[:-1].split(','))
        for k in yyy:
            y=str(k)
            date.extend(('{},'.format(y)*j)[:-1].split(','))
            index_date = y
            
            rank.extend(list(range(1,j+1)))
            rr.append(list(mm['모멘텀순위']))
        mm = result_total(get_momentum_rank, start_date, end_date, initial_money, price_df, fr_df, fs_df, j, date_range=i)
        Momentum = Series(data=list(mm.index))
        mo_c = pd.concat([mo_c, Momentum], axis = 0) 
    df_momen = pd.concat([df_momen,mo_c], axis=1)


df_momen.columns=m_name
df_momen['stocks_num']=kinds
df_momen['rank'] = rank
df_momen['year'] = date
df_momen = df_momen[['stocks_num',
               'rank',
               'year',
               'momentum_50',
               'momentum_100',
               'momentum_150',
               'momentum_200',
               'momentum_250',
               'momentum_300',
               'momentum_400',
               'momentum_500']]
df_momen

# In[498]:


df_momen.to_csv('./4.mo_strategy_stocks.csv', index=False, encoding='utf-8')
