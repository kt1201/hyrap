from datetime import datetime, timedelta
from sqlalchemy import create_engine, types
import pandas as pd
import numpy as np
import time
import sys
import os
import requests
import json

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import Stock

engine = Stock.engine
now_year = datetime.now().year

url = 'http://www.fnspace.com/Api/FinanceApi'
api_key='3D7E4600CC0444456E63'
column_list = Stock.Stock("company_fs").get_column_list()  # 재무재표 항목 list 생성

def collector(stock, trcode, company, cnt, total_cnt, skip):
    global engine
    global now_year
    global url
    global api_key
    global column_list
    code = "A" + company['code']

    try:
        # 컬럼 json 형식 생성
        fs = {}
        for i in range(9):
            year = now_year - 1 - i
            fs[str(year)] = {}

        item_list = []
        item_str = ""
        for i in range(len(column_list)):
            if (i%20 == 0):
                if (i != 0):
                    item_list.append(item_str[:-1])
                    item_str = ""
            column = column_list[i]
            item_str = item_str + column['code'] + ','
            if (i == len(column_list)-1):
                item_list.append(item_str[:-1])
                item_str = ""

        for item in item_list:
            # 항목코드 code, name 형식의 dict 생성
            item_array = item.split(',')
            item_array = stock.fs_array_to_dict(item_array)
            params = {
                'key': api_key,
                'format': 'json',
                'consolgb': 'M',
                'annualgb': 'A',
                'fraccyear': str(now_year-9),
                'toaccyear': str(now_year-1),
                'code': code,
                'item': item
            }
            res = requests.get(url, params)
            result = json.loads(res.text)
            if (len(result['dataset']) != 0):
                data_list = result['dataset'][0]['DATA']
                for data in data_list:
                    year = data['DATE'].replace(" ", "")
                    for column in item_array:
                        fs[year][column['name']] = data[column['code']]
            else:
                fs = 0

        if (fs != 0):
            df = pd.DataFrame.from_dict(fs, orient='index')
            df['기준년도'] = df.index
            df['종목코드'] = company['code']

            df.replace('NaN', '', inplace=True)  # 'NaN' 값  ''으로 대체
            # print(df)

            df.to_sql(trcode, engine, if_exists='append', index=False, chunksize=1000)
            print("{}/{} {}-{} 수집/적재 완료".format(cnt, total_cnt, trcode, company["name"]))
            pkey = "{}".format(company["name"])
            stock.set_batch_result(trcode, pkey, "SUCCESS", skip)
        else:
            print("{}/{} {}-{} 데이터 없음".format(cnt, total_cnt, trcode, company["name"]))
            pkey = "{}".format(company["name"])
            stock.set_batch_result(trcode, pkey, "NO DATA", skip)
    except Exception as ex:
        print("{}/{} {}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, company["name"]))
        pkey = "{}".format(company["name"])
        stock.set_batch_result(trcode, pkey, "FAIL", 3)
        print(ex)


def main(stock, trcode):
    cnt = 0
    # company_list = stock.get_list("kospi_200")  # 코스피 200 종목 list 생성
    company_list = stock.get_list("stock")  # 전체 종목 list 생성
    total_cnt = len(company_list)

    print("{} 수집/적재 시작".format(trcode))
    for company in company_list:
        cnt = cnt + 1
        # 과거 수집/적재 조회
        pkey = "{}".format(company["name"])
        skip = stock.past_collect(trcode, pkey)
        if skip == 1 or skip == 2:
            print("{}/{} {}-{} 건너뛰기".format(cnt, total_cnt, trcode, company["name"]))
        else:
            collector(stock, trcode, company, cnt, total_cnt, skip)
    stock.get_status(trcode)


if __name__ == "__main__":
    start = time.time()  # 시작시간 저장
    stock = Stock.Stock("company_fs")
    main(stock, "company_fs")
    # collector("company_info", {"code": "000120", "name": "CJ대한통운"}, 1, 1, 0)
    elapsed_time = stock.convert_seconds_to_time(time.time() - start)
    print("실행시간 :", elapsed_time)  # 현재시각 - 시작시간 = 실행 시간