from pykiwoom.kiwoom import *
from datetime import datetime, timedelta
from sqlalchemy import create_engine, types
import psycopg2
import pandas as pd
import numpy as np
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import Stock

kiwoom = Kiwoom()
engine = Stock.engine
now = datetime.now().strftime("%Y%m%d")

def collector(stock, trcode, company, cnt, total_cnt, skip):
    global engine
    global now

    try:
        # 점검시간 확인
        stock.maintenance()

        # 데이터 수집
        df = kiwoom.block_request(trcode,
                    종목코드=company["code"],
                    조회일자=now,
                    표시구분=0,
                    output="일별주가요청",
                    next=0)
        # 없는 데이터들은 삭제
        indexs = df[df["날짜"] == ''].index
        df.drop(indexs, inplace=True)
        # 데이터가 있을 때만 적재
        if len(df) != 0:
            df.replace('NaN', '', inplace=True)  # 비어있는 값  ''으로 대체
            df.rename(columns = {'금액(백만)': '금액'}, inplace=True)
            df["종목코드"] = company["code"]

            # -- -> - 로 치환
            for i in range(len(df)):
                row_data = []
                for column in df.columns:
                    data = df.loc[i][column]
                    data = str(data).replace("--", "-").replace("+-", "+")
                    row_data.append(data)
                df.loc[i] = row_data

            df.replace('', psycopg2.extensions.AsIs('NULL'), inplace=True)  # '' 값  NULL 값으로 대체
            df.replace('NaN', psycopg2.extensions.AsIs('NULL'), inplace=True)  # 'NaN' 값  NULL 값으로 대체
            # 데이터 적재
            df.to_sql(trcode, engine, if_exists='append', index=False, chunksize=1000)
            print("{}/{} {}-{} 수집/적재 완료".format(cnt, total_cnt, trcode, company["code"]))
            pkey = "{}".format(company["code"])
            stock.set_batch_result(trcode, pkey, "SUCCESS", skip)
        else:
            print("{}/{} {}-{} 데이터 없음".format(cnt, total_cnt, trcode, company["code"]))
            pkey = "{}".format(company["code"])
            stock.set_batch_result(trcode, pkey, "NO DATA", skip)
    except Exception as ex:
        print("{}/{} {}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, company["code"]))
        pkey = "{}".format(company["code"])
        stock.set_batch_result(trcode, pkey, "FAIL", 3)
        print(ex)

def main(stock, trcode):

    cnt = 0
    use_api_cnt = 0  # 조회제한 회피를 위한 api 횟수 카운트
    # company_list = stock.get_list("kospi_200")  # 코스피 200 종목 list 생성
    company_list = stock.get_list("stock")  # 전체 종목 list 생성
    total_cnt = len(company_list)
    exitcode = stock.StockDB.truncate_table(trcode)
    if (exitcode == 0):
        print("{} 테이블 비우기 완료".format(trcode))
        print("{} 수집/적재 시작".format(trcode))
        for company in company_list:
            cnt = cnt + 1
            use_api = False
            # 과거 수집/적재 조회
            pkey = "{}".format(company["code"])
            skip = stock.past_collect(trcode, pkey)
            if skip == 1:
                print("{}/{} {}-{} 건너뛰기".format(cnt, total_cnt, trcode, company["code"]))
            else:
                collector(stock, trcode, company, cnt, total_cnt, skip)
                use_api = True
                use_api_cnt = use_api_cnt + 1
            # API 사용시에만 조회제한 delay
            if use_api:
                stock.delay(use_api_cnt)  # 조회제한 회피
    else:
        print("{} 테이블 비우기 실패".format(trcode))
        print("{}/{} {}-{}-{} 수집/적재 실패".format(cnt, total_cnt, trcode))
        stock.set_batch_result(trcode, trcode, "FAIL", 3)
    stock.get_status(trcode)


if __name__ == "__main__":
    print("{} 배치 시작".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    start = time.time()  # 시작시간 저장
    stock = Stock.Stock()
    main(stock, "opt10086")
    elapsed_time = stock.convert_seconds_to_time(time.time() - start)
    print("실행시간 :", elapsed_time)  # 현재시각 - 시작시간 = 실행 시간
    print("{} 배치 종료".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))