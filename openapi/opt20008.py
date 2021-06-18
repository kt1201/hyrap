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
end_date = ""
now = datetime.now().strftime("%Y%m%d")

def collector(stock, trcode, sector, std_date, cnt, total_cnt, skip, column_list):
    global engine
    global end_date

    try:
        # 점검시간 확인
        stock.maintenance()

        # 데이터 수집
        df = kiwoom.block_request(trcode,
                    업종코드=sector["code"],
                    기준일자=std_date,
                    output="업종월봉조회",
                    next=0)
        # 없는 데이터들은 삭제
        indexs = df[df["일자"] == ''].index
        df.drop(indexs, inplace=True)
        # 데이터가 있을 때만 적재
        if len(df) != 0:
            end_date = df['일자'][len(df)-1]
            df.drop(columns=["대업종구분", "소업종구분", "종목정보", "전일종가"], inplace=True)
            df['업종코드'] = sector["code"]
            df.replace('', psycopg2.extensions.AsIs('NULL'), inplace=True)  # '' 값  NULL 값으로 대체
            df.replace('NaN', psycopg2.extensions.AsIs('NULL'), inplace=True)  # 'NaN' 값  NULL 값으로 대체
            # 데이터 적재
            for i in range(len(df)):
                dup_query = "SELECT * FROM {} WHERE 업종코드 = %s AND 일자 = %s".format(trcode)
                dup_binds = [df.iloc[i]["업종코드"], df.iloc[i]["일자"]]
                result = stock.StockDB.execute(dup_query, dup_binds)[0]
                if (len(result) == 0):
                    binds = []
                    for column in column_list:
                        binds.append(df.iloc[i][column])
                    stock.StockDB.insert(trcode, binds)
                    print("{}/{} {}-{}-{} 수집/적재 완료".format(cnt, total_cnt, trcode, sector["code"], df.iloc[i]["일자"]))
                    pkey = "{}-{}".format(sector["code"], df.iloc[i]["일자"])
                    stock.set_batch_result(trcode, pkey, "SUCCESS", skip)
                else:
                    end_date = 'NO DATA'
        else:
            end_date = 'NO DATA'
            print("{}/{} {}-{}-{} 데이터 없음".format(cnt, total_cnt, trcode, sector["code"], std_date))
            pkey = "{}-{}".format(sector["code"], std_date)
            stock.set_batch_result(trcode, pkey, "NO DATA", skip)
    except Exception as ex:
        print("{}/{} {}-{}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, sector["code"], std_date))
        pkey = "{}-{}".format(sector["code"], std_date)
        stock.set_batch_result(trcode, pkey, "FAIL", 3)
        print(ex)

def make_binds(stock, trcode):
    query = "SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = %s"
    binds = [trcode]
    result = []
    for column in stock.StockDB.execute(query, binds)[0]:
        result.append(column[0])
    return result

def main(stock, trcode):
    global end_date
    global now

    cnt = 0
    use_api_cnt = 0  # 조회제한 회피를 위한 api 횟수 카운트
    sector_list = stock.get_list("sector")  # 업종 list 생성
    total_cnt = len(sector_list)
    column_list = make_binds(stock, trcode)
    print("{} 수집/적재 시작".format(trcode))
    for sector in sector_list:
        cnt = cnt + 1
        loop_cnt = 0
        end_date = now  # end_date 초기화
        while(end_date != 'NO DATA'):
            loop_cnt = loop_cnt + 1
            if (datetime.strptime(end_date, "%Y%m%d").weekday() == 0):
                date = datetime.strptime(end_date, "%Y%m%d") - timedelta(3)
            elif (datetime.strptime(end_date, "%Y%m%d").weekday() == 6):
                date = datetime.strptime(end_date, "%Y%m%d") - timedelta(2)
            else:
                date = datetime.strptime(end_date, "%Y%m%d") - timedelta(1)
            end_date = date.strftime("%Y%m%d")
            # 과거 수집/적재 조회
            pkey = "{}-{}".format(sector["code"], end_date)
            pkey_column = ["종목코드"]
            skip = stock.past_collect(trcode, pkey, pkey_column)
            if skip == 1:
                print("{}/{} {}-{}-{} 건너뛰기".format(cnt, total_cnt, trcode, sector["code"], end_date))
                end_date = "NO DATA"
            elif skip == 2:
                end_date = "NO DATA"
            elif skip == 3:
                end_date = now
                print("{}/{} {}-{} 실패 발견...재적재".format(cnt, total_cnt, trcode, sector["code"]))
                stock.delete_company_data(trcode, sector)
            else:
                collector(stock, trcode, sector, end_date, cnt, total_cnt, skip, column_list)
                use_api_cnt = use_api_cnt + 1
                stock.delay(use_api_cnt)  # 조회제한 회피
    stock.get_status(trcode)


if __name__ == "__main__":
    print("{} 배치 시작".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    start = time.time()  # 시작시간 저장
    stock = Stock.Stock()
    main(stock, "opt20008")
    elapsed_time = stock.convert_seconds_to_time(time.time() - start)
    print("실행시간 :", elapsed_time)  # 현재시각 - 시작시간 = 실행 시간
    print("{} 배치 종료".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))