from pykiwoom.kiwoom import *
from datetime import datetime, timedelta
from sqlalchemy import create_engine, types
import pandas as pd
import numpy as np
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import Stock

kiwoom = Kiwoom()
engine = Stock.engine

# 수집/적재
def collector(stock, trcode, market, cnt, total_cnt):
    global engine

    try:
        # 점검시간 확인
        stock.maintenance()

        # 데이터 수집
        df = kiwoom.block_request(trcode,
                        시장구분=market["code"],
                        거래량구분="0",
                        종목조건="0",
                        상하한포함="1",
                        신용조건="0",
                        output="신용비율상위요청",
                        next=0)
        # 데이터가 있을 때만 적재
        if len(df) != 0:
            df['시장구분'] = market['name']
            df.to_sql(trcode, engine, if_exists='append', index=False, chunksize=1000)
            # df.to_csv("./csv/" + trcode + ".csv", sep=',', index=False, header=False, chunksize=50)
            print("{}/{} {}-{} 수집/적재 완료".format(cnt, total_cnt, trcode, market["code"]))
            pkey = "{}".format(market["code"])
            stock.set_batch_result(trcode, pkey, "SUCCESS")
        else:
            print("{}/{} {}-{} 데이터 없음".format(cnt, total_cnt, trcode, market["code"]))
            pkey = "{}".format(market["code"])
            stock.set_batch_result(trcode, pkey, "NO DATA")
    except Exception as ex:
        print("{}/{} {}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, market["code"]))
        pkey = "{}".format(market["code"])
        stock.set_batch_result(trcode, pkey, "FAIL", 3)
        print(ex)

def main(stock, trcode):

    cnt = 0
    use_api_cnt = 0  # 조회제한 회피를 위한 api 횟수 카운트
    market_list = [{'code': '001', 'name': '코스피'}, {'code': '101', 'name': '코스닥'}]  # 시장 list 생성
    total_cnt = len(market_list)
    exitcode = stock.StockDB.truncate_table(trcode)
    if (exitcode == 0):
        print("{} 테이블 비우기 완료".format(trcode))
        print("{} 수집/적재 시작".format(trcode))
        for market in market_list:
            cnt = cnt + 1
            use_api = False
            collector(stock, trcode, market, cnt, total_cnt)
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
    main(stock, "opt10033")
    elapsed_time = stock.convert_seconds_to_time(time.time() - start)
    print("실행시간 :", elapsed_time)  # 현재시각 - 시작시간 = 실행 시간
    print("{} 배치 종료".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))