import os
import sys
import Stock
import time
from datetime import datetime

from openapi import company_fs, news, opt10001, opt10027, opt10030, opt10033, opt10081, opt10082, opt10083, opt10086, opt20002, opt20003, opt20006, opt20007, opt20008, opt90002

def main(batch, stock):

    python32 = "c:/Users/ktkim1201/AppData/Local/Programs/Python/Python37-32/python.exe"
    RoboAdvisor = "D:/Competition2021"
    
    batch_time = [{'table_nm': 'news', 'module': news}]
    batch_day = [
        {'table_nm': 'opt10001', 'module': opt10001},
        {'table_nm': 'opt10027', 'module': opt10027},
        {'table_nm': 'opt10030', 'module': opt10030},
        {'table_nm': 'opt10033', 'module': opt10033},
        {'table_nm': 'opt10081', 'module': opt10081},
        {'table_nm': 'opt10086', 'module': opt10086},
        {'table_nm': 'opt20002', 'module': opt20002},
        {'table_nm': 'opt20003', 'module': opt20003},
        {'table_nm': 'opt20006', 'module': opt20006},
        {'table_nm': 'opt90002', 'module': opt90002}
    ]
    batch_week = [
        {'table_nm': 'opt10082', 'module': opt10082},
        {'table_nm': 'opt20007', 'module': opt20007}
    ]
    batch_month = [
        {'table_nm': 'opt10083', 'module': opt10083},
        {'table_nm': 'opt20008', 'module': opt20008}
    ]

    if (batch == "time"):
        for table in batch_time:
            status = 0
            print("========== {} 시작 ==========".format(table["table_nm"]))
            try:
                command = 'powershell "{0} {1}/openapi/{2}.py 2>&1 | tee {1}/log/{2}.log"'.format(python32, RoboAdvisor, table["table_nm"])
                os.system(command)
            except Exception as ex:
                print(ex)
                status = 1
            if (status == 0):
                print("========== {} 성공 ==========".format(table["table_nm"]))
            else:
                print("========== {} 실패 ==========".format(table["table_nm"]))
    elif (batch == "day"):
        exitcode = stock.StockDB.insert_company()
        if (exitcode == 0):
            print("stock 업데이트 완료")
            for table in batch_day:
                query = "SELECT status FROM batch_result WHERE pkey = %s AND batch_date = CAST(%s AS DATE) AND status = 'SUCCESS'"
                binds = [table["table_nm"], datetime.now().strftime("%Y%m%d")]
                data = stock.StockDB.execute(query, binds)[0]
                if (len(data) == 0):
                    status = 0
                    print("========== {} 시작 ==========".format(table["table_nm"]))
                    try:
                        command = 'powershell "{0} {1}/openapi/{2}.py 2>&1 | tee {1}/log/{2}.log"'.format(python32, RoboAdvisor, table["table_nm"])
                        os.system(command)
                    except Exception as ex:
                        print(ex)
                        status = 1
                    if (status == 0):
                        print("========== {} 성공 ==========".format(table["table_nm"]))
                    else:
                        print("========== {} 실패 ==========".format(table["table_nm"]))
                    time.sleep(60)
                else:
                    print("========== {} 건너뛰기 ==========".format(table["table_nm"]))
    elif (batch == "week"):
        for table in batch_week:
            query = "SELECT status FROM batch_result WHERE pkey = %s AND batch_date = CAST(%s AS DATE) AND status = 'SUCCESS'"
            binds = [table["table_nm"], datetime.now().strftime("%Y%m%d")]
            data = stock.StockDB.execute(query, binds)[0]
            if (len(data) == 0):
                status = 0
                print("========== {} 시작 ==========".format(table["table_nm"]))
                try:
                    command = 'powershell "{0} {1}/openapi/{2}.py 2>&1 | tee {1}/log/{2}.log"'.format(python32, RoboAdvisor, table["table_nm"])
                    os.system(command)
                except Exception as ex:
                    print(ex)
                    status = 1
                if (status == 0):
                    print("========== {} 성공 ==========".format(table["table_nm"]))
                else:
                    print("========== {} 실패 ==========".format(table["table_nm"]))
                time.sleep(60)
            else:
                print("========== {} 건너뛰기 ==========".format(table["table_nm"]))
    elif (batch == "month"):
        for table in batch_month:
            query = "SELECT status FROM batch_result WHERE pkey = %s AND batch_date = CAST(%s AS DATE) AND status = 'SUCCESS'"
            binds = [table["table_nm"], datetime.now().strftime("%Y%m%d")]
            data = stock.StockDB.execute(query, binds)[0]
            if (len(data) == 0):
                status = 0
                print("========== {} 시작 ==========".format(table["table_nm"]))
                try:
                    command = 'powershell "{0} {1}/openapi/{2}.py 2>&1 | tee {1}/log/{2}.log"'.format(python32, RoboAdvisor, table["table_nm"])
                    os.system(command)
                except Exception as ex:
                    print(ex)
                    status = 1
                if (status == 0):
                    print("========== {} 성공 ==========".format(table["table_nm"]))
                else:
                    print("========== {} 실패 ==========".format(table["table_nm"]))
                time.sleep(60)
            else:
                print("========== {} 건너뛰기 ==========".format(table["table_nm"]))

if __name__ == "__main__":
    start = time.time()  # 시작시간 저장
    if len(sys.argv) != 2:
        print("python batch.py [time|day|week|month]")
        sys.exit()
    else:
        if (sys.argv[1] == "time"):
            stock = Stock.Stock("news")
            main(sys.argv[1], stock)
        else:
            stock = Stock.Stock()
            main(sys.argv[1], stock)
    elapsed_time = stock.convert_seconds_to_time(time.time() - start)
    print("실행시간 :", elapsed_time)  # 현재시각 - 시작시간 = 실행 시간