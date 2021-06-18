# pip3 install pykiwoom
# pip3 install pywin32

from pykiwoom.kiwoom import *
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import time

import StockDB

engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=rootpass")
now = datetime.now().strftime("%Y%m%d")  # 현재날짜
collect_start = datetime.strptime("20000101", "%Y%m%d")  # 처음 수집할 날짜

class Stock(Kiwoom):
    def __init__(self, trcode="opt"):
        self.StockDB = StockDB.StockDB(trcode)


    def maintenance(self):
        day_list = ["월", "화", "수", "목", "금", "토", "일"]
        t = datetime.now().strftime('%H:%M:%S')
        now_time = time.strptime(t, '%H:%M:%S')
        now_day = datetime.today().weekday()
        if now_day in [0,1,2,3,4,5]:
            if (now_time > time.strptime('04:55:00', '%H:%M:%S') and now_time < time.strptime('05:10:00', '%H:%M:%S')):
                print("{}요일은 05:00 ~ 05:10 까지 점검시간".format(day_list[now_day]))
                print("점검중...")
                time.sleep(20*60)
        else:
            if (now_time > time.strptime('03:55:00', '%H:%M:%S') and now_time < time.strptime('03:59:59', '%H:%M:%S')):
                print("일요일은 04:00 ~ 04:30 까지 점검시간")
                print("점검중...")
                time.sleep(40*60)


    # 과거 수집/적재 확인, 0: 데이터 적재 기록 없음, 1: SUCCESS, 2: NO DATA, 3: FAIL 
    def past_collect(self, trcode, pkey, pkey_column=[]):
        skip = 0
        query = "SELECT * from batch_result WHERE table_nm = %s AND pkey = %s AND batch_date = CAST(%s AS DATE);"
        binds = [trcode, pkey, now]
        data = self.StockDB.execute(query, binds)[0]
        if len(data) == 1:
            if data[0][2] == "SUCCESS":
                skip = 1
            elif data[0][2] == "NO DATA":
                skip = 2
            elif data[0][2] == "FAIL":
                skip = 3
                # # date 루프가 있는 수집기는 pkey_column이 존재한다.
                # # 해당 기본키에 관한 데이터 삭제후 재적재 한다.(기본키 제약)
                # if len(pkey_column) > 0:
                #     where = ""
                #     i = 0
                #     pkey_list = pkey.split("-")
                #     for column in pkey_column:
                #         one = column + " = '" + pkey_list[i] + "',"
                #         where = where + one
                #         i = i + 1
                #     query1 = "DELETE FROM {} WHERE {};".format(trcode, where[:-1])
                #     query1 = "DELETE FROM batch_result WHERE {};".format(trcode, where[:-1])
        return skip

    
    def delete_overlap(self, table_nm, date_column_name, df):
        df_list = []
        for i in range(len(df)-1):
            query = "SELECT * FROM {} WHERE {} = %s".format(table_nm, date_column_name)
            binds = [df['일자'][i]]
            data = self.StockDB.execute(query, binds)[0]
            if data == 0:
                df_list.append(df.loc[i])
        df = pd.concat(df_list)
        return df


    def delete_company_data(self, table_nm, company):
        query1 = "DELETE FROM {} WHERE 종목코드 = %s".format(table_nm, company['code'])
        self.StockDB.execute(query1)
        query2 = "DELETE FROM batch_result WHERE table_nm = '{}' AND pkey LIKE '{}-%';".format(table_nm, company['code'])
        self.StockDB.execute(query2)


    def get_status(self, trcode):
        query = "SELECT status FROM batch_result WHERE table_nm = %s AND batch_date = CAST(%s AS DATE)"
        binds = [trcode, now]
        data = self.StockDB.execute(query, binds)[0]
        if "FAIL" in data:
            print("{} 수집/적재 실패".format(trcode))
        else:
            print("{} 수집/적재 완료".format(trcode))
            self.set_batch_result(trcode, trcode, "SUCCESS", 1)

    
    # 수집 결과 저장
    def set_batch_result(self, table_nm, pkey, status, skip=0):
        binds = [table_nm, pkey, status, now]
        if skip == 3:
            query = "SELECT * FROM batch_result WHERE table_nm = '{}' AND pkey = '{}';".format(table_nm, pkey)
            data = self.StockDB.execute(query)[0]
            if len(data) > 0:
                self.StockDB.batch_result_update(table_nm, pkey, status)
            else:
                self.StockDB.insert("batch_result", binds)
        else:
            self.StockDB.insert("batch_result", binds)


    # 종목코드 배열을 dict배열 형태로 변환
    def array_to_dict(self, array):
        dict_array = []
        for data in array:
            if type(data) == str:
                name = self.GetMasterCodeName(data)
                dict = {"code": data, "name": name}
            elif type(data) == tuple:
                dict = {"code": data[0], "name": data[1]}
            dict_array.append(dict)
        return dict_array
    

    # 항목 코드 배열을 dict 형태로 변환
    def fs_array_to_dict(self, array):
        dict_array = []
        for item in array:
            query = "SELECT 항목명 FROM fs_column WHERE 항목코드 = '{}'".format(item)
            data = self.StockDB.execute(query)[0][0][0]
            dict = {'code': item, 'name': data}
            dict_array.append(dict)
        return dict_array


    # 코드&이름 리스트 가져와 {"code": code, "name": name} 형태의 배열로 리턴
    def get_list(self, table_nm):
        query = "SELECT * FROM {};".format(table_nm)
        data = self.StockDB.execute(query)[0]
        list = self.array_to_dict(data)
        return list


    # 항목코드&항목명 리스트 가져와 {"code": code, "name": name} 형태의 배열로 리턴
    def get_column_list(self):
        query = 'SELECT "항목코드", "항목명" FROM fs_column;'
        data = self.StockDB.execute(query)[0]
        list = self.array_to_dict(data)
        return list

    
    # 조회제한 회피, 5개 요청 후 17초 딜레이
    def delay(self, cnt=0):
        if cnt%5 == 0:
            print("조회제한 회피...17초 대기중...")
            time.sleep(17)
        elif cnt%999 == 0:
            print("조회제한 회피...300초 대기중...")
            time.sleep(300)


    def convert_seconds_to_time(self, second_time): 
        """
        초를 입력받아 n days, nn:nn:nn으로 변환
        """
        return str(timedelta(seconds=second_time))


if __name__ == "__main__":
    stock = Stock()