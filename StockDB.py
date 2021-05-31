from pykiwoom.kiwoom import *

import psycopg2
import pandas as pd

exitcode = 0

class StockDB(Kiwoom):
    def __init__(self, trcode="opt"):
        self.login(trcode)
        self.connect_DB()


    # 키움증권 OPEN API 로그인
    def login(self, trcode="opt"):
        if trcode.startswith("opt"):
            self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
            self.connected = False              # for login event
            self.received = False               # for tr event
            self.tr_items = None                # tr input/output items
            self.tr_data = None                 # tr output data
            self.tr_record = None
            self.tr_remained = False
            self.condition_loaded = False
            self._set_signals_slots()
            
            self.CommConnect(block=True)
            print("로그인 완료")

            return self.connected
        else:
            return None


    # DB 연결한다.
    def connect_DB(self):
        try:
            self.conn = psycopg2.connect("host='localhost' port='5432' user='postgres' password='rootpass'")
            self.cursor = self.conn.cursor()
            print("DB 연결 성공")
        except Exception as ex:
            print("DB 연결 실패")
            print(ex)


    # 쿼리 실행하여 [데이터, 실행결과코드] 배열로 리턴
    def execute(self, query, binds=None):
        global exitcode
        exitcode = 0
        result = ["", exitcode]

        try:
            self.cursor.execute(query, binds)
            if query.startswith("SELECT"):
                data = self.cursor.fetchall()
                result = [data, exitcode]
            else:
                self.conn.commit()
            return result
        except Exception as ex:
            print(ex)
            exitcode = 1
            result = ["", exitcode]
            return result


    # 테이블 생성
    def create_table(self):
        query = open("./sql/schema.sql", encoding='UTF8').read()
        exitcode1 = self.execute(query)[1]
        exitcode2 = self.insert_company()  # 전체 종목 DB 저장
        if (exitcode1 == 0 and exitcode2 == 0):
            print("테이블 생성 완료")
        else:
            print("테이블 생성 실패")


    # 테이블의 컬럼 개수
    def column_cnt(self, table):
        query = "SELECT COUNT(COLUMN_NAME) FROM information_schema.columns WHERE TABLE_NAME = %s"
        binds = [table]
        cnt = self.execute(query, binds)[0][0][0]

        return cnt


    # 테이블을 파라미터로 받아서 insert 쿼리문을 만들어 실행한다.
    def insert(self, table, binds):
        global exitcode
        exitcode = 0
        query = "INSERT INTO " + table + " VALUES "
        
        # 테이블의 컬럼 개수만큼 value 안에 들어갈 값 생성
        cnt = self.column_cnt(table)
        value = "("
        i = 0
        while(i<cnt):
            if (i == cnt-1):
                value = value + "%s)"
            else:
                value = value + "%s, "
            i = i + 1
        query = query + value
        exitcode = self.execute(query, binds)[1]
        return exitcode

    def batch_result_update(self, table, pkey, status):
        query = "UPDATE batch_result SET status = '{}' WHERE table_nm = '{}' AND pkey = '{}';".format(status, table, pkey)
        self.execute(query)


    # 전체 종목 DB 저장
    def insert_company(self):
        global exitcode
        exitcode = 0
        try:
            exitcode = self.truncate_table("stock")
            if (exitcode == 0):
                kospi = self.GetCodeListByMarket('0')
                kosdaq = self.GetCodeListByMarket('10')
                codes = kospi + kosdaq
                for code in codes:
                    name = self.GetMasterCodeName(code)
                    binds = [code, name]
                    self.insert("stock", binds)
        except Exception as ex:
            print(ex)
            exitcode = 1
        return exitcode
    
    # 테이블 비우기
    def truncate_table(self, table_nm):
        global exitcode
        exitcode = 0
        query = "TRUNCATE TABLE {}".format(table_nm)
        exitcode = self.execute(query)[1]
        return exitcode



if __name__ == "__main__":
    StockDB = StockDB()
    StockDB.cursor.close()
    StockDB.conn.close()