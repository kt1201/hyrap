from dateutil.parser import parse
from datetime import datetime, timedelta
import json
import os
import sys
import time
import urllib.request
from urllib.error import HTTPError

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import Stock

# API 정보
client_id = "y24f2QZnJc9s5UEEzy6R"
client_secret = "ke3yiKGjdH"

# 과거 12시간 안에 올라온 뉴스만 수집하기 위해 시간 지정
past_batch_start = datetime.now() - timedelta(hours=12)

# 데이터 수집, 재시도 5회코드 포함
def get_response(request):
    try:
        response = urllib.request.urlopen(request)
        return response
    except HTTPError as ex:
        for i in range(5):
            print("{}차 재시도 전 60초 대기중...".format(i+1))
            time.sleep(60)
            try:
                response = urllib.request.urlopen(request)
                print("{}차 재시도 성공...".format(i+1))
                return response
            except HTTPError as ex:
                print("{}차 재시도 실패...".format(i+1))
                if (i==4):
                    return None

def main(stock, trcode):
    global past_batch_start

    company_list = stock.get_list("stock")  # 전체 종목 list 생성
    total_cnt = len(company_list)
    cnt = 0
    for company in company_list:
        status = 0
        cnt += 1
        encText = urllib.parse.quote("\"{}\"+주가".format(company["name"]))
        url = "https://openapi.naver.com/v1/search/news?query={}&display={},".format(encText, 100) # json 결과
        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id",client_id)
        request.add_header("X-Naver-Client-Secret",client_secret)
        response = get_response(request) # api 응답을 가져오는데 실패시 재시도 5번
        if (response != None):
            rescode = response.getcode()
            if(rescode==200):
                response_body = json.loads(response.read())
                items = response_body["items"]
                # 데이터가 존재할 때
                if (len(items) > 0):
                    first_row_pubDate = datetime.strptime(str(parse(items[0]["pubDate"]).date()) + " " + str(parse(items[0]["pubDate"]).time()), "%Y-%m-%d %H:%M:%S")
                    if (first_row_pubDate > past_batch_start):
                        for item in items:
                            # 날짜 형식 custom
                            custom_pubDate = str(parse(item["pubDate"]).date()) + " " + str(parse(item["pubDate"]).time())
                            pubDate = datetime.strptime(custom_pubDate, "%Y-%m-%d %H:%M:%S")
                            # 과거 12시간 안의 뉴스들만 수집
                            if (pubDate > past_batch_start):
                                # 적재 여부 조회
                                dup_query = "SELECT * FROM news WHERE link = %s"
                                dup_binds = [item["originallink"]]
                                result = stock.StockDB.execute(dup_query, dup_binds)[0]
                                # 적재 데이터 없을 때만 수집
                                if (len(result) == 0):
                                    binds = [company["code"], item["title"], item["originallink"], custom_pubDate]
                                    exitcode = stock.StockDB.insert("news", binds)
                                    pkey = "{}-{}-{}".format(company["name"], custom_pubDate, item["originallink"])
                                    skip = stock.past_collect(trcode, pkey)
                                    if (exitcode == 0):
                                        print("{}/{} {}-{}-{}-{} 수집/적재 성공".format(cnt, total_cnt, trcode, company["name"], custom_pubDate, item["originallink"]))
                                        stock.set_batch_result(trcode, pkey, "SUCCESS", skip)
                                    else:
                                        print("{}/{} {}-{}-{}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, company["name"], custom_pubDate, item["originallink"]))
                                        stock.set_batch_result(trcode, pkey, "FAIL", 3)
                                        status = 1
                    else:
                        print("{}/{} {}-{} 건너뛰기".format(cnt, total_cnt, trcode, company["name"]))
                else:
                    print("{}/{} {}-{} 건너뛰기".format(cnt, total_cnt, trcode, company["name"]))
                if (status != 0):
                    print("{}/{} {}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, company["name"]))
                    pkey = "{}-{}".format(company["name"], str(datetime.now()))
                    stock.set_batch_result(trcode, pkey, "FAIL", 3)
            else:
                print("Error Code:" + rescode)
                print("{}/{} {}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, company["name"]))
                pkey = "{}-{}".format(company["name"], str(datetime.now()))
                stock.set_batch_result(trcode, pkey, "FAIL", 3)
        else:
            print("HTTPError: 504 Gateway Time-out")
            print("{}/{} {}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, company["name"]))
            pkey = "{}-{}".format(company["name"], str(datetime.now()))
            stock.set_batch_result(trcode, pkey, "FAIL", 3)

if __name__ == "__main__":
    print("{} 배치 시작".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    start = time.time()  # 시작시간 저장
    stock = Stock.Stock("company_fs")
    main(stock, "news")
    elapsed_time = stock.convert_seconds_to_time(time.time() - start)
    print("실행시간 :", elapsed_time)  # 현재시각 - 시작시간 = 실행 시간
    print("{} 배치 종료".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))