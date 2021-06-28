#!/usr/bin/env python
# coding: utf-8

import psycopg2
import FinanceDataReader as fdr
import pandas as pd 
import numpy as np
import time 
import os
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine, types

# 딥러닝을 위한 라이브러리 keras 
import tensorflow as tf
from tensorflow.keras.optimizers import Adam
from tensorflow.python.keras.models import load_model
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.layers import LSTM, Dropout, Dense, Activation, Embedding, Input, concatenate, BatchNormalization, Flatten
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
from tensorflow.keras.utils import plot_model

random.seed(777)
os.environ['PYTHONHASHSEED'] = str(0)
np.random.seed(777)
tf.random.set_seed(777)

# conn = psycopg2.connect(host='localhost', dbname='hadoop', user='hadoop', password='1234', port='5431')
conn = psycopg2.connect("host='localhost' port='5432' user='postgres' password='rootpass'")
cursor = conn.cursor()
engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=rootpass")

def query(query):
    cursor.execute(query)
    col_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns = col_names)
    return result

def multi_data(target, idx, start_idx , end_idx, history_size, target_size):
    data = []
    labels = []
    input_idx = []
    target_idx = []
    
    start_idx = start_idx + history_size
    if end_idx is None:
        end_idx = len(target) - target_size
        
    for i in range(start_idx, end_idx):
        indices = range(i-history_size, i)
        data.append(target[indices])
        input_idx.append(idx[indices])
        
        labels.append(target[i:i+target_size])
        target_idx.append(idx[i:i+target_size])
    
    return np.array(data), np.array(labels), input_idx, target_idx

def main(trcode):

    # trunc_query = "TRUNCATE TABLE {}".format(trcode)
    # cursor.execute(trunc_query)
    # conn.commit()
    # print("테이블 비우기")

    total_volume = query('select * from opt10001')

    need_tot_vol = total_volume.sort_values(by='시가총액', ascending= False)

    need_com = {}
    for item1, item2 in zip(need_tot_vol['종목코드'].values, need_tot_vol['종목명']):
        need_com[item1] = item2

    rev_need_com = {v:k for k,v in need_com.items()}

    df = pd.DataFrame()
    today = datetime.now().strftime("%Y-%m-%d")
    total_cnt = len(rev_need_com)
    cnt = 0
    for need in rev_need_com:
        try:
            cnt += 1
            dup_query = "SELECT * FROM stock_prediction WHERE stock_code = %s AND batch_date = %s"
            dup_binds = [rev_need_com[need], today]
            cursor.execute(dup_query, dup_binds)
            result = cursor.fetchall()
            # 적재 데이터 없을 때만 수집
            if (len(result) == 0):
                code = rev_need_com[need]
                temp = fdr.DataReader(code, '2010-01-01', today)
                temp['Date'] = temp.index
                temp['Code'] = code
                temp['Korean'] = need_com[code]
                df = pd.concat([df, temp])

                target = df[df['Korean'] == need]
                convert = target[['Open', 'High', 'Low', 'Close', 'Volume']]
                convert = np.log(convert+0.01) - np.log((convert+0.01).shift(1))
                convert = convert.dropna()

                _min = convert.min()
                _max = convert.max()
                convert = (convert-_min)/(_max - _min)
                date = convert.index
                target_values = convert['Close'].values

                split = int(len(target)*0.9)
                history = 50
                target_size = 10

                x_train, y_train, x_train_idx, y_train_idx = multi_data(target_values, date, 0, split, history, target_size)
                x_test, y_test, x_test_idx, y_test_idx = multi_data(target_values, date, split, None, history, target_size)

                x_train = x_train.reshape(x_train.shape[-2], x_train.shape[-1], 1)
                x_test = x_test.reshape(x_test.shape[-2], x_test.shape[-1], 1)

                embed_train = np.array(target[['Open', 'High', 'Low', 'Close', 'Volume']].loc[np.array(x_train_idx)[:, -1]].values)/100
                embed_test = np.array(target[['Open', 'High', 'Low', 'Close', 'Volume']].loc[np.array(x_test_idx)[:, -1]].values)/100

                embed_train = embed_train.reshape(embed_train.shape[-2], embed_train.shape[-1],1)/100
                embed_test = embed_test.reshape(embed_test.shape[-2], embed_test.shape[-1],1)/100

                lstm_input = Input(shape=x_train.shape[-2:])
                lstm_out = LSTM(50)(lstm_input)
                lstm_out = Dropout(0.3)(lstm_out)
                lstm_model = Model(inputs=lstm_input, outputs=lstm_out)

                embed_input = Input(shape=embed_train.shape[-2:])
                embed_out = Embedding(1000000, 1, input_length=5)(embed_input)
                embed_out = Flatten()(embed_out)
                embed_out = Dense(50, activation='relu')(embed_out)

                embed_model = Model(inputs=embed_input, outputs=embed_out)

                concatenated = concatenate([lstm_model.output, embed_model.output])
                concat_out = Dense(10, activation='linear')(concatenated)

                concat_model = Model(inputs = [lstm_model.input, embed_model.input], outputs = concat_out)

                concat_model.compile(loss='mse', optimizer=Adam(0.01)) 

                early_stop = EarlyStopping(monitor='val_loss', patience=10)
                model_path = 'D:/Competition2021/analysis/model/'
                filename = os.path.join(model_path, need+'_checkpoint.h5')
                checkpoint = ModelCheckpoint(filename, monitor='val_loss', verbose=1, save_best_only=True, mode='auto')

                concat_model.fit(x = [x_train, embed_train], y = y_train, epochs = 100, batch_size=10, validation_data=([x_test, embed_test], y_test), callbacks=[early_stop, checkpoint])

                concat_model = load_model(filename)

                pred = pd.DataFrame(concat_model.predict([convert['Close'].iloc[-50:].values.reshape(1,50,1), convert.iloc[-1].values.reshape(1,5,1)])).T
                pred = pred * (_max['Close'] - _min['Close']) + (_min['Close'])

                convert_pred = []
                for item in pred.values:
                    if len(convert_pred) == 0:
                        last = target['Close'].loc[convert.iloc[-1].name]
                    else:
                        last = convert_pred[-1]
                    convert_pred.append(int(np.exp(item + np.log(last+0.01))-0.01))

                trend = ''
                if (convert_pred[-1] - convert_pred[0])/convert_pred[0] > 0.01:
                    trend = '증가'
                elif (convert_pred[-1] - convert_pred[0])/convert_pred[0] < -0.01:
                    trend = '감소'
                else:
                    trend = '유지'

                st_date = max(convert.index)

                pred_section = []
                for i in range(1, 20):
                    next_date = st_date + timedelta(days=i)
                    if next_date.weekday() < 5:
                        pred_section.append(next_date)
                    if len(pred_section) == 10:
                        break

                pred_df = pd.DataFrame(convert_pred, index = pred_section, columns =['예측값'])
                pred_df.index.name = '예측구간'
                pred_df = pred_df.reset_index()

                pred_df['종목코드'] = rev_need_com[need]
                pred_df['종목명'] = need
                pred_df['기준일자'] = st_date
                pred_df['추세'] = trend

                pred_df = pred_df[['종목코드', '종목명', '기준일자', '예측구간', '예측값', '추세']]
                pred_df.columns = ['stock_code', 'stock_name', 'batch_date', 'predict_date', 'predict_value', 'trend']

                pred_df.to_sql(trcode, engine, if_exists='append', index=False, chunksize=1000)
                print("{}/{} {}-{} 수집/적재 완료".format(cnt, total_cnt, trcode, need))
            else:
                print("{}/{} {}-{} 건너뛰기".format(cnt, total_cnt, trcode, need))
        except Exception as ex:
            print("{}/{} {}-{} 수집/적재 실패".format(cnt, total_cnt, trcode, need))
            print(ex)

        
if __name__ == "__main__":
    print("{} 배치 시작".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    start = time.time()  # 시작시간 저장
    main("stock_prediction")
    elapsed_time = str(timedelta(seconds=(time.time() - start)))
    print("실행시간 :", elapsed_time)  # 현재시각 - 시작시간 = 실행 시간
    print("{} 배치 종료".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))