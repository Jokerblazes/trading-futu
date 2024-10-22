from flask import Flask, request, jsonify
from futu import *
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import time
import random
import psycopg2
import psycopg2.extras

app = Flask(__name__)

class StockDataFetcher:
    def __init__(self, host='localhost', database='postgres', user='postgres', password='mysecretpassword'):
        self.quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
        self.db_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password
        }
        self.init_db()

    def init_db(self):
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS kline_cache (
                        code TEXT,
                        name TEXT,
                        time_key TIMESTAMP,
                        open REAL,
                        close REAL,
                        high REAL,
                        low REAL,
                        pe_ratio REAL,
                        turnover_rate REAL,
                        volume BIGINT,
                        turnover REAL,
                        change_rate REAL,
                        last_close REAL,
                        PRIMARY KEY (code, time_key)
                    )
                ''')
                conn.commit()

    def get_historical_kline(self, stock_code, start_date, end_date, ktype='K_DAY', max_retries=3, retry_delay=5):
        for attempt in range(max_retries):
            ret_code, data, _ = self.quote_ctx.request_history_kline(stock_code, start=start_date, end=end_date, ktype=ktype)
            if ret_code == RET_OK:
                kline_data = data.to_dict(orient='records')
                self.save_to_cache(kline_data)
                print(f'将 {stock_code} 的K线数据保存到数据库')
                return kline_data
            else:
                print(f'获取{stock_code}的K线数据失败 (尝试 {attempt + 1}/{max_retries}): {data}')
                if attempt < max_retries - 1:
                    sleep_time = retry_delay + random.uniform(0, 2)
                    print(f'等待 {sleep_time:.2f} 秒后重试...')
                    time.sleep(sleep_time)
        print(f'获取{stock_code}的K线数据最终失败')
        return None

    def save_to_cache(self, data):
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, '''
                    INSERT INTO kline_cache 
                    (code, name, time_key, open, close, high, low, pe_ratio, turnover_rate, volume, turnover, change_rate, last_close)
                    VALUES (%(code)s, %(name)s, %(time_key)s, %(open)s, %(close)s, %(high)s, %(low)s, %(pe_ratio)s, %(turnover_rate)s, %(volume)s, %(turnover)s, %(change_rate)s, %(last_close)s)
                    ON CONFLICT (code, time_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    open = EXCLUDED.open,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    pe_ratio = EXCLUDED.pe_ratio,
                    turnover_rate = EXCLUDED.turnover_rate,
                    volume = EXCLUDED.volume,
                    turnover = EXCLUDED.turnover,
                    change_rate = EXCLUDED.change_rate,
                    last_close = EXCLUDED.last_close
                ''', data)

    def get_index_constituents(self, index_code):
        ret, data = self.quote_ctx.get_plate_stock(index_code)
        if ret == RET_OK:
            return data['code'].tolist()
        else:
            print(f'���取{index_code}成份股失败:', data)
            return []

    def close(self):
        self.quote_ctx.close()

class StockDataReader:
    def __init__(self, host='localhost', database='postgres', user='postgres', password='mysecretpassword'):
        self.db_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password
        }

    def get_from_cache(self, stock_code, start_date, end_date):
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute('''
                    SELECT * FROM kline_cache
                    WHERE code = %s AND time_key BETWEEN %s AND %s
                    ORDER BY time_key
                ''', (stock_code, start_date, end_date))
                rows = cur.fetchall()
                if rows:
                    return [dict(row) for row in rows]
        return None

def run_batch_job():
    fetcher = StockDataFetcher()
    index_code = 'HK.800000'  # 恒生指数
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    constituents = fetcher.get_index_constituents(index_code)
    fetcher.get_historical_kline(index_code, start_date, end_date)
    
    for stock in constituents:
        print(f'正在获取{stock}的K线数据...')
        fetcher.get_historical_kline(stock, start_date, end_date)
        time.sleep(0.5)  # 在每次请求之间添加0.5秒的延时
    
    fetcher.close()
    print("批处理任务完成")

if __name__ == '__main__':
    run_batch_job()
