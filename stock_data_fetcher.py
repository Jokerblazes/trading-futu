# batch_job.py

from futu import *
import psycopg2
from psycopg2.extras import Json
import datetime
import time

# PostgreSQL连接参数
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'mysecretpassword',
    'host': 'localhost',
    'port': '5432'
}

class StockDataFetcher:
    def __init__(self, host='127.0.0.1', port=11111):
        self.quote_ctx = OpenQuoteContext(host=host, port=port)

    def get_index_constituents(self, index_code):
        ret, data = self.quote_ctx.get_plate_stock(index_code)
        if ret == RET_OK:
            return data['code'].tolist()
        else:
            print(f'获取{index_code}成份股失败:', data)
            return []

    def get_historical_kline(self, stock_code, start_date, end_date, ktype=KLType.K_DAY):
        try:
            ret_code, data, page_req_key = self.quote_ctx.request_history_kline(
                code=stock_code, 
                start=start_date, 
                end=end_date, 
                ktype=ktype
            )
            
            if ret_code == RET_OK:
                all_data = data
                while page_req_key is not None:
                    # 添加0.5秒的延迟
                    time.sleep(0.5)
                    
                    ret_code, next_data, page_req_key = self.quote_ctx.request_history_kline(
                        code=stock_code, 
                        start=start_date, 
                        end=end_date, 
                        ktype=ktype,
                        page_req_key=page_req_key
                    )
                    if ret_code == RET_OK:
                        all_data = all_data.append(next_data)
                    else:
                        print(f'获取{stock_code}的额外K线数据失败:', next_data)
                        break

                return all_data.to_dict(orient='records')
            else:
                print(f'获取{stock_code}的K线数据失败:', data)
                return None
        except Exception as e:
            print(f'获取{stock_code}的K线数据时发生错误:', str(e))
            return None

    def get_klines_for_index(self, index_code, start_date, end_date, ktype=KLType.K_DAY):
        all_data = {'index': self.get_historical_kline(index_code, start_date, end_date, ktype)}
        constituents = self.get_index_constituents(index_code)
        all_data['constituents'] = {}
        for stock in constituents:
            print(f'正在获取{stock}的K线数据...')
            # 每次获取股票数据后添加0.5秒的延迟
            time.sleep(0.5)
            kline_data = self.get_historical_kline(stock, start_date, end_date, ktype)
            if kline_data is not None:
                all_data['constituents'][stock] = kline_data
        return all_data

    def close(self):
        self.quote_ctx.close()

def init_db():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS kline_data
                   (index_code TEXT, stock_code TEXT, date DATE, data JSONB,
                    PRIMARY KEY (index_code, stock_code, date))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS moving_average_data
                   (index_code TEXT, stock_code TEXT, date DATE,
                    ma_5 NUMERIC, ma_10 NUMERIC, ma_20 NUMERIC, ma_50 NUMERIC, ma_200 NUMERIC,
                    PRIMARY KEY (index_code, stock_code, date))''')
    conn.commit()
    cur.close()
    conn.close()

def get_latest_date(index_code):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) FROM kline_data WHERE index_code = %s", (index_code,))
    latest_date = cur.fetchone()[0]
    cur.close()
    conn.close()
    return latest_date

def save_to_db(index_code, kline_data):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    for stock_code, data in kline_data['constituents'].items():
        for record in data:
            cur.execute("INSERT INTO kline_data VALUES (%s, %s, %s, %s) ON CONFLICT (index_code, stock_code, date) DO UPDATE SET data = EXCLUDED.data",
                        (index_code, stock_code, record['time_key'], Json(record)))
    for record in kline_data['index']:
        cur.execute("INSERT INTO kline_data VALUES (%s, %s, %s, %s) ON CONFLICT (index_code, stock_code, date) DO UPDATE SET data = EXCLUDED.data",
                    (index_code, index_code, record['time_key'], Json(record)))
    conn.commit()   
    cur.close()
    conn.close()

def calculate_moving_average(data, period):
    moving_average = []
    for i in range(len(data)):
        if i < period - 1:
            moving_average.append({'date': data[i]['time_key'], 'value': None})
            continue
        sum_values = sum(item['close'] for item in data[i - period + 1:i + 1])
        moving_average.append({'date': data[i]['time_key'], 'value': sum_values / period})
    return moving_average

def calculate_and_save_moving_averages(index_code, stock_code, data):
    periods = [5, 10, 20, 50, 200]
    moving_averages = {f'ma_{p}': calculate_moving_average(data, p) for p in periods}

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    for i, record in enumerate(data):
        date = record['time_key']
        ma_values = {f'ma_{p}': moving_averages[f'ma_{p}'][i]['value'] for p in periods}
        cur.execute('''INSERT INTO moving_average_data (index_code, stock_code, date, ma_5, ma_10, ma_20, ma_50, ma_200)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (index_code, stock_code, date) DO UPDATE
                       SET ma_5 = EXCLUDED.ma_5, ma_10 = EXCLUDED.ma_10, ma_20 = EXCLUDED.ma_20,
                           ma_50 = EXCLUDED.ma_50, ma_200 = EXCLUDED.ma_200''',
                    (index_code, stock_code, date, ma_values['ma_5'], ma_values['ma_10'],
                     ma_values['ma_20'], ma_values['ma_50'], ma_values['ma_200']))
    conn.commit()
    cur.close()
    conn.close()

def run_batch_job():
    fetcher = StockDataFetcher()
    index_codes = ['HK.800000', 'HK.800700']  # 添加您需要的指数代码
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    for index_code in index_codes:
        print(f"正在获取 {index_code} 的数据...")
        latest_date = get_latest_date(index_code)
        if latest_date:
            start_date = (latest_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date = '2024-01-01'  # 如果没有数据，使用默认的开始日期

        kline_data = fetcher.get_klines_for_index(index_code, start_date, end_date)
        save_to_db(index_code, kline_data)
        print(f"{index_code} 数据已保存到数据库")

        # 计算并保存移动平均值
        for stock_code, data in kline_data['constituents'].items():
            calculate_and_save_moving_averages(index_code, stock_code, data)

    fetcher.close()

if __name__ == '__main__':
    init_db()
    while True:
        run_batch_job()
        print("批处理任务完成，等待下一次运行...")
        time.sleep(86400)  # 等待24小时
