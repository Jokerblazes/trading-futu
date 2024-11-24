import os
import psycopg2
from psycopg2.extras import RealDictCursor

DEFAULT_DB_URL = "postgresql://postgres:mysecretpassword@localhost:5432/postgres"

class IndexKlineService:
    def __init__(self, database_url):
        self.database_url = database_url

    def get_from_db(self, index_code, start_date, end_date):
        conn = psycopg2.connect(self.database_url)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        result = {'index': [], 'breadth': [], 'net_high_low': []}
        
        cur.execute("SELECT data FROM kline_data WHERE index_code = %s AND stock_code = %s AND date BETWEEN %s AND %s ORDER BY date",
                    (index_code, index_code, start_date, end_date))
        kline_data = [row['data'] for row in cur.fetchall()]

        cur.execute('''SELECT date, ma_5, ma_10, ma_20, ma_50, ma_200 
                       FROM moving_average_data 
                       WHERE index_code = %s AND stock_code = %s AND date BETWEEN %s AND %s ORDER BY date''',
                    (index_code, index_code, start_date, end_date))
        ma_data = cur.fetchall()

        combined_data = []
        ma_dict = {row['date'].strftime('%Y-%m-%d'): row for row in ma_data}
        for record in kline_data:
            date = record['time_key'].split(' ')[0]
            record['time_key'] = date
            if date in ma_dict:
                record['ma'] = {
                    'ma_5': ma_dict[date]['ma_5'],
                    'ma_10': ma_dict[date]['ma_10'],
                    'ma_20': ma_dict[date]['ma_20'],
                    'ma_50': ma_dict[date]['ma_50'],
                    'ma_200': ma_dict[date]['ma_200']
                }
            combined_data.append(record)

        result['index'] = combined_data

        cur.execute('''SELECT date, breadth_value 
                       FROM breadth_data 
                       WHERE index_code = %s AND date BETWEEN %s AND %s ORDER BY date''',
                    (index_code, start_date, end_date))
        breadth_data = cur.fetchall()
        result['breadth'] = [{'date': row['date'].strftime('%Y-%m-%d'), 'value': row['breadth_value']} for row in breadth_data]

        cur.execute('''SELECT date, net_high_low_value 
                       FROM net_high_low_data 
                       WHERE index_code = %s AND date BETWEEN %s AND %s ORDER BY date''',
                    (index_code, start_date, end_date))
        net_high_low_data = cur.fetchall()
        result['net_high_low'] = [{'date': row['date'].strftime('%Y-%m-%d'), 'value': row['net_high_low_value']} for row in net_high_low_data]
        
        cur.close()
        conn.close()
        return result 