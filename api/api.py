# api_service.py

from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)
DEFAULT_DB_URL = "postgresql://postgres:mysecretpassword@localhost:5432/postgres"


def get_from_db(index_code, start_date, end_date):
    database_url = os.environ.get('POSTGRES_URL', DEFAULT_DB_URL)
    conn = psycopg2.connect(database_url)
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

@app.route('/api/index_kline', methods=['GET'])
def get_index_kline_data():
    index_code = request.args.get('index', 'HK.800000')
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD"}), 400

    try:
        kline_data = get_from_db(index_code, start_date, end_date)
        return jsonify(kline_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)