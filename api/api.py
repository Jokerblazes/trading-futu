# api_service.py

from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
# PostgreSQL连接参数
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'mysecretpassword',
    'host': 'localhost',
    'port': '5432'
}

def get_from_db(index_code, start_date, end_date):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("SELECT DISTINCT stock_code FROM kline_data WHERE index_code = %s", (index_code,))
    stock_codes = [row['stock_code'] for row in cur.fetchall()]
    
    result = {'index': [], 'constituents': {}}
    for stock_code in stock_codes:
        cur.execute("SELECT data FROM kline_data WHERE index_code = %s AND stock_code = %s AND date BETWEEN %s AND %s ORDER BY date",
                    (index_code, stock_code, start_date, end_date))
        data = [row['data'] for row in cur.fetchall()]
        if stock_code == index_code:
            result['index'] = data
        else:
            result['constituents'][stock_code] = data
    
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