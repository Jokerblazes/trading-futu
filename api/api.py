from flask import Flask, request, jsonify
from service.index_kline_service import IndexKlineService
from datetime import datetime, timedelta
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)

index_kline_service = IndexKlineService()

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
        kline_data = index_kline_service.get_from_db(index_code, start_date, end_date)
        return jsonify(kline_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)