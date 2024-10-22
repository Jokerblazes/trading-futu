from flask import Flask, request, jsonify
from stock_data_fetcher import StockDataReader
from datetime import datetime, timedelta
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
reader = StockDataReader()

@app.route('/api/kline', methods=['GET'])
def get_kline_data():
    stock_code = request.args.get('stock', 'HK.800000')
    start_date = request.args.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    kline_data = reader.get_from_cache(stock_code, start_date, end_date)
    if kline_data:
        return jsonify(kline_data)
    else:
        return jsonify({"error": "没有找到数据"}), 404

if __name__ == '__main__':
    app.run(debug=True)
