from flask import Flask, request, jsonify
from service.index_kline_service import IndexKlineService
from service.email_service import EmailService
from service.stock_service import StockService
from datetime import datetime, timedelta
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)
DEFAULT_DB_URL = "postgresql://postgres:mysecretpassword@localhost:5432/postgres"

index_kline_service = IndexKlineService(os.environ.get('POSTGRES_URL', DEFAULT_DB_URL))
email_service = EmailService(os.environ.get('POSTGRES_URL', DEFAULT_DB_URL))
stock_service = StockService(os.environ.get('POSTGRES_URL', DEFAULT_DB_URL))
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

@app.route('/api/email', methods=['POST'])
def save_email():
    data = request.get_json()
    email = data.get('email')
    
    if not email:
        return jsonify({"error": "Email is required"}), 400
    
    try:
        email_service.save_email(email)
        return jsonify({"message": "Email saved successfully"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/api/above_ma50', methods=['GET'])
def get_stocks_above_ma50():
    index_code = request.args.get('index', 'HK.800000')
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    
    try:
        # 验证日期格式
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD"}), 400

    try:
        stocks = stock_service.get_stocks_above_ma50(index_code, date)
        return jsonify({
            "index_code": index_code,
            "date": date,
            "stocks": stocks,
            "total_count": len(stocks)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
if __name__ == '__main__':
    app.run(debug=True)