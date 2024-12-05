from psycopg2.extras import RealDictCursor
import psycopg2

class StockService:
    def __init__(self, database_url):
        self.database_url = database_url

    def get_stocks_above_ma50(self, index_code, date):
        """
        获取指定日期在指定指数中高于MA50的股票列表
        """
        try:
            conn = psycopg2.connect(self.database_url)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute('''
                SELECT s.stock_code, k.data->>'name' as stock_name
                FROM stock_ma_status s
                JOIN kline_data k ON k.stock_code = s.stock_code 
                    AND k.index_code = s.index_code 
                    AND k.date = s.date
                WHERE s.index_code = %s 
                    AND s.date = %s 
                    AND s.above_ma_50 = true
                ORDER BY s.stock_code
            ''', (index_code, date))
            
            stocks = cur.fetchall()
            
            cur.close()
            conn.close()
            
            return stocks
        except Exception as e:
            raise Exception(f"Error getting stocks above MA50: {str(e)}") 