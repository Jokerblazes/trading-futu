import psycopg2

class EmailService:
    def __init__(self, database_url):
        self.database_url = database_url

    def save_email(self, email):
        conn = psycopg2.connect(self.database_url)
        cur = conn.cursor()
        try:
            # 检查电子邮件是否已经存在
            cur.execute("SELECT 1 FROM emails WHERE email = %s", (email,))
            if cur.fetchone() is not None:
                # 如果电子邮件已经存在，直接返回
                return
            
            # 如果电子邮件不存在，插入新记录
            cur.execute("INSERT INTO emails (email) VALUES (%s)", (email,))
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close() 