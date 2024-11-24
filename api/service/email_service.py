import psycopg2

class EmailService:
    def __init__(self, database_url):
        self.database_url = database_url

    def save_email(self, email):
        conn = psycopg2.connect(self.database_url)
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO emails (email) VALUES (%s)", (email,))
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close() 