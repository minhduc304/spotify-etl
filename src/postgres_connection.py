import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

class PostgresConnect:
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST')
        self.port = os.getenv('POSTGRES_PORT')
        self.dbname = os.getenv('POSTGRES_DBNAME')
        self.pg_user = os.getenv('POSTGRES_USER')
        self.pg_password = os.getenv('POSTGRES_PASSWORD')

    def connector(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.pg_user,
            password=self.pg_password
        )
        return conn

if __name__ == "__main__":
    conn = PostgresConnect()
    conn.connector()