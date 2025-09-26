import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

class PostgresConnect:
    def __init__(self):
        self.host = os.getenv('SPOTIFY_DB_HOST', 'localhost')
        self.port = os.getenv('SPOTIFY_DB_PORT', '5432')
        self.dbname = os.getenv('SPOTIFY_DB', 'spotify')
        self.pg_user = os.getenv('SPOTIFY_DB_USER', 'analyst')
        self.pg_password = os.getenv('SPOTIFY_DB_PASSWORD', 'analyst_password')

    def connector(self):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.pg_user,
                password=self.pg_password
            )
            return conn
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            raise

if __name__ == "__main__":
    try:
        pg_conn = PostgresConnect()
        conn = pg_conn.connector()
        print("Successfully connected to database")
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")