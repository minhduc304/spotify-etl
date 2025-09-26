import os
import psycopg2
import pandas as pd
from typing import Optional
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables
load_dotenv()

class DatabaseConnection:
    """Handles database connections and queries for the Spotify dashboard"""
    
    def __init__(self):
        self.connection_params = {
            'host': os.getenv('SPOTIFY_DB_HOST', 'localhost'),
            'port': int(os.getenv('SPOTIFY_DB_PORT', 5432)),
            'database': os.getenv('SPOTIFY_DB', 'spotify'),
            'user': os.getenv('SPOTIFY_DB_USER', 'analyst'),
            'password': os.getenv('SPOTIFY_DB_PASSWORD', 'analyst_password')
        }
        self._connection = None
        self._engine = None
    
    def get_connection(self):
        """Get or create database connection"""
        if self._connection is None or self._connection.closed:
            try:
                self._connection = psycopg2.connect(**self.connection_params)
                self._connection.autocommit = True
            except Exception as e:
                st.error(f"Database connection failed: {str(e)}")
                raise
        return self._connection
    
    def get_engine(self):
        """Get or create SQLAlchemy engine"""
        if self._engine is None:
            try:
                db_url = f"postgresql://{self.connection_params['user']}:{self.connection_params['password']}@{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"
                self._engine = create_engine(db_url)
            except Exception as e:
                st.error(f"Engine creation failed: {str(e)}")
                raise
        return self._engine
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame"""
        try:
            engine = self.get_engine()
            df = pd.read_sql_query(query, engine, params=params)
            return df
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            return pd.DataFrame()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return True
        except:
            return False
    
    def close(self):
        """Close database connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()