#!/usr/bin/env python3
"""Test script for database connection"""

import os
import sys
from dotenv import load_dotenv

# Add utils to path
sys.path.append('.')

# Load environment variables
load_dotenv()

try:
    from utils.database import DatabaseConnection
    
    print("🔄 Testing database connection...")
    
    # Test connection
    with DatabaseConnection() as db:
        if db.test_connection():
            print("Database connection successful!")
            
            # Test basic queries
            print("\n Testing basic queries...")
            
            # Test table counts
            queries = {
                "Artists": "SELECT COUNT(*) FROM artists",
                "Tracks": "SELECT COUNT(*) FROM tracks", 
                "Listening History": "SELECT COUNT(*) FROM user_listening_history",
                "dbt Models": "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('public_analytics', 'public_reporting')"
            }
            
            for name, query in queries.items():
                try:
                    result = db.execute_query(query)
                    count = result.iloc[0, 0] if not result.empty else 0
                    print(f"  {name}: {count:,} records")
                except Exception as e:
                    print(f"  {name}: Error - {str(e)}")
            
            print("\n🎵 Dashboard should work! Run with:")
            print("   streamlit run main.py")
            
        else:
            print(" Database connection failed")
            print("\n🔧 Troubleshooting:")
            print("1. Check if PostgreSQL is running: docker ps | grep postgres")
            print("2. Verify environment variables in .env")
            print("3. Test manually: psql -h localhost -p 5432 -U analyst -d spotify")

except ImportError as e:
    print(f" Import error: {e}")
    print("Make sure you've installed requirements: pip install -r requirements.txt")
    
except Exception as e:
    print(f" Unexpected error: {e}")
    import traceback
    traceback.print_exc()