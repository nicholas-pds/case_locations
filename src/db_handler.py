# src/db_handler.py
import os
import pandas as pd
import pyodbc # <--- NEW: Library for SQL Server
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_sql_server_credentials() -> dict:
    """Fetches SQL Server credentials from environment variables."""
    # Fetch all the necessary credentials defined in your .env
    return {
        "SERVER": os.getenv("SQL_SERVER"),
        "DATABASE": os.getenv("SQL_DATABASE"),
        "USERNAME": os.getenv("SQL_USERNAME"),
        "PASSWORD": os.getenv("SQL_PASSWORD"),
    }

def read_sql_query(file_path: str) -> str:
    """Reads a SQL query from a file."""
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        print(f"Error: SQL query file not found at {file_path}")
        return ""

def execute_sql_to_dataframe(sql_query_file: str) -> pd.DataFrame:
    """
    Connects to the SQL Server database, executes the SQL query, and returns a DataFrame.
    """
    
    query = read_sql_query(sql_query_file)
    if not query:
        return pd.DataFrame() 

    creds = get_sql_server_credentials()
    conn = None
    
    # 🌟 NEW: Define the ODBC Driver (You may need to adjust this)
    # Common driver names include 'ODBC Driver 17 for SQL Server' or 'SQL Server'
    driver = '{ODBC Driver 17 for SQL Server}' 
    
    conn_str = (
        f'DRIVER={driver};'
        f'SERVER={creds["SERVER"]};'
        f'DATABASE={creds["DATABASE"]};'
        f'UID={creds["USERNAME"]};'
        f'PWD={creds["PASSWORD"]}'
    )
    
    try:
        # 3. Establish connection using pyodbc
        print(f"Connecting to SQL Server: {creds['SERVER']}/{creds['DATABASE']}")
        conn = pyodbc.connect(conn_str)
        
        # 4. Use pandas to read SQL
        print("Executing query and fetching data...")
        # pd.read_sql is compatible with the pyodbc Connection object
        df = pd.read_sql(query, conn)
        
        print(f"Successfully loaded {len(df)} rows into DataFrame.")
        return df

    except pyodbc.Error as e:
        print(f"SQL Server (pyodbc) error occurred: {e}")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()
        
    finally:
        # 5. Close connection
        if conn:
            conn.close()
            print("Database connection closed.")