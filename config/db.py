from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os

# Read credentials from environment (safer than hardcoding)
DB_USER = os.getenv('OLIST_DB_USER', 'root')
DB_PASS = os.getenv('OLIST_DB_PASS')
DB_HOST = os.getenv('OLIST_DB_HOST', 'localhost')
DB_PORT = int(os.getenv('OLIST_DB_PORT', 3306))
DB_NAME = os.getenv('OLIST_DB_NAME', 'olist')

# Example: mysql+pymysql://user:pass@host:port/dbname


def get_connection_string():
    # quote password in case it has special chars
    p = quote_plus(DB_PASS)
    return f"mysql+pymysql://{DB_USER}:{p}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"


def get_engine(echo=False):
    from sqlalchemy import create_engine
    conn = get_connection_string()
    engine = create_engine(conn, pool_pre_ping=True, echo=echo)
    return engine
