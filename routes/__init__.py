"""
Routes package for CDC Analytics Dashboard
Modular Flask Blueprint structure
"""

import os
import oracledb
from pathlib import Path
from dotenv import load_dotenv

# Load .env from parent directory
ENV_PATH = Path(__file__).parent.parent / '.env'
load_dotenv(ENV_PATH)

# Oracle Configuration (shared across all routes)
ORACLE_CONFIG = {
    'user': os.getenv('ORACLE_USER'),
    'password': os.getenv('ORACLE_PASSWORD'),
    'host': os.getenv('ORACLE_HOST'),
    'port': int(os.getenv('ORACLE_PORT', 1521)),
    'service_name': os.getenv('ORACLE_SERVICE_NAME', 'XE'),
}


def get_connection():
    """Get Oracle database connection"""
    dsn = oracledb.makedsn(
        ORACLE_CONFIG['host'],
        ORACLE_CONFIG['port'],
        service_name=ORACLE_CONFIG['service_name']
    )
    return oracledb.connect(
        user=ORACLE_CONFIG['user'],
        password=ORACLE_CONFIG['password'],
        dsn=dsn
    )


def execute_query(query, params=None):
    """Execute query and return results as list of dicts"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params or {})
        columns = [col[0].lower() for col in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"Query error: {e}")
        return []
    finally:
        if conn:
            conn.close()


def execute_single(query, params=None):
    """Execute query and return single row as dict"""
    results = execute_query(query, params)
    return results[0] if results else {}
