"""
CDC Analytics Dashboard
Flask-based dashboard for CONVERSATION_SUMMARY analytics
"""

import os
import oracledb
from flask import Flask, render_template, jsonify, request
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Oracle Configuration (reuse from config.py pattern)
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


# ==================
# Routes
# ==================

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/api/summary')
def api_summary():
    """Get overall summary statistics"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN SOURCE_TYPE='CALL' THEN 1 END) as calls,
            COUNT(CASE WHEN SOURCE_TYPE='WAPP' THEN 1 END) as whatsapp,
            ROUND(AVG(SATISFACTION), 2) as avg_satisfaction,
            ROUND(AVG(CHURN_SCORE), 2) as avg_churn_score,
            COUNT(CASE WHEN LOWER(SENTIMENT) LIKE '%חיובי%' OR LOWER(SENTIMENT) LIKE '%positive%' THEN 1 END) as positive,
            COUNT(CASE WHEN LOWER(SENTIMENT) LIKE '%שלילי%' OR LOWER(SENTIMENT) LIKE '%negative%' THEN 1 END) as negative,
            COUNT(CASE WHEN LOWER(SENTIMENT) LIKE '%נייטרלי%' OR LOWER(SENTIMENT) LIKE '%neutral%' THEN 1 END) as neutral
        FROM CONVERSATION_SUMMARY
        WHERE CREATION_DATE > SYSDATE - :days
    """

    result = execute_single(query, {'days': days})
    return jsonify(result)


@app.route('/api/categories')
def api_categories():
    """Get category distribution"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT CATEGORY_CODE as category, COUNT(*) as count
        FROM CONVERSATION_CATEGORY
        WHERE CREATION_DATE > SYSDATE - :days
        GROUP BY CATEGORY_CODE
        ORDER BY count DESC
        FETCH FIRST 15 ROWS ONLY
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/sentiment')
def api_sentiment():
    """Get sentiment breakdown"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT
            CASE
                WHEN LOWER(SENTIMENT) LIKE '%חיובי%' OR LOWER(SENTIMENT) LIKE '%positive%' THEN 'Positive'
                WHEN LOWER(SENTIMENT) LIKE '%שלילי%' OR LOWER(SENTIMENT) LIKE '%negative%' THEN 'Negative'
                ELSE 'Neutral'
            END as sentiment,
            COUNT(*) as count
        FROM CONVERSATION_SUMMARY
        WHERE CREATION_DATE > SYSDATE - :days
        GROUP BY CASE
            WHEN LOWER(SENTIMENT) LIKE '%חיובי%' OR LOWER(SENTIMENT) LIKE '%positive%' THEN 'Positive'
            WHEN LOWER(SENTIMENT) LIKE '%שלילי%' OR LOWER(SENTIMENT) LIKE '%negative%' THEN 'Negative'
            ELSE 'Neutral'
        END
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/churn')
def api_churn():
    """Get churn risk distribution"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT
            CASE
                WHEN CHURN_SCORE >= 70 THEN 'High Risk (70+)'
                WHEN CHURN_SCORE >= 40 THEN 'Medium Risk (40-69)'
                ELSE 'Low Risk (0-39)'
            END as risk_level,
            COUNT(*) as count
        FROM CONVERSATION_SUMMARY
        WHERE CREATION_DATE > SYSDATE - :days
        GROUP BY CASE
            WHEN CHURN_SCORE >= 70 THEN 'High Risk (70+)'
            WHEN CHURN_SCORE >= 40 THEN 'Medium Risk (40-69)'
            ELSE 'Low Risk (0-39)'
        END
        ORDER BY risk_level
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/satisfaction')
def api_satisfaction():
    """Get satisfaction distribution (1-5)"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT SATISFACTION as rating, COUNT(*) as count
        FROM CONVERSATION_SUMMARY
        WHERE CREATION_DATE > SYSDATE - :days
        AND SATISFACTION IS NOT NULL
        GROUP BY SATISFACTION
        ORDER BY SATISFACTION
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/errors')
def api_errors():
    """Get recent CDC errors"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT
            ERROR_TYPE as error_type,
            SUBSTR(ERROR_MESSAGE, 1, 200) as error_message,
            CALL_ID as call_id,
            TO_CHAR(ERROR_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') as timestamp
        FROM ERROR_LOG
        WHERE ERROR_TIMESTAMP > SYSDATE - :days
        ORDER BY ERROR_TIMESTAMP DESC
        FETCH FIRST 100 ROWS ONLY
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/recent')
def api_recent():
    """Get recent conversations"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT
            SOURCE_ID as id,
            SOURCE_TYPE as type,
            TO_CHAR(CREATION_DATE, 'YYYY-MM-DD HH24:MI') as created,
            SUBSTR(SUMMARY, 1, 150) as summary,
            SENTIMENT as sentiment,
            SATISFACTION as satisfaction,
            ROUND(CHURN_SCORE, 1) as churn_score
        FROM CONVERSATION_SUMMARY
        WHERE CREATION_DATE > SYSDATE - :days
        ORDER BY CREATION_DATE DESC
        FETCH FIRST 50 ROWS ONLY
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/daily')
def api_daily():
    """Get daily conversation counts for trend"""
    days = request.args.get('days', 30, type=int)

    query = """
        SELECT
            TO_CHAR(TRUNC(CREATION_DATE), 'YYYY-MM-DD') as date,
            COUNT(*) as count,
            ROUND(AVG(SATISFACTION), 2) as avg_satisfaction,
            ROUND(AVG(CHURN_SCORE), 2) as avg_churn
        FROM CONVERSATION_SUMMARY
        WHERE CREATION_DATE > SYSDATE - :days
        GROUP BY TRUNC(CREATION_DATE)
        ORDER BY TRUNC(CREATION_DATE)
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/health')
def api_health():
    """Health check endpoint"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT SYSDATE FROM dual")
        db_time = cursor.fetchone()[0]
        conn.close()
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'db_time': str(db_time),
            'server_time': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e)
        }), 500


if __name__ == '__main__':
    print("=" * 50)
    print("CDC Analytics Dashboard")
    print("=" * 50)
    print(f"Oracle Host: {ORACLE_CONFIG['host']}")
    print(f"Oracle Port: {ORACLE_CONFIG['port']}")
    print(f"Oracle Service: {ORACLE_CONFIG['service_name']}")
    print("=" * 50)
    print("Starting dashboard on http://localhost:5001")
    print("=" * 50)

    app.run(host='0.0.0.0', port=5001, debug=True)
