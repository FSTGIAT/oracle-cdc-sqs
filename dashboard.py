"""
CDC Analytics Dashboard
Flask-based dashboard for CONVERSATION_SUMMARY analytics
"""

import os
import json
import oracledb
from flask import Flask, render_template, jsonify, request
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env from the same directory as this file
ENV_PATH = Path(__file__).parent / '.env'
load_dotenv(ENV_PATH)

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


@app.route('/api/category/calls')
def api_category_calls():
    """Get calls for a specific category"""
    category = request.args.get('category', '')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 50, type=int)

    query = """
        SELECT
            cc.SOURCE_ID as call_id,
            cc.SOURCE_TYPE as type,
            TO_CHAR(cc.CREATION_DATE, 'YYYY-MM-DD HH24:MI') as created,
            cs.SUMMARY as summary,
            cs.SENTIMENT as sentiment,
            cs.SATISFACTION as satisfaction,
            ROUND(cs.CHURN_SCORE, 1) as churn_score,
            cs.PRODUCTS as products,
            cs.ACTION_ITEMS as action_items
        FROM CONVERSATION_CATEGORY cc
        LEFT JOIN CONVERSATION_SUMMARY cs ON cc.SOURCE_ID = cs.SOURCE_ID AND cc.SOURCE_TYPE = cs.SOURCE_TYPE
        WHERE cc.CATEGORY_CODE = :category
        AND cc.CREATION_DATE > SYSDATE - :days
        ORDER BY cc.CREATION_DATE DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'category': category, 'days': days, 'limit': limit})
    return jsonify(results)


@app.route('/api/sentiment/calls')
def api_sentiment_calls():
    """Get calls for a specific sentiment type"""
    sentiment_type = request.args.get('sentiment', '')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 50, type=int)

    # Map sentiment type to query condition
    if sentiment_type == 'Positive':
        sentiment_condition = "(LOWER(SENTIMENT) LIKE '%חיובי%' OR LOWER(SENTIMENT) LIKE '%positive%')"
    elif sentiment_type == 'Negative':
        sentiment_condition = "(LOWER(SENTIMENT) LIKE '%שלילי%' OR LOWER(SENTIMENT) LIKE '%negative%')"
    else:
        sentiment_condition = "(SENTIMENT IS NULL OR (LOWER(SENTIMENT) NOT LIKE '%חיובי%' AND LOWER(SENTIMENT) NOT LIKE '%positive%' AND LOWER(SENTIMENT) NOT LIKE '%שלילי%' AND LOWER(SENTIMENT) NOT LIKE '%negative%'))"

    query = f"""
        SELECT
            SOURCE_ID as call_id,
            SOURCE_TYPE as type,
            TO_CHAR(CREATION_DATE, 'YYYY-MM-DD HH24:MI') as created,
            SUMMARY as summary,
            SENTIMENT as sentiment,
            SATISFACTION as satisfaction,
            ROUND(CHURN_SCORE, 1) as churn_score,
            PRODUCTS as products,
            ACTION_ITEMS as action_items
        FROM CONVERSATION_SUMMARY
        WHERE {sentiment_condition}
        AND CREATION_DATE > SYSDATE - :days
        ORDER BY CREATION_DATE DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'days': days, 'limit': limit})
    return jsonify(results)


@app.route('/api/churn/calls')
def api_churn_calls():
    """Get calls for a specific churn risk level"""
    risk_level = request.args.get('risk_level', '')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 50, type=int)

    # Map risk level to score range
    if 'High' in risk_level or '70' in risk_level:
        score_condition = "CHURN_SCORE >= 70"
    elif 'Medium' in risk_level or '40' in risk_level:
        score_condition = "CHURN_SCORE >= 40 AND CHURN_SCORE < 70"
    else:
        score_condition = "CHURN_SCORE < 40 OR CHURN_SCORE IS NULL"

    query = f"""
        SELECT
            SOURCE_ID as call_id,
            SOURCE_TYPE as type,
            TO_CHAR(CREATION_DATE, 'YYYY-MM-DD HH24:MI') as created,
            SUMMARY as summary,
            SENTIMENT as sentiment,
            SATISFACTION as satisfaction,
            ROUND(CHURN_SCORE, 1) as churn_score,
            PRODUCTS as products,
            ACTION_ITEMS as action_items
        FROM CONVERSATION_SUMMARY
        WHERE ({score_condition})
        AND CREATION_DATE > SYSDATE - :days
        ORDER BY CHURN_SCORE DESC NULLS LAST, CREATION_DATE DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'days': days, 'limit': limit})
    return jsonify(results)


@app.route('/api/call/<call_id>')
def api_call_details(call_id):
    """Get call details from CONVERSATION_SUMMARY"""
    query = """
        SELECT
            SOURCE_ID as call_id,
            SOURCE_TYPE as type,
            TO_CHAR(CREATION_DATE, 'YYYY-MM-DD HH24:MI:SS') as created,
            SUMMARY as summary,
            SENTIMENT as sentiment,
            SATISFACTION as satisfaction,
            ROUND(CHURN_SCORE, 1) as churn_score,
            PRODUCTS as products,
            ACTION_ITEMS as action_items,
            UNRESOLVED_ISSUES as unresolved_issues,
            BAN as ban,
            SUBSCRIBER_NO as subscriber_no
        FROM CONVERSATION_SUMMARY
        WHERE SOURCE_ID = :call_id
    """

    result = execute_single(query, {'call_id': call_id})

    # Get categories for this call
    cat_query = """
        SELECT CATEGORY_CODE as category
        FROM CONVERSATION_CATEGORY
        WHERE SOURCE_ID = :call_id
    """
    categories = execute_query(cat_query, {'call_id': call_id})
    result['categories'] = [c['category'] for c in categories]

    # Get queue name from VERINT_TEXT_ANALYSIS
    queue_query = """
        SELECT QUEUE_NAME
        FROM VERINT_TEXT_ANALYSIS
        WHERE CALL_ID = :call_id
        FETCH FIRST 1 ROW ONLY
    """
    queue_result = execute_single(queue_query, {'call_id': call_id})
    result['queue_name'] = queue_result.get('queue_name') if queue_result else None

    # Get subscriber status from SUBSCRIBER table
    if result.get('subscriber_no') and result.get('ban'):
        status_query = """
            SELECT SUB_STATUS, PRODUCT_CODE
            FROM SUBSCRIBER
            WHERE SUBSCRIBER_NO = :subscriber_no
            AND CUSTOMER_BAN = :ban
        """
        status_result = execute_single(status_query, {
            'subscriber_no': result['subscriber_no'],
            'ban': result['ban']
        })
        if status_result:
            result['sub_status'] = status_result.get('sub_status')
            result['product_code'] = status_result.get('product_code')
        else:
            result['sub_status'] = None
            result['product_code'] = None
    else:
        result['sub_status'] = None
        result['product_code'] = None

    return jsonify(result)


@app.route('/api/call/<call_id>/conversation')
def api_call_conversation(call_id):
    """Get full conversation from VERINT_TEXT_ANALYSIS"""
    query = """
        SELECT
            CALL_ID as call_id,
            OWNER as speaker,
            TO_CHAR(CALL_TIME, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
            DBMS_LOB.SUBSTR(TEXT, 4000, 1) as text
        FROM VERINT_TEXT_ANALYSIS
        WHERE CALL_ID = :call_id
        ORDER BY CALL_TIME ASC
    """

    results = execute_query(query, {'call_id': call_id})

    # Format speaker labels
    for msg in results:
        if msg.get('speaker') == 'A':
            msg['speaker_label'] = 'Agent'
            msg['speaker_class'] = 'agent'
        elif msg.get('speaker') == 'C':
            msg['speaker_label'] = 'Customer'
            msg['speaker_class'] = 'customer'
        else:
            msg['speaker_label'] = msg.get('speaker', 'Unknown')
            msg['speaker_class'] = 'other'

    return jsonify({
        'call_id': call_id,
        'message_count': len(results),
        'messages': results
    })


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


# ========================================
# CHURN ANALYTICS API ENDPOINTS
# ========================================

@app.route('/api/subscriber-status/<subscriber_no>/<ban>')
def api_subscriber_status(subscriber_no, ban):
    """Get subscriber active status from SUBSCRIBER table"""
    query = """
        SELECT SUB_STATUS, PRODUCT_CODE
        FROM SUBSCRIBER
        WHERE SUBSCRIBER_NO = :subscriber_no
        AND CUSTOMER_BAN = :ban
    """
    result = execute_single(query, {'subscriber_no': subscriber_no, 'ban': ban})
    return jsonify({
        'status': result.get('sub_status', 'UNKNOWN') if result else 'UNKNOWN',
        'is_active': result.get('sub_status') == 'A' if result else False,
        'product_code': result.get('product_code') if result else None
    })


@app.route('/api/churn/accuracy')
def api_churn_accuracy():
    """Get churn prediction accuracy stats for score >= 70"""
    # Query 1: Total predictions with churn_score >= 70
    total_query = """
        SELECT COUNT(*) as total_predictions
        FROM CONVERSATION_SUMMARY
        WHERE CHURN_SCORE >= 70
    """

    # Query 2: Actual churns (subscribers with score>=70 who churned)
    actual_query = """
        SELECT COUNT(*) as actual_churns
        FROM SUBSCRIBER
        WHERE (SUBSCRIBER_NO, CUSTOMER_BAN) IN (
            SELECT SUBSCRIBER_NO, BAN
            FROM CONVERSATION_SUMMARY
            WHERE CHURN_SCORE >= 70
        ) AND SUB_STATUS = 'C'
    """

    total = execute_single(total_query)
    actual = execute_single(actual_query)

    total_predictions = total.get('total_predictions', 0) or 0
    actual_churns = actual.get('actual_churns', 0) or 0
    accuracy = round((actual_churns / total_predictions * 100), 1) if total_predictions > 0 else 0

    return jsonify({
        'total_predictions': total_predictions,
        'actual_churns': actual_churns,
        'accuracy_rate': accuracy,
        'false_positives': total_predictions - actual_churns
    })


@app.route('/api/churn/by-product')
def api_churn_by_product():
    """Get churn breakdown by product code"""
    query = """
        SELECT PRODUCT_CODE, COUNT(*) as count
        FROM SUBSCRIBER
        WHERE (SUBSCRIBER_NO, CUSTOMER_BAN) IN (
            SELECT SUBSCRIBER_NO, BAN
            FROM CONVERSATION_SUMMARY
            WHERE CHURN_SCORE >= 70
        ) AND SUB_STATUS = 'C'
        GROUP BY PRODUCT_CODE
        ORDER BY count DESC
    """
    results = execute_query(query)
    return jsonify(results)


@app.route('/api/churn/by-score-range')
def api_churn_by_score_range():
    """Get churn analysis by score ranges (90-100, 70-90, 40-70, 0-40)"""
    ranges = [
        {'label': '90-100 (Critical)', 'min': 90, 'max': 100},
        {'label': '70-90 (High)', 'min': 70, 'max': 89},
        {'label': '40-70 (Medium)', 'min': 40, 'max': 69},
        {'label': '0-40 (Low)', 'min': 0, 'max': 39},
    ]

    results = []
    for r in ranges:
        # Total predictions in this range
        pred_query = """
            SELECT COUNT(*) as count
            FROM CONVERSATION_SUMMARY
            WHERE CHURN_SCORE >= :min_score AND CHURN_SCORE <= :max_score
        """
        pred = execute_single(pred_query, {'min_score': r['min'], 'max_score': r['max']})

        # Actual churns in this range
        churn_query = """
            SELECT COUNT(*) as count
            FROM SUBSCRIBER
            WHERE (SUBSCRIBER_NO, CUSTOMER_BAN) IN (
                SELECT SUBSCRIBER_NO, BAN
                FROM CONVERSATION_SUMMARY
                WHERE CHURN_SCORE >= :min_score AND CHURN_SCORE <= :max_score
            ) AND SUB_STATUS = 'C'
        """
        churns = execute_single(churn_query, {'min_score': r['min'], 'max_score': r['max']})

        predictions = pred.get('count', 0) or 0
        actual_churns = churns.get('count', 0) or 0
        accuracy = round((actual_churns / predictions * 100), 1) if predictions > 0 else 0

        results.append({
            'label': r['label'],
            'range': f"{r['min']}-{r['max']}",
            'predictions': predictions,
            'actual_churns': actual_churns,
            'accuracy': accuracy,
            'false_positives': predictions - actual_churns
        })

    return jsonify(results)


@app.route('/api/churn/trend')
def api_churn_trend():
    """Get churn score trend over time (daily breakdown by risk level)"""
    days = request.args.get('days', 30, type=int)

    query = """
        SELECT
            TO_CHAR(TRUNC(CREATION_DATE), 'YYYY-MM-DD') as date,
            COUNT(*) as total_calls,
            COUNT(CASE WHEN CHURN_SCORE >= 70 THEN 1 END) as high_risk,
            COUNT(CASE WHEN CHURN_SCORE >= 40 AND CHURN_SCORE < 70 THEN 1 END) as medium_risk,
            COUNT(CASE WHEN CHURN_SCORE < 40 OR CHURN_SCORE IS NULL THEN 1 END) as low_risk,
            ROUND(AVG(CHURN_SCORE), 1) as avg_score
        FROM CONVERSATION_SUMMARY
        WHERE CREATION_DATE > SYSDATE - :days
        GROUP BY TRUNC(CREATION_DATE)
        ORDER BY TRUNC(CREATION_DATE)
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/churn/high-risk-calls')
def api_high_risk_calls():
    """Get list of high risk calls (score >= 70) with subscriber status"""
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 100, type=int)

    query = """
        SELECT
            cs.SOURCE_ID as call_id,
            cs.SOURCE_TYPE as type,
            TO_CHAR(cs.CREATION_DATE, 'YYYY-MM-DD HH24:MI') as created,
            cs.CHURN_SCORE as churn_score,
            cs.SUBSCRIBER_NO as subscriber_no,
            cs.BAN as ban,
            SUBSTR(cs.SUMMARY, 1, 100) as summary,
            s.SUB_STATUS as sub_status,
            s.PRODUCT_CODE as product_code
        FROM CONVERSATION_SUMMARY cs
        LEFT JOIN SUBSCRIBER s ON cs.SUBSCRIBER_NO = s.SUBSCRIBER_NO
                                 AND cs.BAN = s.CUSTOMER_BAN
        WHERE cs.CHURN_SCORE >= 70
        AND cs.CREATION_DATE > SYSDATE - :days
        ORDER BY cs.CHURN_SCORE DESC, cs.CREATION_DATE DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'days': days, 'limit': limit})
    return jsonify(results)


# ========================================
# ML QUALITY API ENDPOINTS
# ========================================

# AWS clients for config management
S3_BUCKET = os.getenv('ML_CONFIG_S3_BUCKET', 'pelephone-ml-configs')
SQS_QUEUE = os.getenv('ML_CONFIG_SQS_QUEUE',
    'https://sqs.eu-west-1.amazonaws.com/320708867194/ml-config-updates')

# Initialize AWS clients (lazy loading)
_s3_client = None
_sqs_client = None


def get_s3_client():
    """Get or create S3 client."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3', region_name='eu-west-1')
    return _s3_client


def get_sqs_client():
    """Get or create SQS client."""
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client('sqs', region_name='eu-west-1')
    return _sqs_client


@app.route('/api/ml-quality/recommendations')
def api_ml_recommendations():
    """Get pending ML recommendations for review."""
    query = """
        SELECT
            RAWTOHEX(REC_ID) as rec_id,
            REC_TYPE,
            REC_DETAILS,
            STATUS,
            TO_CHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI') as created_at,
            APPROVED_BY,
            TO_CHAR(APPROVED_AT, 'YYYY-MM-DD HH24:MI') as approved_at
        FROM ML_CONFIG_RECOMMENDATIONS
        WHERE STATUS = 'PENDING'
        ORDER BY CREATED_AT DESC
    """
    results = execute_query(query)

    # Parse JSON details
    for r in results:
        if r.get('rec_details'):
            try:
                r['rec_details'] = json.loads(r['rec_details'])
            except:
                pass  # Keep as string if not valid JSON

    return jsonify(results)


@app.route('/api/ml-quality/history')
def api_ml_history():
    """Get ML evaluation history."""
    days = request.args.get('days', 90, type=int)

    query = """
        SELECT
            RAWTOHEX(EVAL_ID) as eval_id,
            TO_CHAR(EVAL_DATE, 'YYYY-MM-DD') as eval_date,
            CHURNED_COUNT,
            WITH_SCORE_COUNT,
            ROUND(RECALL_RATE * 100, 1) as recall_percent,
            ROUND(COVERAGE_RATE * 100, 1) as coverage_percent,
            ROUND(AVG_CHURN_SCORE, 1) as avg_churn_score,
            RECOMMENDATIONS_GENERATED
        FROM ML_EVALUATION_HISTORY
        WHERE EVAL_DATE > SYSDATE - :days
        ORDER BY EVAL_DATE DESC
    """
    results = execute_query(query, {'days': days})
    return jsonify(results)


@app.route('/api/ml-quality/approve', methods=['POST'])
def api_ml_approve():
    """
    Approve a recommendation - uploads config to S3 but does NOT trigger ML reload.
    Human must separately click "Apply to ML" to trigger reload.
    """
    import json as json_module

    data = request.json
    rec_id = data.get('rec_id')
    approver = data.get('approver', 'dashboard_user')

    if not rec_id:
        return jsonify({'error': 'rec_id is required'}), 400

    # Get recommendation details
    query = """
        SELECT REC_TYPE, REC_DETAILS
        FROM ML_CONFIG_RECOMMENDATIONS
        WHERE RAWTOHEX(REC_ID) = :rec_id AND STATUS = 'PENDING'
    """
    rec = execute_single(query, {'rec_id': rec_id})
    if not rec:
        return jsonify({'error': 'Recommendation not found or already processed'}), 404

    try:
        rec_details = json_module.loads(rec['rec_details']) if isinstance(rec['rec_details'], str) else rec['rec_details']
        rec_type = rec['rec_type']

        # Apply changes to S3 config ONLY (no SQS trigger yet!)
        s3 = get_s3_client()

        if rec_type == 'churn_keywords':
            # Download current keywords config
            obj = s3.get_object(Bucket=S3_BUCKET, Key='configs/classification-keywords.json')
            config = json_module.loads(obj['Body'].read().decode('utf-8'))

            # Add new keywords to appropriate category
            new_keywords = rec_details.get('keywords', [])
            existing_medium = set(config.get('churn_keywords', {}).get('medium', []))
            config['churn_keywords']['medium'] = list(existing_medium | set(new_keywords))

            # Upload updated config
            s3.put_object(
                Bucket=S3_BUCKET,
                Key='configs/classification-keywords.json',
                Body=json_module.dumps(config, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Added {len(new_keywords)} new churn keywords to S3")

        elif rec_type == 'churn_threshold':
            # Download current classifications config
            obj = s3.get_object(Bucket=S3_BUCKET, Key='configs/call-classifications.json')
            config = json_module.loads(obj['Body'].read().decode('utf-8'))

            # Update threshold
            new_threshold = rec_details.get('recommended_value', 40)
            if 'churn_detection' in config:
                config['churn_detection']['threshold'] = new_threshold / 100.0

            # Upload updated config
            s3.put_object(
                Bucket=S3_BUCKET,
                Key='configs/call-classifications.json',
                Body=json_module.dumps(config, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Updated churn threshold to {new_threshold} in S3")

        # Mark as approved (but NOT applied to ML yet)
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE ML_CONFIG_RECOMMENDATIONS
            SET STATUS = 'APPROVED', APPROVED_BY = :approver, APPROVED_AT = SYSTIMESTAMP
            WHERE RAWTOHEX(REC_ID) = :rec_id
        """, {'approver': approver, 'rec_id': rec_id})
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Config uploaded to S3. Use "Apply to ML" when ready to reload.',
            'rec_type': rec_type
        })

    except Exception as e:
        logger.error(f"Error approving recommendation: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ml-quality/apply-to-ml', methods=['POST'])
def api_ml_apply():
    """
    MANUAL TRIGGER - Send SQS message to tell ML service to download configs from S3.
    This gives human full control over WHEN ML service picks up new configs.
    """
    import json as json_module

    data = request.json
    triggered_by = data.get('triggered_by', 'dashboard_user')

    try:
        sqs = get_sqs_client()

        # Send SQS notification to ML service
        sqs.send_message(
            QueueUrl=SQS_QUEUE,
            MessageBody=json_module.dumps({
                'action': 'reload_configs',
                'triggered_by': triggered_by,
                'timestamp': datetime.utcnow().isoformat()
            })
        )

        logger.info(f"SQS reload trigger sent by {triggered_by}")

        return jsonify({
            'success': True,
            'message': 'SQS message sent - ML service will reload configs shortly'
        })

    except Exception as e:
        logger.error(f"Error sending SQS message: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ml-quality/reject', methods=['POST'])
def api_ml_reject():
    """Reject a recommendation - marks as rejected, no changes applied."""
    data = request.json
    rec_id = data.get('rec_id')
    rejected_by = data.get('rejected_by', 'dashboard_user')
    reason = data.get('reason', '')

    if not rec_id:
        return jsonify({'error': 'rec_id is required'}), 400

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE ML_CONFIG_RECOMMENDATIONS
            SET STATUS = 'REJECTED', NOTES = :reason
            WHERE RAWTOHEX(REC_ID) = :rec_id
        """, {'rec_id': rec_id, 'reason': f"Rejected by {rejected_by}: {reason}"})
        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'Recommendation rejected'})

    except Exception as e:
        logger.error(f"Error rejecting recommendation: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ml-quality/feedback', methods=['POST'])
def api_ml_feedback():
    """Submit human feedback on ML classification."""
    data = request.json

    call_id = data.get('call_id')
    ml_category = data.get('ml_category')
    correct_category = data.get('correct_category')
    is_correct = data.get('is_correct', False)
    reviewer = data.get('reviewer', 'dashboard_user')

    if not call_id:
        return jsonify({'error': 'call_id is required'}), 400

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ML_CLASSIFICATION_FEEDBACK (
                FEEDBACK_ID, CALL_ID, ML_CATEGORY, CORRECT_CATEGORY,
                IS_CORRECT, REVIEWER, CREATED_AT
            ) VALUES (
                SYS_GUID(), :call_id, :ml_category, :correct_category,
                :is_correct, :reviewer, SYSTIMESTAMP
            )
        """, {
            'call_id': call_id,
            'ml_category': ml_category,
            'correct_category': correct_category if not is_correct else None,
            'is_correct': 1 if is_correct else 0,
            'reviewer': reviewer
        })
        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'Feedback recorded'})

    except Exception as e:
        logger.error(f"Error recording feedback: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ml-quality/metrics')
def api_ml_metrics():
    """Get current ML quality metrics."""
    days = request.args.get('days', 7, type=int)

    # Get latest evaluation results
    latest_eval = execute_single("""
        SELECT
            TO_CHAR(EVAL_DATE, 'YYYY-MM-DD') as last_eval_date,
            CHURNED_COUNT as churned,
            ROUND(RECALL_RATE * 100, 1) as recall_percent,
            ROUND(COVERAGE_RATE * 100, 1) as coverage_percent,
            ROUND(AVG_CHURN_SCORE, 1) as avg_score
        FROM ML_EVALUATION_HISTORY
        ORDER BY EVAL_DATE DESC
        FETCH FIRST 1 ROW ONLY
    """)

    # Count pending recommendations
    pending = execute_single("""
        SELECT COUNT(*) as count
        FROM ML_CONFIG_RECOMMENDATIONS
        WHERE STATUS = 'PENDING'
    """)

    # Count feedback entries
    feedback_stats = execute_single("""
        SELECT
            COUNT(*) as total_feedback,
            SUM(CASE WHEN IS_CORRECT = 1 THEN 1 ELSE 0 END) as correct_count,
            SUM(CASE WHEN IS_CORRECT = 0 THEN 1 ELSE 0 END) as incorrect_count
        FROM ML_CLASSIFICATION_FEEDBACK
        WHERE CREATED_AT > SYSDATE - :days
    """, {'days': days})

    return jsonify({
        'last_evaluation': latest_eval,
        'pending_recommendations': pending.get('count', 0) if pending else 0,
        'feedback_stats': feedback_stats
    })


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
