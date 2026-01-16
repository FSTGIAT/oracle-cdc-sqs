"""
Analytics Routes - Summary, sentiment, categories, satisfaction, daily, recent, errors
"""

from flask import Blueprint, jsonify, request
from . import execute_query, execute_single

analytics_bp = Blueprint('analytics', __name__)


@analytics_bp.route('/summary')
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
            COUNT(CASE WHEN SENTIMENT >= 4 THEN 1 END) as positive,
            COUNT(CASE WHEN SENTIMENT <= 2 THEN 1 END) as negative,
            COUNT(CASE WHEN SENTIMENT = 3 OR SENTIMENT IS NULL THEN 1 END) as neutral
        FROM CONVERSATION_SUMMARY
        WHERE CONVERSATION_TIME > SYSDATE - :days
    """

    result = execute_single(query, {'days': days})
    return jsonify(result)


@analytics_bp.route('/categories')
def api_categories():
    """Get category distribution"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT CATEGORY_CODE as category, COUNT(*) as count
        FROM CONVERSATION_CATEGORY cc
        JOIN CONVERSATION_SUMMARY cs ON cc.SOURCE_ID = cs.SOURCE_ID AND cc.SOURCE_TYPE = cs.SOURCE_TYPE
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        GROUP BY CATEGORY_CODE
        ORDER BY count DESC
        FETCH FIRST 15 ROWS ONLY
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/sentiment')
def api_sentiment():
    """Get sentiment breakdown"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT
            CASE
                WHEN SENTIMENT >= 4 THEN 'Positive'
                WHEN SENTIMENT <= 2 THEN 'Negative'
                ELSE 'Neutral'
            END as sentiment,
            COUNT(*) as count
        FROM CONVERSATION_SUMMARY
        WHERE CONVERSATION_TIME > SYSDATE - :days
        GROUP BY CASE
            WHEN SENTIMENT >= 4 THEN 'Positive'
            WHEN SENTIMENT <= 2 THEN 'Negative'
            ELSE 'Neutral'
        END
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/churn')
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
        WHERE CONVERSATION_TIME > SYSDATE - :days
        GROUP BY CASE
            WHEN CHURN_SCORE >= 70 THEN 'High Risk (70+)'
            WHEN CHURN_SCORE >= 40 THEN 'Medium Risk (40-69)'
            ELSE 'Low Risk (0-39)'
        END
        ORDER BY risk_level
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/satisfaction')
def api_satisfaction():
    """Get satisfaction distribution (1-5)"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT SATISFACTION as rating, COUNT(*) as count
        FROM CONVERSATION_SUMMARY
        WHERE CONVERSATION_TIME > SYSDATE - :days
        AND SATISFACTION IS NOT NULL
        GROUP BY SATISFACTION
        ORDER BY SATISFACTION
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/errors')
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


@analytics_bp.route('/recent')
def api_recent():
    """Get recent conversations"""
    days = request.args.get('days', 7, type=int)

    query = """
        SELECT
            SOURCE_ID as id,
            SOURCE_TYPE as type,
            TO_CHAR(CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
            SUBSTR(SUMMARY, 1, 150) as summary,
            SENTIMENT as sentiment,
            SATISFACTION as satisfaction,
            ROUND(CHURN_SCORE, 1) as churn_score
        FROM CONVERSATION_SUMMARY
        WHERE CONVERSATION_TIME > SYSDATE - :days
        ORDER BY CONVERSATION_TIME DESC
        FETCH FIRST 50 ROWS ONLY
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/daily')
def api_daily():
    """Get daily conversation counts for trend"""
    days = request.args.get('days', 30, type=int)

    query = """
        SELECT
            TO_CHAR(TRUNC(CONVERSATION_TIME), 'YYYY-MM-DD') as call_date,
            COUNT(*) as count,
            ROUND(AVG(SATISFACTION), 2) as avg_satisfaction,
            ROUND(AVG(CHURN_SCORE), 2) as avg_churn
        FROM CONVERSATION_SUMMARY
        WHERE CONVERSATION_TIME > SYSDATE - :days
        GROUP BY TRUNC(CONVERSATION_TIME)
        ORDER BY TRUNC(CONVERSATION_TIME)
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)
