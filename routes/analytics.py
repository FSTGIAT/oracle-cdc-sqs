"""
Analytics Routes - Summary, sentiment, categories, satisfaction, daily, recent, errors
"""

from flask import Blueprint, jsonify, request
from . import execute_query, execute_single, build_call_type_filter

analytics_bp = Blueprint('analytics', __name__)


@analytics_bp.route('/summary')
def api_summary():
    """Get overall summary statistics"""
    days = request.args.get('days', 7, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN cs.SOURCE_TYPE='CALL' THEN 1 END) as calls,
            COUNT(CASE WHEN cs.SOURCE_TYPE='WAPP' THEN 1 END) as whatsapp,
            ROUND(AVG(cs.SATISFACTION), 2) as avg_satisfaction,
            ROUND(AVG(cs.CHURN_SCORE), 2) as avg_churn_score,
            COUNT(CASE WHEN cs.SENTIMENT >= 4 THEN 1 END) as positive,
            COUNT(CASE WHEN cs.SENTIMENT <= 2 THEN 1 END) as negative,
            COUNT(CASE WHEN cs.SENTIMENT = 3 OR cs.SENTIMENT IS NULL THEN 1 END) as neutral
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
    """

    result = execute_single(query, {'days': days})
    return jsonify(result)


@analytics_bp.route('/categories')
def api_categories():
    """Get category distribution"""
    days = request.args.get('days', 7, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT CATEGORY_CODE as category, COUNT(*) as count
        FROM CONVERSATION_CATEGORY cc
        JOIN CONVERSATION_SUMMARY cs ON cc.SOURCE_ID = cs.SOURCE_ID AND cc.SOURCE_TYPE = cs.SOURCE_TYPE
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
        GROUP BY CATEGORY_CODE
        ORDER BY count DESC
        FETCH FIRST 15 ROWS ONLY
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/sentiment')
def api_sentiment():
    """Get sentiment breakdown - Negative (1-2) and Other (3-5)"""
    days = request.args.get('days', 7, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            CASE
                WHEN cs.SENTIMENT <= 2 THEN 'Negative'
                ELSE 'Other'
            END as sentiment,
            COUNT(*) as count
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
        GROUP BY CASE
            WHEN cs.SENTIMENT <= 2 THEN 'Negative'
            ELSE 'Other'
        END
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/churn')
def api_churn():
    """Get churn risk distribution - Critical (95-100) and High Risk (90-94)"""
    days = request.args.get('days', 7, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            CASE
                WHEN cs.CHURN_SCORE >= 95 THEN 'Critical (95-100)'
                WHEN cs.CHURN_SCORE >= 90 THEN 'High Risk (90-94)'
            END as risk_level,
            COUNT(*) as count
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        AND cs.CHURN_SCORE >= 90
        {call_type_filter}
        GROUP BY CASE
            WHEN cs.CHURN_SCORE >= 95 THEN 'Critical (95-100)'
            WHEN cs.CHURN_SCORE >= 90 THEN 'High Risk (90-94)'
        END
        ORDER BY risk_level DESC
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/satisfaction')
def api_satisfaction():
    """Get satisfaction distribution (1-5)"""
    days = request.args.get('days', 7, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT cs.SATISFACTION as rating, COUNT(*) as count
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        AND cs.SATISFACTION IS NOT NULL
        {call_type_filter}
        GROUP BY cs.SATISFACTION
        ORDER BY cs.SATISFACTION
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
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            cs.SOURCE_ID as id,
            cs.SOURCE_TYPE as type,
            TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
            SUBSTR(cs.SUMMARY, 1, 150) as summary,
            cs.SENTIMENT as sentiment,
            cs.SATISFACTION as satisfaction,
            ROUND(cs.CHURN_SCORE, 1) as churn_score
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
        ORDER BY cs.CONVERSATION_TIME DESC
        FETCH FIRST 50 ROWS ONLY
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/daily')
def api_daily():
    """Get daily conversation counts for trend"""
    days = request.args.get('days', 30, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            TO_CHAR(TRUNC(cs.CONVERSATION_TIME), 'YYYY-MM-DD') as call_date,
            COUNT(*) as count,
            ROUND(AVG(cs.SATISFACTION), 2) as avg_satisfaction,
            ROUND(AVG(cs.CHURN_SCORE), 2) as avg_churn
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
        GROUP BY TRUNC(cs.CONVERSATION_TIME)
        ORDER BY TRUNC(cs.CONVERSATION_TIME)
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@analytics_bp.route('/categories/overview')
def api_categories_overview():
    """Get ALL categories with counts and stats for overview chart"""
    days = request.args.get('days', 7, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    # Get all categories with counts
    query = f"""
        SELECT
            CATEGORY_CODE as category,
            COUNT(*) as count
        FROM CONVERSATION_CATEGORY cc
        JOIN CONVERSATION_SUMMARY cs
            ON cc.SOURCE_ID = cs.SOURCE_ID
            AND cc.SOURCE_TYPE = cs.SOURCE_TYPE
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
        GROUP BY CATEGORY_CODE
        ORDER BY count DESC
    """
    categories = execute_query(query, {'days': days})

    # Get stats
    stats_query = f"""
        SELECT
            COUNT(DISTINCT cs.SOURCE_ID) as total_conversations,
            COUNT(DISTINCT cc.CATEGORY_CODE) as unique_categories,
            COUNT(*) as total_category_assignments,
            ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT cs.SOURCE_ID), 0), 2) as avg_per_conversation
        FROM CONVERSATION_CATEGORY cc
        JOIN CONVERSATION_SUMMARY cs
            ON cc.SOURCE_ID = cs.SOURCE_ID
            AND cc.SOURCE_TYPE = cs.SOURCE_TYPE
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
    """
    stats = execute_single(stats_query, {'days': days})

    return jsonify({
        'categories': categories,
        'stats': {
            'total_conversations': stats.get('total_conversations', 0) or 0,
            'unique_categories': stats.get('unique_categories', 0) or 0,
            'avg_per_conversation': stats.get('avg_per_conversation', 0) or 0,
            'top_category': categories[0]['category'] if categories else '-'
        }
    })
