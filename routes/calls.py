"""
Calls Routes - Call details, conversation transcript, drill-down endpoints
"""

from flask import Blueprint, jsonify, request
from . import execute_query, execute_single, get_connection

calls_bp = Blueprint('calls', __name__)


@calls_bp.route('/category/calls')
def api_category_calls():
    """Get calls for a specific category"""
    category = request.args.get('category', '')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 50, type=int)

    query = """
        SELECT
            cc.SOURCE_ID as call_id,
            cc.SOURCE_TYPE as type,
            TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
            cs.SUMMARY as summary,
            cs.SENTIMENT as sentiment,
            cs.SATISFACTION as satisfaction,
            ROUND(cs.CHURN_SCORE, 1) as churn_score,
            cs.PRODUCTS as products,
            cs.ACTION_ITEMS as action_items
        FROM CONVERSATION_CATEGORY cc
        LEFT JOIN CONVERSATION_SUMMARY cs ON cc.SOURCE_ID = cs.SOURCE_ID AND cc.SOURCE_TYPE = cs.SOURCE_TYPE
        WHERE cc.CATEGORY_CODE = :category
        AND cs.CONVERSATION_TIME > SYSDATE - :days
        ORDER BY cs.CONVERSATION_TIME DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'category': category, 'days': days, 'limit': limit})
    return jsonify(results)


@calls_bp.route('/sentiment/calls')
def api_sentiment_calls():
    """Get calls for a specific sentiment type"""
    sentiment_type = request.args.get('sentiment', '')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 50, type=int)

    # Map sentiment type to query condition (SENTIMENT is numeric 1-5 scale)
    # New categories: Negative (1-2) and Other (3-5)
    if sentiment_type == 'Negative':
        sentiment_condition = "SENTIMENT <= 2"
    else:  # 'Other' or any other value
        sentiment_condition = "(SENTIMENT >= 3 OR SENTIMENT IS NULL)"

    query = f"""
        SELECT
            SOURCE_ID as call_id,
            SOURCE_TYPE as type,
            TO_CHAR(CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
            SUMMARY as summary,
            SENTIMENT as sentiment,
            SATISFACTION as satisfaction,
            ROUND(CHURN_SCORE, 1) as churn_score,
            PRODUCTS as products,
            ACTION_ITEMS as action_items
        FROM CONVERSATION_SUMMARY
        WHERE {sentiment_condition}
        AND CONVERSATION_TIME > SYSDATE - :days
        ORDER BY CONVERSATION_TIME DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'days': days, 'limit': limit})
    return jsonify(results)


@calls_bp.route('/churn/calls')
def api_churn_calls():
    """Get calls for a specific churn risk level"""
    risk_level = request.args.get('risk_level', '')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 50, type=int)

    # Map risk level to score range
    # New categories: Critical (95-100) and High Risk (90-94)
    if 'Critical' in risk_level or '95' in risk_level:
        score_condition = "CHURN_SCORE >= 95"
    elif 'High' in risk_level or '90' in risk_level:
        score_condition = "CHURN_SCORE >= 90 AND CHURN_SCORE < 95"
    else:
        score_condition = "CHURN_SCORE >= 90"  # Default to all high-risk

    query = f"""
        SELECT
            SOURCE_ID as call_id,
            SOURCE_TYPE as type,
            TO_CHAR(CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
            SUMMARY as summary,
            SENTIMENT as sentiment,
            SATISFACTION as satisfaction,
            ROUND(CHURN_SCORE, 1) as churn_score,
            PRODUCTS as products,
            ACTION_ITEMS as action_items
        FROM CONVERSATION_SUMMARY
        WHERE ({score_condition})
        AND CONVERSATION_TIME > SYSDATE - :days
        ORDER BY CHURN_SCORE DESC NULLS LAST, CONVERSATION_TIME DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'days': days, 'limit': limit})
    return jsonify(results)


@calls_bp.route('/call-details')
def api_call_details():
    """Get call details from CONVERSATION_SUMMARY with subscriber status"""
    call_id = request.args.get('id', '')
    if not call_id:
        return jsonify({'error': 'Missing call id'}), 400

    # Use LEFT JOIN to get subscriber status in one query (same pattern as churn.py)
    query = """
        SELECT
            cs.SOURCE_ID as call_id,
            cs.SOURCE_TYPE as type,
            TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI:SS') as created,
            cs.SUMMARY as summary,
            cs.SENTIMENT as sentiment,
            cs.SATISFACTION as satisfaction,
            ROUND(cs.CHURN_SCORE, 1) as churn_score,
            cs.PRODUCTS as products,
            cs.ACTION_ITEMS as action_items,
            cs.UNRESOLVED_ISSUES as unresolved_issues,
            cs.BAN as ban,
            cs.SUBSCRIBER_NO || ' ' as subscriber_no,
            s.SUB_STATUS as sub_status,
            s.PRODUCT_CODE as product_code
        FROM CONVERSATION_SUMMARY cs
        LEFT JOIN SUBSCRIBER s
            ON s.SUBSCRIBER_NO = cs.SUBSCRIBER_NO || ' '
            AND s.CUSTOMER_BAN = cs.BAN
        WHERE cs.SOURCE_ID = :call_id
    """

    result = execute_single(query, {'call_id': call_id})

    if not result:
        return jsonify({'error': 'Call not found', 'call_id': call_id}), 404

    # Get categories for this call
    cat_query = """
        SELECT CATEGORY_CODE as category
        FROM CONVERSATION_CATEGORY
        WHERE SOURCE_ID = :call_id
    """
    categories = execute_query(cat_query, {'call_id': call_id})
    result['categories'] = [c['category'] for c in categories]

    # Get queue name from VERINT_TEXT_ANALYSIS (with performance filter)
    queue_query = """
        SELECT QUEUE_NAME
        FROM VERINT_TEXT_ANALYSIS
        WHERE CALL_ID = :call_id
        AND CALL_TIME > SYSDATE - 365
        FETCH FIRST 1 ROW ONLY
    """
    queue_result = execute_single(queue_query, {'call_id': call_id})
    result['queue_name'] = queue_result.get('queue_name') if queue_result else None

    return jsonify(result)


@calls_bp.route('/call-conversation')
def api_call_conversation():
    """Get full conversation from VERINT_TEXT_ANALYSIS"""
    call_id = request.args.get('id', '')
    if not call_id:
        return jsonify({'error': 'Missing call id', 'messages': []}), 400

    query = """
        SELECT
            CALL_ID as call_id,
            OWNER as speaker,
            TO_CHAR(CALL_TIME, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
            DBMS_LOB.SUBSTR(TEXT, 4000, 1) as text
        FROM VERINT_TEXT_ANALYSIS
        WHERE CALL_ID = :call_id
        AND CALL_TIME > SYSDATE - 365
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


@calls_bp.route('/subscriber-status/<subscriber_no>/<ban>')
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
