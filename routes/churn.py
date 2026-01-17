"""
Churn Analytics Routes - Accuracy, by-product, by-score-range, trend, high-risk-calls
"""

from math import ceil
from flask import Blueprint, jsonify, request
from . import execute_query, execute_single

churn_bp = Blueprint('churn', __name__)


@churn_bp.route('/accuracy')
def api_churn_accuracy():
    """Get churn prediction accuracy stats for score >= 70"""
    days = request.args.get('days', 180, type=int)

    # Query 1: Total predictions
    total_query = """
        SELECT COUNT(*) as total_predictions
        FROM CONVERSATION_SUMMARY
        WHERE CHURN_SCORE >= 70
        AND CONVERSATION_TIME > SYSDATE - :days
    """

    # Query 2: Actual churns - use TO_CHAR for type conversion
    actual_query = """
        SELECT COUNT(*) as actual_churns
        FROM SUBSCRIBER a
        WHERE (a.SUBSCRIBER_NO, a.CUSTOMER_BAN) IN (
            SELECT TO_CHAR(SUBSCRIBER_NO), BAN
            FROM CONVERSATION_SUMMARY
            WHERE CHURN_SCORE >= 70
            AND CONVERSATION_TIME > SYSDATE - :days
        ) AND a.SUB_STATUS = 'C'
    """

    total = execute_single(total_query, {'days': days})
    actual = execute_single(actual_query, {'days': days})

    total_predictions = total.get('total_predictions', 0) or 0
    actual_churns = actual.get('actual_churns', 0) or 0
    accuracy = round((actual_churns / total_predictions * 100), 1) if total_predictions > 0 else 0

    return jsonify({
        'total_predictions': total_predictions,
        'actual_churns': actual_churns,
        'accuracy_rate': accuracy,
        'false_positives': total_predictions - actual_churns
    })


@churn_bp.route('/by-product')
def api_churn_by_product():
    """Get churn breakdown by product code"""
    days = request.args.get('days', 180, type=int)

    query = """
        SELECT a.PRODUCT_CODE, COUNT(*) as count
        FROM SUBSCRIBER a
        WHERE (a.SUBSCRIBER_NO, a.CUSTOMER_BAN) IN (
            SELECT TO_CHAR(SUBSCRIBER_NO), BAN
            FROM CONVERSATION_SUMMARY
            WHERE CHURN_SCORE >= 70
            AND CONVERSATION_TIME > SYSDATE - :days
        ) AND a.SUB_STATUS = 'C'
        GROUP BY a.PRODUCT_CODE
        ORDER BY count DESC
    """
    results = execute_query(query, {'days': days})
    return jsonify(results if results else [])


@churn_bp.route('/by-score-range')
def api_churn_by_score_range():
    """Get churn analysis by score ranges (90-100, 70-90, 40-70, 0-40)"""
    days = request.args.get('days', 180, type=int)

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
            AND CONVERSATION_TIME > SYSDATE - :days
        """
        pred = execute_single(pred_query, {'min_score': r['min'], 'max_score': r['max'], 'days': days})
        predictions = pred.get('count', 0) or 0

        # Count churned - use TO_CHAR for type conversion
        churn_query = """
            SELECT COUNT(*) as count
            FROM SUBSCRIBER a
            WHERE (a.SUBSCRIBER_NO, a.CUSTOMER_BAN) IN (
                SELECT TO_CHAR(SUBSCRIBER_NO), BAN
                FROM CONVERSATION_SUMMARY
                WHERE CHURN_SCORE >= :min_score AND CHURN_SCORE <= :max_score
                AND CONVERSATION_TIME > SYSDATE - :days
            )
            AND a.SUB_STATUS = 'C'
        """
        churn_result = execute_single(churn_query, {'min_score': r['min'], 'max_score': r['max'], 'days': days})
        actual_churns = churn_result.get('count', 0) or 0

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


@churn_bp.route('/trend')
def api_churn_trend():
    """Get churn score trend over time (daily breakdown by risk level)"""
    days = request.args.get('days', 30, type=int)

    query = """
        SELECT
            TO_CHAR(TRUNC(CONVERSATION_TIME), 'YYYY-MM-DD') as call_date,
            COUNT(*) as total_calls,
            COUNT(CASE WHEN CHURN_SCORE >= 70 THEN 1 END) as high_risk,
            COUNT(CASE WHEN CHURN_SCORE >= 40 AND CHURN_SCORE < 70 THEN 1 END) as medium_risk,
            COUNT(CASE WHEN CHURN_SCORE < 40 OR CHURN_SCORE IS NULL THEN 1 END) as low_risk,
            ROUND(AVG(CHURN_SCORE), 1) as avg_score
        FROM CONVERSATION_SUMMARY
        WHERE CONVERSATION_TIME > SYSDATE - :days
        GROUP BY TRUNC(CONVERSATION_TIME)
        ORDER BY TRUNC(CONVERSATION_TIME)
    """

    results = execute_query(query, {'days': days})
    return jsonify(results)


@churn_bp.route('/high-risk-calls')
def api_high_risk_calls():
    """Get high risk calls with filter and pagination"""
    days = request.args.get('days', 7, type=int)
    min_score = request.args.get('min_score', 70, type=int)
    max_score = request.args.get('max_score', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    limit = request.args.get('limit', 25, type=int)

    # Get total count for pagination
    count_query = """
        SELECT COUNT(*) as total FROM CONVERSATION_SUMMARY
        WHERE CHURN_SCORE >= :min_score AND CHURN_SCORE <= :max_score
        AND CONVERSATION_TIME > SYSDATE - :days
    """
    count_result = execute_single(count_query, {
        'min_score': min_score, 'max_score': max_score, 'days': days
    })
    total = count_result.get('total', 0) or 0

    # Single query with LEFT JOIN - no loop, uses || '' pattern
    calls_query = """
        SELECT
            cs.SOURCE_ID as call_id,
            cs.SOURCE_TYPE as type,
            TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
            cs.CHURN_SCORE as churn_score,
            cs.SUBSCRIBER_NO || ' ' as subscriber_no,
            cs.BAN as ban,
            SUBSTR(cs.SUMMARY, 1, 100) as summary,
            s.SUB_STATUS as sub_status,
            s.PRODUCT_CODE as product_code
        FROM CONVERSATION_SUMMARY cs
        LEFT JOIN SUBSCRIBER s
            ON s.SUBSCRIBER_NO = TO_CHAR(cs.SUBSCRIBER_NO)
            AND s.CUSTOMER_BAN = cs.BAN
        WHERE cs.CHURN_SCORE >= :min_score AND cs.CHURN_SCORE <= :max_score
        AND cs.CONVERSATION_TIME > SYSDATE - :days
        ORDER BY cs.CHURN_SCORE DESC, cs.CONVERSATION_TIME DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """
    results = execute_query(calls_query, {
        'min_score': min_score,
        'max_score': max_score,
        'days': days,
        'offset': offset,
        'limit': limit
    })

    return jsonify({
        'data': results,
        'total': total,
        'offset': offset,
        'limit': limit,
        'pages': ceil(total / limit) if limit > 0 else 1
    })
