"""
Alert Evaluator Module
Evaluates alert conditions and creates history records when thresholds are exceeded
"""

import json
from . import execute_query, execute_single, get_connection


def evaluate_metric(metric_source, metric_name, time_window_hours, filter_product=None):
    """
    Evaluate a metric and return current value plus affected subscribers
    Returns: (metric_value, affected_subscribers_list)
    """
    params = {'hours': time_window_hours}

    # Build product filter if specified
    product_filter = ""
    if filter_product:
        product_filter = "AND cs.SOURCE_ID IN (SELECT SOURCE_ID FROM CONVERSATION_SUMMARY WHERE BAN IN (SELECT CUSTOMER_BAN FROM SUBSCRIBER WHERE PRODUCT_CODE = :product))"
        params['product'] = filter_product

    # ===== CHURN METRICS =====
    if metric_source == 'churn':
        if metric_name == 'high_risk_count':
            # Count calls with churn score >= 70
            count_query = f"""
                SELECT COUNT(*) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE cs.CHURN_SCORE >= 70
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(count_query, params)
            value = result.get('value', 0) or 0

            # Get affected subscribers
            subs_query = f"""
                SELECT
                    cs.SUBSCRIBER_NO || ' ' as subscriber_no,
                    cs.BAN,
                    cs.CHURN_SCORE,
                    TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as call_time,
                    s.PRODUCT_CODE,
                    s.SUB_STATUS
                FROM CONVERSATION_SUMMARY cs
                LEFT JOIN SUBSCRIBER s ON s.SUBSCRIBER_NO = cs.SUBSCRIBER_NO || ' ' AND s.CUSTOMER_BAN = cs.BAN
                WHERE cs.CHURN_SCORE >= 70
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
                ORDER BY cs.CHURN_SCORE DESC
                FETCH FIRST 100 ROWS ONLY
            """
            subscribers = execute_query(subs_query, params)
            return value, subscribers

        elif metric_name == 'avg_churn_score':
            query = f"""
                SELECT ROUND(AVG(CHURN_SCORE), 1) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE CHURN_SCORE IS NOT NULL
                AND CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(query, params)
            return result.get('value', 0) or 0, []

        elif metric_name == 'critical_risk_count':
            count_query = f"""
                SELECT COUNT(*) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE cs.CHURN_SCORE >= 90
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(count_query, params)
            value = result.get('value', 0) or 0

            subs_query = f"""
                SELECT
                    cs.SUBSCRIBER_NO || ' ' as subscriber_no,
                    cs.BAN,
                    cs.CHURN_SCORE,
                    TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as call_time,
                    s.PRODUCT_CODE,
                    s.SUB_STATUS
                FROM CONVERSATION_SUMMARY cs
                LEFT JOIN SUBSCRIBER s ON s.SUBSCRIBER_NO = cs.SUBSCRIBER_NO || ' ' AND s.CUSTOMER_BAN = cs.BAN
                WHERE cs.CHURN_SCORE >= 90
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
                ORDER BY cs.CHURN_SCORE DESC
                FETCH FIRST 100 ROWS ONLY
            """
            subscribers = execute_query(subs_query, params)
            return value, subscribers

    # ===== SENTIMENT METRICS =====
    elif metric_source == 'sentiment':
        if metric_name == 'negative_count':
            count_query = f"""
                SELECT COUNT(*) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE LOWER(cs.OVERALL_SENTIMENT) IN ('negative', 'שלילי')
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(count_query, params)
            value = result.get('value', 0) or 0

            subs_query = f"""
                SELECT
                    cs.SUBSCRIBER_NO || ' ' as subscriber_no,
                    cs.BAN,
                    cs.OVERALL_SENTIMENT,
                    cs.CHURN_SCORE,
                    TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as call_time,
                    s.PRODUCT_CODE
                FROM CONVERSATION_SUMMARY cs
                LEFT JOIN SUBSCRIBER s ON s.SUBSCRIBER_NO = cs.SUBSCRIBER_NO || ' ' AND s.CUSTOMER_BAN = cs.BAN
                WHERE LOWER(cs.OVERALL_SENTIMENT) IN ('negative', 'שלילי')
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
                ORDER BY cs.CONVERSATION_TIME DESC
                FETCH FIRST 100 ROWS ONLY
            """
            subscribers = execute_query(subs_query, params)
            return value, subscribers

        elif metric_name == 'negative_percent':
            query = f"""
                SELECT
                    ROUND(
                        COUNT(CASE WHEN LOWER(OVERALL_SENTIMENT) IN ('negative', 'שלילי') THEN 1 END) * 100.0 /
                        NULLIF(COUNT(*), 0)
                    , 1) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(query, params)
            return result.get('value', 0) or 0, []

        elif metric_name == 'positive_percent':
            query = f"""
                SELECT
                    ROUND(
                        COUNT(CASE WHEN LOWER(OVERALL_SENTIMENT) IN ('positive', 'חיובי') THEN 1 END) * 100.0 /
                        NULLIF(COUNT(*), 0)
                    , 1) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(query, params)
            return result.get('value', 0) or 0, []

    # ===== SATISFACTION METRICS =====
    elif metric_source == 'satisfaction':
        if metric_name == 'avg_satisfaction':
            query = f"""
                SELECT ROUND(AVG(CUSTOMER_SATISFACTION), 2) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE CUSTOMER_SATISFACTION IS NOT NULL
                AND CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(query, params)
            return result.get('value', 0) or 0, []

        elif metric_name == 'low_satisfaction_count':
            count_query = f"""
                SELECT COUNT(*) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE cs.CUSTOMER_SATISFACTION < 3
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(count_query, params)
            value = result.get('value', 0) or 0

            subs_query = f"""
                SELECT
                    cs.SUBSCRIBER_NO || ' ' as subscriber_no,
                    cs.BAN,
                    cs.CUSTOMER_SATISFACTION,
                    cs.CHURN_SCORE,
                    TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as call_time,
                    s.PRODUCT_CODE
                FROM CONVERSATION_SUMMARY cs
                LEFT JOIN SUBSCRIBER s ON s.SUBSCRIBER_NO = cs.SUBSCRIBER_NO || ' ' AND s.CUSTOMER_BAN = cs.BAN
                WHERE cs.CUSTOMER_SATISFACTION < 3
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
                ORDER BY cs.CUSTOMER_SATISFACTION ASC
                FETCH FIRST 100 ROWS ONLY
            """
            subscribers = execute_query(subs_query, params)
            return value, subscribers

    # ===== ML QUALITY METRICS =====
    elif metric_source == 'ml_quality':
        if metric_name == 'pending_count':
            query = """
                SELECT COUNT(*) as value
                FROM ML_CONFIG_RECOMMENDATIONS
                WHERE STATUS = 'PENDING'
            """
            result = execute_single(query)
            return result.get('value', 0) or 0, []

        elif metric_name == 'recall_rate':
            # This would need the ML evaluation logic
            return 0, []

    # ===== OPERATIONAL METRICS =====
    elif metric_source == 'operational':
        if metric_name == 'error_count':
            query = f"""
                SELECT COUNT(*) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE cs.ERROR_MESSAGE IS NOT NULL
                AND cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(query, params)
            return result.get('value', 0) or 0, []

        elif metric_name == 'call_volume':
            query = f"""
                SELECT COUNT(*) as value
                FROM CONVERSATION_SUMMARY cs
                WHERE cs.CONVERSATION_TIME > SYSDATE - :hours/24
                {product_filter}
            """
            result = execute_single(query, params)
            return result.get('value', 0) or 0, []

    return 0, []


def check_condition(value, operator, threshold):
    """Check if value meets the threshold condition"""
    if value is None:
        return False

    if operator == 'gt':
        return value > threshold
    elif operator == 'gte':
        return value >= threshold
    elif operator == 'lt':
        return value < threshold
    elif operator == 'lte':
        return value <= threshold
    elif operator == 'eq':
        return value == threshold
    return False


def evaluate_all_alerts():
    """
    Evaluate all enabled alert configurations
    Returns list of evaluation results
    """
    # Get all enabled configurations
    configs_query = """
        SELECT
            RAWTOHEX(ALERT_ID) as alert_id,
            ALERT_NAME,
            METRIC_SOURCE,
            METRIC_NAME,
            CONDITION_OPERATOR,
            THRESHOLD_VALUE,
            TIME_WINDOW_HOURS,
            FILTER_PRODUCT,
            SEVERITY
        FROM ALERT_CONFIGURATIONS
        WHERE IS_ENABLED = 1
    """
    configs = execute_query(configs_query)

    results = []
    for config in configs:
        alert_id = config['alert_id']
        metric_source = config['metric_source']
        metric_name = config['metric_name']
        operator = config['condition_operator']
        threshold = config['threshold_value']
        time_window = config['time_window_hours']
        filter_product = config.get('filter_product')
        severity = config['severity']

        # Evaluate the metric
        value, subscribers = evaluate_metric(
            metric_source, metric_name, time_window, filter_product
        )

        # Check if threshold is exceeded
        triggered = check_condition(value, operator, threshold)

        result = {
            'alert_id': alert_id,
            'alert_name': config['alert_name'],
            'metric_value': value,
            'threshold': threshold,
            'triggered': triggered
        }

        if triggered:
            # Check if there's already an active alert for this config
            existing_query = """
                SELECT COUNT(*) as count
                FROM ALERT_HISTORY
                WHERE ALERT_ID = HEXTORAW(:alert_id)
                AND STATUS = 'ACTIVE'
            """
            existing = execute_single(existing_query, {'alert_id': alert_id})

            if existing.get('count', 0) == 0:
                # Create new alert history record
                conn = None
                try:
                    conn = get_connection()
                    cursor = conn.cursor()

                    # Convert subscribers to JSON
                    subscribers_json = json.dumps(subscribers) if subscribers else '[]'

                    insert_query = """
                        INSERT INTO ALERT_HISTORY (
                            ALERT_ID, METRIC_VALUE, THRESHOLD_VALUE, SEVERITY,
                            AFFECTED_SUBSCRIBERS, AFFECTED_COUNT
                        ) VALUES (
                            HEXTORAW(:alert_id), :metric_value, :threshold,
                            :severity, :subscribers, :affected_count
                        )
                    """

                    cursor.execute(insert_query, {
                        'alert_id': alert_id,
                        'metric_value': value,
                        'threshold': threshold,
                        'severity': severity,
                        'subscribers': subscribers_json,
                        'affected_count': len(subscribers)
                    })

                    conn.commit()
                    result['created_alert'] = True

                except Exception as e:
                    print(f"Error creating alert history: {e}")
                    result['error'] = str(e)
                finally:
                    if conn:
                        conn.close()
            else:
                result['already_active'] = True

        results.append(result)

    return results
