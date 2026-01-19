"""
Alerts Routes - Alert configurations, history, and subscriber drill-down
"""

import json
from math import ceil
from flask import Blueprint, jsonify, request
from . import execute_query, execute_single, get_connection

alerts_bp = Blueprint('alerts', __name__)


# ========================================
# ALERT CONFIGURATIONS CRUD
# ========================================

@alerts_bp.route('/configurations', methods=['GET'])
def get_configurations():
    """Get all alert configurations"""
    query = """
        SELECT
            RAWTOHEX(ALERT_ID) as alert_id,
            ALERT_NAME,
            ALERT_NAME_HE,
            ALERT_TYPE,
            METRIC_SOURCE,
            METRIC_NAME,
            CONDITION_OPERATOR,
            THRESHOLD_VALUE,
            TIME_WINDOW_HOURS,
            FILTER_PRODUCT,
            FILTER_SENTIMENT,
            SEVERITY,
            IS_ENABLED,
            DESCRIPTION,
            TO_CHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI') as created_at
        FROM ALERT_CONFIGURATIONS
        ORDER BY CREATED_AT DESC
    """
    results = execute_query(query)
    return jsonify(results)


@alerts_bp.route('/configurations', methods=['POST'])
def create_configuration():
    """Create a new alert configuration"""
    data = request.json

    required = ['alert_name', 'metric_source', 'metric_name', 'condition_operator', 'threshold_value']
    for field in required:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Use bind variables for security
        insert_query = """
            INSERT INTO ALERT_CONFIGURATIONS (
                ALERT_NAME, ALERT_NAME_HE, ALERT_TYPE, METRIC_SOURCE, METRIC_NAME,
                CONDITION_OPERATOR, THRESHOLD_VALUE, TIME_WINDOW_HOURS,
                FILTER_PRODUCT, FILTER_SENTIMENT, SEVERITY, DESCRIPTION
            ) VALUES (
                :alert_name, :alert_name_he, :alert_type, :metric_source, :metric_name,
                :condition_operator, :threshold_value, :time_window_hours,
                :filter_product, :filter_sentiment, :severity, :description
            )
        """

        cursor.execute(insert_query, {
            'alert_name': data.get('alert_name'),
            'alert_name_he': data.get('alert_name_he'),
            'alert_type': data.get('alert_type', 'threshold'),
            'metric_source': data.get('metric_source'),
            'metric_name': data.get('metric_name'),
            'condition_operator': data.get('condition_operator'),
            'threshold_value': data.get('threshold_value'),
            'time_window_hours': data.get('time_window_hours', 24),
            'filter_product': data.get('filter_product'),
            'filter_sentiment': data.get('filter_sentiment'),
            'severity': data.get('severity', 'WARNING'),
            'description': data.get('description')
        })

        conn.commit()
        return jsonify({'success': True, 'message': 'Alert configuration created'})

    except Exception as e:
        print(f"Error creating alert config: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@alerts_bp.route('/configurations/<alert_id>', methods=['PUT'])
def update_configuration(alert_id):
    """Update an existing alert configuration"""
    data = request.json

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        update_query = """
            UPDATE ALERT_CONFIGURATIONS SET
                ALERT_NAME = :alert_name,
                ALERT_NAME_HE = :alert_name_he,
                ALERT_TYPE = :alert_type,
                METRIC_SOURCE = :metric_source,
                METRIC_NAME = :metric_name,
                CONDITION_OPERATOR = :condition_operator,
                THRESHOLD_VALUE = :threshold_value,
                TIME_WINDOW_HOURS = :time_window_hours,
                FILTER_PRODUCT = :filter_product,
                FILTER_SENTIMENT = :filter_sentiment,
                SEVERITY = :severity,
                DESCRIPTION = :description,
                UPDATED_AT = SYSTIMESTAMP
            WHERE ALERT_ID = HEXTORAW(:alert_id)
        """

        cursor.execute(update_query, {
            'alert_id': alert_id,
            'alert_name': data.get('alert_name'),
            'alert_name_he': data.get('alert_name_he'),
            'alert_type': data.get('alert_type', 'threshold'),
            'metric_source': data.get('metric_source'),
            'metric_name': data.get('metric_name'),
            'condition_operator': data.get('condition_operator'),
            'threshold_value': data.get('threshold_value'),
            'time_window_hours': data.get('time_window_hours', 24),
            'filter_product': data.get('filter_product'),
            'filter_sentiment': data.get('filter_sentiment'),
            'severity': data.get('severity', 'WARNING'),
            'description': data.get('description')
        })

        conn.commit()
        return jsonify({'success': True, 'message': 'Alert configuration updated'})

    except Exception as e:
        print(f"Error updating alert config: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@alerts_bp.route('/configurations/<alert_id>', methods=['DELETE'])
def delete_configuration(alert_id):
    """Delete an alert configuration"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM ALERT_CONFIGURATIONS WHERE ALERT_ID = HEXTORAW(:alert_id)",
            {'alert_id': alert_id}
        )

        conn.commit()
        return jsonify({'success': True, 'message': 'Alert configuration deleted'})

    except Exception as e:
        print(f"Error deleting alert config: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@alerts_bp.route('/configurations/<alert_id>/toggle', methods=['POST'])
def toggle_configuration(alert_id):
    """Enable/disable an alert configuration"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Toggle the IS_ENABLED flag
        cursor.execute("""
            UPDATE ALERT_CONFIGURATIONS
            SET IS_ENABLED = CASE WHEN IS_ENABLED = 1 THEN 0 ELSE 1 END,
                UPDATED_AT = SYSTIMESTAMP
            WHERE ALERT_ID = HEXTORAW(:alert_id)
        """, {'alert_id': alert_id})

        # Get the new state
        cursor.execute(
            "SELECT IS_ENABLED FROM ALERT_CONFIGURATIONS WHERE ALERT_ID = HEXTORAW(:alert_id)",
            {'alert_id': alert_id}
        )
        row = cursor.fetchone()
        new_state = row[0] if row else None

        conn.commit()
        return jsonify({
            'success': True,
            'is_enabled': new_state == 1,
            'message': 'Alert enabled' if new_state == 1 else 'Alert disabled'
        })

    except Exception as e:
        print(f"Error toggling alert config: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


# ========================================
# ALERT HISTORY
# ========================================

@alerts_bp.route('/history', methods=['GET'])
def get_history():
    """Get alert history with optional filters"""
    days = request.args.get('days', 7, type=int)
    status = request.args.get('status')  # Optional: 'ACTIVE', 'ACKNOWLEDGED', 'RESOLVED'
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)

    # Build query with optional status filter
    status_filter = "AND h.STATUS = :status" if status else ""

    query = f"""
        SELECT
            RAWTOHEX(h.HISTORY_ID) as history_id,
            RAWTOHEX(h.ALERT_ID) as alert_id,
            c.ALERT_NAME,
            c.ALERT_NAME_HE,
            c.METRIC_SOURCE,
            c.FILTER_PRODUCT,
            h.METRIC_VALUE,
            h.THRESHOLD_VALUE,
            h.SEVERITY,
            h.STATUS,
            TO_CHAR(h.TRIGGERED_AT, 'YYYY-MM-DD HH24:MI') as triggered_at,
            h.AFFECTED_COUNT,
            h.ACKNOWLEDGED_BY,
            TO_CHAR(h.ACKNOWLEDGED_AT, 'YYYY-MM-DD HH24:MI') as acknowledged_at,
            h.RESOLVED_BY,
            TO_CHAR(h.RESOLVED_AT, 'YYYY-MM-DD HH24:MI') as resolved_at
        FROM ALERT_HISTORY h
        JOIN ALERT_CONFIGURATIONS c ON h.ALERT_ID = c.ALERT_ID
        WHERE h.TRIGGERED_AT > SYSDATE - :days
        {status_filter}
        ORDER BY h.TRIGGERED_AT DESC
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    params = {'days': days, 'limit': limit, 'offset': offset}
    if status:
        params['status'] = status

    results = execute_query(query, params)

    # Get total count
    count_query = f"""
        SELECT COUNT(*) as total
        FROM ALERT_HISTORY h
        WHERE h.TRIGGERED_AT > SYSDATE - :days
        {status_filter}
    """
    count_params = {'days': days}
    if status:
        count_params['status'] = status
    count_result = execute_single(count_query, count_params)

    return jsonify({
        'data': results,
        'total': count_result.get('total', 0),
        'offset': offset,
        'limit': limit
    })


@alerts_bp.route('/history/<history_id>/acknowledge', methods=['POST'])
def acknowledge_alert(history_id):
    """Acknowledge an active alert"""
    data = request.json or {}
    acknowledged_by = data.get('acknowledged_by', 'Dashboard User')

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE ALERT_HISTORY
            SET STATUS = 'ACKNOWLEDGED',
                ACKNOWLEDGED_BY = :acknowledged_by,
                ACKNOWLEDGED_AT = SYSTIMESTAMP
            WHERE HISTORY_ID = HEXTORAW(:history_id)
            AND STATUS = 'ACTIVE'
        """, {'history_id': history_id, 'acknowledged_by': acknowledged_by})

        conn.commit()
        return jsonify({'success': True, 'message': 'Alert acknowledged'})

    except Exception as e:
        print(f"Error acknowledging alert: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@alerts_bp.route('/history/<history_id>/resolve', methods=['POST'])
def resolve_alert(history_id):
    """Resolve an alert"""
    data = request.json or {}
    resolved_by = data.get('resolved_by', 'Dashboard User')
    resolution_notes = data.get('resolution_notes', '')

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE ALERT_HISTORY
            SET STATUS = 'RESOLVED',
                RESOLVED_BY = :resolved_by,
                RESOLVED_AT = SYSTIMESTAMP,
                RESOLUTION_NOTES = :resolution_notes
            WHERE HISTORY_ID = HEXTORAW(:history_id)
            AND STATUS IN ('ACTIVE', 'ACKNOWLEDGED')
        """, {
            'history_id': history_id,
            'resolved_by': resolved_by,
            'resolution_notes': resolution_notes
        })

        conn.commit()
        return jsonify({'success': True, 'message': 'Alert resolved'})

    except Exception as e:
        print(f"Error resolving alert: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@alerts_bp.route('/history/<history_id>/subscribers', methods=['GET'])
def get_affected_subscribers(history_id):
    """Get list of affected subscribers for an alert"""
    # First get the alert history record to retrieve the AFFECTED_SUBSCRIBERS CLOB
    query = """
        SELECT
            h.AFFECTED_SUBSCRIBERS,
            h.AFFECTED_COUNT,
            c.ALERT_NAME,
            c.METRIC_SOURCE,
            c.FILTER_PRODUCT
        FROM ALERT_HISTORY h
        JOIN ALERT_CONFIGURATIONS c ON h.ALERT_ID = c.ALERT_ID
        WHERE h.HISTORY_ID = HEXTORAW(:history_id)
    """

    result = execute_single(query, {'history_id': history_id})

    if not result:
        return jsonify({'error': 'Alert not found'}), 404

    subscribers = []
    if result.get('affected_subscribers'):
        try:
            # Parse the JSON array from the CLOB
            clob_data = result['affected_subscribers']
            # Handle LOB object
            if hasattr(clob_data, 'read'):
                clob_data = clob_data.read()
            subscribers = json.loads(clob_data) if clob_data else []
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error parsing affected_subscribers JSON: {e}")

    return jsonify({
        'alert_name': result.get('alert_name'),
        'metric_source': result.get('metric_source'),
        'filter_product': result.get('filter_product'),
        'affected_count': result.get('affected_count', 0),
        'subscribers': subscribers
    })


# ========================================
# ALERT SUMMARY & METRICS
# ========================================

@alerts_bp.route('/summary', methods=['GET'])
def get_summary():
    """Get alert summary for dashboard badges"""
    # Count active alerts
    active_query = """
        SELECT
            COUNT(*) as active_count,
            COUNT(CASE WHEN SEVERITY = 'CRITICAL' THEN 1 END) as critical_count
        FROM ALERT_HISTORY
        WHERE STATUS = 'ACTIVE'
    """
    active = execute_single(active_query)

    # Count enabled rules
    rules_query = """
        SELECT COUNT(*) as rules_count
        FROM ALERT_CONFIGURATIONS
        WHERE IS_ENABLED = 1
    """
    rules = execute_single(rules_query)

    # Count alerts in last 24 hours
    recent_query = """
        SELECT COUNT(*) as recent_count
        FROM ALERT_HISTORY
        WHERE TRIGGERED_AT > SYSDATE - 1
    """
    recent = execute_single(recent_query)

    return jsonify({
        'active_count': active.get('active_count', 0) or 0,
        'critical_count': active.get('critical_count', 0) or 0,
        'enabled_rules': rules.get('rules_count', 0) or 0,
        'alerts_24h': recent.get('recent_count', 0) or 0
    })


@alerts_bp.route('/available-metrics', methods=['GET'])
def get_available_metrics():
    """Get list of available metrics for alert configuration"""
    metrics = [
        # Churn metrics
        {'source': 'churn', 'name': 'high_risk_count', 'label': 'High Risk (70+)', 'type': 'count'},
        {'source': 'churn', 'name': 'critical_risk_count', 'label': 'Critical Risk (90+)', 'type': 'count'},
        {'source': 'churn', 'name': 'avg_churn_score', 'label': 'Avg Churn Score', 'type': 'average'},

        # Sentiment metrics
        {'source': 'sentiment', 'name': 'negative_percent', 'label': 'Negative %', 'type': 'percent'},
        {'source': 'sentiment', 'name': 'negative_count', 'label': 'Negative Count', 'type': 'count'},
        {'source': 'sentiment', 'name': 'positive_percent', 'label': 'Positive %', 'type': 'percent'},

        # Satisfaction metrics
        {'source': 'satisfaction', 'name': 'avg_satisfaction', 'label': 'Avg Satisfaction', 'type': 'average'},
        {'source': 'satisfaction', 'name': 'low_satisfaction_count', 'label': 'Low Satisfaction (<3)', 'type': 'count'},

        # ML Quality metrics
        {'source': 'ml_quality', 'name': 'pending_count', 'label': 'ML Pending', 'type': 'count'},

        # Operational metrics
        {'source': 'operational', 'name': 'call_volume', 'label': 'Call Volume', 'type': 'count'},
        {'source': 'operational', 'name': 'error_count', 'label': 'Errors', 'type': 'count'},
    ]

    return jsonify(metrics)


# ========================================
# MANUAL ALERT EVALUATION (for testing)
# ========================================

@alerts_bp.route('/evaluate', methods=['POST'])
def evaluate_alerts():
    """Manually trigger alert evaluation for all enabled rules"""
    from .alert_evaluator import evaluate_all_alerts

    try:
        results = evaluate_all_alerts()
        return jsonify({
            'success': True,
            'evaluated': len(results),
            'triggered': sum(1 for r in results if r.get('triggered')),
            'results': results
        })
    except Exception as e:
        print(f"Error evaluating alerts: {e}")
        return jsonify({'error': str(e)}), 500
