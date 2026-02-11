"""
New Features Routes - Heatmap, Trends, Products, Agent Performance, Customer Journey
"""

import re
from collections import defaultdict
from flask import Blueprint, jsonify, request
from . import execute_query, execute_single, build_call_type_filter

new_features_bp = Blueprint('new_features', __name__)


def normalize_product(product):
    """Normalize product names - fix iPhone 7/12 to 17, convert Hebrew to English"""
    if not product:
        return None
    p = product.strip()

    # Skip junk entries
    junk_patterns = [
        r'^quantity\s*:\s*\d+',
        r'^n/?a$', r'^na$', r'^none$', r'^:$',
        r'^price(\s*:.*)?$', r'^name(\s*:.*)?$',
        r'^model(\s*:.*)?$', r'^color(\s*:.*)?$'
    ]
    for pattern in junk_patterns:
        if re.match(pattern, p.lower()):
            return None

    # Convert Hebrew iPhone variants to English
    p = re.sub(r'(איי?פו?ן|איי?פ?פו?ן)', 'iphone', p, flags=re.IGNORECASE)

    # Fix iPhone 7/12 to iPhone 17 (LLM confusion)
    p = re.sub(r'iphone\s*(7|12)(?!\d)', 'iphone 17', p, flags=re.IGNORECASE)

    # Clean up multiple spaces
    p = re.sub(r'\s{2,}', ' ', p).strip()

    return p if p else None


# ========================================
# 1. CALL VOLUME HEATMAP
# ========================================

@new_features_bp.route('/heatmap/call-volume')
def api_heatmap_call_volume():
    """
    Get call volume by hour-of-day (0-23) and day-of-week (0-6, Sun-Sat).
    Returns data for heatmap visualization.
    """
    days = request.args.get('days', 30, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            TO_CHAR(cs.CONVERSATION_TIME, 'D') - 1 as day_of_week,
            TO_NUMBER(TO_CHAR(cs.CONVERSATION_TIME, 'HH24')) as hour,
            COUNT(*) as count
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
        GROUP BY TO_CHAR(cs.CONVERSATION_TIME, 'D'), TO_CHAR(cs.CONVERSATION_TIME, 'HH24')
        ORDER BY day_of_week, hour
    """

    results = execute_query(query, {'days': days})

    # Convert to proper types
    data = []
    max_count = 0
    for row in results:
        count = row.get('count', 0) or 0
        data.append({
            'day_of_week': int(row.get('day_of_week', 0) or 0),
            'hour': int(row.get('hour', 0) or 0),
            'count': count
        })
        max_count = max(max_count, count)

    return jsonify({
        'data': data,
        'max_count': max_count,
        'days': days
    })


@new_features_bp.route('/heatmap/drill-down')
def api_heatmap_drilldown():
    """
    Get calls for a specific hour and day of week (drill-down from heatmap).
    """
    day_of_week = request.args.get('day_of_week', 0, type=int)  # 0=Sun, 6=Sat
    hour = request.args.get('hour', 0, type=int)  # 0-23
    days = request.args.get('days', 30, type=int)
    limit = request.args.get('limit', 50, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            cs.SOURCE_ID as call_id,
            cs.SOURCE_TYPE as type,
            TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
            cs.SUMMARY as summary,
            cs.SENTIMENT as sentiment,
            cs.SATISFACTION as satisfaction,
            ROUND(cs.CHURN_SCORE, 1) as churn_score
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        AND TO_CHAR(cs.CONVERSATION_TIME, 'D') - 1 = :day_of_week
        AND TO_NUMBER(TO_CHAR(cs.CONVERSATION_TIME, 'HH24')) = :hour
        {call_type_filter}
        ORDER BY cs.CONVERSATION_TIME DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {
        'days': days,
        'day_of_week': day_of_week,
        'hour': hour,
        'limit': limit
    })

    return jsonify(results)


# ========================================
# 2. TREND COMPARISONS (WEEK vs WEEK)
# ========================================

@new_features_bp.route('/trends/comparison')
def api_trends_comparison():
    """
    Compare metrics between current period and previous period.
    Returns totals, averages, and percentage deltas.
    """
    current_days = request.args.get('current_days', 7, type=int)
    compare_days = request.args.get('compare_days', 7, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs', ':days')

    # Current period stats
    current_query = f"""
        SELECT
            COUNT(*) as total_calls,
            ROUND(AVG(cs.SATISFACTION), 2) as avg_satisfaction,
            ROUND(AVG(cs.CHURN_SCORE), 1) as avg_churn_score,
            COUNT(CASE WHEN cs.SENTIMENT >= 4 THEN 1 END) as positive,
            COUNT(CASE WHEN cs.SENTIMENT <= 2 THEN 1 END) as negative,
            COUNT(CASE WHEN cs.SENTIMENT = 3 OR cs.SENTIMENT IS NULL THEN 1 END) as neutral,
            MIN(TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD')) as start_date,
            MAX(TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD')) as end_date
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        {call_type_filter}
    """
    current = execute_single(current_query, {'days': current_days})

    # Previous period stats - use different filter for BETWEEN clause
    call_type_filter_prev = build_call_type_filter(call_type, 'cs', ':current_days + :compare_days')

    previous_query = f"""
        SELECT
            COUNT(*) as total_calls,
            ROUND(AVG(cs.SATISFACTION), 2) as avg_satisfaction,
            ROUND(AVG(cs.CHURN_SCORE), 1) as avg_churn_score,
            COUNT(CASE WHEN cs.SENTIMENT >= 4 THEN 1 END) as positive,
            COUNT(CASE WHEN cs.SENTIMENT <= 2 THEN 1 END) as negative,
            COUNT(CASE WHEN cs.SENTIMENT = 3 OR cs.SENTIMENT IS NULL THEN 1 END) as neutral,
            MIN(TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD')) as start_date,
            MAX(TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD')) as end_date
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME BETWEEN SYSDATE - :current_days - :compare_days AND SYSDATE - :current_days
        {call_type_filter_prev}
    """
    previous = execute_single(previous_query, {
        'current_days': current_days,
        'compare_days': compare_days
    })

    def calc_delta(current_val, previous_val):
        """Calculate delta with direction"""
        current_val = current_val or 0
        previous_val = previous_val or 0
        if previous_val == 0:
            return {'value': current_val, 'percent': 100 if current_val > 0 else 0, 'direction': 'up' if current_val > 0 else 'same'}
        diff = current_val - previous_val
        pct = round((diff / previous_val) * 100, 1)
        return {
            'value': round(diff, 2),
            'percent': pct,
            'direction': 'up' if diff > 0 else ('down' if diff < 0 else 'same')
        }

    return jsonify({
        'current_period': {
            'start': current.get('start_date'),
            'end': current.get('end_date'),
            'total_calls': current.get('total_calls', 0) or 0,
            'avg_satisfaction': current.get('avg_satisfaction') or 0,
            'avg_churn_score': current.get('avg_churn_score') or 0,
            'sentiment_breakdown': {
                'positive': current.get('positive', 0) or 0,
                'negative': current.get('negative', 0) or 0,
                'neutral': current.get('neutral', 0) or 0
            }
        },
        'previous_period': {
            'start': previous.get('start_date'),
            'end': previous.get('end_date'),
            'total_calls': previous.get('total_calls', 0) or 0,
            'avg_satisfaction': previous.get('avg_satisfaction') or 0,
            'avg_churn_score': previous.get('avg_churn_score') or 0,
            'sentiment_breakdown': {
                'positive': previous.get('positive', 0) or 0,
                'negative': previous.get('negative', 0) or 0,
                'neutral': previous.get('neutral', 0) or 0
            }
        },
        'deltas': {
            'total_calls': calc_delta(current.get('total_calls'), previous.get('total_calls')),
            'avg_satisfaction': calc_delta(current.get('avg_satisfaction'), previous.get('avg_satisfaction')),
            'avg_churn_score': calc_delta(current.get('avg_churn_score'), previous.get('avg_churn_score')),
            'positive_sentiment': calc_delta(current.get('positive'), previous.get('positive'))
        }
    })


# ========================================
# 3. PRODUCTS BREAKDOWN BY DAY
# ========================================

@new_features_bp.route('/products/daily-breakdown')
def api_products_daily_breakdown():
    """
    Get products mentioned in calls, broken down by day.
    """
    days = request.args.get('days', 30, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            TO_CHAR(TRUNC(cs.CONVERSATION_TIME), 'YYYY-MM-DD') as call_date,
            cs.PRODUCTS as products_raw,
            COUNT(*) as count
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        AND cs.PRODUCTS IS NOT NULL
        AND TRIM(cs.PRODUCTS) IS NOT NULL
        {call_type_filter}
        GROUP BY TRUNC(cs.CONVERSATION_TIME), cs.PRODUCTS
        ORDER BY TRUNC(cs.CONVERSATION_TIME)
    """

    results = execute_query(query, {'days': days})

    # Parse and aggregate products by date
    dates_set = set()
    products_by_date = defaultdict(lambda: defaultdict(int))
    total_by_product = defaultdict(int)

    for row in results:
        call_date = row.get('call_date')
        products_raw = row.get('products_raw', '') or ''
        count = row.get('count', 0) or 0

        if not call_date or not products_raw:
            continue

        dates_set.add(call_date)

        # Parse products (comma-separated or single value)
        if ',' in products_raw:
            products = [p.strip() for p in products_raw.split(',') if p.strip()]
        else:
            products = [products_raw.strip()] if products_raw.strip() else []

        for product in products:
            normalized = normalize_product(product)
            if normalized:
                products_by_date[call_date][normalized] += count
                total_by_product[normalized] += count

    # Sort dates
    sorted_dates = sorted(dates_set)

    # Get top 5 products
    top_products = sorted(total_by_product.keys(), key=lambda x: total_by_product[x], reverse=True)[:5]

    # Build time series for each product
    products_data = {}
    for product in top_products:
        products_data[product] = [products_by_date[date].get(product, 0) for date in sorted_dates]

    return jsonify({
        'dates': sorted_dates,
        'products': products_data,
        'totals_by_product': dict(total_by_product)
    })


# ========================================
# 4. AGENT/QUEUE PERFORMANCE
# ========================================

@new_features_bp.route('/agent-performance')
def api_agent_performance():
    """
    Get performance metrics by product type.
    Shows avg satisfaction and churn risk by product.
    """
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 10, type=int)
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            cs.PRODUCTS as queue_name,
            COUNT(*) as call_count,
            ROUND(AVG(cs.SATISFACTION), 1) as avg_satisfaction,
            ROUND(AVG(cs.CHURN_SCORE), 0) as avg_churn_score
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.CONVERSATION_TIME > SYSDATE - :days
        AND cs.PRODUCTS IS NOT NULL
        AND TRIM(cs.PRODUCTS) IS NOT NULL
        {call_type_filter}
        GROUP BY cs.PRODUCTS
        ORDER BY call_count DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'days': days, 'limit': limit})

    # Add display_name (normalized) while keeping queue_name (original) for drill-down
    queues = []
    for row in results:
        queue_name = row.get('queue_name', '') or ''
        display_name = normalize_product(queue_name) or queue_name
        queues.append({
            'queue_name': queue_name,  # Original - for drill-down queries
            'display_name': display_name,  # Normalized - for chart display
            'call_count': row.get('call_count', 0),
            'avg_satisfaction': row.get('avg_satisfaction'),
            'avg_churn_score': row.get('avg_churn_score')
        })

    return jsonify({
        'queues': queues,
        'days': days
    })


@new_features_bp.route('/agent-performance/calls')
def api_agent_performance_calls():
    """
    Get calls for a specific product (drill-down from performance chart).
    """
    queue_name = request.args.get('queue_name', '')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 50, type=int)
    call_type = request.args.get('call_type', 'service')

    if not queue_name:
        return jsonify([])

    call_type_filter = build_call_type_filter(call_type, 'cs')

    # Handle 'Unknown' which means NULL products
    if queue_name == 'Unknown':
        query = f"""
            SELECT
                cs.SOURCE_ID as call_id,
                cs.SOURCE_TYPE as type,
                TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
                cs.SUMMARY as summary,
                cs.SENTIMENT as sentiment,
                cs.SATISFACTION as satisfaction,
                ROUND(cs.CHURN_SCORE, 1) as churn_score
            FROM CONVERSATION_SUMMARY cs
            WHERE cs.PRODUCTS IS NULL
            AND cs.CONVERSATION_TIME > SYSDATE - :days
            {call_type_filter}
            ORDER BY cs.CONVERSATION_TIME DESC
            FETCH FIRST :limit ROWS ONLY
        """
        results = execute_query(query, {'days': days, 'limit': limit})
    else:
        # Match exact product or product in comma-separated list
        query = f"""
            SELECT
                cs.SOURCE_ID as call_id,
                cs.SOURCE_TYPE as type,
                TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
                cs.SUMMARY as summary,
                cs.SENTIMENT as sentiment,
                cs.SATISFACTION as satisfaction,
                ROUND(cs.CHURN_SCORE, 1) as churn_score
            FROM CONVERSATION_SUMMARY cs
            WHERE cs.PRODUCTS = :product_name
            AND cs.CONVERSATION_TIME > SYSDATE - :days
            {call_type_filter}
            ORDER BY cs.CONVERSATION_TIME DESC
            FETCH FIRST :limit ROWS ONLY
        """
        results = execute_query(query, {
            'product_name': queue_name,
            'days': days,
            'limit': limit
        })

    return jsonify(results if results else [])


# ========================================
# 5. CUSTOMER JOURNEY TIMELINE
# ========================================

@new_features_bp.route('/customer-journey')
def api_customer_journey():
    """
    Get all interactions for a customer over time.
    Returns timeline suitable for visualization.
    """
    subscriber_no = request.args.get('subscriber_no', '')
    ban = request.args.get('ban', '')

    if not subscriber_no and not ban:
        return jsonify({'error': 'subscriber_no or ban is required'}), 400

    # Build condition based on provided params
    # Use AND when both available for exact subscriber match (same BAN can have multiple subs)
    if subscriber_no and ban:
        condition = "cs.SUBSCRIBER_NO || ' ' = :subscriber_no AND cs.BAN = :ban"
        sub_param = subscriber_no if subscriber_no.endswith(' ') else subscriber_no + ' '
        params = {'subscriber_no': sub_param, 'ban': ban}
    elif subscriber_no:
        condition = "cs.SUBSCRIBER_NO || ' ' = :subscriber_no"
        sub_param = subscriber_no if subscriber_no.endswith(' ') else subscriber_no + ' '
        params = {'subscriber_no': sub_param}
    else:
        condition = "cs.BAN = :ban"
        params = {'ban': ban}

    # Get customer interactions from CONVERSATION_SUMMARY only
    query = f"""
        SELECT
            cs.SOURCE_ID as source_id,
            cs.SOURCE_TYPE as source_type,
            TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI:SS') as call_date,
            cs.SENTIMENT as sentiment,
            ROUND(cs.CHURN_SCORE, 1) as churn_score,
            cs.SATISFACTION as satisfaction,
            SUBSTR(cs.SUMMARY, 1, 200) as summary,
            cs.PRODUCTS as products,
            cs.SUBSCRIBER_NO || ' ' as subscriber_no,
            cs.BAN as ban
        FROM CONVERSATION_SUMMARY cs
        WHERE {condition}
        AND cs.CONVERSATION_TIME > SYSDATE - 365
        ORDER BY cs.CONVERSATION_TIME DESC
        FETCH FIRST 50 ROWS ONLY
    """

    results = execute_query(query, params)

    # Get categories for each interaction
    if results:
        source_ids = [r['source_id'] for r in results if r.get('source_id')]
        if source_ids:
            # Build IN clause safely
            placeholders = ', '.join([f':id{i}' for i in range(len(source_ids))])
            cat_params = {f'id{i}': sid for i, sid in enumerate(source_ids)}

            cat_query = f"""
                SELECT SOURCE_ID, CATEGORY_CODE
                FROM CONVERSATION_CATEGORY
                WHERE SOURCE_ID IN ({placeholders})
            """
            categories = execute_query(cat_query, cat_params)

            # Map categories to source_ids
            cat_map = defaultdict(list)
            for cat in categories:
                cat_map[cat['source_id']].append(cat['category_code'])

            # Add categories to results
            for r in results:
                r['categories'] = cat_map.get(r['source_id'], [])

    # Get subscriber status from SUBSCRIBER table
    # Match pattern from dashboard.py - use subscriber_no as string
    status_info = None
    if subscriber_no or ban:
        if subscriber_no and ban:
            status_query = """
                SELECT SUB_STATUS, PRODUCT_CODE
                FROM SUBSCRIBER
                WHERE SUBSCRIBER_NO = :subscriber_no AND CUSTOMER_BAN = :ban
                FETCH FIRST 1 ROW ONLY
            """
            status_info = execute_single(status_query, {
                'subscriber_no': subscriber_no,
                'ban': ban
            })
        elif subscriber_no:
            status_query = """
                SELECT SUB_STATUS, PRODUCT_CODE
                FROM SUBSCRIBER
                WHERE SUBSCRIBER_NO = :subscriber_no
                FETCH FIRST 1 ROW ONLY
            """
            status_info = execute_single(status_query, {'subscriber_no': subscriber_no})
        else:
            status_query = """
                SELECT SUB_STATUS, PRODUCT_CODE
                FROM SUBSCRIBER
                WHERE CUSTOMER_BAN = :ban
                FETCH FIRST 1 ROW ONLY
            """
            status_info = execute_single(status_query, {'ban': ban})

    # Calculate average churn score if there are calls with churn scores
    avg_churn_score = None
    if results:
        churn_scores = [r.get('churn_score') for r in results if r.get('churn_score') is not None]
        if churn_scores:
            avg_churn_score = round(sum(churn_scores) / len(churn_scores), 1)

    return jsonify({
        'customer': {
            'subscriber_no': subscriber_no or (results[0].get('subscriber_no') if results else None),
            'ban': ban or (results[0].get('ban') if results else None),
            'status': status_info.get('sub_status') if status_info else 'Unknown',
            'product_code': status_info.get('product_code') if status_info else None,
            'total_interactions': len(results),
            'avg_churn_score': avg_churn_score
        },
        'timeline': results
    })


# ========================================
# CUSTOMER LOOKUP
# ========================================

@new_features_bp.route('/customer-lookup')
def api_customer_lookup():
    """
    Lookup customer by phone number or call ID.
    Returns customer info + recent calls preview.
    """
    search_type = request.args.get('type', 'phone')  # 'phone' or 'source_id'
    value = request.args.get('value', '').strip()

    if not value:
        return jsonify({'found': False, 'error': 'No search value provided'})

    subscriber_no = None
    ban = None

    if search_type == 'source_id':
        # Lookup by call/source ID first to get subscriber info
        call_query = """
            SELECT SUBSCRIBER_NO || ' ' as subscriber_no, BAN as ban
            FROM CONVERSATION_SUMMARY
            WHERE SOURCE_ID = :source_id
        """
        call_result = execute_single(call_query, {'source_id': value})
        if call_result:
            subscriber_no = call_result.get('subscriber_no')
            ban = call_result.get('ban')
        else:
            return jsonify({'found': False, 'error': 'Call ID not found'})
    else:
        # Search by phone number (subscriber_no)
        # Try to find any call with this subscriber
        phone_query = """
            SELECT SUBSCRIBER_NO || ' ' as subscriber_no, BAN as ban
            FROM CONVERSATION_SUMMARY
            WHERE SUBSCRIBER_NO || ' ' = :phone
            AND ROWNUM = 1
        """
        phone_result = execute_single(phone_query, {'phone': value + ' '})
        if phone_result:
            subscriber_no = phone_result.get('subscriber_no')
            ban = phone_result.get('ban')
        else:
            return jsonify({'found': False, 'error': 'No calls found for this phone number'})

    # Get recent calls for this customer
    # Use AND when both subscriber_no and ban are available for exact match
    if subscriber_no and ban:
        calls_query = """
            SELECT
                cs.SOURCE_ID as source_id,
                cs.SOURCE_TYPE as source_type,
                TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as call_date,
                cs.SENTIMENT as sentiment,
                ROUND(cs.CHURN_SCORE, 1) as churn_score,
                cs.SATISFACTION as satisfaction,
                SUBSTR(cs.SUMMARY, 1, 80) as summary
            FROM CONVERSATION_SUMMARY cs
            WHERE cs.SUBSCRIBER_NO || ' ' = :subscriber_no
            AND cs.BAN = :ban
            AND cs.CONVERSATION_TIME > SYSDATE - 365
            ORDER BY cs.CONVERSATION_TIME DESC
            FETCH FIRST 10 ROWS ONLY
        """
        calls = execute_query(calls_query, {'subscriber_no': subscriber_no, 'ban': ban})

        count_query = """
            SELECT COUNT(*) as total
            FROM CONVERSATION_SUMMARY
            WHERE SUBSCRIBER_NO || ' ' = :subscriber_no
            AND BAN = :ban
            AND CONVERSATION_TIME > SYSDATE - 365
        """
        count_result = execute_single(count_query, {'subscriber_no': subscriber_no, 'ban': ban})
    else:
        # Fallback to single field match
        calls_query = """
            SELECT
                cs.SOURCE_ID as source_id,
                cs.SOURCE_TYPE as source_type,
                TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as call_date,
                cs.SENTIMENT as sentiment,
                ROUND(cs.CHURN_SCORE, 1) as churn_score,
                cs.SATISFACTION as satisfaction,
                SUBSTR(cs.SUMMARY, 1, 80) as summary
            FROM CONVERSATION_SUMMARY cs
            WHERE (cs.SUBSCRIBER_NO || ' ' = :subscriber_no OR cs.BAN = :ban)
            AND cs.CONVERSATION_TIME > SYSDATE - 365
            ORDER BY cs.CONVERSATION_TIME DESC
            FETCH FIRST 10 ROWS ONLY
        """
        calls = execute_query(calls_query, {'subscriber_no': subscriber_no or '', 'ban': ban or ''})

        count_query = """
            SELECT COUNT(*) as total
            FROM CONVERSATION_SUMMARY
            WHERE (SUBSCRIBER_NO || ' ' = :subscriber_no OR BAN = :ban)
            AND CONVERSATION_TIME > SYSDATE - 365
        """
        count_result = execute_single(count_query, {'subscriber_no': subscriber_no or '', 'ban': ban or ''})
    total_interactions = count_result.get('total', 0) if count_result else 0

    # Get subscriber status from SUBSCRIBER table
    status_info = None
    if subscriber_no and ban:
        status_query = """
            SELECT SUB_STATUS, PRODUCT_CODE
            FROM SUBSCRIBER
            WHERE SUBSCRIBER_NO = :subscriber_no AND CUSTOMER_BAN = :ban
            FETCH FIRST 1 ROW ONLY
        """
        status_info = execute_single(status_query, {'subscriber_no': subscriber_no, 'ban': ban})

    return jsonify({
        'found': True,
        'customer': {
            'subscriber_no': subscriber_no,
            'ban': ban,
            'status': status_info.get('sub_status') if status_info else 'Unknown',
            'product_code': status_info.get('product_code') if status_info else None,
            'total_interactions': total_interactions
        },
        'recent_calls': calls
    })


# ========================================
# 6. QUEUE DISTRIBUTION (by QUEUE_NAME)
# ========================================

@new_features_bp.route('/queue-distribution')
def api_queue_distribution():
    """
    Group calls by QUEUE_NAME from VERINT_TEXT_ANALYSIS.
    Returns queue names with call counts and avg metrics.
    """
    days = request.args.get('days', 1, type=float)
    call_type = request.args.get('call_type', 'service')
    limit = request.args.get('limit', 15, type=int)

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            v.QUEUE_NAME as queue_name,
            COUNT(DISTINCT v.CALL_ID) as call_count,
            ROUND(AVG(cs.SATISFACTION), 1) as avg_satisfaction,
            ROUND(AVG(cs.CHURN_SCORE), 0) as avg_churn_score
        FROM VERINT_TEXT_ANALYSIS v
        LEFT JOIN CONVERSATION_SUMMARY cs ON v.CALL_ID = cs.SOURCE_ID
        WHERE v.CALL_TIME > SYSDATE - :days
        AND v.QUEUE_NAME IS NOT NULL
        AND TRIM(v.QUEUE_NAME) IS NOT NULL
        {call_type_filter}
        GROUP BY v.QUEUE_NAME
        ORDER BY call_count DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {'days': days, 'limit': limit})

    queues = [{
        'queue_name': row.get('queue_name', ''),
        'call_count': row.get('call_count', 0) or 0,
        'avg_satisfaction': row.get('avg_satisfaction'),
        'avg_churn_score': row.get('avg_churn_score') or 0
    } for row in results]

    return jsonify({
        'queues': queues,
        'days': days
    })


@new_features_bp.route('/queue-distribution/calls')
def api_queue_distribution_calls():
    """
    Get calls for a specific queue (drill-down from queue distribution chart).
    """
    queue_name = request.args.get('queue_name', '')
    days = request.args.get('days', 1, type=float)
    limit = request.args.get('limit', 100, type=int)
    call_type = request.args.get('call_type', 'service')

    if not queue_name:
        return jsonify([])

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        SELECT
            cs.SOURCE_ID as call_id,
            cs.SOURCE_TYPE as type,
            TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI') as created,
            cs.SENTIMENT as sentiment,
            cs.SATISFACTION as satisfaction,
            ROUND(cs.CHURN_SCORE, 1) as churn_score,
            cs.SUMMARY as summary
        FROM CONVERSATION_SUMMARY cs
        WHERE cs.SOURCE_ID IN (
            SELECT DISTINCT v.CALL_ID
            FROM VERINT_TEXT_ANALYSIS v
            WHERE v.QUEUE_NAME = :queue_name
            AND v.CALL_TIME > SYSDATE - :days
        )
        {call_type_filter}
        ORDER BY cs.CONVERSATION_TIME DESC
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {
        'queue_name': queue_name,
        'days': days,
        'limit': limit
    })

    return jsonify(results if results else [])


# ========================================
# 7. REPEAT CALLERS (subscribers calling multiple times)
# ========================================

@new_features_bp.route('/repeat-callers')
def api_repeat_callers():
    """
    Get subscribers grouped by call frequency in last 24h.
    Always uses fixed 24h window regardless of global time filter.
    """
    days = 1  # Fixed 24 hours
    call_type = request.args.get('call_type', 'service')

    call_type_filter = build_call_type_filter(call_type, 'cs')

    query = f"""
        WITH caller_counts AS (
            SELECT
                cs.SUBSCRIBER_NO,
                cs.BAN,
                COUNT(*) as call_count,
                MAX(cs.CHURN_SCORE) as max_churn_score,
                MAX(cs.CONVERSATION_TIME) as last_call
            FROM CONVERSATION_SUMMARY cs
            WHERE cs.CONVERSATION_TIME > SYSDATE - :days
            AND cs.SUBSCRIBER_NO IS NOT NULL
            AND TRIM(cs.SUBSCRIBER_NO) IS NOT NULL
            {call_type_filter}
            GROUP BY cs.SUBSCRIBER_NO, cs.BAN
        ),
        bucketed AS (
            SELECT
                CASE
                    WHEN call_count = 1 THEN 1
                    WHEN call_count = 2 THEN 2
                    WHEN call_count = 3 THEN 3
                    ELSE 4
                END as bucket_order,
                CASE
                    WHEN call_count = 1 THEN '1 call'
                    WHEN call_count = 2 THEN '2 calls'
                    WHEN call_count = 3 THEN '3 calls'
                    ELSE '4+ calls'
                END as frequency_bucket,
                max_churn_score
            FROM caller_counts
        )
        SELECT
            frequency_bucket,
            COUNT(*) as subscriber_count,
            SUM(CASE WHEN max_churn_score >= 70 THEN 1 ELSE 0 END) as high_risk_count
        FROM bucketed
        GROUP BY bucket_order, frequency_bucket
        ORDER BY bucket_order
    """

    results = execute_query(query, {'days': days})

    # Ensure all buckets exist (even with 0 count)
    bucket_map = {'1 call': 0, '2 calls': 1, '3 calls': 2, '4+ calls': 3}
    buckets = [
        {'label': '1 call', 'count': 0, 'high_risk': 0},
        {'label': '2 calls', 'count': 0, 'high_risk': 0},
        {'label': '3 calls', 'count': 0, 'high_risk': 0},
        {'label': '4+ calls', 'count': 0, 'high_risk': 0}
    ]

    total_subscribers = 0
    total_high_risk = 0

    for row in results:
        label = row.get('frequency_bucket', '')
        if label in bucket_map:
            idx = bucket_map[label]
            count = row.get('subscriber_count', 0) or 0
            high_risk = row.get('high_risk_count', 0) or 0
            buckets[idx]['count'] = count
            buckets[idx]['high_risk'] = high_risk
            total_subscribers += count
            total_high_risk += high_risk

    return jsonify({
        'buckets': buckets,
        'total_subscribers': total_subscribers,
        'total_high_risk': total_high_risk
    })


@new_features_bp.route('/repeat-callers/subscribers')
def api_repeat_callers_subscribers():
    """
    Get subscribers for a specific frequency bucket (drill-down).
    """
    min_calls = request.args.get('min_calls', 1, type=int)
    max_calls = request.args.get('max_calls', 999, type=int)
    days = 1  # Fixed 24 hours
    call_type = request.args.get('call_type', 'service')
    high_risk_only = request.args.get('high_risk_only', 'false') == 'true'
    limit = request.args.get('limit', 100, type=int)

    call_type_filter = build_call_type_filter(call_type, 'cs')
    high_risk_filter = "AND max_churn_score >= 70" if high_risk_only else ""

    query = f"""
        WITH caller_counts AS (
            SELECT
                cs.SUBSCRIBER_NO,
                cs.BAN,
                COUNT(*) as call_count,
                MAX(cs.CHURN_SCORE) as max_churn_score,
                MAX(TO_CHAR(cs.CONVERSATION_TIME, 'YYYY-MM-DD HH24:MI')) as last_call
            FROM CONVERSATION_SUMMARY cs
            WHERE cs.CONVERSATION_TIME > SYSDATE - :days
            AND cs.SUBSCRIBER_NO IS NOT NULL
            AND TRIM(cs.SUBSCRIBER_NO) IS NOT NULL
            {call_type_filter}
            GROUP BY cs.SUBSCRIBER_NO, cs.BAN
            HAVING COUNT(*) >= :min_calls AND COUNT(*) <= :max_calls
        )
        SELECT
            SUBSCRIBER_NO as subscriber_no,
            BAN as ban,
            call_count,
            max_churn_score,
            last_call
        FROM caller_counts
        WHERE 1=1 {high_risk_filter}
        ORDER BY call_count DESC, max_churn_score DESC NULLS LAST
        FETCH FIRST :limit ROWS ONLY
    """

    results = execute_query(query, {
        'days': days,
        'min_calls': min_calls,
        'max_calls': max_calls,
        'limit': limit
    })

    subscribers = [{
        'subscriber_no': row.get('subscriber_no', ''),
        'ban': row.get('ban', ''),
        'call_count': row.get('call_count', 0),
        'max_churn_score': row.get('max_churn_score'),
        'last_call': row.get('last_call', '')
    } for row in results]

    return jsonify({
        'subscribers': subscribers,
        'count': len(subscribers)
    })


# ========================================
# HEALTH CHECK
# ========================================

@new_features_bp.route('/health')
def api_health():
    """Health check endpoint"""
    from datetime import datetime
    from . import get_connection

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
