"""
ML Quality Routes - Recommendations, history, approve, apply-to-ml, reject, feedback, metrics
"""

import os
import json
import logging
from datetime import datetime
from flask import Blueprint, jsonify, request
from . import execute_query, execute_single, get_connection

logger = logging.getLogger(__name__)

ml_quality_bp = Blueprint('ml_quality', __name__)

# AWS Configuration
S3_BUCKET = os.getenv('ML_CONFIG_S3_BUCKET', 'pelephone-ml-configs')
SQS_QUEUE = os.getenv('ML_CONFIG_SQS_QUEUE',
    'https://sqs.eu-west-1.amazonaws.com/320708867194/ml-config-updates')

# Lazy-loaded AWS clients
_s3_client = None
_sqs_client = None


def get_s3_client():
    """Get or create S3 client."""
    global _s3_client
    if _s3_client is None:
        import boto3
        _s3_client = boto3.client('s3', region_name='eu-west-1')
    return _s3_client


def get_sqs_client():
    """Get or create SQS client."""
    global _sqs_client
    if _sqs_client is None:
        import boto3
        _sqs_client = boto3.client('sqs', region_name='eu-west-1')
    return _sqs_client


@ml_quality_bp.route('/recommendations')
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


@ml_quality_bp.route('/history')
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


@ml_quality_bp.route('/approve', methods=['POST'])
def api_ml_approve():
    """
    Approve a recommendation - uploads config to S3 but does NOT trigger ML reload.
    Human must separately click "Apply to ML" to trigger reload.
    """
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
        rec_details = json.loads(rec['rec_details']) if isinstance(rec['rec_details'], str) else rec['rec_details']
        rec_type = rec['rec_type']

        # Apply changes to S3 config ONLY (no SQS trigger yet!)
        s3 = get_s3_client()

        if rec_type == 'churn_keywords':
            # Download current keywords config
            obj = s3.get_object(Bucket=S3_BUCKET, Key='configs/classification-keywords.json')
            config = json.loads(obj['Body'].read().decode('utf-8'))

            # Add new keywords to appropriate category
            new_keywords = rec_details.get('keywords', [])
            existing_medium = set(config.get('churn_keywords', {}).get('medium', []))
            config['churn_keywords']['medium'] = list(existing_medium | set(new_keywords))

            # Upload updated config
            s3.put_object(
                Bucket=S3_BUCKET,
                Key='configs/classification-keywords.json',
                Body=json.dumps(config, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Added {len(new_keywords)} new churn keywords to S3")

        elif rec_type == 'churn_threshold':
            # Download current classifications config
            obj = s3.get_object(Bucket=S3_BUCKET, Key='configs/call-classifications.json')
            config = json.loads(obj['Body'].read().decode('utf-8'))

            # Update threshold
            new_threshold = rec_details.get('recommended_value', 40)
            if 'churn_detection' in config:
                config['churn_detection']['threshold'] = new_threshold / 100.0

            # Upload updated config
            s3.put_object(
                Bucket=S3_BUCKET,
                Key='configs/call-classifications.json',
                Body=json.dumps(config, ensure_ascii=False, indent=2),
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


@ml_quality_bp.route('/apply-to-ml', methods=['POST'])
def api_ml_apply():
    """
    MANUAL TRIGGER - Send SQS message to tell ML service to download configs from S3.
    This gives human full control over WHEN ML service picks up new configs.
    """
    data = request.json
    triggered_by = data.get('triggered_by', 'dashboard_user')

    try:
        sqs = get_sqs_client()

        # Send SQS notification to ML service
        sqs.send_message(
            QueueUrl=SQS_QUEUE,
            MessageBody=json.dumps({
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


@ml_quality_bp.route('/reject', methods=['POST'])
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


@ml_quality_bp.route('/feedback', methods=['POST'])
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


@ml_quality_bp.route('/metrics')
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
