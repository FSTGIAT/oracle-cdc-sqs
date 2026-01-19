#!/usr/bin/env python3
"""
Alert Evaluation Service
Runs periodically (via cron/PM2) to evaluate alert conditions and trigger alerts

Usage:
    python alert_evaluation_service.py

PM2 Configuration (ecosystem.config.js):
    {
        name: 'alert-evaluator',
        script: 'alert_evaluation_service.py',
        interpreter: 'python3',
        cron_restart: '*/5 * * * *',  // Every 5 minutes
        autorestart: false
    }
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables
from dotenv import load_dotenv
ENV_PATH = Path(__file__).parent / '.env'
load_dotenv(ENV_PATH)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / 'logs' / 'alert_evaluation.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Import the evaluator module
from routes.alert_evaluator import evaluate_all_alerts


def ensure_log_directory():
    """Ensure the logs directory exists"""
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)


def main():
    """Main entry point for alert evaluation"""
    ensure_log_directory()

    logger.info("=" * 50)
    logger.info("Alert Evaluation Service Started")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 50)

    try:
        # Run evaluation
        results = evaluate_all_alerts()

        # Log results
        evaluated_count = len(results)
        triggered_count = sum(1 for r in results if r.get('triggered'))
        created_count = sum(1 for r in results if r.get('created_alert'))

        logger.info(f"Evaluation complete:")
        logger.info(f"  - Rules evaluated: {evaluated_count}")
        logger.info(f"  - Conditions triggered: {triggered_count}")
        logger.info(f"  - New alerts created: {created_count}")

        # Log each triggered alert
        for result in results:
            if result.get('triggered'):
                status = 'NEW' if result.get('created_alert') else 'EXISTING'
                logger.info(
                    f"  [{status}] {result['alert_name']}: "
                    f"value={result['metric_value']}, threshold={result['threshold']}"
                )

        # Write summary to a status file for monitoring
        status_file = Path(__file__).parent / 'logs' / 'alert_evaluation_status.json'
        with open(status_file, 'w') as f:
            json.dump({
                'last_run': datetime.now().isoformat(),
                'rules_evaluated': evaluated_count,
                'conditions_triggered': triggered_count,
                'alerts_created': created_count,
                'status': 'success'
            }, f, indent=2)

        logger.info("Alert evaluation completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Alert evaluation failed: {e}", exc_info=True)

        # Write error status
        status_file = Path(__file__).parent / 'logs' / 'alert_evaluation_status.json'
        with open(status_file, 'w') as f:
            json.dump({
                'last_run': datetime.now().isoformat(),
                'status': 'error',
                'error': str(e)
            }, f, indent=2)

        return 1


if __name__ == '__main__':
    sys.exit(main())
