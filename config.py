import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

load_dotenv()

# ============================
# Oracle Configuration
# ============================
ORACLE_CONFIG = {
    'user': os.getenv('ORACLE_USER', 'your_user'),
    'password': os.getenv('ORACLE_PASSWORD', 'your_password'),
    'host': os.getenv('ORACLE_HOST', 'localhost'),
    'port': int(os.getenv('ORACLE_PORT', 1521)),
    'service_name': os.getenv('ORACLE_SERVICE_NAME', 'XE'),
    'schema': 'rtbi'
}

# ============================
# AWS SQS Configuration
# ============================
AWS_CONFIG = {
    'region_name': os.getenv('AWS_REGION', 'eu-west-1'),
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    'aws_session_token': os.getenv('AWS_SESSION_TOKEN'),  # For temporary credentials
}

# Outbound queue: CDC sends conversations TO ML service
SQS_OUTBOUND_QUEUE_URL = os.getenv(
    'SQS_OUTBOUND_QUEUE_URL',
    'https://sqs.eu-west-1.amazonaws.com/320708867194/summary-pipe-queue'
)

# Inbound queue: CDC receives ML results FROM ML service (only successful summaries)
SQS_INBOUND_QUEUE_URL = os.getenv(
    'SQS_INBOUND_QUEUE_URL',
    'https://sqs.eu-west-1.amazonaws.com/320708867194/summary-pipe-complete'
)

# Backward compatibility - default to outbound queue
SQS_QUEUE_URL = SQS_OUTBOUND_QUEUE_URL

# ============================
# CDC Configuration
# ============================
CDC_CONFIG = {
    'normal_mode_enabled': True,
    'normal_mode_minutes': 10,
    'normal_poll_interval_seconds': 10,
    'historical_mode_enabled': False,
    'historical_start_date': '2024-01-01',
    'historical_batch_size': 50,
    'max_batch_size': 50,
    'max_concurrent_calls': 10,
    'message_visibility_timeout': 600,
    'max_retries': 3,
    'retry_delay_seconds': 5,
}

# ============================
# Message Types
# ============================
MESSAGE_TYPES = {
    'CONVERSATION_TO_ML': 'CONVERSATION_ASSEMBLY',
    'ML_RESULT': 'ML_PROCESSING_RESULT',
}

# ============================
# Enhanced Logging Configuration
# ============================
LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'log_dir': os.getenv('LOG_DIR', './logs'),
    'max_bytes': 50 * 1024 * 1024,  # 50MB per file
    'backup_count': 10,  # Keep 10 backup files
    'format': '%(asctime)s - [%(levelname)s] - %(name)s - %(funcName)s:%(lineno)d - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',

    # Separate log files
    'files': {
        'main': 'cdc_service.log',
        'oracle': 'cdc_oracle.log',
        'sqs': 'cdc_sqs.log',
        'errors': 'cdc_errors.log',
        'performance': 'cdc_performance.log',
    }
}

# ============================
# Table Validation Configuration
# ============================
REQUIRED_TABLES = [
    'CDC_PROCESSING_STATUS',
    'CDC_PROCESSED_CALLS',
    'CDC_PROCESSING_LOG',
    'ERROR_LOG',
    'SQS_PERMANENT_FAILURES',
    'DICTA_CALL_SUMMARY',
]

SOURCE_TABLE = 'VERINT_TEXT_ANALYSIS'
SOURCE_SCHEMA = 'call_analytics'  # Using call_analytics schema instead of rtbi for XE compatibility

# ============================
# Multi-Source Table Configuration
# ============================
TABLE_SOURCES = {
    'verint': {
        'table_name': 'VERINT_TEXT_ANALYSIS',
        'id_column': 'CALL_ID',
        'time_column': 'CALL_TIME',
        'text_time_column': 'CALL_TIME',
        'valid_channels': {'A', 'C'},  # Agent and Customer
        'required_channels': {'A', 'C'},  # Both Agent and Customer must be present
        'index_hint': '/*+ index (VERINT_TEXT_ANALYSIS VERINT_TEXT_ANALYSIS_3ix) */',
        'base_filter': '',
        'time_filter': 'CALL_TIME > SYSDATE - (120 / 1440)',
        'cdc_mode_key': 'CDC_NORMAL_MODE',
        'min_segments': 11,
        'enabled': True,
        'dest_source_type': 'CALL',  # For CONVERSATION_SUMMARY.source_type
    },
    'sf_oc': {
        'table_name': 'SF_OC_TEXT_ANALYSIS_TEMP',
        'id_column': 'CASE_ID',
        'time_column': 'CASE_DATE',
        'text_time_column': 'MESSAGE_DATE',
        'valid_channels': {'A', 'B', 'C'},  # Agent, Bot, and Customer - any combination allowed
        'required_channels': {'C'},  # Customer must always be present
        'index_hint': '',
        'base_filter': "channel_code <> 2 AND last_run_date > trunc(sysdate) AND CHANNEL_DESC = 'WhatsApp' AND TEXT IS NOT NULL",
        'time_filter': 'case_date > trunc(sysdate) - 7',
        'cdc_mode_key': 'CDC_NORMAL_MODE_SF_OC',
        'min_segments': 5,
        'enabled': True,
        'dest_source_type': 'WAPP',  # For CONVERSATION_SUMMARY.source_type (WhatsApp)
    }
}


# ============================
# Setup Logging System
# ============================
def setup_logging():
    """Initialize comprehensive logging system with multiple handlers"""
    import os

    # Create log directory
    log_dir = LOGGING_CONFIG['log_dir']
    os.makedirs(log_dir, exist_ok=True)

    # Base formatter
    formatter = logging.Formatter(
        LOGGING_CONFIG['format'],
        datefmt=LOGGING_CONFIG['date_format']
    )

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, LOGGING_CONFIG['level']))

    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Main rotating file handler
    main_log = os.path.join(log_dir, LOGGING_CONFIG['files']['main'])
    main_handler = RotatingFileHandler(
        main_log,
        maxBytes=LOGGING_CONFIG['max_bytes'],
        backupCount=LOGGING_CONFIG['backup_count']
    )
    main_handler.setLevel(logging.DEBUG)
    main_handler.setFormatter(formatter)
    root_logger.addHandler(main_handler)

    # Error-only log file
    error_log = os.path.join(log_dir, LOGGING_CONFIG['files']['errors'])
    error_handler = RotatingFileHandler(
        error_log,
        maxBytes=LOGGING_CONFIG['max_bytes'],
        backupCount=LOGGING_CONFIG['backup_count']
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)

    # Oracle-specific logger
    oracle_logger = logging.getLogger('oracle')
    oracle_log = os.path.join(log_dir, LOGGING_CONFIG['files']['oracle'])
    oracle_handler = RotatingFileHandler(
        oracle_log,
        maxBytes=LOGGING_CONFIG['max_bytes'],
        backupCount=LOGGING_CONFIG['backup_count']
    )
    oracle_handler.setFormatter(formatter)
    oracle_logger.addHandler(oracle_handler)
    oracle_logger.setLevel(logging.DEBUG)

    # SQS-specific logger
    sqs_logger = logging.getLogger('sqs')
    sqs_log = os.path.join(log_dir, LOGGING_CONFIG['files']['sqs'])
    sqs_handler = RotatingFileHandler(
        sqs_log,
        maxBytes=LOGGING_CONFIG['max_bytes'],
        backupCount=LOGGING_CONFIG['backup_count']
    )
    sqs_handler.setFormatter(formatter)
    sqs_logger.addHandler(sqs_handler)
    sqs_logger.setLevel(logging.DEBUG)

    # Performance logger
    perf_logger = logging.getLogger('performance')
    perf_log = os.path.join(log_dir, LOGGING_CONFIG['files']['performance'])
    perf_handler = RotatingFileHandler(
        perf_log,
        maxBytes=LOGGING_CONFIG['max_bytes'],
        backupCount=LOGGING_CONFIG['backup_count']
    )
    perf_handler.setFormatter(formatter)
    perf_logger.addHandler(perf_handler)
    perf_logger.setLevel(logging.INFO)

    # Silence boto3 debug logs
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    logging.info(f"✅ Logging system initialized - Log directory: {log_dir}")
    return root_logger
