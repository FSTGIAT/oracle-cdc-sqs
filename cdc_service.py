import oracledb
import boto3
import json
import time
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from functools import wraps
from config import (
    ORACLE_CONFIG, AWS_CONFIG,
    SQS_OUTBOUND_QUEUE_URL, SQS_INBOUND_QUEUE_URL,
    CDC_CONFIG, MESSAGE_TYPES, REQUIRED_TABLES,
    SOURCE_TABLE, SOURCE_SCHEMA, setup_logging
)

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)
oracle_logger = logging.getLogger('oracle')
sqs_logger = logging.getLogger('sqs')
perf_logger = logging.getLogger('performance')


# ============================
# Decorators for Enhanced Logging
# ============================

def extract_action_items_text(value, max_length: int = 500) -> str:
    """
    Extract only the actual action text from action_items, ignoring metadata.

    Input can be:
        - JSON string: '[{"action": "...", "due_date": "..."}, ...]'
        - List of dicts: [{"name": "...", "instructions": "..."}]
        - List of strings: ["action 1", "action 2"]
        - Simple string

    Output: Clean comma-separated action texts, truncated to max_length.

    Extracts text from these fields (priority order):
        action, description, name, instructions, task, item, text
    Ignores: due_date, time, priority, status, assignee, etc.
    """
    if not value:
        return ''

    # Text fields to extract (in priority order)
    TEXT_FIELDS = ['action', 'description', 'name', 'instructions', 'task', 'item', 'text']

    def extract_text_from_dict(d: dict) -> str:
        """Extract action text from a dict, trying multiple field names."""
        if not isinstance(d, dict):
            return str(d).strip() if d else ''

        # Try each text field in priority order
        for field in TEXT_FIELDS:
            if field in d and d[field]:
                text = str(d[field]).strip()
                if text and text.lower() != 'none':
                    return text

        # Fallback: if no known field, return empty (don't include metadata)
        return ''

    action_texts = []

    # Parse JSON string if needed
    if isinstance(value, str):
        value = value.strip()
        try:
            parsed = json.loads(value)
            value = parsed
        except (json.JSONDecodeError, TypeError):
            # Not valid JSON - treat as plain text
            # Clean brackets and quotes
            cleaned = value
            for char in ['[', ']', '{', '}', '"', "'"]:
                cleaned = cleaned.replace(char, '')
            return cleaned[:max_length].strip()

    # Handle list of items
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                text = extract_text_from_dict(item)
                if text:
                    action_texts.append(text)
            elif item and str(item).strip() and str(item).lower() != 'none':
                action_texts.append(str(item).strip())

    # Handle single dict
    elif isinstance(value, dict):
        text = extract_text_from_dict(value)
        if text:
            action_texts.append(text)

    # Join and truncate
    result = ', '.join(action_texts)

    # Truncate to max_length (cut at last comma before limit if possible)
    if len(result) > max_length:
        result = result[:max_length]
        # Try to cut at last complete item
        last_comma = result.rfind(',')
        if last_comma > max_length * 0.5:  # Only if we keep at least half
            result = result[:last_comma]
        result = result.rstrip(', ')

    return result


def clean_json_to_csv(value) -> str:
    """
    Convert JSON array/object to clean comma-separated string.
    Removes [], {}, "", '' and returns comma-separated values.
    Examples:
        '["a", "b", "c"]' -> 'a, b, c'
        '{"key": "value"}' -> 'key: value'
        ['a', 'b'] -> 'a, b'
    """
    if not value:
        return ''

    # If it's already a list, join it
    if isinstance(value, list):
        # Clean each item from quotes and brackets
        cleaned_items = []
        for item in value:
            if item:
                item_str = str(item)
                # Remove brackets and quotes from each item
                for char in ['[', ']', '{', '}', '"', "'"]:
                    item_str = item_str.replace(char, '')
                item_str = item_str.strip()
                if item_str:
                    cleaned_items.append(item_str)
        return ', '.join(cleaned_items)

    # If it's a dict, format as key: value pairs
    if isinstance(value, dict):
        parts = []
        for k, v in value.items():
            if v:
                v_str = str(v)
                for char in ['[', ']', '{', '}', '"', "'"]:
                    v_str = v_str.replace(char, '')
                parts.append(f"{k}: {v_str.strip()}")
        return ', '.join(parts)

    # If it's a string, try to parse as JSON
    if isinstance(value, str):
        value = value.strip()

        # Try to parse as JSON
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                cleaned_items = []
                for item in parsed:
                    if item:
                        item_str = str(item)
                        for char in ['[', ']', '{', '}', '"', "'"]:
                            item_str = item_str.replace(char, '')
                        item_str = item_str.strip()
                        if item_str:
                            cleaned_items.append(item_str)
                return ', '.join(cleaned_items)
            if isinstance(parsed, dict):
                parts = []
                for k, v in parsed.items():
                    if v:
                        v_str = str(v)
                        for char in ['[', ']', '{', '}', '"', "'"]:
                            v_str = v_str.replace(char, '')
                        parts.append(f"{k}: {v_str.strip()}")
                return ', '.join(parts)
        except (json.JSONDecodeError, TypeError):
            pass

        # If not valid JSON, just clean the string manually
        # Remove [], {}, "", ''
        cleaned = value
        for char in ['[', ']', '{', '}', '"', "'"]:
            cleaned = cleaned.replace(char, '')

        # Clean up extra spaces and commas
        cleaned = ', '.join(part.strip() for part in cleaned.split(',') if part.strip())
        return cleaned

    return str(value)


def log_function_call(func):
    """Decorator to log function entry, exit, and timing"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        logger.debug(f"? Entering {func_name}")

        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = (time.time() - start_time) * 1000  # ms

            # Log to performance logger
            perf_logger.info(f"{func_name} completed in {elapsed:.2f}ms")

            logger.debug(f"? Exiting {func_name} | Time: {elapsed:.2f}ms")
            return result

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            logger.error(f"? Exception in {func_name} after {elapsed:.2f}ms: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    return wrapper


# ============================
# Oracle CDC Service
# ============================

class OracleCDCService:
    """
    On-Premises Oracle CDC Service with Comprehensive Logging
    """

    def __init__(self):
        logger.info("="*80)
        logger.info("? Initializing Oracle CDC Service")
        logger.info("="*80)

        self.oracle_conn = None
        self.sqs_client = None
        self.is_running = False
        self.tables_validated = False
        self.startup_time = datetime.utcnow()

        # Statistics
        self.stats = {
            'total_calls_processed': 0,
            'total_calls_failed': 0,
            'total_sqs_sent': 0,
            'total_sqs_failed': 0,
            'total_ml_results_received': 0,
            'total_ml_results_written': 0,
            'last_cycle_time': None,
            'cycles_completed': 0,
        }

        logger.info(f"? Statistics initialized")

    # ============================
    # Connection Management
    # ============================

    @log_function_call
    def connect_oracle(self) -> bool:
        """Establish Oracle connection with detailed logging"""
        oracle_logger.info("? Attempting Oracle connection...")
        oracle_logger.info(f"   Host: {ORACLE_CONFIG['host']}")
        oracle_logger.info(f"   Port: {ORACLE_CONFIG['port']}")
        oracle_logger.info(f"   Service: {ORACLE_CONFIG['service_name']}")
        oracle_logger.info(f"   User: {ORACLE_CONFIG['user']}")
        oracle_logger.info(f"   Schema: {ORACLE_CONFIG['schema']}")

        try:
            dsn = oracledb.makedsn(
                ORACLE_CONFIG['host'],
                ORACLE_CONFIG['port'],
                service_name=ORACLE_CONFIG['service_name']
            )
            oracle_logger.debug(f"DSN created: {dsn}")

            self.oracle_conn = oracledb.connect(
                user=ORACLE_CONFIG['user'],
                password=ORACLE_CONFIG['password'],
                dsn=dsn
            )

            # Test connection
            cursor = self.oracle_conn.cursor()
            cursor.execute("SELECT SYSDATE FROM dual")
            db_time = cursor.fetchone()[0]
            cursor.close()

            oracle_logger.info(f"? Oracle connected successfully")
            oracle_logger.info(f"   Database time: {db_time}")
            oracle_logger.info(f"   Oracle version: {self.oracle_conn.version}")

            return True

        except oracledb.DatabaseError as e:
            error_obj, = e.args
            oracle_logger.error(f"? Oracle DatabaseError:")
            oracle_logger.error(f"   Code: {error_obj.code}")
            oracle_logger.error(f"   Message: {error_obj.message}")
            oracle_logger.error(f"   Context: {error_obj.context}")
            return False

        except Exception as e:
            oracle_logger.error(f"? Failed to connect to Oracle: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            return False

    @log_function_call
    def connect_sqs(self) -> bool:
        """Establish AWS SQS connection with detailed logging"""
        sqs_logger.info("? Attempting SQS connection...")
        sqs_logger.info(f"   Region: {AWS_CONFIG['region_name']}")
        sqs_logger.info(f"   Outbound Queue: {SQS_OUTBOUND_QUEUE_URL}")
        sqs_logger.info(f"   Inbound Queue: {SQS_INBOUND_QUEUE_URL}")


        try:
            # Build SQS client config
            sqs_config = {
                'region_name': AWS_CONFIG['region_name'],
                'aws_access_key_id': AWS_CONFIG['aws_access_key_id'],
                'aws_secret_access_key': AWS_CONFIG['aws_secret_access_key']
            }

            # Add session token if provided (for temporary credentials)
            #if AWS_CONFIG.get('aws_session_token'):
            #    sqs_config['aws_session_token'] = AWS_CONFIG['aws_session_token']

            self.sqs_client = boto3.client('sqs', **sqs_config)

            # Test connection and get queue attributes
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=SQS_OUTBOUND_QUEUE_URL,
                AttributeNames=['All']
            )


            attrs = response.get('Attributes', {})
            sqs_logger.info(f"? SQS Outbound connected successfully")

            sqs_logger.info(f"? SQS connected successfully")
            sqs_logger.info(f"   Queue ARN: {attrs.get('QueueArn', 'N/A')}")
            sqs_logger.info(f"   Messages available: {attrs.get('ApproximateNumberOfMessages', '0')}")
            sqs_logger.info(f"   Messages in flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', '0')}")
            sqs_logger.info(f"   Visibility timeout: {attrs.get('VisibilityTimeout', 'N/A')}s")

            response_inbound = self.sqs_client.get_queue_attributes(

                QueueUrl=SQS_INBOUND_QUEUE_URL,  # ? Changed

                AttributeNames=['All']

            )

            attrs_inbound = response_inbound.get('Attributes',{})
            sqs_logger.info(f"? SQS Inbound connected successfully")


            return True

        except Exception as e:
            sqs_logger.error(f"? Failed to connect to SQS: {e}")
            sqs_logger.error(f"   Traceback: {traceback.format_exc()}")
            return False

    # ============================
    # Table Validation
    # ============================

    @log_function_call
    def validate_tables(self) -> bool:
        """
        Validate all required tables exist
        Returns True if all tables exist, False otherwise
        """
        oracle_logger.info("? Validating required tables...")

        cursor = self.oracle_conn.cursor()
        all_valid = True

        try:
            # Check CDC tables (in current user schema)
            oracle_logger.info(f"? Checking CDC tables in current schema:")
            for table_name in REQUIRED_TABLES:
                exists = self._check_table_exists(cursor, table_name, None)

                if exists:
                    row_count = self._get_table_row_count(cursor, table_name, None)
                    oracle_logger.info(f"   ? {table_name} exists ({row_count} rows)")
                else:
                    oracle_logger.error(f"   ? {table_name} MISSING")
                    all_valid = True

            # Check source table in rtbi schema
            oracle_logger.info(f"? Checking source table:")
            exists = self._check_table_exists(cursor, SOURCE_TABLE, SOURCE_SCHEMA)

            if exists:
                row_count = self._get_table_row_count(cursor, SOURCE_TABLE)
                oracle_logger.info(f"   ? {SOURCE_TABLE} exists ({row_count} rows)")
            else:
                oracle_logger.error(f"   ? {SOURCE_TABLE} MISSING")
                all_valid =True

            self.tables_validated = all_valid

            if all_valid:
                oracle_logger.info("? All required tables validated successfully")
            else:
                oracle_logger.error("? Some required tables are missing")
                oracle_logger.error("   Run init_oracle_tables.sql to create missing tables")

            return all_valid

        except Exception as e:
            oracle_logger.error(f"? Error validating tables: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            return False
        finally:
            cursor.close()

    @log_function_call
    def _check_table_exists(self, cursor, table_name: str, schema: Optional[str]) -> bool:
        """Check if a table exists"""
        try:
            if schema:
                query = """
                    SELECT COUNT(*)
                    FROM all_tables
                    WHERE owner = UPPER(:schema_name) AND table_name = UPPER(:tbl_name)
                """
                cursor.execute(query, {'schema_name': schema, 'tbl_name': table_name})
            else:
                query = """
                    SELECT COUNT(*)
                    FROM user_tables
                    WHERE table_name = UPPER(:tbl_name)
                """
                cursor.execute(query, {'tbl_name': table_name})

            exists = cursor.fetchone()[0] > 0
            oracle_logger.debug(f"Table check: {schema + '.' if schema else ''}{table_name} = {exists}")
            return exists

        except Exception as e:
            oracle_logger.error(f"Error checking table {table_name}: {e}")
            return False

    @log_function_call
    def _get_table_row_count(self, cursor, table_name: str, schema: Optional[str] = None) -> int:
        """Get approximate row count for a table"""
        try:
            full_name = f"{table_name}" if schema else table_name
            query = f"SELECT COUNT(*) FROM {full_name} WHERE ROWNUM <= 1000"
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            oracle_logger.debug(f"Could not get row count for {table_name}: {e}")
            return -1

    # ============================
    # Table Creation
    # ============================

    @log_function_call
    def create_tables(self):
        """Create all required CDC tracking tables"""
        oracle_logger.info("? Creating CDC tables...")

        cursor = self.oracle_conn.cursor()

        tables = {
            'CDC_PROCESSING_STATUS': """
                CREATE TABLE CDC_PROCESSING_STATUS (
                    TABLE_NAME VARCHAR2(100) PRIMARY KEY,
                    LAST_PROCESSED_TIMESTAMP TIMESTAMP,
                    LAST_CHANGE_ID NUMBER,
                    TOTAL_PROCESSED NUMBER DEFAULT 0,
                    IS_ENABLED NUMBER DEFAULT 1,
                    CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                    LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """,

            'CDC_PROCESSED_CALLS': """
                CREATE TABLE CDC_PROCESSED_CALLS (
                    CALL_ID VARCHAR2(50) PRIMARY KEY,
                    PROCESSED_AT TIMESTAMP DEFAULT SYSTIMESTAMP,
                    TEXT_TIME TIMESTAMP,
                    SQS_MESSAGE_ID VARCHAR2(200)
                )
            """,

            'CDC_PROCESSING_LOG': """
                CREATE TABLE CDC_PROCESSING_LOG (
                    LOG_ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    BATCH_ID VARCHAR2(50),
                    CALL_ID VARCHAR2(50),
                    OPERATION_TYPE VARCHAR2(20),
                    PROCESSING_TIME_MS NUMBER,
                    STATUS VARCHAR2(50),
                    ERROR_MESSAGE CLOB,
                    CREATED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """,

            'ERROR_LOG': """
                CREATE TABLE ERROR_LOG (
                    ERROR_ID RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
                    CALL_ID VARCHAR2(50),
                    ERROR_MESSAGE CLOB,
                    ERROR_TYPE VARCHAR2(100),
                    RETRY_COUNT NUMBER DEFAULT 0,
                    ERROR_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """,

            'SQS_PERMANENT_FAILURES': """
                CREATE TABLE SQS_PERMANENT_FAILURES (
                    FAILURE_ID RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
                    CALL_ID VARCHAR2(50),
                    ERROR_MESSAGE CLOB,
                    TOTAL_ATTEMPTS NUMBER,
                    ORIGINAL_MESSAGE CLOB,
                    MARKED_FAILED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """,

            'DICTA_CALL_SUMMARY': """
                CREATE TABLE DICTA_CALL_SUMMARY (
                    CALL_ID VARCHAR2(50) PRIMARY KEY,
                    CUSTOMER_ID VARCHAR2(50),
                    SUBSCRIBER_NO VARCHAR2(50),
                    CALL_TIME TIMESTAMP,
                    SUMMARY_TEXT CLOB,
                    SENTIMENT VARCHAR2(50),
                    CLASSIFICATION_PRIMARY VARCHAR2(255),
                    CLASSIFICATION_ALL CLOB,
                    CONFIDENCE_SCORE NUMBER(5,2),
                    ML_PROCESSING_TIME_MS NUMBER,
                    ML_MODEL_VERSION VARCHAR2(50),
                    PROCESSED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
                )
            """
        }

        for table_name, create_sql in tables.items():
            try:
                exists = self._check_table_exists(cursor, table_name, None)

                if not exists:
                    oracle_logger.info(f"   Creating {table_name}...")
                    cursor.execute(create_sql)
                    oracle_logger.info(f"   ? Created table: {table_name}")
                else:
                    oracle_logger.info(f"   ??  Table already exists: {table_name}")

            except Exception as e:
                oracle_logger.error(f"   ? Failed to create table {table_name}: {e}")
                oracle_logger.error(f"      Traceback: {traceback.format_exc()}")

        # Initialize CDC_PROCESSING_STATUS
        try:
            oracle_logger.info("   Initializing CDC_PROCESSING_STATUS...")

            cursor.execute("""
                MERGE INTO CDC_PROCESSING_STATUS t
                USING (SELECT 'CDC_NORMAL_MODE' AS name FROM dual) s
                ON (t.TABLE_NAME = s.name)
                WHEN NOT MATCHED THEN
                    INSERT (TABLE_NAME, LAST_PROCESSED_TIMESTAMP, IS_ENABLED)
                    VALUES ('CDC_NORMAL_MODE', SYSTIMESTAMP - 1, 1)
            """)

            cursor.execute("""
                MERGE INTO CDC_PROCESSING_STATUS t
                USING (SELECT 'CDC_HISTORICAL_MODE' AS name FROM dual) s
                ON (t.TABLE_NAME = s.name)
                WHEN NOT MATCHED THEN
                    INSERT (TABLE_NAME, LAST_PROCESSED_TIMESTAMP, IS_ENABLED)
                    VALUES ('CDC_HISTORICAL_MODE', TO_TIMESTAMP(:start_date, 'YYYY-MM-DD'), 0)
            """, {'start_date': CDC_CONFIG['historical_start_date']})

            self.oracle_conn.commit()
            oracle_logger.info("   ? CDC_PROCESSING_STATUS initialized")

        except Exception as e:
            oracle_logger.error(f"   ? Failed to initialize CDC status: {e}")
            oracle_logger.error(f"      Traceback: {traceback.format_exc()}")

        cursor.close()
        oracle_logger.info("? Table creation process completed")

    # ============================
    # CDC Collection - Normal Mode
    # ============================

    @log_function_call
    def collect_new_calls(self) -> List[str]:
        """Collect new calls from last N minutes"""
        oracle_logger.info(f"? Scanning for new calls (last {CDC_CONFIG['normal_mode_minutes']} minutes)...")

        cursor = self.oracle_conn.cursor()

        try:
            # Get last processed timestamp
            cursor.execute("""
                SELECT LAST_PROCESSED_TIMESTAMP, TOTAL_PROCESSED
                FROM CDC_PROCESSING_STATUS
                WHERE TABLE_NAME = 'CDC_NORMAL_MODE' AND IS_ENABLED = 1
            """)
            row = cursor.fetchone()
            last_timestamp = row[0] if row else None
            total_processed = row[1] if row else 0

            oracle_logger.debug(f"   Last processed: {last_timestamp}")
            oracle_logger.debug(f"   Total processed to date: {total_processed}")

            # Query for new calls
            minutes = CDC_CONFIG['normal_mode_minutes']
            query = f"""
                SELECT /*+ index (VERINT_TEXT_ANALYSIS VERINT_TEXT_ANALYSIS_3ix ) */
                DISTINCT CALL_ID, CALL_TIME
                FROM {SOURCE_TABLE}
                WHERE CALL_TIME > SYSDATE - 500/1440
                AND CALL_ID NOT IN (
                    SELECT CALL_ID FROM CDC_PROCESSED_CALLS where TEXT_TIME > SYSDATE -  1200/1440
                )
                ORDER BY CALL_TIME ASC
                FETCH FIRST :batch_size ROWS ONLY
            """


            oracle_logger.info(f"Executing query:\n{query}")
            oracle_logger.info(f"Parameters: batch_size={CDC_CONFIG['max_batch_size']}")

            cursor.execute(query, {'batch_size': CDC_CONFIG['max_batch_size']})
            rows = cursor.fetchall()

            call_ids = [row[0] for row in rows]

            if call_ids:
                oracle_logger.info(f"? Found {len(call_ids)} new call(s)")
                oracle_logger.debug(f"   Call IDs: {call_ids[:10]}{'...' if len(call_ids) > 10 else ''}")
            else:
                oracle_logger.debug("   No new calls found")

            return call_ids

        except Exception as e:
            oracle_logger.error(f"? Error collecting new calls: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            return []
        finally:
            cursor.close()

    # ============================
    # CDC Collection - Historical Mode
    # ============================

    @log_function_call
    def collect_historical_calls(self) -> List[str]:
        """Collect historical calls for backfill"""
        oracle_logger.info("? Scanning for historical calls...")

        cursor = self.oracle_conn.cursor()

        try:
            # Check if historical mode is enabled
            cursor.execute("""
                SELECT LAST_PROCESSED_TIMESTAMP, IS_ENABLED, TOTAL_PROCESSED
                FROM CDC_PROCESSING_STATUS
                WHERE TABLE_NAME = 'CDC_HISTORICAL_MODE'
            """)
            row = cursor.fetchone()

            if not row or row[1] == 0:
                oracle_logger.debug("   Historical mode disabled")
                return []

            last_timestamp = row[0]
            total_processed = row[2]

            oracle_logger.info(f"   Processing from: {last_timestamp}")
            oracle_logger.debug(f"   Total historical processed: {total_processed}")

            # Query for historical calls
            query = f"""
                SELECT  /*+ index (VERINT_TEXT_ANALYSIS VERINT_TEXT_ANALYSIS_3ix ) */
                DISTINCT CALL_ID, CALL_TIME
                FROM {SOURCE_TABLE}
                WHERE CALL_TIME >= :start_time
                AND CALL_TIME < :start_time + INTERVAL '1' DAY
                AND CALL_ID NOT IN (
                    SELECT CALL_ID FROM CDC_PROCESSED_CALLS WHERE TEXT_TIME > SYSDATE - (420 / 1440)
                )
                ORDER BY CALL_TIME ASC
                FETCH FIRST :batch_size ROWS ONLY
            """

            cursor.execute(query, {
                'start_time': last_timestamp,
                'batch_size': CDC_CONFIG['historical_batch_size']
            })
            rows = cursor.fetchall()

            call_ids = [row[0] for row in rows]

            if call_ids:
                oracle_logger.info(f"? Found {len(call_ids)} historical call(s)")
                oracle_logger.debug(f"   Call IDs: {call_ids[:10]}{'...' if len(call_ids) > 10 else ''}")
            else:
                oracle_logger.debug("   No historical calls in current time window")

            return call_ids

        except Exception as e:
            oracle_logger.error(f"? Error collecting historical calls: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            return []
        finally:
            cursor.close()

    # ============================
    # Conversation Assembly
    # ============================

    @log_function_call
    def assemble_conversation(self, call_id: str) -> Optional[Dict[str, Any]]:
        """Fetch all segments for a CALL_ID and assemble into conversation"""
        oracle_logger.debug(f"? Assembling conversation: {call_id}")

        cursor = self.oracle_conn.cursor()

        # Configure cursor to fetch CLOB content as strings automatically
        cursor.prefetchrows = 0
        cursor.arraysize = 100

        try:
            query = f"""
                SELECT
                    CALL_ID, BAN, SUBSCRIBER_NO, OWNER,
                    CALL_TIME, DBMS_LOB.SUBSTR(TEXT, 4000, 1) as TEXT
                FROM {SOURCE_TABLE}
                WHERE CALL_ID = :call_id
                AND CALL_TIME > SYSDATE - 1200/1440
                ORDER BY CALL_TIME ASC
            """

            cursor.execute(query, {'call_id': call_id})
            rows = cursor.fetchall()

            if not rows:
                oracle_logger.warning(f"??  No data found for CALL_ID: {call_id}")
                return None
            # Check minimum conversation length
            if not rows or len(rows) < 11:
                oracle_logger.warning(f"??  Conversation too short ({len(rows) if rows else 0} rows): {call_id}")
                self.mark_call_processed(call_id, 'SKIPPED_TOO_SHORT')
                return None

            oracle_logger.debug(f"   Found {len(rows)} segment(s)")

            # Check for conversation completeness
            channels = set(row[3] for row in rows if row[3])
            oracle_logger.debug(f"   Channels present: {channels}")

            if 'A' not in channels or 'C' not in channels:
                oracle_logger.warning(f"??  Incomplete conversation (missing A or C): {call_id}")
                oracle_logger.debug(f"      Channels: {channels}")
                self.mark_call_processed(call_id, 'SKIPPED_MISSING_CHANNEL')
                return None

            # Assemble messages
            messages = []
            for idx, row in enumerate(rows):
                # TEXT is now extracted as string via DBMS_LOB.SUBSTR
                text_content = row[5]
                if text_content and str(text_content).strip():
                    messages.append({
                        'channel': row[3],  # OWNER ('C' or 'A')
                        'text': str(text_content),  # TEXT extracted as string from CLOB
                        'timestamp': row[4].isoformat() if row[4] else None  # TEXT_TIME
                    })
                else:
                    oracle_logger.debug(f"   Skipping empty text for row {idx}")

            # Build conversation object
            conversation = {
                'type': MESSAGE_TYPES['CONVERSATION_TO_ML'],
                'callId': str(call_id),  # Convert to string for JSON
                'ban': rows[0][1],          # BAN
                'subscriberNo': rows[0][2],  # SUBSCRIBER_NO
                'callTime': rows[0][4].isoformat() if rows[0][4] else None,  # CALL_TIME
                'messages': messages,
                'messageCount': len(messages),
                'assembledAt': datetime.utcnow().isoformat(),
                'source': 'on-premises-cdc'
            }

            oracle_logger.info(f"? Assembled: {call_id} ({len(messages)} messages, channels: {channels})")
            return conversation

        except Exception as e:
            oracle_logger.error(f"? Error assembling conversation {call_id}: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            return None
        finally:
            cursor.close()

    # ============================
    # SQS Communication - Send
    # ============================

    @log_function_call
    def send_to_sqs(self, conversation: Dict[str, Any]) -> Optional[str]:
        """Send conversation to AWS SQS for ML processing"""
        call_id = conversation.get('callId', 'UNKNOWN')
        sqs_logger.info(f"? Sending to SQS: {call_id}")

        try:
            message_body = json.dumps(conversation, ensure_ascii=False)
            body_size = len(message_body)

            sqs_logger.debug(f"   Message size: {body_size} bytes")
            sqs_logger.debug(f"   Message count: {conversation.get('messageCount', 0)}")

            response = self.sqs_client.send_message(
                QueueUrl=SQS_OUTBOUND_QUEUE_URL,
                MessageBody=message_body,
                MessageAttributes={
                    'messageType': {
                        'StringValue': MESSAGE_TYPES['CONVERSATION_TO_ML'],
                        'DataType': 'String'
                    },
                    'source': {
                        'StringValue': 'on-premises-cdc',
                        'DataType': 'String'
                    },
                    'callId': {
                        'StringValue': str(call_id),  # Convert to string
                        'DataType': 'String'
                    },
                    'timestamp': {
                        'StringValue': datetime.utcnow().isoformat(),
                        'DataType': 'String'
                    }
                }
            )

            message_id = response.get('MessageId')
            sqs_logger.info(f"? Sent successfully: {call_id} ? SQS Message ID: {message_id}")

            # Mark as processed
            self.mark_call_processed(call_id, message_id)

            # Update stats
            self.stats['total_sqs_sent'] += 1

            return message_id

        except Exception as e:
            sqs_logger.error(f"? Failed to send to SQS: {call_id}")
            sqs_logger.error(f"   Error: {e}")
            sqs_logger.error(f"   Traceback: {traceback.format_exc()}")

            self.log_error(call_id, str(e), 'SQS_SEND_FAILED')
            self.stats['total_sqs_failed'] += 1

            return None



    # ============================
    # SQS Communication - Receive ML Results
    # ============================

    @log_function_call
    def receive_ml_results(self):
        """Poll SQS inbound queue for ML processing results from AWS ML service"""
        sqs_logger.debug("? Polling SQS inbound queue for ML results...")

        try:
            # Receive from inbound queue (summary-pipe-complete)
            response = self.sqs_client.receive_message(
                QueueUrl=SQS_INBOUND_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,  # Short polling for CDC loop
                MessageAttributeNames=['All'],
                AttributeNames=['All']
            )

            messages = response.get('Messages', [])

            if messages:
                sqs_logger.info(f"? Received {len(messages)} message(s) from SQS inbound queue")

            for message in messages:
                try:
                    message_id = message.get('MessageId')
                    body = json.loads(message['Body'])

                    # Check message type
                    msg_attrs = message.get('MessageAttributes', {})
                    msg_type = msg_attrs.get('messageType', {}).get('StringValue')

                    sqs_logger.debug(f"   Processing message: {message_id}")
                    sqs_logger.debug(f"   Type: {msg_type}")

                    if msg_type == MESSAGE_TYPES['ML_RESULT']:
                        call_id = body.get('callId', 'UNKNOWN')
                        sqs_logger.info(f"   ? ML Result for: {call_id}")

                        # Process ML result - writes to DICTA_CALL_SUMMARY, CONVERSATION_SUMMARY, CONVERSATION_CATEGORY
                        success = self.write_ml_result(body)

                        if success:
                            # Delete message from inbound queue after successful processing
                            self.sqs_client.delete_message(
                                QueueUrl=SQS_INBOUND_QUEUE_URL,
                                ReceiptHandle=message['ReceiptHandle']
                            )

                            sqs_logger.info(f"   ? Processed and deleted: {message_id}")
                            self.stats['total_ml_results_received'] += 1
                    else:
                        sqs_logger.debug(f"   Skipping message type: {msg_type}")

                except json.JSONDecodeError as e:
                    sqs_logger.error(f"   ? Invalid JSON in message: {e}")

                except Exception as e:
                    sqs_logger.error(f"   ? Failed to process SQS message: {e}")
                    sqs_logger.error(f"      Traceback: {traceback.format_exc()}")

        except Exception as e:
            sqs_logger.error(f"? Error receiving from SQS: {e}")
            sqs_logger.error(f"   Traceback: {traceback.format_exc()}")

    # ============================
    # Database Updates
    # ============================

    @log_function_call
    def write_ml_result(self, result: Dict[str, Any]) -> bool:
        """Write ML processing result to DICTA_CALL_SUMMARY table"""
        call_id = result.get('callId', 'UNKNOWN')
        oracle_logger.info(f"? Writing ML result: {call_id}")

        cursor = self.oracle_conn.cursor()

        try:
            # Handle sentiment - extract as number (1-5 scale, default 3 for neutral)
            sentiment_data = result.get('sentiment', {})
            if isinstance(sentiment_data, dict):
                sentiment_raw = sentiment_data.get('overall', 3)
            else:
                sentiment_raw = sentiment_data if sentiment_data else 3

            # Ensure sentiment is always numeric
            if isinstance(sentiment_raw, str):
                sentiment_map = {'?????': 4, 'positive': 4, '?????': 2, 'negative': 2,
                               '???????': 3, 'neutral': 3, '?????': 3, 'mixed': 3, 'unknown': 3}
                sentiment = sentiment_map.get(sentiment_raw.lower().strip(), 3)
            else:
                sentiment = int(sentiment_raw) if sentiment_raw else 3

            # Handle classification - try multiple sources
            classification_data = result.get('classification', {})
            classifications_list = result.get('classifications', [])

            if isinstance(classification_data, dict) and classification_data.get('primary'):
                classification = classification_data.get('primary')
                all_classifications = classification_data.get('all', [])
            elif classifications_list and len(classifications_list) > 0:
                # Fallback to classifications list if classification.primary is empty
                classification = classifications_list[0]
                all_classifications = classifications_list
            else:
                classification = str(classification_data) if classification_data else 'unknown'
                all_classifications = []

            # Handle summary - can be dict or string
            summary_data = result.get('summary', '')
            if isinstance(summary_data, dict):
                summary_text = summary_data.get('text', '')
            else:
                summary_text = str(summary_data) if summary_data else ''

            oracle_logger.info(f"   Sentiment: {sentiment}")
            oracle_logger.info(f"   Classification: {classification}")
            oracle_logger.info(f"   Confidence: {result.get('confidence', 0)}")
            oracle_logger.info(f"   summary_text:{summary_text}")


            # Prepare parameters with proper handling
            insert_params = {
                'call_id': str(call_id),
                'customer_id': result.get('ban') or result.get('customerId'),
                'subscriber_no': result.get('subscriberNo') or result.get('subscriber_no'),
                'call_time': result.get('callTime') or result.get('call_time'),
                'summary': summary_text[:4000] if summary_text else '',  # Oracle VARCHAR2(4000) limit
                'sentiment': sentiment if sentiment is not None else 3,  # Numeric 1-5
                'classification': classification[:100] if classification else 'unknown',
                'all_classifications': ', '.join(all_classifications) if isinstance(all_classifications, list) else str(all_classifications),
                'confidence': float(result.get('confidence', 0.0)) if result.get('confidence') is not None else 0.0,
                'processing_time': int(result.get('processingTime', 0)),
                'model_version': str(result.get('modelVersion', 'dictalm-2.0'))[:50]
            }

            # Use DELETE + INSERT instead of MERGE to avoid ORA-14402 partition key update error
            # First delete existing record if any
            cursor.execute("""
                DELETE FROM DICTA_CALL_SUMMARY WHERE CALL_ID = :call_id
            """, {'call_id': str(call_id)})

            # Then insert fresh record
            cursor.execute("""
                INSERT INTO DICTA_CALL_SUMMARY (
                    CALL_ID, CUSTOMER_ID, SUBSCRIBER_NO, CALL_TIME,
                    SUMMARY_TEXT, SENTIMENT, CLASSIFICATION_PRIMARY,
                    CLASSIFICATION_ALL, CONFIDENCE_SCORE,
                    ML_PROCESSING_TIME_MS, ML_MODEL_VERSION, PROCESSED_AT
                ) VALUES (
                    :call_id, :customer_id, :subscriber_no, TO_TIMESTAMP(:call_time, 'YYYY-MM-DD"T"HH24:MI:SS.FF'),
                    :summary, :sentiment, :classification,
                    :all_classifications, :confidence,
                    :processing_time, :model_version, SYSTIMESTAMP
                )
            """, insert_params)

            self.oracle_conn.commit()


            # Also save to CONVERSATION_SUMMARY (MERGE to handle re-processing)
            try:
                # Extract sentiment - can be dict {'overall': '...', 'score': 0.8} or string
                sentiment_raw = result.get('sentiment', {})
                if isinstance(sentiment_raw, dict):
                    sentiment_value = sentiment_raw.get('overall', 'neutral')
                else:
                    sentiment_value = str(sentiment_raw) if sentiment_raw else 'neutral'

                # Clean JSON arrays to comma-separated values (remove [], {}, "", '')
                products_val = clean_json_to_csv(result.get('products', ''))
                # Use specialized extractor for action_items - removes metadata, keeps only action text, max 500 chars
                action_items_val = extract_action_items_text(result.get('action_items', ''), max_length=500)
                unresolved_val = clean_json_to_csv(result.get('unresolved_issues', ''))
                satisfaction_val = result.get('customer_satisfaction', 3)

                # Get churn score (0-100 scale) from embedding-based churn detection
                churn_confidence = result.get('churn_confidence', 0.0)
                churn_score = churn_confidence * 100

                # FIX: Extended time filter from 2 hours to 7 days to ensure we find the record
                # Get BAN, SUBSCRIBER_NO, and CALL_TIME from VERINT_TEXT_ANALYSIS
                cursor.execute("""
                    SELECT BAN, SUBSCRIBER_NO, CALL_TIME
                    FROM VERINT_TEXT_ANALYSIS
                    WHERE CALL_ID = :call_id
                    AND CALL_TIME > SYSDATE - 90
                    AND ROWNUM = 1
                """, {'call_id': call_id})
                verint_row = cursor.fetchone()

                ban_val = verint_row[0] if verint_row else None
                subscriber_no_val = verint_row[1] if verint_row else None
                conversation_time_val = verint_row[2] if verint_row else None

                oracle_logger.info(f"? CONVERSATION_SUMMARY data for {call_id}:")
                oracle_logger.info(f"   ban: {ban_val}, subscriber_no: {subscriber_no_val}, conversation_time: {conversation_time_val}")
                oracle_logger.info(f"   products: {products_val}")
                oracle_logger.info(f"   action_items: {action_items_val}")
                oracle_logger.info(f"   unresolved_issues: {unresolved_val}")
                oracle_logger.info(f"   satisfaction: {satisfaction_val}")
                oracle_logger.info(f"   sentiment: {sentiment_value}")
                oracle_logger.info(f"   churn_score: {churn_score:.3f}")

                # Use DELETE + INSERT instead of MERGE to avoid partition key issues
                # cursor.execute("""
                #     DELETE FROM CONVERSATION_SUMMARY
                #     WHERE SOURCE_ID = :source_id AND SOURCE_TYPE = :source_type
                # """, {'source_id': call_id, 'source_type': 'CALL'})

                # FIX: Changed text_time to conversation_time
                cursor.execute("""
                    INSERT INTO CONVERSATION_SUMMARY (
                        source_type, source_id, creation_date, summary,
                        satisfaction, sentiment, products, unresolved_issues, action_items,
                        ban, subscriber_no, conversation_time, churn_score
                    ) VALUES (
                        :source_type, :source_id, SYSDATE, :summary,
                        :satisfaction, :sentiment, :products, :unresolved_issues, :action_items,
                        :ban, :subscriber_no, :conversation_time, :churn_score
                    )
                """, {
                    'source_type': 'CALL',
                    'source_id': call_id,
                    'summary': summary_text[:4000] if summary_text else '',
                    'satisfaction': satisfaction_val,
                    'sentiment': sentiment_value,
                    'products': products_val,
                    'unresolved_issues': unresolved_val,
                    'action_items': action_items_val,
                    'ban': ban_val,
                    'subscriber_no': subscriber_no_val,
                    'conversation_time': conversation_time_val,
                    'churn_score': churn_score
                })
                self.oracle_conn.commit()
                oracle_logger.info(f"? Conversation summary written: {call_id}")
            except Exception as e:
                oracle_logger.error(f"? Failed to write to CONVERSATION_SUMMARY for {call_id}: {e}")
                oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
                self.oracle_conn.rollback()
                return False

            # Also save to CONVERSATION_CATEGORY - insert ALL classifications (one row per category)
            try:
                # Get all classifications from result
                all_classifications = result.get('classifications', [])

                # Fallback: try classification.all if classifications is empty
                if not all_classifications:
                    classification_obj = result.get('classification', {})
                    if isinstance(classification_obj, dict):
                        all_classifications = classification_obj.get('all', [])

                # Final fallback: use primary classification
                if not all_classifications:
                    all_classifications = [classification] if classification else []

                # Ensure it's a list
                if isinstance(all_classifications, str):
                    all_classifications = [all_classifications]

                # Filter out empty values and duplicates
                all_classifications = list(set([c for c in all_classifications if c and str(c).strip()]))

                oracle_logger.info(f"? Inserting {len(all_classifications)} categories for {call_id}: {all_classifications}")

                # Delete existing categories for this call (avoid duplicates on reprocess)
                cursor.execute("""
                    DELETE FROM CONVERSATION_CATEGORY
                    WHERE SOURCE_ID = :source_id AND SOURCE_TYPE = 'CALL'
                """, {'source_id': call_id})

                # Insert each classification as a separate row
                categories_inserted = 0
                for category_code in all_classifications:
                    cursor.execute("""
                        INSERT INTO CONVERSATION_CATEGORY (SOURCE_ID, SOURCE_TYPE, CREATION_DATE, CATEGORY_CODE)
                        VALUES (:source_id, :source_type, SYSDATE, :category_code)
                    """, {
                        'source_id': call_id,
                        'source_type': 'CALL',
                        'category_code': str(category_code)[:255]  # Truncate if needed
                    })
                    categories_inserted += 1

                self.oracle_conn.commit()
                oracle_logger.info(f"? Conversation categories written: {call_id} ({categories_inserted} categories)")
            except Exception as e:
                oracle_logger.error(f"? Failed to write to CONVERSATION_CATEGORY for {call_id}: {e}")
                oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
                self.oracle_conn.rollback()
                return False


            oracle_logger.info(f"? ML result written: {call_id}")
            self.stats['total_ml_results_written'] += 1



            return True

        except Exception as e:
            oracle_logger.error(f"? Failed to write ML result for {call_id}: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            self.oracle_conn.rollback()
            return False
        finally:
            cursor.close()

    @log_function_call
    def mark_call_processed(self, call_id: str, sqs_message_id: str):
        """Mark call as processed in CDC_PROCESSED_CALLS"""
        oracle_logger.debug(f"? Marking processed: {call_id}")

        cursor = self.oracle_conn.cursor()

        try:
            # First check if already processed
            cursor.execute("""
                SELECT COUNT(*) FROM CDC_PROCESSED_CALLS
                WHERE CALL_ID = :call_id
            """, {'call_id': str(call_id)})

            if cursor.fetchone()[0] == 0:
                # Not processed yet, insert it
                cursor.execute("""
                    INSERT INTO CDC_PROCESSED_CALLS (CALL_ID, SQS_MESSAGE_ID, TEXT_TIME)
                    SELECT :call_id, :msg_id, MAX(CALL_TIME)
                    FROM VERINT_TEXT_ANALYSIS
                    WHERE CALL_TIME >  sysdate - (1200/1440)
                    AND CALL_ID = :call_id
                """, {
                    'call_id': str(call_id),
                    'msg_id': sqs_message_id
                })
                self.oracle_conn.commit()
                oracle_logger.debug(f"   ? Marked: {call_id}")
            else:
                oracle_logger.debug(f"   ??  Already processed: {call_id}")

        except Exception as e:
            oracle_logger.error(f"   ? Failed to mark call processed: {e}")
        finally:
            cursor.close()

    @log_function_call
    def update_cdc_status(self, mode: str, timestamp: datetime):
        """Update CDC processing status"""
        oracle_logger.debug(f"Updating CDC status: {mode} ? {timestamp}")

        cursor = self.oracle_conn.cursor()

        try:
            cursor.execute("""
                UPDATE CDC_PROCESSING_STATUS
                SET LAST_PROCESSED_TIMESTAMP = :ts,
                    TOTAL_PROCESSED = TOTAL_PROCESSED + 1,
                    LAST_UPDATED = SYSTIMESTAMP
                WHERE TABLE_NAME = :table_name
            """, {
                'ts': timestamp,
                'table_name': mode
            })

            self.oracle_conn.commit()

        except Exception as e:
            oracle_logger.error(f"Failed to update CDC status: {e}")
        finally:
            cursor.close()

    @log_function_call
    def log_error(self, call_id: str, error_message: str, error_type: str):
        """Log error to ERROR_LOG table"""
        oracle_logger.debug(f"Logging error for {call_id}: {error_type}")

        cursor = self.oracle_conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO ERROR_LOG (CALL_ID, ERROR_MESSAGE, ERROR_TYPE)
                VALUES (:call_id, :error_msg, :error_type)
            """, {
                'call_id': call_id,
                'error_msg': error_message,
                'error_type': error_type
            })

            self.oracle_conn.commit()

        except Exception as e:
            oracle_logger.error(f"Failed to log error: {e}")
        finally:
            cursor.close()


    @log_function_call
    def flush_all_sqs_to_db(self):
        """Flush all messages from SQS inbound queue to the database."""
        total_processed = 0
        while True:
            response = self.sqs_client.receive_message(
                QueueUrl=SQS_INBOUND_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,  # Short poll for speed
                MessageAttributeNames=['All'],
                AttributeNames=['All']
            )
            messages = response.get('Messages', [])
            if not messages:
                logger.info(f"? SQS flush complete. Total messages processed: {total_processed}")
                break


            for message in messages:
                try:
                    message_id = message.get('MessageId')
                    body = json.loads(message['Body'])
                    msg_attrs = message.get('MessageAttributes', {})
                    msg_type = msg_attrs.get('messageType', {}).get('StringValue')

                    if msg_type == MESSAGE_TYPES['ML_RESULT']:
                        logger.info(f"   Processing ML_RESULT message: {message_id}")
                        success = self.write_ml_result(body)
                        if success:
                            self.sqs_client.delete_message(
                                QueueUrl=SQS_INBOUND_QUEUE_URL,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            total_processed += 1
                            logger.info(f"   ? Processed and deleted: {message_id}")
                        else:
                            logger.error(f"   ? Failed to process ML_RESULT message: {message_id}")
                    else:
                        logger.info(f"   Skipping message type: {msg_type}")
                        # Do NOT delete the message

                except Exception as e:
                    logger.error(f"   ? Error processing SQS message: {e}")
                    logger.error(f"      Traceback: {traceback.format_exc()}")


        logger.info(f"? All SQS messages flushed to database.")


    # ============================
    # Statistics & Health
    # ============================

    @log_function_call
    def print_statistics(self):
        """Print current statistics"""
        logger.info("="*80)
        logger.info("? CDC SERVICE STATISTICS")
        logger.info("="*80)
        logger.info(f"Uptime: {datetime.utcnow() - self.startup_time}")
        logger.info(f"Cycles completed: {self.stats['cycles_completed']}")
        logger.info(f"Calls processed: {self.stats['total_calls_processed']}")
        logger.info(f"Calls failed: {self.stats['total_calls_failed']}")
        logger.info(f"SQS sent: {self.stats['total_sqs_sent']}")
        logger.info(f"SQS failed: {self.stats['total_sqs_failed']}")
        logger.info(f"ML results received: {self.stats['total_ml_results_received']}")
        logger.info(f"ML results written: {self.stats['total_ml_results_written']}")
        logger.info(f"Last cycle: {self.stats['last_cycle_time']}")
        logger.info("="*80)

    # ============================
    # Main Processing Loop
    # ============================

    @log_function_call
    def process_batch(self, call_ids: List[str], mode: str):
        """Process a batch of call IDs"""
        logger.info(f"??  Processing batch of {len(call_ids)} calls ({mode})")

        for idx, call_id in enumerate(call_ids, 1):
            try:
                logger.debug(f"   [{idx}/{len(call_ids)}] Processing: {call_id}")

                # Assemble conversation
                conversation = self.assemble_conversation(call_id)

                if conversation:
                    # Send to SQS
                    message_id = self.send_to_sqs(conversation)

                    if message_id:
                        # Update status
                        self.update_cdc_status(mode, datetime.utcnow())
                        self.stats['total_calls_processed'] += 1
                    else:
                        self.stats['total_calls_failed'] += 1
                else:
                    self.stats['total_calls_failed'] += 1

            except Exception as e:
                logger.error(f"   ? Error processing call {call_id}: {e}")
                self.log_error(call_id, str(e), 'PROCESSING_ERROR')
                self.stats['total_calls_failed'] += 1

        logger.info(f"? Batch complete: {len(call_ids)} calls processed")

    def run_forever(self):
        """Main 24/7 processing loop"""
        logger.info("="*80)
        logger.info("? ORACLE CDC SERVICE - STARTING 24/7 MODE")
        logger.info("="*80)

        # Initialize connections
        if not self.connect_oracle():
            logger.error("? Cannot start without Oracle connection")
            return

        if not self.connect_sqs():
            logger.error("? Cannot start without SQS connection")
            return

        # Validate tables
        if not self.validate_tables():
            logger.warning("??  Some tables missing - attempting to create...")
            self.create_tables()

            # Re-validate
            if not self.validate_tables():
                logger.error("? Cannot start - required tables still missing")
                return

        self.is_running = True
        logger.info("? All systems ready - starting CDC loop")
        logger.info("="*80)

        while self.is_running:
            try:
                cycle_start = time.time()
                self.stats['cycles_completed'] += 1
                cycle_num = self.stats['cycles_completed']

                logger.info(f"")
                logger.info(f"{'='*80}")
                logger.info(f"? CDC CYCLE #{cycle_num} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*80}")

                # Process normal mode
                if CDC_CONFIG['normal_mode_enabled']:
                    call_ids = self.collect_new_calls()
                    if call_ids:
                        self.process_batch(call_ids, 'CDC_NORMAL_MODE')

                # Process historical mode
                if CDC_CONFIG['historical_mode_enabled']:
                    call_ids = self.collect_historical_calls()
                    if call_ids:
                        self.process_batch(call_ids, 'CDC_HISTORICAL_MODE')

                # Check for ML results
                self.receive_ml_results()

                # Cycle complete
                cycle_time = time.time() - cycle_start
                self.stats['last_cycle_time'] = datetime.utcnow()

                logger.info(f"? Cycle #{cycle_num} complete in {cycle_time:.2f}s")

                # Print stats every 10 cycles
                if cycle_num % 10 == 0:
                    self.print_statistics()

                # Sleep
                logger.info(f"? Sleeping {CDC_CONFIG['normal_poll_interval_seconds']}s until next cycle...")
                time.sleep(CDC_CONFIG['normal_poll_interval_seconds'])

            except KeyboardInterrupt:
                logger.warning("??  SHUTDOWN SIGNAL RECEIVED (Ctrl+C)")
                self.is_running = False

            except Exception as e:
                logger.error(f"? CRITICAL ERROR in main loop: {e}")
                logger.error(f"   Traceback: {traceback.format_exc()}")
                logger.warning("   Waiting 30s before retry...")
                time.sleep(30)

        # Cleanup
        logger.info("="*80)
        logger.info("? SHUTTING DOWN CDC SERVICE")
        logger.info("="*80)

        self.print_statistics()

        if self.oracle_conn:
            self.oracle_conn.close()
            oracle_logger.info("? Oracle connection closed")

        logger.info("? Oracle CDC Service stopped gracefully")


    def run_flush_mode(self, interval_seconds: int = 30):
        """
        Run in flush mode - continuously poll SQS inbound queue every N seconds.
        Never stops even when no messages in queue.

        Usage: python cdc_service_prod_fixed.py flush_mode [interval_seconds]
        Default interval: 30 seconds
        """
        logger.info("="*80)
        logger.info("? STARTING CDC SERVICE IN FLUSH MODE")
        logger.info(f"   Interval: {interval_seconds} seconds")
        logger.info(f"   Will continuously poll SQS inbound queue for ML results")
        logger.info("="*80)

        # Connect to services
        self.connect_oracle()
        self.connect_sqs()

        # Validate tables
        if not self.validate_tables():
            logger.warning("??  Some tables missing - attempting to create...")
            self.create_tables()

        self.is_running = True
        flush_count = 0

        while self.is_running:
            try:
                flush_count += 1
                flush_start = time.time()

                logger.info(f"")
                logger.info(f"{'='*80}")
                logger.info(f"? FLUSH CYCLE #{flush_count} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*80}")

                # Flush all messages from SQS to DB
                total_processed = 0
                empty_polls = 0
                max_empty_polls = 3  # Stop polling after 3 empty responses in a row

                while empty_polls < max_empty_polls:
                    response = self.sqs_client.receive_message(
                        QueueUrl=SQS_INBOUND_QUEUE_URL,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=2,
                        MessageAttributeNames=['All'],
                        AttributeNames=['All']
                    )
                    messages = response.get('Messages', [])

                    if not messages:
                        empty_polls += 1
                        logger.debug(f"   No messages (empty poll {empty_polls}/{max_empty_polls})")
                        continue

                    empty_polls = 0  # Reset counter when we get messages

                    for message in messages:
                        try:
                            message_id = message.get('MessageId')
                            body = json.loads(message['Body'])
                            msg_attrs = message.get('MessageAttributes', {})
                            msg_type = msg_attrs.get('messageType', {}).get('StringValue')

                            if msg_type == MESSAGE_TYPES['ML_RESULT']:
                                logger.info(f"   Processing ML_RESULT: {message_id}")
                                success = self.write_ml_result(body)
                                if success:
                                    self.sqs_client.delete_message(
                                        QueueUrl=SQS_INBOUND_QUEUE_URL,
                                        ReceiptHandle=message['ReceiptHandle']
                                    )
                                    total_processed += 1
                                    self.stats['total_ml_results_received'] += 1
                                    logger.info(f"   ? Processed and deleted: {message_id}")
                                else:
                                    logger.error(f"   ? Failed to write ML result: {message_id}")
                            else:
                                logger.debug(f"   Skipping message type: {msg_type}")

                        except Exception as e:
                            logger.error(f"   ? Error processing message: {e}")

                flush_time = time.time() - flush_start
                logger.info(f"? Flush #{flush_count} complete: {total_processed} messages in {flush_time:.2f}s")

                # Print stats every 10 flushes
                if flush_count % 10 == 0:
                    self.print_statistics()

                # Sleep until next flush
                logger.info(f"? Sleeping {interval_seconds}s until next flush...")
                time.sleep(interval_seconds)

            except KeyboardInterrupt:
                logger.warning("??  SHUTDOWN SIGNAL RECEIVED (Ctrl+C)")
                self.is_running = False

            except Exception as e:
                logger.error(f"? Error in flush mode: {e}")
                logger.error(f"   Traceback: {traceback.format_exc()}")
                logger.warning(f"   Retrying in {interval_seconds}s...")
                time.sleep(interval_seconds)

        # Cleanup
        logger.info("="*80)
        logger.info("? SHUTTING DOWN FLUSH MODE")
        self.print_statistics()
        if self.oracle_conn:
            self.oracle_conn.close()
        logger.info("? Flush mode stopped gracefully")


# ============================
# Entry Point
# ============================

if __name__ == '__main__':
    import sys
    try:
        cdc = OracleCDCService()

        if len(sys.argv) > 1:
            mode = sys.argv[1]

            if mode == 'flush_sqs':
                # One-time flush
                cdc.connect_oracle()
                cdc.connect_sqs()
                cdc.flush_all_sqs_to_db()

            elif mode == 'flush_mode':
                # Continuous flush mode (default 30 seconds)
                interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
                cdc.run_flush_mode(interval_seconds=interval)

            else:
                print(f"Unknown mode: {mode}")
                print("Usage:")
                print("  python cdc_service_prod_fixed.py              # Normal CDC mode")
                print("  python cdc_service_prod_fixed.py flush_sqs    # One-time flush")
                print("  python cdc_service_prod_fixed.py flush_mode   # Continuous flush (30s)")
                print("  python cdc_service_prod_fixed.py flush_mode 60  # Continuous flush (60s)")
                exit(1)
        else:
            # Default: run normal CDC mode
            cdc.run_forever()

    except Exception as e:
        logger.critical(f"? FATAL ERROR: {e}")
        logger.critical(f"Traceback: {traceback.format_exc()}")
        exit(1)
