import oracledb
import boto3
import json
import time
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from functools import wraps
from config_temp import (
    ORACLE_CONFIG, AWS_CONFIG,
    SQS_OUTBOUND_QUEUE_URL, SQS_INBOUND_QUEUE_URL,
    CDC_CONFIG, MESSAGE_TYPES, REQUIRED_TABLES,
    SOURCE_TABLE, SOURCE_SCHEMA, TABLE_SOURCES, setup_logging
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

def log_function_call(func):
    """Decorator to log function entry, exit, and timing"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        logger.debug(f"-> Entering {func_name}")

        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = (time.time() - start_time) * 1000  # ms

            # Log to performance logger
            perf_logger.info(f"{func_name} completed in {elapsed:.2f}ms")

            logger.debug(f"<- Exiting {func_name} | Time: {elapsed:.2f}ms")
            return result

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            logger.error(f"X Exception in {func_name} after {elapsed:.2f}ms: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    return wrapper


# ============================
# Oracle CDC Service - Multi-Source
# ============================

class OracleCDCService:
    """
    On-Premises Oracle CDC Service with Multi-Source Support
    Supports: VERINT_TEXT_ANALYSIS and SF_OC_TEXT_ANALYSIS_TEMP
    """

    def __init__(self):
        logger.info("="*80)
        logger.info("INIT: Oracle CDC Service (Multi-Source)")
        logger.info("="*80)

        self.oracle_conn = None
        self.sqs_client = None
        self.is_running = False
        self.tables_validated = False
        self.startup_time = datetime.utcnow()

        # Track which source each pending call came from (for write_ml_result)
        self.pending_source_types = {}  # {call_id: 'CALL' or 'WAPP'}

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

        # Log enabled sources
        enabled_sources = [sid for sid, cfg in TABLE_SOURCES.items() if cfg['enabled']]
        logger.info(f"Enabled sources: {enabled_sources}")

    # ============================
    # Connection Management
    # ============================

    @log_function_call
    def connect_oracle(self) -> bool:
        """Establish Oracle connection with detailed logging"""
        oracle_logger.info("Attempting Oracle connection...")
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

            oracle_logger.info(f"OK Oracle connected successfully")
            oracle_logger.info(f"   Database time: {db_time}")
            oracle_logger.info(f"   Oracle version: {self.oracle_conn.version}")

            return True

        except oracledb.DatabaseError as e:
            error_obj, = e.args
            oracle_logger.error(f"ERR Oracle DatabaseError:")
            oracle_logger.error(f"   Code: {error_obj.code}")
            oracle_logger.error(f"   Message: {error_obj.message}")
            oracle_logger.error(f"   Context: {error_obj.context}")
            return False

        except Exception as e:
            oracle_logger.error(f"ERR Failed to connect to Oracle: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            return False

    @log_function_call
    def connect_sqs(self) -> bool:
        """Establish AWS SQS connection with detailed logging"""
        sqs_logger.info("Attempting SQS connection...")
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

            self.sqs_client = boto3.client('sqs', **sqs_config)

            # Test connection and get queue attributes
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=SQS_OUTBOUND_QUEUE_URL,
                AttributeNames=['All']
            )


            attrs = response.get('Attributes', {})
            sqs_logger.info(f"OK SQS Outbound connected successfully")

            sqs_logger.info(f"OK SQS connected successfully")
            sqs_logger.info(f"   Queue ARN: {attrs.get('QueueArn', 'N/A')}")
            sqs_logger.info(f"   Messages available: {attrs.get('ApproximateNumberOfMessages', '0')}")
            sqs_logger.info(f"   Messages in flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', '0')}")
            sqs_logger.info(f"   Visibility timeout: {attrs.get('VisibilityTimeout', 'N/A')}s")

            response_inbound = self.sqs_client.get_queue_attributes(

                QueueUrl=SQS_INBOUND_QUEUE_URL,

                AttributeNames=['All']

            )

            attrs_inbound = response_inbound.get('Attributes',{})
            sqs_logger.info(f"OK SQS Inbound connected successfully")


            return True

        except Exception as e:
            sqs_logger.error(f"ERR Failed to connect to SQS: {e}")
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
        oracle_logger.info("Validating required tables...")

        cursor = self.oracle_conn.cursor()
        all_valid = True

        try:
            # Check CDC tables (in current user schema)
            oracle_logger.info(f"Checking CDC tables in current schema:")
            for table_name in REQUIRED_TABLES:
                exists = self._check_table_exists(cursor, table_name, None)

                if exists:
                    row_count = self._get_table_row_count(cursor, table_name, None)
                    oracle_logger.info(f"   OK {table_name} exists ({row_count} rows)")
                else:
                    oracle_logger.error(f"   ERR {table_name} MISSING")
                    all_valid = True

            # Check source tables
            oracle_logger.info(f"Checking source tables:")
            for source_id, source in TABLE_SOURCES.items():
                if not source['enabled']:
                    continue
                exists = self._check_table_exists(cursor, source['table_name'], SOURCE_SCHEMA)
                if exists:
                    oracle_logger.info(f"   OK [{source_id}] {source['table_name']} exists")
                else:
                    oracle_logger.warning(f"   WARN [{source_id}] {source['table_name']} MISSING")

            self.tables_validated = all_valid

            if all_valid:
                oracle_logger.info("OK All required tables validated successfully")
            else:
                oracle_logger.error("ERR Some required tables are missing")
                oracle_logger.error("   Run init_oracle_tables.sql to create missing tables")

            return all_valid

        except Exception as e:
            oracle_logger.error(f"ERR Error validating tables: {e}")
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
    def _get_table_row_count(self, cursor, table_name: str, schema: Optional[str]) -> int:
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
        oracle_logger.info("Creating CDC tables...")

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
                    oracle_logger.info(f"   OK Created table: {table_name}")
                else:
                    oracle_logger.info(f"   INFO Table already exists: {table_name}")

            except Exception as e:
                oracle_logger.error(f"   ERR Failed to create table {table_name}: {e}")
                oracle_logger.error(f"      Traceback: {traceback.format_exc()}")

        # Initialize CDC_PROCESSING_STATUS for each source
        try:
            oracle_logger.info("   Initializing CDC_PROCESSING_STATUS...")

            # VERINT source
            cursor.execute("""
                MERGE INTO CDC_PROCESSING_STATUS t
                USING (SELECT 'CDC_NORMAL_MODE' AS name FROM dual) s
                ON (t.TABLE_NAME = s.name)
                WHEN NOT MATCHED THEN
                    INSERT (TABLE_NAME, LAST_PROCESSED_TIMESTAMP, IS_ENABLED)
                    VALUES ('CDC_NORMAL_MODE', SYSTIMESTAMP - 1, 1)
            """)

            # SF_OC source
            cursor.execute("""
                MERGE INTO CDC_PROCESSING_STATUS t
                USING (SELECT 'CDC_NORMAL_MODE_SF_OC' AS name FROM dual) s
                ON (t.TABLE_NAME = s.name)
                WHEN NOT MATCHED THEN
                    INSERT (TABLE_NAME, LAST_PROCESSED_TIMESTAMP, IS_ENABLED)
                    VALUES ('CDC_NORMAL_MODE_SF_OC', SYSTIMESTAMP - 1, 1)
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
            oracle_logger.info("   OK CDC_PROCESSING_STATUS initialized")

        except Exception as e:
            oracle_logger.error(f"   ERR Failed to initialize CDC status: {e}")
            oracle_logger.error(f"      Traceback: {traceback.format_exc()}")

        cursor.close()
        oracle_logger.info("OK Table creation process completed")

    # ============================
    # CDC Collection - Multi-Source
    # ============================

    @log_function_call
    def collect_new_calls_for_source(self, source_id: str) -> List[str]:
        """Collect new records from specified source table"""
        source = TABLE_SOURCES[source_id]
        oracle_logger.info(f"[{source_id}] Scanning for new records...")

        cursor = self.oracle_conn.cursor()

        try:
            # Build dynamic query based on source config
            id_col = source['id_column']
            time_col = source['time_column']
            table_name = source['table_name']
            index_hint = source['index_hint']
            time_filter = source['time_filter']
            base_filter = source['base_filter']

            # Build WHERE clause
            where_parts = [time_filter]
            if base_filter:
                where_parts.append(base_filter)
            where_clause = ' AND '.join(where_parts)

            query = f"""
                SELECT {index_hint}
                DISTINCT {id_col}, {time_col}
                FROM {table_name}
                WHERE {where_clause}
                AND {id_col} NOT IN (
                    SELECT CALL_ID FROM CDC_PROCESSED_CALLS
                    WHERE TEXT_TIME > SYSDATE - (420 / 1440)
                )
                ORDER BY {time_col} ASC
                FETCH FIRST :batch_size ROWS ONLY
            """

            oracle_logger.debug(f"[{source_id}] Executing query:\n{query}")
            oracle_logger.debug(f"[{source_id}] Parameters: batch_size={CDC_CONFIG['max_batch_size']}")

            cursor.execute(query, {'batch_size': CDC_CONFIG['max_batch_size']})
            rows = cursor.fetchall()

            record_ids = [row[0] for row in rows]

            if record_ids:
                oracle_logger.info(f"[{source_id}] OK Found {len(record_ids)} new record(s)")
                oracle_logger.debug(f"[{source_id}]    IDs: {record_ids[:10]}{'...' if len(record_ids) > 10 else ''}")
            else:
                oracle_logger.debug(f"[{source_id}]    No new records found")

            return record_ids

        except Exception as e:
            oracle_logger.error(f"[{source_id}] ERR Error collecting records: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            return []
        finally:
            cursor.close()

    # ============================
    # Conversation Assembly - Multi-Source
    # ============================

    @log_function_call
    def assemble_conversation_for_source(self, record_id: str, source_id: str) -> Optional[Dict[str, Any]]:
        """Fetch all segments for a record and assemble into conversation"""
        source = TABLE_SOURCES[source_id]
        oracle_logger.debug(f"[{source_id}] Assembling conversation: {record_id}")

        cursor = self.oracle_conn.cursor()

        # Configure cursor to fetch CLOB content as strings automatically
        cursor.prefetchrows = 0
        cursor.arraysize = 100

        try:
            # Build dynamic query
            id_col = source['id_column']
            text_time_col = source['text_time_column']
            table_name = source['table_name']
            base_filter = source['base_filter']

            # Build WHERE clause
            where_parts = [f"{id_col} = :record_id"]
            if base_filter:
                where_parts.append(base_filter)
            where_clause = ' AND '.join(where_parts)

            query = f"""
                SELECT
                    {id_col}, BAN, SUBSCRIBER_NO, OWNER,
                    {text_time_col}, DBMS_LOB.SUBSTR(TEXT, 4000, 1) as TEXT
                FROM {table_name}
                WHERE {where_clause}
                ORDER BY {text_time_col} ASC
            """

            cursor.execute(query, {'record_id': record_id})
            rows = cursor.fetchall()

            if not rows:
                oracle_logger.warning(f"[{source_id}] WARN No data found for: {record_id}")
                return None

            # Check minimum segments
            min_segments = source['min_segments']
            if len(rows) < min_segments:
                oracle_logger.warning(f"[{source_id}] WARN Conversation too short ({len(rows)} rows, need {min_segments}): {record_id}")
                return None

            oracle_logger.debug(f"[{source_id}]    Found {len(rows)} segment(s)")

            # Check for conversation completeness using source-specific channels
            channels = set(row[3] for row in rows if row[3])
            oracle_logger.debug(f"[{source_id}]    Channels present: {channels}")

            # required_channels = channels that MUST be present (e.g., {'A', 'C'} for calls, {'C'} for chat)
            required_channels = source.get('required_channels', source['valid_channels'])
            if not required_channels.issubset(channels):
                missing = required_channels - channels
                oracle_logger.warning(f"[{source_id}] WARN Incomplete conversation (missing {missing}): {record_id}")
                oracle_logger.debug(f"[{source_id}]       Channels: {channels}, Required: {required_channels}")
                return None

            # Filter out unknown channels (keep only valid ones)
            valid_channels = source['valid_channels']
            unknown_channels = channels - valid_channels
            if unknown_channels:
                oracle_logger.debug(f"[{source_id}]    Unknown channels will be included: {unknown_channels}")

            # Assemble messages
            messages = []
            for idx, row in enumerate(rows):
                # TEXT is now extracted as string via DBMS_LOB.SUBSTR
                text_content = row[5]
                if text_content and str(text_content).strip():
                    messages.append({
                        'channel': row[3],  # OWNER ('C', 'A', or 'B')
                        'text': str(text_content),  # TEXT extracted as string from CLOB
                        'timestamp': row[4].isoformat() if row[4] else None
                    })
                else:
                    oracle_logger.debug(f"[{source_id}]    Skipping empty text for row {idx}")

            # Build conversation object
            conversation = {
                'type': MESSAGE_TYPES['CONVERSATION_TO_ML'],
                'callId': str(record_id),  # Convert to string for JSON
                'ban': rows[0][1],          # BAN
                'subscriberNo': rows[0][2],  # SUBSCRIBER_NO
                'callTime': rows[0][4].isoformat() if rows[0][4] else None,
                'messages': messages,
                'messageCount': len(messages),
                'assembledAt': datetime.utcnow().isoformat(),
                'source': 'on-premises-cdc',
                'sourceId': source_id,  # Track which source table
            }

            oracle_logger.info(f"[{source_id}] OK Assembled: {record_id} ({len(messages)} messages, channels: {channels})")
            return conversation

        except Exception as e:
            oracle_logger.error(f"[{source_id}] ERR Error assembling conversation {record_id}: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            return None
        finally:
            cursor.close()

    # ============================
    # SQS Communication - Send
    # ============================

    @log_function_call
    def send_to_sqs(self, conversation: Dict[str, Any], source_id: str) -> Optional[str]:
        """Send conversation to AWS SQS for ML processing"""
        call_id = conversation.get('callId', 'UNKNOWN')
        sqs_logger.info(f"[{source_id}] Sending to SQS: {call_id}")

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
                        'StringValue': str(call_id),
                        'DataType': 'String'
                    },
                    'sourceId': {
                        'StringValue': source_id,
                        'DataType': 'String'
                    },
                    'timestamp': {
                        'StringValue': datetime.utcnow().isoformat(),
                        'DataType': 'String'
                    }
                }
            )

            message_id = response.get('MessageId')
            sqs_logger.info(f"[{source_id}] OK Sent successfully: {call_id} -> SQS Message ID: {message_id}")

            # Track source type for when ML result returns
            source = TABLE_SOURCES[source_id]
            self.pending_source_types[str(call_id)] = source['dest_source_type']

            # Mark as processed
            self.mark_call_processed(call_id, message_id, source_id)

            # Update stats
            self.stats['total_sqs_sent'] += 1

            return message_id

        except Exception as e:
            sqs_logger.error(f"[{source_id}] ERR Failed to send to SQS: {call_id}")
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
        sqs_logger.debug("Polling SQS inbound queue for ML results...")

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
                sqs_logger.info(f"Received {len(messages)} message(s) from SQS inbound queue")

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
                        sqs_logger.info(f"   ML Result for: {call_id}")

                        # Process ML result - writes to DICTA_CALL_SUMMARY, CONVERSATION_SUMMARY, CONVERSATION_CATEGORY
                        success = self.write_ml_result(body)

                        if success:
                            # Delete message from inbound queue after successful processing
                            self.sqs_client.delete_message(
                                QueueUrl=SQS_INBOUND_QUEUE_URL,
                                ReceiptHandle=message['ReceiptHandle']
                            )

                            sqs_logger.info(f"   OK Processed and deleted: {message_id}")
                            self.stats['total_ml_results_received'] += 1
                    else:
                        sqs_logger.debug(f"   Skipping message type: {msg_type}")

                except json.JSONDecodeError as e:
                    sqs_logger.error(f"   ERR Invalid JSON in message: {e}")

                except Exception as e:
                    sqs_logger.error(f"   ERR Failed to process SQS message: {e}")
                    sqs_logger.error(f"      Traceback: {traceback.format_exc()}")

        except Exception as e:
            sqs_logger.error(f"ERR Error receiving from SQS: {e}")
            sqs_logger.error(f"   Traceback: {traceback.format_exc()}")

    # ============================
    # Database Updates
    # ============================

    @log_function_call
    def write_ml_result(self, result: Dict[str, Any]) -> bool:
        """Write ML processing result to DICTA_CALL_SUMMARY table"""
        call_id = result.get('callId', 'UNKNOWN')
        oracle_logger.info(f"Writing ML result: {call_id}")

        # Get source type from tracking dict (default to 'CALL' for backwards compatibility)
        source_type = self.pending_source_types.pop(str(call_id), 'CALL')
        oracle_logger.info(f"   Source type: {source_type}")

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
                sentiment_map = {'positive': 4, 'negative': 2,
                               'neutral': 3, 'mixed': 3, 'unknown': 3}
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
                'all_classifications': ', '.join(all_classifications) if isinstance(all_classifications, list) else str(all_classifications),                'confidence': float(result.get('confidence', 0.0)) if result.get('confidence') is not None else 0.0,
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


            # Also save to CONVERSATION_SUMMARY - use source_type from tracking
            try:
                # Extract sentiment - can be dict {'overall': '...', 'score': 0.8} or string
                sentiment_raw = result.get('sentiment', {})
                if isinstance(sentiment_raw, dict):
                    sentiment_value = sentiment_raw.get('overall', 'neutral')
                else:
                    sentiment_value = str(sentiment_raw) if sentiment_raw else 'neutral'

                # Log what we're about to insert for debugging
                products_val = result.get('products', '[]')
                action_items_val = result.get('action_items', '[]')
                unresolved_val = result.get('unresolved_issues', '')
                satisfaction_val = result.get('customer_satisfaction', 3)

                # Determine which source table to query for BAN, SUBSCRIBER_NO, etc.
                # We need to query the appropriate table based on source_type
                if source_type == 'WAPP':
                    # SF_OC table
                    source = TABLE_SOURCES['sf_oc']
                    id_col = source['id_column']
                    text_time_col = source['text_time_column']
                    table_name = source['table_name']
                    base_filter = source['base_filter']

                    query = f"""
                        SELECT BAN, SUBSCRIBER_NO, {text_time_col}
                        FROM {table_name}
                        WHERE {id_col} = :call_id
                        {f'AND {base_filter}' if base_filter else ''}
                        AND ROWNUM = 1
                    """
                else:
                    # VERINT table (default)
                    query = """
                        SELECT BAN, SUBSCRIBER_NO, CALL_TIME
                        FROM VERINT_TEXT_ANALYSIS
                        WHERE CALL_ID = :call_id
                        AND CALL_TIME > SYSDATE - (120 / 1440)
                        AND ROWNUM = 1
                    """

                cursor.execute(query, {'call_id': call_id})
                source_row = cursor.fetchone()

                ban_val = source_row[0] if source_row else None
                subscriber_no_val = source_row[1] if source_row else None
                text_time_val = source_row[2] if source_row else None

                oracle_logger.info(f"CONVERSATION_SUMMARY data for {call_id}:")
                oracle_logger.info(f"   source_type: {source_type}")
                oracle_logger.info(f"   ban: {ban_val}, subscriber_no: {subscriber_no_val}, text_time: {text_time_val}")
                oracle_logger.info(f"   products: {products_val}")
                oracle_logger.info(f"   action_items: {action_items_val}")
                oracle_logger.info(f"   unresolved_issues: {unresolved_val}")
                oracle_logger.info(f"   satisfaction: {satisfaction_val}")
                oracle_logger.info(f"   sentiment: {sentiment_value}")

                # Use DELETE + INSERT instead of MERGE to avoid partition key issues
                cursor.execute("""
                    DELETE FROM CONVERSATION_SUMMARY
                    WHERE SOURCE_ID = :source_id AND SOURCE_TYPE = :source_type
                """, {'source_id': call_id, 'source_type': source_type})

                cursor.execute("""
                    INSERT INTO CONVERSATION_SUMMARY (
                        source_type, source_id, creation_date, summary,
                        satisfaction, sentiment, products, unresolved_issues, action_items,
                        ban, subscriber_no, text_time
                    ) VALUES (
                        :source_type, :source_id, SYSDATE, :summary,
                        :satisfaction, :sentiment, :products, :unresolved_issues, :action_items,
                        :ban, :subscriber_no, :text_time
                    )
                """, {
                    'source_type': source_type,  # 'CALL' or 'WAPP'
                    'source_id': call_id,
                    'summary': summary_text[:4000] if summary_text else '',
                    'satisfaction': satisfaction_val,
                    'sentiment': sentiment_value,
                    'products': products_val,
                    'unresolved_issues': unresolved_val,
                    'action_items': action_items_val,
                    'ban': ban_val,
                    'subscriber_no': subscriber_no_val,
                    'text_time': text_time_val
                })
                self.oracle_conn.commit()
                oracle_logger.info(f"OK Conversation summary written: {call_id} (source_type={source_type})")
            except Exception as e:
                oracle_logger.error(f"ERR Failed to write to CONVERSATION_SUMMARY for {call_id}: {e}")
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

                oracle_logger.info(f"Inserting {len(all_classifications)} categories for {call_id}: {all_classifications}")

                # Delete existing categories for this call (avoid duplicates on reprocess)
                cursor.execute("""
                    DELETE FROM CONVERSATION_CATEGORY
                    WHERE SOURCE_ID = :source_id AND SOURCE_TYPE = :source_type
                """, {'source_id': call_id, 'source_type': source_type})

                # Insert each classification as a separate row
                categories_inserted = 0
                for category_code in all_classifications:
                    cursor.execute("""
                        INSERT INTO CONVERSATION_CATEGORY (SOURCE_ID, SOURCE_TYPE, CREATION_DATE, CATEGORY_CODE)
                        VALUES (:source_id, :source_type, SYSDATE, :category_code)
                    """, {
                        'source_id': call_id,
                        'source_type': source_type,  # 'CALL' or 'WAPP'
                        'category_code': str(category_code)[:255]  # Truncate if needed
                    })
                    categories_inserted += 1

                self.oracle_conn.commit()
                oracle_logger.info(f"OK Conversation categories written: {call_id} ({categories_inserted} categories, source_type={source_type})")
            except Exception as e:
                oracle_logger.error(f"ERR Failed to write to CONVERSATION_CATEGORY for {call_id}: {e}")
                oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
                self.oracle_conn.rollback()
                return False


            oracle_logger.info(f"OK ML result written: {call_id}")
            self.stats['total_ml_results_written'] += 1



            return True

        except Exception as e:
            oracle_logger.error(f"ERR Failed to write ML result for {call_id}: {e}")
            oracle_logger.error(f"   Traceback: {traceback.format_exc()}")
            self.oracle_conn.rollback()
            return False
        finally:
            cursor.close()

    @log_function_call
    def mark_call_processed(self, call_id: str, sqs_message_id: str, source_id: str):
        """Mark call as processed in CDC_PROCESSED_CALLS"""
        oracle_logger.debug(f"[{source_id}] Marking processed: {call_id}")

        source = TABLE_SOURCES[source_id]
        cursor = self.oracle_conn.cursor()

        try:
            # First check if already processed
            cursor.execute("""
                SELECT COUNT(*) FROM CDC_PROCESSED_CALLS
                WHERE CALL_ID = :call_id
            """, {'call_id': str(call_id)})

            if cursor.fetchone()[0] == 0:
                # Not processed yet, insert it
                # Use source-specific query to get the max text time
                text_time_col = source['text_time_column']
                table_name = source['table_name']
                id_col = source['id_column']
                base_filter = source['base_filter']

                # Build the subquery to get max text time
                query = f"""
                    INSERT INTO CDC_PROCESSED_CALLS (CALL_ID, SQS_MESSAGE_ID, TEXT_TIME)
                    SELECT :call_id, :msg_id, MAX({text_time_col})
                    FROM {table_name}
                    WHERE {id_col} = :call_id
                    {f'AND {base_filter}' if base_filter else ''}
                """

                cursor.execute(query, {
                    'call_id': str(call_id),
                    'msg_id': sqs_message_id
                })
                self.oracle_conn.commit()
                oracle_logger.debug(f"[{source_id}]    OK Marked: {call_id}")
            else:
                oracle_logger.debug(f"[{source_id}]    INFO Already processed: {call_id}")

        except Exception as e:
            oracle_logger.error(f"[{source_id}]    ERR Failed to mark call processed: {e}")
        finally:
            cursor.close()

    @log_function_call
    def update_cdc_status(self, mode: str, timestamp: datetime):
        """Update CDC processing status"""
        oracle_logger.debug(f"Updating CDC status: {mode} -> {timestamp}")

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
                logger.info(f"OK SQS flush complete. Total messages processed: {total_processed}")
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
                            logger.info(f"   OK Processed and deleted: {message_id}")
                        else:
                            logger.error(f"   ERR Failed to process ML_RESULT message: {message_id}")
                    else:
                        logger.info(f"   Skipping message type: {msg_type}")
                        # Do NOT delete the message

                except Exception as e:
                    logger.error(f"   ERR Error processing SQS message: {e}")
                    logger.error(f"      Traceback: {traceback.format_exc()}")


        logger.info(f"All SQS messages flushed to database.")


    # ============================
    # Statistics & Health
    # ============================

    @log_function_call
    def print_statistics(self):
        """Print current statistics"""
        logger.info("="*80)
        logger.info("CDC SERVICE STATISTICS")
        logger.info("="*80)
        logger.info(f"Uptime: {datetime.utcnow() - self.startup_time}")
        logger.info(f"Cycles completed: {self.stats['cycles_completed']}")
        logger.info(f"Calls processed: {self.stats['total_calls_processed']}")
        logger.info(f"Calls failed: {self.stats['total_calls_failed']}")
        logger.info(f"SQS sent: {self.stats['total_sqs_sent']}")
        logger.info(f"SQS failed: {self.stats['total_sqs_failed']}")
        logger.info(f"ML results received: {self.stats['total_ml_results_received']}")
        logger.info(f"ML results written: {self.stats['total_ml_results_written']}")
        logger.info(f"Pending source types: {len(self.pending_source_types)}")
        logger.info(f"Last cycle: {self.stats['last_cycle_time']}")
        logger.info("="*80)

    # ============================
    # Main Processing Loop - Multi-Source
    # ============================

    @log_function_call
    def process_batch_for_source(self, record_ids: List[str], source_id: str):
        """Process a batch of record IDs for a specific source"""
        source = TABLE_SOURCES[source_id]
        mode = source['cdc_mode_key']

        logger.info(f"[{source_id}] Processing batch of {len(record_ids)} records")

        for idx, record_id in enumerate(record_ids, 1):
            try:
                logger.debug(f"[{source_id}]    [{idx}/{len(record_ids)}] Processing: {record_id}")

                # Assemble conversation using source-specific method
                conversation = self.assemble_conversation_for_source(record_id, source_id)

                if conversation:
                    # Send to SQS
                    message_id = self.send_to_sqs(conversation, source_id)

                    if message_id:
                        # Update status
                        self.update_cdc_status(mode, datetime.utcnow())
                        self.stats['total_calls_processed'] += 1
                    else:
                        self.stats['total_calls_failed'] += 1
                else:
                    self.stats['total_calls_failed'] += 1

            except Exception as e:
                logger.error(f"[{source_id}]    ERR Error processing record {record_id}: {e}")
                self.log_error(record_id, str(e), 'PROCESSING_ERROR')
                self.stats['total_calls_failed'] += 1

        logger.info(f"[{source_id}] OK Batch complete: {len(record_ids)} records processed")

    def run_forever(self):
        """Main 24/7 processing loop - processes all configured sources"""
        logger.info("="*80)
        logger.info("ORACLE CDC SERVICE - STARTING 24/7 MODE (Multi-Source)")
        logger.info("="*80)

        # Initialize connections
        if not self.connect_oracle():
            logger.error("ERR Cannot start without Oracle connection")
            return

        if not self.connect_sqs():
            logger.error("ERR Cannot start without SQS connection")
            return

        # Validate tables
        if not self.validate_tables():
            logger.warning("WARN Some tables missing - attempting to create...")
            self.create_tables()

            # Re-validate
            if not self.validate_tables():
                logger.error("ERR Cannot start - required tables still missing")
                return

        self.is_running = True
        logger.info("OK All systems ready - starting CDC loop")
        logger.info("="*80)

        while self.is_running:
            try:
                cycle_start = time.time()
                self.stats['cycles_completed'] += 1
                cycle_num = self.stats['cycles_completed']

                logger.info(f"")
                logger.info(f"{'='*80}")
                logger.info(f"CDC CYCLE #{cycle_num} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*80}")

                # Process each configured source
                for source_id, source in TABLE_SOURCES.items():
                    if not source['enabled']:
                        logger.debug(f"[{source_id}] Skipping (disabled)")
                        continue

                    logger.info(f"[{source_id}] Processing normal mode...")

                    # Collect records for this source
                    record_ids = self.collect_new_calls_for_source(source_id)
                    if record_ids:
                        self.process_batch_for_source(record_ids, source_id)

                # Check for ML results (shared for all sources)
                self.receive_ml_results()

                # Cycle complete
                cycle_time = time.time() - cycle_start
                self.stats['last_cycle_time'] = datetime.utcnow()

                logger.info(f"OK Cycle #{cycle_num} complete in {cycle_time:.2f}s")

                # Print stats every 10 cycles
                if cycle_num % 10 == 0:
                    self.print_statistics()

                # Sleep
                logger.info(f"Sleeping {CDC_CONFIG['normal_poll_interval_seconds']}s until next cycle...")
                time.sleep(CDC_CONFIG['normal_poll_interval_seconds'])

            except KeyboardInterrupt:
                logger.warning("WARN SHUTDOWN SIGNAL RECEIVED (Ctrl+C)")
                self.is_running = False

            except Exception as e:
                logger.error(f"ERR CRITICAL ERROR in main loop: {e}")
                logger.error(f"   Traceback: {traceback.format_exc()}")
                logger.warning("   Waiting 30s before retry...")
                time.sleep(30)

        # Cleanup
        logger.info("="*80)
        logger.info("SHUTTING DOWN CDC SERVICE")
        logger.info("="*80)

        self.print_statistics()

        if self.oracle_conn:
            self.oracle_conn.close()
            oracle_logger.info("OK Oracle connection closed")

        logger.info("Oracle CDC Service stopped gracefully")


# ============================
# Entry Point
# ============================

if __name__ == '__main__':
    import sys
    try:
        cdc = OracleCDCService()
        if len(sys.argv) > 1 and sys.argv[1] == 'flush_sqs':
            cdc.connect_oracle()
            cdc.connect_sqs()
            cdc.flush_all_sqs_to_db()
        else:
            cdc.run_forever()
    except Exception as e:
        logger.critical(f"FATAL ERROR: {e}")
        logger.critical(f"Traceback: {traceback.format_exc()}")
        exit(1)
