"""
Historical Backfill Service for VERINT_TEXT_ANALYSIS
Processes 90 days of historical data and sends to SQS

Phase 1 (BULK): Full table scan with PARALLEL hint
Phase 2 (DELTA): Index-based 2-hour queries until caught up
Exits when complete (one-shot)
"""

import oracledb
import boto3
import json
import time
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Load .env from the same directory as this file
ENV_PATH = Path(__file__).parent / '.env'
load_dotenv(ENV_PATH)

from config import (
    ORACLE_CONFIG, AWS_CONFIG,
    SQS_OUTBOUND_QUEUE_URL,
    setup_logging
)

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)
oracle_logger = logging.getLogger('oracle')
sqs_logger = logging.getLogger('sqs')


class BackfillService:
    """
    Historical backfill for VERINT_TEXT_ANALYSIS
    Phase 1: ONE-TIME bulk 90-day scan with PARALLEL hint (no batching)
    Phase 2: Delta 2-hour queries with INDEX hint (batched)
    """

    def __init__(self):
        self.oracle_conn = None
        self.sqs_client = None

        # Configuration
        self.days_back = 90
        self.bulk_batch_size = 1000  # Memory-efficient batching for BULK
        self.delta_batch_size = 50   # Smaller batches for DELTA phase
        self.min_segments = 16

        # State
        self.phase = 'BULK'  # BULK or DELTA
        self.total_processed = 0
        self.total_sent = 0
        self.total_skipped = 0
        self.start_time = datetime.now()

    def connect_oracle(self) -> bool:
        """Connect to Oracle database"""
        try:
            oracle_logger.info("Connecting to Oracle...")
            oracle_logger.info(f"  Host: {ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}")
            oracle_logger.info(f"  Service: {ORACLE_CONFIG['service_name']}")

            dsn = oracledb.makedsn(
                ORACLE_CONFIG['host'],
                ORACLE_CONFIG['port'],
                service_name=ORACLE_CONFIG['service_name']
            )

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

            oracle_logger.info(f"Oracle connected. DB time: {db_time}")
            return True

        except Exception as e:
            oracle_logger.error(f"Oracle connection failed: {e}")
            return False

    def connect_sqs(self) -> bool:
        """Connect to AWS SQS"""
        try:
            sqs_logger.info("Connecting to SQS...")

            self.sqs_client = boto3.client(
                'sqs',
                region_name=AWS_CONFIG['region_name'],
                aws_access_key_id=AWS_CONFIG.get('aws_access_key_id'),
                aws_secret_access_key=AWS_CONFIG.get('aws_secret_access_key'),
                aws_session_token=AWS_CONFIG.get('aws_session_token')
            )

            # Test connection
            self.sqs_client.get_queue_attributes(
                QueueUrl=SQS_OUTBOUND_QUEUE_URL,
                AttributeNames=['QueueArn']
            )

            sqs_logger.info(f"SQS connected. Queue: {SQS_OUTBOUND_QUEUE_URL}")
            return True

        except Exception as e:
            sqs_logger.error(f"SQS connection failed: {e}")
            return False

    def collect_bulk_calls(self) -> List[Dict]:
        """
        Phase 1: Full table scan with PARALLEL hint
        BATCHED query for unprocessed calls (memory efficient)
        """
        query = """
        SELECT /*+ FULL(VERINT_TEXT_ANALYSIS) PARALLEL(VERINT_TEXT_ANALYSIS, 4) */
        DISTINCT CALL_ID, CALL_TIME
        FROM VERINT_TEXT_ANALYSIS
        WHERE CALL_TIME > SYSDATE - :days_back
        AND CALL_ID NOT IN (
            SELECT CALL_ID FROM CDC_PROCESSED_CALLS
            WHERE TEXT_TIME > SYSDATE - :days_back
        )
        ORDER BY CALL_TIME ASC
        FETCH FIRST :batch_size ROWS ONLY
        """

        try:
            oracle_logger.info(f"[BULK] Fetching batch of {self.bulk_batch_size} calls...")
            cursor = self.oracle_conn.cursor()
            cursor.execute(query, {'days_back': self.days_back, 'batch_size': self.bulk_batch_size})

            rows = cursor.fetchall()
            cursor.close()

            calls = [{'call_id': row[0], 'call_time': row[1]} for row in rows]
            oracle_logger.info(f"[BULK] Found {len(calls)} calls in this batch")

            return calls

        except Exception as e:
            oracle_logger.error(f"Bulk query failed: {e}")
            return []

    def collect_delta_calls(self) -> List[Dict]:
        """
        Phase 2: Index-based 2-hour window query
        Gets recent unprocessed calls
        """
        query = """
        SELECT /*+ INDEX(VERINT_TEXT_ANALYSIS VERINT_TEXT_ANALYSIS_3ix) */
        DISTINCT CALL_ID, CALL_TIME
        FROM VERINT_TEXT_ANALYSIS
        WHERE CALL_TIME > SYSDATE - 500/1440
        AND CALL_ID NOT IN (
            SELECT CALL_ID FROM CDC_PROCESSED_CALLS
            WHERE TEXT_TIME > SYSDATE - 1200/1440
        )
        ORDER BY CALL_TIME ASC
        FETCH FIRST :batch_size ROWS ONLY
        """

        try:
            cursor = self.oracle_conn.cursor()
            cursor.execute(query, {'batch_size': self.delta_batch_size})

            rows = cursor.fetchall()
            cursor.close()

            calls = [{'call_id': row[0], 'call_time': row[1]} for row in rows]

            if calls:
                oracle_logger.info(f"[DELTA] Found {len(calls)} calls to process")

            return calls

        except Exception as e:
            oracle_logger.error(f"Delta query failed: {e}")
            return []

    def assemble_conversation(self, call_id: str) -> Optional[Dict]:
        """Assemble conversation from all segments for a CALL_ID"""
        query = """
        SELECT
            CALL_ID, BAN, SUBSCRIBER_NO, CALL_TIME, OWNER,
            DBMS_LOB.SUBSTR(TEXT, 4000, 1) AS TEXT_CONTENT
        FROM VERINT_TEXT_ANALYSIS
        WHERE CALL_ID = :call_id
        ORDER BY CALL_TIME ASC
        """

        try:
            cursor = self.oracle_conn.cursor()
            cursor.execute(query, {'call_id': call_id})
            rows = cursor.fetchall()
            cursor.close()

            if len(rows) < self.min_segments:
                logger.debug(f"Skipping {call_id}: only {len(rows)} segments (min: {self.min_segments})")
                return None

            # Check for both Agent (A) and Customer (C) channels
            channels = set(row[4] for row in rows if row[4])
            if 'A' not in channels or 'C' not in channels:
                logger.debug(f"Skipping {call_id}: missing A or C channel")
                return None

            # Build messages array
            messages = []
            for row in rows:
                text = row[5]
                if text and text.strip():
                    messages.append({
                        'channel': row[4],
                        'text': text.strip(),
                        'timestamp': row[3].isoformat() if row[3] else None
                    })

            if not messages:
                return None

            # Build conversation payload
            conversation = {
                'type': 'CONVERSATION_ASSEMBLY',
                'callId': call_id,
                'ban': rows[0][1],
                'subscriberNo': rows[0][2],
                'callTime': rows[0][3].isoformat() if rows[0][3] else None,
                'messages': messages,
                'messageCount': len(messages),
                'assembledAt': datetime.now().isoformat(),
                'source': 'backfill-service'
            }

            return conversation

        except Exception as e:
            logger.error(f"Assembly failed for {call_id}: {e}")
            return None

    def send_to_sqs(self, conversation: Dict) -> bool:
        """Send conversation to SQS"""
        try:
            call_id = conversation['callId']

            response = self.sqs_client.send_message(
                QueueUrl=SQS_OUTBOUND_QUEUE_URL,
                MessageBody=json.dumps(conversation),
                MessageAttributes={
                    'messageType': {'DataType': 'String', 'StringValue': 'CONVERSATION_ASSEMBLY'},
                    'source': {'DataType': 'String', 'StringValue': 'backfill'},
                    'callId': {'DataType': 'String', 'StringValue': call_id}
                }
            )

            message_id = response.get('MessageId')
            sqs_logger.debug(f"Sent {call_id} -> {message_id}")

            return True

        except Exception as e:
            sqs_logger.error(f"SQS send failed: {e}")
            return False

    def mark_processed(self, call_id: str, call_time: datetime) -> bool:
        """Mark call as processed in CDC_PROCESSED_CALLS"""
        try:
            cursor = self.oracle_conn.cursor()
            cursor.execute("""
                MERGE INTO CDC_PROCESSED_CALLS target
                USING (SELECT :call_id AS CALL_ID FROM dual) source
                ON (target.CALL_ID = source.CALL_ID)
                WHEN NOT MATCHED THEN
                    INSERT (CALL_ID, TEXT_TIME, PROCESSED_AT)
                    VALUES (:call_id, :text_time, SYSTIMESTAMP)
            """, {
                'call_id': call_id,
                'text_time': call_time
            })
            self.oracle_conn.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Mark processed failed for {call_id}: {e}")
            return False

    def process_batch(self, calls: List[Dict]):
        """Process a batch of calls"""
        total_calls = len(calls)
        for i, call in enumerate(calls):
            call_id = call['call_id']
            call_time = call['call_time']

            # Assemble conversation
            conversation = self.assemble_conversation(call_id)

            if conversation:
                # Send to SQS
                if self.send_to_sqs(conversation):
                    self.mark_processed(call_id, call_time)
                    self.total_sent += 1
                else:
                    logger.error(f"Failed to send {call_id}")
            else:
                # Mark as processed anyway to avoid re-checking
                self.mark_processed(call_id, call_time)
                self.total_skipped += 1

            self.total_processed += 1

            # Progress update every 100 calls
            if self.total_processed % 100 == 0:
                elapsed = (datetime.now() - self.start_time).total_seconds()
                rate = self.total_processed / elapsed if elapsed > 0 else 0
                pct = (i + 1) / total_calls * 100
                logger.info(f"[{self.phase}] Processed: {self.total_processed} | Sent: {self.total_sent} | Skipped: {self.total_skipped} | Rate: {rate:.1f}/sec | {pct:.1f}%")

    def print_summary(self):
        """Print final summary"""
        elapsed = (datetime.now() - self.start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("BACKFILL COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total processed: {self.total_processed}")
        logger.info(f"Total sent to SQS: {self.total_sent}")
        logger.info(f"Total skipped: {self.total_skipped}")
        logger.info(f"Elapsed time: {elapsed:.1f} seconds")
        logger.info(f"Average rate: {self.total_processed / elapsed:.1f} calls/sec" if elapsed > 0 else "N/A")
        logger.info("=" * 60)

    def run(self):
        """
        Main execution loop
        Phase 1: Bulk until no more records
        Phase 2: Delta until caught up
        Then exit
        """
        logger.info("=" * 60)
        logger.info("BACKFILL SERVICE STARTING")
        logger.info(f"Days back: {self.days_back}")
        logger.info(f"Bulk batch size: {self.bulk_batch_size}")
        logger.info(f"Delta batch size: {self.delta_batch_size}")
        logger.info("=" * 60)

        # Connect
        if not self.connect_oracle():
            logger.error("Failed to connect to Oracle. Exiting.")
            return

        if not self.connect_sqs():
            logger.error("Failed to connect to SQS. Exiting.")
            return

        try:
            # Phase 1: BULK - BATCHED full table scan (memory efficient)
            logger.info("")
            logger.info("=" * 40)
            logger.info(f"PHASE 1: BULK ({self.days_back} days, PARALLEL, batch={self.bulk_batch_size})")
            logger.info("=" * 40)

            bulk_batch_num = 0
            while self.phase == 'BULK':
                bulk_batch_num += 1
                bulk_calls = self.collect_bulk_calls()

                if not bulk_calls:
                    logger.info("BULK phase complete - no more calls to process")
                    break

                logger.info(f"[BULK] Processing batch #{bulk_batch_num} ({len(bulk_calls)} calls)...")
                self.process_batch(bulk_calls)
                time.sleep(0.5)  # Small pause between batches

            self.phase = 'DELTA'

            # Phase 2: DELTA
            logger.info("")
            logger.info("=" * 40)
            logger.info("PHASE 2: DELTA (2 hours, INDEX)")
            logger.info("=" * 40)

            while self.phase == 'DELTA':
                calls = self.collect_delta_calls()

                if not calls:
                    logger.info("Delta phase complete - all caught up!")
                    break

                self.process_batch(calls)
                time.sleep(0.5)

            # Done
            self.print_summary()

        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
            self.print_summary()
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            logger.error(traceback.format_exc())
        finally:
            if self.oracle_conn:
                self.oracle_conn.close()
                logger.info("Oracle connection closed")


if __name__ == '__main__':
    service = BackfillService()
    service.run()
