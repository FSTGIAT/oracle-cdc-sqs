#!/usr/bin/env python3
import oracledb
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Oracle connection parameters
ORACLE_USER = os.getenv('ORACLE_USER', 'call_analytics')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD', 'CallAnalytics2024!')
ORACLE_HOST = os.getenv('ORACLE_HOST', 'localhost')
ORACLE_PORT = int(os.getenv('ORACLE_PORT', '1521'))
ORACLE_SERVICE = os.getenv('ORACLE_SERVICE_NAME', 'XE')

def main():
    """Reset CDC tables and update VERINT_TEXT_ANALYSIS timestamps"""

    try:
        # Create Oracle connection
        dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
        connection = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn
        )
        cursor = connection.cursor()

        print("Connected to Oracle database")

        # 1. Delete from CDC_PROCESSED_CALLS
        print("\n1. Deleting records from CDC_PROCESSED_CALLS...")
        cursor.execute("DELETE FROM CDC_PROCESSED_CALLS")
        deleted_processed = cursor.rowcount
        print(f"   Deleted {deleted_processed} records from CDC_PROCESSED_CALLS")

        # 2. Delete from CDC_CONVERSATION_ASSEMBLY
        print("\n2. Deleting records from CDC_CONVERSATION_ASSEMBLY...")
        cursor.execute("DELETE FROM CDC_CONVERSATION_ASSEMBLY")
        deleted_assembly = cursor.rowcount
        print(f"   Deleted {deleted_assembly} records from CDC_CONVERSATION_ASSEMBLY")

        # 3. Check current records in VERINT_TEXT_ANALYSIS
        print("\n3. Checking VERINT_TEXT_ANALYSIS records...")
        cursor.execute("""
            SELECT COUNT(*), MIN(TEXT_TIME), MAX(TEXT_TIME)
            FROM call_analytics.VERINT_TEXT_ANALYSIS
        """)
        count, min_time, max_time = cursor.fetchone()
        print(f"   Found {count} records")
        if count > 0:
            print(f"   Current time range: {min_time} to {max_time}")

        # 4. Update TEXT_TIME to current timestamp
        if count > 0:
            print("\n4. Updating TEXT_TIME to current timestamp...")
            # Get current time
            current_time = datetime.now()

            # Update all records to current timestamp
            cursor.execute("""
                UPDATE call_analytics.VERINT_TEXT_ANALYSIS
                SET TEXT_TIME = :current_time
            """, {'current_time': current_time})

            updated_count = cursor.rowcount
            print(f"   Updated {updated_count} records with timestamp: {current_time}")

            # Verify the update
            cursor.execute("""
                SELECT COUNT(*), MIN(TEXT_TIME), MAX(TEXT_TIME)
                FROM call_analytics.VERINT_TEXT_ANALYSIS
            """)
            count, min_time, max_time = cursor.fetchone()
            print(f"   New time range: {min_time} to {max_time}")

        # 5. Commit the changes
        connection.commit()
        print("\n5. Changes committed successfully!")

        # 6. Show sample of updated records
        print("\n6. Sample of updated records:")
        cursor.execute("""
            SELECT CALL_ID, _OWNER, TEXT_TIME, SUBSTR(TEXT, 1, 50) as TEXT_PREVIEW
            FROM call_analytics.VERINT_TEXT_ANALYSIS
            WHERE ROWNUM <= 5
            ORDER BY _OWNER, TEXT_TIME
        """)

        print("\n   CALL_ID | OWNER    | TEXT_TIME           | TEXT_PREVIEW")
        print("   " + "-" * 80)
        for row in cursor:
            call_id, owner, text_time, text_preview = row
            print(f"   {call_id:7} | {owner:8} | {text_time} | {text_preview}...")

        cursor.close()
        connection.close()
        print("\n✅ Database reset and update completed successfully!")
        print("\nYou can now run the CDC service with: python3 cdc_service.py")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()