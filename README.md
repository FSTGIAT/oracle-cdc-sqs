# Oracle CDC to AWS SQS Service

On-premises Change Data Capture (CDC) service that monitors Oracle database for new call records and sends them to AWS SQS for ML processing.

## Architecture

```
┌─────────────────────┐
│   Oracle (RTDB)     │
│ On-Premises         │
│ VERINT_TEXT_        │
│ ANALYSIS            │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Python CDC Service │
│  (This Service)     │
│  - Monitor Oracle   │
│  - Assemble Convs   │
│  - Send to SQS      │
│  - Receive ML       │
│    Results          │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│    AWS SQS Queue    │
│  summary-pipe-queue │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   ML Service (ECS)  │
│  - Process Text     │
│  - Classification   │
│  - Sentiment        │
│  - Summary          │
└──────────┬──────────┘
           │
           ▼ (Results back to SQS)
┌─────────────────────┐
│  Python CDC Service │
│  Receives Results   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Oracle (RTDB)     │
│ DICTA_CALL_SUMMARY  │
└─────────────────────┘
```

## Features

- ✅ **Comprehensive Logging**: 5 separate log files (main, oracle, sqs, errors, performance)
- ✅ **Table Validation**: Automatic detection and creation of missing tables
- ✅ **Dual Mode CDC**: Normal mode (recent calls) + Historical mode (backfill)
- ✅ **Conversation Assembly**: Groups call segments by CALL_ID with completeness validation
- ✅ **Bidirectional SQS**: Sends conversations to AWS, receives ML results
- ✅ **Error Tracking**: Comprehensive error logging to Oracle tables
- ✅ **Statistics**: Real-time metrics and periodic reporting
- ✅ **24/7 Operation**: Continuous monitoring with graceful error recovery

## Prerequisites

### System Requirements
- Python 3.8+
- Oracle Instant Client 21.x or later
- Access to on-premises Oracle RTDB
- AWS credentials with SQS access

### Oracle Setup
- User with SELECT on `rtbi.VERINT_TEXT_ANALYSIS`
- User with CREATE TABLE privilege
- User with INSERT/UPDATE/DELETE on CDC tables

### AWS Setup
- SQS queue: `summary-pipe-queue`
- IAM user with policies:
  - `sqs:SendMessage`
  - `sqs:ReceiveMessage`
  - `sqs:DeleteMessage`
  - `sqs:GetQueueAttributes`

## Installation

### 1. Clone and Setup

```bash
cd oracle-cdc-sqs

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit configuration
nano .env
```

Update the following values in `.env`:

```bash
# Oracle Configuration
ORACLE_USER=your_actual_oracle_user
ORACLE_PASSWORD=your_actual_password
ORACLE_HOST=your_oracle_host_ip
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=XE

# AWS Configuration
AWS_REGION=eu-west-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=wJalr...
SQS_QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/320708867194/summary-pipe-queue

# Logging Configuration
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR
LOG_DIR=./logs
```

### 3. Initialize Oracle Tables

```bash
# Connect to Oracle as your CDC user
sqlplus your_user/your_password@your_host:1521/XE

# Run initialization script
@init_oracle_tables.sql

# Verify tables were created
SELECT table_name FROM user_tables WHERE table_name LIKE 'CDC%' OR table_name = 'DICTA_CALL_SUMMARY';

# Exit SQL*Plus
exit;
```

### 4. Test Connection

```bash
# Test run (will exit after first cycle)
python cdc_service.py
```

Check the output for:
- ✅ Oracle connection successful
- ✅ SQS connection successful
- ✅ All tables validated
- 🔄 First CDC cycle completed

## Running the Service

### Foreground (Testing)

```bash
# Activate virtual environment
source venv/bin/activate

# Run service
python cdc_service.py
```

Press `Ctrl+C` to stop gracefully.

### Background (Production)

#### Option 1: Using nohup

```bash
nohup python cdc_service.py > /dev/null 2>&1 &
echo $! > cdc_service.pid

# Check status
ps aux | grep cdc_service.py

# Stop service
kill $(cat cdc_service.pid)
```

#### Option 2: Using systemd (Linux)

Create service file:

```bash
sudo nano /etc/systemd/system/oracle-cdc.service
```

Add content:

```ini
[Unit]
Description=Oracle CDC to AWS SQS Service
After=network.target

[Service]
Type=simple
User=your_user
Group=your_group
WorkingDirectory=/path/to/oracle-cdc-sqs
Environment="PATH=/path/to/oracle-cdc-sqs/venv/bin"
ExecStart=/path/to/oracle-cdc-sqs/venv/bin/python cdc_service.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Start service:

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable on boot
sudo systemctl enable oracle-cdc

# Start service
sudo systemctl start oracle-cdc

# Check status
sudo systemctl status oracle-cdc

# View logs
sudo journalctl -u oracle-cdc -f
```

## Monitoring

### Log Files

The service creates 5 separate log files in the `logs/` directory:

1. **`cdc_service.log`** - Main comprehensive log (all levels)
2. **`cdc_oracle.log`** - Oracle-specific operations
3. **`cdc_sqs.log`** - SQS-specific operations
4. **`cdc_errors.log`** - Errors only (easy debugging)
5. **`cdc_performance.log`** - Timing metrics for all operations

```bash
# Watch main log
tail -f logs/cdc_service.log

# Watch errors only
tail -f logs/cdc_errors.log

# Check performance
tail -f logs/cdc_performance.log
```

### Database Monitoring

```sql
-- Check CDC processing status
SELECT * FROM CDC_PROCESSING_STATUS;

-- Check recent processed calls
SELECT CALL_ID, PROCESSED_AT, SQS_MESSAGE_ID
FROM CDC_PROCESSED_CALLS
ORDER BY PROCESSED_AT DESC
FETCH FIRST 20 ROWS ONLY;

-- Check for errors
SELECT ERROR_ID, CALL_ID, ERROR_TYPE, ERROR_MESSAGE, ERROR_TIMESTAMP
FROM ERROR_LOG
ORDER BY ERROR_TIMESTAMP DESC
FETCH FIRST 20 ROWS ONLY;

-- Check ML results
SELECT CALL_ID, SENTIMENT, CLASSIFICATION_PRIMARY, CONFIDENCE_SCORE, PROCESSED_AT
FROM DICTA_CALL_SUMMARY
ORDER BY PROCESSED_AT DESC
FETCH FIRST 20 ROWS ONLY;

-- View processing statistics
SELECT
    TABLE_NAME,
    TOTAL_PROCESSED,
    LAST_PROCESSED_TIMESTAMP,
    IS_ENABLED
FROM CDC_PROCESSING_STATUS;
```

### Service Statistics

The service logs statistics every 10 cycles:

```
📊 CDC SERVICE STATISTICS
================================================
Uptime: 0:15:32.123456
Cycles completed: 90
Calls processed: 45
Calls failed: 2
SQS sent: 45
SQS failed: 0
ML results received: 38
ML results written: 38
Last cycle: 2025-01-15 14:23:45
================================================
```

## Configuration

### CDC Modes

Edit `config.py` to enable/disable modes:

```python
CDC_CONFIG = {
    # Normal mode: Collect recent calls
    'normal_mode_enabled': True,
    'normal_mode_minutes': 10,  # Last N minutes
    'normal_poll_interval_seconds': 10,  # Sleep between cycles

    # Historical mode: Backfill old calls
    'historical_mode_enabled': False,  # Set to True to enable
    'historical_start_date': '2024-01-01',  # Start date for backfill
    'historical_batch_size': 50,

    # Processing limits
    'max_batch_size': 50,
    'max_concurrent_calls': 10,
}
```

### Logging Levels

Available levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

```bash
# In .env file
LOG_LEVEL=INFO  # Default
LOG_LEVEL=DEBUG  # Verbose debugging
LOG_LEVEL=ERROR  # Errors only
```

## Troubleshooting

### Connection Issues

**Problem**: `❌ Failed to connect to Oracle`

**Solutions**:
1. Check Oracle host/port in `.env`
2. Verify Oracle service is running: `lsnrctl status`
3. Test connection: `sqlplus user/pass@host:port/service`
4. Check firewall rules

---

**Problem**: `❌ Failed to connect to SQS`

**Solutions**:
1. Verify AWS credentials in `.env`
2. Check IAM permissions for SQS
3. Test AWS CLI: `aws sqs get-queue-url --queue-name summary-pipe-queue`
4. Verify region is correct (`eu-west-1`)

### Table Issues

**Problem**: `❌ Some required tables are missing`

**Solutions**:
1. Run `init_oracle_tables.sql` script
2. Check user has CREATE TABLE privilege
3. Verify schema permissions
4. Review `cdc_oracle.log` for specific errors

### No New Calls

**Problem**: Service runs but finds 0 new calls

**Checks**:
1. Verify source table has data:
   ```sql
   SELECT COUNT(*) FROM rtbi.VERINT_TEXT_ANALYSIS
   WHERE TEXT_TIME > SYSDATE - (10/1440);
   ```
2. Check if calls already processed:
   ```sql
   SELECT COUNT(*) FROM CDC_PROCESSED_CALLS;
   ```
3. Verify normal mode is enabled in `config.py`
4. Check `SYSDATE` vs `TEXT_TIME` timezone alignment

### ML Results Not Arriving

**Problem**: Calls sent to SQS but no ML results received

**Checks**:
1. Verify ML service is running in AWS ECS
2. Check SQS queue for messages:
   ```bash
   aws sqs get-queue-attributes \
     --queue-url https://sqs.eu-west-1.amazonaws.com/320708867194/summary-pipe-queue \
     --attribute-names ApproximateNumberOfMessages
   ```
3. Review ML service logs in CloudWatch
4. Check `cdc_sqs.log` for receive errors

## Performance Tuning

### Batch Size

```python
# config.py
CDC_CONFIG = {
    'max_batch_size': 100,  # Increase for higher throughput
}
```

### Poll Interval

```python
# config.py
CDC_CONFIG = {
    'normal_poll_interval_seconds': 5,  # Decrease for lower latency
}
```

### Log Rotation

```python
# config.py
LOGGING_CONFIG = {
    'max_bytes': 100 * 1024 * 1024,  # 100MB per file
    'backup_count': 20,  # Keep 20 backup files
}
```

## Security Best Practices

1. **Never commit `.env` file** to version control
2. **Use AWS IAM roles** instead of access keys when possible
3. **Rotate credentials** regularly
4. **Restrict Oracle user** to minimum required privileges
5. **Use SSL/TLS** for Oracle connections in production
6. **Monitor error logs** for suspicious activity
7. **Set restrictive file permissions**:
   ```bash
   chmod 600 .env
   chmod 700 cdc_service.py
   ```

## Maintenance

### Daily Tasks
- Monitor log files for errors
- Check service status: `systemctl status oracle-cdc`

### Weekly Tasks
- Review statistics and performance metrics
- Check disk space for log files: `du -sh logs/`
- Verify ML results are being written to Oracle

### Monthly Tasks
- Rotate/archive old log files
- Clean up old entries from `CDC_PROCESSING_LOG` (optional)
- Review and update configuration if needed

### Backup Strategy
- Backup CDC tables weekly (for audit trail)
- Keep `.env` configuration in secure location
- Document any custom configuration changes

## Support

### Logs Location
- Main logs: `./logs/cdc_service.log`
- Error logs: `./logs/cdc_errors.log`
- Performance: `./logs/cdc_performance.log`

### Common Queries

Get service health status:
```sql
SELECT
    'CDC Service Health' AS check_type,
    CASE
        WHEN LAST_UPDATED > SYSTIMESTAMP - INTERVAL '5' MINUTE
        THEN 'HEALTHY'
        ELSE 'STALE - CHECK SERVICE'
    END AS status,
    LAST_UPDATED
FROM CDC_PROCESSING_STATUS
WHERE TABLE_NAME = 'CDC_NORMAL_MODE';
```

## License

Internal use only - Call Analytics AI Platform

## Version

**Version**: 1.0.0
**Last Updated**: January 2025
**Python**: 3.8+
**Oracle**: 21c+
