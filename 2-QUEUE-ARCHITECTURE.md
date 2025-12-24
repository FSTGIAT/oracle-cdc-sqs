# 2-Queue Architecture - ML Processing Pipeline

## Overview
Implemented separate SQS queues for bidirectional communication between CDC service and ML service to prevent bad summaries from being saved to the database.

## Problem Solved
**Before:** Single queue mixed conversations and results. Failed ML processing would send full transcriptions to database instead of summaries.

**After:** Separate queues ensure only successful AI-generated summaries reach the database.

## Architecture Flow

```
Oracle DB
    ↓
CDC Service (send conversations)
    ↓
summary-pipe-queue (OUTBOUND)
    ↓
ML Service (process with DictaLM)
    ↓
[Only if success=True]
    ↓
summary-pipe-complete (INBOUND)
    ↓
CDC Service (receive results)
    ↓
Oracle DB (DICTA_CALL_SUMMARY)
```

## Queue Configuration

| Queue | Purpose | Direction | Contains |
|-------|---------|-----------|----------|
| `summary-pipe-queue` | Outbound | CDC → ML | Conversation assemblies |
| `summary-pipe-complete` | Inbound | ML → CDC | **Successful summaries only** |
| `summary-pipe-complete-dlq` | Dead Letter | N/A | Failed messages for monitoring |

## Files Changed

### 1. oracle-cdc-sqs/config.py
- Added `SQS_OUTBOUND_QUEUE_URL` (summary-pipe-queue)
- Added `SQS_INBOUND_QUEUE_URL` (summary-pipe-complete)

### 2. oracle-cdc-sqs/cdc_service.py
- Import both queue URLs
- `connect_sqs()`: Test connection to both queues
- `send_to_sqs()`: Send conversations to **OUTBOUND** queue
- `receive_ml_results()`: Receive results from **INBOUND** queue
- `delete_message()`: Delete from **INBOUND** queue

### 3. ml-service/src/services/sqs_producer_service.py
- Changed default queue to `summary-pipe-complete`
- Changed DLQ to `summary-pipe-complete-dlq`

### 4. ml-service/app.py
- Added check: Only send to SQS when `success=True`
- Failed summaries skip database save entirely

## Key Benefits

1. **Clean Separation**: Conversations and results use different queues
2. **No Bad Data**: Only successful AI summaries reach the database
3. **Easy Monitoring**: Track pending conversations vs. pending results separately
4. **Defensive Architecture**: Multiple layers prevent incorrect data from being saved

## Environment Variables (Optional)

```bash
SQS_OUTBOUND_QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/320708867194/summary-pipe-queue
SQS_INBOUND_QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/320708867194/summary-pipe-complete
SQS_COMPLETE_QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/320708867194/summary-pipe-complete
SQS_COMPLETE_DLQ_URL=https://sqs.eu-west-1.amazonaws.com/320708867194/summary-pipe-complete-dlq
```

## Deployment Date
October 27, 2025

## Status
✅ Deployed to ECS - Production Ready
