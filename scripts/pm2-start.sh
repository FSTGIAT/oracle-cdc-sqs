#!/bin/bash
# PM2 Startup Script for Oracle CDC Services
set -e

SERVICE_ROOT="/home/roygi/call-analytics-ai-platform_aws/call-analytics/oracle-cdc-sqs"
cd "$SERVICE_ROOT"

echo "=============================================="
echo "  Oracle CDC Services - PM2 Startup"
echo "=============================================="

# Ensure logs directory exists
mkdir -p "$SERVICE_ROOT/logs/pm2"

# Verify Python venv
if [ ! -f "$SERVICE_ROOT/venv/bin/python3" ]; then
    echo "ERROR: Python virtual environment not found!"
    echo "Run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Verify .env
if [ ! -f "$SERVICE_ROOT/.env" ]; then
    echo "WARNING: .env file not found. Copy from .env.example"
fi

# Start main services
echo ""
echo "Starting cdc-service and cdc-dashboard..."
pm2 start ecosystem.config.js --only cdc-service,cdc-dashboard

echo ""
pm2 status

echo ""
echo "=============================================="
echo "  Services Started Successfully"
echo "=============================================="
echo ""
echo "  Dashboard:    http://localhost:5001"
echo "  View logs:    pm2 logs"
echo "  Monitor:      pm2 monit"
echo ""
echo "  Manual commands:"
echo "    Flush SQS:  pm2 start cdc-flush-sqs"
echo "    Evaluate:   pm2 start cdc-evaluation"
echo ""
