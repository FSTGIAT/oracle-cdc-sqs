#!/bin/bash
# PM2 Graceful Shutdown Script for Oracle CDC Services
set -e

SERVICE_ROOT="/home/roygi/call-analytics-ai-platform_aws/call-analytics/oracle-cdc-sqs"
cd "$SERVICE_ROOT"

echo "=============================================="
echo "  Oracle CDC Services - Graceful Shutdown"
echo "=============================================="

# Stop all services gracefully
echo ""
echo "Stopping all CDC services..."
pm2 stop ecosystem.config.js 2>/dev/null || true

echo ""
pm2 status

echo ""
echo "All services stopped."
echo ""
echo "To remove from PM2 process list:"
echo "  pm2 delete ecosystem.config.js"
echo ""
