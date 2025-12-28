#!/bin/bash
# Health Check Script for Oracle CDC Services

SERVICE_ROOT="/home/roygi/call-analytics-ai-platform_aws/call-analytics/oracle-cdc-sqs"

echo "=============================================="
echo "  Oracle CDC Services - Health Check"
echo "=============================================="
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="

# PM2 Status
echo ""
echo "[PM2 Process Status]"
pm2 jlist 2>/dev/null | python3 -c "
import json, sys
try:
    procs = json.load(sys.stdin)
    for p in procs:
        name = p.get('name', 'unknown')
        if not name.startswith('cdc'):
            continue
        status = p.get('pm2_env', {}).get('status', 'unknown')
        restarts = p.get('pm2_env', {}).get('restart_time', 0)
        memory_bytes = p.get('monit', {}).get('memory', 0)
        memory_mb = memory_bytes / 1024 / 1024
        cpu = p.get('monit', {}).get('cpu', 0)

        # Status emoji
        if status == 'online':
            icon = 'OK'
        elif status == 'stopped':
            icon = 'STOPPED'
        else:
            icon = 'ERROR'

        print(f'  [{icon}] {name}: {status} | restarts: {restarts} | mem: {memory_mb:.1f}MB | cpu: {cpu}%')
except Exception as e:
    print(f'  Error: {e}')
" 2>/dev/null || echo "  PM2 not running or no processes found"

# Dashboard Health
echo ""
echo "[Dashboard HTTP Check]"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/ 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    echo "  [OK] Dashboard responding (HTTP $HTTP_CODE)"
else
    echo "  [ERROR] Dashboard not responding (HTTP $HTTP_CODE)"
fi

# Recent Errors
echo ""
echo "[Recent Errors (last 5)]"
if [ -f "$SERVICE_ROOT/logs/cdc_errors.log" ]; then
    ERRORS=$(tail -5 "$SERVICE_ROOT/logs/cdc_errors.log" 2>/dev/null)
    if [ -n "$ERRORS" ]; then
        echo "$ERRORS" | while read line; do
            echo "  $line"
        done
    else
        echo "  No recent errors"
    fi
else
    echo "  Error log not found"
fi

# Disk Usage
echo ""
echo "[Log Directory Size]"
if [ -d "$SERVICE_ROOT/logs" ]; then
    SIZE=$(du -sh "$SERVICE_ROOT/logs" 2>/dev/null | cut -f1)
    echo "  Logs: $SIZE"
else
    echo "  Logs directory not found"
fi

echo ""
echo "=============================================="
echo "  Health check complete"
echo "=============================================="
