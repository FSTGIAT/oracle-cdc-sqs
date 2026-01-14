/**
 * PM2 Ecosystem Configuration
 * Oracle CDC Services Process Management
 *
 * Services:
 *   - cdc-service:    Main CDC polling (24/7)
 *   - cdc-dashboard:  Flask analytics UI (port 5001)
 *   - cdc-flush-sqs:  SQS flush mode (manual)
 *   - cdc-evaluation: Weekly ML evaluation (off by default)
 *
 * Usage:
 *   pm2 start ecosystem.config.js --only cdc-service,cdc-dashboard
 *   pm2 logs
 *   pm2 monit
 */

const path = require('path');

const SERVICE_ROOT = '/home/roygi/call-analytics-ai-platform_aws/call-analytics/oracle-cdc-sqs';
const PYTHON = path.join(SERVICE_ROOT, 'venv/bin/python3');
const LOG_DIR = path.join(SERVICE_ROOT, 'logs/pm2');

module.exports = {
  apps: [
    // ========================================
    // CDC Service - Main continuous polling
    // ========================================
    {
      name: 'cdc-service',
      script: PYTHON,
      args: 'cdc_service.py',
      cwd: SERVICE_ROOT,
      interpreter: 'none',

      // Process management
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '30s',
      restart_delay: 5000,

      // Graceful shutdown (allow Oracle connections to close)
      kill_timeout: 30000,
      shutdown_with_message: true,

      // Memory management
      max_memory_restart: '500M',

      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: path.join(LOG_DIR, 'cdc-service-error.log'),
      out_file: path.join(LOG_DIR, 'cdc-service-out.log'),
      combine_logs: true,
      merge_logs: true,

      // Environment
      env: {
        NODE_ENV: 'production',
        LOG_LEVEL: 'INFO',
      },
    },

    // ========================================
    // SQS Flush Service - Manual/One-shot
    // ========================================
    {
      name: 'cdc-flush-sqs',
      script: PYTHON,
      args: 'cdc_service.py flush_sqs',
      cwd: SERVICE_ROOT,
      interpreter: 'none',

      // One-shot execution (no auto-restart)
      instances: 1,
      exec_mode: 'fork',
      autorestart: false,
      watch: false,

      // Allow time for flush to complete
      kill_timeout: 60000,

      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: path.join(LOG_DIR, 'cdc-flush-error.log'),
      out_file: path.join(LOG_DIR, 'cdc-flush-out.log'),
      combine_logs: true,

      env: {
        NODE_ENV: 'production',
        LOG_LEVEL: 'INFO',
      },
    },

    // ========================================
    // Dashboard - Flask Web Server
    // ========================================
    {
      name: 'cdc-dashboard',
      script: PYTHON,
      args: 'dashboard.py',
      cwd: SERVICE_ROOT,
      interpreter: 'none',

      // Process management
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      watch: false,
      max_restarts: 5,
      min_uptime: '10s',
      restart_delay: 3000,

      // Graceful HTTP shutdown
      kill_timeout: 15000,

      // Memory management
      max_memory_restart: '256M',

      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: path.join(LOG_DIR, 'dashboard-error.log'),
      out_file: path.join(LOG_DIR, 'dashboard-out.log'),
      combine_logs: true,

      env: {
        NODE_ENV: 'production',
        FLASK_ENV: 'production',
        LOG_LEVEL: 'INFO',
      },
    },

    // ========================================
    // Evaluation Service - Weekly (OFF)
    // ========================================
    {
      name: 'cdc-evaluation',
      script: PYTHON,
      args: 'evaluation_service.py',
      cwd: SERVICE_ROOT,
      interpreter: 'none',

      // Manual execution only (no auto-start)
      instances: 1,
      exec_mode: 'fork',
      autorestart: false,
      watch: false,

      // Allow evaluation to complete
      kill_timeout: 120000,

      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: path.join(LOG_DIR, 'evaluation-error.log'),
      out_file: path.join(LOG_DIR, 'evaluation-out.log'),
      combine_logs: true,

      env: {
        NODE_ENV: 'production',
        LOG_LEVEL: 'INFO',
      },

      // Optional: Weekly cron (Sunday 8 AM)
      // Uncomment to enable automatic weekly runs
      // cron_restart: '0 8 * * 0',
    },

    // ========================================
    // Backfill Service - Historical data processing
    // ========================================
    {
      name: 'cdc-backfill',
      script: PYTHON,
      args: 'backfill_service.py',
      cwd: SERVICE_ROOT,
      interpreter: 'none',

      // One-shot execution (exits when complete)
      instances: 1,
      exec_mode: 'fork',
      autorestart: false,
      watch: false,

      // Allow long-running backfill to complete
      kill_timeout: 300000,

      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: path.join(LOG_DIR, 'backfill-error.log'),
      out_file: path.join(LOG_DIR, 'backfill-out.log'),
      combine_logs: true,

      env: {
        NODE_ENV: 'production',
        LOG_LEVEL: 'INFO',
      },
    },
  ],
};
