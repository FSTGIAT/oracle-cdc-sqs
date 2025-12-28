# Oracle CDC Services - PM2 Process Management
# Usage: make [target]

.PHONY: help start stop restart status logs monit flush evaluate health install save webui

SERVICE_ROOT := /home/roygi/call-analytics-ai-platform_aws/call-analytics/oracle-cdc-sqs

# Default target
help:
	@echo "Oracle CDC Services - PM2 Commands"
	@echo ""
	@echo "  make start     Start CDC service and dashboard"
	@echo "  make stop      Stop all services gracefully"
	@echo "  make restart   Graceful reload (zero downtime)"
	@echo "  make status    Show process status"
	@echo "  make logs      Stream all logs"
	@echo "  make monit     Open PM2 terminal dashboard"
	@echo "  make health    Run health check"
	@echo ""
	@echo "  make flush     Run SQS flush (one-shot)"
	@echo "  make evaluate  Run ML evaluation"
	@echo ""
	@echo "  make install   Install PM2 and modules"
	@echo "  make save      Save processes for reboot"
	@echo "  make webui     Start PM2 web interface"

# Start main services (CDC + Dashboard)
start:
	@mkdir -p $(SERVICE_ROOT)/logs/pm2
	@echo "Starting CDC services..."
	pm2 start $(SERVICE_ROOT)/ecosystem.config.js --only cdc-service,cdc-dashboard
	@pm2 status

# Stop all services
stop:
	@echo "Stopping all CDC services..."
	pm2 stop $(SERVICE_ROOT)/ecosystem.config.js || true
	@pm2 status

# Graceful restart
restart:
	@echo "Restarting services gracefully..."
	pm2 reload $(SERVICE_ROOT)/ecosystem.config.js --only cdc-service,cdc-dashboard
	@pm2 status

# Show status
status:
	pm2 status

# Stream logs
logs:
	pm2 logs

# Terminal dashboard
monit:
	pm2 monit

# Health check
health:
	@$(SERVICE_ROOT)/scripts/health-check.sh

# Run SQS flush (manual one-shot)
flush:
	@echo "Starting SQS flush..."
	pm2 start $(SERVICE_ROOT)/ecosystem.config.js --only cdc-flush-sqs
	pm2 logs cdc-flush-sqs

# Run ML evaluation (manual)
evaluate:
	@echo "Starting ML evaluation..."
	pm2 start $(SERVICE_ROOT)/ecosystem.config.js --only cdc-evaluation
	pm2 logs cdc-evaluation

# Install PM2 and configure
install:
	@echo "Installing PM2 and modules..."
	npm install -g pm2
	pm2 install pm2-logrotate
	pm2 set pm2-logrotate:max_size 50M
	pm2 set pm2-logrotate:retain 10
	pm2 set pm2-logrotate:compress true
	@echo ""
	@echo "Setup system startup with: pm2 startup systemd"
	@echo "Then run the suggested command with sudo"

# Save process list for reboot
save:
	pm2 save
	@echo "Process list saved. Will restore on reboot."

# Start web UI
webui:
	@echo "Starting PM2 Web UI on port 9615..."
	@echo "Access at: http://localhost:9615"
	pm2-webui --port 9615
