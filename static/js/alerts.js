/**
 * Alerts Module
 * Handles alert configurations, history, and affected subscribers drill-down
 */

// Module state
let alertConfigurations = [];
let alertHistory = [];
let availableMetrics = [];
let currentAlertForSubscribers = null;

/**
 * Initialize alerts module
 */
function initAlerts() {
    // Register section callback
    if (window.Sidebar) {
        window.Sidebar.onSectionLoad('alerts', loadAlertsSection);
    }

    // Listen for section change events
    document.addEventListener('sectionChanged', (e) => {
        if (e.detail.section === 'alerts') {
            loadAlertsSection();
        }
    });
}

/**
 * Load alerts section data
 */
async function loadAlertsSection() {
    await Promise.all([
        loadAlertsSummary(),
        loadActiveAlerts(),
        loadAlertConfigurations(),
        loadAlertHistory(),
        loadAvailableMetrics()
    ]);
}

/**
 * Load summary stats for KPI cards
 */
async function loadAlertsSummary() {
    try {
        const response = await fetch('/api/alerts/summary');
        if (!response.ok) throw new Error('Failed to fetch summary');

        const data = await response.json();

        // Update KPI cards
        document.getElementById('alertsActiveCount').textContent = data.active_count || 0;
        document.getElementById('alertsCriticalCount').textContent = data.critical_count || 0;
        document.getElementById('alertsRulesCount').textContent = data.enabled_rules || 0;
        document.getElementById('alerts24hCount').textContent = data.alerts_24h || 0;

    } catch (error) {
        console.error('Error loading alerts summary:', error);
    }
}

/**
 * Load active alerts list
 */
async function loadActiveAlerts() {
    const container = document.getElementById('activeAlertsList');
    const emptyState = document.getElementById('activeAlertsEmpty');

    try {
        const response = await fetch('/api/alerts/history?status=ACTIVE&limit=20');
        if (!response.ok) throw new Error('Failed to fetch active alerts');

        const data = await response.json();
        const alerts = data.data || [];

        if (alerts.length === 0) {
            container.innerHTML = '';
            emptyState.style.display = 'block';
            return;
        }

        emptyState.style.display = 'none';
        container.innerHTML = alerts.map(alert => renderActiveAlert(alert)).join('');

    } catch (error) {
        console.error('Error loading active alerts:', error);
        container.innerHTML = '<div class="text-danger">Error loading alerts</div>';
    }
}

/**
 * Render a single active alert card
 */
function renderActiveAlert(alert) {
    const severityClass = {
        'CRITICAL': 'danger',
        'WARNING': 'warning',
        'INFO': 'info'
    }[alert.severity] || 'secondary';

    const severityIcon = {
        'CRITICAL': 'exclamation-triangle-fill',
        'WARNING': 'exclamation-circle',
        'INFO': 'info-circle'
    }[alert.severity] || 'bell';

    return `
        <div class="alert-item border-start border-${severityClass} border-3 p-2 mb-2 bg-light rounded-end">
            <div class="d-flex justify-content-between align-items-center">
                <div class="flex-grow-1">
                    <div class="d-flex align-items-center">
                        <i class="bi bi-${severityIcon} text-${severityClass} me-2"></i>
                        <span class="fw-medium small">${escapeHtml(alert.alert_name)}</span>
                        ${alert.filter_product ? `<span class="badge bg-secondary ms-1" style="font-size: 0.6rem;">${alert.filter_product}</span>` : ''}
                    </div>
                    <div class="d-flex align-items-center mt-1">
                        <span class="badge bg-${severityClass} me-2">${alert.metric_value}</span>
                        <small class="text-muted">${alert.triggered_at}</small>
                    </div>
                </div>
                <div class="btn-group btn-group-sm">
                    ${alert.affected_count > 0 ? `
                        <button class="btn btn-outline-primary btn-sm py-0" onclick="viewAffectedSubscribers('${alert.history_id}')" title="View">
                            <i class="bi bi-people"></i> ${alert.affected_count}
                        </button>
                    ` : ''}
                    <button class="btn btn-outline-success btn-sm py-0" onclick="acknowledgeAlert('${alert.history_id}')" title="OK">
                        <i class="bi bi-check"></i>
                    </button>
                    <button class="btn btn-outline-secondary btn-sm py-0" onclick="resolveAlert('${alert.history_id}')" title="Done">
                        <i class="bi bi-check-all"></i>
                    </button>
                </div>
            </div>
        </div>
    `;
}

/**
 * Load alert configurations
 */
async function loadAlertConfigurations() {
    const container = document.getElementById('alertRulesList');

    try {
        const response = await fetch('/api/alerts/configurations');
        if (!response.ok) throw new Error('Failed to fetch configurations');

        alertConfigurations = await response.json();

        if (alertConfigurations.length === 0) {
            container.innerHTML = '<div class="text-muted text-center py-3">No alert rules configured</div>';
            return;
        }

        container.innerHTML = alertConfigurations.map(config => renderAlertRule(config)).join('');

    } catch (error) {
        console.error('Error loading alert configurations:', error);
        container.innerHTML = '<div class="text-danger">Error loading rules</div>';
    }
}

/**
 * Render a single alert rule
 */
function renderAlertRule(config) {
    const operatorLabels = {
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<=',
        'eq': '='
    };

    const severityColors = {
        'CRITICAL': 'danger',
        'WARNING': 'warning',
        'INFO': 'info'
    };

    const severityIcons = {
        'CRITICAL': 'exclamation-triangle-fill',
        'WARNING': 'exclamation-circle',
        'INFO': 'info-circle'
    };

    return `
        <div class="rule-item d-flex align-items-center justify-content-between p-2 border-bottom ${config.is_enabled ? '' : 'opacity-50'}">
            <div class="d-flex align-items-center">
                <div class="form-check form-switch me-2">
                    <input class="form-check-input" type="checkbox" id="rule-${config.alert_id}"
                           ${config.is_enabled ? 'checked' : ''}
                           onchange="toggleAlertRule('${config.alert_id}')">
                </div>
                <div>
                    <div class="d-flex align-items-center">
                        <i class="bi bi-${severityIcons[config.severity] || 'bell'} text-${severityColors[config.severity] || 'secondary'} me-1"></i>
                        <span class="fw-medium small">${escapeHtml(config.alert_name)}</span>
                        ${config.filter_product ? `<span class="badge bg-light text-dark ms-1" style="font-size: 0.65rem;">${config.filter_product}</span>` : ''}
                    </div>
                    <small class="text-muted" style="font-size: 0.7rem;">
                        ${operatorLabels[config.condition_operator] || config.condition_operator} ${config.threshold_value} / ${config.time_window_hours}h
                    </small>
                </div>
            </div>
            <div class="btn-group btn-group-sm">
                <button class="btn btn-outline-secondary btn-sm py-0 px-1" onclick="editAlertRule('${config.alert_id}')" title="Edit">
                    <i class="bi bi-pencil" style="font-size: 0.75rem;"></i>
                </button>
                <button class="btn btn-outline-danger btn-sm py-0 px-1" onclick="deleteAlertRule('${config.alert_id}')" title="Delete">
                    <i class="bi bi-trash" style="font-size: 0.75rem;"></i>
                </button>
            </div>
        </div>
    `;
}

/**
 * Load alert history
 */
async function loadAlertHistory(days = 7) {
    const tbody = document.getElementById('alertHistoryTable');

    try {
        const response = await fetch(`/api/alerts/history?days=${days}&limit=50`);
        if (!response.ok) throw new Error('Failed to fetch history');

        const data = await response.json();
        alertHistory = data.data || [];

        if (alertHistory.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-3">No alerts in this period</td></tr>';
            return;
        }

        tbody.innerHTML = alertHistory.map(item => `
            <tr>
                <td>${item.triggered_at}</td>
                <td>${escapeHtml(item.alert_name)}</td>
                <td>${item.metric_value}</td>
                <td>
                    <span class="badge bg-${getSeverityColor(item.severity)}">${item.severity}</span>
                </td>
                <td>
                    <span class="badge bg-${getStatusColor(item.status)}">${item.status}</span>
                </td>
            </tr>
        `).join('');

    } catch (error) {
        console.error('Error loading alert history:', error);
        tbody.innerHTML = '<tr><td colspan="5" class="text-danger">Error loading history</td></tr>';
    }
}

/**
 * Load available metrics for alert configuration
 */
async function loadAvailableMetrics() {
    try {
        const response = await fetch('/api/alerts/available-metrics');
        if (!response.ok) throw new Error('Failed to fetch metrics');

        availableMetrics = await response.json();
        populateMetricsDropdown();

    } catch (error) {
        console.error('Error loading available metrics:', error);
    }
}

/**
 * Populate metrics dropdown in create/edit modal
 */
function populateMetricsDropdown() {
    const select = document.getElementById('alertMetricSelect');
    if (!select) return;

    // Group metrics by source with friendly names
    const sourceLabels = {
        'churn': 'Churn Risk',
        'sentiment': 'Sentiment',
        'satisfaction': 'Satisfaction',
        'ml_quality': 'ML Quality',
        'operational': 'Operations'
    };

    const grouped = {};
    availableMetrics.forEach(m => {
        if (!grouped[m.source]) grouped[m.source] = [];
        grouped[m.source].push(m);
    });

    let html = '<option value="">Select...</option>';
    for (const [source, metrics] of Object.entries(grouped)) {
        const label = sourceLabels[source] || source;
        html += `<optgroup label="${label}">`;
        metrics.forEach(m => {
            html += `<option value="${m.source}|${m.name}">${m.label}</option>`;
        });
        html += '</optgroup>';
    }

    select.innerHTML = html;
}

/**
 * View affected subscribers for an alert
 */
async function viewAffectedSubscribers(historyId) {
    currentAlertForSubscribers = historyId;

    const modal = new bootstrap.Modal(document.getElementById('subscribersModal'));
    const container = document.getElementById('subscribersContent');
    const loading = document.getElementById('subscribersLoading');

    loading.style.display = 'block';
    container.style.display = 'none';
    modal.show();

    try {
        const response = await fetch(`/api/alerts/history/${historyId}/subscribers`);
        if (!response.ok) throw new Error('Failed to fetch subscribers');

        const data = await response.json();

        document.getElementById('subscribersAlertName').textContent = data.alert_name || 'Alert';
        document.getElementById('subscribersCount').textContent = data.affected_count || 0;

        const tbody = document.getElementById('subscribersTableBody');
        if (data.subscribers && data.subscribers.length > 0) {
            tbody.innerHTML = data.subscribers.map(sub => `
                <tr>
                    <td>${escapeHtml(sub.subscriber_no || '-')}</td>
                    <td>${escapeHtml(sub.product_code || '-')}</td>
                    <td>${getChurnBadge(sub.churn_score)}</td>
                    <td>${sub.call_time || '-'}</td>
                    <td>
                        <button class="btn btn-sm btn-outline-primary"
                                onclick="viewSubscriberJourney('${sub.subscriber_no}', '${sub.ban}')">
                            <i class="bi bi-clock-history"></i> Journey
                        </button>
                    </td>
                </tr>
            `).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">No subscriber data available</td></tr>';
        }

        loading.style.display = 'none';
        container.style.display = 'block';

    } catch (error) {
        console.error('Error loading subscribers:', error);
        loading.innerHTML = '<div class="text-danger">Error loading subscribers</div>';
    }
}

/**
 * View subscriber journey from alerts
 */
function viewSubscriberJourney(subscriberNo, ban) {
    // Close subscribers modal
    const subscribersModal = bootstrap.Modal.getInstance(document.getElementById('subscribersModal'));
    if (subscribersModal) {
        subscribersModal.hide();
    }

    // Open customer journey modal
    if (typeof showCustomerJourney === 'function') {
        showCustomerJourney(subscriberNo);
    } else {
        console.error('showCustomerJourney function not available');
    }
}

/**
 * Acknowledge an alert
 */
async function acknowledgeAlert(historyId) {
    try {
        const response = await fetch(`/api/alerts/history/${historyId}/acknowledge`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });

        if (!response.ok) throw new Error('Failed to acknowledge');

        // Refresh alerts
        await loadActiveAlerts();
        await loadAlertsSummary();

        // Update sidebar badge
        if (window.Sidebar) {
            window.Sidebar.updateBadges();
        }

    } catch (error) {
        console.error('Error acknowledging alert:', error);
        alert('Failed to acknowledge alert');
    }
}

/**
 * Resolve an alert
 */
async function resolveAlert(historyId, notes = '') {
    try {
        const response = await fetch(`/api/alerts/history/${historyId}/resolve`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ resolution_notes: notes })
        });

        if (!response.ok) throw new Error('Failed to resolve');

        // Refresh alerts
        await loadActiveAlerts();
        await loadAlertsSummary();
        await loadAlertHistory();

        // Update sidebar badge
        if (window.Sidebar) {
            window.Sidebar.updateBadges();
        }

    } catch (error) {
        console.error('Error resolving alert:', error);
        alert('Failed to resolve alert');
    }
}

/**
 * Toggle alert rule enabled/disabled
 */
async function toggleAlertRule(alertId) {
    try {
        const response = await fetch(`/api/alerts/configurations/${alertId}/toggle`, {
            method: 'POST'
        });

        if (!response.ok) throw new Error('Failed to toggle');

        await loadAlertConfigurations();
        await loadAlertsSummary();

    } catch (error) {
        console.error('Error toggling alert rule:', error);
        alert('Failed to toggle alert rule');
        // Reload to reset checkbox state
        await loadAlertConfigurations();
    }
}

/**
 * Show create alert modal
 */
function showCreateAlertModal() {
    // Reset form
    document.getElementById('alertForm').reset();
    document.getElementById('alertFormAlertId').value = '';
    document.getElementById('alertModalTitle').textContent = 'Create Alert Rule';

    const modal = new bootstrap.Modal(document.getElementById('alertFormModal'));
    modal.show();
}

/**
 * Edit alert rule
 */
function editAlertRule(alertId) {
    const config = alertConfigurations.find(c => c.alert_id === alertId);
    if (!config) return;

    // Populate form
    document.getElementById('alertFormAlertId').value = alertId;
    document.getElementById('alertFormName').value = config.alert_name || '';
    document.getElementById('alertFormNameHe').value = config.alert_name_he || '';
    document.getElementById('alertMetricSelect').value = `${config.metric_source}|${config.metric_name}`;
    document.getElementById('alertFormOperator').value = config.condition_operator || 'gt';
    document.getElementById('alertFormThreshold').value = config.threshold_value || '';
    document.getElementById('alertFormTimeWindow').value = config.time_window_hours || 24;
    document.getElementById('alertFormProduct').value = config.filter_product || '';
    document.getElementById('alertFormSeverity').value = config.severity || 'WARNING';
    document.getElementById('alertFormDescription').value = config.description || '';

    document.getElementById('alertModalTitle').textContent = 'Edit Alert Rule';

    const modal = new bootstrap.Modal(document.getElementById('alertFormModal'));
    modal.show();
}

/**
 * Save alert rule (create or update)
 */
async function saveAlertRule() {
    const alertId = document.getElementById('alertFormAlertId').value;
    const metricValue = document.getElementById('alertMetricSelect').value;

    if (!metricValue) {
        alert('Please select a metric');
        return;
    }

    const [metricSource, metricName] = metricValue.split('|');

    const data = {
        alert_name: document.getElementById('alertFormName').value,
        alert_name_he: document.getElementById('alertFormNameHe').value,
        metric_source: metricSource,
        metric_name: metricName,
        condition_operator: document.getElementById('alertFormOperator').value,
        threshold_value: parseFloat(document.getElementById('alertFormThreshold').value),
        time_window_hours: parseInt(document.getElementById('alertFormTimeWindow').value),
        filter_product: document.getElementById('alertFormProduct').value || null,
        severity: document.getElementById('alertFormSeverity').value,
        description: document.getElementById('alertFormDescription').value
    };

    try {
        const url = alertId
            ? `/api/alerts/configurations/${alertId}`
            : '/api/alerts/configurations';
        const method = alertId ? 'PUT' : 'POST';

        const response = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        if (!response.ok) throw new Error('Failed to save');

        // Close modal and refresh
        const modal = bootstrap.Modal.getInstance(document.getElementById('alertFormModal'));
        modal.hide();

        await loadAlertConfigurations();
        await loadAlertsSummary();

    } catch (error) {
        console.error('Error saving alert rule:', error);
        alert('Failed to save alert rule');
    }
}

/**
 * Delete alert rule
 */
async function deleteAlertRule(alertId) {
    if (!confirm('Are you sure you want to delete this alert rule?')) {
        return;
    }

    try {
        const response = await fetch(`/api/alerts/configurations/${alertId}`, {
            method: 'DELETE'
        });

        if (!response.ok) throw new Error('Failed to delete');

        await loadAlertConfigurations();
        await loadAlertsSummary();

    } catch (error) {
        console.error('Error deleting alert rule:', error);
        alert('Failed to delete alert rule');
    }
}

/**
 * Manually trigger alert evaluation
 */
async function evaluateAlerts() {
    const btn = document.getElementById('evaluateAlertsBtn');
    const originalText = btn.innerHTML;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Evaluating...';
    btn.disabled = true;

    try {
        const response = await fetch('/api/alerts/evaluate', {
            method: 'POST'
        });

        if (!response.ok) throw new Error('Failed to evaluate');

        const result = await response.json();
        alert(`Evaluated ${result.evaluated} rules, triggered ${result.triggered} alerts`);

        // Refresh data
        await loadAlertsSection();

    } catch (error) {
        console.error('Error evaluating alerts:', error);
        alert('Failed to evaluate alerts');
    } finally {
        btn.innerHTML = originalText;
        btn.disabled = false;
    }
}

// Helper functions
function getSeverityColor(severity) {
    return { 'CRITICAL': 'danger', 'WARNING': 'warning', 'INFO': 'info' }[severity] || 'secondary';
}

function getStatusColor(status) {
    return { 'ACTIVE': 'danger', 'ACKNOWLEDGED': 'warning', 'RESOLVED': 'success' }[status] || 'secondary';
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', initAlerts);

// Export for other modules
window.Alerts = {
    loadAlertsSection,
    viewAffectedSubscribers,
    acknowledgeAlert,
    resolveAlert,
    showCreateAlertModal,
    evaluateAlerts
};
