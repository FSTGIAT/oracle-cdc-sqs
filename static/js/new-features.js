/**
 * New Dashboard Features Module
 * Heatmap, Trend Comparisons, Products Breakdown, Agent Performance
 */

let productsChartInstance, agentPerformanceChartInstance;
let queueDistributionChartInstance, repeatCallersChartInstance;
let queueDistributionData = [];  // Store for drill-down
let repeatCallersData = [];  // Store for drill-down

// ========================================
// TREND COMPARISONS
// ========================================

async function loadTrendComparisons() {
    try {
        const days = getTimeFilterDays();
        const callType = getCallType();
        const response = await fetch(`${API_BASE}/api/trends/comparison?current_days=${days}&compare_days=${days}&call_type=${callType}`);
        const data = await response.json();

        // Update trend cards
        updateTrendCard('trendTotalCalls', data.current_period.total_calls, data.deltas.total_calls);
        updateTrendCard('trendAvgSatisfaction', data.current_period.avg_satisfaction?.toFixed(1), data.deltas.avg_satisfaction);
        updateTrendCard('trendHighRisk', data.current_period.high_risk_count, data.deltas.high_risk_count, true);
        updateTrendCard('trendPositive', formatPercent(data.current_period.positive_percent), data.deltas.positive_percent);

    } catch (error) {
        console.error('Error loading trend comparisons:', error);
    }
}

function updateTrendCard(elementId, currentValue, delta, inverse = false) {
    const el = document.getElementById(elementId);
    if (!el) return;

    const valueEl = el.querySelector('.trend-value');
    const deltaEl = el.querySelector('.trend-delta');

    if (valueEl) valueEl.textContent = currentValue ?? '-';
    if (deltaEl && delta) {
        deltaEl.innerHTML = getDeltaIndicator(delta.value, delta.percent, inverse) +
            `<span class="vs-text">vs previous</span>`;
    }
}

// ========================================
// CALL VOLUME HEATMAP
// ========================================

async function loadHeatmap() {
    const container = document.getElementById('heatmapContainer');
    if (!container) return;

    try {
        const days = getTimeFilterDays();
        const callType = getCallType();
        const response = await fetch(`${API_BASE}/api/heatmap/call-volume?days=${days}&call_type=${callType}`);
        const data = await response.json();

        renderHeatmap(data.data, data.max_count);

    } catch (error) {
        console.error('Error loading heatmap:', error);
        container.innerHTML = '<div class="text-danger text-center py-4">Error loading heatmap</div>';
    }
}

function renderHeatmap(data, maxCount) {
    const container = document.getElementById('heatmapContainer');
    if (!container) return;

    const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const hours = Array.from({length: 24}, (_, i) => i.toString().padStart(2, '0'));

    // Create lookup map
    const lookup = {};
    data.forEach(d => {
        const key = `${d.day_of_week}-${d.hour}`;
        lookup[key] = d.count;
    });

    // Build table
    let html = '<table class="heatmap-table"><thead><tr><th class="heatmap-header"></th>';
    hours.forEach(h => {
        html += `<th class="heatmap-header">${h}</th>`;
    });
    html += '</tr></thead><tbody>';

    days.forEach((day, dayIndex) => {
        html += `<tr><th class="heatmap-header">${day}</th>`;
        hours.forEach((_, hourIndex) => {
            const count = lookup[`${dayIndex}-${hourIndex}`] || 0;
            const intensity = maxCount > 0 ? count / maxCount : 0;
            const color = getHeatmapColor(intensity);
            html += `<td class="heatmap-cell" style="background-color: ${color};"
                        onclick="drillDownHeatmap(${dayIndex}, ${hourIndex})"
                        title="${day} ${hourIndex}:00 - ${count} calls">${count || ''}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table>';
    container.innerHTML = html;
}

function getHeatmapColor(intensity) {
    // Green to Yellow to Red gradient
    if (intensity === 0) return '#f8f9fa';
    if (intensity < 0.33) {
        const r = Math.round(40 + (255 - 40) * (intensity / 0.33));
        return `rgb(${r}, 167, 69)`;
    } else if (intensity < 0.66) {
        const g = Math.round(193 - (193 - 167) * ((intensity - 0.33) / 0.33));
        return `rgb(255, ${g}, 7)`;
    } else {
        const g = Math.round(193 * (1 - (intensity - 0.66) / 0.34));
        return `rgb(220, ${Math.round(g * 0.27)}, 69)`;
    }
}

async function drillDownHeatmap(dayOfWeek, hour) {
    const days = getTimeFilterDays();
    const callType = getCallType();
    document.getElementById('categoryModalTitle').textContent =
        `Calls on ${['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][dayOfWeek]} at ${hour}:00`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const response = await fetch(
            `${API_BASE}/api/heatmap/drill-down?day_of_week=${dayOfWeek}&hour=${hour}&days=${days}&call_type=${callType}`
        );
        const calls = await response.json();

        document.getElementById('categoryCallCount').textContent = calls.length;
        document.getElementById('categoryCallsBody').innerHTML = calls.map(c => {
            const callId = escapeHtml(String(c.call_id || ''));
            const summary = c.summary ? String(c.summary) : '';
            return `
            <tr class="call-row" onclick="showCallDetails('${callId}')">
                <td><code class="text-primary">${callId || '-'}</code></td>
                <td><span class="badge ${c.type === 'CALL' ? 'bg-primary' : 'bg-success'}">${c.type || '-'}</span></td>
                <td>${c.created || '-'}</td>
                <td>${getSentimentBadge(c.sentiment)}</td>
                <td>${c.satisfaction || '-'}</td>
                <td>${getChurnBadge(c.churn_score)}</td>
                <td class="summary-preview" title="${escapeHtml(summary)}">${summary.substring(0, 60)}...</td>
            </tr>
            `;
        }).join('');

        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';

    } catch (error) {
        console.error('Error loading heatmap drill-down:', error);
        document.getElementById('categoryCallsBody').innerHTML =
            '<tr><td colspan="7" class="text-center text-danger">Error loading calls</td></tr>';
        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    }
}

// ========================================
// PRODUCTS BREAKDOWN
// ========================================

async function loadProductsBreakdown() {
    const ctx = document.getElementById('productsChart');
    if (!ctx) {
        return;
    }

    try {
        const days = getTimeFilterDays();
        const callType = getCallType();
        const response = await fetch(`${API_BASE}/api/products/daily-breakdown?days=${days}&call_type=${callType}`);
        const data = await response.json();

        renderProductsChart(data);

    } catch (error) {
        console.error('Error loading products breakdown:', error);
    }
}

function renderProductsChart(data) {
    const ctx = document.getElementById('productsChart');
    if (!ctx) return;

    if (productsChartInstance) productsChartInstance.destroy();

    const dates = data.dates || [];
    const productsData = data.products || {};
    const totals = data.totals_by_product || {};

    if (dates.length === 0 || Object.keys(productsData).length === 0) {
        const legendContainer = document.getElementById('productsLegend');
        if (legendContainer) {
            legendContainer.innerHTML = '<div class="text-muted">No products data available</div>';
        }
        return;
    }

    // Sort by total and take top 5 for cleaner view
    const topProducts = Object.entries(totals)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([name]) => name);

    // Clean distinct colors for top 5
    const colors = ['#0d6efd', '#198754', '#fd7e14', '#6f42c1', '#20c997'];

    const datasets = topProducts.map((product, idx) => ({
        label: product,
        data: productsData[product] || [],
        borderColor: colors[idx],
        backgroundColor: colors[idx] + '20',
        borderWidth: 2,
        fill: false,
        tension: 0.3
    }));

    productsChartInstance = new Chart(ctx, {
        type: 'line',
        data: { labels: dates, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: { boxWidth: 12, padding: 8 }
                }
            },
            scales: {
                x: {
                    ticks: { maxTicksLimit: 7 }
                },
                y: { beginAtZero: true }
            }
        }
    });

    // Clear custom legend - using chart legend instead
    const legendContainer = document.getElementById('productsLegend');
    if (legendContainer) {
        legendContainer.innerHTML = '';
    }
}

// ========================================
// AGENT/QUEUE PERFORMANCE
// ========================================

async function loadAgentPerformance() {
    const ctx = document.getElementById('agentPerformanceChart');
    if (!ctx) {
        console.log('Agent performance chart canvas not found');
        return;
    }

    try {
        const days = getTimeFilterDays();
        const callType = getCallType();
        console.log('Loading agent performance for', days, 'days', callType);
        const response = await fetch(`${API_BASE}/api/agent-performance?days=${days}&call_type=${callType}`);

        if (!response.ok) {
            console.error('Agent performance API error:', response.status, response.statusText);
            return;
        }

        const data = await response.json();
        console.log('Agent performance API response:', data);

        renderAgentPerformanceChart(data.queues || [], ctx);

    } catch (error) {
        console.error('Error loading agent performance:', error);
    }
}

function renderAgentPerformanceChart(queues, ctx) {
    if (!ctx) {
        ctx = document.getElementById('agentPerformanceChart');
        if (!ctx) return;
    }

    if (agentPerformanceChartInstance) {
        agentPerformanceChartInstance.destroy();
    }

    if (!queues || queues.length === 0) {
        return;
    }

    // Horizontal bar chart - calls by product with satisfaction color coding
    const labels = queues.map(d => d.display_name || d.queue_name || '');
    const callCounts = queues.map(d => d.call_count || 0);

    // Color based on avg satisfaction (green=high, red=low)
    const colors = queues.map(d => {
        const sat = d.avg_satisfaction || 3;
        if (sat >= 4) return '#198754';  // green - good
        if (sat <= 2) return '#dc3545';  // red - bad
        return '#0d6efd';  // blue - neutral
    });

    agentPerformanceChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Calls',
                data: callCounts,
                backgroundColor: colors
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        afterLabel: (context) => {
                            const q = queues[context.dataIndex];
                            return `Avg Satisfaction: ${q.avg_satisfaction || '-'}\nAvg Churn: ${q.avg_churn_score || '-'}`;
                        }
                    }
                }
            },
            scales: {
                x: { beginAtZero: true }
            },
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const index = elements[0].index;
                    const queueName = queues[index].queue_name;
                    if (queueName) drillDownAgentPerformance(queueName);
                }
            }
        }
    });
}

async function drillDownAgentPerformance(queueName) {
    const days = getTimeFilterDays();
    const callType = getCallType();
    document.getElementById('categoryModalTitle').textContent = `Queue: ${queueName}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const response = await fetch(
            `${API_BASE}/api/agent-performance/calls?queue_name=${encodeURIComponent(queueName)}&days=${days}&call_type=${callType}`
        );
        const calls = await response.json();

        document.getElementById('categoryCallCount').textContent = calls.length;
        document.getElementById('categoryCallsBody').innerHTML = calls.map(c => {
            const callId = escapeHtml(String(c.call_id || ''));
            const summary = c.summary ? String(c.summary) : '';
            return `
            <tr class="call-row" onclick="showCallDetails('${callId}')">
                <td><code class="text-primary">${callId || '-'}</code></td>
                <td><span class="badge ${c.type === 'CALL' ? 'bg-primary' : 'bg-success'}">${c.type || '-'}</span></td>
                <td>${c.created || '-'}</td>
                <td>${getSentimentBadge(c.sentiment)}</td>
                <td>${c.satisfaction || '-'}</td>
                <td>${getChurnBadge(c.churn_score)}</td>
                <td class="summary-preview" title="${escapeHtml(summary)}">${summary.substring(0, 60)}...</td>
            </tr>
            `;
        }).join('');

        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';

    } catch (error) {
        console.error('Error loading queue calls:', error);
        document.getElementById('categoryCallsBody').innerHTML =
            '<tr><td colspan="7" class="text-center text-danger">Error loading calls</td></tr>';
        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    }
}

// ========================================
// QUEUE DISTRIBUTION (Donut + Table)
// ========================================

// Soft color palette for queue distribution
const queueColors = [
    'rgba(99, 179, 237, 0.8)',   // Sky blue
    'rgba(52, 211, 153, 0.8)',   // Teal
    'rgba(251, 191, 36, 0.8)',   // Amber
    'rgba(167, 139, 250, 0.8)',  // Purple
    'rgba(245, 101, 101, 0.8)', // Coral
    'rgba(34, 197, 94, 0.8)',    // Green
    'rgba(249, 115, 22, 0.8)',   // Orange
    'rgba(139, 92, 246, 0.8)',   // Violet
    'rgba(236, 72, 153, 0.8)',   // Pink
    'rgba(20, 184, 166, 0.8)',   // Cyan
    'rgba(132, 204, 22, 0.8)',   // Lime
    'rgba(244, 63, 94, 0.8)',    // Rose
];

async function loadQueueDistribution() {
    const ctx = document.getElementById('queueDistributionChart');
    if (!ctx) return;

    try {
        const days = getTimeFilterDays();
        const callType = getCallType();
        const response = await fetch(`${API_BASE}/api/queue-distribution?days=${days}&call_type=${callType}`);
        const data = await response.json();

        queueDistributionData = data.queues || [];

        // Update badge
        const badge = document.getElementById('queueChartTotal');
        if (badge) badge.textContent = queueDistributionData.length;

        renderQueueDistributionChart(queueDistributionData, ctx);
        updateQueueRankingTable(queueDistributionData);

    } catch (error) {
        console.error('Error loading queue distribution:', error);
    }
}

function renderQueueDistributionChart(queues, ctx) {
    if (!ctx) {
        ctx = document.getElementById('queueDistributionChart');
        if (!ctx) return;
    }

    if (queueDistributionChartInstance) {
        queueDistributionChartInstance.destroy();
    }

    if (!queues || queues.length === 0) {
        return;
    }

    queueDistributionChartInstance = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: queues.map(q => q.queue_name),
            datasets: [{
                data: queues.map(q => q.call_count),
                backgroundColor: queues.map((_, i) => queueColors[i % queueColors.length]),
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '55%',
            plugins: {
                legend: { display: false },  // Use table instead
                tooltip: {
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    titleColor: '#333',
                    bodyColor: '#666',
                    borderColor: '#e0e0e0',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 12,
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((context.raw / total) * 100).toFixed(1);
                            return `${context.label}: ${context.raw.toLocaleString()} (${percentage}%)`;
                        },
                        afterLabel: function(context) {
                            const q = queues[context.dataIndex];
                            return `Avg Churn: ${q.avg_churn_score || '-'}`;
                        }
                    }
                }
            },
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const queueName = queues[elements[0].index].queue_name;
                    showQueueCalls(queueName);
                }
            },
            onHover: (event, elements) => {
                event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
            }
        }
    });
}

function updateQueueRankingTable(queues) {
    const total = queues.reduce((sum, q) => sum + (q.call_count || 0), 0);
    const tbody = document.getElementById('queueRankingTable');
    if (!tbody) return;

    tbody.innerHTML = queues.map((q, index) => {
        const percentage = total > 0 ? ((q.call_count / total) * 100).toFixed(1) : 0;
        const colorDot = queueColors[index % queueColors.length];
        const escapedQueue = escapeHtml(q.queue_name || 'Unknown');
        return `
            <tr class="queue-row" onclick="showQueueCalls('${escapedQueue}')" style="cursor: pointer;">
                <td>
                    <span style="display: inline-block; width: 10px; height: 10px; border-radius: 50%; background: ${colorDot}; margin-right: 4px;"></span>
                    ${index + 1}
                </td>
                <td class="text-truncate" style="max-width: 120px;" title="${escapedQueue}">${escapedQueue}</td>
                <td class="text-end">${(q.call_count || 0).toLocaleString()}</td>
                <td class="text-end">${percentage}%</td>
            </tr>
        `;
    }).join('');
}

async function showQueueCalls(queueName) {
    const days = getTimeFilterDays();
    const callType = getCallType();

    document.getElementById('categoryModalTitle').textContent = `Queue: ${queueName}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const response = await fetch(
            `${API_BASE}/api/queue-distribution/calls?queue_name=${encodeURIComponent(queueName)}&days=${days}&call_type=${callType}`
        );
        const calls = await response.json();

        document.getElementById('categoryCallCount').textContent = calls.length;
        document.getElementById('categoryCallsBody').innerHTML = calls.map(c => {
            const callId = escapeHtml(String(c.call_id || ''));
            const summary = c.summary ? String(c.summary) : '';
            return `
            <tr class="call-row" onclick="showCallDetails('${callId}')">
                <td><code class="text-primary">${callId || '-'}</code></td>
                <td><span class="badge ${c.type === 'CALL' ? 'bg-primary' : 'bg-success'}">${c.type || '-'}</span></td>
                <td>${c.created || '-'}</td>
                <td>${getSentimentBadge(c.sentiment)}</td>
                <td>${c.satisfaction || '-'}</td>
                <td>${getChurnBadge(c.churn_score)}</td>
                <td class="summary-preview" title="${escapeHtml(summary)}">${summary.substring(0, 60)}...</td>
            </tr>
            `;
        }).join('');

        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';

    } catch (error) {
        console.error('Error loading queue calls:', error);
        document.getElementById('categoryCallsBody').innerHTML =
            '<tr><td colspan="7" class="text-center text-danger">Error loading calls</td></tr>';
        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    }
}


// ========================================
// REPEAT CALLERS
// ========================================

async function loadRepeatCallers() {
    const ctx = document.getElementById('repeatCallersChart');
    if (!ctx) return;

    try {
        const callType = getCallType();
        // Always uses 24h fixed window (backend ignores days param)
        const response = await fetch(`${API_BASE}/api/repeat-callers?call_type=${callType}`);
        const data = await response.json();

        repeatCallersData = data.buckets || [];

        // Update badge
        const badge = document.getElementById('repeatCallersRiskBadge');
        if (badge) {
            if (data.total_high_risk > 0) {
                badge.textContent = `${data.total_high_risk} high risk`;
                badge.style.display = 'inline';
            } else {
                badge.style.display = 'none';
            }
        }

        renderRepeatCallersChart(data, ctx);

        // Load top callers (7+)
        loadTopRepeatCallers();

    } catch (error) {
        console.error('Error loading repeat callers:', error);
    }
}

async function loadTopRepeatCallers() {
    try {
        const callType = getCallType();
        const response = await fetch(`${API_BASE}/api/repeat-callers/top?call_type=${callType}`);
        const data = await response.json();

        const customers = data.customers || [];
        const tbody = document.getElementById('topRepeatCallersTable');
        const badge = document.getElementById('topCallersCount');

        if (badge) {
            if (customers.length > 0) {
                badge.textContent = customers.length;
                badge.style.display = 'inline';
            } else {
                badge.style.display = 'none';
            }
        }

        if (tbody) {
            if (customers.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted">No callers with 7+ calls</td></tr>';
            } else {
                tbody.innerHTML = customers.map(c => {
                    const subscriberNo = escapeHtml(String(c.subscriber_no || ''));
                    const ban = escapeHtml(String(c.ban || ''));
                    return `
                        <tr onclick="showCustomerJourney('${subscriberNo}', '${ban}')" style="cursor: pointer;">
                            <td>${subscriberNo || '-'}</td>
                            <td>${ban || '-'}</td>
                            <td class="text-end fw-bold">${c.call_count}</td>
                            <td>${getChurnBadge(c.max_churn_score)}</td>
                        </tr>
                    `;
                }).join('');
            }
        }

    } catch (error) {
        console.error('Error loading top repeat callers:', error);
    }
}

function renderRepeatCallersChart(data, ctx) {
    if (!ctx) {
        ctx = document.getElementById('repeatCallersChart');
        if (!ctx) return;
    }

    if (repeatCallersChartInstance) {
        repeatCallersChartInstance.destroy();
    }

    const buckets = data.buckets || [];
    if (buckets.length === 0) {
        return;
    }

    repeatCallersChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: buckets.map(b => b.label),
            datasets: [
                {
                    label: 'Normal',
                    data: buckets.map(b => b.count - b.high_risk),
                    backgroundColor: 'rgba(99, 179, 237, 0.7)',   // Soft sky blue
                    borderColor: 'rgba(99, 179, 237, 1)',
                    borderWidth: 1,
                    borderRadius: 4
                },
                {
                    label: 'High Risk',
                    data: buckets.map(b => b.high_risk),
                    backgroundColor: 'rgba(245, 101, 101, 0.7)', // Soft coral red
                    borderColor: 'rgba(245, 101, 101, 1)',
                    borderWidth: 1,
                    borderRadius: 4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { stacked: true, grid: { display: false } },
                y: { stacked: true, beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } }
            },
            plugins: {
                legend: { position: 'top', labels: { usePointStyle: true, padding: 15 } },
                tooltip: {
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    titleColor: '#333',
                    bodyColor: '#666',
                    borderColor: '#e0e0e0',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 12
                }
            },
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const index = elements[0].index;
                    const bucket = buckets[index];
                    // Determine min/max calls based on bucket label
                    let minCalls, maxCalls;
                    if (bucket.label === '2 calls') { minCalls = 2; maxCalls = 2; }
                    else if (bucket.label === '3 calls') { minCalls = 3; maxCalls = 3; }
                    else if (bucket.label === '4 calls') { minCalls = 4; maxCalls = 4; }
                    else if (bucket.label === '5 calls') { minCalls = 5; maxCalls = 5; }
                    else { minCalls = 6; maxCalls = 999; }  // 6+ calls
                    showRepeatCallerSubscribers(bucket.label, minCalls, maxCalls);
                }
            },
            onHover: (event, elements) => {
                event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
            }
        }
    });
}

async function showRepeatCallerSubscribers(label, minCalls, maxCalls) {
    const callType = getCallType();

    document.getElementById('categoryModalTitle').textContent = `Repeat Callers: ${label}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const response = await fetch(
            `${API_BASE}/api/repeat-callers/subscribers?min_calls=${minCalls}&max_calls=${maxCalls}&call_type=${callType}`
        );
        const data = await response.json();
        const subscribers = data.subscribers || [];

        document.getElementById('categoryCallCount').textContent = subscribers.length;

        // Build a custom table for subscribers
        document.getElementById('categoryCallsBody').innerHTML = subscribers.map(s => {
            const subscriberNo = escapeHtml(String(s.subscriber_no || ''));
            const ban = escapeHtml(String(s.ban || ''));
            return `
            <tr class="call-row" onclick="showCustomerJourney('${subscriberNo}', '${ban}')" style="cursor: pointer;">
                <td><code class="text-primary">${subscriberNo || '-'}</code></td>
                <td>${ban || '-'}</td>
                <td>${s.last_call || '-'}</td>
                <td>-</td>
                <td><span class="badge bg-info">${s.call_count} calls</span></td>
                <td>${getChurnBadge(s.max_churn_score)}</td>
                <td class="small text-muted">Click to view journey</td>
            </tr>
            `;
        }).join('');

        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';

    } catch (error) {
        console.error('Error loading repeat caller subscribers:', error);
        document.getElementById('categoryCallsBody').innerHTML =
            '<tr><td colspan="7" class="text-center text-danger">Error loading subscribers</td></tr>';
        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    }
}


// ========================================
// LOAD ALL NEW FEATURES
// ========================================

async function loadNewFeatures() {
    await Promise.all([
        loadTrendComparisons(),
        loadHeatmap(),
        loadProductsBreakdown(),
        loadAgentPerformance(),
        loadQueueDistribution(),
        loadRepeatCallers()
    ]);
}
