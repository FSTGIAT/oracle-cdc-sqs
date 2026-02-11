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
// QUEUE DISTRIBUTION
// ========================================

async function loadQueueDistribution() {
    const ctx = document.getElementById('queueDistributionChart');
    if (!ctx) return;

    try {
        const days = getTimeFilterDays();
        const callType = getCallType();
        const response = await fetch(`${API_BASE}/api/queue-distribution?days=${days}&call_type=${callType}`);
        const data = await response.json();

        queueDistributionData = data.queues || [];
        renderQueueDistributionChart(queueDistributionData, ctx);

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

    // Soft color palette based on avg_churn_score
    const getBarColor = (churnScore) => {
        if (churnScore >= 70) return 'rgba(245, 101, 101, 0.7)';  // Soft coral (high risk)
        if (churnScore >= 40) return 'rgba(251, 191, 36, 0.7)';   // Soft amber (medium)
        return 'rgba(52, 211, 153, 0.7)';                         // Soft teal (low risk)
    };

    const getBorderColor = (churnScore) => {
        if (churnScore >= 70) return 'rgba(245, 101, 101, 1)';
        if (churnScore >= 40) return 'rgba(251, 191, 36, 1)';
        return 'rgba(52, 211, 153, 1)';
    };

    queueDistributionChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: queues.map(q => q.queue_name),
            datasets: [{
                label: 'Calls',
                data: queues.map(q => q.call_count),
                backgroundColor: queues.map(q => getBarColor(q.avg_churn_score)),
                borderColor: queues.map(q => getBorderColor(q.avg_churn_score)),
                borderWidth: 1,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',  // Horizontal bars
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                    titleColor: '#333',
                    bodyColor: '#666',
                    borderColor: '#e0e0e0',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 12,
                    callbacks: {
                        afterLabel: (context) => {
                            const q = queues[context.dataIndex];
                            return `Avg Satisfaction: ${q.avg_satisfaction || '-'}\nAvg Churn: ${q.avg_churn_score || '-'}`;
                        }
                    }
                }
            },
            scales: {
                x: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.05)' } },
                y: { grid: { display: false } }
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

    } catch (error) {
        console.error('Error loading repeat callers:', error);
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
                    // Determine min/max calls for this bucket
                    let minCalls, maxCalls;
                    if (index === 0) { minCalls = 1; maxCalls = 1; }
                    else if (index === 1) { minCalls = 2; maxCalls = 2; }
                    else if (index === 2) { minCalls = 3; maxCalls = 3; }
                    else { minCalls = 4; maxCalls = 999; }
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
