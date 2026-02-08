/**
 * New Dashboard Features Module
 * Heatmap, Trend Comparisons, Products Breakdown, Agent Performance
 */

let productsChartInstance, agentPerformanceChartInstance;

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
// LOAD ALL NEW FEATURES
// ========================================

async function loadNewFeatures() {
    await Promise.all([
        loadTrendComparisons(),
        loadHeatmap(),
        loadProductsBreakdown(),
        loadAgentPerformance()
    ]);
}
