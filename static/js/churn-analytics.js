/**
 * Churn Analytics Tab Module
 * Accuracy metrics, product breakdown, trends, and high-risk calls
 */

let churnAccuracyChartInstance, churnByProductChartInstance;
let churnScoreRangeChartInstance, churnTrendChartInstance;
let churnAnalyticsLoaded = false;

// Pagination state for high-risk calls
let highRiskCurrentPage = 0;
let highRiskPageSize = 25;
let highRiskTotalPages = 1;

// Load all churn analytics data
async function loadChurnAnalytics() {
    console.log('Loading churn analytics...');
    try {
        const [accuracyRes, productRes, rangeRes, trendRes, highRiskRes] = await Promise.all([
            fetch(`${API_BASE}/api/churn/accuracy`),
            fetch(`${API_BASE}/api/churn/by-product`),
            fetch(`${API_BASE}/api/churn/by-score-range`),
            fetch(`${API_BASE}/api/churn/trend?days=30`),
            fetch(`${API_BASE}/api/churn/high-risk-calls?days=7&limit=100`)
        ]);

        const accuracy = await accuracyRes.json();
        const products = await productRes.json();
        const ranges = await rangeRes.json();
        const trend = await trendRes.json();
        const highRisk = await highRiskRes.json();

        // === KPI Cards ===
        document.getElementById('churnTotalPredictions').textContent = formatNumber(accuracy.total_predictions);
        document.getElementById('churnActualChurns').textContent = formatNumber(accuracy.actual_churns);
        document.getElementById('churnAccuracyRate').textContent = (accuracy.accuracy_rate || 0) + '%';
        document.getElementById('churnFalsePositives').textContent = formatNumber(accuracy.false_positives);

        // === Accuracy Pie Chart ===
        if (churnAccuracyChartInstance) churnAccuracyChartInstance.destroy();
        churnAccuracyChartInstance = new Chart(document.getElementById('churnAccuracyChart'), {
            type: 'doughnut',
            data: {
                labels: ['Correct Predictions', 'False Positives'],
                datasets: [{
                    data: [accuracy.actual_churns || 0, accuracy.false_positives || 0],
                    backgroundColor: ['#28a745', '#ffc107']
                }]
            },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } }
        });

        // === By Product Chart ===
        if (churnByProductChartInstance) churnByProductChartInstance.destroy();
        churnByProductChartInstance = new Chart(document.getElementById('churnByProductChart'), {
            type: 'bar',
            data: {
                labels: products.map(p => p.product_code || 'Unknown'),
                datasets: [{
                    label: 'Churned Customers',
                    data: products.map(p => p.count),
                    backgroundColor: '#dc3545'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { legend: { display: false } }
            }
        });

        // === Score Range Chart (Grouped Bar) ===
        if (churnScoreRangeChartInstance) churnScoreRangeChartInstance.destroy();
        churnScoreRangeChartInstance = new Chart(document.getElementById('churnScoreRangeChart'), {
            type: 'bar',
            data: {
                labels: ranges.map(r => r.label),
                datasets: [
                    {
                        label: 'Predictions',
                        data: ranges.map(r => r.predictions),
                        backgroundColor: '#0d6efd'
                    },
                    {
                        label: 'Actual Churns',
                        data: ranges.map(r => r.actual_churns),
                        backgroundColor: '#dc3545'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'top' } }
            }
        });

        // === Score Range Table ===
        const tableBody = document.querySelector('#scoreRangeTable tbody');
        if (tableBody) {
            tableBody.innerHTML = ranges.map(r => `
                <tr>
                    <td><strong>${r.label}</strong></td>
                    <td>${formatNumber(r.predictions)}</td>
                    <td>${formatNumber(r.actual_churns)}</td>
                    <td>
                        <span class="badge ${r.accuracy >= 50 ? 'bg-success' : r.accuracy >= 30 ? 'bg-warning' : 'bg-secondary'}">
                            ${r.accuracy || 0}%
                        </span>
                    </td>
                </tr>
            `).join('');
        }

        // === Trend Chart ===
        renderChurnTrendChart(trend);

        // === High Risk Table ===
        renderHighRiskCalls(highRisk);

        churnAnalyticsLoaded = true;

    } catch (error) {
        console.error('Error loading churn analytics:', error);
        alert('Error loading churn analytics: ' + error.message);
    }
}

// Render churn trend chart
function renderChurnTrendChart(trend) {
    if (churnTrendChartInstance) churnTrendChartInstance.destroy();
    churnTrendChartInstance = new Chart(document.getElementById('churnTrendChart'), {
        type: 'line',
        data: {
            labels: trend.map(t => t.call_date),
            datasets: [
                {
                    label: 'High Risk (70+)',
                    data: trend.map(t => t.high_risk),
                    borderColor: '#dc3545',
                    backgroundColor: 'rgba(220, 53, 69, 0.1)',
                    fill: true
                },
                {
                    label: 'Medium Risk (40-70)',
                    data: trend.map(t => t.medium_risk),
                    borderColor: '#ffc107',
                    backgroundColor: 'rgba(255, 193, 7, 0.1)',
                    fill: true
                },
                {
                    label: 'Low Risk (0-40)',
                    data: trend.map(t => t.low_risk),
                    borderColor: '#28a745',
                    backgroundColor: 'rgba(40, 167, 69, 0.1)',
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: { y: { beginAtZero: true } }
        }
    });
}

// Load churn trend with different days
async function loadChurnTrend(days) {
    try {
        const res = await fetch(`${API_BASE}/api/churn/trend?days=${days}`);
        const trend = await res.json();
        renderChurnTrendChart(trend);
    } catch (error) {
        console.error('Error loading churn trend:', error);
    }
}

// Load high risk calls with filters
async function loadHighRiskCalls(page = 0) {
    const minScore = document.getElementById('minScoreFilter')?.value || 70;
    const maxScore = document.getElementById('maxScoreFilter')?.value || 100;
    const offset = page * highRiskPageSize;

    try {
        const res = await fetch(
            `${API_BASE}/api/churn/high-risk-calls?days=7&min_score=${minScore}&max_score=${maxScore}&offset=${offset}&limit=${highRiskPageSize}`
        );
        const result = await res.json();
        renderHighRiskCalls(result);
    } catch (error) {
        console.error('Error loading high risk calls:', error);
    }
}

// Change page for high risk calls
function changeHighRiskPage(delta) {
    const newPage = highRiskCurrentPage + delta;
    if (newPage >= 0 && newPage < highRiskTotalPages) {
        loadHighRiskCalls(newPage);
    }
}

// Render high risk calls table
function renderHighRiskCalls(result) {
    const calls = result.data || result;
    const total = result.total ?? calls.length;
    const offset = result.offset ?? 0;
    const limit = result.limit ?? calls.length;
    const pages = result.pages ?? 1;

    highRiskCurrentPage = Math.floor(offset / highRiskPageSize);
    highRiskTotalPages = pages;

    document.getElementById('highRiskCount').textContent = total;

    const tbody = document.querySelector('#highRiskTable tbody');
    if (tbody) {
        tbody.innerHTML = calls.map(c => `
            <tr class="call-row" onclick="showCallDetails('${escapeHtml(String(c.call_id || ''))}')">
                <td><code class="text-primary">${c.call_id || '-'}</code></td>
                <td>${c.created || '-'}</td>
                <td><span class="badge bg-danger">${c.churn_score || '-'}</span></td>
                <td>${c.subscriber_no || '-'}</td>
                <td>
                    <span class="badge ${c.sub_status === 'A' ? 'bg-success' : c.sub_status === 'C' ? 'bg-danger' : 'bg-secondary'}">
                        ${c.sub_status === 'A' ? 'Active' : c.sub_status === 'C' ? 'Churned' : c.sub_status || 'Unknown'}
                    </span>
                </td>
                <td>${c.product_code || '-'}</td>
                <td class="summary-preview">${(c.summary || '-').substring(0, 60)}${c.summary?.length > 60 ? '...' : ''}</td>
            </tr>
        `).join('');
    }

    // Update pagination controls
    const start = total > 0 ? offset + 1 : 0;
    const end = Math.min(offset + limit, total);
    document.getElementById('highRiskShowing').textContent = `${start}-${end}`;
    document.getElementById('highRiskTotal').textContent = total;
    document.getElementById('highRiskPageInfo').textContent = `${highRiskCurrentPage + 1} / ${highRiskTotalPages}`;

    const prevPage = document.getElementById('highRiskPrevPage');
    const nextPage = document.getElementById('highRiskNextPage');
    if (prevPage) prevPage.classList.toggle('disabled', highRiskCurrentPage === 0);
    if (nextPage) nextPage.classList.toggle('disabled', highRiskCurrentPage >= highRiskTotalPages - 1);
}
