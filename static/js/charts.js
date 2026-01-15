/**
 * Dashboard Charts Module
 * Chart initialization and rendering using Chart.js
 */

// Chart instances
let sentimentChart, churnChart, satisfactionChart, categoriesChart;
let categoryLabels = [];
let churnLabels = [];
let sentimentLabels = [];

// Initialize all analytics charts
function initCharts() {
    const pieOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: 'bottom' } }
    };

    // Sentiment Distribution Chart
    sentimentChart = new Chart(document.getElementById('sentimentChart'), {
        type: 'doughnut',
        data: { labels: [], datasets: [{ data: [], backgroundColor: ['#28a745', '#dc3545', '#6c757d'] }] },
        options: {
            ...pieOptions,
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const index = elements[0].index;
                    const sentiment = sentimentLabels[index];
                    if (sentiment) {
                        showSentimentCalls(sentiment);
                    }
                }
            },
            onHover: (event, elements) => {
                event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
            }
        }
    });

    // Churn Risk Distribution Chart
    churnChart = new Chart(document.getElementById('churnChart'), {
        type: 'doughnut',
        data: { labels: [], datasets: [{ data: [], backgroundColor: ['#dc3545', '#ffc107', '#28a745'] }] },
        options: {
            ...pieOptions,
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const index = elements[0].index;
                    const riskLevel = churnLabels[index];
                    if (riskLevel) {
                        showChurnRiskCalls(riskLevel);
                    }
                }
            },
            onHover: (event, elements) => {
                event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
            }
        }
    });

    // Satisfaction Distribution Chart
    satisfactionChart = new Chart(document.getElementById('satisfactionChart'), {
        type: 'bar',
        data: { labels: ['1', '2', '3', '4', '5'], datasets: [{ label: 'Count', data: [], backgroundColor: '#0d6efd' }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
    });

    // Categories Chart (Horizontal Bar)
    categoriesChart = new Chart(document.getElementById('categoriesChart'), {
        type: 'bar',
        data: { labels: [], datasets: [{ label: 'Count', data: [], backgroundColor: '#0d6efd' }] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: { legend: { display: false } },
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const index = elements[0].index;
                    const category = categoryLabels[index];
                    if (category) {
                        showCategoryCalls(category);
                    }
                }
            },
            onHover: (event, elements) => {
                event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
            }
        }
    });
}

// Update sentiment chart with data
function updateSentimentChart(sentimentData) {
    const colorMap = { 'Positive': '#28a745', 'Negative': '#dc3545', 'Neutral': '#6c757d' };
    sentimentLabels = sentimentData.map(s => s.sentiment);
    sentimentChart.data.labels = sentimentLabels;
    sentimentChart.data.datasets[0].data = sentimentData.map(s => s.count);
    sentimentChart.data.datasets[0].backgroundColor = sentimentData.map(s => colorMap[s.sentiment] || '#6c757d');
    sentimentChart.update();
}

// Update churn chart with data
function updateChurnChart(churnData) {
    const colorMap = { 'High Risk (70+)': '#dc3545', 'Medium Risk (40-69)': '#ffc107', 'Low Risk (0-39)': '#28a745' };
    churnLabels = churnData.map(c => c.risk_level);
    churnChart.data.labels = churnLabels;
    churnChart.data.datasets[0].data = churnData.map(c => c.count);
    churnChart.data.datasets[0].backgroundColor = churnData.map(c => colorMap[c.risk_level] || '#6c757d');
    churnChart.update();
}

// Update satisfaction chart with data
function updateSatisfactionChart(satisfactionData) {
    const satData = [0, 0, 0, 0, 0];
    satisfactionData.forEach(s => { if (s.rating >= 1 && s.rating <= 5) satData[s.rating - 1] = s.count; });
    satisfactionChart.data.datasets[0].data = satData;
    satisfactionChart.update();
}

// Update categories chart with data
function updateCategoriesChart(categoriesData) {
    categoryLabels = categoriesData.map(c => c.category || 'Unknown');
    categoriesChart.data.labels = categoryLabels;
    categoriesChart.data.datasets[0].data = categoriesData.map(c => c.count);
    categoriesChart.update();
}

// Destroy chart instance safely
function destroyChart(chartInstance) {
    if (chartInstance) {
        chartInstance.destroy();
    }
    return null;
}
