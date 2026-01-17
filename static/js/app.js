/**
 * Dashboard Main Application
 * Initializes all modules and handles data fetching
 */

// Fetch and update all dashboard data
async function fetchData() {
    const days = getTimeFilterDays();

    try {
        // Fetch all data in parallel
        const [summary, sentiment, churn, satisfaction, categories, errors, recent] = await Promise.all([
            fetch(`${API_BASE}/api/summary?days=${days}`).then(r => r.json()),
            fetch(`${API_BASE}/api/sentiment?days=${days}`).then(r => r.json()),
            fetch(`${API_BASE}/api/churn?days=${days}`).then(r => r.json()),
            fetch(`${API_BASE}/api/satisfaction?days=${days}`).then(r => r.json()),
            fetch(`${API_BASE}/api/categories?days=${days}`).then(r => r.json()),
            fetch(`${API_BASE}/api/errors?days=${days}`).then(r => r.json()),
            fetch(`${API_BASE}/api/recent?days=${days}`).then(r => r.json())
        ]);

        // Update summary KPIs
        document.getElementById('totalConversations').textContent = summary.total || 0;
        document.getElementById('callCount').textContent = summary.calls || 0;
        document.getElementById('whatsappCount').textContent = summary.whatsapp || 0;
        document.getElementById('avgSatisfaction').textContent = summary.avg_satisfaction?.toFixed(1) || '-';
        document.getElementById('avgChurn').textContent = summary.avg_churn_score?.toFixed(1) || '-';

        const total = (summary.positive || 0) + (summary.negative || 0) + (summary.neutral || 0);
        const posPercent = total > 0 ? ((summary.positive / total) * 100).toFixed(1) : 0;
        document.getElementById('positivePercent').textContent = posPercent + '%';

        // Update charts
        updateSentimentChart(sentiment);
        updateChurnChart(churn);
        updateSatisfactionChart(satisfaction);
        updateCategoriesChart(categories);

        // Update errors count
        const errorCountEl = document.getElementById('errorCount');
        if (errorCountEl) {
            errorCountEl.textContent = errors.length > 0 ? `${errors.length} errors` : '0 errors';
        }

        // Update errors table if it exists
        const errorsTable = document.getElementById('errorsTable');
        if (errorsTable) {
            errorsTable.innerHTML = errors.slice(0, 20).map(e => `
                <tr class="error-row">
                    <td>${e.timestamp || '-'}</td>
                    <td><span class="badge bg-danger">${e.error_type || '-'}</span></td>
                    <td>${e.call_id || '-'}</td>
                    <td title="${escapeHtml(e.error_message || '')}">${(e.error_message || '').substring(0, 50)}...</td>
                </tr>
            `).join('');
        }

        // Update recent conversations table
        document.getElementById('recentTable').innerHTML = recent.map(r => `
            <tr class="call-row" onclick="showCallDetails('${escapeHtml(String(r.call_id || ''))}')">
                <td>${r.created || '-'}</td>
                <td><span class="badge ${r.type === 'CALL' ? 'bg-primary' : 'bg-success'}">${r.type}</span></td>
                <td>${getSentimentBadge(r.sentiment)}</td>
                <td>${r.satisfaction || '-'}</td>
                <td>${getChurnBadge(r.churn_score)}</td>
                <td class="summary-preview" title="${escapeHtml(r.summary || '')}">${(r.summary || '').substring(0, 40)}...</td>
            </tr>
        `).join('');

        // Update last refresh time
        const lastUpdateEl = document.getElementById('lastUpdate');
        if (lastUpdateEl) {
            lastUpdateEl.textContent = 'Updated: ' + new Date().toLocaleTimeString();
        }

        // Load new features data (heatmap, products, agent performance)
        if (typeof loadHeatmap === 'function') {
            loadHeatmap();
        }
        if (typeof loadProductsBreakdown === 'function') {
            loadProductsBreakdown();
        }
        if (typeof loadAgentPerformance === 'function') {
            loadAgentPerformance();
        }

    } catch (error) {
        console.error('Error fetching data:', error);
        const lastUpdateEl = document.getElementById('lastUpdate');
        if (lastUpdateEl) {
            lastUpdateEl.textContent = 'Error loading data';
        }
    }
}

// Initialize dashboard
document.addEventListener('DOMContentLoaded', async () => {
    const splash = document.getElementById('splashScreen');
    const splashShown = sessionStorage.getItem('splashShown');

    // Initialize Bootstrap modals
    initModals();

    // Initialize charts
    initCharts();
    initEvalHistoryChart();

    // Initialize GridStack for drag-and-drop (if enabled)
    if (typeof GridStack !== 'undefined') {
        initGridStack();
    }

    // Handle splash screen and initial data load
    if (!splashShown && splash) {
        // First visit or hard refresh - show splash and preload all data
        splash.style.display = 'flex';

        const minTime = new Promise(resolve => setTimeout(resolve, 2000));
        const dataLoad = Promise.all([
            fetchData(),
            typeof loadChurnAnalytics === 'function' ? loadChurnAnalytics() : Promise.resolve()
        ]);

        // Wait for both minimum time AND data to load
        await Promise.all([minTime, dataLoad]);

        // Fade out splash
        splash.classList.add('fade-out');
        setTimeout(() => {
            splash.style.display = 'none';
        }, 500);

        // Mark splash as shown for this session
        sessionStorage.setItem('splashShown', 'true');

        // Mark churn as loaded since we preloaded it
        if (typeof churnAnalyticsLoaded !== 'undefined') {
            churnAnalyticsLoaded = true;
        }
    } else {
        // Normal load - hide splash immediately, load data normally
        if (splash) splash.style.display = 'none';
        fetchData();
    }

    // Time filter change handler
    const timeFilter = document.getElementById('timeFilter');
    if (timeFilter) {
        timeFilter.addEventListener('change', fetchData);
    }

    // Auto-refresh every 30 seconds
    setInterval(fetchData, 30000);

    // ML Quality tab lazy loading
    const mlQualityTab = document.getElementById('mlquality-tab');
    if (mlQualityTab) {
        mlQualityTab.addEventListener('shown.bs.tab', () => {
            if (!mlQualityLoaded) {
                loadMLQualityData();
            }
        });
    }

    // Churn Analytics tab lazy loading
    const churnAnalyticsTab = document.getElementById('churnanalytics-tab');
    if (churnAnalyticsTab) {
        churnAnalyticsTab.addEventListener('shown.bs.tab', () => {
            if (!churnAnalyticsLoaded) {
                loadChurnAnalytics();
                churnAnalyticsLoaded = true;
            }
        });
    }

    // Churn trend days filter
    const churnTrendDays = document.getElementById('churnTrendDays');
    if (churnTrendDays) {
        churnTrendDays.addEventListener('change', function() {
            loadChurnTrend(this.value);
        });
    }

    // Check for pending ML recommendations (badge in nav)
    checkPendingRecommendations();

    console.log('Dashboard initialized');
});
