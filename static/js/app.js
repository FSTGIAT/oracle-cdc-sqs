/**
 * Dashboard Main Application
 * Initializes all modules and handles data fetching
 */

// Auto-refresh interval (10 minutes)
const AUTO_REFRESH_INTERVAL = 600000;

// Update the time picker label to show current selection
function updateTimePickerLabel() {
    const mode = document.getElementById('timeFilterMode')?.value || 'quick';
    const label = document.getElementById('timePickerLabel');

    if (mode === 'range') {
        const startEl = document.getElementById('timeFilterStart');
        const endEl = document.getElementById('timeFilterEnd');
        if (startEl?.value && endEl?.value) {
            const startDate = new Date(startEl.value);
            const endDate = new Date(endEl.value);
            const formatDate = (d) => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            label.textContent = `${formatDate(startDate)} - ${formatDate(endDate)}`;
        }
    } else {
        const value = document.getElementById('timeFilterValue')?.value || 24;
        const unit = document.getElementById('timeFilterUnit')?.value || 'hours';

        const unitLabels = {
            'minutes': value == 1 ? 'minute' : 'minutes',
            'hours': value == 1 ? 'hour' : 'hours',
            'days': value == 1 ? 'day' : 'days'
        };

        if (label) {
            label.textContent = `Last ${value} ${unitLabels[unit]}`;
        }
    }

    // Update quick button active state
    const value = document.getElementById('timeFilterValue')?.value;
    const unit = document.getElementById('timeFilterUnit')?.value;
    document.querySelectorAll('.time-quick-btn').forEach(btn => {
        const btnValue = btn.dataset.value;
        const btnUnit = btn.dataset.unit;
        btn.classList.toggle('active', btnValue === value && btnUnit === unit && mode === 'quick');
    });
}

// Initialize date range inputs with default values
function initializeDateRangePicker() {
    const now = new Date();
    const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    const formatForInput = (d) => {
        const pad = (n) => n.toString().padStart(2, '0');
        return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
    };

    const startEl = document.getElementById('timeFilterStart');
    const endEl = document.getElementById('timeFilterEnd');

    if (startEl) startEl.value = formatForInput(yesterday);
    if (endEl) endEl.value = formatForInput(now);

    // When date inputs change, switch to range mode
    [startEl, endEl].forEach(el => {
        if (el) {
            el.addEventListener('change', () => {
                document.getElementById('timeFilterMode').value = 'range';
                document.querySelectorAll('.time-quick-btn').forEach(btn => btn.classList.remove('active'));
            });
        }
    });
}

// Fetch and update all dashboard data
async function fetchData() {
    const days = getTimeFilterDays();
    const callType = getCallType();

    try {
        // Fetch all data in parallel
        const [summary, sentiment, churn, satisfaction, categories, errors, recent] = await Promise.all([
            fetch(`${API_BASE}/api/summary?days=${days}&call_type=${callType}`).then(r => r.json()),
            fetch(`${API_BASE}/api/sentiment?days=${days}&call_type=${callType}`).then(r => r.json()),
            fetch(`${API_BASE}/api/churn?days=${days}&call_type=${callType}`).then(r => r.json()),
            fetch(`${API_BASE}/api/satisfaction?days=${days}&call_type=${callType}`).then(r => r.json()),
            fetch(`${API_BASE}/api/categories?days=${days}&call_type=${callType}`).then(r => r.json()),
            fetch(`${API_BASE}/api/errors?days=${days}`).then(r => r.json()),
            fetch(`${API_BASE}/api/recent?days=${days}&call_type=${callType}`).then(r => r.json())
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
        const recentTable = document.getElementById('recentTable');
        recentTable.innerHTML = recent.map(r => `
            <tr class="call-row" data-call-id="${escapeHtml(String(r.id || ''))}">
                <td>${r.created || '-'}</td>
                <td><span class="badge ${r.type === 'CALL' ? 'bg-primary' : 'bg-success'}">${r.type}</span></td>
                <td>${getSentimentBadge(r.sentiment)}</td>
                <td>${r.satisfaction || '-'}</td>
                <td>${getChurnBadge(r.churn_score)}</td>
                <td class="summary-preview" title="${escapeHtml(r.summary || '')}">${(r.summary || '').substring(0, 40)}...</td>
            </tr>
        `).join('');

        // Add click handlers for call rows
        recentTable.querySelectorAll('.call-row').forEach(row => {
            row.onclick = () => showCallDetails(row.dataset.callId);
        });

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

    // Initialize date range picker
    initializeDateRangePicker();

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

    // Time filter apply button handler
    const timeFilterApply = document.getElementById('timeFilterApply');
    if (timeFilterApply) {
        timeFilterApply.addEventListener('click', () => {
            updateTimePickerLabel();
            // Close the dropdown
            const dropdown = bootstrap.Dropdown.getInstance(document.getElementById('timePickerBtn'));
            if (dropdown) dropdown.hide();

            fetchData();
            // Reload other sections if they're loaded
            if (typeof loadChurnAnalytics === 'function' && churnAnalyticsLoaded) {
                loadChurnAnalytics();
            }
            if (typeof loadCategoriesOverview === 'function' && categoriesOverviewLoaded) {
                loadCategoriesOverview();
            }
        });
    }

    // Quick select buttons handler
    document.querySelectorAll('.time-quick-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const value = btn.dataset.value;
            const unit = btn.dataset.unit;

            // Update hidden fields
            document.getElementById('timeFilterValue').value = value;
            document.getElementById('timeFilterUnit').value = unit;
            document.getElementById('timeFilterMode').value = 'quick';

            // Update active state
            document.querySelectorAll('.time-quick-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // Update label and close dropdown
            updateTimePickerLabel();
            const dropdown = bootstrap.Dropdown.getInstance(document.getElementById('timePickerBtn'));
            if (dropdown) dropdown.hide();

            // Trigger refresh
            fetchData();
            if (typeof loadChurnAnalytics === 'function' && churnAnalyticsLoaded) {
                loadChurnAnalytics();
            }
            if (typeof loadCategoriesOverview === 'function' && categoriesOverviewLoaded) {
                loadCategoriesOverview();
            }
        });
    });

    // Allow Enter key to trigger time filter
    const timeFilterValue = document.getElementById('timeFilterValue');
    const timeFilterUnit = document.getElementById('timeFilterUnit');

    if (timeFilterValue) {
        timeFilterValue.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                timeFilterApply?.click();
            }
        });
        // Update quick button states when custom value changes
        timeFilterValue.addEventListener('input', () => {
            document.querySelectorAll('.time-quick-btn').forEach(btn => {
                const btnValue = btn.dataset.value;
                const btnUnit = btn.dataset.unit;
                btn.classList.toggle('active',
                    btnValue === timeFilterValue.value && btnUnit === timeFilterUnit?.value);
            });
        });
    }

    if (timeFilterUnit) {
        timeFilterUnit.addEventListener('change', () => {
            document.querySelectorAll('.time-quick-btn').forEach(btn => {
                const btnValue = btn.dataset.value;
                const btnUnit = btn.dataset.unit;
                btn.classList.toggle('active',
                    btnValue === timeFilterValue?.value && btnUnit === timeFilterUnit.value);
            });
        });
    }

    // Call type toggle handler
    const callTypeToggle = document.getElementById('callTypeToggle');
    if (callTypeToggle) {
        callTypeToggle.addEventListener('click', (e) => {
            const btn = e.target.closest('button[data-call-type]');
            if (!btn) return;

            const newType = btn.dataset.callType;
            if (newType === getCallType()) return;  // Already selected

            // Update button states
            callTypeToggle.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // Update global state and trigger refresh
            setCallType(newType);
            fetchData();

            // Reload churn analytics if loaded
            if (typeof loadChurnAnalytics === 'function' && churnAnalyticsLoaded) {
                loadChurnAnalytics();
            }
            // Reload categories overview if loaded
            if (typeof loadCategoriesOverview === 'function' && categoriesOverviewLoaded) {
                loadCategoriesOverview();
            }
        });
    }

    // Auto-refresh every 10 minutes
    setInterval(fetchData, AUTO_REFRESH_INTERVAL);
    setInterval(() => {
        if (churnAnalyticsLoaded) {
            loadChurnAnalytics();
        }
    }, AUTO_REFRESH_INTERVAL);

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

    // Check for pending ML recommendations (badge in nav)
    checkPendingRecommendations();

    console.log('Dashboard initialized');
});
