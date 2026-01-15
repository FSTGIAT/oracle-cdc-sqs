/**
 * ML Quality Tab Module
 * Recommendations, evaluation history, and feedback management
 */

let evalHistoryChart;
let mlQualityLoaded = false;

// Initialize ML Quality evaluation history chart
function initEvalHistoryChart() {
    const ctx = document.getElementById('evalHistoryChart');
    if (!ctx) return;

    evalHistoryChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Recall %',
                    data: [],
                    borderColor: '#0d6efd',
                    backgroundColor: 'rgba(13, 110, 253, 0.1)',
                    fill: true,
                    tension: 0.3
                },
                {
                    label: 'Coverage %',
                    data: [],
                    borderColor: '#198754',
                    backgroundColor: 'rgba(25, 135, 84, 0.1)',
                    fill: true,
                    tension: 0.3
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top' }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    title: { display: true, text: 'Percentage' }
                }
            }
        }
    });
}

// Load ML Quality data
async function loadMLQualityData() {
    try {
        const [metrics, recommendations, history] = await Promise.all([
            fetch(`${API_BASE}/api/ml-quality/metrics`).then(r => r.json()),
            fetch(`${API_BASE}/api/ml-quality/recommendations`).then(r => r.json()),
            fetch(`${API_BASE}/api/ml-quality/history`).then(r => r.json())
        ]);

        // Update metrics cards
        if (metrics.last_evaluation) {
            document.getElementById('mlRecall').textContent =
                metrics.last_evaluation.recall_percent ? `${metrics.last_evaluation.recall_percent}%` : '-';
            document.getElementById('mlCoverage').textContent =
                metrics.last_evaluation.coverage_percent ? `${metrics.last_evaluation.coverage_percent}%` : '-';
        }

        document.getElementById('mlPending').textContent = metrics.pending_recommendations || 0;

        // Update pending badge in nav
        const pendingBadge = document.getElementById('pendingBadge');
        if (metrics.pending_recommendations > 0) {
            pendingBadge.textContent = metrics.pending_recommendations;
            pendingBadge.style.display = 'inline';
        } else {
            pendingBadge.style.display = 'none';
        }

        if (metrics.feedback_stats) {
            document.getElementById('mlFeedbackTotal').textContent =
                metrics.feedback_stats.total_feedback || 0;
            document.getElementById('mlFeedbackCorrect').textContent =
                `${metrics.feedback_stats.correct_count || 0} correct / ${metrics.feedback_stats.incorrect_count || 0} incorrect`;
        }

        renderRecommendations(recommendations);
        updateEvalHistory(history);

        mlQualityLoaded = true;

    } catch (error) {
        console.error('Error loading ML quality data:', error);
        const loading = document.getElementById('recommendationsLoading');
        if (loading) {
            loading.innerHTML = '<div class="text-danger">Error loading data. Check console.</div>';
        }
    }
}

// Render recommendations list
function renderRecommendations(recommendations) {
    const loading = document.getElementById('recommendationsLoading');
    const empty = document.getElementById('recommendationsEmpty');
    const list = document.getElementById('recommendationsList');

    if (loading) loading.style.display = 'none';

    if (!recommendations || recommendations.length === 0) {
        if (empty) empty.style.display = 'block';
        if (list) list.innerHTML = '';
        return;
    }

    if (empty) empty.style.display = 'none';

    list.innerHTML = recommendations.map(rec => {
        const details = rec.rec_details || {};
        let detailsHtml = '';

        if (rec.rec_type === 'churn_keywords') {
            const keywords = details.keywords || [];
            detailsHtml = `
                <div class="mb-2">
                    <strong>New Keywords:</strong>
                    ${keywords.map(k => `<span class="badge bg-secondary me-1">${escapeHtml(k)}</span>`).join('')}
                </div>
                <div class="text-muted small">${escapeHtml(details.reason || '')}</div>
            `;
        } else if (rec.rec_type === 'churn_threshold') {
            detailsHtml = `
                <div class="mb-2">
                    <strong>Threshold Change:</strong>
                    ${details.current_value || '?'} → ${details.recommended_value || '?'}
                </div>
                <div class="text-muted small">${escapeHtml(details.reason || '')}</div>
                <div class="text-warning small">${escapeHtml(details.impact || '')}</div>
            `;
        } else if (rec.rec_type === 'classification_fix') {
            const misclass = details.misclassifications || [];
            detailsHtml = `
                <div class="mb-2">
                    <strong>Misclassifications Found:</strong>
                    <ul class="mb-0 small">
                        ${misclass.map(m => `<li>${escapeHtml(m.predicted)} → ${escapeHtml(m.actual)} (${m.count}x)</li>`).join('')}
                    </ul>
                </div>
            `;
        }

        return `
            <div class="card mb-3 border-warning">
                <div class="card-header d-flex justify-content-between align-items-center py-2">
                    <span>
                        <span class="badge bg-warning text-dark me-2">${escapeHtml(rec.rec_type)}</span>
                        <small class="text-muted">${rec.created_at}</small>
                    </span>
                    <div>
                        <button class="btn btn-sm btn-success me-1" onclick="approveRecommendation('${rec.rec_id}')">
                            Approve
                        </button>
                        <button class="btn btn-sm btn-outline-danger" onclick="rejectRecommendation('${rec.rec_id}')">
                            Reject
                        </button>
                    </div>
                </div>
                <div class="card-body py-2">
                    ${detailsHtml}
                </div>
            </div>
        `;
    }).join('');
}

// Update evaluation history chart and table
function updateEvalHistory(history) {
    const table = document.getElementById('evalHistoryTable');
    if (!history || history.length === 0) {
        if (table) {
            table.innerHTML = '<tr><td colspan="7" class="text-center text-muted">No evaluation history yet</td></tr>';
        }
        return;
    }

    // Update chart
    if (evalHistoryChart) {
        const labels = history.map(h => h.eval_date).reverse();
        const recallData = history.map(h => h.recall_percent || 0).reverse();
        const coverageData = history.map(h => h.coverage_percent || 0).reverse();

        evalHistoryChart.data.labels = labels;
        evalHistoryChart.data.datasets[0].data = recallData;
        evalHistoryChart.data.datasets[1].data = coverageData;
        evalHistoryChart.update();
    }

    // Update table
    if (table) {
        table.innerHTML = history.map(h => `
            <tr>
                <td>${h.eval_date || '-'}</td>
                <td>${h.churned_count || 0}</td>
                <td>${h.with_score_count || 0}</td>
                <td><span class="badge ${h.recall_percent >= 70 ? 'bg-success' : h.recall_percent >= 50 ? 'bg-warning' : 'bg-danger'}">${h.recall_percent || 0}%</span></td>
                <td><span class="badge ${h.coverage_percent >= 80 ? 'bg-success' : 'bg-warning'}">${h.coverage_percent || 0}%</span></td>
                <td>${h.avg_churn_score || '-'}</td>
                <td>${h.recommendations_generated || 0}</td>
            </tr>
        `).join('');
    }
}

// Approve recommendation
async function approveRecommendation(recId) {
    if (!confirm('Approve this recommendation? Config will be uploaded to S3.')) return;

    try {
        const response = await fetch(`${API_BASE}/api/ml-quality/approve`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ rec_id: recId, approver: 'dashboard_user' })
        });

        const result = await response.json();

        if (result.success) {
            alert('Approved! Config uploaded to S3. Use "Apply to ML" when ready.');
            loadMLQualityData();
        } else {
            alert('Error: ' + (result.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Approve error:', error);
        alert('Error approving recommendation');
    }
}

// Reject recommendation
async function rejectRecommendation(recId) {
    const reason = prompt('Reason for rejection (optional):');
    if (reason === null) return;

    try {
        const response = await fetch(`${API_BASE}/api/ml-quality/reject`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ rec_id: recId, rejected_by: 'dashboard_user', reason: reason })
        });

        const result = await response.json();

        if (result.success) {
            alert('Recommendation rejected');
            loadMLQualityData();
        } else {
            alert('Error: ' + (result.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Reject error:', error);
        alert('Error rejecting recommendation');
    }
}

// Apply configs to ML service
async function applyToML() {
    if (!confirm('Send reload signal to ML service? This will apply all approved configs.')) return;

    const btn = document.getElementById('applyToMlBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Sending...';

    try {
        const response = await fetch(`${API_BASE}/api/ml-quality/apply-to-ml`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ triggered_by: 'dashboard_user' })
        });

        const result = await response.json();

        if (result.success) {
            alert('SQS message sent. ML service will reload configs shortly.');
        } else {
            alert('Error: ' + (result.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Apply error:', error);
        alert('Error sending reload signal');
    } finally {
        btn.disabled = false;
        btn.innerHTML = 'Apply Approved Configs to ML Service';
    }
}

// Check pending recommendations on load (for badge)
async function checkPendingRecommendations() {
    try {
        const metrics = await fetch(`${API_BASE}/api/ml-quality/metrics`).then(r => r.json());
        const pendingBadge = document.getElementById('pendingBadge');
        if (pendingBadge && metrics.pending_recommendations > 0) {
            pendingBadge.textContent = metrics.pending_recommendations;
            pendingBadge.style.display = 'inline';
        }
    } catch (e) {
        // Silently fail if ML tables don't exist yet
    }
}
