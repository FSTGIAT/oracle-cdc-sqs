/**
 * Dashboard Utility Functions
 * Shared helpers used across all modules
 */

const API_BASE = '';

// Escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Get sentiment badge HTML
function getSentimentBadge(sentiment) {
    if (!sentiment || typeof sentiment !== 'string') return '<span class="badge bg-secondary">-</span>';
    const s = sentiment.toLowerCase();
    if (s.includes('חיובי') || s.includes('positive')) return '<span class="badge bg-success">+</span>';
    if (s.includes('שלילי') || s.includes('negative')) return '<span class="badge bg-danger">-</span>';
    return '<span class="badge bg-secondary">~</span>';
}

// Get churn risk badge HTML
function getChurnBadge(score) {
    if (score === null || score === undefined) return '<span class="badge bg-secondary">-</span>';
    const numScore = parseFloat(score);
    if (numScore >= 70) return `<span class="badge bg-danger">${numScore}</span>`;
    if (numScore >= 40) return `<span class="badge bg-warning text-dark">${numScore}</span>`;
    return `<span class="badge bg-success">${numScore}</span>`;
}

// Format number with commas
function formatNumber(num) {
    if (num === null || num === undefined) return '-';
    return num.toLocaleString();
}

// Format percentage with sign
function formatPercent(value, decimals = 1) {
    if (value === null || value === undefined) return '-';
    return value.toFixed(decimals) + '%';
}

// Get delta indicator HTML (up/down arrow with color)
function getDeltaIndicator(value, percent, inverse = false) {
    if (value === 0) {
        return '<span class="delta-same">→ 0%</span>';
    }
    const isPositive = value > 0;
    const isGood = inverse ? !isPositive : isPositive;
    const cssClass = isGood ? 'delta-up' : 'delta-down';
    const arrow = isPositive ? '↑' : '↓';
    return `<span class="${cssClass}"><span class="delta-arrow">${arrow}</span><span class="delta-percent">${Math.abs(percent).toFixed(1)}%</span></span>`;
}

// Debounce function
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Get time filter value
function getTimeFilterDays() {
    const el = document.getElementById('timeFilter');
    return el ? parseInt(el.value, 10) : 7;
}

// Show loading spinner in element
function showLoading(elementId) {
    const el = document.getElementById(elementId);
    if (el) {
        el.innerHTML = `
            <div class="text-center py-4">
                <div class="spinner-border text-primary" role="status"></div>
                <p class="mt-2 text-muted">Loading...</p>
            </div>
        `;
    }
}

// Show error message in element
function showError(elementId, message) {
    const el = document.getElementById(elementId);
    if (el) {
        el.innerHTML = `
            <div class="text-center py-4">
                <div class="text-danger">
                    <i class="bi bi-exclamation-triangle" style="font-size: 2rem;"></i>
                    <p class="mt-2">${escapeHtml(message)}</p>
                </div>
            </div>
        `;
    }
}
