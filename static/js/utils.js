/**
 * Dashboard Utility Functions
 * Shared helpers used across all modules
 */

const API_BASE = '';

// ========================================
// CALL TYPE STATE (Service/Sales)
// ========================================
let currentCallType = 'service';

function getCallType() {
    return currentCallType;
}

function setCallType(type) {
    currentCallType = type;
    document.dispatchEvent(new CustomEvent('callTypeChanged', { detail: { type } }));
}

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

// Get time filter value (converts to days)
function getTimeFilterDays() {
    const mode = document.getElementById('timeFilterMode')?.value || 'quick';

    if (mode === 'range') {
        const startEl = document.getElementById('timeFilterStart');
        const endEl = document.getElementById('timeFilterEnd');

        if (startEl?.value && endEl?.value) {
            const startDate = new Date(startEl.value);
            const endDate = new Date(endEl.value);
            const diffMs = endDate.getTime() - startDate.getTime();
            const diffDays = diffMs / (1000 * 60 * 60 * 24);
            return Math.max(diffDays, 0.01);  // Minimum ~15 minutes
        }
    }

    // Quick select mode
    const valueEl = document.getElementById('timeFilterValue');
    const unitEl = document.getElementById('timeFilterUnit');

    const value = parseFloat(valueEl?.value) || 24;
    const unit = unitEl?.value || 'hours';

    switch (unit) {
        case 'minutes':
            return value / (24 * 60);  // Convert minutes to days
        case 'hours':
            return value / 24;  // Convert hours to days
        default:
            return value;  // Already in days
    }
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
