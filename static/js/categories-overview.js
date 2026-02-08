/**
 * Categories Overview Module
 * Big donut chart with all categories and drill-down
 */

let categoriesOverviewChart = null;
let categoriesOverviewLabels = [];
let categoriesOverviewLoaded = false;

// Color palette for categories (25 colors)
const categoryColors = [
    '#0d6efd', '#6610f2', '#6f42c1', '#d63384', '#dc3545',
    '#fd7e14', '#ffc107', '#198754', '#20c997', '#0dcaf0',
    '#6c757d', '#343a40', '#4e73df', '#1cc88a', '#36b9cc',
    '#f6c23e', '#e74a3b', '#858796', '#5a5c69', '#2e59d9',
    '#17a673', '#2c9faf', '#f8b739', '#eb6060', '#6b7280'
];

/**
 * Initialize the categories overview chart
 */
function initCategoriesOverviewChart() {
    const ctx = document.getElementById('categoriesOverviewChart');
    if (!ctx) return;

    categoriesOverviewChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: [],
            datasets: [{
                data: [],
                backgroundColor: categoryColors,
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '55%',
            plugins: {
                legend: {
                    display: false  // Use table instead
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((context.raw / total) * 100).toFixed(1);
                            return `${context.label}: ${context.raw.toLocaleString()} (${percentage}%)`;
                        }
                    }
                }
            },
            onClick: (event, elements) => {
                if (elements.length > 0) {
                    const index = elements[0].index;
                    const category = categoriesOverviewLabels[index];
                    if (category) {
                        showCategoryCalls(category);  // Reuse existing drill-down
                    }
                }
            },
            onHover: (event, elements) => {
                event.native.target.style.cursor = elements.length > 0 ? 'pointer' : 'default';
            }
        }
    });
}

/**
 * Load categories overview data from API
 */
async function loadCategoriesOverview() {
    const days = document.getElementById('categoriesOverviewDays')?.value || 7;
    const callType = getCallType();

    try {
        const response = await fetch(`${API_BASE}/api/categories/overview?days=${days}&call_type=${callType}`);
        const data = await response.json();

        // Update KPIs
        document.getElementById('catTotalConversations').textContent =
            (data.stats.total_conversations || 0).toLocaleString();
        document.getElementById('catTotalCategories').textContent =
            data.stats.unique_categories || 0;
        document.getElementById('catTopCategory').textContent =
            truncateCategoryText(data.stats.top_category || '-', 15);
        document.getElementById('catAvgPerConversation').textContent =
            data.stats.avg_per_conversation || '-';

        // Update chart
        const categories = data.categories || [];
        categoriesOverviewLabels = categories.map(c => c.category || 'Unknown');

        if (categoriesOverviewChart) {
            categoriesOverviewChart.data.labels = categoriesOverviewLabels;
            categoriesOverviewChart.data.datasets[0].data = categories.map(c => c.count);
            categoriesOverviewChart.data.datasets[0].backgroundColor =
                categories.map((_, i) => categoryColors[i % categoryColors.length]);
            categoriesOverviewChart.update();
        }

        // Update badge
        document.getElementById('catChartTotal').textContent = categories.length;

        // Update ranking table
        updateCategoriesRankingTable(categories);

        categoriesOverviewLoaded = true;

    } catch (error) {
        console.error('Error loading categories overview:', error);
    }
}

/**
 * Update the categories ranking table
 * @param {Array} categories - Array of category objects with category and count
 */
function updateCategoriesRankingTable(categories) {
    const total = categories.reduce((sum, c) => sum + (c.count || 0), 0);
    const tbody = document.getElementById('categoriesRankingTable');
    if (!tbody) return;

    tbody.innerHTML = categories.map((cat, index) => {
        const percentage = total > 0 ? ((cat.count / total) * 100).toFixed(1) : 0;
        const colorDot = categoryColors[index % categoryColors.length];
        const escapedCategory = escapeHtml(cat.category || 'Unknown');
        return `
            <tr class="category-row" onclick="showCategoryCalls('${escapedCategory}')" style="cursor: pointer;">
                <td>
                    <span style="display: inline-block; width: 12px; height: 12px; border-radius: 50%; background: ${colorDot}; margin-right: 5px;"></span>
                    ${index + 1}
                </td>
                <td class="text-truncate" style="max-width: 150px;" title="${escapedCategory}">${escapedCategory}</td>
                <td class="text-end">${(cat.count || 0).toLocaleString()}</td>
                <td class="text-end">${percentage}%</td>
            </tr>
        `;
    }).join('');
}

/**
 * Truncate text to a maximum length
 * @param {string} text - Text to truncate
 * @param {number} maxLength - Maximum length
 * @returns {string} Truncated text
 */
function truncateCategoryText(text, maxLength) {
    if (!text || text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
}

/**
 * Initialize on page load
 */
document.addEventListener('DOMContentLoaded', function() {
    initCategoriesOverviewChart();

    // Register section callback
    if (window.Sidebar && window.Sidebar.onSectionLoad) {
        window.Sidebar.onSectionLoad('categoriesoverview', function() {
            if (!categoriesOverviewLoaded) {
                loadCategoriesOverview();
            }
        });
    }

    // Listen for section changes
    document.addEventListener('sectionChanged', function(event) {
        if (event.detail.section === 'categoriesoverview' && !categoriesOverviewLoaded) {
            loadCategoriesOverview();
        }
    });
});
