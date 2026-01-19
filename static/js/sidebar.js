/**
 * Sidebar Navigation Module
 * Handles sidebar toggle, section switching, and badge updates
 */

// Current active section
let currentSection = 'analytics';

// Section load callbacks
const sectionCallbacks = {
    'analytics': null,
    'mlquality': null,
    'churnanalytics': null,
    'alerts': null
};

/**
 * Initialize sidebar functionality
 */
function initSidebar() {
    // Set up click handlers for sidebar items
    document.querySelectorAll('.sidebar-item[data-section]').forEach(item => {
        item.addEventListener('click', () => {
            const section = item.dataset.section;
            switchSection(section);
        });
    });

    // Mobile menu toggle
    const mobileToggle = document.querySelector('.mobile-menu-toggle');
    if (mobileToggle) {
        mobileToggle.addEventListener('click', toggleMobileSidebar);
    }

    // Overlay click closes mobile sidebar
    const overlay = document.querySelector('.sidebar-overlay');
    if (overlay) {
        overlay.addEventListener('click', closeMobileSidebar);
    }

    // Handle browser back/forward
    window.addEventListener('popstate', handlePopState);

    // Set initial section from URL hash or default to analytics
    const hash = window.location.hash.slice(1);
    if (hash && document.querySelector(`[data-section="${hash}"]`)) {
        switchSection(hash, false);
    } else {
        switchSection('analytics', false);
    }

    // Start periodic badge updates
    updateBadges();
    setInterval(updateBadges, 60000); // Update every minute
}

/**
 * Switch to a different section
 * @param {string} section - Section ID to switch to
 * @param {boolean} updateHistory - Whether to update browser history
 */
function switchSection(section, updateHistory = true) {
    if (section === currentSection && document.querySelector(`.content-section[data-section="${section}"].active`)) {
        return;
    }

    // Update sidebar active state
    document.querySelectorAll('.sidebar-item').forEach(item => {
        item.classList.toggle('active', item.dataset.section === section);
    });

    // Update content sections
    document.querySelectorAll('.content-section').forEach(sec => {
        sec.classList.toggle('active', sec.dataset.section === section);
    });

    currentSection = section;

    // Update browser URL
    if (updateHistory) {
        history.pushState({ section }, '', `#${section}`);
    }

    // Close mobile sidebar
    closeMobileSidebar();

    // Fire section callback if defined
    if (sectionCallbacks[section]) {
        sectionCallbacks[section]();
    }

    // Dispatch custom event for other modules to listen to
    document.dispatchEvent(new CustomEvent('sectionChanged', {
        detail: { section }
    }));
}

/**
 * Register a callback for when a section is activated
 * @param {string} section - Section ID
 * @param {Function} callback - Callback function
 */
function onSectionLoad(section, callback) {
    sectionCallbacks[section] = callback;
}

/**
 * Handle browser back/forward navigation
 */
function handlePopState(event) {
    const section = event.state?.section || 'analytics';
    switchSection(section, false);
}

/**
 * Toggle mobile sidebar visibility
 */
function toggleMobileSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.querySelector('.sidebar-overlay');

    sidebar.classList.toggle('mobile-open');
    overlay.classList.toggle('show');
}

/**
 * Close mobile sidebar
 */
function closeMobileSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const overlay = document.querySelector('.sidebar-overlay');

    sidebar.classList.remove('mobile-open');
    overlay.classList.remove('show');
}

/**
 * Update badge counts in sidebar
 */
async function updateBadges() {
    try {
        // Fetch alert summary
        const alertsResponse = await fetch('/api/alerts/summary');
        if (alertsResponse.ok) {
            const alertData = await alertsResponse.json();
            updateAlertsBadge(alertData.active_count, alertData.critical_count > 0);
        }

        // Fetch ML quality metrics for pending badge
        const mlResponse = await fetch('/api/ml-quality/metrics');
        if (mlResponse.ok) {
            const mlData = await mlResponse.json();
            updateMLBadge(mlData.pending_recommendations || 0);
        }
    } catch (error) {
        console.error('Error updating badges:', error);
    }
}

/**
 * Update alerts badge in sidebar and main page banner
 * @param {number} count - Number of active alerts
 * @param {boolean} hasCritical - Whether there are critical alerts
 */
function updateAlertsBadge(count, hasCritical) {
    // Update sidebar badge
    const badge = document.querySelector('.sidebar-item[data-section="alerts"] .sidebar-badge');
    if (badge) {
        if (count > 0) {
            badge.textContent = count > 99 ? '99+' : count;
            badge.style.display = 'flex';
            badge.classList.toggle('has-critical', hasCritical);
        } else {
            badge.style.display = 'none';
        }
    }

    // Update main page alert banner
    const banner = document.getElementById('alertsBanner');
    const bannerCount = document.getElementById('alertsBannerCount');
    if (banner && bannerCount) {
        if (count > 0) {
            bannerCount.textContent = count;
            banner.style.display = 'flex';
            banner.style.setProperty('display', 'flex', 'important');
        } else {
            banner.style.display = 'none';
            banner.style.setProperty('display', 'none', 'important');
        }
    }
}

/**
 * Update ML Quality badge in sidebar
 * @param {number} count - Number of pending recommendations
 */
function updateMLBadge(count) {
    const badge = document.querySelector('.sidebar-item[data-section="mlquality"] .sidebar-badge');
    if (!badge) return;

    if (count > 0) {
        badge.textContent = count;
        badge.style.display = 'flex';
    } else {
        badge.style.display = 'none';
    }
}

/**
 * Get current active section
 * @returns {string} Current section ID
 */
function getCurrentSection() {
    return currentSection;
}

/**
 * Programmatically navigate to a section
 * @param {string} section - Section ID
 */
function navigateToSection(section) {
    switchSection(section, true);
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', initSidebar);

// Export for other modules
window.Sidebar = {
    switchSection,
    getCurrentSection,
    navigateToSection,
    onSectionLoad,
    updateBadges,
    updateAlertsBadge,
    updateMLBadge
};
