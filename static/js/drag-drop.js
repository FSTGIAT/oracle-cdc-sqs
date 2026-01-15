/**
 * Drag-and-Drop Layout Module
 * Uses GridStack.js for dashboard customization
 */

let gridStackInstance = null;

// Initialize GridStack
function initGridStack() {
    const gridEl = document.querySelector('.grid-stack');
    if (!gridEl) return;

    gridStackInstance = GridStack.init({
        column: 12,
        cellHeight: 100,
        animate: true,
        float: true,
        minRow: 1,
        margin: 10,
        handle: '.grid-drag-handle',
        resizable: {
            handles: 'e, se, s, sw, w'
        }
    });

    // Load saved layout
    loadSavedLayout();

    // Save layout on change
    gridStackInstance.on('change', saveLayout);
    gridStackInstance.on('resizestop', saveLayout);
    gridStackInstance.on('dragstop', saveLayout);

    console.log('GridStack initialized');
}

// Save layout to localStorage
function saveLayout() {
    if (!gridStackInstance) return;

    const layout = gridStackInstance.save(false);
    localStorage.setItem('dashboard_layout', JSON.stringify(layout));
    console.log('Layout saved');
}

// Load layout from localStorage
function loadSavedLayout() {
    if (!gridStackInstance) return;

    const savedLayout = localStorage.getItem('dashboard_layout');
    if (savedLayout) {
        try {
            const layout = JSON.parse(savedLayout);
            gridStackInstance.load(layout, true);
            console.log('Layout loaded from localStorage');
        } catch (error) {
            console.error('Error loading saved layout:', error);
        }
    }
}

// Reset layout to default
function resetLayout() {
    if (!confirm('Reset dashboard layout to default?')) return;

    localStorage.removeItem('dashboard_layout');
    location.reload();
}

// Lock/Unlock layout for editing
function toggleLayoutLock() {
    if (!gridStackInstance) return;

    const isLocked = gridStackInstance.opts.staticGrid;
    gridStackInstance.setStatic(!isLocked);

    const btn = document.getElementById('lockLayoutBtn');
    if (btn) {
        btn.innerHTML = isLocked ?
            '<i class="bi bi-unlock"></i> Lock Layout' :
            '<i class="bi bi-lock"></i> Unlock Layout';
        btn.classList.toggle('btn-warning', !isLocked);
        btn.classList.toggle('btn-secondary', isLocked);
    }

    console.log('Layout ' + (isLocked ? 'unlocked' : 'locked'));
}

// Add widget to grid
function addWidget(options) {
    if (!gridStackInstance) return null;

    const widget = gridStackInstance.addWidget({
        x: options.x || 0,
        y: options.y || 0,
        w: options.w || 4,
        h: options.h || 3,
        content: options.content || '',
        id: options.id || 'widget-' + Date.now()
    });

    saveLayout();
    return widget;
}

// Remove widget from grid
function removeWidget(widgetId) {
    if (!gridStackInstance) return;

    const widget = document.querySelector(`[gs-id="${widgetId}"]`);
    if (widget) {
        gridStackInstance.removeWidget(widget);
        saveLayout();
    }
}

// Get default layout configuration
function getDefaultLayout() {
    return [
        // Row 1: KPI Cards with Trends
        { id: 'kpi-total', x: 0, y: 0, w: 3, h: 2 },
        { id: 'kpi-satisfaction', x: 3, y: 0, w: 3, h: 2 },
        { id: 'kpi-churn', x: 6, y: 0, w: 3, h: 2 },
        { id: 'kpi-positive', x: 9, y: 0, w: 3, h: 2 },

        // Row 2: Charts
        { id: 'chart-sentiment', x: 0, y: 2, w: 4, h: 3 },
        { id: 'chart-churn', x: 4, y: 2, w: 4, h: 3 },
        { id: 'chart-satisfaction', x: 8, y: 2, w: 4, h: 3 },

        // Row 3: Heatmap and Categories
        { id: 'heatmap', x: 0, y: 5, w: 6, h: 4 },
        { id: 'chart-categories', x: 6, y: 5, w: 6, h: 4 },

        // Row 4: Agent Performance and Products
        { id: 'agent-performance', x: 0, y: 9, w: 6, h: 4 },
        { id: 'products-breakdown', x: 6, y: 9, w: 6, h: 4 },

        // Row 5: Tables
        { id: 'recent-conversations', x: 0, y: 13, w: 8, h: 4 },
        { id: 'recent-errors', x: 8, y: 13, w: 4, h: 4 }
    ];
}

// Export layout to JSON
function exportLayout() {
    if (!gridStackInstance) return;

    const layout = gridStackInstance.save(false);
    const dataStr = JSON.stringify(layout, null, 2);
    const dataUri = 'data:application/json;charset=utf-8,' + encodeURIComponent(dataStr);

    const exportLink = document.createElement('a');
    exportLink.setAttribute('href', dataUri);
    exportLink.setAttribute('download', 'dashboard-layout.json');
    document.body.appendChild(exportLink);
    exportLink.click();
    document.body.removeChild(exportLink);
}

// Import layout from JSON
function importLayout(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = function(e) {
        try {
            const layout = JSON.parse(e.target.result);
            if (gridStackInstance) {
                gridStackInstance.load(layout, true);
                saveLayout();
                console.log('Layout imported successfully');
            }
        } catch (error) {
            alert('Error importing layout: ' + error.message);
        }
    };
    reader.readAsText(file);
}
