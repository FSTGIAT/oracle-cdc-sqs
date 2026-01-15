/**
 * Dashboard Feedback Module
 * Classification feedback for ML improvement
 */

// Quick feedback - thumbs up (correct) or thumbs down (wrong)
function quickFeedback(isCorrect) {
    if (!currentCallIdForFeedback) {
        alert('No call selected');
        return;
    }

    if (isCorrect) {
        submitQuickFeedbackToAPI(true, null);
    } else {
        document.getElementById('correctCategoryInput').style.display = 'inline';
        document.getElementById('btnGood').disabled = true;
        document.getElementById('btnBad').disabled = true;
        document.getElementById('quickCorrectCategory').focus();
    }
}

// Cancel quick feedback
function cancelQuickFeedback() {
    document.getElementById('correctCategoryInput').style.display = 'none';
    document.getElementById('quickCorrectCategory').value = '';
    document.getElementById('btnGood').disabled = false;
    document.getElementById('btnBad').disabled = false;
}

// Submit quick feedback with correct category
function submitQuickFeedback() {
    const correctCategory = document.getElementById('quickCorrectCategory').value.trim();
    if (!correctCategory) {
        alert('Please enter the correct category');
        return;
    }
    submitQuickFeedbackToAPI(false, correctCategory);
}

// Submit feedback to API
async function submitQuickFeedbackToAPI(isCorrect, correctCategory) {
    const statusEl = document.getElementById('feedbackStatus');
    statusEl.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';

    try {
        const response = await fetch(`${API_BASE}/api/ml-quality/feedback`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                call_id: currentCallIdForFeedback,
                ml_category: currentCategoriesForFeedback.join(', '),
                correct_category: correctCategory,
                is_correct: isCorrect,
                reviewer: 'dashboard_user'
            })
        });

        const result = await response.json();

        if (result.success) {
            if (isCorrect) {
                statusEl.innerHTML = '<span class="badge bg-success">✓ Marked as correct</span>';
            } else {
                statusEl.innerHTML = `<span class="badge bg-info">✓ Corrected to: ${escapeHtml(correctCategory)}</span>`;
            }
            document.getElementById('btnGood').style.display = 'none';
            document.getElementById('btnBad').style.display = 'none';
            document.getElementById('correctCategoryInput').style.display = 'none';
        } else {
            statusEl.innerHTML = '<span class="badge bg-danger">Error saving</span>';
        }
    } catch (error) {
        console.error('Quick feedback error:', error);
        statusEl.innerHTML = '<span class="badge bg-danger">Error</span>';
    }
}

// Reset feedback UI when opening new call
function resetFeedbackUI() {
    const statusEl = document.getElementById('feedbackStatus');
    if (statusEl) statusEl.innerHTML = '';

    const btnGood = document.getElementById('btnGood');
    const btnBad = document.getElementById('btnBad');
    const categoryInput = document.getElementById('correctCategoryInput');
    const quickCategory = document.getElementById('quickCorrectCategory');

    if (btnGood) {
        btnGood.style.display = 'inline-block';
        btnGood.disabled = false;
    }
    if (btnBad) {
        btnBad.style.display = 'inline-block';
        btnBad.disabled = false;
    }
    if (categoryInput) categoryInput.style.display = 'none';
    if (quickCategory) quickCategory.value = '';
}
