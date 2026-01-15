/**
 * Dashboard Modals Module
 * Handles call details, category drill-down, and customer journey modals
 */

// Bootstrap modal instances
let categoryModal, callModal, journeyModal;

// Current call state for feedback
let currentCallIdForFeedback = null;
let currentCategoriesForFeedback = [];

// Initialize modals
function initModals() {
    categoryModal = new bootstrap.Modal(document.getElementById('categoryModal'));
    callModal = new bootstrap.Modal(document.getElementById('callModal'));

    // Journey modal (new feature)
    const journeyEl = document.getElementById('journeyModal');
    if (journeyEl) {
        journeyModal = new bootstrap.Modal(journeyEl);
    }
}

// Show calls for a category
async function showCategoryCalls(category) {
    const days = getTimeFilterDays();
    document.getElementById('categoryModalTitle').textContent = `Category: ${category}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const calls = await fetch(`${API_BASE}/api/category/calls?category=${encodeURIComponent(category)}&days=${days}`).then(r => r.json());
        document.getElementById('categoryCallCount').textContent = calls.length;

        document.getElementById('categoryCallsBody').innerHTML = calls.map(c => {
            const callId = escapeHtml(String(c.call_id || ''));
            const summary = c.summary ? String(c.summary) : '';
            return `
            <tr class="call-row" onclick="showCallDetails('${callId}')">
                <td><code class="text-primary">${callId || '-'}</code></td>
                <td><span class="badge ${c.type === 'CALL' ? 'bg-primary' : 'bg-success'}">${c.type || '-'}</span></td>
                <td>${c.created || '-'}</td>
                <td>${getSentimentBadge(c.sentiment)}</td>
                <td>${c.satisfaction || '-'}</td>
                <td>${getChurnBadge(c.churn_score)}</td>
                <td class="summary-preview" title="${escapeHtml(summary)}">${summary.substring(0, 60)}...</td>
            </tr>
            `;
        }).join('');

        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    } catch (error) {
        console.error('Error loading category calls:', error);
        document.getElementById('categoryCallsBody').innerHTML = `
            <tr><td colspan="7" class="text-center text-danger">Error loading calls</td></tr>
        `;
        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    }
}

// Show calls for a sentiment type
async function showSentimentCalls(sentimentType) {
    const days = getTimeFilterDays();

    let badgeClass = 'bg-secondary';
    if (sentimentType === 'Positive') badgeClass = 'bg-success';
    else if (sentimentType === 'Negative') badgeClass = 'bg-danger';

    document.getElementById('categoryModalTitle').innerHTML = `<span class="badge ${badgeClass} me-2">Sentiment</span> ${sentimentType}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const calls = await fetch(`${API_BASE}/api/sentiment/calls?sentiment=${encodeURIComponent(sentimentType)}&days=${days}`).then(r => r.json());
        document.getElementById('categoryCallCount').textContent = calls.length;

        document.getElementById('categoryCallsBody').innerHTML = calls.map(c => {
            const callId = escapeHtml(String(c.call_id || ''));
            const summary = c.summary ? String(c.summary) : '';
            return `
            <tr class="call-row" onclick="showCallDetails('${callId}')">
                <td><code class="text-primary">${callId || '-'}</code></td>
                <td><span class="badge ${c.type === 'CALL' ? 'bg-primary' : 'bg-success'}">${c.type || '-'}</span></td>
                <td>${c.created || '-'}</td>
                <td>${getSentimentBadge(c.sentiment)}</td>
                <td>${c.satisfaction || '-'}</td>
                <td>${getChurnBadge(c.churn_score)}</td>
                <td class="summary-preview" title="${escapeHtml(summary)}">${summary.substring(0, 60)}...</td>
            </tr>
            `;
        }).join('');

        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    } catch (error) {
        console.error('Error loading sentiment calls:', error);
        document.getElementById('categoryCallsBody').innerHTML = `
            <tr><td colspan="7" class="text-center text-danger">Error loading calls</td></tr>
        `;
        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    }
}

// Show calls for a churn risk level
async function showChurnRiskCalls(riskLevel) {
    const days = getTimeFilterDays();

    let badgeClass = 'bg-success';
    if (riskLevel.includes('High')) badgeClass = 'bg-danger';
    else if (riskLevel.includes('Medium')) badgeClass = 'bg-warning text-dark';

    document.getElementById('categoryModalTitle').innerHTML = `<span class="badge ${badgeClass} me-2">Churn Risk</span> ${riskLevel}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const calls = await fetch(`${API_BASE}/api/churn/calls?risk_level=${encodeURIComponent(riskLevel)}&days=${days}`).then(r => r.json());
        document.getElementById('categoryCallCount').textContent = calls.length;

        document.getElementById('categoryCallsBody').innerHTML = calls.map(c => {
            const callId = escapeHtml(String(c.call_id || ''));
            const summary = c.summary ? String(c.summary) : '';
            return `
            <tr class="call-row" onclick="showCallDetails('${callId}')">
                <td><code class="text-primary">${callId || '-'}</code></td>
                <td><span class="badge ${c.type === 'CALL' ? 'bg-primary' : 'bg-success'}">${c.type || '-'}</span></td>
                <td>${c.created || '-'}</td>
                <td>${getSentimentBadge(c.sentiment)}</td>
                <td>${c.satisfaction || '-'}</td>
                <td>${getChurnBadge(c.churn_score)}</td>
                <td class="summary-preview" title="${escapeHtml(summary)}">${summary.substring(0, 60)}...</td>
            </tr>
            `;
        }).join('');

        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    } catch (error) {
        console.error('Error loading churn risk calls:', error);
        document.getElementById('categoryCallsBody').innerHTML = `
            <tr><td colspan="7" class="text-center text-danger">Error loading calls</td></tr>
        `;
        document.getElementById('categoryCallsLoading').style.display = 'none';
        document.getElementById('categoryCallsTable').style.display = 'table';
    }
}

// Show call details with conversation
async function showCallDetails(callId) {
    document.getElementById('callModalId').textContent = callId;
    document.getElementById('callDetailsLoading').style.display = 'block';
    document.getElementById('callDetailsContent').style.display = 'none';

    // Set current call for feedback
    currentCallIdForFeedback = callId;
    resetFeedbackUI();

    callModal.show();

    try {
        // Fetch call details and conversation in parallel
        const [details, conversation] = await Promise.all([
            fetch(`${API_BASE}/api/call/${callId}`).then(r => r.json()),
            fetch(`${API_BASE}/api/call/${callId}/conversation`).then(r => r.json())
        ]);

        // Populate details
        document.getElementById('callSummary').textContent = details.summary || '-';
        document.getElementById('callSentiment').innerHTML = getSentimentBadge(details.sentiment);
        document.getElementById('callSatisfaction').textContent = details.satisfaction || '-';
        document.getElementById('callChurn').innerHTML = getChurnBadge(details.churn_score);
        document.getElementById('callProducts').textContent = details.products || '-';
        document.getElementById('callActionItems').textContent = details.action_items || '-';
        document.getElementById('callUnresolved').textContent = details.unresolved_issues || '-';

        // Store categories for feedback
        currentCategoriesForFeedback = details.categories || [];
        document.getElementById('callCategories').innerHTML = currentCategoriesForFeedback
            .map(c => `<span class="badge bg-info me-1">${c}</span>`).join('') || '-';

        // Display queue name
        document.getElementById('callQueueName').textContent = details.queue_name || '-';

        // Display subscriber status
        const statusBadge = document.getElementById('callCustomerStatus');
        const statusText = document.getElementById('callCustomerStatusText');

        if (details.sub_status === 'A') {
            statusBadge.className = 'badge bg-success ms-2';
            statusBadge.textContent = 'Active';
            statusText.innerHTML = '<span class="text-success">✓ Active Customer</span>';
        } else if (details.sub_status === 'C') {
            statusBadge.className = 'badge bg-danger ms-2';
            statusBadge.textContent = 'Churned';
            statusText.innerHTML = '<span class="text-danger">✗ Churned Customer</span>';
        } else {
            statusBadge.className = 'badge bg-secondary ms-2';
            statusBadge.textContent = '-';
            statusText.textContent = '-';
        }

        // Show customer journey button if we have subscriber info
        const journeyBtn = document.getElementById('showJourneyBtn');
        if (journeyBtn) {
            if (details.subscriber_no || details.ban) {
                journeyBtn.style.display = 'inline-block';
                journeyBtn.onclick = () => showCustomerJourney(details.subscriber_no, details.ban);
            } else {
                journeyBtn.style.display = 'none';
            }
        }

        // Populate conversation
        document.getElementById('messageCount').textContent = `${conversation.message_count || 0} messages`;
        document.getElementById('conversationContainer').innerHTML = renderConversation(conversation.messages || []);

        document.getElementById('callDetailsLoading').style.display = 'none';
        document.getElementById('callDetailsContent').style.display = 'block';

    } catch (error) {
        console.error('Error loading call details:', error);
        document.getElementById('callDetailsLoading').innerHTML = `
            <div class="text-danger">Error loading call details: ${error.message}</div>
        `;
    }
}

// Render conversation messages
function renderConversation(messages) {
    if (!messages || messages.length === 0) {
        return '<div class="text-center text-muted py-4">No conversation data available</div>';
    }

    return messages.map(msg => {
        const isAgent = msg.speaker_class === 'agent';
        const isCustomer = msg.speaker_class === 'customer';
        const messageClass = isAgent ? 'message-agent' : (isCustomer ? 'message-customer' : 'message-other');
        const speakerClass = isAgent ? 'speaker-agent' : 'speaker-customer';

        return `
            <div class="message ${messageClass}">
                <div class="message-header">
                    <span class="speaker-label ${speakerClass}">${msg.speaker_label || msg.speaker || 'Unknown'}</span>
                    <span class="ms-2">${msg.timestamp || ''}</span>
                </div>
                <div class="message-text">${escapeHtml(msg.text || '')}</div>
            </div>
        `;
    }).join('');
}

// Show customer journey timeline
async function showCustomerJourney(subscriberNo, ban) {
    if (!journeyModal) return;

    const container = document.getElementById('journeyTimeline');
    if (!container) return;

    container.innerHTML = `
        <div class="text-center py-4">
            <div class="spinner-border text-primary"></div>
            <p class="mt-2">Loading customer journey...</p>
        </div>
    `;

    journeyModal.show();

    try {
        const params = new URLSearchParams();
        if (subscriberNo) params.append('subscriber_no', subscriberNo);
        if (ban) params.append('ban', ban);

        const journey = await fetch(`${API_BASE}/api/customer-journey?${params}`).then(r => r.json());

        if (!journey.calls || journey.calls.length === 0) {
            container.innerHTML = '<div class="text-center text-muted py-4">No previous calls found for this customer</div>';
            return;
        }

        // Update header
        document.getElementById('journeySubscriberNo').textContent = subscriberNo || '-';
        document.getElementById('journeyTotalCalls').textContent = journey.total_calls || journey.calls.length;

        // Render timeline
        container.innerHTML = journey.calls.map((call, index) => {
            let churnClass = 'churn-low';
            if (call.churn_score >= 70) churnClass = 'churn-high';
            else if (call.churn_score >= 40) churnClass = 'churn-medium';

            return `
                <div class="timeline-item ${churnClass}">
                    <div class="timeline-date">${call.call_date || '-'}</div>
                    <div class="timeline-content">
                        <div class="d-flex justify-content-between align-items-start mb-2">
                            <span class="badge ${call.source_type === 'CALL' ? 'bg-primary' : 'bg-success'}">${call.source_type || '-'}</span>
                            <div>
                                ${getSentimentBadge(call.sentiment)}
                                ${getChurnBadge(call.churn_score)}
                            </div>
                        </div>
                        <div class="timeline-summary">${escapeHtml(call.summary || 'No summary available')}</div>
                        <button class="btn btn-sm btn-outline-primary mt-2" onclick="showCallDetails('${call.source_id}')">
                            View Details
                        </button>
                    </div>
                </div>
            `;
        }).join('');

    } catch (error) {
        console.error('Error loading customer journey:', error);
        container.innerHTML = `<div class="text-danger text-center py-4">Error loading journey: ${error.message}</div>`;
    }
}
