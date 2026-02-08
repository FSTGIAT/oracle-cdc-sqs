/**
 * Dashboard Modals Module
 * Handles call details, category drill-down, and customer journey modals
 */

// Bootstrap modal instances
let categoryModal, callModal, journeyModal;

// Current call state for feedback
let currentCallIdForFeedback = null;
let currentCategoriesForFeedback = [];

// Journey context for back navigation
let currentJourneyContext = null;

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
    const callType = getCallType();
    document.getElementById('categoryModalTitle').textContent = `Category: ${category}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const calls = await fetch(`${API_BASE}/api/category/calls?category=${encodeURIComponent(category)}&days=${days}&call_type=${callType}`).then(r => r.json());
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
    const callType = getCallType();

    let badgeClass = 'bg-secondary';
    if (sentimentType === 'Positive') badgeClass = 'bg-success';
    else if (sentimentType === 'Negative') badgeClass = 'bg-danger';

    document.getElementById('categoryModalTitle').innerHTML = `<span class="badge ${badgeClass} me-2">Sentiment</span> ${sentimentType}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const calls = await fetch(`${API_BASE}/api/sentiment/calls?sentiment=${encodeURIComponent(sentimentType)}&days=${days}&call_type=${callType}`).then(r => r.json());
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
    const callType = getCallType();

    let badgeClass = 'bg-success';
    if (riskLevel.includes('High')) badgeClass = 'bg-danger';
    else if (riskLevel.includes('Medium')) badgeClass = 'bg-warning text-dark';

    document.getElementById('categoryModalTitle').innerHTML = `<span class="badge ${badgeClass} me-2">Churn Risk</span> ${riskLevel}`;
    document.getElementById('categoryCallsLoading').style.display = 'block';
    document.getElementById('categoryCallsTable').style.display = 'none';

    categoryModal.show();

    try {
        const calls = await fetch(`${API_BASE}/api/churn/calls?risk_level=${encodeURIComponent(riskLevel)}&days=${days}&call_type=${callType}`).then(r => r.json());
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
async function showCallDetails(callId, fromJourney = false) {
    document.getElementById('callModalId').textContent = callId;
    document.getElementById('callDetailsLoading').style.display = 'block';
    document.getElementById('callDetailsContent').style.display = 'none';

    // Show/hide back to journey button
    const backBtn = document.getElementById('backToJourneyBtn');
    if (backBtn) {
        backBtn.style.display = fromJourney ? 'inline-block' : 'none';
    }

    // Set current call for feedback
    currentCallIdForFeedback = callId;
    resetFeedbackUI();

    callModal.show();

    try {
        // Fetch call details and conversation in parallel
        const encodedCallId = encodeURIComponent(callId);
        const [details, conversation] = await Promise.all([
            fetch(`${API_BASE}/api/call-details?id=${encodedCallId}`).then(r => r.json()),
            fetch(`${API_BASE}/api/call-conversation?id=${encodedCallId}`).then(r => r.json())
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

    // Store context for back navigation
    currentJourneyContext = { subscriberNo, ban };

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

        if (!journey.timeline || journey.timeline.length === 0) {
            container.innerHTML = '<div class="text-center text-muted py-4">No previous calls found for this customer</div>';
            return;
        }

        // Update header
        document.getElementById('journeySubscriberNo').textContent = subscriberNo || journey.customer?.subscriber_no || '-';
        document.getElementById('journeyTotalCalls').textContent = `${journey.customer?.total_interactions || journey.timeline.length} calls`;

        // Update avg churn badge - only show when 2+ calls
        const avgChurnBadge = document.getElementById('journeyAvgChurn');
        const totalCalls = journey.customer?.total_interactions || journey.timeline.length;
        const avgChurn = journey.customer?.avg_churn_score;

        if (avgChurnBadge && totalCalls >= 2 && avgChurn !== null && avgChurn !== undefined) {
            avgChurnBadge.textContent = `Avg Churn: ${avgChurn}`;
            avgChurnBadge.style.display = 'inline';

            // Set color based on risk level
            if (avgChurn >= 70) {
                avgChurnBadge.className = 'badge ms-2 bg-danger';
            } else if (avgChurn >= 40) {
                avgChurnBadge.className = 'badge ms-2 bg-warning text-dark';
            } else {
                avgChurnBadge.className = 'badge ms-2 bg-success';
            }
        } else if (avgChurnBadge) {
            avgChurnBadge.style.display = 'none';
        }

        // Render timeline
        container.innerHTML = journey.timeline.map((call, index) => {
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
                        <button class="btn btn-sm btn-outline-primary mt-2" onclick="showCallFromJourney('${escapeHtml(call.source_id)}')">
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

// Show call details from journey (hides journey modal first)
function showCallFromJourney(callId) {
    journeyModal.hide();
    showCallDetails(callId, true);
}

// Back to journey from call details
function backToJourney() {
    callModal.hide();
    if (currentJourneyContext) {
        showCustomerJourney(currentJourneyContext.subscriberNo, currentJourneyContext.ban);
    }
}

// ========================================
// CUSTOMER LOOKUP
// ========================================

let lookupModal = null;
let currentLookupCustomer = null;

// Initialize lookup modal
document.addEventListener('DOMContentLoaded', function() {
    const lookupModalEl = document.getElementById('lookupModal');
    if (lookupModalEl) {
        lookupModal = new bootstrap.Modal(lookupModalEl);

        // Allow Enter key to trigger search
        document.getElementById('lookupInput').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                searchCustomer();
            }
        });
    }
});

// Show lookup modal
function showCustomerLookup() {
    // Reset state
    document.getElementById('lookupInput').value = '';
    document.getElementById('lookupResults').style.display = 'none';
    document.getElementById('lookupLoading').style.display = 'none';
    document.getElementById('lookupNoResults').style.display = 'none';
    document.getElementById('lookupInitial').style.display = 'block';
    currentLookupCustomer = null;

    lookupModal.show();

    // Focus input
    setTimeout(() => document.getElementById('lookupInput').focus(), 300);
}

// Search customer
async function searchCustomer() {
    const value = document.getElementById('lookupInput').value.trim();
    const type = document.querySelector('input[name="lookupType"]:checked').value;

    if (!value) {
        return;
    }

    // Show loading
    document.getElementById('lookupInitial').style.display = 'none';
    document.getElementById('lookupResults').style.display = 'none';
    document.getElementById('lookupNoResults').style.display = 'none';
    document.getElementById('lookupLoading').style.display = 'block';

    try {
        const response = await fetch(`${API_BASE}/api/customer-lookup?type=${type}&value=${encodeURIComponent(value)}`);
        const data = await response.json();

        document.getElementById('lookupLoading').style.display = 'none';

        if (!data.found) {
            document.getElementById('lookupNoResults').style.display = 'block';
            return;
        }

        // Store customer info for journey button
        currentLookupCustomer = data.customer;

        // Populate customer card
        document.getElementById('lookupSubscriberNo').textContent = data.customer.subscriber_no || '-';
        document.getElementById('lookupBan').textContent = data.customer.ban || '-';
        document.getElementById('lookupProduct').textContent = data.customer.product_code || '-';
        document.getElementById('lookupTotalCalls').textContent = `${data.customer.total_interactions} interactions`;

        // Status badge
        const statusBadge = document.getElementById('lookupStatusBadge');
        if (data.customer.status === 'A') {
            statusBadge.className = 'badge bg-success ms-2';
            statusBadge.textContent = 'Active';
        } else if (data.customer.status === 'C') {
            statusBadge.className = 'badge bg-danger ms-2';
            statusBadge.textContent = 'Churned';
        } else {
            statusBadge.className = 'badge bg-secondary ms-2';
            statusBadge.textContent = 'Unknown';
        }

        // Populate recent calls table
        const tbody = document.getElementById('lookupCallsBody');
        if (data.recent_calls && data.recent_calls.length > 0) {
            tbody.innerHTML = data.recent_calls.map(call => `
                <tr class="call-row" onclick="viewCallFromLookup('${call.source_id}')">
                    <td>${call.call_date || '-'}</td>
                    <td><span class="badge ${call.source_type === 'WAPP' ? 'bg-success' : 'bg-primary'}">${call.source_type || '-'}</span></td>
                    <td>${getSentimentBadge(call.sentiment)}</td>
                    <td>${getChurnBadge(call.churn_score)}</td>
                    <td class="summary-preview">${escapeHtml(call.summary || '-')}</td>
                    <td><i class="bi bi-chevron-right text-muted"></i></td>
                </tr>
            `).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No recent calls found</td></tr>';
        }

        document.getElementById('lookupResults').style.display = 'block';

    } catch (error) {
        console.error('Error searching customer:', error);
        document.getElementById('lookupLoading').style.display = 'none';
        document.getElementById('lookupNoResults').style.display = 'block';
    }
}

// View call details from lookup
function viewCallFromLookup(callId) {
    lookupModal.hide();
    showCallDetails(callId);
}

// View full journey from lookup
function viewFullJourney() {
    if (currentLookupCustomer) {
        lookupModal.hide();
        showCustomerJourney(currentLookupCustomer.subscriber_no, currentLookupCustomer.ban);
    }
}
