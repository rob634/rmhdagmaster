/**
 * DAG Orchestrator Dashboard JavaScript
 */

// Initialize Feather icons when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Initialize Feather icons
    if (typeof feather !== 'undefined') {
        feather.replace();
    }

    // Initialize auto-refresh if enabled
    initAutoRefresh();

    // Initialize any interactive elements
    initInteractive();
});

/**
 * Auto-refresh functionality for dashboard pages
 */
function initAutoRefresh() {
    const refreshInterval = 30000; // 30 seconds
    const autoRefreshEnabled = document.body.dataset.autoRefresh === 'true';

    if (autoRefreshEnabled) {
        setInterval(function() {
            // Only refresh if page is visible
            if (!document.hidden) {
                refreshDashboardData();
            }
        }, refreshInterval);
    }
}

/**
 * Refresh dashboard data via AJAX
 */
async function refreshDashboardData() {
    try {
        const response = await fetch('/api/v1/health');
        if (response.ok) {
            const data = await response.json();
            updateHealthIndicators(data);
        }
    } catch (error) {
        console.error('Failed to refresh dashboard data:', error);
    }
}

/**
 * Update health indicators on the page
 */
function updateHealthIndicators(data) {
    // Update orchestrator status
    const statusDot = document.querySelector('.orchestrator-status .status-dot');
    if (statusDot) {
        statusDot.className = 'status-dot ' + (data.orchestrator?.running ? 'running' : 'stopped');
    }
}

/**
 * Initialize interactive elements
 */
function initInteractive() {
    // Add click handlers for job rows
    document.querySelectorAll('tr[data-job-id]').forEach(function(row) {
        row.style.cursor = 'pointer';
        row.addEventListener('click', function() {
            window.location.href = '/ui/jobs/' + this.dataset.jobId;
        });
    });

    // Add confirmation for dangerous actions
    document.querySelectorAll('[data-confirm]').forEach(function(element) {
        element.addEventListener('click', function(e) {
            if (!confirm(this.dataset.confirm)) {
                e.preventDefault();
            }
        });
    });
}

/**
 * Format duration in human-readable format
 */
function formatDuration(ms) {
    if (ms < 1000) return ms + 'ms';
    if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
    if (ms < 3600000) return Math.floor(ms / 60000) + 'm ' + Math.floor((ms % 60000) / 1000) + 's';
    return Math.floor(ms / 3600000) + 'h ' + Math.floor((ms % 3600000) / 60000) + 'm';
}

/**
 * Format timestamp in local timezone
 */
function formatTimestamp(isoString) {
    if (!isoString) return '-';
    const date = new Date(isoString);
    return date.toLocaleString();
}

/**
 * Format relative time (e.g., "5 minutes ago")
 */
function formatRelativeTime(isoString) {
    if (!isoString) return '-';
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now - date;

    if (diffMs < 60000) return 'just now';
    if (diffMs < 3600000) return Math.floor(diffMs / 60000) + ' min ago';
    if (diffMs < 86400000) return Math.floor(diffMs / 3600000) + ' hours ago';
    return Math.floor(diffMs / 86400000) + ' days ago';
}

/**
 * Copy text to clipboard
 */
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(function() {
        showToast('Copied to clipboard');
    }).catch(function(err) {
        console.error('Failed to copy:', err);
    });
}

/**
 * Show a toast notification
 */
function showToast(message, type = 'info') {
    // Simple toast implementation
    const toast = document.createElement('div');
    toast.className = 'toast toast-' + type;
    toast.textContent = message;
    toast.style.cssText = 'position:fixed;bottom:20px;right:20px;padding:12px 24px;background:#334155;color:#f1f5f9;border-radius:6px;z-index:9999;';
    document.body.appendChild(toast);

    setTimeout(function() {
        toast.remove();
    }, 3000);
}

/**
 * Cancel a job
 */
async function cancelJob(jobId) {
    if (!confirm('Are you sure you want to cancel this job?')) {
        return;
    }

    try {
        const response = await fetch('/api/v1/jobs/' + jobId + '/cancel', {
            method: 'POST'
        });

        if (response.ok) {
            showToast('Job cancelled successfully', 'success');
            setTimeout(function() {
                location.reload();
            }, 1000);
        } else {
            const error = await response.json();
            showToast('Failed to cancel job: ' + error.detail, 'error');
        }
    } catch (error) {
        showToast('Failed to cancel job', 'error');
        console.error('Cancel job error:', error);
    }
}
