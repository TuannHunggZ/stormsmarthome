const alertList = document.getElementById('alertList');
const connectionStatus = document.getElementById('connectionStatus');
const connectionHint = document.getElementById('connectionHint');
const alertCount = document.getElementById('alertCount');

const maxVisibleAlerts = 20;
let socket;
let reconnectTimer;
let isConnected = false;

const anomalyPresentation = {
    MAX: {
        severity: 'HIGH',
        severityClass: 'high',
        title: 'High energy consumption detected',
        summary: 'Consumption exceeded its historical maximum.'
    },
    AVG: {
        severity: 'WARNING',
        severityClass: 'warning',
        title: 'Unusual energy consumption detected',
        summary: 'Consumption is significantly above the historical average.'
    },
    MIN: {
        severity: 'LOW',
        severityClass: 'low',
        title: 'Low energy consumption detected',
        summary: 'Consumption is significantly below the historical baseline.'
    }
};

function formatDate(value) {
    try {
        const date = new Date(value);
        if (!Number.isNaN(date.getTime())) {
            return date.toLocaleString();
        }

        const asNumber = Number(value);
        if (!Number.isNaN(asNumber)) {
            return new Date(asNumber * 1000).toLocaleString();
        }

        return 'Unknown';
    } catch (error) {
        return String(value || 'Unknown');
    }
}

function formatValue(value) {
    if (value === null || value === undefined) {
        return 'N/A';
    }

    if (typeof value === 'number') {
        return Number.isInteger(value) ? String(value) : value.toFixed(2);
    }

    return String(value);
}

function setConnected(isConnected) {
    connectionStatus.textContent = isConnected ? 'Live' : 'Disconnected';
    connectionHint.textContent = isConnected
        ? 'Monitoring anomaly events in real time.'
        : 'Attempting to reconnect...';
    connectionStatus.classList.toggle('connected', isConnected);
    connectionStatus.classList.toggle('disconnected', !isConnected);
}

function updateAlertCount() {
    const visibleAlerts = alertList.querySelectorAll('.alert').length;
    alertCount.textContent = `${visibleAlerts} alerts`;
}

function ensureEmptyState() {
    const hasAlerts = alertList.querySelectorAll('.alert').length > 0;
    const existingState = document.getElementById('emptyState');

    if (hasAlerts) {
        if (existingState) {
            existingState.remove();
        }
        return;
    }

    const title = isConnected ? 'No anomalies detected' : 'Connection lost';
    const message = isConnected
        ? 'The system is monitoring device activity in real time.'
        : 'Attempting to reconnect...';

    if (existingState) {
        existingState.querySelector('h3').textContent = title;
        existingState.querySelector('p').textContent = message;
        return;
    }

    const state = document.createElement('section');
    state.id = 'emptyState';
    state.className = 'empty-state';

    const heading = document.createElement('h3');
    heading.textContent = title;

    const description = document.createElement('p');
    description.textContent = message;

    state.appendChild(heading);
    state.appendChild(description);
    alertList.appendChild(state);
}

function getAlertPresentation(alert) {
    const key = String(alert.anomalyType || '').toUpperCase();
    return anomalyPresentation[key] || {
        severity: 'WARNING',
        severityClass: 'warning',
        title: 'Anomalous device activity detected',
        summary: 'Consumption behavior differs from historical baseline.'
    };
}

function getScopeLine(alert) {
    const parts = [];

    if (alert.plugId !== null && alert.plugId !== undefined) {
        parts.push(`Device ${formatValue(alert.plugId)}`);
    }

    if (alert.householdId !== null && alert.householdId !== undefined) {
        parts.push(`Household ${formatValue(alert.householdId)}`);
    }

    parts.push(`House ${formatValue(alert.houseId)}`);
    return parts.join(' · ');
}

function getScopeSentence(alert) {
    if (alert.plugId !== null && alert.plugId !== undefined) {
        const device = `Device ${formatValue(alert.plugId)}`;
        const household = alert.householdId !== null && alert.householdId !== undefined
            ? ` in Household ${formatValue(alert.householdId)}`
            : '';
        return `${device}${household}`;
    }

    return `House ${formatValue(alert.houseId)}`;
}

function getAlertDetailMessage(alert) {
    const anomalyType = String(alert.anomalyType || '').toUpperCase();
    const scope = getScopeSentence(alert);

    if (anomalyType === 'MAX') {
        return `${scope} exceeded its historical maximum consumption.`;
    }

    if (anomalyType === 'AVG') {
        return `${scope} is consuming significantly more energy than its historical average.`;
    }

    if (anomalyType === 'MIN') {
        return `${scope} is consuming significantly less energy than its historical baseline.`;
    }

    return `${scope} is showing anomalous consumption behavior compared with historical baseline.`;
}

function renderAlert(alert, highlight = true) {
    ensureEmptyState();

    const card = document.createElement('article');
    card.className = `alert${highlight ? ' recent' : ''}`;
    card.dataset.anomalyType = String(alert.anomalyType || '').toUpperCase();

    const presentation = getAlertPresentation(alert);

    const title = document.createElement('div');
    title.className = 'alert-top';

    const heading = document.createElement('div');
    heading.className = 'alert-title';

    const badge = document.createElement('span');
    badge.className = `severity-badge severity-${presentation.severityClass}`;
    badge.textContent = presentation.severity;

    const severityLabel = document.createElement('span');
    severityLabel.className = 'severity-label';
    severityLabel.textContent = 'Severity';

    const name = document.createElement('h3');
    name.className = 'alert-name';
    name.textContent = presentation.title;

    const scope = document.createElement('p');
    scope.className = 'alert-meta';
    scope.textContent = getScopeLine(alert);

    heading.appendChild(badge);
    heading.appendChild(severityLabel);
    heading.appendChild(name);
    heading.appendChild(scope);

    const time = document.createElement('div');
    time.className = 'alert-time';
    time.textContent = `Detected ${formatDate(alert.receivedAt || alert.timestamp)}`;

    title.appendChild(heading);
    title.appendChild(time);

    const summary = document.createElement('p');
    summary.className = 'alert-summary';
    summary.textContent = getAlertDetailMessage(alert);

    const grid = document.createElement('div');
    grid.className = 'alert-grid';

    const fields = [
        ['Current value', alert.value],
        ['Historical average', alert.avg],
        ['Historical maximum', alert.max],
        ['Historical minimum', alert.min],
        ['Window', `${formatValue(alert.windowSize)} min`],
        ['House', alert.houseId],
        ['Household', alert.householdId],
        ['Device', alert.plugId],
        ['Threshold', `${formatValue(alert.anomalyThresholdPercent)}%`],
        ['Detected', formatDate(alert.receivedAt || alert.timestamp)]
    ];

    for (const [label, value] of fields) {
        const field = document.createElement('div');
        field.className = 'field';

        const labelNode = document.createElement('span');
        labelNode.className = 'label';
        labelNode.textContent = label;

        const valueNode = document.createElement('span');
        valueNode.className = 'value';
        valueNode.textContent = formatValue(value);

        field.appendChild(labelNode);
        field.appendChild(valueNode);
        grid.appendChild(field);
    }

    card.appendChild(title);
    card.appendChild(summary);
    card.appendChild(grid);
    alertList.prepend(card);

    while (alertList.querySelectorAll('.alert').length > maxVisibleAlerts) {
        const removableAlert = alertList.querySelector('.alert:last-of-type');
        if (!removableAlert) {
            break;
        }
        removableAlert.remove();
    }

    updateAlertCount();

    if (highlight) {
        window.setTimeout(() => {
            card.classList.remove('recent');
        }, 1500);
    }
}

function handleMessage(event) {
    const payload = JSON.parse(event.data);

    if (payload.type === 'history' && Array.isArray(payload.alerts)) {
        alertList.innerHTML = '';
        payload.alerts.slice().reverse().forEach((alert) => renderAlert(alert, false));
        ensureEmptyState();
        updateAlertCount();
        return;
    }

    renderAlert(payload, true);
}

function connect() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) {
        return;
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${protocol}//${window.location.host}`);

    socket.addEventListener('open', () => {
        isConnected = true;
        setConnected(true);
        ensureEmptyState();
        if (reconnectTimer) {
            window.clearTimeout(reconnectTimer);
            reconnectTimer = undefined;
        }
    });

    socket.addEventListener('message', handleMessage);

    socket.addEventListener('close', () => {
        isConnected = false;
        setConnected(false);
        ensureEmptyState();
        reconnectTimer = window.setTimeout(connect, 2000);
    });

    socket.addEventListener('error', () => {
        isConnected = false;
        setConnected(false);
        ensureEmptyState();
    });
}

setConnected(false);
ensureEmptyState();
connect();