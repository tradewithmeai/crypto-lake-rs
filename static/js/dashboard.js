/**
 * Crypto Lake Dashboard
 */

const API = window.location.origin + '/api/v1';
const WS_BASE = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/api/v1';

// ── State ─────────────────────────────────────────────────────────────────

const state = {
    exchange: '',
    symbol: '',
    tf: '5m',
    activeTab: 'chart',
    lastPrice: null,
    ws: null,
    chart: null,
    candleSeries: null,
    volumeSeries: null,
    currentCandle: null,
    currentCandleTime: null,
    currentVolume: 0,
    allSymbols: {},      // { exchange: [symbol, ...] }
    symbolPrices: {},    // { "exchange:symbol": price }
    lakeSortCol: 'live_pct',
    lakeSortDir: 'desc',
    lakeData: [],
    tickerTrades: [],
    healthPollInterval: null,
    lakePollInterval: null,
};

const TF_SEC = { '1s': 1, '1m': 60, '5m': 300, '15m': 900, '1h': 3600 };

// ── Init ──────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
    setupNavigation();
    setupTfButtons();
    await loadSymbols();
    createChart();
    await loadChartData();
    connectWebSocket();
    startHealthPoll();
});

// ── Navigation ────────────────────────────────────────────────────────────

function setupNavigation() {
    document.querySelectorAll('.nav-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
}

function switchTab(tab) {
    state.activeTab = tab;

    document.querySelectorAll('.nav-btn').forEach(b => {
        b.classList.toggle('active', b.dataset.tab === tab);
    });

    document.querySelectorAll('.tab-panel').forEach(p => {
        p.classList.toggle('active', p.id === `tab-${tab}`);
    });

    // Show/hide TF controls — only relevant on chart tab
    document.getElementById('tf-controls').style.visibility =
        tab === 'chart' ? 'visible' : 'hidden';

    if (tab === 'lake') loadLakeData();
    if (tab === 'system') updateSystemView();
}

// ── Symbol loading ────────────────────────────────────────────────────────

async function loadSymbols() {
    try {
        const data = await apiFetch('/symbols');
        state.allSymbols = data.exchanges || {};
        buildSidebarSymbols();

        // Default to first exchange + first symbol
        const firstEx = Object.keys(state.allSymbols)[0] || '';
        const firstSym = (state.allSymbols[firstEx] || [])[0] || '';
        selectSymbol(firstEx, firstSym);
    } catch (err) {
        console.error('Failed to load symbols:', err);
    }
}

function buildSidebarSymbols() {
    const knownExchanges = ['binance', 'coinbase', 'kraken'];

    // Add any exchanges not in the known list dynamically
    for (const ex of Object.keys(state.allSymbols)) {
        if (!knownExchanges.includes(ex)) knownExchanges.push(ex);
    }

    for (const ex of knownExchanges) {
        const symbols = state.allSymbols[ex];
        if (!symbols || symbols.length === 0) {
            // Hide the whole group
            const group = document.getElementById(`exchange-${ex}`);
            if (group) group.style.display = 'none';
            continue;
        }

        let listEl = document.getElementById(`symbol-list-${ex}`);
        if (!listEl) {
            // Create dynamic group
            const group = buildExchangeGroup(ex);
            document.querySelector('.sidebar').appendChild(group);
            listEl = group.querySelector('.symbol-list');
        }

        listEl.innerHTML = '';
        for (const sym of symbols) {
            const btn = document.createElement('button');
            btn.className = 'symbol-btn';
            btn.dataset.exchange = ex;
            btn.dataset.symbol = sym;
            btn.innerHTML = `<span>${sym}</span><span class="symbol-price" id="sp-${ex}-${sym}"></span>`;
            btn.addEventListener('click', () => selectSymbol(ex, sym));
            listEl.appendChild(btn);
        }

        // Wire up exchange header toggle
        const header = document.querySelector(`#exchange-${ex} .exchange-header`);
        if (header) {
            header.addEventListener('click', () => {
                const group = document.getElementById(`exchange-${ex}`);
                group.classList.toggle('collapsed');
            });
        }
    }
}

function buildExchangeGroup(ex) {
    const div = document.createElement('div');
    div.className = 'exchange-group';
    div.id = `exchange-${ex}`;
    div.innerHTML = `
        <div class="exchange-header" data-exchange="${ex}">
            <span class="exchange-dot"></span>
            <span class="exchange-name">${cap(ex)}</span>
            <svg class="exchange-chevron" width="10" height="10" viewBox="0 0 10 10" fill="none">
                <polyline points="2,3 5,7 8,3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
        </div>
        <div class="symbol-list" id="symbol-list-${ex}"></div>`;
    return div;
}

function selectSymbol(exchange, symbol) {
    if (!exchange || !symbol) return;
    state.exchange = exchange;
    state.symbol = symbol;

    // Update sidebar active state
    document.querySelectorAll('.symbol-btn').forEach(btn => {
        btn.classList.toggle('active',
            btn.dataset.exchange === exchange && btn.dataset.symbol === symbol);
    });

    // Update topbar labels
    document.getElementById('symbol-label').textContent = symbol;
    document.getElementById('exchange-label').textContent = exchange;

    if (state.activeTab === 'chart') {
        loadChartData();
        reconnectWebSocket();
    }
}

// ── TF buttons ────────────────────────────────────────────────────────────

function setupTfButtons() {
    document.querySelectorAll('.tf-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            state.tf = btn.dataset.tf;
            loadChartData();
        });
    });
}

// ── Chart ─────────────────────────────────────────────────────────────────

function createChart() {
    const container = document.getElementById('chart-container');
    container.innerHTML = '';

    state.chart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: container.clientHeight,
        layout: {
            background: { color: '#07090e' },
            textColor: '#7b8db8',
            fontFamily: "'JetBrains Mono', monospace",
        },
        grid: {
            vertLines: { color: '#0d1117' },
            horzLines: { color: '#0d1117' },
        },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
        rightPriceScale: { borderColor: '#1e2a3d' },
        timeScale: {
            borderColor: '#1e2a3d',
            timeVisible: true,
            secondsVisible: false,
        },
    });

    state.candleSeries = state.chart.addCandlestickSeries({
        upColor: '#34d399',
        downColor: '#f87171',
        borderDownColor: '#f87171',
        borderUpColor: '#34d399',
        wickDownColor: '#f87171',
        wickUpColor: '#34d399',
    });

    state.volumeSeries = state.chart.addHistogramSeries({
        color: '#34d399',
        priceFormat: { type: 'volume' },
        priceScaleId: '',
    });

    state.volumeSeries.priceScale().applyOptions({
        scaleMargins: { top: 0.8, bottom: 0 },
    });

    window.addEventListener('resize', () => {
        state.chart.applyOptions({
            width: container.clientWidth,
            height: container.clientHeight,
        });
    });
}

async function loadChartData() {
    if (!state.symbol) return;

    try {
        const data = await apiFetch(`/bars/${state.symbol}/latest?tf=${state.tf}&limit=500`);

        if (!data.data || data.data.length === 0) {
            state.candleSeries.setData([]);
            state.volumeSeries.setData([]);
            document.getElementById('live-price').textContent = 'No data';
            return;
        }

        const bars = data.data.reverse(); // API returns newest first

        const candles = bars.map(b => ({
            time: Math.floor(new Date(b.ts).getTime() / 1000),
            open: b.open, high: b.high, low: b.low, close: b.close,
        }));

        const volumes = bars.map(b => ({
            time: Math.floor(new Date(b.ts).getTime() / 1000),
            value: b.volume_base || 0,
            color: b.close >= b.open ? 'rgba(52,211,153,0.25)' : 'rgba(248,113,113,0.25)',
        }));

        state.candleSeries.setData(candles);
        state.volumeSeries.setData(volumes);

        // Gap markers
        const expected = TF_SEC[state.tf] || 300;
        const gaps = [];
        for (let i = 1; i < candles.length; i++) {
            const diff = candles[i].time - candles[i - 1].time;
            if (diff > expected * 1.5) {
                const mins = Math.round(diff / 60);
                gaps.push({
                    time: candles[i - 1].time,
                    position: 'belowBar',
                    color: '#e8a020',
                    shape: 'arrowUp',
                    text: mins >= 60 ? `GAP ${Math.round(mins / 60)}h` : `GAP ${mins}m`,
                });
            }
        }
        state.candleSeries.setMarkers(gaps);

        const gapBadge = document.getElementById('gap-badge');
        if (gaps.length > 0) {
            gapBadge.textContent = `${gaps.length} gap${gaps.length > 1 ? 's' : ''}`;
            gapBadge.style.display = 'inline';
        } else {
            gapBadge.style.display = 'none';
        }

        const latest = bars[bars.length - 1];
        const prev = bars.length > 1 ? bars[bars.length - 2].close : latest.open;
        updatePrice(latest.close, prev);

        state.currentCandle = null;
        state.currentCandleTime = null;
        state.currentVolume = 0;

        state.chart.timeScale().fitContent();
    } catch (err) {
        console.error('Chart load failed:', err);
    }
}

function updatePrice(price, prev) {
    const decimals = price > 100 ? 2 : price > 1 ? 4 : 6;
    const priceEl = document.getElementById('live-price');
    const changeEl = document.getElementById('price-change');

    priceEl.textContent = price.toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
    });

    if (prev) {
        const pct = ((price - prev) / prev) * 100;
        const sign = pct >= 0 ? '+' : '';
        changeEl.textContent = `${sign}${pct.toFixed(2)}%`;
        changeEl.className = `price-change ${pct >= 0 ? 'price-up' : 'price-down'}`;
        priceEl.className = `live-price ${pct >= 0 ? 'price-up' : 'price-down'}`;
    }

    state.lastPrice = price;

    // Update sidebar price
    const spEl = document.getElementById(`sp-${state.exchange}-${state.symbol}`);
    if (spEl) {
        const d = price > 100 ? 2 : price > 1 ? 4 : 6;
        spEl.textContent = price.toFixed(d);
    }
}

// ── WebSocket ─────────────────────────────────────────────────────────────

function connectWebSocket() {
    if (state.ws) { state.ws.close(); state.ws = null; }
    if (!state.symbol) return;

    // Subscribe to all symbols for ticker, filter in JS
    state.ws = new WebSocket(`${WS_BASE}/ws/stream`);

    state.ws.onopen = () => {
        setWsStatus('connected');
    };

    state.ws.onmessage = (ev) => {
        try {
            const trade = JSON.parse(ev.data);
            if (trade.stream !== 'trade') return;
            onTrade(trade);
        } catch {}
    };

    state.ws.onclose = () => {
        setWsStatus('disconnected');
        setTimeout(() => { if (state.symbol) connectWebSocket(); }, 3000);
    };

    state.ws.onerror = () => setWsStatus('disconnected');
}

function reconnectWebSocket() {
    state.currentCandle = null;
    state.currentCandleTime = null;
    state.currentVolume = 0;
    connectWebSocket();
}

function setWsStatus(status) {
    const dot = document.getElementById('ws-dot');
    const label = document.getElementById('ws-label');
    dot.className = `ws-dot ${status}`;
    label.textContent = status === 'connected' ? 'live' : 'offline';

    // Update feed indicator in system tab
    const fi = document.getElementById('feed-indicator');
    if (fi) {
        fi.className = `feed-indicator ${status === 'connected' ? 'live' : ''}`;
        const txt = fi.querySelector('#feed-status-text');
        if (txt) txt.textContent = status === 'connected' ? 'Live feed connected' : 'Disconnected';
    }
}

function onTrade(trade) {
    const price = trade.price;
    const qty = trade.qty || 0;

    // Update current symbol chart + price
    if (trade.symbol === state.symbol && trade.exchange === state.exchange) {
        updateChart(trade);
        updatePrice(price, state.lastPrice || price);
    }

    // Update sidebar price for any symbol we track
    const spEl = document.getElementById(`sp-${trade.exchange}-${trade.symbol}`);
    if (spEl) {
        const d = price > 100 ? 2 : price > 1 ? 4 : 6;
        spEl.textContent = price.toFixed(d);
    }

    // Feed ticker (system tab)
    addTickerTrade(trade);
}

function updateChart(trade) {
    if (!state.candleSeries) return;
    const price = trade.price;
    const qty = trade.qty || 0;
    const tfSec = TF_SEC[state.tf] || 300;
    // Use receive time for consistency with aggregator
    const now = Math.floor(Date.now() / 1000);
    const candleStart = Math.floor(now / tfSec) * tfSec;

    if (!state.currentCandle || candleStart > state.currentCandleTime) {
        state.currentCandle = { time: candleStart, open: price, high: price, low: price, close: price };
        state.currentCandleTime = candleStart;
        state.currentVolume = qty;
    } else {
        state.currentCandle.high = Math.max(state.currentCandle.high, price);
        state.currentCandle.low = Math.min(state.currentCandle.low, price);
        state.currentCandle.close = price;
        state.currentVolume += qty;
    }

    state.candleSeries.update(state.currentCandle);
    state.volumeSeries.update({
        time: candleStart,
        value: state.currentVolume,
        color: state.currentCandle.close >= state.currentCandle.open
            ? 'rgba(52,211,153,0.25)' : 'rgba(248,113,113,0.25)',
    });
}

// ── Trade ticker (system tab) ─────────────────────────────────────────────

function addTickerTrade(trade) {
    state.tickerTrades.unshift(trade);
    if (state.tickerTrades.length > 8) state.tickerTrades.pop();

    if (state.activeTab === 'system') renderTicker();
}

function renderTicker() {
    const container = document.getElementById('ticker-rows');
    if (!container) return;
    container.innerHTML = '';
    for (const t of state.tickerTrades) {
        const d = t.price > 100 ? 2 : t.price > 1 ? 4 : 6;
        const row = document.createElement('div');
        row.className = 'ticker-row';
        row.innerHTML = `
            <span class="ticker-sym">${t.exchange}:${t.symbol}</span>
            <span class="ticker-price">${t.price.toFixed(d)}</span>
            <span class="${t.side === 'buy' ? 'ticker-side-buy' : 'ticker-side-sell'}">${t.side.toUpperCase()}</span>`;
        container.appendChild(row);
    }
}

// ── Health polling ────────────────────────────────────────────────────────

function startHealthPoll() {
    pollHealth();
    state.healthPollInterval = setInterval(pollHealth, 10000);
}

async function pollHealth() {
    try {
        const data = await apiFetch('/health');
        state.health = data;
        if (state.activeTab === 'system') updateSystemView();
    } catch {}
}

function updateSystemView() {
    const h = state.health || {};

    const set = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = typeof val === 'number' ? val.toLocaleString() : (val || '--');
    };

    set('stat-messages', h.messages_received);
    set('stat-trades', h.trades_received);
    set('stat-bars', h.bars_produced);
    set('stat-disconnects', h.ws_disconnects);
    set('stat-reconnects', h.ws_reconnects);

    // Exchange status list
    const list = document.getElementById('exchange-status-list');
    if (list) {
        list.innerHTML = '';
        for (const [ex, syms] of Object.entries(state.allSymbols)) {
            const isLive = state.ws && state.ws.readyState === WebSocket.OPEN;
            const item = document.createElement('div');
            item.className = 'exchange-status-item';
            item.innerHTML = `
                <div class="ex-status-dot ${isLive ? 'live' : ''}"></div>
                <div class="ex-status-info">
                    <div class="ex-status-name">${cap(ex)}</div>
                    <div class="ex-status-detail">${syms.length} symbol${syms.length !== 1 ? 's' : ''}</div>
                </div>`;
            list.appendChild(item);
        }
    }

    renderTicker();
}

// ── Data Lake tab ─────────────────────────────────────────────────────────

async function loadLakeData() {
    const tbody = document.getElementById('lake-table-body');
    tbody.innerHTML = '<tr><td colspan="9" class="table-loading">Scanning parquet archive...</td></tr>';

    try {
        const data = await apiFetch('/analysis/summary');
        state.lakeData = data.symbols || [];
        renderLakeTable();
        renderLakeSummary();
    } catch (err) {
        tbody.innerHTML = `<tr><td colspan="9" class="table-loading">Failed to load: ${err.message}</td></tr>`;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('lake-refresh').addEventListener('click', loadLakeData);

    document.querySelectorAll('.lake-table th[data-sort]').forEach(th => {
        th.addEventListener('click', () => {
            const col = th.dataset.sort;
            if (state.lakeSortCol === col) {
                state.lakeSortDir = state.lakeSortDir === 'asc' ? 'desc' : 'asc';
            } else {
                state.lakeSortCol = col;
                state.lakeSortDir = col === 'symbol' || col === 'exchange' ? 'asc' : 'desc';
            }
            renderLakeTable();
        });
    });
});

function renderLakeSummary() {
    const rows = state.lakeData;
    const totalBars = rows.reduce((s, r) => s + r.total_bars, 0);
    const liveBars = rows.reduce((s, r) => s + r.live_bars, 0);
    const totalTrades = rows.reduce((s, r) => s + r.total_trades, 0);
    const avgLive = rows.length > 0
        ? rows.reduce((s, r) => s + r.live_pct, 0) / rows.length : 0;

    const bar = document.getElementById('lake-summary-bar');
    bar.innerHTML = `
        <div class="summary-stat">
            <span class="summary-stat-label">Symbols</span>
            <span class="summary-stat-value accent">${rows.length}</span>
        </div>
        <div class="summary-stat">
            <span class="summary-stat-label">Total bars</span>
            <span class="summary-stat-value">${fmtNum(totalBars)}</span>
        </div>
        <div class="summary-stat">
            <span class="summary-stat-label">Live bars</span>
            <span class="summary-stat-value up">${fmtNum(liveBars)}</span>
        </div>
        <div class="summary-stat">
            <span class="summary-stat-label">Total trades</span>
            <span class="summary-stat-value">${fmtNum(totalTrades)}</span>
        </div>
        <div class="summary-stat">
            <span class="summary-stat-label">Avg live %</span>
            <span class="summary-stat-value ${qualityClass(avgLive)}">${avgLive.toFixed(1)}%</span>
        </div>`;
}

function renderLakeTable() {
    const rows = [...state.lakeData];
    const col = state.lakeSortCol;
    const dir = state.lakeSortDir === 'asc' ? 1 : -1;

    rows.sort((a, b) => {
        const av = a[col] ?? '';
        const bv = b[col] ?? '';
        if (typeof av === 'number') return (av - bv) * dir;
        return av.localeCompare(bv) * dir;
    });

    // Update sort indicators
    document.querySelectorAll('.lake-table th[data-sort]').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
        if (th.dataset.sort === col) {
            th.classList.add(state.lakeSortDir === 'asc' ? 'sort-asc' : 'sort-desc');
        }
    });

    const tbody = document.getElementById('lake-table-body');
    tbody.innerHTML = '';

    if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" class="table-loading">No parquet data found yet.</td></tr>';
        return;
    }

    for (const row of rows) {
        const pct = row.live_pct;
        const fillColor = pct >= 80 ? '#34d399' : pct >= 40 ? '#e8a020' : '#f87171';

        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><span class="sym-cell" data-ex="${row.exchange}" data-sym="${row.symbol}">${row.symbol}</span></td>
            <td><span class="ex-badge">${row.exchange}</span></td>
            <td class="num-col live-pct-cell ${qualityClass(pct)}">${pct.toFixed(1)}%</td>
            <td class="num-col">
                <div class="quality-bar-wrap">
                    <div class="quality-bar">
                        <div class="quality-bar-fill" style="width:${Math.min(pct,100)}%;background:${fillColor}"></div>
                    </div>
                </div>
            </td>
            <td class="num-col">${fmtNum(row.total_bars)}</td>
            <td class="num-col">${fmtNum(row.total_trades)}</td>
            <td class="num-col">${row.data_hours.toFixed(1)}h</td>
            <td class="num-col">${fmtPrice(row.last_close)}</td>
            <td class="num-col">${fmtTime(row.latest_ts)}</td>`;

        // Click symbol to switch to chart view
        tr.querySelector('.sym-cell').addEventListener('click', (e) => {
            selectSymbol(e.target.dataset.ex, e.target.dataset.sym);
            switchTab('chart');
        });

        tbody.appendChild(tr);
    }
}

function qualityClass(pct) {
    if (pct >= 80) return 'live-pct-high';
    if (pct >= 40) return 'live-pct-mid';
    return 'live-pct-low';
}

// ── Utilities ─────────────────────────────────────────────────────────────

async function apiFetch(path) {
    const resp = await fetch(API + path);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return resp.json();
}

function fmtNum(n) {
    if (n == null) return '--';
    return n.toLocaleString('en-US');
}

function fmtPrice(p) {
    if (!p) return '--';
    const d = p > 100 ? 2 : p > 1 ? 4 : 6;
    return p.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

function fmtTime(isoStr) {
    if (!isoStr) return '--';
    try {
        const d = new Date(isoStr);
        return d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
    } catch {
        return isoStr;
    }
}

function cap(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}
