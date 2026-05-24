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

    // Indicators
    bars: [],            // processed bars from last fetch: [{time, open, high, low, close, volume, vwap}]
    indicators: { bb: false, sma20: false, sma50: false, sma200: false, ema12: false, ema26: false, vwap: false, rsi: false },
    indSeries: {},       // name -> LWC series (or {upper,middle,lower} for BB)
    rsiChart: null,
    rsiSeries: null,
    rsiSyncBusy: false,
};

const TF_SEC = { '1s': 1, '1m': 60, '5m': 300, '15m': 900, '1h': 3600 };

// ── Init ──────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
    setupNavigation();
    setupTfButtons();
    setupIndicatorToolbar();
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

    // Crosshair legend
    state.chart.subscribeCrosshairMove(param => {
        if (!param.time || !param.point) { clearLegend(); return; }
        const bar = param.seriesData.get(state.candleSeries);
        if (!bar) { clearLegend(); return; }
        updateLegend(param.time, bar, param);
    });

    window.addEventListener('resize', () => {
        const w = container.clientWidth;
        const h = container.clientHeight;
        state.chart.applyOptions({ width: w, height: h });
        if (state.rsiChart) {
            state.rsiChart.applyOptions({ width: document.getElementById('rsi-container').clientWidth });
        }
    });

    // Reset zoom button
    document.getElementById('reset-zoom').addEventListener('click', () => {
        state.chart.timeScale().fitContent();
        if (state.rsiChart) state.rsiChart.timeScale().fitContent();
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

        // Build processed bars with all fields needed for indicators
        const processed = bars.map(b => ({
            time: Math.floor(new Date(b.ts).getTime() / 1000),
            open: b.open, high: b.high, low: b.low, close: b.close,
            volume: b.volume_base || 0,
            vwap: b.vwap || b.close,
        }));
        state.bars = processed;

        state.candleSeries.setData(processed.map(b => ({
            time: b.time, open: b.open, high: b.high, low: b.low, close: b.close,
        })));
        state.volumeSeries.setData(processed.map(b => ({
            time: b.time,
            value: b.volume,
            color: b.close >= b.open ? 'rgba(52,211,153,0.25)' : 'rgba(248,113,113,0.25)',
        })));

        // Gap markers
        const expected = TF_SEC[state.tf] || 300;
        const gaps = [];
        for (let i = 1; i < processed.length; i++) {
            const diff = processed[i].time - processed[i - 1].time;
            if (diff > expected * 1.5) {
                const mins = Math.round(diff / 60);
                gaps.push({
                    time: processed[i - 1].time,
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

        renderIndicators(state.bars);
        state.chart.timeScale().fitContent();
        if (state.rsiChart) state.rsiChart.timeScale().fitContent();
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

// rAF throttle flag — only one pending chart repaint at a time
let _chartRafPending = false;

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

    // Keep state.bars in sync immediately (for indicator math)
    const liveBar = { ...state.currentCandle, volume: state.currentVolume, vwap: state.currentCandle.close };
    if (state.bars.length > 0 && state.bars[state.bars.length - 1].time === candleStart) {
        state.bars[state.bars.length - 1] = liveBar;
    } else if (state.bars.length === 0 || candleStart > state.bars[state.bars.length - 1].time) {
        state.bars.push(liveBar);
    }

    // Throttle visual chart updates to animation frames (~60fps)
    if (!_chartRafPending) {
        _chartRafPending = true;
        requestAnimationFrame(() => {
            _chartRafPending = false;
            if (!state.currentCandle) return;
            state.candleSeries.update(state.currentCandle);
            state.volumeSeries.update({
                time: state.currentCandleTime,
                value: state.currentVolume,
                color: state.currentCandle.close >= state.currentCandle.open
                    ? 'rgba(52,211,153,0.25)' : 'rgba(248,113,113,0.25)',
            });
            const bar = state.bars[state.bars.length - 1];
            if (bar) updateLiveIndicators(bar);
        });
    }
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

// ── Indicator toolbar ──────────────────────────────────────────────────────

function setupIndicatorToolbar() {
    document.querySelectorAll('.ind-btn').forEach(btn => {
        btn.addEventListener('click', () => toggleIndicator(btn.dataset.ind));
    });
}

function toggleIndicator(name) {
    state.indicators[name] = !state.indicators[name];
    document.querySelectorAll(`.ind-btn[data-ind="${name}"]`).forEach(btn => {
        btn.classList.toggle('active', state.indicators[name]);
    });

    if (!state.indicators[name]) {
        removeIndicatorSeries(name);
        if (name === 'rsi') {
            document.getElementById('rsi-container').classList.remove('visible');
        }
    } else {
        renderIndicators(state.bars);
    }
}

function removeIndicatorSeries(name) {
    const s = state.indSeries[name];
    if (!s) return;
    if (name === 'bb') {
        try { state.chart.removeSeries(s.upper); } catch {}
        try { state.chart.removeSeries(s.middle); } catch {}
        try { state.chart.removeSeries(s.lower); } catch {}
    } else {
        try { state.chart.removeSeries(s); } catch {}
    }
    delete state.indSeries[name];
}

// ── Indicator math ──────────────────────────────────────────────────────────

function calcSMA(values, period) {
    const result = new Array(values.length).fill(null);
    let sum = 0;
    for (let i = 0; i < values.length; i++) {
        sum += values[i];
        if (i >= period) sum -= values[i - period];
        if (i >= period - 1) result[i] = sum / period;
    }
    return result;
}

function calcEMA(values, period) {
    const result = new Array(values.length).fill(null);
    const alpha = 2 / (period + 1);
    let ema = null;
    for (let i = 0; i < values.length; i++) {
        if (ema === null) {
            // Seed with SMA of first `period` values
            if (i === period - 1) {
                let sum = 0;
                for (let j = 0; j < period; j++) sum += values[j];
                ema = sum / period;
                result[i] = ema;
            }
        } else {
            ema = alpha * values[i] + (1 - alpha) * ema;
            result[i] = ema;
        }
    }
    return result;
}

function calcBB(values, period = 20, mult = 2) {
    const middle = calcSMA(values, period);
    const upper = new Array(values.length).fill(null);
    const lower = new Array(values.length).fill(null);
    for (let i = period - 1; i < values.length; i++) {
        let variance = 0;
        for (let j = i - period + 1; j <= i; j++) {
            const diff = values[j] - middle[i];
            variance += diff * diff;
        }
        const stddev = Math.sqrt(variance / period);
        upper[i] = middle[i] + mult * stddev;
        lower[i] = middle[i] - mult * stddev;
    }
    return { upper, middle, lower };
}

function calcRSI(values, period = 14) {
    const result = new Array(values.length).fill(null);
    if (values.length < period + 1) return result;

    let gainSum = 0, lossSum = 0;
    for (let i = 1; i <= period; i++) {
        const diff = values[i] - values[i - 1];
        if (diff > 0) gainSum += diff; else lossSum -= diff;
    }
    let avgGain = gainSum / period;
    let avgLoss = lossSum / period;
    result[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);

    for (let i = period + 1; i < values.length; i++) {
        const diff = values[i] - values[i - 1];
        const gain = diff > 0 ? diff : 0;
        const loss = diff < 0 ? -diff : 0;
        avgGain = (avgGain * (period - 1) + gain) / period;
        avgLoss = (avgLoss * (period - 1) + loss) / period;
        result[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
    }
    return result;
}

function barsToSeries(times, values) {
    const out = [];
    for (let i = 0; i < times.length; i++) {
        if (values[i] !== null) out.push({ time: times[i], value: values[i] });
    }
    return out;
}

// ── Indicator rendering ─────────────────────────────────────────────────────

const IND_COLORS = {
    sma20:  '#60a5fa',
    sma50:  '#f59e0b',
    sma200: '#f87171',
    ema12:  '#34d399',
    ema26:  '#a78bfa',
    bb:     '#4b5563',
    vwap:   '#38bdf8',
};

function renderIndicators(bars) {
    if (!bars || bars.length === 0 || !state.chart) return;
    const times = bars.map(b => b.time);
    const closes = bars.map(b => b.close);

    // SMA 20
    if (state.indicators.sma20) {
        if (!state.indSeries.sma20) {
            state.indSeries.sma20 = state.chart.addLineSeries({
                color: IND_COLORS.sma20, lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
            });
        }
        state.indSeries.sma20.setData(barsToSeries(times, calcSMA(closes, 20)));
    }

    // SMA 50
    if (state.indicators.sma50) {
        if (!state.indSeries.sma50) {
            state.indSeries.sma50 = state.chart.addLineSeries({
                color: IND_COLORS.sma50, lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
            });
        }
        state.indSeries.sma50.setData(barsToSeries(times, calcSMA(closes, 50)));
    }

    // SMA 200
    if (state.indicators.sma200) {
        if (!state.indSeries.sma200) {
            state.indSeries.sma200 = state.chart.addLineSeries({
                color: IND_COLORS.sma200, lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
            });
        }
        state.indSeries.sma200.setData(barsToSeries(times, calcSMA(closes, 200)));
    }

    // EMA 12
    if (state.indicators.ema12) {
        if (!state.indSeries.ema12) {
            state.indSeries.ema12 = state.chart.addLineSeries({
                color: IND_COLORS.ema12, lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
            });
        }
        state.indSeries.ema12.setData(barsToSeries(times, calcEMA(closes, 12)));
    }

    // EMA 26
    if (state.indicators.ema26) {
        if (!state.indSeries.ema26) {
            state.indSeries.ema26 = state.chart.addLineSeries({
                color: IND_COLORS.ema26, lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
            });
        }
        state.indSeries.ema26.setData(barsToSeries(times, calcEMA(closes, 26)));
    }

    // Bollinger Bands
    if (state.indicators.bb) {
        const { upper, middle, lower } = calcBB(closes, 20, 2);
        if (!state.indSeries.bb) {
            state.indSeries.bb = {
                upper: state.chart.addLineSeries({ color: IND_COLORS.bb, lineWidth: 1, lineStyle: 1, priceLineVisible: false, lastValueVisible: false }),
                middle: state.chart.addLineSeries({ color: '#6b7280', lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false }),
                lower: state.chart.addLineSeries({ color: IND_COLORS.bb, lineWidth: 1, lineStyle: 1, priceLineVisible: false, lastValueVisible: false }),
            };
        }
        state.indSeries.bb.upper.setData(barsToSeries(times, upper));
        state.indSeries.bb.middle.setData(barsToSeries(times, middle));
        state.indSeries.bb.lower.setData(barsToSeries(times, lower));
    }

    // VWAP
    if (state.indicators.vwap) {
        if (!state.indSeries.vwap) {
            state.indSeries.vwap = state.chart.addLineSeries({
                color: IND_COLORS.vwap, lineWidth: 1, priceLineVisible: false, lastValueVisible: false,
            });
        }
        state.indSeries.vwap.setData(bars.map(b => ({ time: b.time, value: b.vwap })));
    }

    // RSI
    if (state.indicators.rsi) {
        const rsiContainer = document.getElementById('rsi-container');
        rsiContainer.classList.add('visible');
        if (!state.rsiChart) {
            state.rsiChart = LightweightCharts.createChart(rsiContainer, {
                width: rsiContainer.clientWidth,
                height: 130,
                layout: { background: { color: '#07090e' }, textColor: '#7b8db8', fontFamily: "'JetBrains Mono', monospace" },
                grid: { vertLines: { color: '#0d1117' }, horzLines: { color: '#0d1117' } },
                crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
                rightPriceScale: { borderColor: '#1e2a3d', scaleMargins: { top: 0.1, bottom: 0.1 } },
                timeScale: { borderColor: '#1e2a3d', timeVisible: true, secondsVisible: false, visible: true },
                handleScroll: false,
                handleScale: false,
            });
            state.rsiSeries = state.rsiChart.addLineSeries({ color: '#a78bfa', lineWidth: 1, priceLineVisible: false, lastValueVisible: true });
            state.rsiSeries.createPriceLine({ price: 70, color: '#374151', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '70' });
            state.rsiSeries.createPriceLine({ price: 30, color: '#374151', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '30' });

            // Sync main → RSI
            state.chart.timeScale().subscribeVisibleLogicalRangeChange(range => {
                if (!state.rsiSyncBusy && range && state.rsiChart) {
                    state.rsiSyncBusy = true;
                    state.rsiChart.timeScale().setVisibleLogicalRange(range);
                    state.rsiSyncBusy = false;
                }
            });
            // Sync RSI → main
            state.rsiChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
                if (!state.rsiSyncBusy && range) {
                    state.rsiSyncBusy = true;
                    state.chart.timeScale().setVisibleLogicalRange(range);
                    state.rsiSyncBusy = false;
                }
            });
        }
        const rsiValues = calcRSI(closes, 14);
        state.rsiSeries.setData(barsToSeries(times, rsiValues));
        state.rsiChart.timeScale().fitContent();
    }
}

// Update only the last data point of active indicators (called on live trades)
function updateLiveIndicators(liveBar) {
    if (!state.bars || state.bars.length < 2) return;
    const bars = state.bars;
    const times = bars.map(b => b.time);
    const closes = bars.map(b => b.close);
    const lastIdx = bars.length - 1;
    const t = liveBar.time;

    if (state.indSeries.sma20) {
        const sma = calcSMA(closes, 20);
        if (sma[lastIdx] !== null) state.indSeries.sma20.update({ time: t, value: sma[lastIdx] });
    }
    if (state.indSeries.sma50) {
        const sma = calcSMA(closes, 50);
        if (sma[lastIdx] !== null) state.indSeries.sma50.update({ time: t, value: sma[lastIdx] });
    }
    if (state.indSeries.ema12) {
        const ema = calcEMA(closes, 12);
        if (ema[lastIdx] !== null) state.indSeries.ema12.update({ time: t, value: ema[lastIdx] });
    }
    if (state.indSeries.ema26) {
        const ema = calcEMA(closes, 26);
        if (ema[lastIdx] !== null) state.indSeries.ema26.update({ time: t, value: ema[lastIdx] });
    }
    if (state.indSeries.vwap) {
        state.indSeries.vwap.update({ time: t, value: liveBar.vwap });
    }
    if (state.rsiSeries && state.indicators.rsi) {
        const rsi = calcRSI(closes, 14);
        if (rsi[lastIdx] !== null) state.rsiSeries.update({ time: t, value: rsi[lastIdx] });
    }
    // BB — just re-render the last bar
    if (state.indSeries.bb) {
        const { upper, middle, lower } = calcBB(closes, 20, 2);
        if (upper[lastIdx] !== null) {
            state.indSeries.bb.upper.update({ time: t, value: upper[lastIdx] });
            state.indSeries.bb.middle.update({ time: t, value: middle[lastIdx] });
            state.indSeries.bb.lower.update({ time: t, value: lower[lastIdx] });
        }
    }
}

// ── Crosshair legend ────────────────────────────────────────────────────────

const pf = (v) => {
    if (v == null) return '--';
    const d = v > 100 ? 2 : v > 1 ? 4 : 6;
    return v.toFixed(d);
};

function clearLegend() {
    ['leg-label','leg-o','leg-h','leg-l','leg-c','leg-v','leg-ind-values'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.textContent = '';
    });
}

function updateLegend(time, bar, param) {
    const d = new Date(time * 1000);
    const label = d.toLocaleString('en-GB', { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });

    const setLeg = (id, text, className) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.textContent = text;
        if (className) el.className = className;
    };

    setLeg('leg-label', label, 'leg-label');
    setLeg('leg-o', `O ${pf(bar.open)}`, 'leg-o');
    setLeg('leg-h', `H ${pf(bar.high)}`, 'leg-h');
    setLeg('leg-l', `L ${pf(bar.low)}`, 'leg-l');
    setLeg('leg-c', `C ${pf(bar.close)}`, bar.close >= bar.open ? 'leg-c price-up' : 'leg-c price-down');

    // Volume from histogram series
    const volData = param.seriesData.get(state.volumeSeries);
    const vol = volData ? volData.value : null;
    setLeg('leg-v', vol != null ? `V ${fmtNum(Math.round(vol))}` : '', 'leg-v');

    // Indicator values at crosshair
    const indParts = [];
    const indNames = { sma20: 'SMA20', sma50: 'SMA50', sma200: 'SMA200', ema12: 'EMA12', ema26: 'EMA26', vwap: 'VWAP' };
    for (const [key, label] of Object.entries(indNames)) {
        const s = state.indSeries[key];
        if (!s) continue;
        const d = param.seriesData.get(s);
        if (d) indParts.push(`<span style="color:${IND_COLORS[key]}">${label} ${pf(d.value)}</span>`);
    }
    if (state.indSeries.bb) {
        const du = param.seriesData.get(state.indSeries.bb.upper);
        const dl = param.seriesData.get(state.indSeries.bb.lower);
        if (du && dl) indParts.push(`<span style="color:#9ca3af">BB ${pf(dl.value)}–${pf(du.value)}</span>`);
    }
    const indEl = document.getElementById('leg-ind-values');
    if (indEl) indEl.innerHTML = indParts.join(' ');
}
