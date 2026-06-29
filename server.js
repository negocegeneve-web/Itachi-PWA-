/**
 * ITACHI SERVER — Bot de trading Binance Futures (testnet/mainnet)
 * ----------------------------------------------------------------
 * TOUTE la logique de trading vit ici, côté serveur :
 *   - Calcul des signaux (EMA 8/21 + momentum + Quality score)
 *   - Signature HMAC-SHA256 des ordres
 *   - WebSocket prix live Binance
 *   - Gestion des positions (ouverture / SL / TP / trailing / fermeture)
 *   - Réconciliation : Binance = source de vérité
 *   - Broadcast WebSocket vers le dashboard (affichage uniquement)
 *
 * Le dashboard.html ne contient AUCUNE clé et AUCUNE logique de trading.
 *
 * Variables d'environnement (Railway) :
 *   BINANCE_API_KEY      clé API testnet/mainnet
 *   BINANCE_API_SECRET   secret API
 *   BINANCE_MODE         "testnet" (défaut) | "mainnet"
 *   ASSET                "BTCUSDT" (défaut)
 *   CAPITAL              capital de départ en USDT (défaut 500)
 *   PORT                 port HTTP (Railway le fournit)
 */

'use strict';

const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');

// ------------------------------------------------------------------
// CONFIG
// ------------------------------------------------------------------
const MODE = (process.env.BINANCE_MODE || 'testnet').toLowerCase();
const IS_TESTNET = MODE !== 'mainnet';

const API_KEY = process.env.BINANCE_API_KEY || '';
const API_SECRET = process.env.BINANCE_API_SECRET || '';
const ASSET = (process.env.ASSET || 'BTCUSDT').toUpperCase();
const CAPITAL_START = parseFloat(process.env.CAPITAL || '500');
const PORT = parseInt(process.env.PORT || '8080', 10);

// URLs Binance — testnet vs mainnet
const REST_BASE = IS_TESTNET
  ? 'https://demo-fapi.binance.com'
  : 'https://fapi.binance.com';
const WS_BASE = IS_TESTNET
  ? 'wss://demo-fstream.binance.com'
  : 'wss://fstream.binance.com';

// ------------------------------------------------------------------
// PARAMÈTRES STRATÉGIE (validés sessions précédentes)
// ------------------------------------------------------------------
const STRAT = {
  EMA_FAST: 8,
  EMA_SLOW: 21,
  // Mises en % du capital selon Quality score
  STAKE_LOW: 0.08, // Q < 55
  STAKE_MED: 0.11, // 55 <= Q < 80
  STAKE_HIGH: 0.14, // Q >= 80
  // Levier selon Quality score
  LEV_LOW: 3,
  LEV_MED: 7,
  LEV_HIGH: 12,
  // Risk management
  SL_PCT: 0.015, // -1.5% stop-loss fixe initial
  TP_PCT: 0.020, // +2.0% take-profit
  TRAIL_PCT: 0.005, // -0.5% trailing après TP atteint
  KILL_PCT: 0.20, // -20% capital => arrêt total
  // Garde-fous
  MAX_POSITIONS: 2, // positions simultanées max
  MIN_GAP_MS: 30000, // 30s minimum entre 2 entrées
  Q_MIN: 50, // quality minimum pour ouvrir
  FEE_PER_SIDE: 0.0004, // 0.04% taker par leg (info P&L)
};

// ------------------------------------------------------------------
// ÉTAT GLOBAL
// ------------------------------------------------------------------
const state = {
  running: false, // démarre en PAUSE — clic "Lancer" requis
  mode: MODE,
  asset: ASSET,
  capital: CAPITAL_START,
  capitalStart: CAPITAL_START,
  price: 0,
  prices: [], // historique pour EMA
  position: null, // { side, entry, qty, stake, lev, sl, tp, peak, trailing, openedAt }
  lastEntryAt: 0,
  trades: [], // journal
  stats: { wins: 0, losses: 0, gross: 0, fees: 0, net: 0 },
  log: [],
};

function logLine(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  state.log.unshift(line);
  if (state.log.length > 200) state.log.pop();
  broadcast({ type: 'log', line });
}

// ------------------------------------------------------------------
// SIGNATURE HMAC-SHA256 + APPELS REST SIGNÉS
// ------------------------------------------------------------------
function sign(query) {
  return crypto.createHmac('sha256', API_SECRET).update(query).digest('hex');
}

async function signedRequest(method, path, params = {}) {
  const timestamp = Date.now();
  const query = new URLSearchParams({ ...params, timestamp, recvWindow: 5000 }).toString();
  const signature = sign(query);
  const url = `${REST_BASE}${path}?${query}&signature=${signature}`;

  const res = await fetch(url, {
    method,
    headers: { 'X-MBX-APIKEY': API_KEY },
  });
  const data = await res.json();
  if (!res.ok) {
    throw new Error(`Binance ${res.status}: ${JSON.stringify(data)}`);
  }
  return data;
}

async function publicRequest(path, params = {}) {
  const query = new URLSearchParams(params).toString();
  const url = `${REST_BASE}${path}${query ? '?' + query : ''}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Binance public ${res.status}`);
  return res.json();
}

// ------------------------------------------------------------------
// PRÉCISION SYMBOLE (stepSize / tickSize)
// ------------------------------------------------------------------
let SYMBOL_INFO = { stepSize: 0.001, tickSize: 0.1, qtyPrecision: 3, pricePrecision: 1 };

async function loadSymbolInfo() {
  try {
    const info = await publicRequest('/fapi/v1/exchangeInfo');
    const sym = info.symbols.find((s) => s.symbol === ASSET);
    if (!sym) return;
    const lot = sym.filters.find((f) => f.filterType === 'LOT_SIZE');
    const price = sym.filters.find((f) => f.filterType === 'PRICE_FILTER');
    SYMBOL_INFO.stepSize = parseFloat(lot.stepSize);
    SYMBOL_INFO.tickSize = parseFloat(price.tickSize);
    SYMBOL_INFO.qtyPrecision = sym.quantityPrecision;
    SYMBOL_INFO.pricePrecision = sym.pricePrecision;
    logLine(`Symbol ${ASSET}: stepSize=${SYMBOL_INFO.stepSize} tickSize=${SYMBOL_INFO.tickSize}`);
  } catch (e) {
    logLine(`⚠️ loadSymbolInfo: ${e.message}`);
  }
}

function roundQty(q) {
  const p = SYMBOL_INFO.qtyPrecision;
  return parseFloat(q.toFixed(p));
}
function roundPrice(p) {
  return parseFloat(p.toFixed(SYMBOL_INFO.pricePrecision));
}

// ------------------------------------------------------------------
// INDICATEURS
// ------------------------------------------------------------------
function ema(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  let e = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < values.length; i++) {
    e = values[i] * k + e * (1 - k);
  }
  return e;
}

function computeSignal() {
  const p = state.prices;
  if (p.length < STRAT.EMA_SLOW + 2) return null;

  const emaFast = ema(p, STRAT.EMA_FAST);
  const emaSlow = ema(p, STRAT.EMA_SLOW);
  if (emaFast == null || emaSlow == null) return null;

  const last = p[p.length - 1];
  const prev = p[p.length - 6] || p[0];
  const momentum = (last - prev) / prev; // variation récente

  const bull = emaFast > emaSlow && momentum > 0;
  const bear = emaFast < emaSlow && momentum < 0;
  if (!bull && !bear) return null;

  // Quality score 0-100
  const spread = Math.abs(emaFast - emaSlow) / emaSlow; // écart EMA
  const emaScore = Math.min(30, spread * 5000); // 0-30
  const momScore = Math.min(50, Math.abs(momentum) * 8000); // 0-50
  const trendBonus = bull && momentum > 0.001 ? 20 : bear && momentum < -0.001 ? 20 : 10;
  const quality = Math.round(emaScore + momScore + trendBonus);

  return { side: bull ? 'BUY' : 'SELL', quality, emaFast, emaSlow, momentum };
}

function sizing(quality) {
  let stakePct, lev;
  if (quality >= 80) {
    stakePct = STRAT.STAKE_HIGH;
    lev = STRAT.LEV_HIGH;
  } else if (quality >= 55) {
    stakePct = STRAT.STAKE_MED;
    lev = STRAT.LEV_MED;
  } else {
    stakePct = STRAT.STAKE_LOW;
    lev = STRAT.LEV_LOW;
  }
  return { stake: state.capital * stakePct, lev };
}

// ------------------------------------------------------------------
// ORDRES BINANCE
// ------------------------------------------------------------------
async function setLeverage(lev) {
  try {
    await signedRequest('POST', '/fapi/v1/leverage', { symbol: ASSET, leverage: lev });
  } catch (e) {
    logLine(`⚠️ setLeverage: ${e.message}`);
  }
}

async function marketOrder(side, qty, reduceOnly = false) {
  const params = { symbol: ASSET, side, type: 'MARKET', quantity: qty };
  if (reduceOnly) params.reduceOnly = 'true';
  return signedRequest('POST', '/fapi/v1/order', params);
}

// Lit la position réelle sur Binance (source de vérité)
async function fetchBinancePosition() {
  try {
    const data = await signedRequest('GET', '/fapi/v2/positionRisk', { symbol: ASSET });
    const pos = Array.isArray(data) ? data.find((x) => x.symbol === ASSET) : null;
    if (!pos) return null;
    const amt = parseFloat(pos.positionAmt);
    if (Math.abs(amt) < SYMBOL_INFO.stepSize) return null; // pas de position réelle (ignore dust)
    return {
      side: amt > 0 ? 'BUY' : 'SELL',
      qty: Math.abs(amt),
      entry: parseFloat(pos.entryPrice),
      unrealized: parseFloat(pos.unRealizedProfit),
    };
  } catch (e) {
    logLine(`⚠️ fetchBinancePosition: ${e.message}`);
    return null;
  }
}

// ------------------------------------------------------------------
// LOGIQUE D'OUVERTURE / FERMETURE
// ------------------------------------------------------------------
async function openPosition(signal) {
  const now = Date.now();
  if (state.position) return;
  if (now - state.lastEntryAt < STRAT.MIN_GAP_MS) return;
  if (signal.quality < STRAT.Q_MIN) return;

  const { stake, lev } = sizing(signal.quality);
  const notional = stake * lev;
  const qty = roundQty(notional / state.price);
  if (qty <= 0) return;

  await setLeverage(lev);

  try {
    await marketOrder(signal.side, qty);
  } catch (e) {
    logLine(`❌ Ouverture échouée: ${e.message}`);
    return;
  }

  const entry = state.price;
  const slPrice =
    signal.side === 'BUY' ? entry * (1 - STRAT.SL_PCT) : entry * (1 + STRAT.SL_PCT);
  const tpPrice =
    signal.side === 'BUY' ? entry * (1 + STRAT.TP_PCT) : entry * (1 - STRAT.TP_PCT);

  state.position = {
    side: signal.side,
    entry,
    qty,
    stake,
    lev,
    quality: signal.quality,
    sl: slPrice,
    tp: tpPrice,
    peak: entry,
    trailing: false,
    openedAt: now,
  };
  state.lastEntryAt = now;

  logLine(
    `🟢 OUVERTURE ${signal.side} ${ASSET} qty=${qty} @ ${entry.toFixed(2)} ` +
      `lev=${lev}x Q=${signal.quality} SL=${slPrice.toFixed(2)} TP=${tpPrice.toFixed(2)}`
  );
  broadcast({ type: 'position', position: state.position });
}

async function closePosition(reason) {
  const pos = state.position;
  if (!pos) return;

  const closeSide = pos.side === 'BUY' ? 'SELL' : 'BUY';
  try {
    await marketOrder(closeSide, pos.qty, true);
  } catch (e) {
    logLine(`❌ Fermeture échouée: ${e.message}`);
    return;
  }

  const exit = state.price;
  const dir = pos.side === 'BUY' ? 1 : -1;
  const pnlPct = ((exit - pos.entry) / pos.entry) * dir;
  const gross = pnlPct * pos.stake * pos.lev;
  const fees = pos.stake * pos.lev * STRAT.FEE_PER_SIDE * 2;
  const net = gross - fees;

  state.capital += net;
  state.stats.gross += gross;
  state.stats.fees += fees;
  state.stats.net += net;
  if (net >= 0) state.stats.wins++;
  else state.stats.losses++;

  const trade = {
    side: pos.side,
    entry: pos.entry,
    exit,
    qty: pos.qty,
    lev: pos.lev,
    quality: pos.quality,
    pnlPct: (pnlPct * 100).toFixed(2),
    gross: gross.toFixed(2),
    fees: fees.toFixed(2),
    net: net.toFixed(2),
    reason,
    closedAt: Date.now(),
  };
  state.trades.unshift(trade);
  if (state.trades.length > 100) state.trades.pop();

  logLine(
    `🔴 FERMETURE ${pos.side} @ ${exit.toFixed(2)} | ${reason} | ` +
      `net=${net.toFixed(2)}$ | capital=${state.capital.toFixed(2)}$`
  );

  state.position = null;
  broadcast({ type: 'trade', trade, stats: state.stats, capital: state.capital });

  // Kill switch
  if (state.capital <= state.capitalStart * (1 - STRAT.KILL_PCT)) {
    state.running = false;
    logLine(`🛑 KILL SWITCH activé — capital ${state.capital.toFixed(2)}$. Bot arrêté.`);
    broadcast({ type: 'status', running: false });
  }
}

// Gestion SL / TP / trailing à chaque tick de prix
function managePosition() {
  const pos = state.position;
  if (!pos) return;
  const px = state.price;
  const dir = pos.side === 'BUY' ? 1 : -1;
  const pnlPct = ((px - pos.entry) / pos.entry) * dir;

  // Mise à jour du pic (dans le sens favorable uniquement)
  if (pos.side === 'BUY' && px > pos.peak) pos.peak = px;
  if (pos.side === 'SELL' && px < pos.peak) pos.peak = px;

  // Activation du trailing une fois le TP atteint
  if (!pos.trailing && pnlPct >= STRAT.TP_PCT) {
    pos.trailing = true;
    logLine(`📈 TP atteint (+${(pnlPct * 100).toFixed(2)}%) — trailing activé`);
  }

  if (pos.trailing) {
    // Fermeture si recul de TRAIL_PCT depuis le pic
    const drawFromPeak =
      pos.side === 'BUY'
        ? (pos.peak - px) / pos.peak
        : (px - pos.peak) / pos.peak;
    if (drawFromPeak >= STRAT.TRAIL_PCT) {
      closePosition('TRAILING');
      return;
    }
  } else {
    // SL fixe avant activation du trailing
    if (pnlPct <= -STRAT.SL_PCT) {
      closePosition('STOP-LOSS');
      return;
    }
  }
}

// ------------------------------------------------------------------
// BOUCLE PRINCIPALE
// ------------------------------------------------------------------
async function tradingTick() {
  if (!state.running || state.price <= 0) return;

  // 1. Gérer la position en cours (SL/TP/trailing)
  managePosition();

  // 2. Chercher une nouvelle entrée si pas de position
  if (!state.position) {
    const signal = computeSignal();
    if (signal) {
      await openPosition(signal);
    }
  }
}

// Réconciliation périodique : Binance = vérité
async function reconcile() {
  if (!API_KEY || !API_SECRET) return;
  const real = await fetchBinancePosition();
  if (!real && state.position) {
    logLine('🔄 Réconciliation : position fermée côté Binance, on nettoie l\'état local.');
    state.position = null;
    broadcast({ type: 'position', position: null });
  }
}

// ------------------------------------------------------------------
// WEBSOCKET PRIX BINANCE
// ------------------------------------------------------------------
let priceWs = null;
function connectPriceStream() {
  const stream = `${ASSET.toLowerCase()}@markPrice@1s`;
  const url = `${WS_BASE}/ws/${stream}`;
  priceWs = new WebSocket(url);

  priceWs.on('open', () => logLine(`🔌 WebSocket prix connecté (${MODE}) ${ASSET}`));
  priceWs.on('message', (raw) => {
    try {
      const d = JSON.parse(raw);
      const p = parseFloat(d.p || d.markPrice);
      if (p > 0) {
        state.price = p;
        state.prices.push(p);
        if (state.prices.length > 500) state.prices.shift();
        broadcast({ type: 'price', price: p });
        tradingTick();
      }
    } catch (e) {
      /* ignore */
    }
  });
  priceWs.on('close', () => {
    logLine('⚠️ WebSocket prix fermé — reconnexion dans 3s');
    setTimeout(connectPriceStream, 3000);
  });
  priceWs.on('error', (e) => logLine(`⚠️ WS error: ${e.message}`));
}

// ------------------------------------------------------------------
// SERVEUR HTTP + WEBSOCKET DASHBOARD
// ------------------------------------------------------------------
const clients = new Set();
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const c of clients) {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/dashboard') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(DASHBOARD_HTML);
    return;
  }
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, mode: MODE, running: state.running }));
    return;
  }
  if (req.url === '/state') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(snapshot()));
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

function snapshot() {
  return {
    mode: state.mode,
    asset: state.asset,
    running: state.running,
    capital: state.capital,
    capitalStart: state.capitalStart,
    price: state.price,
    position: state.position,
    stats: state.stats,
    trades: state.trades.slice(0, 30),
    log: state.log.slice(0, 50),
  };
}

const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: 'snapshot', data: snapshot() }));

  ws.on('message', async (raw) => {
    let cmd;
    try {
      cmd = JSON.parse(raw);
    } catch {
      return;
    }
    if (cmd.action === 'start') {
      state.running = true;
      logLine('▶️ Bot LANCÉ');
      broadcast({ type: 'status', running: true });
    } else if (cmd.action === 'stop') {
      state.running = false;
      logLine('⏸️ Bot EN PAUSE');
      broadcast({ type: 'status', running: false });
    } else if (cmd.action === 'closeAll') {
      if (state.position) await closePosition('MANUEL');
      logLine('🧹 Fermeture manuelle de toutes les positions');
    }
  });

  ws.on('close', () => clients.delete(ws));
});

// ------------------------------------------------------------------
// DASHBOARD HTML (affichage uniquement, aucune logique de trading)
// ------------------------------------------------------------------
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Itachi Server — Dashboard</title>
<style>
  :root{--bg:#0b0e14;--card:#151a23;--accent:#3b82f6;--green:#22c55e;--red:#ef4444;--txt:#e5e7eb;--mut:#94a3b8}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--txt);font-family:system-ui,-apple-system,sans-serif;padding:16px}
  h1{font-size:18px;margin-bottom:4px}
  .mut{color:var(--mut);font-size:13px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:16px 0}
  .card{background:var(--card);border-radius:10px;padding:14px}
  .card .k{color:var(--mut);font-size:12px;text-transform:uppercase}
  .card .v{font-size:22px;font-weight:700;margin-top:4px}
  .green{color:var(--green)}.red{color:var(--red)}
  .controls{display:flex;gap:8px;margin:16px 0}
  button{background:var(--accent);color:#fff;border:0;border-radius:8px;padding:10px 18px;font-size:14px;font-weight:600;cursor:pointer}
  button.stop{background:#475569}button.danger{background:var(--red)}
  table{width:100%;border-collapse:collapse;font-size:13px;margin-top:8px}
  th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #1f2630}
  th{color:var(--mut);font-weight:500}
  .log{background:#0d1117;border-radius:8px;padding:10px;font-family:monospace;font-size:11px;max-height:200px;overflow:auto;color:#9aa4b2}
  .badge{display:inline-block;padding:2px 8px;border-radius:6px;font-size:11px;font-weight:600}
  .on{background:rgba(34,197,94,.15);color:var(--green)}
  .off{background:rgba(148,163,184,.15);color:var(--mut)}
  .sec{margin-top:20px}
</style></head>
<body>
  <h1>⚔️ Itachi Server <span id="mode" class="badge off"></span></h1>
  <div class="mut" id="asset"></div>

  <div class="controls">
    <button id="start">▶ Lancer</button>
    <button id="stop" class="stop">⏸ Pause</button>
    <button id="closeAll" class="danger">🧹 Tout fermer</button>
  </div>

  <div class="grid">
    <div class="card"><div class="k">Prix</div><div class="v" id="price">—</div></div>
    <div class="card"><div class="k">Capital</div><div class="v" id="capital">—</div></div>
    <div class="card"><div class="k">P&L net</div><div class="v" id="net">—</div></div>
    <div class="card"><div class="k">Win rate</div><div class="v" id="wr">—</div></div>
    <div class="card"><div class="k">Trades</div><div class="v" id="ntrades">—</div></div>
  </div>

  <div class="sec"><strong>Position ouverte</strong>
    <table><tbody id="pos"><tr><td class="mut">Aucune position</td></tr></tbody></table>
  </div>

  <div class="sec"><strong>Journal des trades</strong>
    <table><thead><tr><th>Sens</th><th>Entrée</th><th>Sortie</th><th>P&L %</th><th>Net $</th><th>Raison</th></tr></thead>
    <tbody id="trades"></tbody></table>
  </div>

  <div class="sec"><strong>Logs</strong><div class="log" id="log"></div></div>

<script>
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(proto + '://' + location.host);
  const $ = id => document.getElementById(id);

  function fmt(n,d=2){return Number(n).toLocaleString('fr-FR',{minimumFractionDigits:d,maximumFractionDigits:d})}
  function render(s){
    $('mode').textContent = s.mode.toUpperCase();
    $('mode').className = 'badge ' + (s.running ? 'on':'off');
    $('asset').textContent = s.asset + (s.running ? ' — EN MARCHE' : ' — EN PAUSE');
    $('price').textContent = s.price ? fmt(s.price) : '—';
    $('capital').textContent = '$' + fmt(s.capital);
    const net = s.stats.net;
    $('net').textContent = (net>=0?'+':'') + '$' + fmt(net);
    $('net').className = 'v ' + (net>=0?'green':'red');
    const tot = s.stats.wins + s.stats.losses;
    $('wr').textContent = tot ? Math.round(s.stats.wins/tot*100)+'%' : '—';
    $('ntrades').textContent = tot;
    // position
    const pb = $('pos');
    if(s.position){const p=s.position;
      pb.innerHTML = '<tr><td>'+p.side+'</td><td>entrée '+fmt(p.entry)+'</td><td>SL '+fmt(p.sl)+'</td><td>TP '+fmt(p.tp)+'</td><td>'+p.lev+'x Q'+p.quality+'</td></tr>';
    } else { pb.innerHTML = '<tr><td class="mut">Aucune position</td></tr>'; }
    // trades
    $('trades').innerHTML = (s.trades||[]).map(t=>
      '<tr><td>'+t.side+'</td><td>'+fmt(t.entry)+'</td><td>'+fmt(t.exit)+'</td><td class="'+(t.net>=0?'green':'red')+'">'+t.pnlPct+'%</td><td class="'+(t.net>=0?'green':'red')+'">'+t.net+'</td><td class="mut">'+t.reason+'</td></tr>'
    ).join('');
    // log
    $('log').innerHTML = (s.log||[]).join('<br>');
  }

  let snap = null;
  ws.onmessage = e => {
    const m = JSON.parse(e.data);
    if(m.type==='snapshot'){ snap=m.data; render(snap); }
    else if(snap){
      if(m.type==='price') snap.price=m.price;
      if(m.type==='status') snap.running=m.running;
      if(m.type==='trade'){ snap.trades.unshift(m.trade); snap.stats=m.stats; snap.capital=m.capital; }
      if(m.type==='position') snap.position=m.position;
      if(m.type==='log'){ snap.log.unshift(m.line); if(snap.log.length>50)snap.log.pop(); }
      render(snap);
    }
  };
  $('start').onclick = ()=> ws.send(JSON.stringify({action:'start'}));
  $('stop').onclick = ()=> ws.send(JSON.stringify({action:'stop'}));
  $('closeAll').onclick = ()=> ws.send(JSON.stringify({action:'closeAll'}));
</script>
</body></html>`;

// ------------------------------------------------------------------
// DÉMARRAGE
// ------------------------------------------------------------------
async function start() {
  logLine(`🚀 Itachi Server — mode ${MODE.toUpperCase()} — asset ${ASSET} — capital $${CAPITAL_START}`);
  if (!API_KEY || !API_SECRET) {
    logLine('⚠️ BINANCE_API_KEY / BINANCE_API_SECRET manquants — le bot tourne en lecture seule.');
  }
  await loadSymbolInfo();
  connectPriceStream();
  setInterval(reconcile, 15000); // réconciliation toutes les 15s
  server.listen(PORT, () => logLine(`🌐 Dashboard sur le port ${PORT}`));
}

start();
