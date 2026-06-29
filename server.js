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
  MAX_TRANCHES: 5, // pyramiding : nombre max de tranches sur la position BTC
  MIN_GAP_MS: 90000, // 90s (1min30) minimum entre 2 tranches
  Q_MIN: 50, // quality minimum pour ouvrir/ajouter
  FEE_PER_SIDE: 0.0004, // 0.04% taker par leg (info P&L)
  // Calibrage du signal (ajustable selon les conditions de marché)
  MOM_WINDOW: 20, // nb de ticks pour mesurer le momentum (~20s)
  MOM_MULT: 25000, // sensibilité du score momentum
  EMA_MULT: 12000, // sensibilité du score écart EMA
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
  // Indicateurs live (calculés à chaque tick, même hors signal)
  indicators: {
    emaFast: null,
    emaSlow: null,
    momentum: null,
    quality: null,
    bias: 'NEUTRE', // LONG / SHORT / NEUTRE
    nextLev: null,
  },
  peakCapital: CAPITAL_START, // pour le drawdown max
  maxDrawdown: 0,
  // Analyse multi-timeframe (rafraîchie périodiquement depuis les vraies bougies Binance)
  mtf: {
    alignNorm: null,
    alignScore: 0,
    trends: {},
    rsi1m: null,
    support: null,
    resistance: null,
    updatedAt: 0,
  },
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
  const prev = p[p.length - STRAT.MOM_WINDOW] || p[0];
  const momentum = (last - prev) / prev; // variation sur la fenêtre

  const bull = emaFast > emaSlow && momentum > 0;
  const bear = emaFast < emaSlow && momentum < 0;
  if (!bull && !bear) return null;

  // Score de base (EMA + momentum) — la partie MTF/RSI/SR est ajoutée dans tradingTick
  const spread = Math.abs(emaFast - emaSlow) / emaSlow;
  const emaScore = Math.min(20, spread * STRAT.EMA_MULT); // 0-20
  const momScore = Math.min(20, Math.abs(momentum) * STRAT.MOM_MULT); // 0-20
  const baseQ = emaScore + momScore;

  return { side: bull ? 'BUY' : 'SELL', baseQ, emaScore, momScore, emaFast, emaSlow, momentum };
}

// Met à jour les indicateurs live affichés (toujours, même sans signal exploitable)
function updateIndicators() {
  const p = state.prices;
  if (p.length < STRAT.EMA_SLOW + 2) return;
  const emaFast = ema(p, STRAT.EMA_FAST);
  const emaSlow = ema(p, STRAT.EMA_SLOW);
  if (emaFast == null || emaSlow == null) return;

  const last = p[p.length - 1];
  const prev = p[p.length - STRAT.MOM_WINDOW] || p[0];
  const momentum = (last - prev) / prev;

  let bias = 'NEUTRE';
  if (emaFast > emaSlow && momentum > 0) bias = 'LONG';
  else if (emaFast < emaSlow && momentum < 0) bias = 'SHORT';

  // Score complet pour affichage (même logique que le scoring de trade)
  const sig = bias === 'NEUTRE' ? null : { side: bias === 'LONG' ? 'BUY' : 'SELL', momentum };
  const q = sig ? scoreQuality(sig) : null;
  const { lev } = sizing(q != null ? q.quality : 0);

  state.indicators = {
    emaFast,
    emaSlow,
    momentum,
    rsi: state.mtf.rsi1m,
    quality: q ? q.quality : null,
    breakdown: q ? q.breakdown : null,
    mtfAlign: state.mtf.alignScore,
    mtfTrends: state.mtf.trends,
    support: state.mtf.support,
    resistance: state.mtf.resistance,
    bias,
    nextLev: lev,
  };
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
// ANALYSE MULTI-TIMEFRAME (vraies bougies Binance) + RSI + S/R
// ------------------------------------------------------------------
const TIMEFRAMES = ['1m', '3m', '5m', '15m', '1h'];
// Poids de chaque TF dans l'alignement (les TF longs comptent plus)
const TF_WEIGHTS = { '1m': 1, '3m': 1, '5m': 1.5, '15m': 2, '1h': 2.5 };

function emaFromCloses(closes, period) {
  if (closes.length < period) return null;
  const k = 2 / (period + 1);
  let e = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < closes.length; i++) e = closes[i] * k + e * (1 - k);
  return e;
}

function rsi(closes, period = 14) {
  if (closes.length < period + 1) return null;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff >= 0) gains += diff; else losses -= diff;
  }
  const avgG = gains / period, avgL = losses / period;
  if (avgL === 0) return 100;
  const rs = avgG / avgL;
  return 100 - 100 / (1 + rs);
}

// Récupère les bougies d'un timeframe et en déduit tendance + S/R
async function fetchTF(tf) {
  try {
    const kl = await publicRequest('/fapi/v1/klines', { symbol: ASSET, interval: tf, limit: 50 });
    const closes = kl.map((c) => parseFloat(c[4]));
    const highs = kl.map((c) => parseFloat(c[2]));
    const lows = kl.map((c) => parseFloat(c[3]));
    const ef = emaFromCloses(closes, 9);
    const es = emaFromCloses(closes, 21);
    let trend = 0; // +1 haussier, -1 baissier, 0 neutre
    if (ef != null && es != null) trend = ef > es ? 1 : ef < es ? -1 : 0;
    // Support/résistance = plus bas/haut récents (20 dernières bougies)
    const recentHighs = highs.slice(-20);
    const recentLows = lows.slice(-20);
    return {
      trend,
      resistance: Math.max(...recentHighs),
      support: Math.min(...recentLows),
      lastClose: closes[closes.length - 1],
      closes,
    };
  } catch (e) {
    return null;
  }
}

// Rafraîchit l'analyse MTF complète (appelée périodiquement, pas à chaque tick)
async function refreshMTF() {
  if (state.price <= 0) return;
  const results = {};
  for (const tf of TIMEFRAMES) {
    const r = await fetchTF(tf);
    if (r) results[tf] = r;
  }
  if (!Object.keys(results).length) return;

  // Score d'alignement : somme pondérée des tendances, normalisée sur [-1, 1]
  let weighted = 0, totalW = 0;
  const trends = {};
  for (const tf of TIMEFRAMES) {
    if (!results[tf]) continue;
    const w = TF_WEIGHTS[tf];
    weighted += results[tf].trend * w;
    totalW += w;
    trends[tf] = results[tf].trend;
  }
  const alignNorm = totalW ? weighted / totalW : 0; // -1 (tout baissier) à +1 (tout haussier)

  // RSI sur 1m
  const rsi1m = results['1m'] ? rsi(results['1m'].closes, 14) : null;

  // S/R les plus pertinents : 15m et 1h
  const sr = results['15m'] || results['1h'];

  state.mtf = {
    alignNorm, // direction et force de l'alignement
    alignScore: Math.abs(alignNorm), // 0 à 1
    trends, // tendance par TF (pour affichage)
    rsi1m,
    support: sr ? sr.support : null,
    resistance: sr ? sr.resistance : null,
    updatedAt: Date.now(),
  };
}

// ------------------------------------------------------------------
// SCORING DU SIGNAL — combine base (EMA+mom) + RSI + MTF + S/R
// ------------------------------------------------------------------
function scoreQuality(signal) {
  const p = state.prices;
  const emaFast = ema(p, STRAT.EMA_FAST);
  const emaSlow = ema(p, STRAT.EMA_SLOW);
  const spread = emaSlow ? Math.abs(emaFast - emaSlow) / emaSlow : 0;
  const emaScore = Math.min(20, spread * STRAT.EMA_MULT); // 0-20
  const momScore = Math.min(20, Math.abs(signal.momentum) * STRAT.MOM_MULT); // 0-20

  const isLong = signal.side === 'BUY';
  const mtf = state.mtf;

  // RSI : favorise l'achat en survente, la vente en surachat (0-15)
  let rsiScore = 0;
  if (mtf.rsi1m != null) {
    if (isLong) rsiScore = mtf.rsi1m < 30 ? 15 : mtf.rsi1m < 45 ? 10 : mtf.rsi1m < 60 ? 5 : 0;
    else rsiScore = mtf.rsi1m > 70 ? 15 : mtf.rsi1m > 55 ? 10 : mtf.rsi1m > 40 ? 5 : 0;
  }

  // Alignement MTF (0-30) — LE GROS BOOST quand les TF s'accordent dans le bon sens
  let mtfScore = 0;
  if (mtf.alignNorm != null) {
    const aligned = isLong ? mtf.alignNorm : -mtf.alignNorm; // >0 si le marché va dans notre sens
    mtfScore = Math.max(0, Math.min(30, aligned * 30));
  }

  // Position vs Support/Résistance (0-15)
  let srScore = 0;
  const px = state.price;
  if (mtf.support && mtf.resistance && px > 0) {
    const range = mtf.resistance - mtf.support;
    if (range > 0) {
      const posInRange = (px - mtf.support) / range; // 0 = sur support, 1 = sur résistance
      // LONG : bonus si proche du support (marge de hausse). SHORT : si proche résistance.
      if (isLong) srScore = posInRange < 0.4 ? 15 : posInRange < 0.6 ? 8 : 2;
      else srScore = posInRange > 0.6 ? 15 : posInRange > 0.4 ? 8 : 2;
    }
  }

  const quality = Math.round(emaScore + momScore + rsiScore + mtfScore + srScore);
  return {
    quality,
    breakdown: {
      ema: Math.round(emaScore),
      mom: Math.round(momScore),
      rsi: Math.round(rsiScore),
      mtf: Math.round(mtfScore),
      sr: Math.round(srScore),
    },
  };
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
// OUVERTURE / AJOUT DE TRANCHE (pyramiding)
// ------------------------------------------------------------------
// Recalcule le prix d'entrée moyen pondéré + SL/TP global de la position
function recomputePosition() {
  const pos = state.position;
  if (!pos || !pos.tranches.length) return;
  let totalQty = 0;
  let weighted = 0;
  let totalStake = 0;
  for (const t of pos.tranches) {
    totalQty += t.qty;
    weighted += t.entry * t.qty;
    totalStake += t.stake;
  }
  pos.qty = roundQty(totalQty);
  pos.avgEntry = weighted / totalQty;
  pos.stake = totalStake;
  // SL/TP global basés sur le prix d'entrée moyen
  pos.sl =
    pos.side === 'BUY'
      ? pos.avgEntry * (1 - STRAT.SL_PCT)
      : pos.avgEntry * (1 + STRAT.SL_PCT);
  pos.tp =
    pos.side === 'BUY'
      ? pos.avgEntry * (1 + STRAT.TP_PCT)
      : pos.avgEntry * (1 - STRAT.TP_PCT);
}

async function openOrAddTranche(signal) {
  const now = Date.now();

  // Si une position existe déjà : on n'ajoute que dans le MÊME sens
  if (state.position && state.position.side !== signal.side) return;

  // Délai minimum entre 2 tranches
  if (now - state.lastEntryAt < STRAT.MIN_GAP_MS) return;

  // Score de qualité complet : EMA + momentum + RSI + alignement MTF + S/R
  const q = scoreQuality(signal);
  if (q.quality < STRAT.Q_MIN) return;

  // Limite de tranches atteinte
  if (state.position && state.position.tranches.length >= STRAT.MAX_TRANCHES) return;

  const { stake, lev } = sizing(q.quality);
  const notional = stake * lev;
  const qty = roundQty(notional / state.price);
  if (qty <= 0) return;

  // Le levier est fixé sur la 1re tranche (Binance applique 1 levier par symbole)
  const useLev = state.position ? state.position.lev : lev;
  if (!state.position) await setLeverage(useLev);

  try {
    await marketOrder(signal.side, qty);
  } catch (e) {
    logLine(`❌ Entrée échouée: ${e.message}`);
    return;
  }

  const entry = state.price;
  const tranche = {
    n: state.position ? state.position.tranches.length + 1 : 1,
    side: signal.side,
    entry,
    qty,
    stake,
    quality: q.quality,
    breakdown: q.breakdown,
    openedAt: now,
  };

  if (!state.position) {
    // Première tranche → création de la position
    state.position = {
      side: signal.side,
      lev: useLev,
      tranches: [tranche],
      qty: 0,
      avgEntry: 0,
      stake: 0,
      sl: 0,
      tp: 0,
      peak: entry,
      trailing: false,
      openedAt: now,
    };
  } else {
    // Tranche supplémentaire → pyramiding
    state.position.tranches.push(tranche);
  }

  recomputePosition();
  state.lastEntryAt = now;

  logLine(
    `🟢 ${tranche.n === 1 ? 'OUVERTURE' : 'TRANCHE #' + tranche.n} ${signal.side} ${ASSET} ` +
      `qty=${qty} @ ${entry.toFixed(2)} | pos totale=${state.position.qty} ` +
      `entrée moy=${state.position.avgEntry.toFixed(2)} ` +
      `SL=${state.position.sl.toFixed(2)} TP=${state.position.tp.toFixed(2)}`
  );
  broadcast({ type: 'position', position: livePosition() });
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
  const pnlPct = ((exit - pos.avgEntry) / pos.avgEntry) * dir;
  const gross = pnlPct * pos.stake * pos.lev;
  const fees = pos.stake * pos.lev * STRAT.FEE_PER_SIDE * 2;
  const net = gross - fees;

  state.capital += net;
  state.stats.gross += gross;
  state.stats.fees += fees;
  state.stats.net += net;
  if (net >= 0) state.stats.wins++;
  else state.stats.losses++;

  // Drawdown max (sur le capital)
  if (state.capital > state.peakCapital) state.peakCapital = state.capital;
  const dd = (state.peakCapital - state.capital) / state.peakCapital;
  if (dd > state.maxDrawdown) state.maxDrawdown = dd;

  const durationMs = Date.now() - pos.openedAt;
  // Rendement sur la marge investie (net / stake)
  const rendement = (net / pos.stake) * 100;

  const trade = {
    side: pos.side,
    entry: pos.avgEntry,
    exit,
    qty: pos.qty,
    lev: pos.lev,
    tranches: pos.tranches.length,
    quality: pos.tranches[0] ? pos.tranches[0].quality : null,
    investi: pos.stake.toFixed(2), // montant misé total (marge)
    notional: (pos.stake * pos.lev).toFixed(2),
    pnlPct: (pnlPct * 100).toFixed(2), // variation prix
    rendement: rendement.toFixed(1), // gain/perte rapporté à la mise
    gross: gross.toFixed(2),
    fees: fees.toFixed(2),
    net: net.toFixed(2),
    reason,
    durationMs,
    closedAt: Date.now(),
  };
  state.trades.unshift(trade);
  if (state.trades.length > 100) state.trades.pop();

  logLine(
    `🔴 FERMETURE ${pos.side} ${pos.tranches.length} tranche(s) @ ${exit.toFixed(2)} | ${reason} | ` +
      `net=${net.toFixed(2)}$ | capital=${state.capital.toFixed(2)}$`
  );

  state.position = null;
  const tot = state.stats.wins + state.stats.losses;
  broadcast({
    type: 'trade',
    trade,
    stats: state.stats,
    capital: state.capital,
    winRate: tot ? (state.stats.wins / tot) * 100 : null,
    maxDrawdown: state.maxDrawdown * 100,
    position: null,
  });

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
  const pnlPct = ((px - pos.avgEntry) / pos.avgEntry) * dir;

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

  // 1. Gérer la position en cours (SL/TP/trailing global)
  managePosition();

  // 2. Chercher une entrée (nouvelle position OU tranche supplémentaire de pyramiding)
  const signal = computeSignal();
  if (signal) {
    await openOrAddTranche(signal);
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
        updateIndicators();
        broadcast({
          type: 'tick',
          price: p,
          indicators: state.indicators,
          position: livePosition(),
        });
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

// Position enrichie avec P&L live (unrealized) + détail des tranches
function livePosition() {
  const pos = state.position;
  if (!pos) return null;
  const px = state.price || pos.avgEntry;
  const dir = pos.side === 'BUY' ? 1 : -1;

  // P&L global (sur le prix d'entrée moyen)
  const pnlPct = ((px - pos.avgEntry) / pos.avgEntry) * dir;
  const gross = pnlPct * pos.stake * pos.lev;
  const fees = pos.stake * pos.lev * STRAT.FEE_PER_SIDE * 2;
  const net = gross - fees;

  // Détail par tranche (P&L calculé depuis le prix d'entrée de chaque tranche)
  const tranches = pos.tranches.map((t) => {
    const tPnlPct = ((px - t.entry) / t.entry) * dir;
    const tGross = tPnlPct * t.stake * pos.lev;
    const tFees = t.stake * pos.lev * STRAT.FEE_PER_SIDE * 2;
    return {
      n: t.n,
      entry: t.entry,
      qty: t.qty,
      investi: t.stake,
      quality: t.quality,
      pnlPct: tPnlPct * 100,
      netLive: tGross - tFees,
      openedAt: t.openedAt,
    };
  });

  return {
    side: pos.side,
    avgEntry: pos.avgEntry,
    current: px,
    qty: pos.qty,
    lev: pos.lev,
    nbTranches: pos.tranches.length,
    maxTranches: STRAT.MAX_TRANCHES,
    investi: pos.stake,
    notional: pos.stake * pos.lev,
    sl: pos.sl,
    tp: pos.tp,
    trailing: pos.trailing,
    pnlPct: pnlPct * 100,
    netLive: net,
    openedAt: pos.openedAt,
    tranches,
  };
}

function snapshot() {
  const tot = state.stats.wins + state.stats.losses;
  return {
    mode: state.mode,
    asset: state.asset,
    running: state.running,
    capital: state.capital,
    capitalStart: state.capitalStart,
    price: state.price,
    priceHistory: state.prices.slice(-120), // amorce le graphique
    indicators: state.indicators,
    position: livePosition(),
    stats: state.stats,
    winRate: tot ? (state.stats.wins / tot) * 100 : null,
    maxDrawdown: state.maxDrawdown * 100,
    trades: state.trades.slice(0, 50),
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
<meta name="theme-color" content="#00F5C8">
<title>Itachi — CryptoSignal AI</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root{
    --bg:#070b10;--bg2:#0c1219;--card:#101820;--card2:#0e151d;
    --cyan:#00F5C8;--cyan-dim:rgba(0,245,200,.12);
    --green:#22e58a;--red:#ff5470;--amber:#ffb547;
    --txt:#e7f0ef;--mut:#7c8b95;--line:#1a2530;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:radial-gradient(1200px 600px at 50% -200px,rgba(0,245,200,.06),transparent),var(--bg);
    color:var(--txt);font-family:'Segoe UI',system-ui,-apple-system,sans-serif;padding:14px;max-width:1100px;margin:0 auto}
  .head{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:4px}
  .logo{font-size:19px;font-weight:800;letter-spacing:.3px}
  .logo .c{color:var(--cyan)}
  .badge{display:inline-block;padding:3px 9px;border-radius:6px;font-size:11px;font-weight:700;letter-spacing:.5px}
  .badge.net{background:var(--cyan-dim);color:var(--cyan);border:1px solid rgba(0,245,200,.3)}
  .badge.on{background:rgba(34,229,138,.15);color:var(--green)}
  .badge.off{background:rgba(124,139,149,.15);color:var(--mut)}
  .sub{color:var(--mut);font-size:12px;margin-bottom:14px}
  .controls{display:flex;gap:8px;margin:14px 0}
  button{border:0;border-radius:9px;padding:11px 20px;font-size:14px;font-weight:700;cursor:pointer;transition:.15s;font-family:inherit}
  .btn-go{background:var(--cyan);color:#04221d}.btn-go:hover{box-shadow:0 0 18px rgba(0,245,200,.4)}
  .btn-stop{background:#1c2630;color:var(--txt)}
  .btn-kill{background:rgba(255,84,112,.15);color:var(--red);border:1px solid rgba(255,84,112,.3)}

  /* Bandeau indicateurs */
  .ind-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:1px;
    background:var(--line);border:1px solid var(--line);border-radius:12px;overflow:hidden;margin-bottom:14px}
  .ind{background:var(--card2);padding:11px 13px}
  .ind .k{color:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.6px}
  .ind .v{font-size:16px;font-weight:700;margin-top:3px;font-variant-numeric:tabular-nums}
  .ind .v.cyan{color:var(--cyan)}.ind .v.green{color:var(--green)}.ind .v.red{color:var(--red)}.ind .v.mut{color:var(--mut)}

  /* Cartes stats */
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:14px}
  .card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:13px 15px}
  .card .k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  .card .v{font-size:21px;font-weight:800;margin-top:5px;font-variant-numeric:tabular-nums}
  .green{color:var(--green)}.red{color:var(--red)}.cyan{color:var(--cyan)}.mut{color:var(--mut)}

  .sec-title{font-size:13px;font-weight:700;letter-spacing:.4px;margin:18px 0 8px;color:var(--txt);
    display:flex;align-items:center;gap:8px}
  .sec-title .dot{width:6px;height:6px;border-radius:50%;background:var(--cyan)}

  /* Position en cours */
  .pos-card{background:linear-gradient(180deg,var(--card),var(--card2));border:1px solid var(--line);border-radius:12px;padding:0;overflow:hidden}
  .pos-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(90px,1fr));gap:1px;background:var(--line)}
  .pos-cell{background:var(--card);padding:11px 13px}
  .pos-cell .k{color:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.5px}
  .pos-cell .v{font-size:15px;font-weight:700;margin-top:3px;font-variant-numeric:tabular-nums}
  .empty{padding:16px;color:var(--mut);font-size:13px;text-align:center}
  .pill{display:inline-block;padding:2px 8px;border-radius:5px;font-size:11px;font-weight:700}
  .pill.long{background:rgba(34,229,138,.15);color:var(--green)}
  .pill.short{background:rgba(255,84,112,.15);color:var(--red)}

  /* Pyramiding : tranches */
  .pos-global-title{padding:9px 13px;font-size:11px;font-weight:700;color:var(--cyan);
    background:var(--cyan-dim);border-bottom:1px solid var(--line);letter-spacing:.3px}
  .tranches{display:flex;flex-direction:column;gap:1px;background:var(--line);border-top:1px solid var(--line)}
  .tranche-row{display:flex;align-items:center;gap:14px;flex-wrap:wrap;background:var(--card);padding:9px 13px;font-size:12.5px}
  .tr-badge{display:inline-block;min-width:30px;padding:2px 8px;border-radius:5px;font-size:11px;font-weight:800;
    background:rgba(0,245,200,.12);color:var(--cyan);text-align:center}
  .tr-cell{font-variant-numeric:tabular-nums}
  .trk{color:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.4px;margin-right:3px}

  /* Table historique */
  .table-wrap{border:1px solid var(--line);border-radius:12px;overflow-x:auto;background:var(--card2)}
  table{width:100%;border-collapse:collapse;font-size:12.5px;min-width:760px}
  th,td{text-align:right;padding:9px 12px;border-bottom:1px solid var(--line);white-space:nowrap;font-variant-numeric:tabular-nums}
  th:first-child,td:first-child{text-align:left}th:nth-child(2),td:nth-child(2){text-align:left}
  th{color:var(--mut);font-weight:600;font-size:10.5px;text-transform:uppercase;letter-spacing:.5px;background:var(--card)}
  tbody tr:hover{background:rgba(0,245,200,.03)}

  .log{background:var(--card2);border:1px solid var(--line);border-radius:12px;padding:12px;
    font-family:'SF Mono',Consolas,monospace;font-size:11px;max-height:220px;overflow:auto;color:#8aa0a0;line-height:1.7}

  /* Graphique BTC live */
  .chart-card{background:var(--card2);border:1px solid var(--line);border-radius:12px;padding:14px;margin-bottom:14px;position:relative}
  .chart-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
  .chart-head .pair{font-size:13px;font-weight:700}
  .chart-head .live{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--mut)}
  .chart-head .live .dot{width:7px;height:7px;border-radius:50%;background:var(--cyan);box-shadow:0 0 8px var(--cyan);animation:pulse 1.5s infinite}
  .chart-head .px{font-size:15px;font-weight:800;color:var(--cyan);font-variant-numeric:tabular-nums}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
  .chart-box{position:relative;height:240px}

  /* Bandeau multi-timeframe */
  .mtf-strip{display:flex;flex-wrap:wrap;gap:10px;align-items:center;justify-content:space-between;
    background:var(--card2);border:1px solid var(--line);border-radius:12px;padding:10px 14px;margin-bottom:10px}
  .mtf-trends{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
  .mtf-label{font-size:10px;color:var(--mut);letter-spacing:.6px;margin-right:4px}
  .tf{display:inline-flex;align-items:center;gap:5px;padding:4px 9px;border-radius:7px;background:var(--card);border:1px solid var(--line);font-size:12px}
  .tf b{font-size:11px;color:var(--mut)}
  .tf i{font-style:normal;font-weight:800}
  .tf.up i{color:var(--green)} .tf.down i{color:var(--red)} .tf.flat i{color:var(--mut)}
  .tf.up{border-color:rgba(34,229,138,.3)} .tf.down{border-color:rgba(255,84,112,.3)}
  .mtf-extra{display:flex;gap:16px;flex-wrap:wrap}
  .mtf-item{display:flex;flex-direction:column;font-variant-numeric:tabular-nums}
  .mtf-item b{font-size:14px;font-weight:700}

  /* Décomposition du Q */
  .q-break{display:flex;gap:8px;flex-wrap:wrap;align-items:stretch;margin-bottom:14px}
  .qb{display:flex;flex-direction:column;gap:2px;background:var(--card2);border:1px solid var(--line);
    border-radius:9px;padding:7px 12px;font-variant-numeric:tabular-nums}
  .qb b{font-size:15px;font-weight:800}
  .qb.hl{border-color:rgba(0,245,200,.35);background:var(--cyan-dim)}
  .qb.hl b{color:var(--cyan)}
  .qb.total{margin-left:auto;flex-direction:row;align-items:baseline;gap:7px}
  .qb.total b{font-size:20px;color:var(--cyan)}
</style></head>
<body>
  <div class="head">
    <span class="logo">CryptoSignal<span class="c">AI</span> · Itachi</span>
    <span id="mode" class="badge net">TESTNET</span>
    <span id="run" class="badge off">PAUSE</span>
  </div>
  <div class="sub" id="asset">Prix Binance live · BTC/USDT</div>

  <div class="controls">
    <button id="start" class="btn-go">▶ Démarrer</button>
    <button id="stop" class="btn-stop">⏸ Pause</button>
    <button id="closeAll" class="btn-kill">⏹ Tout fermer</button>
  </div>

  <!-- Bandeau indicateurs live -->
  <div class="ind-strip">
    <div class="ind"><div class="k">Prix réel</div><div class="v cyan" id="i-price">—</div></div>
    <div class="ind"><div class="k">EMA 8</div><div class="v" id="i-emaf">—</div></div>
    <div class="ind"><div class="k">EMA 21</div><div class="v" id="i-emas">—</div></div>
    <div class="ind"><div class="k">Momentum</div><div class="v" id="i-mom">—</div></div>
    <div class="ind"><div class="k">Force signal</div><div class="v" id="i-q">—</div></div>
    <div class="ind"><div class="k">Biais</div><div class="v mut" id="i-bias">NEUTRE</div></div>
    <div class="ind"><div class="k">Levier prochain</div><div class="v" id="i-lev">—</div></div>
  </div>

  <!-- Bandeau multi-timeframe + RSI + S/R -->
  <div class="mtf-strip">
    <div class="mtf-trends">
      <span class="mtf-label">TENDANCES</span>
      <span class="tf" id="tf-1m"><b>1m</b><i>—</i></span>
      <span class="tf" id="tf-3m"><b>3m</b><i>—</i></span>
      <span class="tf" id="tf-5m"><b>5m</b><i>—</i></span>
      <span class="tf" id="tf-15m"><b>15m</b><i>—</i></span>
      <span class="tf" id="tf-1h"><b>1h</b><i>—</i></span>
    </div>
    <div class="mtf-extra">
      <span class="mtf-item"><span class="trk">RSI 1m</span><b id="m-rsi">—</b></span>
      <span class="mtf-item"><span class="trk">Align. MTF</span><b id="m-align">—</b></span>
      <span class="mtf-item"><span class="trk">Support</span><b id="m-sup" class="green">—</b></span>
      <span class="mtf-item"><span class="trk">Résistance</span><b id="m-res" class="red">—</b></span>
    </div>
  </div>

  <!-- Décomposition du Quality score -->
  <div class="q-break" id="q-break">
    <span class="qb"><span class="trk">EMA</span><b id="qb-ema">0</b></span>
    <span class="qb"><span class="trk">Mom.</span><b id="qb-mom">0</b></span>
    <span class="qb"><span class="trk">RSI</span><b id="qb-rsi">0</b></span>
    <span class="qb hl"><span class="trk">MTF</span><b id="qb-mtf">0</b></span>
    <span class="qb"><span class="trk">S/R</span><b id="qb-sr">0</b></span>
    <span class="qb total"><span class="trk">Q total</span><b id="qb-tot">0</b><span class="trk">/ 50 requis</span></span>
  </div>

  <!-- Graphique BTC live (calé sur Binance testnet) -->
  <div class="chart-card">
    <div class="chart-head">
      <span class="pair" id="chart-pair">BTC/USDT — Binance</span>
      <span class="live"><span class="dot"></span>LIVE <span class="px" id="chart-px">—</span></span>
    </div>
    <div class="chart-box"><canvas id="chart"></canvas></div>
  </div>

  <!-- Cartes stats -->
  <div class="grid">
    <div class="card"><div class="k">Capital</div><div class="v" id="s-cap">—</div></div>
    <div class="card"><div class="k">P&L Net</div><div class="v" id="s-net">—</div></div>
    <div class="card"><div class="k">P&L Open</div><div class="v" id="s-open">—</div></div>
    <div class="card"><div class="k">Frais cumulés</div><div class="v mut" id="s-fees">—</div></div>
    <div class="card"><div class="k">Win Rate</div><div class="v" id="s-wr">—</div></div>
    <div class="card"><div class="k">Trades</div><div class="v" id="s-n">0</div></div>
    <div class="card"><div class="k">Gagnés / Perdus</div><div class="v" id="s-wl">0 / 0</div></div>
    <div class="card"><div class="k">Drawdown Max</div><div class="v" id="s-dd">0.0%</div></div>
  </div>

  <!-- Trade en cours -->
  <div class="sec-title"><span class="dot"></span>Trade en cours</div>
  <div class="pos-card"><div id="pos"><div class="empty">Aucune position ouverte</div></div></div>

  <!-- Historique -->
  <div class="sec-title"><span class="dot"></span>Historique — Trades fermés</div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>#</th><th>Sens</th><th>Tranches</th><th>Levier</th><th>Entrée moy.</th><th>Sortie</th>
        <th>Investi</th><th>Gain/Perte</th><th>Rendement</th><th>Frais</th><th>Raison</th><th>Durée</th>
      </tr></thead>
      <tbody id="trades"><tr><td colspan="11" class="mut" style="text-align:center;padding:14px">Aucun trade fermé pour l'instant</td></tr></tbody>
    </table>
  </div>

  <!-- Logs -->
  <div class="sec-title"><span class="dot"></span>Journal du bot</div>
  <div class="log" id="log"></div>

<script>
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(proto + '://' + location.host);
  const $ = id => document.getElementById(id);
  const num = (n,d=2) => Number(n).toLocaleString('fr-FR',{minimumFractionDigits:d,maximumFractionDigits:d});
  const sign = n => (n>=0?'+':'');
  const cls = n => n>=0?'green':'red';

  function dur(ms){
    if(!ms) return '—';
    const s=Math.floor(ms/1000); if(s<60) return s+'s';
    const m=Math.floor(s/60); const r=s%60; return m+'m'+(r?r+'s':'');
  }

  // ---- Graphique BTC live ----
  const MAX_POINTS = 120; // ~2 min d'historique à 1 point/s
  let chart = null;
  let pendingSeed = null;
  function initChart(){
    const el = document.getElementById('chart');
    if(!el) return;
    const ctx = el.getContext('2d');
    const grad = ctx.createLinearGradient(0,0,0,240);
    grad.addColorStop(0,'rgba(0,245,200,.25)');
    grad.addColorStop(1,'rgba(0,245,200,0)');
    chart = new Chart(ctx,{
      type:'line',
      data:{labels:[],datasets:[{
        data:[],borderColor:'#00F5C8',borderWidth:2,
        backgroundColor:grad,fill:true,tension:.25,
        pointRadius:0,pointHoverRadius:4,pointHoverBackgroundColor:'#00F5C8'
      }]},
      options:{
        responsive:true,maintainAspectRatio:false,animation:false,
        interaction:{intersect:false,mode:'index'},
        plugins:{legend:{display:false},tooltip:{
          backgroundColor:'#0c1219',borderColor:'#1a2530',borderWidth:1,
          titleColor:'#7c8b95',bodyColor:'#00F5C8',
          callbacks:{label:c=>' '+Number(c.parsed.y).toLocaleString('fr-FR',{minimumFractionDigits:2,maximumFractionDigits:2})}
        }},
        scales:{
          x:{display:false,grid:{display:false}},
          y:{position:'right',grid:{color:'rgba(26,37,48,.6)'},
            ticks:{color:'#7c8b95',font:{size:10},
              callback:v=>Number(v).toLocaleString('fr-FR',{maximumFractionDigits:0})}}
        }
      }
    });
    if(pendingSeed){ seedChart(pendingSeed); pendingSeed=null; }
  }
  function seedChart(history){
    if(!history || !history.length) return;
    if(!chart){ pendingSeed = history; return; }
    const d = chart.data;
    d.labels = history.map((_,i)=>i+'');
    d.datasets[0].data = history.slice();
    if(d.labels.length>MAX_POINTS){
      d.labels = d.labels.slice(-MAX_POINTS);
      d.datasets[0].data = d.datasets[0].data.slice(-MAX_POINTS);
    }
    chart.update('none');
    $('chart-px').textContent = num(history[history.length-1]);
  }
  function pushPrice(p){
    if(!chart) return;
    const t = new Date().toLocaleTimeString('fr-FR');
    const d = chart.data;
    d.labels.push(t); d.datasets[0].data.push(p);
    if(d.labels.length>MAX_POINTS){ d.labels.shift(); d.datasets[0].data.shift(); }
    chart.update('none');
    $('chart-px').textContent = num(p);
  }

  function setTF(id, trend){
    const el = $(id); if(!el) return;
    const i = el.querySelector('i');
    if(trend>0){ el.className='tf up'; i.textContent='▲ HAUSSE'; }
    else if(trend<0){ el.className='tf down'; i.textContent='▼ BAISSE'; }
    else { el.className='tf flat'; i.textContent='— PLAT'; }
  }

  function renderIndicators(ind, price){
    $('i-price').textContent = price ? num(price) : '—';
    if(!ind){return;}
    $('i-emaf').textContent = ind.emaFast!=null ? num(ind.emaFast) : '—';
    $('i-emas').textContent = ind.emaSlow!=null ? num(ind.emaSlow) : '—';
    if(ind.momentum!=null){
      const m=ind.momentum*100;
      $('i-mom').textContent = sign(m)+m.toFixed(3)+'%';
      $('i-mom').className = 'v '+cls(m);
    }
    $('i-q').textContent = ind.quality!=null ? 'Q'+ind.quality : '—';
    $('i-q').className = 'v '+(ind.quality!=null && ind.quality>=50 ? 'cyan':'mut');
    const b = ind.bias||'NEUTRE';
    $('i-bias').textContent = b;
    $('i-bias').className = 'v '+(b==='LONG'?'green':b==='SHORT'?'red':'mut');
    $('i-lev').textContent = ind.nextLev ? ind.nextLev+'×' : '—';

    // Tendances multi-timeframe
    const tr = ind.mtfTrends||{};
    setTF('tf-1m', tr['1m']||0); setTF('tf-3m', tr['3m']||0); setTF('tf-5m', tr['5m']||0);
    setTF('tf-15m', tr['15m']||0); setTF('tf-1h', tr['1h']||0);

    // RSI / alignement / S/R
    if(ind.rsi!=null){
      $('m-rsi').textContent = ind.rsi.toFixed(0);
      $('m-rsi').className = ind.rsi>70?'red':ind.rsi<30?'green':'';
    }
    $('m-align').textContent = ind.mtfAlign!=null ? Math.round(ind.mtfAlign*100)+'%' : '—';
    $('m-sup').textContent = ind.support!=null ? num(ind.support) : '—';
    $('m-res').textContent = ind.resistance!=null ? num(ind.resistance) : '—';

    // Décomposition du Q
    const bd = ind.breakdown;
    if(bd){
      $('qb-ema').textContent = bd.ema; $('qb-mom').textContent = bd.mom;
      $('qb-rsi').textContent = bd.rsi; $('qb-mtf').textContent = bd.mtf; $('qb-sr').textContent = bd.sr;
      const tot = ind.quality!=null?ind.quality:0;
      $('qb-tot').textContent = tot;
      $('qb-tot').style.color = tot>=50 ? 'var(--green)' : 'var(--cyan)';
    } else {
      ['qb-ema','qb-mom','qb-rsi','qb-mtf','qb-sr','qb-tot'].forEach(id=>$(id).textContent='0');
    }
  }

  function renderPosition(p){
    const el = $('pos');
    if(!p){ el.innerHTML = '<div class="empty">Aucune position ouverte</div>'; return; }
    const sideCls = p.side==='BUY'?'long':'short';
    const sideTxt = p.side==='BUY'?'LONG':'SHORT';
    const net = p.netLive!=null ? p.netLive : 0;

    // Bandeau "Position globale Binance" (la vérité)
    let html =
      '<div class="pos-global">'+
      '<div class="pos-global-title">📊 Position globale Binance — '+p.nbTranches+'/'+p.maxTranches+' tranche(s)</div>'+
      '<div class="pos-row">'+
      '<div class="pos-cell"><div class="k">Sens</div><div class="v"><span class="pill '+sideCls+'">'+sideTxt+'</span></div></div>'+
      '<div class="pos-cell"><div class="k">Levier</div><div class="v">'+p.lev+'×</div></div>'+
      '<div class="pos-cell"><div class="k">Entrée moyenne</div><div class="v">'+num(p.avgEntry)+'</div></div>'+
      '<div class="pos-cell"><div class="k">Prix actuel</div><div class="v">'+num(p.current)+'</div></div>'+
      '<div class="pos-cell"><div class="k">Investi total</div><div class="v">$'+num(p.investi)+'</div></div>'+
      '<div class="pos-cell"><div class="k">Exposition</div><div class="v">$'+num(p.notional)+'</div></div>'+
      '<div class="pos-cell"><div class="k">SL global</div><div class="v red">'+num(p.sl)+'</div></div>'+
      '<div class="pos-cell"><div class="k">TP global</div><div class="v green">'+num(p.tp)+'</div></div>'+
      '<div class="pos-cell"><div class="k">P&L live</div><div class="v '+cls(net)+'">'+sign(net)+'$'+num(net)+' ('+sign(p.pnlPct)+p.pnlPct.toFixed(2)+'%)</div></div>'+
      '</div></div>';

    // Détail des tranches
    if(p.tranches && p.tranches.length){
      html += '<div class="tranches">';
      p.tranches.forEach(t=>{
        const tnet = t.netLive!=null ? t.netLive : 0;
        html +=
          '<div class="tranche-row">'+
          '<span class="tr-badge">#'+t.n+'</span>'+
          '<span class="tr-cell"><span class="trk">Entrée</span> '+num(t.entry)+'</span>'+
          '<span class="tr-cell"><span class="trk">Mise</span> $'+num(t.investi)+'</span>'+
          '<span class="tr-cell"><span class="trk">Q</span> '+t.quality+'</span>'+
          '<span class="tr-cell '+cls(tnet)+'">'+sign(tnet)+'$'+num(tnet)+' ('+sign(t.pnlPct)+t.pnlPct.toFixed(2)+'%)</span>'+
          '</div>';
      });
      html += '</div>';
    }
    el.innerHTML = html;
  }

  function renderTrades(trades){
    const tb = $('trades');
    if(!trades || !trades.length){
      tb.innerHTML = '<tr><td colspan="12" class="mut" style="text-align:center;padding:14px">Aucun trade ferm&eacute; pour l&rsquo;instant</td></tr>';
      return;
    }
    tb.innerHTML = trades.map((t,i)=>{
      const net = Number(t.net);
      const sideCls = t.side==='BUY'?'long':'short';
      const sideTxt = t.side==='BUY'?'LONG':'SHORT';
      return '<tr>'+
        '<td>'+(trades.length-i)+'</td>'+
        '<td><span class="pill '+sideCls+'">'+sideTxt+'</span></td>'+
        '<td>'+(t.tranches||1)+'</td>'+
        '<td>'+t.lev+'×</td>'+
        '<td>'+num(t.entry)+'</td>'+
        '<td>'+num(t.exit)+'</td>'+
        '<td>$'+num(t.investi)+'</td>'+
        '<td class="'+cls(net)+'">'+sign(net)+'$'+num(net)+'</td>'+
        '<td class="'+cls(net)+'">'+sign(Number(t.rendement))+t.rendement+'%</td>'+
        '<td class="mut">$'+num(t.fees)+'</td>'+
        '<td class="mut">'+t.reason+'</td>'+
        '<td class="mut">'+dur(t.durationMs)+'</td>'+
        '</tr>';
    }).join('');
  }

  function renderStats(s){
    $('mode').textContent = (s.mode||'testnet').toUpperCase();
    $('run').textContent = s.running ? 'EN MARCHE' : 'PAUSE';
    $('run').className = 'badge ' + (s.running?'on':'off');
    $('asset').textContent = 'Prix Binance live · ' + (s.asset||'BTCUSDT');
    const pairEl = document.getElementById('chart-pair');
    if(pairEl) pairEl.textContent = (s.asset||'BTCUSDT').replace('USDT','/USDT') + ' — Binance ' + (s.mode||'testnet').toUpperCase();
    $('s-cap').textContent = '$'+num(s.capital);
    const net = s.stats.net;
    $('s-net').textContent = sign(net)+'$'+num(net);
    $('s-net').className = 'v '+cls(net);
    $('s-fees').textContent = '$'+num(s.stats.fees);
    const tot = s.stats.wins + s.stats.losses;
    $('s-wr').textContent = s.winRate!=null ? Math.round(s.winRate)+'%' : (tot?Math.round(s.stats.wins/tot*100)+'%':'—');
    $('s-wr').className = 'v '+(s.winRate>=50?'green':s.winRate!=null?'red':'mut');
    $('s-n').textContent = tot;
    $('s-wl').textContent = s.stats.wins+' / '+s.stats.losses;
    $('s-dd').textContent = (s.maxDrawdown!=null?s.maxDrawdown:0).toFixed(1)+'%';
  }

  function renderOpenPnL(p){
    $('s-open').textContent = p ? (sign(p.netLive)+'$'+num(p.netLive)) : '$0.00';
    $('s-open').className = 'v '+(p?cls(p.netLive):'mut');
  }

  let snap = null;
  ws.onmessage = e => {
    const m = JSON.parse(e.data);
    if(m.type==='snapshot'){
      snap = m.data;
      renderStats(snap); renderIndicators(snap.indicators, snap.price);
      renderPosition(snap.position); renderOpenPnL(snap.position);
      renderTrades(snap.trades); $('log').innerHTML=(snap.log||[]).join('<br>');
      seedChart(snap.priceHistory);
    } else if(snap){
      if(m.type==='tick'){
        snap.price=m.price; snap.indicators=m.indicators; snap.position=m.position;
        renderIndicators(m.indicators, m.price); renderPosition(m.position); renderOpenPnL(m.position);
        pushPrice(m.price);
      }
      if(m.type==='status'){ snap.running=m.running; renderStats(snap); }
      if(m.type==='position'){ snap.position=m.position; renderPosition(m.position); renderOpenPnL(m.position); }
      if(m.type==='trade'){
        snap.trades.unshift(m.trade); if(snap.trades.length>50)snap.trades.pop();
        snap.stats=m.stats; snap.capital=m.capital; snap.winRate=m.winRate; snap.maxDrawdown=m.maxDrawdown;
        snap.position=null;
        renderStats(snap); renderTrades(snap.trades); renderPosition(null); renderOpenPnL(null);
      }
      if(m.type==='log'){ snap.log.unshift(m.line); if(snap.log.length>50)snap.log.pop(); $('log').innerHTML=snap.log.join('<br>'); }
    }
  };
  ws.onclose = () => { $('run').textContent='DÉCONNECTÉ'; $('run').className='badge off'; };

  $('start').onclick = ()=> ws.send(JSON.stringify({action:'start'}));
  $('stop').onclick = ()=> ws.send(JSON.stringify({action:'stop'}));
  $('closeAll').onclick = ()=> ws.send(JSON.stringify({action:'closeAll'}));

  // Init du graphique au chargement
  window.addEventListener('load', initChart);
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
  refreshMTF(); // première analyse multi-timeframe au démarrage
  setInterval(refreshMTF, 20000); // analyse MTF toutes les 20s (vraies bougies Binance)
  setInterval(reconcile, 9000); // réconciliation toutes les 9s
  server.listen(PORT, () => logLine(`🌐 Dashboard sur le port ${PORT}`));
}

start();
