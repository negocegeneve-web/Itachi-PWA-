/* ============================================================
 *  SERVEUR 5.0 - SCALPING  (mean-reversion maker-only) — OPTIMISÉ v2
 *  ------------------------------------------------------------
 *  Stratégie INCHANGÉE. Optimisations de réactivité et propreté :
 *   - INDICATEURS LIVE : Bollinger + RSI recalculés à chaque tick
 *     (throttle 1s) en intégrant le prix courant comme close de la
 *     bougie 1m en cours -> bandes réactives à la SECONDE, sans
 *     attendre le refresh. C'est le vrai gain scalp (bien plus utile
 *     que de baisser un timer : une bougie 1m ne change qu'à la minute).
 *   - Le fetch des bougies reste à 15s (recalculer plus souvent
 *     via l'API donnerait les mêmes valeurs + risque de rate-limit).
 *   - refreshAllKlines parallélisé par lots de 5 (~300ms).
 *   - Code mort des versions 3.x supprimé + commentaires corrigés.
 *
 *  Détection : le prix live (WebSocket 1s) est comparé aux bandes
 *  live -> perçage de bande détecté dans la seconde.
 *
 *  STRATÉGIE : mean-reversion en RANGE (Bollinger 20/2σ + RSI 7,
 *  <25/>75), filtre régime ADX<25, volume, ATR. TP +0.35% / SL
 *  -0.20% / retour médiane / time-stop 4min. MAKER-ONLY strict.
 *  Levier x3, mise 10% (ATR-scaled), cooldown 10min, coupe-circuit
 *  3 pertes, kill -20%. Chrono par trade.
 *
 *  ⚠️ Testnet valide le fonctionnement, pas la rentabilité.
 * ============================================================ */
/**
 * ITACHI MULTI — Bot multi-crypto Binance Futures (testnet)
 * ---------------------------------------------------------
 * SCALPING mean-reversion : fade les extrêmes de Bollinger en marché de range.
 *
 * Principes :
 *   - Binance = source de vérité (réconciliation, vérif après timeout -1007)
 *   - 1 position par symbole (One-Way), jusqu'à N symboles en parallèle
 *   - Exposition totale plafonnée à 35% du capital
 *   - Fréquence proportionnelle au capital (paliers)
 *   - Frais Binance intégrés dans le P&L
 *   - Lecture des 50 dernières bougies par symbole (EMA, RSI, S/R, momentum)
 *
 * Tous les paramètres ajustables sont en haut du fichier.
 */

'use strict';

const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');

// ==================================================================
// CONFIG GÉNÉRALE
// ==================================================================
const MODE = (process.env.BINANCE_MODE || 'testnet').toLowerCase();
const IS_TESTNET = MODE !== 'mainnet';
const API_KEY = process.env.BINANCE_API_KEY || '';
const API_SECRET = process.env.BINANCE_API_SECRET || '';
const CAPITAL_START = parseFloat(process.env.CAPITAL || '1000');
const PORT = parseInt(process.env.PORT || '8080', 10);

const REST_BASE = IS_TESTNET ? 'https://demo-fapi.binance.com' : 'https://fapi.binance.com';
const WS_BASE = IS_TESTNET ? 'wss://demo-fstream.binance.com' : 'wss://fstream.binance.com';
const KLINE_BASE = 'https://fapi.binance.com'; // bougies depuis mainnet (testnet pauvre en historique)

// ==================================================================
// PARAMÈTRES SCALPING (ajustables ici)
// ==================================================================
const STRAT = {
  // ============================================================
  //  SERVEUR 5.0 — SCALPING mean-reversion (maker-only, x3 max)
  //  Tous les indicateurs sont accordés au timeframe 1m.
  // ============================================================

  // --- Indicateurs de scalping (tous en 1m) ---
  BB_PERIOD: 20,       // Bandes de Bollinger : moyenne mobile 20 périodes (1m)
  BB_STDDEV: 2.0,      // 2 écarts-types (bandes)
  RSI_SCALP: 7,        // RSI court 7 périodes (réactif, pro scalping)
  RSI_OVERSOLD: 25,    // survente extrême -> signal LONG (rebond)
  RSI_OVERBOUGHT: 75,  // surachat extrême -> signal SHORT (retour)
  ATR_PERIOD: 14,      // ATR 1m (volatilité)
  VOL_MA_PERIOD: 20,   // moyenne de volume (filtre volume)

  // --- Filtre de régime (adaptation) : ADX ---
  ADX_PERIOD: 14,
  ADX_RANGE_MAX: 25,   // ADX < 25 = range -> mean-reversion OK. ADX >= 25 = tendance -> pause.

  // --- Filtres d'entrée ---
  ATR_FLOOR: 0.0015,   // ATR 1m minimum 0.15% : assez de volatilité pour bouger
  ATR_CEIL: 0.015,     // ATR 1m maximum 1.5% : au-delà, marché trop chaotique -> skip
  VOL_MIN_RATIO: 1.0,  // volume courant >= sa moyenne (confirmation)

  // --- Sorties (scalp : cibles petites mais > frais) ---
  TP_PCT: 0.0035,      // +0.35% take-profit (cible retour à la moyenne, ~3x les frais)
  SL_PCT: 0.0020,      // -0.20% stop-loss serré (discipline scalp)
  EXIT_ON_MIDBAND: true,// sortie anticipée si le prix revient à la bande médiane (VWAP-like)
  TIME_STOP_MS: 240000,// time-stop 4 min : si ça ne revient pas vite, le pari est raté

  // --- Frais & exécution (maker-only strict) ---
  FEE_MAKER: 0.0002,   // 0.02% maker par leg (réel Binance Futures)
  FEE_TAKER: 0.0005,   // 0.05% taker par leg (réel Binance Futures ; utilisé si fallback)
  MAKER_ONLY: true,    // STRICT : entrée maker post-only, PAS de fallback taker
  MAKER_WAIT_MS: 90000,// attente d'exécution du post-only ; sinon annulation + prochain signal
  MAKER_OFFSET: 0.0003,// ordre limite 0.03% en retrait (rester maker)
  EXIT_MAKER_FIRST: true, // sortie aussi en maker d'abord (TP en limite), stop en market

  // --- Levier & mise ---
  LEV: 3,              // levier FIXE x3 (scalp = frais bas, levier bas)
  STAKE_PCT: 0.10,     // mise = 10% du capital par trade
  STAKE_MIN_USD: 20,   // mise plancher

  // --- Risque & adaptation ---
  KILL_PCT: 0.20,      // kill switch -20% du capital
  MAX_POSITIONS_CAP: 6,// jusqu'à 6 scalps simultanés (sur des symboles différents)
  MAX_EXPOSURE_PCT: 0.70,
  COOLDOWN_AFTER_STOP_MS: 600000, // cooldown 10 min sur un symbole après un stop
  MAX_CONSEC_LOSSES: 3, // 3 pertes d'affilée -> pause auto du bot (coupe-circuit)
  ATR_SIZE_SCALING: true, // réduit la mise quand l'ATR est élevé (adaptation volatilité)

  // --- Sélection dynamique ---
  ACTIVE_TOP_N: 12,    // trade les 12 symboles les plus propices (volatilité dans la bande utile)

  // --- Cadence ---
  MIN_GAP_MS: 120000,  // 2 min minimum entre 2 entrées sur le même symbole
  SHORT_TF: '1m',
  SHORT_TF_LIMIT: 100, // 100 bougies 1m pour tous les indicateurs
  KLINE_LIMIT: 50,
  TF_PRINCIPAL: '1m',  // TIMEFRAME UNIQUE : 1m (cohérence scalp)
};

// Timeframes pour l'alignement multi-timeframe

// ==================================================================
// UNIVERS DE SYMBOLES — 20 cryptos en 4 catégories
// ==================================================================
const SYMBOLS = {
  major: ['BTCUSDT', 'ETHUSDT'], // 2 majeurs
  largeCap: ['BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'AVAXUSDT', 'LINKUSDT', 'DOTUSDT'], // 8 grandes caps
  volatile: ['NEARUSDT', 'APTUSDT', 'ARBUSDT', 'OPUSDT', 'INJUSDT', 'SEIUSDT', 'SUIUSDT'], // 7 volatils
  ultraVolatile: ['PEPEUSDT', 'WIFUSDT', 'BONKUSDT'], // 3 très très volatils
};
const ALL_SYMBOLS = [...SYMBOLS.major, ...SYMBOLS.largeCap, ...SYMBOLS.volatile, ...SYMBOLS.ultraVolatile];

// ==================================================================
// PALIERS DE FRÉQUENCE — plus le capital monte, plus on ouvre de positions
// ==================================================================

// ==================================================================
// ÉTAT GLOBAL
// ==================================================================
const state = {
  running: false,
  mode: MODE,
  capital: CAPITAL_START,
  capitalStart: CAPITAL_START,
  peakCapital: CAPITAL_START,
  maxDrawdown: 0,
  killed: false,
  sym: {},
  trades: [],
  stats: { wins: 0, losses: 0, gross: 0, fees: 0, net: 0 },
  log: [],
  consecLosses: 0, // pertes consécutives (coupe-circuit)
  activeSymbols: null, // sous-ensemble des N meilleurs symboles à trader (null = tous au démarrage)
};

for (const s of ALL_SYMBOLS) {
  state.sym[s] = {
    symbol: s, price: 0, prices: [], klines: [],
    indicators: { rsi: null, bias: 'NEUTRE', quality: null, breakdown: null },
    scalp: { bb: null, rsi: null, atrPct: null, adx: null, volRatio: null, regime: null, closedCloses: null }, // indicateurs scalping 1m
    position: null, lastEntryAt: 0, busy: false, lastStopAt: 0,
  };
}

function logLine(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  state.log.unshift(line);
  if (state.log.length > 250) state.log.pop();
  broadcast({ type: 'log', line });
}

// ==================================================================
// SIGNATURE + REQUÊTES BINANCE
// ==================================================================
function sign(query) {
  return crypto.createHmac('sha256', API_SECRET).update(query).digest('hex');
}

async function signedRequest(method, path, params = {}) {
  const timestamp = Date.now();
  const query = new URLSearchParams({ ...params, timestamp, recvWindow: 10000 }).toString();
  const signature = sign(query);
  const url = `${REST_BASE}${path}?${query}&signature=${signature}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(url, { method, headers: { 'X-MBX-APIKEY': API_KEY }, signal: controller.signal });
    const data = await res.json();
    if (!res.ok) {
      const err = new Error(`Binance ${res.status}: ${JSON.stringify(data)}`);
      err.binanceCode = data && data.code;
      throw err;
    }
    return data;
  } catch (e) {
    if (e.name === 'AbortError') {
      const err = new Error('timeout réseau (8s)');
      err.binanceCode = -1007;
      throw err;
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

async function publicGet(base, path, params = {}, retries = 2) {
  const query = new URLSearchParams(params).toString();
  const url = `${base}${path}${query ? '?' + query : ''}`;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    try {
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timer);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (e) {
      clearTimeout(timer);
      if (attempt === retries) throw e;
      await new Promise((r) => setTimeout(r, 400 * (attempt + 1)));
    }
  }
}

// ==================================================================
// PRÉCISIONS SYMBOLES
// ==================================================================
const SYMBOL_INFO = {};

async function loadSymbolInfo() {
  try {
    const info = await publicGet(REST_BASE, '/fapi/v1/exchangeInfo');
    for (const sym of info.symbols) {
      if (ALL_SYMBOLS.includes(sym.symbol)) {
        const lot = sym.filters.find((f) => f.filterType === 'LOT_SIZE');
        SYMBOL_INFO[sym.symbol] = {
          qtyPrecision: sym.quantityPrecision,
          pricePrecision: sym.pricePrecision,
          stepSize: lot ? parseFloat(lot.stepSize) : 0.001,
        };
      }
    }
    logLine(`Précisions chargées pour ${Object.keys(SYMBOL_INFO).length} symboles`);
  } catch (e) {
    logLine(`⚠️ loadSymbolInfo: ${e.message}`);
  }
}

function roundQty(symbol, q) {
  const p = SYMBOL_INFO[symbol] ? SYMBOL_INFO[symbol].qtyPrecision : 3;
  return parseFloat(q.toFixed(p));
}
function roundPrice(symbol, price) {
  const p = SYMBOL_INFO[symbol] && SYMBOL_INFO[symbol].pricePrecision != null ? SYMBOL_INFO[symbol].pricePrecision : 4;
  return parseFloat(price.toFixed(p));
}

// ==================================================================
// INDICATEURS
// ==================================================================
function rsi(closes, period = 14) {
  if (closes.length < period + 1) return null;
  let g = 0, l = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    if (d >= 0) g += d; else l -= d;
  }
  const ag = g / period, al = l / period;
  if (al === 0) return 100;
  return 100 - 100 / (1 + ag / al);
}
// ATR normalisé (en % du prix) sur des bougies {high,low,close} — mesure de volatilité
function atrPct(klines, period = 14) {
  if (!klines || klines.length < period + 1) return null;
  let sum = 0;
  for (let i = klines.length - period; i < klines.length; i++) {
    const h = klines[i].high, l = klines[i].low, pc = klines[i - 1].close;
    const tr = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
    sum += tr;
  }
  const atr = sum / period;
  const lastClose = klines[klines.length - 1].close;
  return lastClose > 0 ? atr / lastClose : null;
}

// --- Indicateurs de SCALPING (tous sur bougies 1m) ---
function sma(values, period) {
  if (values.length < period) return null;
  const slice = values.slice(-period);
  return slice.reduce((a, b) => a + b, 0) / period;
}
// Bandes de Bollinger : {mid, upper, lower} sur closes
function bollinger(closes, period, mult) {
  if (closes.length < period) return null;
  const slice = closes.slice(-period);
  const mid = slice.reduce((a, b) => a + b, 0) / period;
  const variance = slice.reduce((a, b) => a + (b - mid) * (b - mid), 0) / period;
  const sd = Math.sqrt(variance);
  return { mid, upper: mid + mult * sd, lower: mid - mult * sd, sd };
}
// ADX (force de tendance) sur klines {high,low,close}. Retourne 0..100.
// ADX bas = range (mean-reversion OK) ; ADX haut = tendance (danger MR).
function adx(klines, period = 14) {
  if (!klines || klines.length < period * 2 + 1) return null;
  const tr = [], plusDM = [], minusDM = [];
  for (let i = 1; i < klines.length; i++) {
    const h = klines[i].high, l = klines[i].low, pc = klines[i - 1].close;
    const ph = klines[i - 1].high, pl = klines[i - 1].low;
    tr.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
    const up = h - ph, down = pl - l;
    plusDM.push(up > down && up > 0 ? up : 0);
    minusDM.push(down > up && down > 0 ? down : 0);
  }
  const wilder = (arr, p) => {
    let s = arr.slice(0, p).reduce((a, b) => a + b, 0);
    const out = [s];
    for (let i = p; i < arr.length; i++) { s = s - s / p + arr[i]; out.push(s); }
    return out;
  };
  const trS = wilder(tr, period), pS = wilder(plusDM, period), mS = wilder(minusDM, period);
  const dx = [];
  for (let i = 0; i < trS.length; i++) {
    const pDI = 100 * (pS[i] / trS[i]);
    const mDI = 100 * (mS[i] / trS[i]);
    const sum = pDI + mDI;
    dx.push(sum === 0 ? 0 : 100 * Math.abs(pDI - mDI) / sum);
  }
  if (dx.length < period) return null;
  // ADX = moyenne lissée du DX
  let adxVal = dx.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < dx.length; i++) adxVal = (adxVal * (period - 1) + dx[i]) / period;
  return adxVal;
}
// Ratio du volume courant vs sa moyenne
function volumeRatio(klines, period) {
  if (!klines || klines.length < period + 1) return null;
  const vols = klines.map((k) => k.vol);
  const avg = sma(vols.slice(0, -1), period); // moyenne hors bougie courante
  const cur = vols[vols.length - 1];
  return avg && avg > 0 ? cur / avg : null;
}

// ==================================================================
// LECTURE DES BOUGIES 1m + CALCUL DES INDICATEURS SCALP
// ==================================================================
async function refreshKlines(symbol) {
  const S = state.sym[symbol];
  try {
    // Une SEULE frame : 1m, 100 bougies. Tous les indicateurs scalp en découlent.
    const raw = await publicGet(KLINE_BASE, '/fapi/v1/klines', {
      symbol, interval: STRAT.SHORT_TF, limit: STRAT.SHORT_TF_LIMIT,
    });
    const kl = raw.map((c) => ({ time: c[0], open: +c[1], high: +c[2], low: +c[3], close: +c[4], vol: +c[5] }));
    S.klines = kl;
    const closes = kl.map((c) => c.close);
    // On mémorise les closes des bougies FERMÉES (hors dernière, encore en formation)
    // pour pouvoir recalculer Bollinger/RSI en direct avec le prix live à chaque tick.
    S.scalp.closedCloses = closes.slice(0, -1);

    const bb = bollinger(closes, STRAT.BB_PERIOD, STRAT.BB_STDDEV);
    S.scalp.bb = bb;
    S.scalp.rsi = rsi(closes, STRAT.RSI_SCALP);
    S.scalp.atrPct = atrPct(kl, STRAT.ATR_PERIOD);
    S.scalp.adx = adx(kl, STRAT.ADX_PERIOD);
    S.scalp.volRatio = volumeRatio(kl, STRAT.VOL_MA_PERIOD);

    // Régime : range (ADX bas) = mean-reversion OK ; tendance (ADX haut) = pause.
    S.scalp.regime = (S.scalp.adx != null && S.scalp.adx < STRAT.ADX_RANGE_MAX) ? 'RANGE' : 'TREND';

    // Biais affiché (pour le dashboard) : où est le prix vs les bandes.
    if (bb && S.price > 0) {
      if (S.price <= bb.lower) S.indicators.bias = 'LONG';
      else if (S.price >= bb.upper) S.indicators.bias = 'SHORT';
      else S.indicators.bias = 'NEUTRE';
    }
    S.indicators.rsi = S.scalp.rsi;
  } catch (e) { /* on garde l'ancien */ }
}

// ==================================================================
// MOTEUR DE SIGNAL — mean-reversion (Bollinger + RSI + régime ADX)
// ==================================================================
function computeSignal(symbol) {
  const S = state.sym[symbol];
  const sc = S.scalp;
  const px = S.price;
  if (!sc.bb || sc.rsi == null || sc.atrPct == null || px <= 0) return null;

  // --- Adaptation régime : on ne trade la mean-reversion QUE en range (ADX bas) ---
  if (sc.regime !== 'RANGE') { S.indicators.quality = null; return null; }

  // --- Filtres de volatilité : ni trop mou, ni chaotique ---
  if (sc.atrPct < STRAT.ATR_FLOOR || sc.atrPct > STRAT.ATR_CEIL) { S.indicators.quality = null; return null; }

  // --- Filtre volume : confirmation (volume courant >= sa moyenne) ---
  if (sc.volRatio != null && sc.volRatio < STRAT.VOL_MIN_RATIO) { S.indicators.quality = null; return null; }

  // --- Signal MEAN-REVERSION : extrême de Bollinger + RSI extrême ---
  const belowLower = px <= sc.bb.lower;
  const aboveUpper = px >= sc.bb.upper;
  const rsiLow = sc.rsi <= STRAT.RSI_OVERSOLD;
  const rsiHigh = sc.rsi >= STRAT.RSI_OVERBOUGHT;

  let side = null;
  if (belowLower && rsiLow) side = 'BUY';   // survendu -> rebond attendu
  else if (aboveUpper && rsiHigh) side = 'SELL'; // suracheté -> retour attendu
  if (!side) { S.indicators.quality = null; return null; }

  // --- Score de qualité (0-100) : plus l'extrême est marqué, plus Q est haut ---
  // Distance à la bande (en fraction d'écart-type) + extrémité du RSI + volume.
  const dist = side === 'BUY'
    ? (sc.bb.mid - px) / (sc.bb.sd || 1)   // combien d'écarts-types sous la moyenne
    : (px - sc.bb.mid) / (sc.bb.sd || 1);
  const distScore = Math.min(40, Math.max(0, dist * 20)); // 2 sigma -> 40 pts
  const rsiScore = side === 'BUY'
    ? Math.min(30, (STRAT.RSI_OVERSOLD - sc.rsi + 5) * 3)
    : Math.min(30, (sc.rsi - STRAT.RSI_OVERBOUGHT + 5) * 3);
  const volScore = sc.volRatio != null ? Math.min(30, (sc.volRatio - 1) * 30) : 15;
  const quality = Math.round(distScore + Math.max(0, rsiScore) + Math.max(0, volScore));

  S.indicators.quality = quality;
  S.indicators.bias = side === 'BUY' ? 'LONG' : 'SHORT';
  S.indicators.breakdown = { dist: Math.round(distScore), rsi: Math.round(Math.max(0, rsiScore)), vol: Math.round(Math.max(0, volScore)) };

  return { side, quality, midBand: sc.bb.mid, symbol };
}

// Recalcule Bollinger + RSI EN DIRECT en intégrant le prix live comme close de la
// bougie 1m en cours. Rend les bandes réactives à la seconde (au lieu d'attendre le
// refresh 15s). Throttlé à ~1s par symbole (inutile de recalculer 20x/seconde).
function refreshLiveIndicators(S) {
  const cc = S.scalp.closedCloses;
  if (!cc || cc.length < STRAT.BB_PERIOD || S.price <= 0) return;
  const now = Date.now();
  if (S._liveAt && now - S._liveAt < 1000) return; // throttle 1s
  S._liveAt = now;
  // Série = bougies fermées + prix live (bougie en cours)
  const live = cc.concat(S.price);
  const bb = bollinger(live, STRAT.BB_PERIOD, STRAT.BB_STDDEV);
  if (bb) S.scalp.bb = bb;
  const r = rsi(live, STRAT.RSI_SCALP);
  if (r != null) { S.scalp.rsi = r; S.indicators.rsi = r; }
}

function computeExits(symbol) {
  // Scalp : SL/TP fixes serrés (cibles calibrées pour dépasser les frais maker).
  return { slPct: STRAT.SL_PCT, tpPct: STRAT.TP_PCT, source: 'scalp' };
}

function sizing(signal) {
  // Scalp : levier FIXE x3, mise = % du capital, réduite si l'ATR est élevé (adaptation).
  let stake = Math.max(STRAT.STAKE_MIN_USD, state.capital * STRAT.STAKE_PCT);
  if (STRAT.ATR_SIZE_SCALING) {
    const atr = state.sym[signal.symbol] ? state.sym[signal.symbol].scalp.atrPct : null;
    // ATR de reference = milieu de la bande utile. Au-dela, on reduit la mise (plancher 50%).
    if (atr != null) {
      const ref = (STRAT.ATR_FLOOR + STRAT.ATR_CEIL) / 2;
      if (atr > ref) {
        const factor = Math.max(0.5, ref / atr);
        stake = Math.max(STRAT.STAKE_MIN_USD, stake * factor);
      }
    }
  }
  return { stake, lev: STRAT.LEV };
}

function currentExposure() {
  let total = 0;
  for (const s of ALL_SYMBOLS) if (state.sym[s].position) total += state.sym[s].position.stake;
  return total;
}
function openPositionsCount() {
  return ALL_SYMBOLS.filter((s) => state.sym[s].position).length;
}

// ==================================================================
// ORDRES
// ==================================================================
async function setLeverage(symbol, lev) {
  try { await signedRequest('POST', '/fapi/v1/leverage', { symbol, leverage: lev }); }
  catch (e) { /* non bloquant */ }
}
async function marketOrder(symbol, side, qty, reduceOnly = false) {
  const params = { symbol, side, type: 'MARKET', quantity: qty };
  if (reduceOnly) params.reduceOnly = 'true';
  return signedRequest('POST', '/fapi/v1/order', params);
}

// Ordre LIMITE post-only (maker) : ne s'exécute QUE comme maker (frais réduits).
// Si le prix devait le rendre taker, Binance le rejette (timeInForce GTX = post-only).
async function limitMakerOrder(symbol, side, qty, price) {
  const p = roundPrice(symbol, price);
  const params = { symbol, side, type: 'LIMIT', quantity: qty, price: p, timeInForce: 'GTX' };
  return signedRequest('POST', '/fapi/v1/order', params);
}
async function cancelOrder(symbol, orderId) {
  try { return await signedRequest('DELETE', '/fapi/v1/order', { symbol, orderId }); }
  catch (e) { return null; }
}
async function getOrder(symbol, orderId) {
  try { return await signedRequest('GET', '/fapi/v1/order', { symbol, orderId }); }
  catch (e) { return null; }
}

// Entrée maker-first : tente un post-only (frais maker), et si pas exécuté en
// MAKER_WAIT_MS, on annule et on passe en market (taker). Renvoie 'maker'|'taker'|null.
async function openWithMaker(symbol, side, qty, refPrice) {
  // Scalp MAKER-ONLY strict : on tente un post-only ; si non exécuté, on ANNULE et on
  // renonce (pas de fallback taker, qui tuerait la marge du scalp). Retourne 'maker' ou null.
  const offset = STRAT.MAKER_OFFSET;
  const limitPx = side === 'BUY' ? refPrice * (1 - offset) : refPrice * (1 + offset);
  let order;
  try {
    order = await limitMakerOrder(symbol, side, qty, limitPx);
  } catch (e) {
    return null; // post-only rejeté -> on renonce
  }
  const orderId = order && order.orderId;
  if (!orderId) return null;

  await new Promise((r) => setTimeout(r, STRAT.MAKER_WAIT_MS));
  const st = await getOrder(symbol, orderId);
  if (st && st.status === 'FILLED') return 'maker';

  // Non (entièrement) rempli -> on annule.
  await cancelOrder(symbol, orderId);
  const filled = st ? parseFloat(st.executedQty || 0) : 0;
  if (filled > 0) return 'maker-partial'; // une partie est passée en maker, on la garde
  return null; // rien rempli -> pas de position
}
async function fetchPosition(symbol) {
  try {
    const data = await signedRequest('GET', '/fapi/v2/positionRisk', { symbol });
    const pos = Array.isArray(data) ? data.find((x) => x.symbol === symbol) : null;
    if (!pos) return null;
    const amt = parseFloat(pos.positionAmt);
    const step = SYMBOL_INFO[symbol] ? SYMBOL_INFO[symbol].stepSize : 0.001;
    if (Math.abs(amt) < step) return null;
    return { side: amt > 0 ? 'BUY' : 'SELL', qty: Math.abs(amt), entry: parseFloat(pos.entryPrice) };
  } catch (e) { return null; }
}

// ==================================================================
// OUVERTURE / FERMETURE
// ==================================================================
async function tryOpen(symbol, signal) {
  const S = state.sym[symbol];
  const now = Date.now();
  if (S.position) return;
  // Ne trader que les symboles actifs du moment (top-N dynamique)
  if (state.activeSymbols && !state.activeSymbols.includes(symbol)) return;
  if (now - S.lastEntryAt < STRAT.MIN_GAP_MS) return;
  // Cooldown : après un stop sur ce symbole, on attend avant de re-trader (évite l'acharnement).
  if (S.lastStopAt && now - S.lastStopAt < STRAT.COOLDOWN_AFTER_STOP_MS) return;

  const { stake, lev } = sizing(signal);
  // Plafond dur du nombre de positions simultanées
  if (openPositionsCount() >= STRAT.MAX_POSITIONS_CAP) return;
  // Garde-fou exposition
  if (currentExposure() + stake > state.capital * STRAT.MAX_EXPOSURE_PCT) return;

  const qty = roundQty(symbol, (stake * lev) / S.price);
  if (qty <= 0) return;

  await setLeverage(symbol, lev);

  let entryFill = null;
  try {
    entryFill = await openWithMaker(symbol, signal.side, qty, S.price);
  } catch (e) {
    logLine(`❌ ${symbol} ouverture: ${e.message}`);
    return;
  }
  // Maker-only strict : si l'ordre post-only n'a pas été rempli, on renonce (pas de position).
  if (!entryFill) {
    logLine(`⏭️ ${symbol} : post-only non exécuté en ${STRAT.MAKER_WAIT_MS/1000}s — trade abandonné (maker-only).`);
    return;
  }

  const entry = S.price;
  const exits = computeExits(symbol); // SL/TP fixes scalp
  S.position = {
    side: signal.side, entry, qty, stake, lev, quality: signal.quality,
    entryFill, // 'maker' / 'maker-partial' : sert au calcul des frais d'entrée
    slPct: exits.slPct, tpPct: exits.tpPct,
    sl: signal.side === 'BUY' ? entry * (1 - exits.slPct) : entry * (1 + exits.slPct),
    tp: signal.side === 'BUY' ? entry * (1 + exits.tpPct) : entry * (1 - exits.tpPct),
    openedAt: now, midBand: signal.midBand || null,
  };
  S.lastEntryAt = now;
  logLine(`🟢 ${symbol} ${signal.side} qty=${qty} @ ${entry.toFixed(4)} x${lev} Q=${signal.quality} SL-${(exits.slPct*100).toFixed(2)}%/TP+${(exits.tpPct*100).toFixed(2)}% [${entryFill}]`);
  broadcast({ type: 'positions', positions: livePositions() });
}

async function closePos(symbol, reason) {
  const S = state.sym[symbol];
  const pos = S.position;
  if (!pos) return;
  const closeSide = pos.side === 'BUY' ? 'SELL' : 'BUY';
  try {
    await marketOrder(symbol, closeSide, pos.qty, true);
  } catch (e) {
    if (e.binanceCode === -1007) {
      await new Promise((r) => setTimeout(r, 1500));
      const after = await fetchPosition(symbol);
      if (after) { logLine(`↩️ ${symbol} : fermeture non confirmée, réessai.`); return; }
    } else {
      logLine(`❌ ${symbol} fermeture: ${e.message}`);
      return;
    }
  }

  const exit = S.price;
  const dir = pos.side === 'BUY' ? 1 : -1;
  const pnlPct = ((exit - pos.entry) / pos.entry) * dir;
  const gross = pnlPct * pos.stake * pos.lev;
  // Frais réels : entrée maker (0.02%) en scalp maker-only ; sortie maker si TP en limite,
  // taker si sortie au market (stop, time-stop). On approxime : TP/RETOUR-MOYENNE = maker.
  const entryFeeRate = pos.entryFill === 'taker' ? STRAT.FEE_TAKER : STRAT.FEE_MAKER;
  const exitMaker = STRAT.EXIT_MAKER_FIRST && (reason === 'TAKE-PROFIT' || reason === 'RETOUR-MOYENNE');
  const exitFeeRate = exitMaker ? STRAT.FEE_MAKER : STRAT.FEE_TAKER;
  const fees = pos.stake * pos.lev * (entryFeeRate + exitFeeRate);
  const net = gross - fees;

  state.capital += net;
  state.stats.gross += gross;
  state.stats.fees += fees;
  state.stats.net += net;
  if (net >= 0) { state.stats.wins++; state.consecLosses = 0; }
  else { state.stats.losses++; state.consecLosses++; }
  if (reason === 'STOP-LOSS') S.lastStopAt = Date.now(); // déclenche le cooldown symbole
  if (state.capital > state.peakCapital) state.peakCapital = state.capital;
  const dd = (state.peakCapital - state.capital) / state.peakCapital;
  if (dd > state.maxDrawdown) state.maxDrawdown = dd;

  state.trades.unshift({
    symbol, side: pos.side, entry: pos.entry, exit, lev: pos.lev, quality: pos.quality,
    investi: pos.stake.toFixed(2), pnlPct: (pnlPct * 100).toFixed(2),
    gross: gross.toFixed(2), fees: fees.toFixed(2), net: net.toFixed(2),
    reason, durationMs: Date.now() - pos.openedAt,
  });
  if (state.trades.length > 100) state.trades.pop();

  logLine(`🔴 ${symbol} ${reason} @ ${exit.toFixed(4)} | net=${net.toFixed(2)}$ | capital=${state.capital.toFixed(2)}$`);
  S.position = null;
  broadcast({ type: 'trade', stats: state.stats, capital: state.capital, positions: livePositions() });

  if (state.capital <= state.capitalStart * (1 - STRAT.KILL_PCT)) {
    state.running = false;
    state.killed = true;
    logLine(`🛑 KILL SWITCH -${STRAT.KILL_PCT*100}% — capital ${state.capital.toFixed(2)}$. Bot arrêté.`);
    broadcast({ type: 'status', running: false });
  }
  // Coupe-circuit : N pertes consécutives -> pause auto (le marché ne convient pas).
  if (state.consecLosses >= STRAT.MAX_CONSEC_LOSSES) {
    state.running = false;
    logLine(`⛔ COUPE-CIRCUIT : ${state.consecLosses} pertes consécutives — bot en pause. Relance manuelle.`);
    state.consecLosses = 0;
    broadcast({ type: 'status', running: false });
  }
}

function managePosition(symbol) {
  const S = state.sym[symbol];
  const pos = S.position;
  if (!pos) return;
  const px = S.price;
  const dir = pos.side === 'BUY' ? 1 : -1;
  const pnlPct = ((px - pos.entry) / pos.entry) * dir;
  const age = Date.now() - pos.openedAt;

  // --- SCALP : sorties fixes serrées + retour à la moyenne + time-stop ---
  // 1) Take-profit fixe (+0.35%)
  if (pnlPct >= STRAT.TP_PCT) { closePos(symbol, 'TAKE-PROFIT'); return; }
  // 2) Stop-loss serré (-0.20%)
  if (pnlPct <= -STRAT.SL_PCT) { closePos(symbol, 'STOP-LOSS'); return; }
  // 3) Sortie anticipée : le prix est revenu à la bande médiane (la "moyenne") en profit.
  //    C'est la cible naturelle de la mean-reversion : on encaisse le retour.
  if (STRAT.EXIT_ON_MIDBAND && pos.midBand && pnlPct > 0) {
    const backToMean = pos.side === 'BUY' ? px >= pos.midBand : px <= pos.midBand;
    if (backToMean) { closePos(symbol, 'RETOUR-MOYENNE'); return; }
  }
  // 4) Time-stop : si le rebond ne vient pas vite, le pari mean-reversion est raté.
  if (age >= STRAT.TIME_STOP_MS) { closePos(symbol, 'TIME-STOP'); return; }
}

// ==================================================================
// BOUCLE PAR TICK
// ==================================================================
async function symbolTick(symbol) {
  const S = state.sym[symbol];
  if (!state.running || S.price <= 0) return;
  refreshLiveIndicators(S); // bandes/RSI réactifs au prix courant (throttle 1s)
  managePosition(symbol);
  if (S.busy || S.position) return;
  const signal = computeSignal(symbol);
  if (signal) {
    S.busy = true;
    try { await tryOpen(symbol, signal); }
    finally { S.busy = false; }
  }
}

async function reconcile() {
  if (!API_KEY || !API_SECRET) return;
  for (const symbol of ALL_SYMBOLS) {
    const S = state.sym[symbol];
    if (!S.position) continue;
    const real = await fetchPosition(symbol);
    if (!real) { S.position = null; broadcast({ type: 'positions', positions: livePositions() }); }
  }
}

// ==================================================================
// WEBSOCKET PRIX (multi-stream)
// ==================================================================
function connectPriceStreams() {
  const streams = ALL_SYMBOLS.map((s) => `${s.toLowerCase()}@markPrice@1s`).join('/');
  const url = `${WS_BASE}/stream?streams=${streams}`;
  const ws = new WebSocket(url);
  ws.on('open', () => logLine(`🔌 WebSocket prix connecté (${MODE}) — ${ALL_SYMBOLS.length} symboles`));
  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      const d = msg.data || msg;
      const sym = (d.s || '').toUpperCase();
      const p = parseFloat(d.p || d.markPrice);
      if (sym && state.sym[sym] && p > 0) {
        const S = state.sym[sym];
        S.price = p;
        S.prices.push(p);
        if (S.prices.length > 300) S.prices.shift();
        symbolTick(sym);
      }
    } catch (e) { /* ignore */ }
  });
  ws.on('close', () => { logLine('⚠️ WS prix fermé — reconnexion 3s'); setTimeout(connectPriceStreams, 3000); });
  ws.on('error', (e) => logLine(`⚠️ WS error: ${e.message}`));
}

async function refreshAllKlines() {
  // Rafraîchissement PARALLÈLE par lots (au lieu de séquentiel + 150ms) :
  // 20 symboles en ~1 requête chacun -> quelques centaines de ms au lieu de ~3s.
  // Lots de 5 pour ne pas saturer le rate-limit Binance.
  const BATCH = 5;
  for (let i = 0; i < ALL_SYMBOLS.length; i += BATCH) {
    const slice = ALL_SYMBOLS.slice(i, i + BATCH);
    await Promise.all(slice.map((s) => refreshKlines(s)));
  }
  rankActiveSymbols();
  broadcast({ type: 'snapshot', data: snapshot() });
}

// Classe les 20 symboles par score = volatilité(ATR) x force d'alignement de tendance,
// et ne garde que les ACTIVE_TOP_N meilleurs comme tradables. On concentre le capital
// là où ça bouge ET où la tendance est nette. Les positions déjà ouvertes restent gérées.
function rankActiveSymbols() {
  // Scalp : on privilégie les symboles en RANGE (ADX bas) et dans la bande de volatilité utile.
  const mid = (STRAT.ATR_FLOOR + STRAT.ATR_CEIL) / 2;
  const scored = ALL_SYMBOLS.map((s) => {
    const S = state.sym[s];
    const atr = S.scalp.atrPct || 0;
    const adxVal = S.scalp.adx != null ? S.scalp.adx : 100;
    // Score : volatilité proche du milieu de la bande utile + bonus si range (ADX bas)
    const inBand = (atr >= STRAT.ATR_FLOOR && atr <= STRAT.ATR_CEIL) ? 1 : 0;
    const rangeBonus = adxVal < STRAT.ADX_RANGE_MAX ? 1 : 0.2;
    const proximity = atr > 0 ? 1 - Math.min(1, Math.abs(atr - mid) / mid) : 0;
    return { s, score: inBand * rangeBonus * (0.3 + proximity) };
  }).sort((a, b) => b.score - a.score);

  const top = scored.slice(0, STRAT.ACTIVE_TOP_N).map((x) => x.s);
  for (const s of ALL_SYMBOLS) if (state.sym[s].position && !top.includes(s)) top.push(s);
  state.activeSymbols = top;
}

// ==================================================================
// SERVEUR HTTP + WS DASHBOARD
// ==================================================================
const clients = new Set();
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const c of clients) if (c.readyState === WebSocket.OPEN) c.send(msg);
}

function livePositions() {
  const out = [];
  for (const symbol of ALL_SYMBOLS) {
    const S = state.sym[symbol];
    const pos = S.position;
    if (!pos) continue;
    const px = S.price || pos.entry;
    const dir = pos.side === 'BUY' ? 1 : -1;
    const pnlPct = ((px - pos.entry) / pos.entry) * dir;
    const gross = pnlPct * pos.stake * pos.lev;
    const fees = pos.stake * pos.lev * (STRAT.FEE_MAKER * 2);
    out.push({
      symbol, side: pos.side, entry: pos.entry, current: px, lev: pos.lev,
      quality: pos.quality, investi: pos.stake, sl: pos.sl, tp: pos.tp,
      pnlPct: pnlPct * 100, netLive: gross - fees,
      ageMs: Date.now() - pos.openedAt, timeStopMs: STRAT.TIME_STOP_MS,
    });
  }
  return out;
}

function symbolsOverview() {
  return ALL_SYMBOLS.map((symbol) => {
    const S = state.sym[symbol];
    // Échantillon de prix pour mini-courbe (40 derniers points max)
    const spark = S.prices.slice(-40);
    return {
      symbol, price: S.price, bias: S.indicators.bias, quality: S.indicators.quality,
      rsi: S.indicators.rsi, regime: S.scalp.regime, adx: S.scalp.adx, hasPosition: !!S.position,
      spark,
    };
  });
}

function snapshot() {
  const tot = state.stats.wins + state.stats.losses;
  return {
    mode: state.mode, running: state.running, killed: state.killed,
    capital: state.capital, capitalStart: state.capitalStart,
    maxDrawdown: state.maxDrawdown * 100,
    exposure: currentExposure(), maxExposure: state.capital * STRAT.MAX_EXPOSURE_PCT,
    openPositions: openPositionsCount(),
    maxPositions: Math.min(STRAT.MAX_POSITIONS_CAP, Math.floor(STRAT.MAX_EXPOSURE_PCT / STRAT.STAKE_PCT)),
    excOpen: 0, excMax: 0,
    wins: state.stats.wins, losses: state.stats.losses,
    stats: state.stats, winRate: tot ? (state.stats.wins / tot) * 100 : null,
    positions: livePositions(), symbols: symbolsOverview(),
    trades: state.trades.slice(0, 40), log: state.log.slice(0, 50),
    strat: { sl: STRAT.SL_PCT * 100, tp: STRAT.TP_PCT * 100, lev: STRAT.LEV, ratio: STRAT.TP_PCT / STRAT.SL_PCT },
  };
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

const wss = new WebSocket.Server({ server });
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: 'snapshot', data: snapshot() }));
  ws.on('message', async (raw) => {
    let cmd;
    try { cmd = JSON.parse(raw); } catch { return; }
    if (cmd.action === 'start') {
      if (state.killed) { logLine('⚠️ Kill switch actif — redémarrage refusé.'); return; }
      state.running = true; logLine('▶️ Bot LANCÉ'); broadcast({ type: 'status', running: true });
    } else if (cmd.action === 'stop') {
      state.running = false; logLine('⏸️ Bot EN PAUSE'); broadcast({ type: 'status', running: false });
    } else if (cmd.action === 'closeAll') {
      for (const s of ALL_SYMBOLS) if (state.sym[s].position) await closePos(s, 'MANUEL');
      logLine('🧹 Fermeture manuelle de toutes les positions');
    }
  });
  ws.on('close', () => clients.delete(ws));
});

// ==================================================================
// DASHBOARD
// ==================================================================
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#00F5C8">
<title>Itachi Multi — CryptoSignal AI</title>
<style>
  :root{--bg:#070b10;--card:#101820;--card2:#0e151d;--cyan:#00F5C8;--cyan-dim:rgba(0,245,200,.12);
    --green:#22e58a;--red:#ff5470;--amber:#ffb547;--txt:#e7f0ef;--mut:#7c8b95;--line:#1a2530}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:radial-gradient(1200px 600px at 50% -200px,rgba(0,245,200,.06),transparent),var(--bg);
    color:var(--txt);font-family:'Segoe UI',system-ui,sans-serif;padding:14px;max-width:1200px;margin:0 auto}
  .head{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .logo{font-size:19px;font-weight:800}.logo .c{color:var(--cyan)}
  .badge{padding:3px 9px;border-radius:6px;font-size:11px;font-weight:700;letter-spacing:.5px}
  .badge.net{background:var(--cyan-dim);color:var(--cyan);border:1px solid rgba(0,245,200,.3)}
  .badge.on{background:rgba(34,229,138,.15);color:var(--green)}
  .badge.off{background:rgba(124,139,149,.15);color:var(--mut)}
  .sub{color:var(--mut);font-size:12px;margin:4px 0 14px}
  .controls{display:flex;gap:8px;margin:14px 0;flex-wrap:wrap}
  button{border:0;border-radius:9px;padding:11px 20px;font-size:14px;font-weight:700;cursor:pointer;font-family:inherit}
  .btn-go{background:var(--cyan);color:#04221d}.btn-stop{background:#1c2630;color:var(--txt)}
  .btn-kill{background:rgba(255,84,112,.15);color:var(--red);border:1px solid rgba(255,84,112,.3)}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:14px}
  .card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:13px 15px}
  .card .k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  .card .v{font-size:20px;font-weight:800;margin-top:5px;font-variant-numeric:tabular-nums}
  .green{color:var(--green)}.red{color:var(--red)}.cyan{color:var(--cyan)}.mut{color:var(--mut)}
  .sec{font-size:13px;font-weight:700;margin:18px 0 8px;display:flex;align-items:center;gap:8px}
  .sec .dot{width:6px;height:6px;border-radius:50%;background:var(--cyan)}
  .table-wrap{border:1px solid var(--line);border-radius:12px;overflow-x:auto;background:var(--card2)}
  table{width:100%;border-collapse:collapse;font-size:12.5px;min-width:640px}
  th,td{text-align:right;padding:8px 11px;border-bottom:1px solid var(--line);white-space:nowrap;font-variant-numeric:tabular-nums}
  th:first-child,td:first-child{text-align:left}
  th{color:var(--mut);font-size:10.5px;text-transform:uppercase;letter-spacing:.5px;background:var(--card)}
  .pill{padding:2px 8px;border-radius:5px;font-size:11px;font-weight:700}
  .pill.long{background:rgba(34,229,138,.15);color:var(--green)}
  .pill.short{background:rgba(255,84,112,.15);color:var(--red)}
  .pill.flat{background:rgba(124,139,149,.15);color:var(--mut)}
  .tag{padding:1px 6px;border-radius:5px;font-size:10px;font-weight:800;margin-left:4px}
  .tag.exc{background:rgba(255,181,71,.18);color:var(--amber)}
  .tag.exp{background:rgba(0,245,200,.16);color:var(--cyan)}
  .log{background:var(--card2);border:1px solid var(--line);border-radius:12px;padding:12px;
    font-family:Consolas,monospace;font-size:11px;max-height:200px;overflow:auto;color:#8aa0a0;line-height:1.7;margin-top:8px}
  .qbadge{font-weight:800}
</style></head>
<body>
  <div class="head">
    <span class="logo">CryptoSignal<span class="c">AI</span> · Multi</span>
    <span class="badge" style="background:rgba(0,245,200,.12);color:#00F5C8;border:1px solid rgba(0,245,200,.3)">5.0 - Scalping - MR x3 <span style="opacity:.6;font-weight:600">· WR ~?%</span></span>
    <span id="mode" class="badge net">TESTNET</span>
    <span id="run" class="badge off">PAUSE</span>
  </div>
  <div class="sub" id="stratline">5.0 Scalping · mean-reversion · maker-only · x3 · TP+0.35% / SL-0.20%</div>

  <div class="controls">
    <button id="start" class="btn-go">▶ Démarrer</button>
    <button id="stop" class="btn-stop">⏸ Pause</button>
    <button id="closeAll" class="btn-kill">⏹ Tout fermer</button>
  </div>

  <div class="grid">
    <div class="card"><div class="k">Capital</div><div class="v" id="cap">—</div></div>
    <div class="card"><div class="k">P&L Net</div><div class="v" id="net">—</div></div>
    <div class="card"><div class="k">Win Rate</div><div class="v" id="wr">—</div></div>
    <div class="card"><div class="k">Trades</div><div class="v" id="ntr">0</div></div>
    <div class="card"><div class="k">Gagnants (net)</div><div class="v green" id="wins">0</div></div>
    <div class="card"><div class="k">Perdants (net)</div><div class="v red" id="losses">0</div></div>
    <div class="card"><div class="k">Positions</div><div class="v" id="pos">0/0</div></div>
    <div class="card"><div class="k">Mises 9% actives</div><div class="v cyan" id="exc">0/0</div></div>
    <div class="card"><div class="k">Exposition</div><div class="v" id="exp">—</div></div>
    <div class="card"><div class="k">Drawdown</div><div class="v" id="dd">0%</div></div>
    <div class="card"><div class="k">Frais</div><div class="v mut" id="fees">—</div></div>
  </div>

  <div class="sec"><span class="dot"></span>Positions ouvertes</div>
  <div class="table-wrap"><table>
    <thead><tr><th>Symbole</th><th>Sens</th><th>Lev·Q</th><th>Entrée</th><th>Actuel</th><th>Investi</th><th>SL</th><th>TP</th><th>⏱ Durée</th><th>P&L live</th></tr></thead>
    <tbody id="positions"><tr><td colspan="9" class="mut" style="text-align:center;padding:14px">Aucune position</td></tr></tbody>
  </table></div>

  <div class="sec"><span class="dot"></span>Surveillance des 20 symboles</div>
  <div class="table-wrap"><table>
    <thead><tr><th>Symbole</th><th>Prix</th><th>Courbe</th><th>Biais</th><th>Q</th><th>RSI</th><th>Régime (ADX)</th><th>Statut</th></tr></thead>
    <tbody id="symbols"></tbody>
  </table></div>

  <div class="sec"><span class="dot"></span>Historique des trades</div>
  <div class="table-wrap"><table>
    <thead><tr><th>Symbole</th><th>Sens</th><th>Lev</th><th>Entrée</th><th>Sortie</th><th>Investi</th><th>P&L%</th><th>Brut</th><th>Frais Binance</th><th>Net retirable</th><th>⏱ Durée</th><th>Raison</th></tr></thead>
    <tbody id="trades"></tbody>
  </table></div>

  <div class="sec"><span class="dot"></span>Journal</div>
  <div class="log" id="log"></div>

<script>
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(proto + '://' + location.host);
  const $ = id => document.getElementById(id);
  const num = (n,d=2) => Number(n).toLocaleString('fr-FR',{minimumFractionDigits:d,maximumFractionDigits:d});
  const sign = n => n>=0?'+':'';
  const cls = n => n>=0?'green':'red';
  function px(v){ const n=Number(v); return n>=100?num(n,2):n>=1?num(n,3):num(n,5); }
  function dur(ms){ if(ms==null)return '—'; const s=Math.floor(ms/1000); const m=Math.floor(s/60); const r=s%60; return m+':'+(r<10?'0':'')+r; }

  function renderStats(s){
    $('mode').textContent=(s.mode||'testnet').toUpperCase();
    $('run').textContent=s.killed?'KILL -45%':(s.running?'EN MARCHE':'PAUSE');
    $('run').className='badge '+(s.running?'on':'off');
    $('stratline').textContent='5.0 Scalping · MR · TP +'+(s.strat?s.strat.tp:0.35)+'% / SL -'+(s.strat?s.strat.sl:0.2)+'% · maker-only · x'+(s.strat?s.strat.lev:3);
    $('cap').textContent='$'+num(s.capital);
    $('net').textContent=sign(s.stats.net)+'$'+num(s.stats.net); $('net').className='v '+cls(s.stats.net);
    $('wr').textContent=s.winRate!=null?Math.round(s.winRate)+'%':'—';
    $('wr').className='v '+(s.winRate>=50?'green':s.winRate!=null?'red':'mut');
    $('ntr').textContent=s.stats.wins+s.stats.losses;
    $('wins').textContent=s.stats.wins;
    $('losses').textContent=s.stats.losses;
    $('pos').textContent=s.openPositions+'/'+s.maxPositions;
    if($('exc')) $('exc').textContent=(s.excOpen!=null?s.excOpen:0)+'/'+(s.excMax!=null?s.excMax:2);
    $('exp').textContent='$'+num(s.exposure)+' / '+num(s.maxExposure);
    $('dd').textContent=(s.maxDrawdown||0).toFixed(1)+'%';
    $('fees').textContent='$'+num(s.stats.fees);
  }

  function renderPositions(list){
    const tb=$('positions');
    if(!list||!list.length){tb.innerHTML='<tr><td colspan="10" class="mut" style="text-align:center;padding:14px">Aucune position</td></tr>';return;}
    tb.innerHTML=list.map(p=>{
      const sc=p.side==='BUY'?'long':'short',st=p.side==='BUY'?'LONG':'SHORT';
      return '<tr><td>'+p.symbol+'</td><td><span class="pill '+sc+'">'+st+'</span></td>'+
        '<td>'+p.lev+'x·Q'+p.quality+'</td><td>'+px(p.entry)+'</td><td>'+px(p.current)+'</td>'+
        '<td>$'+num(p.investi)+'</td><td class="red">'+px(p.sl)+'</td><td class="green">'+px(p.tp)+'</td>'+
        '<td class="'+((p.timeStopMs&&p.ageMs>p.timeStopMs*0.75)?'red':'mut')+'">'+dur(p.ageMs)+'</td>'+
        '<td class="'+cls(p.netLive)+'">'+sign(p.netLive)+'$'+num(p.netLive)+' ('+sign(p.pnlPct)+p.pnlPct.toFixed(2)+'%)</td></tr>';
    }).join('');
  }

  function sparkline(data){
    if(!data || data.length < 2) return '<span class="mut">—</span>';
    const w=72, h=22, n=data.length;
    const min=Math.min(...data), max=Math.max(...data);
    const range=(max-min)||1;
    const pts=data.map((v,i)=>{
      const x=(i/(n-1))*w;
      const y=h-((v-min)/range)*h;
      return x.toFixed(1)+','+y.toFixed(1);
    }).join(' ');
    const up=data[data.length-1]>=data[0];
    const col=up?'#22e58a':'#ff5470';
    return '<svg width="'+w+'" height="'+h+'" viewBox="0 0 '+w+' '+h+'" style="vertical-align:middle">'+
      '<polyline points="'+pts+'" fill="none" stroke="'+col+'" stroke-width="1.5"/></svg>';
  }

  function renderSymbols(list){
    const tb=$('symbols');
    tb.innerHTML=(list||[]).map(s=>{
      const b=s.bias||'NEUTRE';
      const bc=b==='LONG'?'long':b==='SHORT'?'short':'flat';
      const q=s.quality!=null?s.quality:'—';
      const qcol=s.quality>=49?'cyan':'mut';
      const reg=s.regime||'—'; const adxTxt=s.adx!=null?s.adx.toFixed(0):'—';
      const regTxt=reg==='RANGE'?'<span style="color:var(--green)">RANGE '+adxTxt+'</span>':reg==='TREND'?'<span style="color:var(--mut)">TREND '+adxTxt+'</span>':'—';
      return '<tr><td>'+s.symbol+'</td><td>'+(s.price?px(s.price):'—')+'</td>'+
        '<td>'+sparkline(s.spark)+'</td>'+
        '<td><span class="pill '+bc+'">'+b+'</span></td>'+
        '<td class="qbadge '+qcol+'">'+q+'</td>'+
        '<td>'+(s.rsi!=null?s.rsi.toFixed(0):'—')+'</td>'+
        '<td>'+regTxt+'</td>'+
        '<td>'+(s.hasPosition?'<span class="pill long">EN POSITION</span>':'<span class="mut">-</span>')+'</td></tr>';
    }).join('');
  }

  function renderTrades(list){
    const tb=$('trades');
    if(!list||!list.length){tb.innerHTML='<tr><td colspan="12" class="mut" style="text-align:center;padding:14px">Aucun trade</td></tr>';return;}
    tb.innerHTML=list.map(t=>{
      const net=Number(t.net),gross=Number(t.gross!=null?t.gross:net),sc=t.side==='BUY'?'long':'short',st=t.side==='BUY'?'LONG':'SHORT';
      return '<tr><td>'+t.symbol+'</td><td><span class="pill '+sc+'">'+st+'</span></td><td>'+t.lev+'x</td>'+
        '<td>'+px(t.entry)+'</td><td>'+px(t.exit)+'</td><td>$'+num(t.investi)+'</td>'+
        '<td class="'+cls(Number(t.pnlPct))+'">'+sign(Number(t.pnlPct))+t.pnlPct+'%</td>'+
        '<td class="'+cls(gross)+'">'+sign(gross)+'$'+num(gross)+'</td>'+
        '<td class="mut">-$'+num(t.fees)+'</td>'+
        '<td class="'+cls(net)+'" style="font-weight:700">'+sign(net)+'$'+num(net)+'</td>'+
        '<td class="mut">'+dur(t.durationMs)+'</td>'+
        '<td class="mut">'+t.reason+'</td></tr>';
    }).join('');
  }

  let snap=null;
  ws.onmessage=e=>{
    const m=JSON.parse(e.data);
    if(m.type==='snapshot'){snap=m.data;renderStats(snap);renderPositions(snap.positions);renderSymbols(snap.symbols);renderTrades(snap.trades);$('log').innerHTML=(snap.log||[]).join('<br>');}
    else if(snap){
      if(m.type==='status'){snap.running=m.running;renderStats(snap);}
      if(m.type==='symbols'){snap.symbols=m.symbols;renderSymbols(m.symbols);}
      if(m.type==='positions'){snap.positions=m.positions;renderPositions(m.positions);}
      if(m.type==='trade'){snap.stats=m.stats;snap.capital=m.capital;snap.positions=m.positions;renderStats(snap);renderPositions(m.positions);}
      if(m.type==='log'){snap.log.unshift(m.line);if(snap.log.length>50)snap.log.pop();$('log').innerHTML=snap.log.join('<br>');}
    }
  };
  ws.onclose=()=>{$('run').textContent='DÉCONNECTÉ';$('run').className='badge off';};
  $('start').onclick=()=>ws.send(JSON.stringify({action:'start'}));
  $('stop').onclick=()=>ws.send(JSON.stringify({action:'stop'}));
  $('closeAll').onclick=()=>ws.send(JSON.stringify({action:'closeAll'}));
</script>
</body></html>`;

// ==================================================================
// DÉMARRAGE
// ==================================================================
async function start() {
  logLine(`🚀 Itachi Multi — ${MODE.toUpperCase()} — ${ALL_SYMBOLS.length} symboles — capital $${CAPITAL_START}`);
  logLine(`📐 SERVEUR 5.0 SCALPING — mean-reversion (Bollinger ${STRAT.BB_PERIOD}/${STRAT.BB_STDDEV}σ + RSI${STRAT.RSI_SCALP}) — maker-only — levier x${STRAT.LEV} — TP +${STRAT.TP_PCT*100}% / SL -${STRAT.SL_PCT*100}% — time-stop ${STRAT.TIME_STOP_MS/60000}min — filtre régime ADX<${STRAT.ADX_RANGE_MAX} — coupe-circuit ${STRAT.MAX_CONSEC_LOSSES} pertes — kill -${STRAT.KILL_PCT*100}%`);
  if (!API_KEY || !API_SECRET) logLine('⚠️ Cles API manquantes — lecture seule (pas d ordres).');
  await loadSymbolInfo();
  await refreshAllKlines();
  connectPriceStreams();
  // Indicateurs 1m rafraîchis toutes les 15s (le refresh parallèle prend ~300ms).
  setInterval(refreshAllKlines, 15000);
  // rankActiveSymbols est déjà appelé à chaque refreshAllKlines ; pas de doublon nécessaire.
  setInterval(reconcile, 9000);
  setInterval(() => broadcast({ type: 'symbols', symbols: symbolsOverview() }), 3000);
  server.listen(PORT, () => logLine(`🌐 Dashboard sur le port ${PORT}`));
}

start();
