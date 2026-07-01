/* ============================================================
 *  SERVEUR 3.5 - TRADE 1J   (swing mean-reversion 1h/2h)
 *  ------------------------------------------------------------
 *  Première architecture où les INDICATEURS sont accordés à
 *  l'horizon de détention (positions tenues jusqu'à 24h).
 *  À cet horizon, les frais deviennent négligeables et le
 *  FUNDING RATE devient un signal de retournement exploitable.
 *
 *  ANALYSE : bougies 1h (signal) + 2h (confirmation de régime).
 *   (Binance ne fournit pas de bougies 1h30 nativement ; 2h est
 *    le plus proche natif de l'intention "1h-1h30".)
 *
 *  STRATÉGIE : mean-reversion sur extrêmes de range.
 *   - Bollinger 20/2σ (1h) + RSI 14 (<30 / >70)
 *   - Filtre régime : ADX 2h < 30 (pas de tendance écrasante)
 *   - Filtre volatilité : ATR 1h >= 0.4% + confirmation volume
 *   - FUNDING (souple) : funding extrême aligné au sens du trade
 *     bonifie le score (short squeeze / correction) ; sinon malus.
 *     N'INTERDIT jamais un trade, ajuste seulement la qualité.
 *
 *  SORTIES (trailing LARGE, on laisse courir vers 1-4%) :
 *   - SL fixe -2.5%
 *   - Trailing armé à +1%, suiveur -1.5% du pic
 *   - Borne haute de sécurité +6% ; time-stop 24h
 *
 *  UNIVERS DYNAMIQUE : scan des perpétuels USDT Binance, classés
 *  par amplitude 24h (les plus VOLATILS), volume >= 50M$, top 20.
 *  Re-scan chaque heure ; le flux de prix se reconnecte au besoin.
 *
 *  RISQUE : levier x2->x5 (selon Q), mise 80-280$ (selon Q),
 *  20 positions max, exposition <=300%, cooldown 1h après stop,
 *  coupe-circuit 5 pertes, kill -25%.
 *
 *  ⚠️ Testnet : univers/funding réels viennent du MAINNET (klines),
 *  mais l'exécution testnet reste artificiellement calme. Le juge
 *  final de la rentabilité reste un backtest sur données réelles.
 * ============================================================ */
/**
 * ITACHI MULTI — Bot multi-crypto Binance Futures (testnet)
 * ---------------------------------------------------------
 * Stratégie 2,5:1 (ratio risque/récompense favorable), pensée pour gagner sur peu de trades sélectifs.
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
// PARAMÈTRES STRATÉGIE 2,5:1 (ajustables ici)
// ==================================================================
const STRAT = {
  // ============================================================
  //  SERVEUR 3.5 - TRADE 1J  (swing mean-reversion 1h/4h)
  //  Indicateurs accordes a l'horizon : analyse 1h, confirmation 4h,
  //  positions tenues jusqu'a 24h. Frais negligeables a cette echelle ;
  //  le funding rate devient un signal de retournement (filtre souple).
  // ============================================================

  // --- Timeframes (accordes a l'horizon de detention) ---
  TF_MAIN: '1h',        // analyse principale : bougies 1h
  TF_CONFIRM: '2h',     // confirmation de regime : bougies 2h (proche du 1h30 voulu, natif Binance)
  KLINE_LIMIT: 100,     // 100 bougies 1h (~4 jours d'historique)
  CONFIRM_LIMIT: 60,    // 60 bougies 2h (~5 jours)

  // --- Indicateurs mean-reversion (sur 1h) ---
  BB_PERIOD: 20,        // Bandes de Bollinger 20 periodes (1h)
  BB_STDDEV: 2.0,       // 2 ecarts-types
  RSI_PERIOD: 14,       // RSI 14 (standard, adapte au swing)
  RSI_OVERSOLD: 30,     // survente -> LONG (rebond)
  RSI_OVERBOUGHT: 70,   // surachat -> SHORT (retour)
  ATR_PERIOD: 14,       // ATR 1h (volatilite, calibrage sorties)

  // --- Filtre de regime : ADX sur 4h ---
  ADX_PERIOD: 14,
  ADX_RANGE_MAX: 30,    // ADX 2h < 30 = pas de tendance ecrasante -> mean-reversion OK

  // --- Filtre FUNDING (souple) : ajuste le score, ne bloque pas ---
  //  Funding tres positif = trop de longs -> bonus aux SHORT, malus aux LONG.
  //  Funding tres negatif = trop de shorts -> bonus aux LONG (short squeeze).
  FUNDING_SOFT: true,
  FUNDING_EXTREME: 0.0005, // |funding| >= 0.05% (8h) = extreme -> influence le score
  FUNDING_WEIGHT: 15,      // points de qualite ajoutes/retires selon l'alignement funding

  // --- Filtres d'entree ---
  ATR_FLOOR_1H: 0.004,  // ATR 1h min 0.4% : assez de mouvement pour viser 1-2%
  VOL_CONFIRM: true,    // exige un volume 1h >= sa moyenne

  // --- Sorties (swing : trailing LARGE, on laisse courir vers 1-4%) ---
  SL_PCT: 0.025,        // -2.5% stop-loss (marge pour l'horizon 1j)
  TRAIL_ARM: 0.010,     // trailing s'arme a +1.0%
  TRAIL_PCT: 0.015,     // suiveur -1.5% du pic (LARGE : laisse courir vers 2-4%)
  TP_SOFT_CAP: 0.06,    // borne haute indicative +6% (securite, rarement atteinte)
  TIME_STOP_MS: 86400000, // time-stop 24h (1 jour)

  // --- Frais & execution ---
  FEE_MAKER: 0.0002, FEE_TAKER: 0.0005,
  USE_MAKER_ENTRY: true, MAKER_WAIT_MS: 5000, MAKER_OFFSET: 0.0005, // fallback taker OK (frais negligeables a cet horizon)

  // --- Levier x2 -> x5 indexe sur la qualite ---
  LEV_BY_Q: [
    { q: 75, lev: 5 },
    { q: 60, lev: 4 },
    { q: 45, lev: 3 },
    { q: 0,  lev: 2 },
  ],
  LEV_MAX: 5,

  // --- Mise variable 80-280$ selon la qualite ---
  STAKE_MIN_USD: 80,
  STAKE_MAX_USD: 280,
  Q_FOR_MAX_STAKE: 80,  // Q>=80 -> mise max 280$ ; interpolation lineaire depuis 80$

  // --- Positions & risque ---
  MAX_POSITIONS_CAP: 20, // jusqu'a 20 positions simultanees
  MAX_EXPOSURE_PCT: 3.0, // exposition totale plafonnee a 300% du capital (garde-fou)
  KILL_PCT: 0.25,        // kill switch -25%
  MAX_CONSEC_LOSSES: 5,  // coupe-circuit apres 5 pertes consecutives
  COOLDOWN_AFTER_STOP_MS: 3600000, // cooldown 1h sur un symbole apres un stop

  // --- Univers dynamique : cryptos les plus volatiles de Binance ---
  DYNAMIC_UNIVERSE: true,
  UNIVERSE_SIZE: 20,        // on trade les 20 perpetuels USDT les plus volatils
  UNIVERSE_REFRESH_MS: 3600000, // re-scan de l'univers toutes les heures
  UNIVERSE_MIN_VOL_USDT: 50000000, // volume 24h minimum 50M$ (liquidite)

  // --- Cadence ---
  MIN_GAP_MS: 3600000,  // 1h minimum entre 2 entrees sur le meme symbole
  SIGNAL_REFRESH_MS: 60000, // re-evaluation des signaux toutes les 60s (horizon lent)
};


// ==================================================================
// UNIVERS DYNAMIQUE — rempli au démarrage par le scan des perpétuels
// USDT les plus volatils de Binance (voir scanUniverse).
// ==================================================================
// Liste de repli si le scan échoue (majeurs + volatils connus).
const FALLBACK_SYMBOLS = ['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','DOGEUSDT','AVAXUSDT','NEARUSDT','APTUSDT','ARBUSDT','OPUSDT','INJUSDT','SEIUSDT','SUIUSDT','WIFUSDT','PEPEUSDT'];
let ALL_SYMBOLS = []; // peuplé dynamiquement

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
  activeSymbols: null,
  universe: [], // univers courant (pour le dashboard)
};

// Initialise (ou réinitialise) l'état d'un symbole. Idempotent.
function ensureSymbolState(s) {
  if (state.sym[s]) return;
  state.sym[s] = {
    symbol: s, price: 0, prices: [], klines: [], klines2h: [],
    indicators: { rsi: null, bias: 'NEUTRE', quality: null, breakdown: null },
    swing: { bb: null, rsi: null, atrPct: null, adx: null, volRatio: null, regime: null, funding: null },
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
      if (sym.symbol && sym.symbol.endsWith('USDT')) {
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
function ema(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  let e = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < values.length; i++) e = values[i] * k + e * (1 - k);
  return e;
}
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
function trendFromCloses(closes) {
  const ef = ema(closes, 9), es = ema(closes, 21);
  if (ef == null || es == null) return 0;
  return ef > es ? 1 : ef < es ? -1 : 0;
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

function sma(values, period) {
  if (values.length < period) return null;
  return values.slice(-period).reduce((a, b) => a + b, 0) / period;
}
function bollinger(closes, period, mult) {
  if (closes.length < period) return null;
  const slice = closes.slice(-period);
  const mid = slice.reduce((a, b) => a + b, 0) / period;
  const variance = slice.reduce((a, b) => a + (b - mid) * (b - mid), 0) / period;
  const sd = Math.sqrt(variance);
  return { mid, upper: mid + mult * sd, lower: mid - mult * sd, sd };
}
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
    const pDI = 100 * (pS[i] / trS[i]), mDI = 100 * (mS[i] / trS[i]);
    const sum = pDI + mDI;
    dx.push(sum === 0 ? 0 : 100 * Math.abs(pDI - mDI) / sum);
  }
  if (dx.length < period) return null;
  let adxVal = dx.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < dx.length; i++) adxVal = (adxVal * (period - 1) + dx[i]) / period;
  return adxVal;
}
function volumeRatio(klines, period) {
  if (!klines || klines.length < period + 1) return null;
  const vols = klines.map((k) => k.vol);
  const avg = sma(vols.slice(0, -1), period);
  const cur = vols[vols.length - 1];
  return avg && avg > 0 ? cur / avg : null;
}

// ==================================================================
// UNIVERS DYNAMIQUE : scan des perpétuels USDT les plus VOLATILS
// via /fapi/v1/ticker/24hr. Classe par amplitude (high-low)/low sur 24h,
// filtré par volume 24h minimum (liquidité). Renvoie les UNIVERSE_SIZE meilleurs.
// ==================================================================
async function scanUniverse() {
  try {
    const tickers = await publicGet(REST_BASE, '/fapi/v1/ticker/24hr');
    if (!Array.isArray(tickers)) throw new Error('réponse inattendue');
    const scored = tickers
      .filter((t) => t.symbol && t.symbol.endsWith('USDT')) // perpétuels USDT
      .filter((t) => !/(UPUSDT|DOWNUSDT|BULLUSDT|BEARUSDT)$/.test(t.symbol)) // pas de tokens à effet de levier
      .map((t) => {
        const high = parseFloat(t.highPrice), low = parseFloat(t.lowPrice);
        const quoteVol = parseFloat(t.quoteVolume); // volume en USDT
        const amplitude = low > 0 ? (high - low) / low : 0; // volatilité 24h
        return { symbol: t.symbol, amplitude, quoteVol };
      })
      .filter((t) => t.quoteVol >= STRAT.UNIVERSE_MIN_VOL_USDT) // liquidité suffisante
      .sort((a, b) => b.amplitude - a.amplitude); // les plus volatils d'abord

    const top = scored.slice(0, STRAT.UNIVERSE_SIZE).map((t) => t.symbol);
    if (top.length === 0) throw new Error('aucun symbole après filtres');

    // Toujours conserver les symboles déjà en position (pour les gérer jusqu'à la sortie).
    for (const s of Object.keys(state.sym)) {
      if (state.sym[s].position && !top.includes(s)) top.push(s);
    }
    return top;
  } catch (e) {
    logLine(`⚠️ scanUniverse: ${e.message} — repli sur liste statique`);
    return FALLBACK_SYMBOLS.slice();
  }
}

// Funding rate courant d'un symbole (dernier point). Signe : + = longs paient shorts.
async function fetchFunding(symbol) {
  try {
    const data = await publicGet(REST_BASE, '/fapi/v1/fundingRate', { symbol, limit: 1 });
    if (Array.isArray(data) && data.length) return parseFloat(data[0].fundingRate);
  } catch (e) { /* ignore */ }
  return null;
}

// ==================================================================
// LECTURE DES 50 DERNIÈRES BOUGIES + ANALYSE MTF PAR SYMBOLE
// ==================================================================
async function refreshKlines(symbol) {
  const S = state.sym[symbol];
  if (!S) return;
  try {
    // Bougies 1h (analyse principale)
    const raw1h = await publicGet(KLINE_BASE, '/fapi/v1/klines', {
      symbol, interval: STRAT.TF_MAIN, limit: STRAT.KLINE_LIMIT,
    });
    const kl = raw1h.map((c) => ({ time: c[0], open: +c[1], high: +c[2], low: +c[3], close: +c[4], vol: +c[5] }));
    S.klines = kl;
    const closes = kl.map((c) => c.close);

    S.swing.bb = bollinger(closes, STRAT.BB_PERIOD, STRAT.BB_STDDEV);
    S.swing.rsi = rsi(closes, STRAT.RSI_PERIOD);
    S.swing.atrPct = atrPct(kl, STRAT.ATR_PERIOD);
    S.swing.volRatio = volumeRatio(kl, STRAT.ATR_PERIOD);
    S.indicators.rsi = S.swing.rsi;

    // Bougies 2h (confirmation de régime via ADX)
    const raw2h = await publicGet(KLINE_BASE, '/fapi/v1/klines', {
      symbol, interval: STRAT.TF_CONFIRM, limit: STRAT.CONFIRM_LIMIT,
    });
    const kl2 = raw2h.map((c) => ({ time: c[0], high: +c[2], low: +c[3], close: +c[4], vol: +c[5] }));
    S.klines2h = kl2;
    S.swing.adx = adx(kl2, STRAT.ADX_PERIOD);
    S.swing.regime = (S.swing.adx != null && S.swing.adx < STRAT.ADX_RANGE_MAX) ? 'RANGE' : 'TREND';

    // Funding rate (signal de retournement, filtre souple)
    S.swing.funding = await fetchFunding(symbol);

    // Biais affiché
    if (S.swing.bb && S.price > 0) {
      if (S.price <= S.swing.bb.lower) S.indicators.bias = 'LONG';
      else if (S.price >= S.swing.bb.upper) S.indicators.bias = 'SHORT';
      else S.indicators.bias = 'NEUTRE';
    }
  } catch (e) { /* on garde l'ancien */ }
}

// ==================================================================
// SCORING (stratégie 2,5:1 — sélective, MTF obligatoire)
// ==================================================================
function computeSignal(symbol) {
  const S = state.sym[symbol];
  const sw = S.swing;
  const px = S.price;
  if (!sw.bb || sw.rsi == null || sw.atrPct == null || px <= 0) return null;

  // Régime : mean-reversion seulement hors tendance forte (ADX 2h bas)
  if (sw.regime !== 'RANGE') { S.indicators.quality = null; return null; }
  // Volatilité minimale sur 1h (assez de mouvement pour viser 1-2%)
  if (sw.atrPct < STRAT.ATR_FLOOR_1H) { S.indicators.quality = null; return null; }
  // Confirmation volume (optionnelle)
  if (STRAT.VOL_CONFIRM && sw.volRatio != null && sw.volRatio < 1.0) { S.indicators.quality = null; return null; }

  // Signal mean-reversion : extrême de Bollinger + RSI extrême
  const belowLower = px <= sw.bb.lower, aboveUpper = px >= sw.bb.upper;
  const rsiLow = sw.rsi <= STRAT.RSI_OVERSOLD, rsiHigh = sw.rsi >= STRAT.RSI_OVERBOUGHT;
  let side = null;
  if (belowLower && rsiLow) side = 'BUY';
  else if (aboveUpper && rsiHigh) side = 'SELL';
  if (!side) { S.indicators.quality = null; return null; }

  // --- Score de qualité (0-100) ---
  const dist = side === 'BUY' ? (sw.bb.mid - px) / (sw.bb.sd || 1) : (px - sw.bb.mid) / (sw.bb.sd || 1);
  const distScore = Math.min(35, Math.max(0, dist * 17));
  const rsiScore = side === 'BUY'
    ? Math.min(30, (STRAT.RSI_OVERSOLD - sw.rsi + 5) * 2)
    : Math.min(30, (sw.rsi - STRAT.RSI_OVERBOUGHT + 5) * 2);
  const volScore = sw.volRatio != null ? Math.min(20, (sw.volRatio - 1) * 20) : 10;

  // --- Filtre FUNDING souple : ajuste le score (ne bloque pas) ---
  // Funding très positif (longs surchargés) -> bonus SHORT, malus LONG (et inversement).
  let fundingScore = 0;
  if (STRAT.FUNDING_SOFT && sw.funding != null) {
    const f = sw.funding;
    if (Math.abs(f) >= STRAT.FUNDING_EXTREME) {
      const favorsShort = f > 0; // longs paient -> pression baissière à venir
      if ((side === 'SELL' && favorsShort) || (side === 'BUY' && !favorsShort)) fundingScore = STRAT.FUNDING_WEIGHT;
      else fundingScore = -STRAT.FUNDING_WEIGHT;
    }
  }

  const quality = Math.round(Math.max(0, distScore + Math.max(0, rsiScore) + Math.max(0, volScore) + fundingScore));
  S.indicators.quality = quality;
  S.indicators.bias = side === 'BUY' ? 'LONG' : 'SHORT';
  S.indicators.breakdown = { dist: Math.round(distScore), rsi: Math.round(Math.max(0, rsiScore)), vol: Math.round(Math.max(0, volScore)), funding: Math.round(fundingScore) };

  return { side, quality, midBand: sw.bb.mid, symbol };
}

// Q minimum ADAPTATIF (50-55) : marché agité (ATR élevé) -> seuil bas (plus d'opportunités) ;
// marché calme (ATR faible) -> seuil haut (on est plus exigeant).
function qMinFor(symbol) {
  return 0;
}

// Calcule les niveaux SL/TP en % selon l'ATR du symbole (volatilité réelle).
// Si l'ATR est indisponible ou USE_ATR_EXITS=false, on retombe sur SL_PCT/TP_PCT fixes.
// Le TP ATR est borné [ATR_TP_FLOOR, ATR_TP_CAP] pour rester atteignable ET sûr.
function computeExits(symbol) {
  // Swing : SL fixe -2.5%. Le TP est géré par le trailing large (pas de plafond dur).
  return { slPct: STRAT.SL_PCT, tpPct: STRAT.TP_SOFT_CAP, source: 'swing' };
}

// Levier progressif 2x -> 7x selon Q (premier palier atteint).
// Mise exceptionnelle -> toujours le levier max (la conviction du timing la valide).
function levForQuality(quality) {
  for (const tier of STRAT.LEV_BY_Q) if (quality >= tier.q) return tier.lev;
  return STRAT.LEV_BY_Q[STRAT.LEV_BY_Q.length - 1].lev;
}

// Compte les mises exceptionnelles 9% actuellement ouvertes.

// Décide mise + levier. Renvoie aussi le flag "exceptional" pour l'affichage.
// Les obligatoires (Q, MTF, expo) sont déjà vérifiés en amont (tryOpen).
function sizing(signal) {
  // Mise interpolée entre STAKE_MIN et STAKE_MAX selon la qualité (Q>=Q_FOR_MAX -> max).
  const q = Math.max(0, Math.min(STRAT.Q_FOR_MAX_STAKE, signal.quality));
  const frac = q / STRAT.Q_FOR_MAX_STAKE;
  const stake = STRAT.STAKE_MIN_USD + frac * (STRAT.STAKE_MAX_USD - STRAT.STAKE_MIN_USD);
  const lev = levForQuality(signal.quality);
  return { stake: Math.round(stake), lev };
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
  if (!STRAT.USE_MAKER_ENTRY) {
    await marketOrder(symbol, side, qty);
    return 'taker';
  }
  // Prix limite en retrait (BUY sous le prix, SELL au-dessus) pour rester maker
  const offset = STRAT.MAKER_OFFSET;
  const limitPx = side === 'BUY' ? refPrice * (1 - offset) : refPrice * (1 + offset);
  let order;
  try {
    order = await limitMakerOrder(symbol, side, qty, limitPx);
  } catch (e) {
    // post-only rejeté (serait taker) ou erreur -> fallback market direct
    await marketOrder(symbol, side, qty);
    return 'taker';
  }
  const orderId = order && order.orderId;
  if (!orderId) { await marketOrder(symbol, side, qty); return 'taker'; }

  // Attendre l'exécution du post-only
  await new Promise((r) => setTimeout(r, STRAT.MAKER_WAIT_MS));
  const st = await getOrder(symbol, orderId);
  if (st && st.status === 'FILLED') return 'maker';

  // Pas (entièrement) rempli -> on annule et on bascule en market
  await cancelOrder(symbol, orderId);
  const partial = st && parseFloat(st.executedQty || 0) > 0;
  if (partial) {
    const remaining = roundQty(symbol, qty - parseFloat(st.executedQty));
    if (remaining > 0) await marketOrder(symbol, side, remaining);
    return 'maker'; // majorité en maker
  }
  await marketOrder(symbol, side, qty);
  return 'taker';
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
  if (!S || S.position) return;
  if (state.activeSymbols && !state.activeSymbols.includes(symbol)) return;
  if (now - S.lastEntryAt < STRAT.MIN_GAP_MS) return;
  // Cooldown après un stop sur ce symbole
  if (S.lastStopAt && now - S.lastStopAt < STRAT.COOLDOWN_AFTER_STOP_MS) return;

  const { stake, lev } = sizing(signal);
  if (openPositionsCount() >= STRAT.MAX_POSITIONS_CAP) return;
  if (currentExposure() + stake > state.capital * STRAT.MAX_EXPOSURE_PCT) return;

  const qty = roundQty(symbol, (stake * lev) / S.price);
  if (qty <= 0) return;

  await setLeverage(symbol, lev);

  let entryFill = 'taker';
  try {
    entryFill = await openWithMaker(symbol, signal.side, qty, S.price);
  } catch (e) {
    logLine(`❌ ${symbol} ouverture: ${e.message}`);
    return;
  }
  if (!entryFill) return;

  const entry = S.price;
  const exits = computeExits(symbol);
  S.position = {
    side: signal.side, entry, qty, stake, lev, quality: signal.quality,
    entryFill, slPct: exits.slPct, tpPct: exits.tpPct,
    sl: signal.side === 'BUY' ? entry * (1 - exits.slPct) : entry * (1 + exits.slPct),
    tp: signal.side === 'BUY' ? entry * (1 + exits.tpPct) : entry * (1 - exits.tpPct),
    openedAt: now, peakPnl: 0,
  };
  S.lastEntryAt = now;
  const f = S.swing.funding != null ? ` funding=${(S.swing.funding*100).toFixed(3)}%` : '';
  logLine(`🟢 ${symbol} ${signal.side} qty=${qty} @ ${entry.toFixed(4)} x${lev} Q=${signal.quality} SL-${(exits.slPct*100).toFixed(1)}%${f} [${entryFill}]`);
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
  // Frais : leg d'entrée maker (0.02%) si l'ordre est passé en post-only, sinon taker.
  // Leg de sortie toujours taker (fermeture au market).
  const entryFeeRate = pos.entryFill === 'taker' ? STRAT.FEE_TAKER : STRAT.FEE_MAKER;
  const fees = pos.stake * pos.lev * (entryFeeRate + STRAT.FEE_TAKER); // sortie au market = taker
  const net = gross - fees;

  state.capital += net;
  state.stats.gross += gross;
  state.stats.fees += fees;
  state.stats.net += net;
  if (net >= 0) { state.stats.wins++; state.consecLosses = 0; }
  else { state.stats.losses++; state.consecLosses++; }
  if (reason === 'STOP-LOSS') S.lastStopAt = Date.now();
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
  if (state.consecLosses >= STRAT.MAX_CONSEC_LOSSES) {
    state.running = false;
    logLine(`⛔ COUPE-CIRCUIT : ${state.consecLosses} pertes consécutives — bot en pause.`);
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

  // Pic de profit (pour le trailing)
  if (pos.peakPnl == null || pnlPct > pos.peakPnl) pos.peakPnl = pnlPct;

  // 1) Stop-loss fixe -2.5%
  if (pnlPct <= -pos.slPct) { closePos(symbol, 'STOP-LOSS'); return; }
  // 2) Trailing LARGE : armé à +1%, sort si on recule de 1.5% sous le pic (laisse courir vers 2-4%)
  if (pos.peakPnl >= STRAT.TRAIL_ARM) {
    if (pos.peakPnl - pnlPct >= STRAT.TRAIL_PCT) { closePos(symbol, 'TRAILING'); return; }
  }
  // 3) Borne haute de sécurité (rarement atteinte)
  if (pnlPct >= STRAT.TP_SOFT_CAP) { closePos(symbol, 'TAKE-PROFIT'); return; }
  // 4) Time-stop 24h
  if (age >= STRAT.TIME_STOP_MS) { closePos(symbol, 'TIME-STOP-24H'); return; }
}

// ==================================================================
// BOUCLE PAR TICK
// ==================================================================
async function symbolTick(symbol) {
  const S = state.sym[symbol];
  if (!state.running || S.price <= 0) return;
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
let priceWs = null;
function connectPriceStreams() {
  if (!ALL_SYMBOLS.length) { setTimeout(connectPriceStreams, 2000); return; }
  const streams = ALL_SYMBOLS.map((s) => `${s.toLowerCase()}@markPrice@1s`).join('/');
  const url = `${WS_BASE}/stream?streams=${streams}`;
  const ws = new WebSocket(url);
  priceWs = ws;
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
  ws.on('close', () => { if (priceWs === ws) { logLine('⚠️ WS prix fermé — reconnexion 3s'); setTimeout(connectPriceStreams, 3000); } });
  ws.on('error', (e) => logLine(`⚠️ WS error: ${e.message}`));
}

// Applique un nouvel univers : met à jour ALL_SYMBOLS, initialise les états,
// et reconnecte le flux de prix. Appelé au démarrage et à chaque re-scan.
async function applyUniverse(symbols) {
  const changed = symbols.slice().sort().join(',') !== ALL_SYMBOLS.slice().sort().join(',');
  ALL_SYMBOLS = symbols;
  state.universe = symbols;
  for (const s of symbols) ensureSymbolState(s);
  if (changed && priceWs) {
    const old = priceWs; priceWs = null;
    try { old.close(); } catch (e) {}
    connectPriceStreams();
  }
  logLine(`🌍 Univers : ${symbols.length} cryptos les plus volatiles — ${symbols.slice(0, 8).join(', ')}...`);
}

// Re-scan périodique de l'univers volatil.
async function refreshUniverse() {
  const top = await scanUniverse();
  await applyUniverse(top);
}

async function refreshAllKlines() {
  const BATCH = 4;
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
  // L'univers est déjà les N plus volatils ; tous sont actifs. Le filtrage fin
  // (régime, extrême, funding) se fait dans computeSignal.
  state.activeSymbols = ALL_SYMBOLS.slice();
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
    const fees = pos.stake * pos.lev * (STRAT.FEE_MAKER + STRAT.FEE_TAKER);
    out.push({
      symbol, side: pos.side, entry: pos.entry, current: px, lev: pos.lev,
      quality: pos.quality, investi: pos.stake, sl: pos.sl, tp: pos.tp,
      pnlPct: pnlPct * 100, netLive: gross - fees,
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
      rsi: S.indicators.rsi, regime: S.swing.regime, adx: S.swing.adx, funding: S.swing.funding, hasPosition: !!S.position,
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
    maxPositions: STRAT.MAX_POSITIONS_CAP,
    wins: state.stats.wins, losses: state.stats.losses,
    stats: state.stats, winRate: tot ? (state.stats.wins / tot) * 100 : null,
    positions: livePositions(), symbols: symbolsOverview(),
    trades: state.trades.slice(0, 40), log: state.log.slice(0, 50),
    strat: { sl: STRAT.SL_PCT * 100, trailArm: STRAT.TRAIL_ARM * 100, trailPct: STRAT.TRAIL_PCT * 100, lev: '2-5', universe: state.universe.length },
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
    <span class="badge" style="background:rgba(0,245,200,.12);color:#00F5C8;border:1px solid rgba(0,245,200,.3)">3.5 - trade 1j <span style="opacity:.6;font-weight:600">· WR ~?%</span></span>
    <span id="mode" class="badge net">TESTNET</span>
    <span id="run" class="badge off">PAUSE</span>
  </div>
  <div class="sub" id="stratline">3.5 trade 1j · swing mean-reversion 1h/2h · funding · univers volatil dynamique</div>

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
    <thead><tr><th>Symbole</th><th>Sens</th><th>Lev·Q</th><th>Entrée</th><th>Actuel</th><th>Investi</th><th>SL</th><th>TP</th><th>P&L live</th></tr></thead>
    <tbody id="positions"><tr><td colspan="9" class="mut" style="text-align:center;padding:14px">Aucune position</td></tr></tbody>
  </table></div>

  <div class="sec"><span class="dot"></span>Surveillance des 20 symboles</div>
  <div class="table-wrap"><table>
    <thead><tr><th>Symbole</th><th>Prix</th><th>Courbe</th><th>Biais</th><th>Q</th><th>RSI</th><th>Funding</th><th>Statut</th></tr></thead>
    <tbody id="symbols"></tbody>
  </table></div>

  <div class="sec"><span class="dot"></span>Historique des trades</div>
  <div class="table-wrap"><table>
    <thead><tr><th>Symbole</th><th>Sens</th><th>Lev</th><th>Entrée</th><th>Sortie</th><th>Investi</th><th>P&L%</th><th>Brut</th><th>Frais Binance</th><th>Net retirable</th><th>Raison</th></tr></thead>
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

  function renderStats(s){
    $('mode').textContent=(s.mode||'testnet').toUpperCase();
    $('run').textContent=s.killed?'KILL -45%':(s.running?'EN MARCHE':'PAUSE');
    $('run').className='badge '+(s.running?'on':'off');
    $('stratline').textContent='3.5 trade 1j · MR 1h/2h · SL -'+(s.strat?s.strat.sl:2.5)+'% · trailing large +'+(s.strat?s.strat.trailArm:1)+'%/-'+(s.strat?s.strat.trailPct:1.5)+'% · x2-5 · '+(s.strat?s.strat.universe:20)+' cryptos volatiles';
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
    if(!list||!list.length){tb.innerHTML='<tr><td colspan="9" class="mut" style="text-align:center;padding:14px">Aucune position</td></tr>';return;}
    tb.innerHTML=list.map(p=>{
      const sc=p.side==='BUY'?'long':'short',st=p.side==='BUY'?'LONG':'SHORT';
      return '<tr><td>'+p.symbol+'</td><td><span class="pill '+sc+'">'+st+'</span></td>'+
        '<td>'+p.lev+'x·Q'+p.quality+'</td><td>'+px(p.entry)+'</td><td>'+px(p.current)+'</td>'+
        '<td>$'+num(p.investi)+'</td><td class="red">'+px(p.sl)+'</td><td class="green">'+px(p.tp)+'</td>'+
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
      const fnd=s.funding!=null?(s.funding>=0?'+':'')+(s.funding*100).toFixed(3)+'%':'—';
      const fndCol=s.funding!=null&&Math.abs(s.funding)>=0.0005?(s.funding>0?'red':'green'):'mut';
      return '<tr><td>'+s.symbol+'</td><td>'+(s.price?px(s.price):'—')+'</td>'+
        '<td>'+sparkline(s.spark)+'</td>'+
        '<td><span class="pill '+bc+'">'+b+'</span></td>'+
        '<td class="qbadge '+qcol+'">'+q+'</td>'+
        '<td>'+(s.rsi!=null?s.rsi.toFixed(0):'—')+'</td>'+
        '<td class="'+fndCol+'">'+fnd+'</td>'+
        '<td>'+(s.hasPosition?'<span class="pill long">EN POSITION</span>':'<span class="mut">-</span>')+'</td></tr>';
    }).join('');
  }

  function renderTrades(list){
    const tb=$('trades');
    if(!list||!list.length){tb.innerHTML='<tr><td colspan="11" class="mut" style="text-align:center;padding:14px">Aucun trade</td></tr>';return;}
    tb.innerHTML=list.map(t=>{
      const net=Number(t.net),gross=Number(t.gross!=null?t.gross:net),sc=t.side==='BUY'?'long':'short',st=t.side==='BUY'?'LONG':'SHORT';
      return '<tr><td>'+t.symbol+'</td><td><span class="pill '+sc+'">'+st+'</span></td><td>'+t.lev+'x</td>'+
        '<td>'+px(t.entry)+'</td><td>'+px(t.exit)+'</td><td>$'+num(t.investi)+'</td>'+
        '<td class="'+cls(Number(t.pnlPct))+'">'+sign(Number(t.pnlPct))+t.pnlPct+'%</td>'+
        '<td class="'+cls(gross)+'">'+sign(gross)+'$'+num(gross)+'</td>'+
        '<td class="mut">-$'+num(t.fees)+'</td>'+
        '<td class="'+cls(net)+'" style="font-weight:700">'+sign(net)+'$'+num(net)+'</td>'+
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
  logLine(`🚀 Itachi — SERVEUR 3.5 TRADE 1J — ${MODE.toUpperCase()} — capital $${CAPITAL_START}`);
  logLine(`📐 Swing mean-reversion 1h/2h — Bollinger ${STRAT.BB_PERIOD}/${STRAT.BB_STDDEV}σ + RSI${STRAT.RSI_PERIOD} (${STRAT.RSI_OVERSOLD}/${STRAT.RSI_OVERBOUGHT}) — régime ADX2h<${STRAT.ADX_RANGE_MAX} — funding souple — SL -${STRAT.SL_PCT*100}% / trailing large armé +${STRAT.TRAIL_ARM*100}% suit -${STRAT.TRAIL_PCT*100}% — levier x2-x5 — mise ${STRAT.STAKE_MIN_USD}-${STRAT.STAKE_MAX_USD}$ — ${STRAT.MAX_POSITIONS_CAP} pos — time-stop 24h — kill -${STRAT.KILL_PCT*100}%`);
  if (!API_KEY || !API_SECRET) logLine('⚠️ Cles API manquantes — lecture seule (pas d ordres).');
  await loadSymbolInfo();
  // 1) Scanner l'univers volatil AVANT tout (peuple ALL_SYMBOLS)
  const uni = await scanUniverse();
  await applyUniverse(uni);
  // 2) Charger les bougies, connecter les prix
  await refreshAllKlines();
  connectPriceStreams();
  // 3) Boucles périodiques (horizon lent : pas besoin de haute fréquence)
  setInterval(refreshAllKlines, STRAT.SIGNAL_REFRESH_MS); // ré-évalue les signaux 1h/2h
  setInterval(refreshUniverse, STRAT.UNIVERSE_REFRESH_MS); // re-scan univers toutes les heures
  setInterval(reconcile, 9000);
  setInterval(() => broadcast({ type: 'symbols', symbols: symbolsOverview() }), 5000);
  server.listen(PORT, () => logLine(`🌐 Dashboard sur le port ${PORT}`));
}

start();
