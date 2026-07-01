/* ============================================================
 *  SERVEUR 3.8 - 1J - MAJ  (correctif démarrage + WebSocket robuste)
 *  ------------------------------------------------------------
 *  2 correctifs de fiabilité (stratégie inchangée) :
 *   1. Le serveur HTTP démarre EN PREMIER (avant les appels réseau) :
 *      le dashboard s'affiche tout de suite, l'init (klines 28 sym,
 *      scan, flux) se fait en arrière-plan non bloquant (try/catch).
 *   2. WebSocket dashboard avec RECONNEXION AUTOMATIQUE : si Railway
 *      coupe une connexion WS inactive, le client se reconnecte seul
 *      (1.5s). Envois protégés (wsSend) : un clic n'est plus perdu
 *      si la connexion n'est pas prête. -> corrige "le bouton Lancer
 *      ne fait rien / reste en pause".
 *
 *  Univers 28 = noyau fixe (BTC/ETH/BNB/SOL/DOGE) + 23 volatils.
 *  25 positions, rotation Q68, clôture manuelle, assoupli togglable,
 *  multi-régime (RANGE/UP/DOWN), réconciliation, chrono, scaling out.
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
  RSI_OVERSOLD: 35,     // survente -> LONG (assoupli 30->35 : un peu plus d'entrées)
  RSI_OVERBOUGHT: 65,   // surachat -> SHORT (assoupli 70->65)

  // --- Assouplissement d'entrée en RANGE (togglable à chaud) ---
  RELAX_RANGE_ENTRY: true,
  RSI_EXTREME_LOW: 25,     // RSI <= 25 -> LONG même sans toucher la bande basse
  RSI_EXTREME_HIGH: 75,    // RSI >= 75 -> SHORT même sans toucher la bande haute
  ATR_PERIOD: 14,       // ATR 1h (volatilite, calibrage sorties)

  // --- Filtre de regime : ADX sur 4h ---
  ADX_PERIOD: 14,
  ADX_RANGE_MAX: 35,    // ADX 2h < 35 (assoupli 30->35 : accepte marchés un peu plus directionnels)

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
  TP_SOFT_CAP: 0.35,    // borne haute indicative +35% (securite, rarement atteinte)
  TIME_STOP_MS: 86400000, // time-stop 24h (1 jour)

  // --- SCALING OUT : prise de profit PARTIELLE, on laisse courir le reste ---
  SCALE_OUT: [
    { at: 0.10, frac: 0.34 }, // a +10% : ferme 34% de la position
    { at: 0.20, frac: 0.50 }, // a +20% : ferme 50% du RESTE
    // le solde court sur le trailing large (capture les +20-35% comme NFPUSDT)
  ],

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
  MAX_POSITIONS_CAP: 25, // jusqu'a 25 positions simultanees
  MAX_EXPOSURE_PCT: 6.0, // exposition relevee a 600% (garde-fou)

  // --- ROTATION DE CAPITAL : fermer un mini-perdant essouffle pour un slot EXCELLENT ---
  ROTATION_ENABLED: true,
  ROTATION_MAX_LOSS: 0.005,     // trade a fermer entre 0 et -0.5%
  ROTATION_MIN_AGE_MS: 1800000, // ouvert >= 30 min
  ROTATION_STALE_PEAK: 0.005,   // n'a jamais depasse +0.5%
  ROTATION_MIN_Q: 68,           // signal candidat Q >= 68
  ROTATION_COOLDOWN_MS: 1800000,// 1 rotation/symbole/30 min
  KILL_PCT: 0.25,        // kill switch -25%
  MAX_CONSEC_LOSSES: 5,  // coupe-circuit apres 5 pertes consecutives
  COOLDOWN_AFTER_STOP_MS: 3600000, // cooldown 1h sur un symbole apres un stop

  // --- Univers dynamique : cryptos les plus volatiles de Binance ---
  DYNAMIC_UNIVERSE: true,
  UNIVERSE_SIZE: 28,        // 28 total = 5 fixes (noyau) + 23 volatils dynamiques
  CORE_SYMBOLS: ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'DOGEUSDT'], // noyau TOUJOURS présent
  DYNAMIC_SIZE: 23,         // nb de volatils dynamiques (hors noyau)
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
        // MARKET_LOT_SIZE borne la quantité d'un ordre MARKET (souvent < LOT_SIZE).
        const mlot = sym.filters.find((f) => f.filterType === 'MARKET_LOT_SIZE');
        // On prend la borne la plus restrictive entre les deux filtres.
        const maxLot = lot ? parseFloat(lot.maxQty) : Infinity;
        const maxMarket = mlot ? parseFloat(mlot.maxQty) : Infinity;
        const minLot = lot ? parseFloat(lot.minQty) : 0;
        SYMBOL_INFO[sym.symbol] = {
          qtyPrecision: sym.quantityPrecision,
          pricePrecision: sym.pricePrecision,
          stepSize: lot ? parseFloat(lot.stepSize) : 0.001,
          maxQty: Math.min(maxLot, maxMarket), // borne dure par ordre
          minQty: minLot,
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
function maxQtyFor(symbol) {
  const info = SYMBOL_INFO[symbol];
  return info && isFinite(info.maxQty) ? info.maxQty : Infinity;
}
function minQtyFor(symbol) {
  const info = SYMBOL_INFO[symbol];
  return info ? info.minQty : 0;
}

// Ferme une quantité donnée en la DÉCOUPANT en plusieurs ordres MARKET reduceOnly
// si elle dépasse le maxQty autorisé par Binance (corrige le -4005). Garde-fou anti-boucle.
async function closeQtyInChunks(symbol, closeSide, totalQty) {
  const maxQ = maxQtyFor(symbol);
  let remaining = totalQty;
  let guard = 0;
  const MAX_CHUNKS = 20; // anti-boucle : jamais plus de 20 ordres
  while (remaining > 0 && guard < MAX_CHUNKS) {
    guard++;
    let chunk = isFinite(maxQ) ? Math.min(remaining, maxQ) : remaining;
    chunk = roundQty(symbol, chunk);
    if (chunk <= 0) break;
    await marketOrder(symbol, closeSide, chunk, true);
    remaining = roundQty(symbol, remaining - chunk);
  }
  if (remaining > 0) {
    logLine(`⚠️ ${symbol} : fermeture partielle (reste ${remaining} après ${guard} tranches).`);
    return false;
  }
  return true;
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
  let lastP = 0, lastM = 0;
  for (let i = 0; i < trS.length; i++) {
    const pDI = 100 * (pS[i] / trS[i]), mDI = 100 * (mS[i] / trS[i]);
    lastP = pDI; lastM = mDI;
    const sum = pDI + mDI;
    dx.push(sum === 0 ? 0 : 100 * Math.abs(pDI - mDI) / sum);
  }
  if (dx.length < period) return null;
  let adxVal = dx.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < dx.length; i++) adxVal = (adxVal * (period - 1) + dx[i]) / period;
  // Direction : +1 tendance haussière (DI+ > DI-), -1 baissière.
  const dir = lastP >= lastM ? 1 : -1;
  return { adx: adxVal, dir, plusDI: lastP, minusDI: lastM };
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
    const core = STRAT.CORE_SYMBOLS;
    const scored = tickers
      .filter((t) => t.symbol && t.symbol.endsWith('USDT'))
      .filter((t) => !/(UPUSDT|DOWNUSDT|BULLUSDT|BEARUSDT)$/.test(t.symbol))
      .filter((t) => !core.includes(t.symbol))
      .map((t) => {
        const high = parseFloat(t.highPrice), low = parseFloat(t.lowPrice);
        const quoteVol = parseFloat(t.quoteVolume);
        const amplitude = low > 0 ? (high - low) / low : 0;
        return { symbol: t.symbol, amplitude, quoteVol };
      })
      .filter((t) => t.quoteVol >= STRAT.UNIVERSE_MIN_VOL_USDT)
      .sort((a, b) => b.amplitude - a.amplitude);
    const volatile = scored.slice(0, STRAT.DYNAMIC_SIZE).map((t) => t.symbol);
    const top = [...core, ...volatile];
    if (top.length === 0) throw new Error('aucun symbole après filtres');
    for (const s of Object.keys(state.sym)) {
      if (state.sym[s].position && !top.includes(s)) top.push(s);
    }
    return top;
  } catch (e) {
    logLine(`⚠️ scanUniverse: ${e.message} — repli sur noyau + statique`);
    return [...STRAT.CORE_SYMBOLS, ...FALLBACK_SYMBOLS.filter((s) => !STRAT.CORE_SYMBOLS.includes(s))];
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
    const adxObj = adx(kl2, STRAT.ADX_PERIOD);
    S.swing.adx = adxObj ? adxObj.adx : null;
    S.swing.adxDir = adxObj ? adxObj.dir : 0; // +1 haussier, -1 baissier
    // Régime à 3 états (jamais de pause) :
    //  RANGE (ADX bas) -> mean-reversion 2 sens
    //  UP  (ADX haut + DI+>DI-) -> tendance haussière : on ne prend que des LONG
    //  DOWN(ADX haut + DI->DI+) -> tendance baissière : on ne prend que des SHORT
    if (S.swing.adx == null) S.swing.regime = null;
    else if (S.swing.adx < STRAT.ADX_RANGE_MAX) S.swing.regime = 'RANGE';
    else S.swing.regime = S.swing.adxDir > 0 ? 'UP' : 'DOWN';

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
  if (!sw.bb || sw.rsi == null || sw.atrPct == null || px <= 0 || !sw.regime) return null;

  // Volatilité minimale (assez de mouvement pour viser 1-4%)
  if (sw.atrPct < STRAT.ATR_FLOOR_1H) { S.indicators.quality = null; return null; }
  if (STRAT.VOL_CONFIRM && sw.volRatio != null && sw.volRatio < 1.0) { S.indicators.quality = null; return null; }

  const belowLower = px <= sw.bb.lower, aboveUpper = px >= sw.bb.upper;
  const belowMid = px < sw.bb.mid, aboveMid = px > sw.bb.mid;
  const rsiLow = sw.rsi <= STRAT.RSI_OVERSOLD, rsiHigh = sw.rsi >= STRAT.RSI_OVERBOUGHT;

  let side = null;
  let mode = sw.regime;

  // ================= LOGIQUE MULTI-RÉGIME (jamais de pause) =================
  if (sw.regime === 'RANGE') {
    // Mean-reversion : fade les extrêmes (base stricte).
    if (belowLower && rsiLow) side = 'BUY';
    else if (aboveUpper && rsiHigh) side = 'SELL';
    // Assouplissement (togglable) : RSI TRÈS extrême suffit, même sans toucher la bande.
    else if (STRAT.RELAX_RANGE_ENTRY) {
      if (sw.rsi <= STRAT.RSI_EXTREME_LOW && belowMid) side = 'BUY';
      else if (sw.rsi >= STRAT.RSI_EXTREME_HIGH && aboveMid) side = 'SELL';
    }
  } else if (sw.regime === 'UP') {
    // Tendance HAUSSIÈRE : on ne prend QUE des LONG, sur repli (buy the dip).
    // Entrée quand le prix corrige vers/sous la médiane sans être en survente extrême,
    // et que le RSI se redresse (>= oversold). On suit la tendance, on ne la contrarie pas.
    if (belowMid && sw.rsi <= 50 && sw.rsi >= STRAT.RSI_OVERSOLD) side = 'BUY';
  } else if (sw.regime === 'DOWN') {
    // Tendance BAISSIÈRE : on ne prend QUE des SHORT, sur rebond (sell the rally).
    // C'est le régime qui fait gagner les shorts en marché baissier (cf. positions réelles).
    if (aboveMid && sw.rsi >= 50 && sw.rsi <= STRAT.RSI_OVERBOUGHT) side = 'SELL';
  }
  if (!side) { S.indicators.quality = null; return null; }

  // --- Score de qualité (0-100) ---
  const dist = side === 'BUY' ? (sw.bb.mid - px) / (sw.bb.sd || 1) : (px - sw.bb.mid) / (sw.bb.sd || 1);
  const distScore = Math.min(35, Math.max(0, Math.abs(dist) * 17));
  let rsiScore;
  if (mode === 'RANGE') {
    rsiScore = side === 'BUY'
      ? Math.min(30, (STRAT.RSI_OVERSOLD - sw.rsi + 5) * 2)
      : Math.min(30, (sw.rsi - STRAT.RSI_OVERBOUGHT + 5) * 2);
  } else {
    // En tendance, un RSI proche de 50 (momentum sain) vaut mieux qu'un extrême.
    rsiScore = 30 - Math.min(30, Math.abs(sw.rsi - 50));
  }
  const volScore = sw.volRatio != null ? Math.min(20, (sw.volRatio - 1) * 20) : 10;

  // Bonus d'alignement tendance : en UP/DOWN, trader dans le sens du marché est un +.
  const trendBonus = (mode === 'UP' && side === 'BUY') || (mode === 'DOWN' && side === 'SELL') ? 10 : 0;

  // --- Filtre FUNDING souple ---
  let fundingScore = 0;
  if (STRAT.FUNDING_SOFT && sw.funding != null) {
    const f = sw.funding;
    if (Math.abs(f) >= STRAT.FUNDING_EXTREME) {
      const favorsShort = f > 0;
      if ((side === 'SELL' && favorsShort) || (side === 'BUY' && !favorsShort)) fundingScore = STRAT.FUNDING_WEIGHT;
      else fundingScore = -STRAT.FUNDING_WEIGHT;
    }
  }

  const quality = Math.round(Math.max(0, distScore + Math.max(0, rsiScore) + Math.max(0, volScore) + trendBonus + fundingScore));
  S.indicators.quality = quality;
  S.indicators.bias = side === 'BUY' ? 'LONG' : 'SHORT';
  S.indicators.breakdown = { mode, dist: Math.round(distScore), rsi: Math.round(Math.max(0, rsiScore)), vol: Math.round(Math.max(0, volScore)), trend: trendBonus, funding: Math.round(fundingScore) };

  return { side, quality, midBand: sw.bb.mid, symbol, mode };
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
// Cherche un trade essoufflé à fermer pour libérer un slot vers un signal excellent.
function findRotationCandidate(exclude) {
  const now = Date.now();
  let worst = null, worstPnl = Infinity;
  for (const s of Object.keys(state.sym)) {
    if (s === exclude) continue;
    const S = state.sym[s];
    const pos = S.position;
    if (!pos || pos.closingManual || pos.adopted) continue;
    if (S.lastRotationAt && now - S.lastRotationAt < STRAT.ROTATION_COOLDOWN_MS) continue;
    const px = S.price;
    if (px <= 0) continue;
    const dir = pos.side === 'BUY' ? 1 : -1;
    const pnlPct = ((px - pos.entry) / pos.entry) * dir;
    const age = now - (pos.openedAt || now);
    const peak = pos.peakPnl != null ? pos.peakPnl : 0;
    const miniLoss = pnlPct <= 0 && pnlPct >= -STRAT.ROTATION_MAX_LOSS;
    const oldEnough = age >= STRAT.ROTATION_MIN_AGE_MS;
    const staled = peak < STRAT.ROTATION_STALE_PEAK;
    if (miniLoss && oldEnough && staled && pnlPct < worstPnl) { worstPnl = pnlPct; worst = s; }
  }
  return worst;
}

async function tryOpen(symbol, signal) {
  const S = state.sym[symbol];
  const now = Date.now();
  if (!S || S.position) return;
  if (state.activeSymbols && !state.activeSymbols.includes(symbol)) return;
  if (now - S.lastEntryAt < STRAT.MIN_GAP_MS) return;
  // Cooldown après un stop sur ce symbole
  if (S.lastStopAt && now - S.lastStopAt < STRAT.COOLDOWN_AFTER_STOP_MS) return;

  const { stake, lev } = sizing(signal);
  if (openPositionsCount() >= STRAT.MAX_POSITIONS_CAP) {
    if (STRAT.ROTATION_ENABLED && signal.quality >= STRAT.ROTATION_MIN_Q) {
      const victim = findRotationCandidate(symbol);
      if (victim) {
        logLine(`🔁 ROTATION : fermeture ${victim} (essoufflé) pour libérer un slot -> ${symbol} Q${signal.quality}.`);
        state.sym[victim].lastRotationAt = Date.now();
        await closePos(victim, 'ROTATION');
      } else { return; }
    } else { return; }
  }
  if (currentExposure() + stake > state.capital * STRAT.MAX_EXPOSURE_PCT) return;

  let qty = roundQty(symbol, (stake * lev) / S.price);
  if (qty <= 0) return;
  // Garde-fou -4005 : si la quantité dépasse le max autorisé par ordre, on plafonne.
  // Si même le max autorisé représente une part dérisoire de la mise voulue, on skip
  // (token trop bon marché pour notre taille : source de trades ingérables).
  const maxQ = maxQtyFor(symbol);
  if (isFinite(maxQ) && qty > maxQ) {
    const coverage = (maxQ * S.price) / (stake * lev); // fraction de la mise réellement plaçable
    if (coverage < 0.5) {
      logLine(`⏭️ ${symbol} @ ${S.price} : quantité voulue ${qty} > max ${maxQ} (couvre ${(coverage*100).toFixed(0)}%) — skip (token trop bon marché pour la mise).`);
      return;
    }
    qty = roundQty(symbol, maxQ); // sinon on plafonne au max plaçable
  }
  if (qty < minQtyFor(symbol)) return;

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
    openedAt: now, peakPnl: 0, scaleDone: [],
  };
  S.lastEntryAt = now;
  const f = S.swing.funding != null ? ` funding=${(S.swing.funding*100).toFixed(3)}%` : '';
  logLine(`🟢 ${symbol} ${signal.side} qty=${qty} @ ${entry.toFixed(4)} x${lev} Q=${signal.quality} SL-${(exits.slPct*100).toFixed(1)}%${f} [${entryFill}]`);
  broadcast({ type: 'positions', positions: livePositions() });
}

async function closePos(symbol, reason, qtyToClose = null) {
  const S = state.sym[symbol];
  const pos = S.position;
  if (!pos) return;
  const closeSide = pos.side === 'BUY' ? 'SELL' : 'BUY';
  // Quantité à fermer : tout par défaut, ou une part (scaling out).
  const qty = qtyToClose != null ? roundQty(symbol, qtyToClose) : pos.qty;
  if (qty <= 0) return;
  try {
    // Fermeture DÉCOUPÉE en tranches <= maxQty (corrige l'erreur -4005).
    const ok = await closeQtyInChunks(symbol, closeSide, qty);
    if (!ok) { logLine(`↩️ ${symbol} : fermeture incomplète, réessai au prochain tick.`); return; }
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

  // --- SCALING OUT : fermeture PARTIELLE (on garde le reste ouvert) ---
  if (qtyToClose != null && qty < pos.qty) {
    const exitPx = S.price;
    const dir = pos.side === 'BUY' ? 1 : -1;
    const pnlPct = ((exitPx - pos.entry) / pos.entry) * dir;
    const gross = pnlPct * (qty / pos.qty) * pos.stake * pos.lev;
    const fees = (qty / pos.qty) * pos.stake * pos.lev * (STRAT.FEE_MAKER + STRAT.FEE_TAKER);
    const net = gross - fees;
    state.capital += net;
    state.stats.gross += gross; state.stats.fees += fees; state.stats.net += net;
    pos.qty = roundQty(symbol, pos.qty - qty);      // réduit la position restante
    pos.stake = pos.stake * (pos.qty / (pos.qty + qty)); // ajuste la mise résiduelle
    logLine(`💰 ${symbol} PRISE PARTIELLE ${reason} : ${(pnlPct*100).toFixed(1)}% sur ${qty} — net ${net.toFixed(2)}$ — reste ${pos.qty}.`);
    broadcast({ type: 'positions', positions: livePositions() });
    return;
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

  // 2) SCALING OUT : prise de profit partielle aux paliers, on garde le reste.
  if (STRAT.SCALE_OUT && !pos.scaleDone) pos.scaleDone = [];
  if (STRAT.SCALE_OUT) {
    for (let i = 0; i < STRAT.SCALE_OUT.length; i++) {
      const step = STRAT.SCALE_OUT[i];
      if (!pos.scaleDone.includes(i) && pnlPct >= step.at && pos.qty > 0) {
        pos.scaleDone.push(i);
        const chunk = roundQty(symbol, pos.qty * step.frac);
        if (chunk > 0 && chunk < pos.qty) { closePos(symbol, `SCALE+${(step.at*100).toFixed(0)}%`, chunk); return; }
      }
    }
  }

  // 3) Trailing LARGE : armé à +1%, sort si on recule de 1.5% sous le pic (laisse courir)
  if (pos.peakPnl >= STRAT.TRAIL_ARM) {
    if (pos.peakPnl - pnlPct >= STRAT.TRAIL_PCT) { closePos(symbol, 'TRAILING'); return; }
  }
  // 4) Borne haute de sécurité (rarement atteinte)
  if (pnlPct >= STRAT.TP_SOFT_CAP) { closePos(symbol, 'TAKE-PROFIT'); return; }
  // 5) Time-stop 24h
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
  try {
    // UN SEUL appel : toutes les positions réelles du compte (source de vérité).
    const data = await signedRequest('GET', '/fapi/v2/positionRisk', {});
    if (!Array.isArray(data)) return;

    // Index des positions réelles non nulles.
    const real = {};
    for (const p of data) {
      const amt = parseFloat(p.positionAmt);
      const step = SYMBOL_INFO[p.symbol] ? SYMBOL_INFO[p.symbol].stepSize : 0.000001;
      if (Math.abs(amt) >= step) {
        real[p.symbol] = {
          side: amt > 0 ? 'BUY' : 'SELL',
          qty: Math.abs(amt),
          entry: parseFloat(p.entryPrice),
          lev: parseFloat(p.leverage) || STRAT.LEV_MAX,
        };
      }
    }

    // 1) Positions FANTÔMES : l'état interne dit "ouvert", Binance dit "fermé" -> on nettoie.
    for (const symbol of Object.keys(state.sym)) {
      const S = state.sym[symbol];
      if (S.position && S.position.closingManual) continue; // fermeture manuelle en cours
      if (S.position && !real[symbol]) {
        S.position = null;
        logLine(`🔄 ${symbol} : position fermée côté Binance — état nettoyé.`);
      }
    }

    // 2) Positions ORPHELINES : Binance a une position que l'état interne ignore -> on l'ADOPTE.
    //    (C'est LE correctif : le bot reprend la main sur des positions qu'il ne suivait plus,
    //    pour les gérer — stop, trailing, scaling out — au lieu de les laisser flotter.)
    for (const symbol of Object.keys(real)) {
      ensureSymbolState(symbol);
      const S = state.sym[symbol];
      const r = real[symbol];
      if (S.position && S.position.closingManual) continue; // fermeture manuelle en cours
      if (!S.position) {
        const exits = computeExits(symbol);
        // On estime la mise à partir de la marge réelle (qty*entry/levier).
        const stake = (r.qty * r.entry) / (r.lev || 1);
        S.position = {
          side: r.side, entry: r.entry, qty: r.qty, stake, lev: r.lev,
          quality: 0, entryFill: 'taker',
          slPct: exits.slPct, tpPct: exits.tpPct,
          sl: r.side === 'BUY' ? r.entry * (1 - exits.slPct) : r.entry * (1 + exits.slPct),
          tp: r.side === 'BUY' ? r.entry * (1 + exits.tpPct) : r.entry * (1 - exits.tpPct),
          openedAt: Date.now(), peakPnl: 0, scaleDone: [], adopted: true,
        };
        // S'assurer que le symbole est surveillé (flux prix) même hors univers courant.
        if (!ALL_SYMBOLS.includes(symbol)) { ALL_SYMBOLS.push(symbol); reconnectPriceStreamsSoon(); }
        logLine(`🩹 ${symbol} : position réelle ADOPTÉE (${r.side} ${r.qty} @ ${r.entry}, x${r.lev}) — désormais gérée.`);
      } else {
        // Position connue : on resynchronise qty/entry sur la réalité Binance.
        S.position.qty = r.qty;
        S.position.entry = r.entry;
      }
    }
    broadcast({ type: 'positions', positions: livePositions() });
  } catch (e) {
    logLine(`⚠️ reconcile: ${e.message}`);
  }
}

// Reconnexion du flux prix (débounce léger) quand l'univers change via adoption.
let _reconnectTimer = null;
function reconnectPriceStreamsSoon() {
  if (_reconnectTimer) return;
  _reconnectTimer = setTimeout(() => {
    _reconnectTimer = null;
    if (priceWs) { const old = priceWs; priceWs = null; try { old.close(); } catch (e) {} connectPriceStreams(); }
  }, 2000);
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
      ageMs: Date.now() - (pos.openedAt || Date.now()), timeStopMs: STRAT.TIME_STOP_MS,
      adopted: !!pos.adopted,
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
    strat: { sl: STRAT.SL_PCT * 100, trailArm: STRAT.TRAIL_ARM * 100, trailPct: STRAT.TRAIL_PCT * 100, lev: '2-5', universe: state.universe.length, relaxOn: STRAT.RELAX_RANGE_ENTRY },
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
      for (const s of Object.keys(state.sym)) if (state.sym[s].position) await closePos(s, 'MANUEL');
      logLine('🧹 Fermeture manuelle de TOUTES les positions');
    } else if (cmd.action === 'closeManual') {
      const sym = cmd.symbol;
      const S = sym && state.sym[sym];
      if (!S || !S.position) { logLine(`⚠️ closeManual: pas de position sur ${sym}`); return; }
      const pct = cmd.pct != null ? Math.max(1, Math.min(100, parseFloat(cmd.pct))) : 100;
      const qtyToClose = pct >= 100 ? null : roundQty(sym, S.position.qty * (pct / 100));
      S.position.closingManual = true;
      logLine(`👤 CLÔTURE MANUELLE ${sym} — ${pct}%`);
      await closePos(sym, pct >= 100 ? 'MANUEL' : `MANUEL-${pct}%`, qtyToClose);
      if (state.sym[sym] && state.sym[sym].position) state.sym[sym].position.closingManual = false;
    } else if (cmd.action === 'toggleRelax') {
      STRAT.RELAX_RANGE_ENTRY = !STRAT.RELAX_RANGE_ENTRY;
      logLine(`🔧 Assouplissement RANGE : ${STRAT.RELAX_RANGE_ENTRY ? 'ACTIVÉ' : 'DÉSACTIVÉ'}`);
      broadcast({ type: 'snapshot', data: snapshot() });
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
    <span class="badge" style="background:rgba(0,245,200,.12);color:#00F5C8;border:1px solid rgba(0,245,200,.3)">3.8 - 1J - MAJ · 28 sym <span style="opacity:.6;font-weight:600">· WR ~?%</span></span>
    <span id="mode" class="badge net">TESTNET</span>
    <span id="run" class="badge off">PAUSE</span>
  </div>
  <div class="sub" id="stratline">3.8-1J-MAJ · 28 sym (5 fixes+23 vol) · 25 trades · rotation · clôture manuelle · multi-régime</div>

  <div class="controls">
    <button id="start" class="btn-go">▶ Démarrer</button>
    <button id="stop" class="btn-stop">⏸ Pause</button>
    <button id="closeAll" class="btn-kill">⏹ Tout fermer</button>
    <button id="toggleRelax" class="btn-stop">🔧 Assoupli: —</button>
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
    <thead><tr><th>Symbole</th><th>Sens</th><th>Lev·Q</th><th>Entrée</th><th>Actuel</th><th>Investi</th><th>SL</th><th>TP</th><th>⏱ Durée</th><th>P&L live</th><th>Action</th></tr></thead>
    <tbody id="positions"><tr><td colspan="11" class="mut" style="text-align:center;padding:14px">Aucune position</td></tr></tbody>
  </table></div>

  <div class="sec"><span class="dot"></span>Surveillance des symboles</div>
  <div class="table-wrap"><table>
    <thead><tr><th>Symbole</th><th>Prix</th><th>Courbe</th><th>Biais</th><th>Q</th><th>RSI</th><th>Funding</th><th>Régime</th><th>Statut</th></tr></thead>
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
  let ws = null;
  let wsReady = false;
  function wsSend(obj){
    // Envoi sûr : si la connexion n'est pas prête, on ne perd pas le clic silencieusement.
    if(ws && ws.readyState === WebSocket.OPEN){ ws.send(JSON.stringify(obj)); }
    else { console.warn('WS pas prêt, action ignorée:', obj); }
  }
  function connect(){
    ws = new WebSocket(proto + '://' + location.host);
    ws.onopen = ()=>{ wsReady=true; console.log('WS connecté'); };
    ws.onmessage = onWsMessage;
    ws.onclose = ()=>{
      wsReady=false;
      $('run').textContent='RECONNEXION…'; $('run').className='badge off';
      setTimeout(connect, 1500); // reconnexion automatique (Railway peut couper les WS inactifs)
    };
    ws.onerror = ()=>{ try{ ws.close(); }catch(e){} };
  }
  const $ = id => document.getElementById(id);
  const num = (n,d=2) => Number(n).toLocaleString('fr-FR',{minimumFractionDigits:d,maximumFractionDigits:d});
  const sign = n => n>=0?'+':'';
  const cls = n => n>=0?'green':'red';
  function px(v){ const n=Number(v); return n>=100?num(n,2):n>=1?num(n,3):num(n,5); }
  function dur(ms){ if(ms==null)return '—'; let s=Math.floor(ms/1000); const h=Math.floor(s/3600); s%=3600; const m=Math.floor(s/60); const r=s%60; return (h>0?h+'h':'')+(m<10&&h>0?'0':'')+m+':'+(r<10?'0':'')+r; }

  function renderStats(s){
    $('mode').textContent=(s.mode||'testnet').toUpperCase();
    $('run').textContent=s.killed?'KILL -45%':(s.running?'EN MARCHE':'PAUSE');
    $('run').className='badge '+(s.running?'on':'off');
    if($('toggleRelax'))$('toggleRelax').textContent='🔧 Assoupli: '+(s.strat&&s.strat.relaxOn?'ON':'OFF');
    $('stratline').textContent='3.8-1J-MAJ · 28 sym · 25 trades · rotation Q68 · multi-régime (RANGE→MR / UP→long / DOWN→short) · SL -'+(s.strat?s.strat.sl:2.5)+'% · trailing +'+(s.strat?s.strat.trailArm:1)+'%/-'+(s.strat?s.strat.trailPct:1.5)+'% · scaling out · x2-5';
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
    if(!list||!list.length){tb.innerHTML='<tr><td colspan="11" class="mut" style="text-align:center;padding:14px">Aucune position</td></tr>';return;}
    tb.innerHTML=list.map(p=>{
      const sc=p.side==='BUY'?'long':'short',st=p.side==='BUY'?'LONG':'SHORT';
      return '<tr><td>'+p.symbol+'</td><td><span class="pill '+sc+'">'+st+'</span></td>'+
        '<td>'+p.lev+'x·Q'+p.quality+'</td><td>'+px(p.entry)+'</td><td>'+px(p.current)+'</td>'+
        '<td>$'+num(p.investi)+'</td><td class="red">'+px(p.sl)+'</td><td class="green">'+px(p.tp)+'</td>'+
        '<td class="'+((p.timeStopMs&&p.ageMs>p.timeStopMs*0.8)?'red':'mut')+'">'+dur(p.ageMs)+(p.adopted?' <span style="color:var(--amber);font-size:9px">adopté</span>':'')+'</td>'+
        '<td class="'+cls(p.netLive)+'">'+sign(p.netLive)+'$'+num(p.netLive)+' ('+sign(p.pnlPct)+p.pnlPct.toFixed(2)+'%)</td>'+
        '<td style="white-space:nowrap"><input id="pct_'+p.symbol+'" type="number" min="1" max="100" value="100" style="width:42px;background:#0e151d;border:1px solid var(--line);color:var(--txt);border-radius:5px;padding:3px;font-size:11px"/>%'+
        ' <button onclick="closePos(\''+p.symbol+'\')" style="background:rgba(255,84,112,.15);color:var(--red);border:1px solid rgba(255,84,112,.3);border-radius:6px;padding:4px 8px;font-size:11px;font-weight:700;cursor:pointer">Fermer</button></td></tr>';
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
      const regTxt=s.regime==='UP'?'<span style="color:var(--green)">▲ UP</span>':s.regime==='DOWN'?'<span style="color:var(--red)">▼ DOWN</span>':s.regime==='RANGE'?'<span style="color:var(--cyan)">↔ RANGE</span>':'—';
      return '<tr><td>'+s.symbol+'</td><td>'+(s.price?px(s.price):'—')+'</td>'+
        '<td>'+sparkline(s.spark)+'</td>'+
        '<td><span class="pill '+bc+'">'+b+'</span></td>'+
        '<td class="qbadge '+qcol+'">'+q+'</td>'+
        '<td>'+(s.rsi!=null?s.rsi.toFixed(0):'—')+'</td>'+
        '<td class="'+fndCol+'">'+fnd+'</td>'+
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
  function onWsMessage(e){
    const m=JSON.parse(e.data);
    if(m.type==='snapshot'){snap=m.data;renderStats(snap);renderPositions(snap.positions);renderSymbols(snap.symbols);renderTrades(snap.trades);$('log').innerHTML=(snap.log||[]).join('<br>');}
    else if(snap){
      if(m.type==='status'){snap.running=m.running;renderStats(snap);}
      if(m.type==='symbols'){snap.symbols=m.symbols;renderSymbols(m.symbols);}
      if(m.type==='positions'){snap.positions=m.positions;renderPositions(m.positions);}
      if(m.type==='trade'){snap.stats=m.stats;snap.capital=m.capital;snap.positions=m.positions;renderStats(snap);renderPositions(m.positions);}
      if(m.type==='log'){snap.log.unshift(m.line);if(snap.log.length>50)snap.log.pop();$('log').innerHTML=snap.log.join('<br>');}
    }
  }
  window.closePos=function(sym){
    const inp=document.getElementById('pct_'+sym);
    const pct=inp?Math.max(1,Math.min(100,Number(inp.value)||100)):100;
    if(confirm(pct>=100?('Fermer TOUTE la position '+sym+' ?'):('Fermer '+pct+'% de '+sym+' ?'))) wsSend({action:'closeManual',symbol:sym,pct:pct});
  };
  $('toggleRelax').onclick=()=>wsSend({action:'toggleRelax'});
  $('start').onclick=()=>wsSend({action:'start'});
  $('stop').onclick=()=>wsSend({action:'stop'});
  $('closeAll').onclick=()=>wsSend({action:'closeAll'});
  connect(); // établit la connexion + reconnexion auto
</script>
</body></html>`;

// ==================================================================
// DÉMARRAGE
// ==================================================================
async function start() {
  logLine(`\u{1F680} Itachi — SERVEUR 3.8-1J-MAJ (28 sym: 5 fixes+23 vol, 25 trades, rotation) — ${MODE.toUpperCase()} — capital $${CAPITAL_START}`);
  logLine(`\u{1F4D0} Swing mean-reversion 1h/2h — Bollinger ${STRAT.BB_PERIOD}/${STRAT.BB_STDDEV}\u03C3 + RSI${STRAT.RSI_PERIOD} (${STRAT.RSI_OVERSOLD}/${STRAT.RSI_OVERBOUGHT}) — SL -${STRAT.SL_PCT*100}% / trailing +${STRAT.TRAIL_ARM*100}%/-${STRAT.TRAIL_PCT*100}% — x2-5 — mise ${STRAT.STAKE_MIN_USD}-${STRAT.STAKE_MAX_USD}$ — ${STRAT.MAX_POSITIONS_CAP} pos — kill -${STRAT.KILL_PCT*100}%`);
  if (!API_KEY || !API_SECRET) logLine('\u26A0\uFE0F Cles API manquantes — lecture seule (pas d ordres).');

  // 0) LE SERVEUR HTTP DÉMARRE EN PREMIER : le dashboard s'affiche tout de suite,
  //    avant tout appel réseau. Il se remplit de données ensuite (arrière-plan).
  server.listen(PORT, '0.0.0.0', () => {
    const source = process.env.PORT ? 'Railway' : "fallback 8080 - Railway n'injecte pas PORT";
    logLine(`\u{1F310} Dashboard sur le port ${PORT} [${source}] — pret.`);
  });

  // Initialisation NON BLOQUANTE en arrière-plan.
  (async () => {
    try {
      await loadSymbolInfo();
      const uni = await scanUniverse();
      await applyUniverse(uni);
      await refreshAllKlines();
      connectPriceStreams();
      logLine('\u2705 Initialisation complete — bot pret a demarrer.');
    } catch (e) {
      logLine(`\u26A0\uFE0F Init arriere-plan: ${e.message} — dashboard actif, reessai auto.`);
    }
    setInterval(refreshAllKlines, STRAT.SIGNAL_REFRESH_MS);
    setInterval(refreshUniverse, STRAT.UNIVERSE_REFRESH_MS);
    setInterval(reconcile, 9000);
    setInterval(() => broadcast({ type: 'symbols', symbols: symbolsOverview() }), 5000);
  })();
}

start();
