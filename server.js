/* ============================================================
 *  SERVEUR 3.2 — Itachi Multi (config haute expo + sorties "laisser courir")
 *  ------------------------------------------------------------
 *  RÉGLAGES UTILISATEUR :
 *   - 10 à 20 positions simultanées (plafond dur 20, expo 90%)
 *   - Kill switch : -45%
 *   - Q d'entrée : 40-50
 *   - Levier x3 -> x12 indexé sur Q (x12 réservé Q>=80 + mise 9%)
 *   - MTF allégé de moitié : 0.125
 *   - Délai entre 2 entrées même symbole : 14 min
 *
 *  SORTIES (révisées) :
 *   - Stop-loss FIXE : -1%
 *   - PAS de take-profit plafond : on laisse courir (TP infini)
 *   - Trailing armé à +0.8%, suit à -0.5% sous le pic
 *   - Fenêtre intermédiaire DÉSACTIVÉE (pas de TEMPS-PROFIT/PERTE/REBOND)
 *   - Filet ultime : sortie forcée à 14 min
 *  => Sortie uniquement par : SL -1%, trailing -0.5%, ou filet 14 min.
 *
 *  ⚠️ Config la plus exposée. À VALIDER au /backtest avant le réel.
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
  // --- Ratio risque/récompense (SL/TP de base, désormais calibrés par l'ATR) ---
  // --- Sorties : SL fixe -1%, PAS de TP plafond (on laisse courir), trailing -0.5% ---
  SL_PCT: 0.010, // -1.0% stop-loss fixe
  TP_PCT: 999, // TP "infini" : aucun plafond, le trade court tant que le trailing tient
  NO_FIXED_TP: true, // marqueur : pas de prise de profit à seuil fixe

  USE_ATR_EXITS: false, // OFF : on garde un SL fixe -1% (pas d'ATR sur les sorties ici)
  ATR_SL_MULT: 1.0, ATR_TP_MULT: 2.0, ATR_TP_CAP: 0.018, ATR_TP_FLOOR: 0.006,

  // --- Trailing : armé à +0.8% (mouvement réel), suit à -0.5% sous le pic ---
  TRAIL_START: 0.008, // le trailing s'arme dès +0.8% de profit
  TRAIL_PCT: 0.005, // une fois armé, stop suiveur à -0.5% du pic (plus serré que le SL)

  // --- Sélectivité (Q adaptatif 50-55 selon volatilité du marché) ---
  Q_MIN_CALM: 50, // marché calme : seuil haut de la zone d'entrée 40-50
  Q_MIN_VOLATILE: 40, // marché agité : seuil bas 40 (plus d'opportunités)
  MTF_REQUIRED: true, // alignement multi-timeframe requis...
  MTF_MIN_ALIGN: 0.125, // ...ALLÉGÉ DE MOITIÉ (0.25 -> 0.125) : laisse passer + de signaux

  // --- Indicateurs ---
  EMA_FAST: 8,
  EMA_SLOW: 21,
  RSI_PERIOD: 14,
  MOM_WINDOW: 20,
  MOM_MULT: 25000,
  EMA_MULT: 12000,
  KLINE_LIMIT: 50, // lecture des 50 dernières bougies (5m) par symbole
  SHORT_TF: '1m', // frame la plus courte
  SHORT_TF_LIMIT: 100, // 100 bougies 1m pour momentum court + ATR (hors boucle de décision)
  ATR_PERIOD: 14, // ATR sur la frame courte (mesure de volatilité)

  // --- Sélection dynamique : on ne trade que les N meilleurs symboles du moment ---
  ACTIVE_TOP_N: 15, // sur les 20 surveillés, 15 actifs (par volatilité x force de tendance)
  RANK_REFRESH_MS: 300000, // re-classement toutes les 5 min

  // --- Capital & risque ---
  KILL_PCT: 0.45, // expulsion à -45% du capital (stop bot)
  MAX_EXPOSURE_PCT: 0.90, // exposition montée à 90% pour permettre 10-20 positions simultanées
  MIN_POSITIONS_TARGET: 10, // objectif : au moins 10 positions possibles
  MAX_POSITIONS_CAP: 20, // plafond dur : jamais plus de 20 positions simultanées
  FEE_PER_SIDE: 0.0004, // 0.04% taker par leg

  // --- Mise en % du capital (remplace BASE_STAKE fixe) ---
  STAKE_PCT: 0.045, // mise standard = 4,5% du capital
  STAKE_PCT_EXCEPTIONAL: 0.09, // mise exceptionnelle = 9% du capital
  MAX_EXCEPTIONAL_CONCURRENT: 4, // jusqu'à 4 mises 9% simultanées (univers de 10-20 positions)

  // --- Levier progressif x3 -> x12, indexé sur le Quality score ---
  // Q d'entrée bas (40-50) => gros leviers réservés aux Q élevés, pas aux signaux faibles.
  LEV_MAX: 12, // levier max (mise exceptionnelle ou Q>=80)
  LEV_BY_Q: [ // premier palier dont le seuil Q est atteint
    { q: 80, lev: 12 },
    { q: 72, lev: 10 },
    { q: 64, lev: 8 },
    { q: 56, lev: 6 },
    { q: 48, lev: 4 },
    { q: 0,  lev: 3 }, // Q 40-47 (entrée la plus faible -> levier plancher x3)
  ],

  // --- Bonus mise exceptionnelle 9% : 2 portes ---
  // Porte 1 : 4 conditions sur 4 réunies -> 9% à tout moment.
  // Porte 2 : si aucun setup 4/4 depuis RELAX_AFTER_MS, on accepte 3/4 -> 9%.
  // Dans les deux cas les OBLIGATOIRES (Q>=49, MTF, expo<35%) restent requis.
  EXC_CONDITIONS_FULL: 4, // setup parfait
  EXC_CONDITIONS_RELAXED: 3, // setup très bon, accepté après attente
  RELAX_AFTER_MS: 450000, // 7min30 (milieu de la fenêtre 6-9 min) sans 4/4 -> on assouplit

  // --- Sortie "explosive" : +2,5% atteint en moins de 45s -> on laisse courir ---
  EXPLOSIVE_PCT: 0.025, // (héritage) le déclencheur explosif utilise désormais le TP réel de la position
  EXPLOSIVE_WINDOW_MS: 45000, // 45s : en deçà, le trade est marqué explosif
  // Un trade explosif échappe au TIME_EXIT_HARD et court sur son trailing.

  // --- Cadence & sortie temporelle intelligente ---
  MIN_GAP_MS: 840000, // 14 min minimum entre 2 entrées (même symbole)
  TIME_EXIT_INTERMEDIATE: false, // fenêtre intermédiaire DÉSACTIVÉE (pas de TEMPS-PROFIT/PERTE/REBOND)
  TIME_EXIT_FROM: 60000, // (inutilisé si TIME_EXIT_INTERMEDIATE=false)
  TIME_EXIT_HARD: 840000, // filet ultime : sortie forcée à 14 min
  TIME_EXIT_GIVEBACK: 0.004, // (inutilisé si fenêtre off)
  TIME_EXIT_WORSENING: -0.006, // (inutilisé si fenêtre off)
  TF_PRINCIPAL: '5m', // timeframe d'analyse principal
};

// Timeframes pour l'alignement multi-timeframe
const TIMEFRAMES = ['1m', '5m', '15m', '1h'];
const TF_WEIGHTS = { '1m': 1, '5m': 1.5, '15m': 2, '1h': 2.5 };

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
function maxConcurrentPositions(capital) {
  if (capital >= 5000) return 12;
  if (capital >= 3000) return 9;
  if (capital >= 2000) return 6;
  if (capital >= 1500) return 4;
  return 3; // palier de départ (1000$)
}

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
  lastFullSetupAt: 0, // horodatage du dernier setup parfait 4/4 (porte 2 du bonus 9%)
  lastRelaxedExcAt: 0, // horodatage de la dernière mise 9% accordée sur un 3/4 (limite : 1 par fenêtre)
  activeSymbols: null, // sous-ensemble des N meilleurs symboles à trader (null = tous au démarrage)
};

for (const s of ALL_SYMBOLS) {
  state.sym[s] = {
    symbol: s, price: 0, prices: [], klines: [],
    indicators: { emaFast: null, emaSlow: null, rsi: null, momentum: null, bias: 'NEUTRE', quality: null, breakdown: null, excConditions: 0 },
    mtf: { alignNorm: null, trends: {}, support: null, resistance: null },
    short: { atrPct: null, mom: null, masterTrend: 0 }, // frame courte 1m (volatilité + momentum)
    position: null, lastEntryAt: 0, busy: false,
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

// ==================================================================
// LECTURE DES 50 DERNIÈRES BOUGIES + ANALYSE MTF PAR SYMBOLE
// ==================================================================
async function refreshKlines(symbol) {
  const S = state.sym[symbol];
  try {
    const main = await publicGet(KLINE_BASE, '/fapi/v1/klines', {
      symbol, interval: STRAT.TF_PRINCIPAL, limit: STRAT.KLINE_LIMIT,
    });
    S.klines = main.map((c) => ({ time: c[0], open: +c[1], high: +c[2], low: +c[3], close: +c[4], vol: +c[5] }));
    const closes = S.klines.map((c) => c.close);

    const recent = S.klines.slice(-20);
    S.mtf.support = Math.min(...recent.map((c) => c.low));
    S.mtf.resistance = Math.max(...recent.map((c) => c.high));
    S.indicators.rsi = rsi(closes, STRAT.RSI_PERIOD);

    let weighted = 0, totalW = 0;
    const trends = {};
    for (const tf of TIMEFRAMES) {
      try {
        const k = await publicGet(KLINE_BASE, '/fapi/v1/klines', { symbol, interval: tf, limit: 30 });
        const t = trendFromCloses(k.map((c) => +c[4]));
        trends[tf] = t;
        weighted += t * TF_WEIGHTS[tf];
        totalW += TF_WEIGHTS[tf];
      } catch (e) { /* tf indisponible */ }
    }
    S.mtf.trends = trends;
    S.mtf.alignNorm = totalW ? weighted / totalW : 0;

    // --- Frame courte : 100 bougies 1m -> momentum court + ATR (volatilité) ---
    // Tourne ici, dans le cycle 30s asynchrone, JAMAIS dans la boucle de décision.
    try {
      const short = await publicGet(KLINE_BASE, '/fapi/v1/klines', {
        symbol, interval: STRAT.SHORT_TF, limit: STRAT.SHORT_TF_LIMIT,
      });
      const sk = short.map((c) => ({ high: +c[2], low: +c[3], close: +c[4] }));
      S.short.atrPct = atrPct(sk, STRAT.ATR_PERIOD);
      const sc = sk.map((c) => c.close);
      if (sc.length >= 20) {
        S.short.mom = (sc[sc.length - 1] - sc[sc.length - 20]) / sc[sc.length - 20];
      }
      // Tendance maître = la 1h (le TF le plus lourd), pour le filtre allégé
      S.short.masterTrend = trends['1h'] != null ? trends['1h'] : 0;
    } catch (e) { /* on garde l'ancien */ }
  } catch (e) { /* on garde l'ancien */ }
}

// ==================================================================
// SCORING (stratégie 2,5:1 — sélective, MTF obligatoire)
// ==================================================================
function computeSignal(symbol) {
  const S = state.sym[symbol];
  const p = S.prices;
  if (p.length < STRAT.EMA_SLOW + 2) return null;

  const emaFast = ema(p, STRAT.EMA_FAST);
  const emaSlow = ema(p, STRAT.EMA_SLOW);
  if (emaFast == null || emaSlow == null) return null;

  const last = p[p.length - 1];
  const prev = p[p.length - STRAT.MOM_WINDOW] || p[0];
  const momentum = (last - prev) / prev;

  const bull = emaFast > emaSlow && momentum > 0;
  const bear = emaFast < emaSlow && momentum < 0;

  S.indicators.emaFast = emaFast;
  S.indicators.emaSlow = emaSlow;
  S.indicators.momentum = momentum;
  S.indicators.bias = bull ? 'LONG' : bear ? 'SHORT' : 'NEUTRE';

  if (!bull && !bear) { S.indicators.quality = null; return null; }
  const isLong = bull;

  // Alignement MTF ALLÉGÉ (0.10 au lieu de 0.30) — débloque les trades
  const align = S.mtf.alignNorm;
  const aligned = isLong ? align : -align;
  if (STRAT.MTF_REQUIRED && (align == null || aligned <= STRAT.MTF_MIN_ALIGN)) {
    S.indicators.quality = null;
    return null;
  }

  // Filtre tendance maître (1h) — léger : on refuse seulement le contre-sens FRANC.
  const master = S.short.masterTrend || 0;
  if (isLong && master < 0 && aligned < 0.25) { S.indicators.quality = null; return null; }
  if (!isLong && master > 0 && aligned < 0.25) { S.indicators.quality = null; return null; }

  const spread = Math.abs(emaFast - emaSlow) / emaSlow;
  const emaScore = Math.min(20, spread * STRAT.EMA_MULT);
  const momScore = Math.min(20, Math.abs(momentum) * STRAT.MOM_MULT);
  const mtfScore = Math.max(0, Math.min(35, aligned * 35));

  let rsiScore = 0;
  if (S.indicators.rsi != null) {
    if (isLong) rsiScore = S.indicators.rsi < 35 ? 15 : S.indicators.rsi < 50 ? 10 : S.indicators.rsi < 65 ? 5 : 0;
    else rsiScore = S.indicators.rsi > 65 ? 15 : S.indicators.rsi > 50 ? 10 : S.indicators.rsi > 35 ? 5 : 0;
  }

  let srScore = 0;
  if (S.mtf.support && S.mtf.resistance && S.price > 0) {
    const range = S.mtf.resistance - S.mtf.support;
    if (range > 0) {
      const posInRange = (S.price - S.mtf.support) / range;
      if (isLong) srScore = posInRange < 0.4 ? 10 : posInRange < 0.6 ? 5 : 0;
      else srScore = posInRange > 0.6 ? 10 : posInRange > 0.4 ? 5 : 0;
    }
  }

  const quality = Math.round(emaScore + momScore + mtfScore + rsiScore + srScore);
  S.indicators.quality = quality;
  S.indicators.breakdown = {
    ema: Math.round(emaScore), mom: Math.round(momScore),
    mtf: Math.round(mtfScore), rsi: Math.round(rsiScore), sr: Math.round(srScore),
  };

  // --- Les 4 CONDITIONS du bonus mise 9% (au-dessus des obligatoires) ---
  // Chacune est un "vrai bon point" du signal. 4/4 = setup parfait.
  // (distinct du droit d'entrer, qui dépend lui de Q_MIN + MTF + expo)
  let excConditions = 0;
  if (emaScore >= 15) excConditions++; // 1. écart EMA large (tendance nette)
  if (momScore >= 15) excConditions++; // 2. momentum fort
  if (rsiScore >= 10) excConditions++; // 3. RSI en zone favorable
  if (srScore >= 10) excConditions++;  // 4. position S/R favorable
  S.indicators.excConditions = excConditions;

  return { side: isLong ? 'BUY' : 'SELL', quality, excConditions };
}

// Q minimum ADAPTATIF (50-55) : marché agité (ATR élevé) -> seuil bas (plus d'opportunités) ;
// marché calme (ATR faible) -> seuil haut (on est plus exigeant).
function qMinFor(symbol) {
  const a = state.sym[symbol].short.atrPct;
  if (a == null) return STRAT.Q_MIN_CALM; // par défaut, prudent
  return a >= 0.004 ? STRAT.Q_MIN_VOLATILE : STRAT.Q_MIN_CALM;
}

// Calcule les niveaux SL/TP en % selon l'ATR du symbole (volatilité réelle).
// Si l'ATR est indisponible ou USE_ATR_EXITS=false, on retombe sur SL_PCT/TP_PCT fixes.
// Le TP ATR est borné [ATR_TP_FLOOR, ATR_TP_CAP] pour rester atteignable ET sûr.
function computeExits(symbol) {
  const atr = state.sym[symbol].short.atrPct;
  if (!STRAT.USE_ATR_EXITS || atr == null || atr <= 0) {
    return { slPct: STRAT.SL_PCT, tpPct: STRAT.TP_PCT, source: 'fixe' };
  }
  let slPct = atr * STRAT.ATR_SL_MULT;
  let tpPct = atr * STRAT.ATR_TP_MULT;
  tpPct = Math.max(STRAT.ATR_TP_FLOOR, Math.min(STRAT.ATR_TP_CAP, tpPct));
  slPct = Math.max(STRAT.SL_PCT, slPct); // jamais un SL plus serré que le plancher
  // Garde-fou ratio : le SL ne doit jamais dépasser le TP (sinon risque > récompense).
  // Sur actif très volatil où le TP est plafonné, on resserre le SL à 0.7x le TP.
  if (slPct >= tpPct) slPct = tpPct * 0.7;
  return { slPct, tpPct, source: 'atr' };
}

// Levier progressif 2x -> 7x selon Q (premier palier atteint).
// Mise exceptionnelle -> toujours le levier max (la conviction du timing la valide).
function levForQuality(quality, isExceptional) {
  if (isExceptional) return STRAT.LEV_MAX;
  for (const tier of STRAT.LEV_BY_Q) {
    if (quality >= tier.q) return tier.lev;
  }
  return STRAT.LEV_BY_Q[STRAT.LEV_BY_Q.length - 1].lev;
}

// Compte les mises exceptionnelles 9% actuellement ouvertes.
function exceptionalOpenCount() {
  let n = 0;
  for (const s of ALL_SYMBOLS) {
    const pos = state.sym[s].position;
    if (pos && pos.isExceptional) n++;
  }
  return n;
}

// Décide mise + levier. Renvoie aussi le flag "exceptional" pour l'affichage.
// Les obligatoires (Q, MTF, expo) sont déjà vérifiés en amont (tryOpen).
function sizing(signal) {
  const now = Date.now();
  const cond = signal.excConditions || 0;

  let isExceptional = false;
  let viaRelaxed = false;

  if (cond >= STRAT.EXC_CONDITIONS_FULL) {
    // Porte 1 : 4 conditions sur 4 -> mise 9% À TOUT MOMENT, sans limite de fréquence.
    isExceptional = true;
  } else if (cond >= STRAT.EXC_CONDITIONS_RELAXED) {
    // Porte 2 : 3 conditions. Autorisée seulement si :
    //  (a) aucun setup 4/4 depuis RELAX_AFTER_MS (7-9 min), ET
    //  (b) on n'a pas déjà accordé une 9%-relâchée dans cette même fenêtre.
    const noFullSince = (now - state.lastFullSetupAt) >= STRAT.RELAX_AFTER_MS;
    const noRelaxedSince = (now - state.lastRelaxedExcAt) >= STRAT.RELAX_AFTER_MS;
    if (noFullSince && noRelaxedSince) {
      isExceptional = true;
      viaRelaxed = true;
    }
  }

  // Plafond de mises exceptionnelles simultanées : au-delà, on retombe en mise standard
  // (le bot CONTINUE d'ouvrir des positions normales 4,5% au lieu de se bloquer).
  if (isExceptional && exceptionalOpenCount() >= STRAT.MAX_EXCEPTIONAL_CONCURRENT) {
    isExceptional = false;
    viaRelaxed = false;
  }

  const pct = isExceptional ? STRAT.STAKE_PCT_EXCEPTIONAL : STRAT.STAKE_PCT;
  const stake = state.capital * pct;
  const lev = levForQuality(signal.quality, isExceptional);

  return { stake, lev, isExceptional, viaRelaxed };
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
  if (signal.quality < qMinFor(symbol)) return;

  // Le timer de la porte 2 se réarme dès qu'un setup parfait 4/4 se présente
  // (qu'on le trade ou non) : la porte 2 ne doit s'ouvrir QUE faute de 4/4.
  if ((signal.excConditions || 0) >= STRAT.EXC_CONDITIONS_FULL) {
    state.lastFullSetupAt = now;
  }

  // Plus de limite de nombre : seul garde-fou = exposition plafonnée à 35% du capital
  const { stake, lev, isExceptional, viaRelaxed } = sizing(signal);
  // Plafond dur du nombre de positions simultanées (10-20 demandées)
  if (openPositionsCount() >= STRAT.MAX_POSITIONS_CAP) return;
  // Garde-fou exposition (montée à 90% pour permettre le grand nombre de positions)
  if (currentExposure() + stake > state.capital * STRAT.MAX_EXPOSURE_PCT) return;

  const qty = roundQty(symbol, (stake * lev) / S.price);
  if (qty <= 0) return;

  await setLeverage(symbol, lev);

  const before = await fetchPosition(symbol);
  const beforeQty = before ? before.qty : 0;

  try {
    await marketOrder(symbol, signal.side, qty);
  } catch (e) {
    if (e.binanceCode === -1007) {
      await new Promise((r) => setTimeout(r, 1500));
      const after = await fetchPosition(symbol);
      const step = SYMBOL_INFO[symbol] ? SYMBOL_INFO[symbol].stepSize : 0.001;
      if (!(after && after.qty > beforeQty + step)) {
        logLine(`↩️ ${symbol} : ordre non passé (timeout). Réessai plus tard.`);
        return;
      }
      logLine(`✅ ${symbol} : ordre passé malgré timeout.`);
    } else {
      logLine(`❌ ${symbol} ouverture: ${e.message}`);
      return;
    }
  }

  const entry = S.price;
  const exits = computeExits(symbol); // SL/TP calibrés sur l'ATR (ou fixes en repli)
  S.position = {
    side: signal.side, entry, qty, stake, lev, quality: signal.quality,
    isExceptional, excConditions: signal.excConditions || 0,
    slPct: exits.slPct, tpPct: exits.tpPct, exitSource: exits.source,
    sl: signal.side === 'BUY' ? entry * (1 - exits.slPct) : entry * (1 + exits.slPct),
    tp: signal.side === 'BUY' ? entry * (1 + exits.tpPct) : entry * (1 - exits.tpPct),
    peak: entry, trailing: false, openedAt: now,
    isExplosive: false, // passe à true si TP atteint en <45s (cf. managePosition)
  };
  S.lastEntryAt = now;
  if (viaRelaxed) state.lastRelaxedExcAt = now; // limite : 1 mise 9% via 3/4 par fenêtre
  const tag = isExceptional ? (viaRelaxed ? ' 💎MISE-EXC-9%(3/4)' : ' 💎MISE-EXC-9%(4/4)') : '';
  logLine(`🟢 ${symbol} ${signal.side} qty=${qty} @ ${entry.toFixed(4)} lev=${lev}x Q=${signal.quality} (${signal.excConditions || 0}/4) SL-${(exits.slPct*100).toFixed(2)}%/TP+${(exits.tpPct*100).toFixed(2)}%[${exits.source}]${tag}`);
  logLine(`🟢 ${symbol} ${signal.side} qty=${qty} @ ${entry.toFixed(4)} lev=${lev}x Q=${signal.quality} (${signal.excConditions || 0}/4)${tag}`);
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
  const fees = pos.stake * pos.lev * STRAT.FEE_PER_SIDE * 2;
  const net = gross - fees;

  state.capital += net;
  state.stats.gross += gross;
  state.stats.fees += fees;
  state.stats.net += net;
  if (net >= 0) state.stats.wins++; else state.stats.losses++;
  if (state.capital > state.peakCapital) state.peakCapital = state.capital;
  const dd = (state.peakCapital - state.capital) / state.peakCapital;
  if (dd > state.maxDrawdown) state.maxDrawdown = dd;

  state.trades.unshift({
    symbol, side: pos.side, entry: pos.entry, exit, lev: pos.lev, quality: pos.quality,
    investi: pos.stake.toFixed(2), pnlPct: (pnlPct * 100).toFixed(2),
    gross: gross.toFixed(2), fees: fees.toFixed(2), net: net.toFixed(2),
    exceptional: !!pos.isExceptional, explosive: !!pos.isExplosive,
    reason, durationMs: Date.now() - pos.openedAt,
  });
  if (state.trades.length > 100) state.trades.pop();

  logLine(`🔴 ${symbol} ${reason} @ ${exit.toFixed(4)} | net=${net.toFixed(2)}$ | capital=${state.capital.toFixed(2)}$`);
  S.position = null;
  broadcast({ type: 'trade', stats: state.stats, capital: state.capital, positions: livePositions() });

  if (state.capital <= state.capitalStart * (1 - STRAT.KILL_PCT)) {
    state.running = false;
    state.killed = true;
    logLine(`🛑 KILL SWITCH -45% — capital ${state.capital.toFixed(2)}$. Bot arrêté.`);
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

  if (pos.side === 'BUY' && px > pos.peak) pos.peak = px;
  if (pos.side === 'SELL' && px < pos.peak) pos.peak = px;

  // Suivi du creux (pour détecter un rebond quand on est en perte)
  if (pos.trough == null) pos.trough = px;
  if (pos.side === 'BUY' && px < pos.trough) pos.trough = px;
  if (pos.side === 'SELL' && px > pos.trough) pos.trough = px;

  if (!pos.trailing && pnlPct >= STRAT.TRAIL_START) pos.trailing = true;

  // Niveau SL propre à CETTE position (fixe -1% ici)
  const slPct = pos.slPct != null ? pos.slPct : STRAT.SL_PCT;

  // --- Sorties (priorité absolue) : SL fixe, puis trailing une fois armé ---
  // Pas de TP plafond (NO_FIXED_TP) : le trade court tant que le trailing tient.
  if (pos.trailing) {
    const draw = pos.side === 'BUY' ? (pos.peak - px) / pos.peak : (px - pos.peak) / pos.peak;
    if (draw >= STRAT.TRAIL_PCT) { closePos(symbol, 'TRAILING'); return; }
  } else if (pnlPct <= -slPct) {
    closePos(symbol, 'STOP-LOSS'); return;
  }

  // --- Fenêtre intermédiaire : DÉSACTIVÉE (TIME_EXIT_INTERMEDIATE=false) ---
  // Seul le filet ultime 14 min ferme par le temps ; sinon SL ou trailing.
  if (STRAT.TIME_EXIT_INTERMEDIATE && age >= STRAT.TIME_EXIT_FROM && age < STRAT.TIME_EXIT_HARD) {
    if (pnlPct > 0) {
      const giveback = pos.side === 'BUY' ? (pos.peak - px) / pos.peak : (px - pos.peak) / pos.peak;
      if (giveback >= STRAT.TIME_EXIT_GIVEBACK) { closePos(symbol, 'TEMPS-PROFIT'); return; }
    } else if (pnlPct <= STRAT.TIME_EXIT_WORSENING) {
      closePos(symbol, 'TEMPS-PERTE'); return;
    } else {
      const rebound = pos.side === 'BUY' ? (px - pos.trough) / pos.trough : (pos.trough - px) / pos.trough;
      if (rebound >= STRAT.TIME_EXIT_GIVEBACK) { closePos(symbol, 'TEMPS-REBOND'); return; }
    }
  }

  // --- Filet ultime : sortie forcée à 14 min ---
  if (age >= STRAT.TIME_EXIT_HARD) { closePos(symbol, 'TEMPS-MAX'); return; }
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
  for (const symbol of ALL_SYMBOLS) {
    await refreshKlines(symbol);
    await new Promise((r) => setTimeout(r, 150));
  }
  rankActiveSymbols(); // recalcule le top-N après chaque rafraîchissement des données
  broadcast({ type: 'snapshot', data: snapshot() });
}

// Classe les 20 symboles par score = volatilité(ATR) x force d'alignement de tendance,
// et ne garde que les ACTIVE_TOP_N meilleurs comme tradables. On concentre le capital
// là où ça bouge ET où la tendance est nette. Les positions déjà ouvertes restent gérées.
function rankActiveSymbols() {
  const scored = ALL_SYMBOLS.map((s) => {
    const S = state.sym[s];
    const atr = S.short.atrPct || 0;
    const align = Math.abs(S.mtf.alignNorm || 0);
    return { s, score: atr * (0.5 + align) };
  }).sort((a, b) => b.score - a.score);

  const top = scored.slice(0, STRAT.ACTIVE_TOP_N).map((x) => x.s);
  // On force l'inclusion des symboles déjà en position (pour continuer à les gérer)
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
    const fees = pos.stake * pos.lev * STRAT.FEE_PER_SIDE * 2;
    out.push({
      symbol, side: pos.side, entry: pos.entry, current: px, lev: pos.lev,
      quality: pos.quality, investi: pos.stake, sl: pos.sl, tp: pos.tp,
      trailing: pos.trailing, exceptional: !!pos.isExceptional, explosive: !!pos.isExplosive,
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
      rsi: S.indicators.rsi, align: S.mtf.alignNorm, hasPosition: !!S.position,
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
    excOpen: exceptionalOpenCount(), excMax: STRAT.MAX_EXCEPTIONAL_CONCURRENT,
    wins: state.stats.wins, losses: state.stats.losses,
    stats: state.stats, winRate: tot ? (state.stats.wins / tot) * 100 : null,
    positions: livePositions(), symbols: symbolsOverview(),
    trades: state.trades.slice(0, 40), log: state.log.slice(0, 50),
    strat: { sl: STRAT.SL_PCT * 100, tp: STRAT.TP_PCT * 100, qMin: STRAT.Q_MIN_VOLATILE, qMax: STRAT.Q_MIN_CALM, ratio: STRAT.TP_PCT / STRAT.SL_PCT },
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
    <span id="mode" class="badge net">TESTNET</span>
    <span id="run" class="badge off">PAUSE</span>
  </div>
  <div class="sub" id="stratline">Stratégie 2,5:1 · 20 cryptos</div>

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
    <thead><tr><th>Symbole</th><th>Prix</th><th>Courbe</th><th>Biais</th><th>Q</th><th>RSI</th><th>Align. MTF</th><th>Statut</th></tr></thead>
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
    $('stratline').textContent='Stratégie '+(s.strat?s.strat.ratio.toFixed(1):'2.5')+':1 · SL -'+(s.strat?s.strat.sl:1)+'% / TP +'+(s.strat?s.strat.tp:2.5)+'% · Q '+(s.strat?s.strat.qMin:40)+'-'+(s.strat?s.strat.qMax:50)+' · levier 3-12x · 10-20 pos';
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
      const tags=(p.exceptional?'<span class="tag exc">9%💎</span>':'')+(p.explosive?'<span class="tag exp">💥</span>':'');
      return '<tr><td>'+p.symbol+tags+'</td><td><span class="pill '+sc+'">'+st+'</span></td>'+
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
      const al=s.align!=null?Math.round(s.align*100)+'%':'—';
      return '<tr><td>'+s.symbol+'</td><td>'+(s.price?px(s.price):'—')+'</td>'+
        '<td>'+sparkline(s.spark)+'</td>'+
        '<td><span class="pill '+bc+'">'+b+'</span></td>'+
        '<td class="qbadge '+qcol+'">'+q+'</td>'+
        '<td>'+(s.rsi!=null?s.rsi.toFixed(0):'—')+'</td>'+
        '<td>'+al+'</td>'+
        '<td>'+(s.hasPosition?'<span class="pill long">EN POSITION</span>':'<span class="mut">-</span>')+'</td></tr>';
    }).join('');
  }

  function renderTrades(list){
    const tb=$('trades');
    if(!list||!list.length){tb.innerHTML='<tr><td colspan="11" class="mut" style="text-align:center;padding:14px">Aucun trade</td></tr>';return;}
    tb.innerHTML=list.map(t=>{
      const net=Number(t.net),gross=Number(t.gross!=null?t.gross:net),sc=t.side==='BUY'?'long':'short',st=t.side==='BUY'?'LONG':'SHORT';
      const tags=(t.exceptional?'<span class="tag exc">9%</span>':'')+(t.explosive?'<span class="tag exp">💥</span>':'');
      return '<tr><td>'+t.symbol+tags+'</td><td><span class="pill '+sc+'">'+st+'</span></td><td>'+t.lev+'x</td>'+
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
  logLine(`🚀 Itachi Multi — ${MODE.toUpperCase()} — ${ALL_SYMBOLS.length} symboles — capital $${CAPITAL_START}`);
  logLine(`📐 Stratégie ${(STRAT.TP_PCT / STRAT.SL_PCT).toFixed(1)}:1 — SL -${STRAT.SL_PCT * 100}% / TP +${STRAT.TP_PCT * 100}% — Q ${STRAT.Q_MIN_VOLATILE}-${STRAT.Q_MIN_CALM} — levier 3-12x — 10-20 positions — kill -45%`);
  if (!API_KEY || !API_SECRET) logLine('⚠️ Cles API manquantes — lecture seule (pas d ordres).');
  await loadSymbolInfo();
  await refreshAllKlines();
  connectPriceStreams();
  setInterval(refreshAllKlines, 30000);
  setInterval(rankActiveSymbols, STRAT.RANK_REFRESH_MS); // re-classement top-N toutes les 5 min
  setInterval(reconcile, 9000);
  setInterval(() => broadcast({ type: 'symbols', symbols: symbolsOverview() }), 3000); // mini-courbes vivantes
  server.listen(PORT, () => logLine(`🌐 Dashboard sur le port ${PORT}`));
}

start();
