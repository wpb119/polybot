/**
 * STALE COPY — do not use for trading.
 * Live + dry engine is src/strategy/gap_swing.rs, matching
 * poly-history/new_strategy/strategy-gap-swing-rust-agent.js
 * (first ask 12–78¢, restart ≥45s left, flatten T−25s).
 *
 * GAP SWING — BTC 5m Up/Down major-swing gap capture
 *
 * Portable copy of poly-history web/src/gapCapture.ts (mode=raw).
 * Drop this file into a new project. No other poly-history imports.
 *
 * Backtest (poly-history, this logic, 10 shares, t+250ms, crypto taker fee):
 *   7d (UTC+9, ~1817 windows): **~+$1,902** · ~1,375 pairs · ~370 losing windows
 *   Target $2k not yet reached — unpaired settle drag is the remaining bottleneck.
 *
 * WHAT IT DOES
 *   1. Zigzag on BTC Δ from PTB (open/strike). Confirm peak/trough after $10 pullback.
 *   2. Keep only major alternating mountains: min swing $40, peaks Δ≥+$40,
 *      troughs Δ≤+$52 (or big swing from last extreme).
 *   3. First leg: peak → buy DOWN (≤60¢), trough → buy UP (≤60¢). Ask must follow.
 *      Skip deep UP dumps (Δ < −$75 unless ask ≤20¢) and flash falls.
 *   4. Second leg: pair opposite at next extreme, or early opposite after DOWN first
 *      leg once Δ dumped ≥$30 from anchor. YES+NO + fees must clear net ≥ 0.
 *   5. Fill = ask at t+250ms. 10 shares. Fee = shares × 0.07 × p × (1−p).
 *   6. Unpaired near expiry (last 25s): buy opposite (`EMERGENCY_OPP`) to force-pair
 *      (ask ≤ 0.95, skip net-gap gate). Leftover → settle.
 *
 * DO NOT
 *   - Instant-lock both sides whenever combined ask < $1 (kills 7d PnL).
 *   - Force TWAP-zone / cheaper-opp every tick (over-pairs losers).
 *   - Mid-window bounce-exit on every UP trough (hurts total more than it helps
 *     a few feedback windows — bounce params default OFF).
 *
 * WIRE-UP (new project)
 *
 *   import { P, fee, GapSwingEngine, runWindow } from './strategy-gap-swing.js';
 *
 *   const bot = new GapSwingEngine({ openMs, endMs, strike: ptb });
 *   bot.onBinance(t, px);
 *   bot.onCoinbase(t, px); // optional; BN Δ is primary
 *   bot.onAsks(t, upAsk, downAsk);
 *   for (const intent of bot.poll(t)) {
 *     // intent.action 'BUY' | 'SELL'
 *     // intent.side   'UP' | 'DOWN'
 *     // intent.shares, intent.reason, intent.limitHint
 *   }
 *
 *   // Batch replay (matches poly-history raw sim):
 *   const { trades, totalPnl, pairs, swings } = runWindow({
 *     openMs, endMs, strike, binance, coinbase, asks, winner,
 *   });
 */

export const P = {
  WINDOW_MS: 300_000,
  GRID_MS: 250,
  TICK_MS: 500,

  TAKER_DELAY_MS: 250,
  SHARES: 10,
  FEE_RATE: 0.07,
  SELL_HAIRCUT: 0.01,

  minSwingUsd: 40,
  confirmUsd: 10,
  maxCheapAsk: 0.6,
  maxSecondAsk: 0.76,
  maxEarlyOppAsk: 0.88,
  minAsk: 0.05,
  minPeakDelta: 40,
  maxTroughDelta: 52,
  regimeDelta: 999,
  regimeMaxAsk: 0.4,
  earlyOppSwingUsd: 30,
  /** 999 = bounce-pair / bounce-exit OFF (7d-optimal). */
  earlyOppBounceUsd: 999,
  bounceRegimeMaxDelta: -999,
  minUpFirstDelta: -75,
  rapidFallUsd: 55,
  rapidFallMs: 12_000,
  /** 999 = mid-window adverse flatten OFF. */
  unpairedAdverseUsd: 999,
  minLeftMs: 10_000,
  maxPairs: 16,
  delayBufferPerLeg: 0.005,
  minRawPairNet: 0,
  minNetGap: 0.015,
  emergencyLeftMs: 25_000,

  /** raw | profitable | primary (marks only). */
  mode: "raw",
};

export function fee(price, shares = P.SHARES) {
  const p = Math.min(0.99, Math.max(0.01, price));
  return Math.round(shares * P.FEE_RATE * p * (1 - p) * 1e5) / 1e5;
}

function feeShare(price) {
  const p = Math.min(0.99, Math.max(0.01, price));
  return P.FEE_RATE * p * (1 - p);
}

function sellPx(ask) {
  return Math.max(0.01, ask - P.SELL_HAIRCUT);
}

function pxAt(series, t) {
  if (!series?.length) return null;
  let found = null;
  for (const p of series) {
    if (p.t > t) break;
    found = p.price;
  }
  return found;
}

function askAt(asks, t, side) {
  let found = null;
  for (const p of asks) {
    if (p.t > t) break;
    const px = side === "UP" ? p.up : p.down;
    if (px > 0 && px <= 1.5) found = px;
  }
  return found;
}

function deltaAt(btc, t, ptb) {
  const px = pxAt(btc, t);
  return px == null ? null : px - ptb;
}

function deltaDropOver(btc, t, ptb, lookbackMs) {
  const now = deltaAt(btc, t, ptb);
  const prev = deltaAt(btc, t - lookbackMs, ptb);
  if (now == null || prev == null) return null;
  return prev - now;
}

/**
 * Classic zigzag on BTC open-delta → major alternating peaks/troughs.
 * @returns {Array<{ t:number, d:number, kind:'peak'|'trough', side:'UP'|'DOWN', ask:number }>}
 */
export function detectMajorSwings(btc, asks, openMs, endMs, ptb, params = P) {
  const grid = [];
  for (let t = openMs + 800; t < endMs - 800; t += params.GRID_MS ?? P.GRID_MS) {
    const px = pxAt(btc, t);
    if (px == null) continue;
    grid.push({ t, d: px - ptb });
  }
  if (grid.length < 8) return [];

  const raw = [];
  let hunting = grid[0].d >= 0 ? "peak" : "trough";
  let extreme = grid[0];
  for (const g of grid) {
    if (hunting === "peak") {
      if (g.d >= extreme.d) {
        extreme = g;
        continue;
      }
      if (extreme.d - g.d >= params.confirmUsd) {
        raw.push({ t: extreme.t, d: extreme.d, kind: "peak" });
        hunting = "trough";
        extreme = g;
      }
    } else {
      if (g.d <= extreme.d) {
        extreme = g;
        continue;
      }
      if (g.d - extreme.d >= params.confirmUsd) {
        raw.push({ t: extreme.t, d: extreme.d, kind: "trough" });
        hunting = "peak";
        extreme = g;
      }
    }
  }

  const major = [];
  let lastIdx = -1;
  for (let i = 0; i < raw.length; i++) {
    const e = raw[i];
    const prev = major.at(-1);
    if (!prev) {
      major.push(e);
      lastIdx = i;
      continue;
    }
    if (prev.kind === e.kind) {
      if (i === lastIdx + 1) {
        if (e.kind === "peak" && e.d >= prev.d) {
          major[major.length - 1] = e;
          lastIdx = i;
        } else if (e.kind === "trough" && e.d <= prev.d) {
          major[major.length - 1] = e;
          lastIdx = i;
        }
        continue;
      }
      const between = raw.slice(lastIdx + 1, i).filter((x) => x.kind !== e.kind);
      let best = null;
      for (const b of between) {
        if (!best) {
          best = b;
          continue;
        }
        if (b.kind === "peak" && b.d >= best.d) best = b;
        if (b.kind === "trough" && b.d <= best.d) best = b;
      }
      const bridgeOk =
        best != null &&
        Math.abs(best.d - prev.d) >= params.minSwingUsd * 0.55 &&
        Math.abs(e.d - best.d) >= params.minSwingUsd;
      if (bridgeOk && best) {
        major.push(best);
        major.push(e);
        lastIdx = i;
      } else if (e.kind === "peak" && e.d >= prev.d) {
        major[major.length - 1] = e;
        lastIdx = i;
      } else if (e.kind === "trough" && e.d <= prev.d) {
        const grand = major.at(-2);
        if (grand && Math.abs(prev.d - grand.d) >= params.minSwingUsd) continue;
        major[major.length - 1] = e;
        lastIdx = i;
      }
      continue;
    }
    if (Math.abs(e.d - prev.d) >= params.minSwingUsd) {
      major.push(e);
      lastIdx = i;
    }
  }

  const out = [];
  for (const e of major) {
    const side = e.kind === "peak" ? "DOWN" : "UP";
    const ask = askAt(asks, e.t, side);
    if (ask == null || ask < params.minAsk) continue;
    if (endMs - e.t < params.minLeftMs) continue;

    const last = out.at(-1);
    if (last && last.kind === e.kind) continue;

    const bigFromLast = last != null && Math.abs(e.d - last.d) >= params.minSwingUsd * 1.15;
    if (e.kind === "peak") {
      if (e.d < params.minPeakDelta && !bigFromLast) continue;
    } else {
      if (e.d > params.maxTroughDelta && !bigFromLast) continue;
    }
    if (last && Math.abs(e.d - last.d) < params.minSwingUsd) continue;

    out.push({ t: e.t, d: e.d, kind: e.kind, side, ask });
  }
  return out;
}

/**
 * Batch sim — same economics as poly-history raw gapCapture.
 *
 * @param {{
 *   openMs: number, endMs: number, strike?: number|null,
 *   binance: Array<{t:number, price:number}>,
 *   coinbase?: Array<{t:number, price:number}>,
 *   asks: Array<{t:number, up:number, down:number}>,
 *   winner?: 'UP'|'DOWN'|null,
 *   params?: Partial<typeof P>,
 * }} w
 */
export function runWindow(w) {
  const params = { ...P, ...(w.params ?? {}), mode: w.params?.mode ?? P.mode };
  const openMs = w.openMs;
  const endMs = w.endMs;
  const ptb = w.strike ?? null;
  const btc = w.binance ?? [];
  const asks = w.asks ?? [];
  const winner = w.winner ?? null;
  const Q = params.SHARES;
  const DELAY = params.TAKER_DELAY_MS;

  const trades = [];
  if (ptb == null || !btc.length || !asks.length) {
    return { trades, totalPnl: 0, pairs: 0, swings: [], missed: 0 };
  }

  const swings = detectMajorSwings(btc, asks, openMs, endMs, ptb, params);
  if (params.mode === "primary") {
    return { trades, totalPnl: 0, pairs: 0, swings, missed: 0 };
  }

  const lastAsk = asks.at(-1);
  const lots = [];
  let nextLotId = 1;
  let pairsDone = 0;
  let missed = 0;
  let nextTradeT = openMs;
  let anchor = null;

  const unpairedLots = (side) => lots.filter((l) => l.side === side && l.shares - l.paired > 0);
  const unpairedShares = () => lots.reduce((n, l) => n + Math.max(0, l.shares - l.paired), 0);

  const pairLots = (t) => {
    for (;;) {
      const up = unpairedLots("UP")[0];
      const dn = unpairedLots("DOWN")[0];
      if (!up || !dn) break;
      const q = Math.min(up.shares - up.paired, dn.shares - dn.paired);
      if (q <= 0) break;
      up.paired += q;
      dn.paired += q;
      const feeShareAmt = (up.fee * q) / up.shares + (dn.fee * q) / dn.shares;
      const gross = q * (1 - up.fill - dn.fill);
      const net = gross - feeShareAmt;
      pairsDone += 1;
      trades.push({
        kind: "PAIR",
        side: "UP",
        t,
        fill: up.fill + dn.fill,
        shares: q,
        reason: "pair",
        net,
        upFill: up.fill,
        downFill: dn.fill,
      });
    }
  };

  const tryFill = (t, side) => {
    const fillT = t + DELAY;
    if (fillT >= endMs - 600) return null;
    const fill = askAt(asks, fillT, side);
    if (fill == null || fill < params.minAsk || fill > 0.95) return null;
    return { fillT, fill };
  };

  const netGapOk = (a, b, loose = false) => {
    const fees = feeShare(a) + feeShare(b) + 2 * params.delayBufferPerLeg;
    const net = 1 - a - b - fees;
    if (params.mode === "raw") {
      const floor = loose ? params.minRawPairNet - 0.006 : params.minRawPairNet;
      return net >= floor;
    }
    const need = loose ? params.minNetGap * 0.5 : params.minNetGap;
    return net >= need;
  };

  const addLot = (side, fillT, fill, extKind, kind, tag, anchorD) => {
    const f = fee(fill, Q);
    lots.push({
      id: nextLotId++,
      side,
      shares: Q,
      fill,
      fee: f,
      t: fillT,
      paired: 0,
      extKind,
      anchorD,
    });
    trades.push({
      kind: "BUY",
      side,
      t: fillT,
      fill,
      shares: Q,
      reason: kind === "FLIP" ? tag || "flip" : tag || "start",
      net: 0,
    });
    const pairedBefore = pairsDone;
    pairLots(fillT);
    nextTradeT = pairsDone > pairedBefore ? fillT + 8_000 : fillT + 400;
    if (kind === "START") {
      anchor = { t: fillT, d: anchorD, side, kind: extKind };
    } else {
      anchor = null;
    }
  };

  const emergencySell = (t, side) => {
    const open = unpairedLots(side);
    if (!open.length) return;
    const fillT = t + DELAY;
    if (fillT >= endMs - 400) return;
    const ask = askAt(asks, fillT, side);
    if (ask == null || ask <= 0) return;
    const px = sellPx(ask);
    for (const lot of open) {
      const q = lot.shares - lot.paired;
      if (q <= 0) continue;
      const feeS = fee(px, q);
      const feeBuy = (lot.fee * q) / lot.shares;
      lot.paired = lot.shares;
      trades.push({
        kind: "SELL",
        side,
        t: fillT,
        fill: px,
        shares: q,
        reason: "flatten",
        net: q * px - (q * lot.fill + feeBuy) - feeS,
      });
    }
  };

  const trySecondLeg = (t, side, extKind, tag) => {
    const needOpp = unpairedLots(side === "UP" ? "DOWN" : "UP");
    if (!needOpp.length) return false;
    const first = needOpp[0];
    const ask = askAt(asks, t, side);
    const force = tag.startsWith("EMERGENCY");
    const maxAsk = force ? 0.95 : tag.startsWith("EARLY_") ? params.maxEarlyOppAsk : params.maxSecondAsk;
    if (ask == null || ask < params.minAsk || ask > maxAsk) return false;
    if (!force && !netGapOk(first.fill, ask, false)) return false;
    const filled = tryFill(t, side);
    if (!filled) {
      missed += 1;
      return false;
    }
    if (!force && !netGapOk(first.fill, filled.fill, false)) {
      missed += 1;
      return false;
    }
    addLot(side, filled.fillT, filled.fill, extKind, "FLIP", tag, first.anchorD);
    return true;
  };

  const tryFirstLeg = (sw) => {
    if (unpairedShares() > 0) return false;
    if (sw.ask > params.maxCheapAsk) return false;
    if (sw.kind === "peak" && sw.d < -params.regimeDelta && sw.ask > params.regimeMaxAsk) return false;
    if (sw.kind === "trough" && sw.d > params.regimeDelta && sw.ask > params.regimeMaxAsk) return false;
    if (sw.kind === "trough" && sw.side === "UP") {
      if (sw.d < params.minUpFirstDelta && sw.ask > 0.2) return false;
      const drop = deltaDropOver(btc, sw.t, ptb, params.rapidFallMs);
      if (params.rapidFallUsd < 500 && drop != null && drop >= params.rapidFallUsd && sw.d < 0 && sw.ask > 0.2)
        return false;
    }
    if (sw.ask < 0.12) return false;

    if (params.mode === "profitable") {
      const expOpp = Math.min(0.58, Math.max(params.minAsk, 1 - sw.ask - params.minNetGap - 0.03));
      if (!netGapOk(sw.ask, expOpp, true)) return false;
    }
    const filled = tryFill(sw.t, sw.side);
    if (!filled) {
      missed += 1;
      return false;
    }
    if (sw.kind === "trough" && sw.side === "UP") {
      const fillD = deltaAt(btc, filled.fillT, ptb);
      if (params.minUpFirstDelta > -500 && fillD != null && fillD < params.minUpFirstDelta && filled.fill > 0.2) {
        missed += 1;
        return false;
      }
    }
    if (filled.fill > params.maxCheapAsk + 0.04) {
      missed += 1;
      return false;
    }
    addLot(sw.side, filled.fillT, filled.fill, sw.kind, "START", `SWING_${sw.kind.toUpperCase()}`, sw.d);
    return true;
  };

  const checkUnpairedRisk = (t, d) => {
    if (params.unpairedAdverseUsd >= 500 || unpairedShares() === 0) return;
    for (const side of ["UP", "DOWN"]) {
      const open = unpairedLots(side);
      if (!open.length) continue;
      const lot = open[0];
      const adverse =
        side === "UP"
          ? lot.anchorD - d >= params.unpairedAdverseUsd
          : d - lot.anchorD >= params.unpairedAdverseUsd;
      if (adverse) {
        emergencySell(t, side);
        anchor = null;
      }
    }
  };

  const checkRegimeBounceExit = (t, d) => {
    if (params.earlyOppBounceUsd >= 500 || params.bounceRegimeMaxDelta <= -500) return;
    if (!anchor || anchor.kind !== "trough" || anchor.side !== "UP") return;
    if (anchor.d > params.bounceRegimeMaxDelta) return;
    const lot = unpairedLots("UP")[0];
    if (!lot) return;
    if (t - lot.t < 18_000) return;
    const bounced = d - anchor.d;
    if (bounced < params.earlyOppBounceUsd || d > params.maxTroughDelta + 15) return;
    if (trySecondLeg(t, "DOWN", "peak", "EARLY_BOUNCE_DN")) return;
    if (d <= anchor.d + 8) {
      emergencySell(t, "UP");
      anchor = null;
    }
  };

  const events = swings.map((sw) => ({ typ: "swing", sw }));
  for (let t = openMs + 1000; t < endMs - params.minLeftMs; t += params.TICK_MS ?? P.TICK_MS) {
    const px = pxAt(btc, t);
    if (px == null) continue;
    events.push({ typ: "tick", t, d: px - ptb });
  }
  events.sort((a, b) => {
    const ta = a.typ === "swing" ? a.sw.t : a.t;
    const tb = b.typ === "swing" ? b.sw.t : b.t;
    if (ta !== tb) return ta - tb;
    return a.typ === "swing" ? -1 : 1;
  });

  for (const ev of events) {
    if (pairsDone >= params.maxPairs && unpairedShares() === 0) break;

    if (ev.typ === "tick") {
      checkUnpairedRisk(ev.t, ev.d);
      if (unpairedShares() === 0 || !anchor) continue;
      if (ev.t < nextTradeT) continue;

      if (params.earlyOppSwingUsd < 500 && anchor.kind === "peak" && anchor.side === "DOWN") {
        if (ev.d <= anchor.d - params.earlyOppSwingUsd) {
          trySecondLeg(ev.t, "UP", "trough", "EARLY_OPP");
        }
      } else if (params.earlyOppBounceUsd < 500 && anchor.kind === "trough" && anchor.side === "UP") {
        if (anchor.d <= params.bounceRegimeMaxDelta) {
          const bounced = ev.d - anchor.d;
          if (bounced >= params.earlyOppBounceUsd && ev.d <= params.maxTroughDelta + 15) {
            trySecondLeg(ev.t, "DOWN", "peak", "EARLY_BOUNCE_DN");
          }
        }
      }
      checkRegimeBounceExit(ev.t, ev.d);
      continue;
    }

    const sw = ev.sw;
    if (sw.t < nextTradeT) continue;
    if (unpairedLots(sw.side === "UP" ? "DOWN" : "UP").length) {
      trySecondLeg(sw.t, sw.side, sw.kind, `SWING_${sw.kind.toUpperCase()}_2`);
      continue;
    }
    if (unpairedShares() > 0) continue;
    tryFirstLeg(sw);
  }

  const lastT = endMs - params.emergencyLeftMs;
  if (unpairedShares() > 0 && lastT > openMs) {
    // Last 25s: buy opposite to force-pair (not sell).
    if (unpairedLots("UP").length) trySecondLeg(lastT, "DOWN", "peak", "EMERGENCY_OPP");
    if (unpairedLots("DOWN").length) trySecondLeg(lastT, "UP", "trough", "EMERGENCY_OPP");
  }

  for (const lot of lots) {
    const q = lot.shares - lot.paired;
    if (q <= 0) continue;
    const mark =
      winner === "UP"
        ? lot.side === "UP"
          ? 1
          : 0
        : winner === "DOWN"
          ? lot.side === "DOWN"
            ? 1
            : 0
          : lot.side === "UP"
            ? (lastAsk?.up ?? lot.fill)
            : (lastAsk?.down ?? lot.fill);
    const feeBuy = (lot.fee * q) / lot.shares;
    lot.paired = lot.shares;
    trades.push({
      kind: "SETTLE",
      side: lot.side,
      t: endMs - 1,
      fill: lot.fill,
      shares: q,
      reason: "settle",
      net: q * mark - (q * lot.fill + feeBuy),
    });
  }

  const totalPnl = trades.reduce((s, tr) => s + (tr.net ?? 0), 0);
  return { trades, totalPnl, pairs: pairsDone, swings, missed };
}

/**
 * Incremental feed API (same shape as TrendTrailEngine).
 * Buffers ticks; poll(t) re-runs swing logic on the buffer so far and emits
 * new BUY/SELL intents that have not been emitted yet.
 */
export class GapSwingEngine {
  /**
   * @param {{ openMs: number, endMs: number, strike?: number | null, params?: Partial<typeof P> }} opts
   */
  constructor(opts) {
    this.openMs = opts.openMs;
    this.endMs = opts.endMs;
    this.strike = opts.strike ?? null;
    this.params = { ...P, ...(opts.params ?? {}) };
    this.binance = [];
    this.coinbase = [];
    this.asks = [];
    this.emitted = new Set();
    this.pending = [];
    this.lastRunT = 0;
    this._lastTrades = [];
  }

  onBinance(t, price) {
    if (price > 0) this.binance.push({ t, price });
  }

  onCoinbase(t, price) {
    if (price > 0) this.coinbase.push({ t, price });
  }

  onAsks(t, up, down) {
    if (up > 0 && down > 0) this.asks.push({ t, up, down });
  }

  /**
   * @returns {Array<{ action:'BUY'|'SELL', side:'UP'|'DOWN', shares:number, reason:string, limitHint:number|null, t:number }>}
   */
  poll(t) {
    if (t - this.lastRunT < 200 && this.pending.length === 0) return [];
    this.lastRunT = t;
    const { trades } = runWindow({
      openMs: this.openMs,
      endMs: Math.min(this.endMs, t + 1),
      strike: this.strike,
      binance: this.binance,
      coinbase: this.coinbase,
      asks: this.asks,
      winner: null,
      params: this.params,
    });
    this._lastTrades = trades;
    for (const tr of trades) {
      if (tr.kind !== "BUY" && tr.kind !== "SELL") continue;
      if (tr.t > t) continue;
      const key = `${tr.kind}:${tr.side}:${tr.t}:${tr.reason}`;
      if (this.emitted.has(key)) continue;
      this.emitted.add(key);
      this.pending.push({
        action: tr.kind,
        side: tr.side,
        shares: tr.shares,
        reason: tr.reason,
        limitHint: tr.fill,
        t: tr.t,
      });
    }
    if (this.pending.length) {
      const out = this.pending;
      this.pending = [];
      return out;
    }
    return [];
  }
}

export function resolveWinner({ upAsk, downAsk, last, ptb }) {
  if (typeof upAsk === "number" && typeof downAsk === "number" && upAsk >= 0.55 && downAsk <= 0.45) return "UP";
  if (typeof upAsk === "number" && typeof downAsk === "number" && downAsk >= 0.55 && upAsk <= 0.45) return "DOWN";
  if (typeof last === "number" && typeof ptb === "number" && last !== ptb) return last > ptb ? "UP" : "DOWN";
  return null;
}

/** Same signature family as strategies/*.js — entry when BUY intent fires. */
export function decideEntry(s) {
  const bot = s.engine;
  if (!bot) return null;
  const intents = bot.poll(s.t).filter((x) => x.action === "BUY");
  const buy = intents[0];
  if (!buy) return null;
  const price = s.sideAsk ? s.sideAsk(buy.side) : buy.limitHint;
  if (!(price > 0 && price < 1)) return null;
  return { side: buy.side, price, reason: buy.reason, shares: buy.shares };
}

/** Flatten / emergency sell. Pair completion is handled inside runWindow. */
export function decideExit(s) {
  const bot = s.engine;
  if (!bot) return null;
  const intents = bot.poll(s.t).filter((x) => x.action === "SELL");
  const sell = intents[0];
  if (!sell) return null;
  return { action: "sell", price: sell.limitHint, reason: sell.reason, shares: sell.shares };
}
