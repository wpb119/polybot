/**
 * VENUE SWING — FINAL. Self-contained engine, zero external imports.
 *
 * This is the popped-out, best-verified strategy for Polymarket BTC 5m
 * Up/Down markets. 7d live-causal DB replay, 10 shares, fill = ask at
 * signal+70ms:  ~+$4,185  (vs +$2,937 for the PTB gap-swing engine).
 *
 * ONE-PARAGRAPH SUMMARY
 *   Run two independent zigzag detectors: one on Binance − Binance
 *   window-open, one on Coinbase − Coinbase window-open. Union their major
 *   swings (keep the more extreme of consecutive same-kind swings). A
 *   confirmed major peak buys DOWN, a confirmed major trough buys UP, always
 *   the cheap side within ask bounds. Pair the opposite side when YES+NO
 *   still nets ≥ 0 after fees. Flatten unpaired lots at T−25s. Settlement
 *   winner is decided by official Chainlink PTB vs final price — the venue
 *   deltas are ONLY a signal, never the settlement rule.
 *
 * READ THIS BEFORE IMPLEMENTING (no ambiguity allowed)
 *   - All timestamps are epoch milliseconds (i64).
 *   - "Window open" openMs = market startTs × 1000; endMs = openMs + 300000.
 *   - Venue open price = last venue print at/before openMs; if none exists,
 *     the first print after openMs. Each venue uses ITS OWN open.
 *   - Delta tape per venue starts with an anchor point {t: openMs, d: 0}
 *     followed by one point per print inside [openMs, endMs]: d = px − open.
 *   - The zigzag runs per venue on a 250ms grid built from that venue's tape
 *     (forward-fill last d at each grid t). Strike is 0 by construction.
 *   - The trade loop consumes: (a) the UNION-merged swing list, and (b) an
 *     interleaved tick tape (both venues' delta points sorted by t) for the
 *     early-opposite dump trigger and the rapid-fall guard.
 *   - Fill model: BUY fills at the displayed ask at signal_t + 70ms.
 *     SELL fills at (ask at signal_t + 70ms) − $0.01 haircut.
 *   - Fee per fill: shares × 0.07 × p × (1−p), p clamped to [0.01, 0.99],
 *     rounded to 5 decimals.
 *   - runWindow() below is the replay/parity oracle. A live agent MUST be a
 *     streaming state machine producing identical decisions (see the guide
 *     VENUE_SWING_AGENT_GUIDE.md); it must never re-run the whole window
 *     per tick.
 *
 * DO NOT
 *   - Use Chainlink PTB, DB "current", or any TWAP as the zigzag input.
 *   - Blend the two venues into one averaged price series (union of separate
 *     zigzags beats the mean blend by ~$1.1k / 7d).
 *   - Require both venues to agree before trading (kills PnL: +$921 vs +$4.2k).
 *   - Instant-lock both sides just because upAsk + downAsk < $1.
 *   - Trail/dump-flatten mid-window; the only forced exit is at T−25s.
 *   - Buy a first leg above 78¢ or below 12¢.
 */

export const P = {
  WINDOW_MS: 300_000,
  GRID_MS: 250, // zigzag sampling grid
  TICK_MS: 500, // early-opposite scan grid
  TAKER_DELAY_MS: 70, // signal → fill latency
  SHARES: 10,
  FEE_RATE: 0.07,
  SELL_HAIRCUT: 0.01, // sells hit ~1¢ inside displayed ask

  minSwingUsd: 40, // adjacent majors must differ by ≥ $40
  confirmUsd: 10, // zigzag reversal confirmation
  minPeakDelta: 40, // standalone peak must be ≥ +$40
  maxTroughDelta: 52, // standalone trough must be ≤ +$52
  maxCheapAsk: 0.78, // first-leg ask ceiling
  maxSecondAsk: 0.76, // swing second-leg ask ceiling
  maxEarlyOppAsk: 0.88, // early-opposite ask ceiling
  minAsk: 0.04, // dead-token floor for any fill
  minFirstAsk: 0.12, // first-leg ask floor
  minUpFirstDelta: -75, // don't buy UP first when d < −$75 (unless ask ≤ 20¢)
  rapidFallUsd: 55, // flash-dump guard
  rapidFallMs: 12_000,
  earlyOppSwingUsd: 30, // dump from peak anchor that triggers early UP
  minLeftMs: 10_000, // no first legs with < 10s left
  restartMinLeftMs: 45_000, // after a pair, new first needs ≥ 45s left
  pairCooldownMs: 8_000, // wait after completing a pair
  firstCooldownMs: 400, // wait after an unpaired buy
  maxPairs: 16,
  delayBufferPerLeg: 0.005, // pessimism added per leg in the pair-gate
  minRawPairNet: 0, // pair only when 1 − a − b − fees ≥ this
  emergencyLeftMs: 25_000, // flatten unpaired at endMs − 25s
};

export const VENUE_SWING_SPEC = Object.freeze({
  name: "venue-swing-final",
  clock: "epoch_ms_i64",
  signal: "two zigzags: binance−bnOpen and coinbase−cbOpen, union-merged",
  settle: "official Chainlink PTB vs final — signal only, never settlement",
  fill: "BUY ask@t+70ms; SELL ask@t+70ms − 1¢",
  fee: "shares × 0.07 × p × (1−p)",
  hotPath: ["append tick", "update zigzag", "merge", "lookup pre-signed order", "submit"],
  presign: { priceStep: 0.01, buyMin: 0.05, buyMax: 0.95, sellMin: 0.01, sellMax: 0.95 },
  expected7dPnl10Shares: 4185,
});

/* ------------------------------------------------------------------ *
 *  Basic helpers                                                      *
 * ------------------------------------------------------------------ */

function roundToStep(price, step, dir) {
  const q = price / step;
  const rounded = dir === "down" ? Math.floor(q) * step : Math.ceil(q) * step;
  return Math.max(0.01, Math.min(0.99, Math.round(rounded * 100) / 100));
}

export function orderKey(action, side, price, shares = P.SHARES, step = 0.01) {
  const dir = action === "SELL" ? "down" : "up";
  const p = roundToStep(price, step, dir).toFixed(2);
  return `${action}:${side}:${p}:${shares}`;
}

/** Pre-sign the full BUY/SELL price grids before the window opens. */
export function buildPresignPlan({
  shares = P.SHARES,
  priceStep = VENUE_SWING_SPEC.presign.priceStep,
  buyMin = VENUE_SWING_SPEC.presign.buyMin,
  buyMax = VENUE_SWING_SPEC.presign.buyMax,
  sellMin = VENUE_SWING_SPEC.presign.sellMin,
  sellMax = VENUE_SWING_SPEC.presign.sellMax,
} = {}) {
  const orders = [];
  const seen = new Set();
  const pushGrid = (action, side, lo, hi) => {
    for (let p = lo; p <= hi + 1e-9; p += priceStep) {
      const price = roundToStep(p, priceStep, action === "SELL" ? "down" : "up");
      const key = orderKey(action, side, price, shares, priceStep);
      if (seen.has(key)) continue;
      seen.add(key);
      orders.push({ key, action, side, price, shares, timeInForce: "IOC" });
    }
  };
  for (const side of ["UP", "DOWN"]) {
    pushGrid("BUY", side, buyMin, buyMax);
    pushGrid("SELL", side, sellMin, sellMax);
  }
  return orders;
}

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

/** Last price at/before t (series sorted by t ascending). */
function pxAt(series, t) {
  if (!series?.length) return null;
  let found = null;
  for (const p of series) {
    if (p.t > t) break;
    found = p.price;
  }
  return found;
}

/** Last valid side ask at/before t. */
function askAt(asks, t, side) {
  let found = null;
  for (const p of asks) {
    if (p.t > t) break;
    const px = side === "UP" ? p.up : p.down;
    if (px > 0 && px <= 1.5) found = px;
  }
  return found;
}

function deltaAt(tape, t) {
  return pxAt(tape, t);
}

function deltaDropOver(tape, t, lookbackMs) {
  const now = deltaAt(tape, t);
  const prev = deltaAt(tape, t - lookbackMs);
  if (now == null || prev == null) return null;
  return prev - now;
}

/* ------------------------------------------------------------------ *
 *  Venue-open delta tapes                                             *
 * ------------------------------------------------------------------ */

function sortedTape(series) {
  if (!series?.length) return [];
  return series
    .filter((p) => p && p.t > 0 && p.price > 0)
    .slice()
    .sort((a, b) => a.t - b.t);
}

function lastPxAtRaw(series, t) {
  let found = null;
  for (const p of series) {
    if (p.t > t) break;
    if (p.price > 0) found = p.price;
  }
  return found;
}

/** Venue open = last print at/before openMs, else first print after. */
export function venueOpenPrice(series, openMs) {
  const atOrBefore = lastPxAtRaw(series, openMs);
  if (atOrBefore != null) return atOrBefore;
  for (const p of series) {
    if (p.t > openMs && p.price > 0) return p.price;
  }
  return null;
}

/** Delta tape: anchor {openMs, 0} + one point per print inside the window. */
export function deltaTape(series, openPx, openMs, endMs) {
  if (openPx == null) return [];
  const tape = [{ t: openMs, price: 0 }];
  for (const p of series) {
    if (p.t < openMs || p.t > endMs) continue;
    tape.push({ t: p.t, price: p.price - openPx });
  }
  return tape;
}

/* ------------------------------------------------------------------ *
 *  Zigzag major-swing detector (per venue, strike = 0)                *
 * ------------------------------------------------------------------ */

/**
 * Returns alternating major swings: [{t, d, kind: "peak"|"trough",
 * side: "DOWN"|"UP", ask}]. Identical algorithm to the PTB gap-swing
 * detector; input is a venue-open delta tape and strike 0.
 */
export function detectMajorSwings(tape, asks, openMs, endMs, params = P) {
  const grid = [];
  for (let t = openMs + 800; t < endMs - 800; t += params.GRID_MS ?? P.GRID_MS) {
    const px = pxAt(tape, t);
    if (px == null) continue;
    grid.push({ t, d: px });
  }
  if (grid.length < 8) return [];

  // 1. Raw zigzag: reversal confirmed after confirmUsd pullback/bounce.
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

  // 2. Keep only alternating majors ≥ minSwingUsd apart (with bridge rescue).
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

  // 3. Tradability filter: ask exists, time left, extremity thresholds.
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
    } else if (e.d > params.maxTroughDelta && !bigFromLast) continue;
    if (last && Math.abs(e.d - last.d) < params.minSwingUsd) continue;
    out.push({ t: e.t, d: e.d, kind: e.kind, side, ask });
  }
  return out;
}

/* ------------------------------------------------------------------ *
 *  Union merge of the two venues' major swings                        *
 * ------------------------------------------------------------------ */

/**
 * Union both lists sorted by t. Consecutive same-kind swings collapse to
 * the more extreme one; an alternation is kept only if it moved ≥
 * minSwingUsd from the previous kept swing.
 */
export function mergeUnionExtreme(bnSw, cbSw, minSwingUsd = P.minSwingUsd) {
  const all = [...bnSw, ...cbSw].sort((a, b) => a.t - b.t);
  const out = [];
  for (const e of all) {
    const last = out.at(-1);
    if (!last) {
      out.push(e);
      continue;
    }
    if (last.kind === e.kind) {
      if (e.kind === "peak" && e.d >= last.d) out[out.length - 1] = e;
      else if (e.kind === "trough" && e.d <= last.d) out[out.length - 1] = e;
      continue;
    }
    if (Math.abs(e.d - last.d) >= minSwingUsd) out.push(e);
  }
  return out;
}

/* ------------------------------------------------------------------ *
 *  Replay oracle                                                      *
 * ------------------------------------------------------------------ */

/**
 * w = {
 *   openMs, endMs,                      // window bounds, epoch ms
 *   binance:  [{t, price}, ...],        // raw venue prints, may pre-date open
 *   coinbase: [{t, price}, ...],
 *   asks:     [{t, up, down}, ...],     // Polymarket best asks
 *   winner:   "UP" | "DOWN" | null,     // official PTB settlement
 *   params?:  overrides of P
 * }
 * Returns { trades, totalPnl, pairs, swings, missed, bnOpen, cbOpen }.
 */
export function runWindow(w) {
  const params = { ...P, ...(w.params ?? {}) };
  const openMs = w.openMs;
  const endMs = w.endMs;
  const asks = w.asks ?? [];
  const winner = w.winner ?? null;
  const Q = params.SHARES;
  const DELAY = params.TAKER_DELAY_MS;
  const trades = [];

  const bn = sortedTape(w.binance ?? []);
  const cb = sortedTape(w.coinbase ?? []);
  const bnOpen = venueOpenPrice(bn, openMs);
  const cbOpen = venueOpenPrice(cb, openMs);
  const bnTape = deltaTape(bn, bnOpen, openMs, endMs);
  const cbTape = deltaTape(cb, cbOpen, openMs, endMs);

  const empty = { trades, totalPnl: 0, pairs: 0, swings: [], missed: 0, bnOpen, cbOpen };
  if (!asks.length) return empty;

  const bnSw = bnTape.length ? detectMajorSwings(bnTape, asks, openMs, endMs, params) : [];
  const cbSw = cbTape.length ? detectMajorSwings(cbTape, asks, openMs, endMs, params) : [];
  const swings = mergeUnionExtreme(bnSw, cbSw, params.minSwingUsd);

  // Interleaved tape drives early-opposite dumps and the rapid-fall guard.
  const tape = [...bnTape, ...cbTape].sort((a, b) => a.t - b.t);
  if (!tape.length) return empty;

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

  const netGapOk = (a, b) => {
    const fees = feeShare(a) + feeShare(b) + 2 * params.delayBufferPerLeg;
    return 1 - a - b - fees >= params.minRawPairNet;
  };

  const addLot = (side, fillT, fill, extKind, kind, tag, anchorD) => {
    const f = fee(fill, Q);
    lots.push({ id: nextLotId++, side, shares: Q, fill, fee: f, t: fillT, paired: 0, extKind, anchorD });
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
    nextTradeT =
      pairsDone > pairedBefore ? fillT + params.pairCooldownMs : fillT + params.firstCooldownMs;
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
    const maxAsk = tag.startsWith("EARLY_") ? params.maxEarlyOppAsk : params.maxSecondAsk;
    if (ask == null || ask < params.minAsk || ask > maxAsk) return false;
    if (!netGapOk(first.fill, ask)) return false;
    const filled = tryFill(t, side);
    if (!filled) {
      missed += 1;
      return false;
    }
    if (!netGapOk(first.fill, filled.fill)) {
      missed += 1;
      return false;
    }
    addLot(side, filled.fillT, filled.fill, extKind, "FLIP", tag, first.anchorD);
    return true;
  };

  const tryFirstLeg = (sw) => {
    if (unpairedShares() > 0) return false;
    if (endMs - sw.t < params.minLeftMs) return false;
    if (pairsDone > 0 && endMs - sw.t < params.restartMinLeftMs) return false;
    if (sw.ask > params.maxCheapAsk || sw.ask < params.minFirstAsk) return false;
    if (sw.kind === "trough" && sw.side === "UP") {
      if (sw.d < params.minUpFirstDelta && sw.ask > 0.2) return false;
      const drop = deltaDropOver(tape, sw.t, params.rapidFallMs);
      if (drop != null && drop >= params.rapidFallUsd && sw.d < 0 && sw.ask > 0.2) return false;
    }
    const filled = tryFill(sw.t, sw.side);
    if (!filled) {
      missed += 1;
      return false;
    }
    if (sw.kind === "trough" && sw.side === "UP") {
      const fillD = deltaAt(tape, filled.fillT);
      if (fillD != null && fillD < params.minUpFirstDelta && filled.fill > 0.2) {
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

  // Event stream: merged swings + 500ms delta ticks, sorted; swings first on tie.
  const events = swings.map((sw) => ({ typ: "swing", sw }));
  for (let t = openMs + 1000; t < endMs - params.minLeftMs; t += params.TICK_MS) {
    const px = pxAt(tape, t);
    if (px == null) continue;
    events.push({ typ: "tick", t, d: px });
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
      if (unpairedShares() === 0 || !anchor) continue;
      if (ev.t < nextTradeT) continue;
      if (anchor.kind === "peak" && anchor.side === "DOWN" && ev.d <= anchor.d - params.earlyOppSwingUsd) {
        trySecondLeg(ev.t, "UP", "trough", "EARLY_OPP");
      }
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

  // Emergency flatten of unpaired lots at endMs − 25s.
  const lastT = endMs - params.emergencyLeftMs;
  if (unpairedShares() > 0 && lastT > openMs) {
    if (unpairedLots("UP").length) emergencySell(lastT, "UP");
    if (unpairedLots("DOWN").length) emergencySell(lastT, "DOWN");
  }

  // Settle any residue at the official winner (or last ask if unresolved).
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
  return { trades, totalPnl, pairs: pairsDone, swings, missed, bnOpen, cbOpen };
}

/** Official settlement rule (unchanged from gap-swing). */
export function resolveWinner({ upAsk, downAsk, last, ptb }) {
  if (typeof upAsk === "number" && typeof downAsk === "number" && upAsk >= 0.55 && downAsk <= 0.45) return "UP";
  if (typeof upAsk === "number" && typeof downAsk === "number" && downAsk >= 0.55 && upAsk <= 0.45) return "DOWN";
  if (typeof last === "number" && typeof ptb === "number" && last !== ptb) return last > ptb ? "UP" : "DOWN";
  return null;
}
