# VENUE SWING — AI Agent Implementation Guide

Engine file (the single source of truth): `strategy-venue-swing-final.js`
Replay oracle: `runWindow()` in that file. **Any implementation is correct if
and only if it reproduces `runWindow()`'s trades exactly** (see §9 Acceptance).

Verified result: 7d live-causal DB replay, 10 shares, fill = ask at
signal+70ms → **+$4,223.71 over 1,970 windows** (vs +$2,937 for the older
PTB gap-swing engine). Zero-parity check: `scripts/parity-venue-final.ts`.

---

## 1. Market and clock

- Market: Polymarket "BTC Up/Down 5m" (`btc-updown-5m-<startTs>`).
- All timestamps are **epoch milliseconds, signed 64-bit int**.
- `openMs = startTs × 1000`, `endMs = openMs + 300_000`. No other clock.
- The contract settles UP if official Chainlink PTB < final price, DOWN
  otherwise. **The engine never computes settlement from venue prices.**

## 2. Inputs (three feeds, nothing else)

| Feed | Content | Used for |
|---|---|---|
| Binance trade prints | `{t, price}` BTCUSDT | zigzag A |
| Coinbase trade prints | `{t, price}` BTC-USD | zigzag B |
| Polymarket best asks | `{t, up, down}` | entries, exits, pair gate |

Rules:
- Keep prints with `price > 0` only; sort ascending by `t`. Prints from
  before `openMs` are kept — they define the venue open.
- An ask value is valid iff `0 < ask ≤ 1.5`. "Ask at time t" = the **last
  valid value at or before t** (forward fill). If none exists yet → null.
- PTB is NOT an input to the signal. It is only used by whoever resolves
  the winner after close.

## 3. Venue opens and delta tapes

For each venue independently:

1. `open = last print at/before openMs`; if no such print, the **first
   print after openMs**; if the venue has no prints at all, its tape is
   empty (engine then runs on the other venue alone).
2. Delta tape = `[{t: openMs, d: 0}]` followed by one point
   `{t, d: price − open}` for every print with `openMs ≤ t ≤ endMs`.

Never mix venues in a tape. Never average the two tapes.

## 4. Zigzag major-swing detector (run twice, once per tape)

Sample the tape on a **250 ms grid**: for `t = openMs+800; t < endMs−800;
t += 250`, take the forward-filled delta `d(t)`; skip grid points before the
first tape point. If fewer than 8 grid points → no swings.

**Pass 1 — raw zigzag.** Start hunting `peak` if `d(first grid point) ≥ 0`
else `trough`. Track the running extreme. A peak is confirmed when price
pulls back **≥ $10** (`confirmUsd`) below the extreme; a trough when it
bounces ≥ $10 above. On confirmation emit `{t, d, kind}` at the *extreme's*
time and flip the hunt. (Live note: the swing timestamp is the extreme, but
you only KNOW it at confirmation time — act at confirmation, see §7.)

**Pass 2 — alternating majors.** Walk the raw list keeping alternating
peaks/troughs at least **$40** (`minSwingUsd`) apart, with these exact rules:
- Same kind twice in a row (adjacent in raw): replace kept swing if the new
  one is more extreme.
- Same kind with skipped raw entries in between: try to "bridge" — find the
  most extreme opposite-kind raw entry in between; if `|bridge − prev| ≥
  0.55 × 40` and `|new − bridge| ≥ 40`, keep both bridge and new. Otherwise
  fall back to replace-if-more-extreme; a trough may NOT replace the kept
  trough when the swing into the kept trough was already ≥ $40.
- Different kind: keep only if `|new.d − prev.d| ≥ 40`.

**Pass 3 — tradability filter**, in list order, building the output:
- side = DOWN for peak, UP for trough; the side's ask at swing time must
  exist and be ≥ **$0.04** (`minAsk`);
- `endMs − t ≥ 10_000` (`minLeftMs`);
- drop if same kind as the previously kept output swing;
- peak needs `d ≥ +$40` (`minPeakDelta`), trough needs `d ≤ +$52`
  (`maxTroughDelta`) — **unless** the move from the previously kept swing is
  ≥ $46 (= 40 × 1.15);
- drop if `|d − previous kept d| < $40`.

## 5. Union merge

Concatenate both venues' swing lists, sort by `t` (stable). Walk once:
- first swing: keep;
- same kind as last kept: replace if more extreme (peak: `d ≥`; trough: `d ≤`);
- different kind: keep only if `|d − last kept d| ≥ $40`.

The merged list is THE signal. Also build the **interleaved tape** (all
delta points of both venues sorted by `t`) — it drives §6.4 and the
rapid-fall guard.

## 6. Trading rules (exact order of evaluation)

State: `lots[]` (each: side, shares, fill, fee, paired), `pairsDone`,
`nextTradeT` (cooldown gate), `anchor` (set on a first leg).

Event stream = merged swings + ticks every **500 ms** from `openMs+1000` to
`endMs−10_000` carrying the interleaved-tape delta. Sort by time; **on a tie
the swing is processed before the tick**. Stop consuming events once
`pairsDone ≥ 16` and no unpaired shares remain.

### 6.1 Fill model (applies to every order)
- BUY: fill price = ask at `signal_t + 70 ms`. Reject if the fill time is
  within 600 ms of `endMs`, or ask is null, < $0.04, or > $0.95.
- SELL: fill price = (ask at `signal_t + 70 ms`) − **$0.01**, floored at $0.01.
- Fee per fill: `shares × 0.07 × p × (1−p)`, `p` clamped [0.01, 0.99],
  rounded to 5 decimals.

### 6.2 First leg (on a merged swing; only when zero unpaired shares)
Skip unless ALL hold:
- `endMs − sw.t ≥ 10_000`; and if `pairsDone > 0`, `endMs − sw.t ≥ 45_000`;
- `sw.t ≥ nextTradeT`;
- swing ask in **[0.12, 0.78]**;
- for a trough/UP first: `sw.d ≥ −$75` unless ask ≤ $0.20; and NOT a flash
  dump — interleaved delta fell ≥ **$55 in the last 12 s** while `sw.d < 0`
  and ask > $0.20.

Then attempt the fill (§6.1). After the fill, re-check for UP firsts:
if the delta at fill time < −$75 and fill > $0.20 → abandon (count missed).
Abandon also if fill > 0.82 (= 0.78 + 0.04 chase cap).
On success: record lot, set `anchor = {d: sw.d, side, kind}`,
`nextTradeT = fillT + 400 ms`.

### 6.3 Second leg (on a merged swing; when holding an unpaired opposite lot)
- opposite side's swing: buy `sw.side` if its ask ≤ **0.76**, ≥ 0.04, and the
  **pair gate** passes: `1 − firstFill − ask − fee(firstFill) − fee(ask) −
  2×0.005 ≥ 0`. Re-check the gate against the actual fill price after §6.1.
- On success the lots pair immediately (FIFO, partial quantities allowed);
  each pair's net = `q × (1 − upFill − downFill) − proportional fees`;
  `nextTradeT = fillT + 8_000 ms`; anchor cleared.

### 6.4 Early opposite (on a 500 ms tick)
Only while holding an unpaired first leg with an anchor, `t ≥ nextTradeT`,
anchor is a **peak/DOWN** hold, and interleaved delta ≤ `anchor.d − $30`:
buy UP if UP ask ≤ **0.88** and the same pair gate passes. Same fill/re-check
rules. (There is deliberately NO symmetric early-DOWN.)

### 6.5 Forced flatten
At `endMs − 25_000`: SELL every unpaired lot (per side, one signal) with the
sell fill model. Never force-buy the opposite at this stage.

### 6.6 Settlement of residue
Any shares still unpaired at close mark at `winner == side ? 1 : 0` using the
official PTB winner (last ask as a fallback only if the winner is unknown).

## 7. Live streaming loop (MANDATORY architecture)

`runWindow()` is the replay oracle. Live must be an **incremental state
machine** — never re-run the window per tick:

```
before window:  presign all orders (buildPresignPlan): BUY 5–95¢,
                SELL 1–95¢, 1¢ grid, both sides, IOC
on venue print: append to that venue's tape; update that venue's zigzag
                (running extreme + hunting flag); on confirmation, emit the
                swing and push it through the union-merge state (§5, streamed:
                keep last-kept swing in memory)
on merged swing / every 500ms tick: evaluate §6.2–6.4; if an order is due,
                LOOK UP the pre-signed order for round(price) and submit —
                never sign on the hot path
at endMs−25s:   flatten unpaired (§6.5)
```

Live caveat (accept it, do not "fix" it): the zigzag emits the swing with the
extreme's timestamp, but detection happens at confirmation ($10 later). The
replay fills at `extreme_t + 70ms`, which is optimistic vs live. This is the
same optimism the +$4.2k figure was measured with; expected live slippage is
the difference between ask at extreme+70ms and ask at confirmation+70ms.

## 8. Parameters (single table, no others exist)

| Param | Value | Meaning |
|---|---|---|
| GRID_MS | 250 | zigzag sampling grid |
| TICK_MS | 500 | early-opposite scan grid |
| TAKER_DELAY_MS | 70 | signal → fill latency |
| SHARES | 10 | per leg |
| FEE_RATE | 0.07 | taker fee coefficient |
| SELL_HAIRCUT | 0.01 | sell inside displayed ask |
| minSwingUsd | 40 | major swing distance |
| confirmUsd | 10 | zigzag confirmation |
| minPeakDelta | 40 | standalone peak floor |
| maxTroughDelta | 52 | standalone trough ceiling |
| maxCheapAsk | 0.78 | first-leg ask ceiling |
| maxSecondAsk | 0.76 | second-leg ask ceiling |
| maxEarlyOppAsk | 0.88 | early-opp ask ceiling |
| minAsk | 0.04 | any-fill floor |
| minFirstAsk | 0.12 | first-leg ask floor |
| minUpFirstDelta | −75 | UP-first delta floor |
| rapidFallUsd / rapidFallMs | 55 / 12000 | flash-dump guard |
| earlyOppSwingUsd | 30 | dump size for early UP |
| minLeftMs | 10000 | first-leg time floor |
| restartMinLeftMs | 45000 | post-pair restart floor |
| pairCooldownMs | 8000 | after a pair |
| firstCooldownMs | 400 | after an unpaired buy |
| maxPairs | 16 | per window |
| delayBufferPerLeg | 0.005 | pair-gate pessimism |
| minRawPairNet | 0 | pair-gate threshold |
| emergencyLeftMs | 25000 | flatten time |

## 9. Acceptance tests (all must pass before going live)

1. **Oracle parity**: port produces byte-identical trade lists
   (kind, side, t, fill) to `runWindow()` on ≥ 7 days of DB windows.
   Reference harness: `scripts/parity-venue-final.ts` (expect
   `mismatches=0`).
2. **PnL reproduction**: 7d total within $0.01 of the JS oracle.
3. **No-lookahead audit**: every decision at time t reads only data with
   timestamp ≤ t. (The oracle is causal by construction; a port must be too.)
4. **Cold-venue degradation**: with Coinbase feed removed, engine still runs
   on Binance alone (and vice versa) — replays `mergeMode` single-venue
   behavior, no crash.
5. **Presign coverage**: every submitted order's key exists in the presign
   plan; the hot path performs zero signing.

## 10. What was tried and rejected (do not re-add)

| Variant | 7d @ 70ms-era result | Verdict |
|---|---|---|
| Δ vs Chainlink PTB (old default) | +$2,937 | lags venues |
| Mean-blend both venues into one tape | +$3,075 (@250ms) | worse than union |
| Require both venues to agree (±3s) | +$921 (@250ms) | far too strict |
| Binance-open only | +$2,158 (@250ms) | CB info lost |
| Coinbase-open only | +$2,707 (@250ms) | BN info lost |
| Mid-window trail/dump exits | negative vs hold | hurts winners more |
| Instant both-sides lock when up+down < $1 | rejected earlier | fee bleed |
