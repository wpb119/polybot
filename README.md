# polybot

Live Polymarket bot for BTC 5m Up/Down.

## Strategies

Set `STRATEGY` in `.env`:

| Value | Description |
|-------|-------------|
| **`venue_swing`** (default) | Best verified engine, ported from poly-history `strategy-venue-swing-final.js` (spec: `VENUE_SWING_AGENT_GUIDE.md`). Two independent zigzags — Binance−Binance-open and Coinbase−Coinbase-open — union-merged. Peak→buy DOWN, trough→buy UP, pair when net ≥ 0, flatten T−25s. Winner is still official PTB. Backtest **~+$4.2k / 7d @ 10sh** (fill t+70ms). |
| `gap_swing` | Previous engine: Δ = Binance − official Chainlink PTB (`strategy-gap-swing.js`). ~+$2.9k / 7d @ 10sh at t+70ms. |
| `pairing` | Legacy impulse START + 4¢ pullback + pair/dead exit. |

### Feeds

- **Binance** `btcusdt@bookTicker` + **Coinbase** `BTC-USD` ticker
  (venue_swing uses BOTH as first-class tapes, each vs its own window-open)
- **Polymarket CLOB** WebSocket for Up/Down asks
- **Chainlink PTB** via RTDS `crypto_prices_twap_sixty` (+ REST fallback) —
  venue_swing uses it ONLY to resolve the settlement winner, never as signal
- **Entry / pair:** GTC limit buys; dry-run fills at send+70ms (venue) / +250ms (gap)

### Venue swing specifics

- Venue open = last print at/before window open (else first after); per venue
- Zigzag per venue on Δ = venue − venue-open (confirm $10, min swing $40),
  union-merge majors (keep the more extreme of consecutive same-kind)
- First leg ask 12–78¢; second ≤ 76¢; early opposite ≤ 88¢ after a $30 dump
- Skip deep UP dumps (Δ < −$75 unless ask ≤ 20¢; flash-dump $55/12s guard)
- After a pair wait 8s; new first leg needs ≥ 45s left
- Flatten unpaired at T−25s at ask−1¢; residue settles at PTB winner
- No PTB wait: trading can start the moment the window opens
- Portable JS oracle: `../poly-history/new_strategy/strategy-venue-swing-final.js`

## Setup

```bash
cd polybot
cp .env.example .env
# Edit .env — STRATEGY=venue_swing, default is dry-run
cargo build --release
```

## Run (dry-run)

```bash
cargo run --release
```

Logs `[DRY] BUY …` when signals fire. No wallet required.

## Live trading

```bash
LIVE_TRADING=true
STRATEGY=venue_swing
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_FUNDER=0x...          # proxy wallet if using browser/Magic login
POLYMARKET_SIGNATURE_TYPE=2      # 2 = GNOSIS_SAFE / browser proxy
ORDER_SHARES=10
```

Live orders are **GTC limit orders** with exact share count: buys at the 0.99 max limit and sells at the 0.01 floor, so they cross the book like market orders. Orders are **presigned** when each window is subscribed (before open). Live fill delay is **0** — the signal hot path is a single HTTP POST of the presigned order; fill resolution, remainder cancel and presign replenish all run in a background task so they never delay the next signal. The backtest's +70ms is the modeled taker latency.

## Project layout

```
src/
  feeds/            Binance, Coinbase, Polymarket, Chainlink RTDS WebSockets
  strategy/
    venue_swing.rs  VENUE SWING (default) — port of strategy-venue-swing-final.js
    gap_swing.rs    PTB gap-swing (previous engine)
    pairing.rs      Legacy pairing state machine
    detector.rs     Impulse START (pairing only)
  gamma.rs          Resolve btc-updown-5m-{ts} market + token IDs
  clob.rs           Order submission (SDK v2)
  bot.rs            Main loop
```

## Confirm Rust ↔ JS oracle parity (venue swing)

Dry-run economics use the same batch oracle as
`poly-history/new_strategy/strategy-venue-swing-final.js`
(`polybot::strategy::run_venue_swing_window`). Verify any time:

```bash
# from poly-history: dump last 7d windows + JS oracle PnL per window (needs DB)
npx tsx scripts/export-venue-windows.ts /tmp/venue-windows.jsonl

cd ../polybot
cargo run --release --bin venue_replay -- /tmp/venue-windows.jsonl
# expect: VENUE_REPLAY ... mismatches=0 and total_pnl == expected_js
```

Live dry-run (`cargo run --release`, `LIVE_TRADING=false`) feeds BN/CB/ask ticks
into that same oracle (with committed fills) and fills at **signal_t + 70ms**
from the ask book.
