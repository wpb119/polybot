# polybot

Live Polymarket bot for BTC 5m Up/Down.

## Strategies

Set `STRATEGY` in `.env`:

| Value | Description |
|-------|-------------|
| **`gap_swing`** (default) | Major-swing gap capture from poly-history (`strategy-gap-swing.js`). Peak→buy DOWN, trough→buy UP, early opposite pair. Backtest ~+$1.9k / 7d @ 10sh. |
| `pairing` | Legacy impulse START + 4¢ pullback + pair/dead exit (poly-prices pairing). |

### Feeds (both)

- **Binance** `btcusdt@bookTicker` + **Coinbase** `BTC-USD` ticker
- **Polymarket CLOB** WebSocket for Up/Down asks
- **Entry / pair:** GTC limit buys; dry-run fills at send+250ms

### Gap swing specifics

- Zigzag on BTC Δ from PTB (confirm $10, min swing $40)
- First leg ask ≤ 60¢; second ≤ 76¢ (early opposite ≤ 88¢)
- Skip deep UP dumps (Δ < −$75 unless ask ≤ 20¢)
- Emergency pair attempt in last 25s if still unpaired
- Portable JS reference: `../poly-history/new_strategy/strategy-gap-swing.js`

## Setup

```bash
cd polybot
cp .env.example .env
# Edit .env — STRATEGY=gap_swing, default is dry-run
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
STRATEGY=gap_swing
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_FUNDER=0x...          # proxy wallet if using browser/Magic login
POLYMARKET_SIGNATURE_TYPE=2      # 2 = GNOSIS_SAFE / browser proxy
ORDER_SHARES=10
```

Live orders are **GTC limit buys** with exact share count at a max price. Orders are **presigned** when each window is subscribed (~5s before open).

## Project layout

```
src/
  feeds/          Binance, Coinbase, Polymarket WebSockets
  strategy/
    gap_swing.rs  Major-swing gap capture (default)
    pairing.rs    Legacy pairing state machine
    detector.rs   Impulse START (pairing only)
  gamma.rs        Resolve btc-updown-5m-{ts} market + token IDs
  clob.rs         Order submission (SDK v2)
  bot.rs          Main loop
```

## Confirm dry-run / backtest PnL (~+$1.9k / 7d)

Dry-run economics use the same batch simulator as `poly-history/new_strategy/strategy-gap-swing.js`
(`polybot::strategy::run_gap_swing_window`). Verified:

```
JS 7d:          total_pnl=1925.13  pairs=1378  windows=1789
Rust dry_replay: total_pnl=1925.13  pairs=1378  windows=1789
```

Replay yourself:

```bash
# from poly-history: dump last 7d windows to JSONL (needs DB)
npx tsx -e '...write /tmp/gap-windows.jsonl...'   # see scripts or ask agent

cd ../polybot
cargo run --release --bin dry_replay -- /tmp/gap-windows.jsonl
# expect: DRY_REPLAY ... total_pnl≈1925
```

Live dry-run (`cargo run --release`, `LIVE_TRADING=false`) feeds BN/ask ticks into that
same simulator and fills at **signal_t + 250ms** from the ask book.
