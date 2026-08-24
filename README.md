# polybot

Live Polymarket bot for the **Pairing** strategy (from `poly-prices`).

- Subscribes to **Binance** (`btcusdt@bookTicker`) and **Coinbase** (`BTC-USD` ticker) BTC prices
- Subscribes to **Polymarket CLOB** WebSocket for Up/Down asks
- Detects BTC impulse START (same thresholds as poly-prices `trendCapture`)
- Applies Pairing entry filters (gap×time, token trend, book lag, max ask)
- **Entry:** buy UP/DOWN with 4¢ pullback or chase; submit to CLOB immediately (250ms taker delay is on CLOB fill, not bot-side wait)
- **Exit:** pair (buy other leg at ~`1 − held + 1¢`), dead flatten, underwater near expiry
- **Orders:** GTC **limit** buys at max price (`size = ORDER_SHARES`); presigned at window start for fast post

## Setup

```bash
cd polybot
cp .env.example .env
# Edit .env — default is dry-run (no orders)
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
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_FUNDER=0x...          # proxy wallet if using browser/Magic login
POLYMARKET_SIGNATURE_TYPE=2      # 2 = GNOSIS_SAFE / browser proxy
MAX_ORDER_USD=50
ORDER_SHARES=10
```

Live orders are **GTC limit buys** with exact share count at a max price (not FAK market buys). Orders are **presigned** when each window is subscribed (~5s before open) so the hot path is only `post_order`. After each fill, the bot presigns a replacement for that leg.

## Strategy docs

See `../poly-prices/strategy/docs/pairing.md` for full strategy description.

## Project layout

```
src/
  feeds/       Binance, Coinbase, Polymarket WebSockets
  strategy/    Trend detector + pairing state machine
  gamma.rs     Resolve btc-updown-5m-{ts} market + token IDs
  clob.rs      Order submission (SDK v2)
  bot.rs       Main loop
```

## Compare with backtest

Backtest PnL: `cd ../poly-prices && npx tsx scripts/compare-strategies.ts`
