use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Same as poly-history DB ingest: combined bookTicker + aggTrade.
const BINANCE_WS: &str =
    "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade";
const BINANCE_REST: &str = "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT";
const REST_POLL_MS: u64 = 2_000;

#[derive(Clone, Debug)]
pub struct BtcQuote {
    pub t: i64,
    pub price: f64,
}

/// Unbounded tick stream — bot must drain every quote (watch-latest drops zigzag extrema).
///
/// Sources (public, no API key — matches poly-history DB / Raw tape):
/// 1. WS `btcusdt@bookTicker` → mid (bid+ask)/2  ← **only this** feeds Raw zigzag
/// 2. WS `btcusdt@aggTrade` → subscribed (DB trade rows) but **not** pushed to strategy
/// 3. REST bookTicker every 2s → catch a mid the socket missed
pub fn spawn_binance() -> mpsc::UnboundedReceiver<BtcQuote> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            if let Err(e) = run_ws_with_rest_backup(&tx).await {
                error!("binance feed: {:#}", e);
            }
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, Duration::from_secs(30));
        }
    });
    rx
}

async fn run_ws_with_rest_backup(tx: &mpsc::UnboundedSender<BtcQuote>) -> Result<()> {
    let (ws, _) = tokio_tungstenite::connect_async(BINANCE_WS).await?;
    info!("binance connected (bookTicker mid→strategy; aggTrade subscribed; REST backup 2s)");
    let (_write, mut read) = ws.split();

    let mut last_mid: Option<(i64, f64)> = None;
    let mut last_ws_ms = now_ms();
    let mut rest = tokio::time::interval(Duration::from_millis(REST_POLL_MS));
    rest.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip immediate first tick — wait 2s after connect.
    rest.tick().await;

    let client = reqwest::Client::new();

    loop {
        tokio::select! {
            msg = read.next() => {
                let Some(msg) = msg else {
                    warn!("binance ws stream closed");
                    break;
                };
                let msg = msg?;
                if !msg.is_text() {
                    continue;
                }
                let text = msg.to_text()?;
                // Keep last_ws_ms alive on any stream traffic so REST stays backup-only.
                if text.contains("bookTicker") || text.contains("aggTrade") {
                    last_ws_ms = now_ms();
                }
                if let Some(q) = parse_book_ticker_mid(text) {
                    if q.price > 0.0 {
                        last_mid = Some((q.t, q.price));
                        if tx.send(q).is_err() {
                            return Ok(());
                        }
                    }
                }
            }
            _ = rest.tick() => {
                // Backup only when the socket has been quiet — catch a missed mid.
                let quiet = now_ms().saturating_sub(last_ws_ms) >= REST_POLL_MS as i64;
                if !quiet {
                    continue;
                }
                match fetch_rest_mid(&client).await {
                    Ok(Some(q)) => {
                        let same = last_mid
                            .map(|(_, p)| (p - q.price).abs() < 0.01)
                            .unwrap_or(false);
                        if same {
                            continue;
                        }
                        last_mid = Some((q.t, q.price));
                        if tx.send(q).is_err() {
                            return Ok(());
                        }
                    }
                    Ok(None) => {}
                    Err(e) => warn!("binance REST bookTicker: {:#}", e),
                }
            }
        }
    }

    warn!("binance disconnected");
    Ok(())
}

/// Raw / hist `ticks.btc` = bookTicker mid only (never aggTrade prints).
fn parse_book_ticker_mid(text: &str) -> Option<BtcQuote> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    let (stream, data) = if v.get("data").is_some() {
        (
            v.get("stream").and_then(|s| s.as_str()).unwrap_or(""),
            v.get("data")?,
        )
    } else {
        ("", &v)
    };

    // Skip aggTrade (and anything without bid/ask).
    if stream.contains("aggTrade") {
        return None;
    }
    if !(stream.contains("bookTicker") || (data.get("b").is_some() && data.get("a").is_some())) {
        return None;
    }

    let bid: f64 = data.get("b")?.as_str()?.parse().ok()?;
    let ask: f64 = data.get("a")?.as_str()?.parse().ok()?;
    if !(bid > 0.0 && ask > 0.0) {
        return None;
    }
    let t = data
        .get("E")
        .and_then(|x| x.as_i64())
        .unwrap_or_else(now_ms);
    Some(BtcQuote {
        t,
        price: (bid + ask) / 2.0,
    })
}

async fn fetch_rest_mid(client: &reqwest::Client) -> Result<Option<BtcQuote>> {
    let res = client
        .get(BINANCE_REST)
        .header("user-agent", "polybot/0.1")
        .send()
        .await?;
    if !res.status().is_success() {
        return Ok(None);
    }
    let v: serde_json::Value = res.json().await?;
    let bid: f64 = v
        .get("bidPrice")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let ask: f64 = v
        .get("askPrice")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    if !(bid > 0.0 && ask > 0.0) {
        return Ok(None);
    }
    Ok(Some(BtcQuote {
        t: now_ms(),
        price: (bid + ask) / 2.0,
    }))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
