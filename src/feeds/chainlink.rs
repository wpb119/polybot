use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use super::binance::BtcQuote;

const RTDS_WS: &str = "wss://ws-live-data.polymarket.com";
const SYMBOL: &str = "btc/usd";

/// Polymarket RTDS BTC oracle.
/// Prefer `crypto_prices_twap_thirty` (resolution-family TWAP) over raw Chainlink mid —
/// the event page / crypto-price PTB is TWAP-based, not mid.
pub fn spawn_chainlink() -> mpsc::UnboundedReceiver<BtcQuote> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            if let Err(e) = run(&tx).await {
                error!("chainlink rtds: {:#}", e);
            }
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, Duration::from_secs(30));
        }
    });
    rx
}

async fn run(tx: &mpsc::UnboundedSender<BtcQuote>) -> Result<()> {
    let (ws, _) = connect_async(RTDS_WS).await?;
    info!("chainlink rtds connected (twap_thirty preferred)");
    let (mut write, mut read) = ws.split();

    let sub = serde_json::json!({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_twap_thirty",
                "type": "update",
                "filters": serde_json::to_string(&serde_json::json!({ "symbol": SYMBOL })).unwrap(),
            },
            {
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": serde_json::to_string(&serde_json::json!({ "symbol": SYMBOL })).unwrap(),
            }
        ]
    });
    write.send(Message::Text(sub.to_string().into())).await?;

    let mut ping = tokio::time::interval(Duration::from_secs(5));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_twap_at: i64 = 0;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                if write.send(Message::Text("PING".into())).await.is_err() {
                    break;
                }
            }
            msg = read.next() => {
                let Some(msg) = msg else { break; };
                let msg = msg?;
                if !msg.is_text() {
                    continue;
                }
                let text = msg.to_text()?.trim();
                if text.is_empty() || text == "PONG" {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                    handle_msg(&v, tx, &mut last_twap_at);
                }
            }
        }
    }

    warn!("chainlink rtds disconnected");
    Ok(())
}

fn handle_msg(v: &serde_json::Value, tx: &mpsc::UnboundedSender<BtcQuote>, last_twap_at: &mut i64) {
    if let Some(arr) = v.as_array() {
        for item in arr {
            handle_msg(item, tx, last_twap_at);
        }
        return;
    }
    let topic = v.get("topic").and_then(|x| x.as_str()).unwrap_or("");
    let is_twap = topic == "crypto_prices_twap_thirty";
    let is_mid =
        topic == "crypto_prices_chainlink" || topic == "prices.crypto.chainlink";
    if !is_twap && !is_mid {
        return;
    }
    let payload = match v.get("payload") {
        Some(p) => p,
        None => return,
    };
    let symbol = payload
        .get("symbol")
        .and_then(|x| x.as_str())
        .unwrap_or(SYMBOL)
        .to_ascii_lowercase();
    if symbol != SYMBOL {
        return;
    }
    let price = payload
        .get("value")
        .and_then(|x| {
            x.as_f64()
                .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0.0);
    if !(price > 0.0) {
        return;
    }
    let t = payload
        .get("timestamp")
        .and_then(|x| x.as_u64().or_else(|| x.as_i64().map(|n| n as u64)))
        .map(|n| n as i64)
        .filter(|n| *n > 1_000_000_000_000)
        .unwrap_or_else(now_ms);

    if is_twap {
        *last_twap_at = t;
    } else if *last_twap_at > 0 && t - *last_twap_at < 15_000 {
        // Same rule as poly-live: ignore mid while TWAP is fresh.
        return;
    }

    let _ = tx.send(BtcQuote { t, price });
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
