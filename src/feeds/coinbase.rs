use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use super::binance::BtcQuote;

const COINBASE_WS: &str = "wss://ws-feed.exchange.coinbase.com";

pub fn spawn_coinbase() -> watch::Receiver<Option<BtcQuote>> {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            if let Err(e) = run(&tx).await {
                error!("coinbase ws: {:#}", e);
            }
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, Duration::from_secs(30));
        }
    });
    rx
}

async fn run(tx: &watch::Sender<Option<BtcQuote>>) -> Result<()> {
    let (ws, _) = connect_async(COINBASE_WS).await?;
    info!("coinbase connected");
    let (mut write, mut read) = ws.split();

    let sub = serde_json::json!({
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker"]
    });
    write.send(Message::Text(sub.to_string().into())).await?;

    while let Some(msg) = read.next().await {
        let msg = msg?;
        if !msg.is_text() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(msg.to_text()?)?;
        if v.get("type").and_then(|x| x.as_str()) != Some("ticker") {
            continue;
        }
        let bid: Option<f64> = v
            .get("best_bid")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok());
        let ask: Option<f64> = v
            .get("best_ask")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok());
        let price = match (bid, ask) {
            (Some(b), Some(a)) => (b + a) / 2.0,
            _ => v
                .get("price")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
        };
        if price <= 0.0 {
            continue;
        }
        let t = now_ms();
        tx.send_replace(Some(BtcQuote { t, price }));
    }

    warn!("coinbase disconnected");
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
