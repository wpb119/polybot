use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

const BINANCE_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";

#[derive(Clone, Debug)]
pub struct BtcQuote {
    pub t: i64,
    pub price: f64,
}

pub fn spawn_binance() -> watch::Receiver<Option<BtcQuote>> {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            if let Err(e) = run(&tx).await {
                error!("binance ws: {:#}", e);
            }
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, Duration::from_secs(30));
        }
    });
    rx
}

async fn run(tx: &watch::Sender<Option<BtcQuote>>) -> Result<()> {
    let (ws, _) = connect_async(BINANCE_WS).await?;
    info!("binance connected");
    let (mut write, mut read) = ws.split();
    backoff_reset();

    while let Some(msg) = read.next().await {
        let msg = msg?;
        if !msg.is_text() {
            continue;
        }
        let text = msg.to_text()?;
        let v: serde_json::Value = serde_json::from_str(text)?;
        let bid: Option<f64> = v.get("b").and_then(|x| x.as_str()).and_then(|s| s.parse().ok());
        let ask: Option<f64> = v.get("a").and_then(|x| x.as_str()).and_then(|s| s.parse().ok());
        if bid == None || ask == None {
            continue;
        }
        let t = v
            .get("E")
            .and_then(|x| x.as_u64())
            .map(|n| n as i64)
            .unwrap_or_else(|| chrono_now_ms());
        let price = (bid.unwrap() + ask.unwrap()) / 2.0;
        tx.send_replace(Some(BtcQuote { t, price }));
    }

    warn!("binance disconnected");
    Ok(())
}

fn backoff_reset() {}

fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
