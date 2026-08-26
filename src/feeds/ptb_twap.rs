//! Polymarket RTDS 60s Chainlink TWAP — official BTC 5m Up/Down current price
//! (`cryptoMarketConfig.id = btc-5m-twap-60`). Ported from strategy_gapswing / jet_live
//! `feed/ptb_twap.rs`. Spot `crypto_prices_chainlink` and 30s TWAP diverge from the UI PTB.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use super::binance::BtcQuote;

const DEFAULT_RTDS_WS: &str = "wss://ws-live-data.polymarket.com";
const TOPIC: &str = "crypto_prices_twap_sixty";
/// Window logs write `ptb_gap` at 1Hz. Ingesting every RTDS update inflates
/// realised vol vs the series the strategy was calibrated on.
const MIN_TICK_MS: i64 = 1000;
static LAST_TICK_MS: AtomicI64 = AtomicI64::new(0);

pub fn spawn_ptb_twap() -> mpsc::UnboundedReceiver<BtcQuote> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            if let Err(e) = session(&tx).await {
                error!(error = %e, "ptb twap rtds");
            }
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, Duration::from_secs(30));
        }
    });
    rx
}

fn rtds_url() -> String {
    std::env::var("POLY_RTDS_WS").unwrap_or_else(|_| DEFAULT_RTDS_WS.to_string())
}

async fn session(tx: &mpsc::UnboundedSender<BtcQuote>) -> Result<()> {
    let url = rtds_url();
    let (ws, _) = connect_async(&url).await?;
    let (mut write, mut read) = ws.split();
    let sub = serde_json::json!({
        "action": "subscribe",
        "subscriptions": [{ "topic": TOPIC, "type": "*", "filters": "" }]
    });
    write.send(Message::Text(sub.to_string().into())).await?;
    info!(topic = TOPIC, "ptb twap RTDS connected");
    loop {
        let Some(msg) = read.next().await else { break };
        let Ok(msg) = msg else { continue };
        match msg {
            Message::Text(t) => parse(&t, tx),
            Message::Ping(p) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    warn!("ptb twap RTDS disconnected");
    Ok(())
}

fn parse(text: &str, tx: &mpsc::UnboundedSender<BtcQuote>) {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    if v.get("topic").and_then(|x| x.as_str()) != Some(TOPIC) {
        return;
    }
    if v.get("type").and_then(|x| x.as_str()) != Some("update") {
        return;
    }
    let payload = match v.get("payload") {
        Some(p) => p,
        None => return,
    };
    let sym = payload
        .get("symbol")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if sym != "btc/usd" {
        return;
    }
    let Some(px) = parse_px(payload) else { return };
    if !(px > 0.0) {
        return;
    }
    // Wall clock: oracle payload timestamps are lagged/quantized and drop as t < open.
    let t_ms = now_ms();
    let prev = LAST_TICK_MS.load(Ordering::Relaxed);
    if prev > 0 && t_ms - prev < MIN_TICK_MS {
        return;
    }
    LAST_TICK_MS.store(t_ms, Ordering::Relaxed);
    let _ = tx.send(BtcQuote { t: t_ms, price: px });
}

fn parse_px(payload: &serde_json::Value) -> Option<f64> {
    if let Some(raw) = payload.get("full_accuracy_value") {
        if let Some(v) = parse_e18(raw) {
            return Some(v);
        }
    }
    payload
        .get("value")
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
}

fn parse_e18(raw: &serde_json::Value) -> Option<f64> {
    let s = if let Some(n) = raw.as_str() {
        n.trim().to_string()
    } else if let Some(n) = raw.as_u64() {
        n.to_string()
    } else if let Some(n) = raw.as_i64() {
        n.to_string()
    } else if let Some(n) = raw.as_f64() {
        if n.abs() > 1e10 {
            return Some(n / 1e18);
        }
        return Some(n);
    } else {
        return None;
    };
    if s.is_empty() {
        return None;
    }
    if s.chars().all(|c| c.is_ascii_digit())
        || (s.starts_with('-') && s[1..].chars().all(|c| c.is_ascii_digit()))
    {
        return s.parse::<f64>().ok().map(|v| v / 1e18);
    }
    let v: f64 = s.parse().ok()?;
    if v.abs() > 1e10 {
        Some(v / 1e18)
    } else {
        Some(v)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn e18_integer_string() {
        let p = serde_json::json!({
            "full_accuracy_value": "68000000000000000000000",
            "value": 68000.0
        });
        let px = parse_px(&p).unwrap();
        assert!((px - 68000.0).abs() < 1e-6);
    }

    #[test]
    fn falls_back_to_value() {
        let p = serde_json::json!({ "value": "73727.594" });
        let px = parse_px(&p).unwrap();
        assert!((px - 73727.594).abs() < 1e-6);
    }
}
