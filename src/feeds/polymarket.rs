use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::gamma::MarketInfo;

const POLY_WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Clone, Debug, Default)]
pub struct PolyQuote {
    pub t: i64,
    pub start_ts: i64,
    pub up_ask: f64,
    pub down_ask: f64,
}

struct TokenMeta {
    start_ts: i64,
    is_up: bool,
}

/// Unbounded ask stream — bot must drain every update (watch-latest drops fill-time asks).
pub fn spawn_polymarket(
    markets_rx: watch::Receiver<Vec<MarketInfo>>,
) -> mpsc::UnboundedReceiver<PolyQuote> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            let markets = markets_rx.borrow().clone();
            if markets.is_empty() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            match run_session(&tx, &markets, &markets_rx).await {
                Ok(()) => {
                    // Intentional resubscribe or clean close — reconnect immediately.
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    error!("polymarket ws: {:#}", e);
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(30));
                }
            }
        }
    });
    rx
}

async fn run_session(
    tx: &mpsc::UnboundedSender<PolyQuote>,
    initial: &[MarketInfo],
    markets_rx: &watch::Receiver<Vec<MarketInfo>>,
) -> Result<()> {
    let (ws, _) = connect_async(POLY_WS).await?;
    let (mut write, mut read) = ws.split();

    let mut markets_rx = markets_rx.clone();
    // Mark current value seen so the first changed() is a real update.
    let _ = markets_rx.borrow_and_update();
    let mut token_map: HashMap<String, TokenMeta> = HashMap::new();
    let mut books: HashMap<i64, MarketBookState> = HashMap::new();
    let mut active_key = String::new();

    apply_subscription(
        initial,
        &mut write,
        &mut token_map,
        &mut books,
        &mut active_key,
    )
    .await?;
    info!("polymarket ws connected");

    loop {
        tokio::select! {
            changed = markets_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                let next = markets_rx.borrow_and_update().clone();
                if next.is_empty() {
                    warn!("polymarket ws: empty market list — ending session");
                    return Ok(());
                }
                let next_key = subscription_key(&next);
                if next_key == active_key {
                    continue;
                }
                // Hot-update on same socket — never tear down (early-buy blackout fix).
                if let Err(e) = apply_subscription(
                    &next,
                    &mut write,
                    &mut token_map,
                    &mut books,
                    &mut active_key,
                )
                .await
                {
                    warn!("polymarket hot-resub failed (keeping socket): {:#}", e);
                }
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    warn!("polymarket ws session ended (stream closed)");
                    return Ok(());
                };
                let msg = msg?;
                if !msg.is_text() {
                    continue;
                }
                let text = msg.to_text()?.trim();
                if text == "PONG" || text.is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                    apply_msg(&v, &token_map, &mut books);
                    let t = now_ms();
                    for (start_ts, state) in &books {
                        if let (Some(u), Some(d)) = (state.up_ask, state.down_ask) {
                            if tx
                                .send(PolyQuote {
                                    t,
                                    start_ts: *start_ts,
                                    up_ask: u,
                                    down_ask: d,
                                })
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn apply_subscription<W>(
    markets: &[MarketInfo],
    write: &mut W,
    token_map: &mut HashMap<String, TokenMeta>,
    books: &mut HashMap<i64, MarketBookState>,
    active_key: &mut String,
) -> Result<()>
where
    W: SinkExt<Message> + Unpin,
    W::Error: std::fmt::Debug,
{
    let slugs: Vec<&str> = markets.iter().map(|m| m.slug.as_str()).collect();
    info!("polymarket subscribe → {:?}", slugs);

    token_map.clear();
    let mut asset_ids: Vec<String> = vec![];
    let keep: std::collections::HashSet<i64> = markets.iter().map(|m| m.start_ts).collect();
    books.retain(|k, _| keep.contains(k));

    for m in markets {
        token_map.insert(
            m.up_token_id.clone(),
            TokenMeta {
                start_ts: m.start_ts,
                is_up: true,
            },
        );
        token_map.insert(
            m.down_token_id.clone(),
            TokenMeta {
                start_ts: m.start_ts,
                is_up: false,
            },
        );
        asset_ids.push(m.up_token_id.clone());
        asset_ids.push(m.down_token_id.clone());
        books.entry(m.start_ts).or_insert_with(MarketBookState::new);
    }

    let sub = serde_json::json!({
        "assets_ids": asset_ids,
        "type": "market",
        "initial_dump": true,
        "level": 2,
        "custom_feature_enabled": true
    });
    write
        .send(Message::Text(sub.to_string().into()))
        .await
        .map_err(|e| anyhow::anyhow!("polymarket subscribe send: {e:?}"))?;
    *active_key = subscription_key(markets);
    Ok(())
}

fn subscription_key(markets: &[MarketInfo]) -> String {
    markets
        .iter()
        .map(|m| m.start_ts.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

struct MarketBookState {
    up_ask: Option<f64>,
    down_ask: Option<f64>,
}

impl MarketBookState {
    fn new() -> Self {
        Self {
            up_ask: None,
            down_ask: None,
        }
    }
}

fn apply_msg(
    v: &serde_json::Value,
    token_map: &HashMap<String, TokenMeta>,
    books: &mut HashMap<i64, MarketBookState>,
) {
    let event_type = v.get("event_type").and_then(|x| x.as_str()).unwrap_or("");
    match event_type {
        "book" => {
            let asset = v.get("asset_id").and_then(|x| x.as_str()).unwrap_or("");
            let meta = token_map.get(asset);
            if meta.is_none() {
                return;
            }
            let meta = meta.unwrap();
            let state = books.get_mut(&meta.start_ts).expect("book state");
            if let Some(asks) = v.get("asks").and_then(|x| x.as_array()) {
                let best = best_price(asks, true);
                if meta.is_up {
                    state.up_ask = best;
                } else {
                    state.down_ask = best;
                }
            }
        }
        "best_bid_ask" => {
            let asset = v.get("asset_id").and_then(|x| x.as_str()).unwrap_or("");
            let meta = token_map.get(asset);
            if meta.is_none() {
                return;
            }
            let meta = meta.unwrap();
            let state = books.get_mut(&meta.start_ts).expect("book state");
            let ask = v.get("best_ask").and_then(parse_f64);
            if meta.is_up {
                if ask.is_some() {
                    state.up_ask = ask;
                }
            } else if ask.is_some() {
                state.down_ask = ask;
            }
        }
        "price_change" => {
            if let Some(changes) = v.get("price_changes").and_then(|x| x.as_array()) {
                for ch in changes {
                    let asset = ch.get("asset_id").and_then(|x| x.as_str()).unwrap_or("");
                    let meta = token_map.get(asset);
                    if meta.is_none() {
                        continue;
                    }
                    let meta = meta.unwrap();
                    let state = books.get_mut(&meta.start_ts).expect("book state");
                    let ask = ch.get("best_ask").and_then(parse_f64);
                    if meta.is_up {
                        if ask.is_some() {
                            state.up_ask = ask;
                        }
                    } else if ask.is_some() {
                        state.down_ask = ask;
                    }
                }
            }
        }
        _ => {}
    }
}

fn best_price(levels: &[serde_json::Value], is_ask: bool) -> Option<f64> {
    let prices: Vec<f64> = levels
        .iter()
        .filter_map(|l| l.get("price").and_then(parse_f64))
        .collect();
    if prices.is_empty() {
        return None;
    }
    if is_ask {
        prices.into_iter().min_by(|a, b| a.partial_cmp(b).unwrap())
    } else {
        prices.into_iter().max_by(|a, b| a.partial_cmp(b).unwrap())
    }
}

fn parse_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
