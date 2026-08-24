use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

pub const WINDOW_SEC: i64 = 300;
pub const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";

#[derive(Clone, Debug)]
pub struct MarketInfo {
    pub slug: String,
    pub title: String,
    pub condition_id: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub up_token_id: String,
    pub down_token_id: String,
}

pub fn window_start_ts(now_ms: i64) -> i64 {
    let now_sec = now_ms / 1000;
    (now_sec / WINDOW_SEC) * WINDOW_SEC
}

pub fn btc5m_slug(start_ts: i64) -> String {
    format!("btc-updown-5m-{}", start_ts)
}

#[derive(Clone, Debug, Deserialize)]
struct GammaMarket {
    question: Option<String>,
    #[serde(alias = "conditionId")]
    condition_id: Option<String>,
    clobTokenIds: Option<serde_json::Value>,
    outcomes: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    title: Option<String>,
    slug: Option<String>,
    markets: Option<Vec<GammaMarket>>,
}

fn parse_json_array(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        serde_json::Value::String(s) => serde_json::from_str(s).unwrap_or_default(),
        _ => vec![],
    }
}

fn pick_up_down(outcomes: &[String], token_ids: &[String]) -> Result<(String, String)> {
    if token_ids.len() < 2 {
        return Err(anyhow!("expected 2 token ids, got {}", token_ids.len()));
    }
    let labels: Vec<String> = outcomes.iter().map(|o| o.to_lowercase()).collect();
    let mut up_idx = labels.iter().position(|l| l == "up" || l == "yes");
    let mut down_idx = labels.iter().position(|l| l == "down" || l == "no");
    if up_idx.is_none() {
        up_idx = Some(0);
    }
    if down_idx.is_none() {
        down_idx = Some(if up_idx == Some(0) { 1 } else { 0 });
    }
    Ok((
        token_ids[up_idx.unwrap()].clone(),
        token_ids[down_idx.unwrap()].clone(),
    ))
}

pub async fn resolve_btc5m_market(start_ts: i64) -> Result<MarketInfo> {
    let slug = btc5m_slug(start_ts);
    let client = reqwest::Client::new();
    let url = format!("{}/events?slug={}", GAMMA_BASE, slug);
    let res = client
        .get(&url)
        .header("user-agent", "polybot/0.1")
        .send()
        .await
        .context("gamma events fetch")?;

    let body: serde_json::Value = res.json().await.context("gamma json")?;
    let event: Option<GammaEvent> = if body.is_array() {
        body.as_array()
            .and_then(|a| a.first())
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    } else {
        serde_json::from_value(body).ok()
    };

    let market = if let Some(ev) = event {
        ev.markets.and_then(|m| m.first().cloned())
    } else {
        None
    };

    let market = match market {
        Some(m) => m,
        None => {
            let url = format!("{}/markets/slug/{}", GAMMA_BASE, slug);
            let res = client
                .get(&url)
                .header("user-agent", "polybot/0.1")
                .send()
                .await?;
            if !res.status().is_success() {
                return Err(anyhow!("gamma market not found for {}", slug));
            }
            res.json().await.context("gamma market json")?
        }
    };

    let token_ids = parse_json_array(
        market
            .clobTokenIds
            .as_ref()
            .unwrap_or(&serde_json::Value::Null),
    );
    let outcomes = parse_json_array(
        market
            .outcomes
            .as_ref()
            .unwrap_or(&serde_json::Value::Null),
    );
    let (up_token_id, down_token_id) = pick_up_down(&outcomes, &token_ids)?;
    let condition_id = market.condition_id.unwrap_or_default();

    Ok(MarketInfo {
        slug: slug.clone(),
        title: market.question.unwrap_or(slug),
        condition_id,
        start_ts,
        end_ts: start_ts + WINDOW_SEC,
        up_token_id,
        down_token_id,
    })
}
