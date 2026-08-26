use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tokio_postgres::NoTls;

pub const WINDOW_SEC: i64 = 300;
pub const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";

#[derive(Clone, Debug)]
pub struct MarketInfo {
    pub slug: String,
    pub title: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub up_token_id: String,
    pub down_token_id: String,
    /// From gamma `cryptoMarketConfig` (BTC 5m UI: 60s TWAP).
    pub twap_enabled: bool,
    pub twap_lookback_sec: i64,
}

pub fn btc5m_slug(start_ts: i64) -> String {
    format!("btc-updown-5m-{}", start_ts)
}

#[derive(Clone, Debug, Deserialize)]
struct GammaMarket {
    question: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<serde_json::Value>,
    outcomes: Option<serde_json::Value>,
    #[serde(rename = "cryptoMarketConfig")]
    crypto_market_config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
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
            .clob_token_ids
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

    let (twap_enabled, twap_lookback_sec) = parse_twap_cfg(market.crypto_market_config.as_ref());
    Ok(MarketInfo {
        slug: slug.clone(),
        title: market.question.unwrap_or(slug),
        start_ts,
        end_ts: start_ts + WINDOW_SEC,
        up_token_id,
        down_token_id,
        twap_enabled,
        twap_lookback_sec,
    })
}

fn parse_twap_cfg(c: Option<&serde_json::Value>) -> (bool, i64) {
    let default_on = env_bool("PTB_TWAP_ENABLED", true);
    let default_lb = env_i64("PTB_TWAP_LOOKBACK_SEC", 60).max(1);
    let enabled = c
        .and_then(|x| x.get("twapEnabled"))
        .and_then(|x| x.as_bool())
        .unwrap_or(default_on);
    let lookback = c
        .and_then(|x| x.get("twapLookbackSeconds"))
        .and_then(|x| x.as_i64())
        .filter(|s| *s > 0)
        .unwrap_or(default_lb);
    (enabled, lookback)
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            v == "true" || v == "1"
        }
        Err(_) => default,
    }
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default)
}

const DEFAULT_PTB_URL: &str = "https://polymarket.com/api/crypto/crypto-price";

/// Official 5m BTC Up/Down PTB REST — `openPrice` with UI TWAP query flags.
/// ISO has **no millis**. Do not substitute Binance if missing.
pub async fn fetch_ptb(
    start_sec: i64,
    twap_enabled: bool,
    twap_lookback_sec: i64,
) -> Result<f64> {
    let iso = chrono_iso(start_sec);
    let end_iso = chrono_iso(start_sec + WINDOW_SEC);
    let base = std::env::var("PTB_URL").unwrap_or_else(|_| DEFAULT_PTB_URL.to_string());
    let asset = std::env::var("POLY_ASSET")
        .unwrap_or_else(|_| "btc".into())
        .to_ascii_uppercase();
    let mut url = format!(
        "{base}?symbol={asset}&eventStartTime={iso}&variant=fiveminute&endDate={end_iso}"
    );
    if twap_enabled {
        url.push_str(&format!(
            "&twapEnabled=true&twapLookbackSeconds={}",
            twap_lookback_sec.max(1)
        ));
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .context("ptb client")?;
    let v: serde_json::Value = client
        .get(&url)
        .header("Accept", "application/json")
        .header("User-Agent", "Mozilla/5.0 (compatible; jet-live/1.0)")
        .send()
        .await?
        .error_for_status()
        .context("ptb http")?
        .json()
        .await?;
    v.get("openPrice")
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
        .filter(|x| *x > 0.0)
        .context("ptb openPrice missing")
}

/// Wait until TWAP openPrice exists and stops drifting. Fallback path only — not RTDS.
pub async fn fetch_ptb_ready(
    start_sec: i64,
    twap_enabled: bool,
    twap_lookback_sec: i64,
) -> Result<f64> {
    let mut last_err = anyhow!("ptb openPrice missing");
    let mut last_px: Option<f64> = None;
    let mut same = 0u32;
    for i in 0..80 {
        match fetch_ptb(start_sec, twap_enabled, twap_lookback_sec).await {
            Ok(p) if p > 0.0 => {
                if last_px.is_some_and(|prev| (prev - p).abs() < 1e-4) {
                    same += 1;
                    if same >= 4 {
                        tracing::info!(
                            open = p,
                            twap_enabled,
                            twap_lookback_sec,
                            "ptb strike locked (stable)"
                        );
                        return Ok(p);
                    }
                } else {
                    if let Some(prev) = last_px {
                        tracing::info!(prev, now = p, "ptb openPrice still moving");
                    }
                    last_px = Some(p);
                    same = 1;
                }
            }
            Ok(_) => last_err = anyhow!("ptb openPrice not ready"),
            Err(e) => last_err = e,
        }
        if i == 0 || i % 8 == 0 {
            tracing::warn!(attempt = i + 1, error = %last_err, last_px, "ptb retry");
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    last_px.ok_or(last_err)
}

pub async fn fetch_official_ptb(start_ts: i64) -> Result<Option<f64>> {
    match fetch_ptb(start_ts, true, 60).await {
        Ok(p) => Ok(Some(p)),
        Err(e) => {
            let msg = format!("{e:#}");
            if msg.contains("429") {
                Err(e)
            } else {
                Ok(None)
            }
        }
    }
}

/// Replay helper: first `ptb_gap` row. Live gap-swing does **not** lock from this.
#[allow(dead_code)]
pub async fn fetch_ptb_gap_only(start_ts: i64) -> Option<f64> {
    match fetch_ptb_gap_db(start_ts).await {
        Ok(v) => v,
        Err(_) => None,
    }
}

async fn fetch_ptb_gap_db(start_ts: i64) -> Result<Option<f64>> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&env_or_default("DB_HOST", "192.3.67.106"));
    cfg.port(env_or_default("DB_PORT", "5432").parse().unwrap_or(5432));
    cfg.user(&env_or_default("DB_USER", "myuser"));
    cfg.password(env_or_default("DB_PASSWORD", "mypassword"));
    cfg.dbname(&env_or_default("DB_NAME", "mydb"));
    cfg.connect_timeout(std::time::Duration::from_secs(1));

    let connect = cfg.connect(NoTls);
    let (client, connection) = tokio::time::timeout(std::time::Duration::from_secs(1), connect)
        .await
        .context("ptb_gap db connect timeout")?
        .context("ptb_gap db connect")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let market_id = btc5m_slug(start_ts);
    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [&market_id];
    let query = client.query_opt(
        "SELECT ptb::float8 AS ptb FROM ptb_gap WHERE market_id = $1 ORDER BY timestamp ASC LIMIT 1",
        &params,
    );
    let row = tokio::time::timeout(std::time::Duration::from_secs(1), query)
        .await
        .context("ptb_gap db query timeout")?
        .context("ptb_gap db query")?;

    Ok(row
        .and_then(|r| r.try_get::<_, f64>("ptb").ok())
        .filter(|p| *p > 0.0))
}

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn chrono_iso(ts: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let secs = (UNIX_EPOCH + Duration::from_secs(ts as u64))
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}Z")
}

fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}
