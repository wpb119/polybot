//! Replay historical windows through the VENUE SWING batch oracle and verify
//! parity against the JS oracle (`strategy-venue-swing-final.js`).
//!
//! Usage:
//!   # In poly-history: npx tsx scripts/export-venue-windows.ts /tmp/venue-windows.jsonl
//!   cargo run --release --bin venue_replay -- /tmp/venue-windows.jsonl
//!
//! JSONL line format:
//!   { "slug", "openMs", "endMs", "winner": "UP"|"DOWN"|null,
//!     "binance":[{"t","price"}], "coinbase":[{"t","price"}],
//!     "asks":[{"t","up","down"}], "expectedPnl": <JS oracle total> }

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

use polybot::strategy::{run_venue_swing_window, PositionSide, VENUE_TAKER_DELAY_MS};

#[derive(Debug, Deserialize)]
struct TickPx {
    t: i64,
    price: f64,
}

#[derive(Debug, Deserialize)]
struct TickAsk {
    t: i64,
    up: f64,
    down: f64,
}

#[derive(Debug, Deserialize)]
struct WindowDump {
    slug: String,
    #[serde(rename = "openMs")]
    open_ms: i64,
    #[serde(rename = "endMs")]
    end_ms: i64,
    winner: Option<String>,
    binance: Vec<TickPx>,
    #[serde(default)]
    coinbase: Vec<TickPx>,
    asks: Vec<TickAsk>,
    #[serde(default)]
    shares: Option<f64>,
    #[serde(rename = "expectedPnl", default)]
    expected_pnl: Option<f64>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let path = env::args()
        .nth(1)
        .context("usage: venue_replay <windows.jsonl>")?;
    let file = File::open(&path).with_context(|| format!("open {path}"))?;
    let reader = BufReader::new(file);

    let mut total = 0.0;
    let mut expected_total = 0.0;
    let mut pairs = 0u64;
    let mut n = 0u64;
    let mut neg = 0u64;
    let mut mismatches = 0u64;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let w: WindowDump = serde_json::from_str(&line).context("parse jsonl")?;
        let bn: Vec<(i64, f64)> = w.binance.iter().map(|x| (x.t, x.price)).collect();
        let cb: Vec<(i64, f64)> = w.coinbase.iter().map(|x| (x.t, x.price)).collect();
        let asks: Vec<(i64, f64, f64)> = w.asks.iter().map(|x| (x.t, x.up, x.down)).collect();
        let winner = match w.winner.as_deref() {
            Some("UP") => Some(PositionSide::Up),
            Some("DOWN") => Some(PositionSide::Down),
            _ => None,
        };
        let shares = w.shares.unwrap_or(10.0);
        let res = run_venue_swing_window(
            w.open_ms,
            w.end_ms,
            &bn,
            &cb,
            &asks,
            winner,
            shares,
            Some(w.end_ms),
            VENUE_TAKER_DELAY_MS,
        );
        total += res.total_pnl;
        pairs += res.pairs as u64;
        n += 1;
        if res.total_pnl < -0.01 {
            neg += 1;
        }
        if let Some(exp) = w.expected_pnl {
            expected_total += exp;
            if (res.total_pnl - exp).abs() > 0.005 {
                mismatches += 1;
                if mismatches <= 10 {
                    info!(
                        "MISMATCH {} rust={:.4} js={:.4} pairs={} trades={}",
                        w.slug,
                        res.total_pnl,
                        exp,
                        res.pairs,
                        res.trades.len()
                    );
                }
            }
        }
    }

    println!(
        "VENUE_REPLAY n={n} total_pnl={total:.2} expected_js={expected_total:.2} mismatches={mismatches} pairs={pairs} neg={neg}"
    );
    if mismatches > 0 {
        std::process::exit(1);
    }
    Ok(())
}
