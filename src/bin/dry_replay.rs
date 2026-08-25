//! Replay historical windows through gap-swing batch sim (same as dry-run economics).
//!
//! Usage:
//!   # Dump windows from poly-history, then:
//!   cargo run --release --bin dry_replay -- /tmp/gap-windows.jsonl
//!
//! JSONL line format:
//!   { "slug", "openMs", "endMs", "ptb", "winner": "UP"|"DOWN"|null,
//!     "binance":[{"t","price"}], "asks":[{"t","up","down"}] }

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

use polybot::strategy::{run_gap_swing_window, PositionSide};

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
    ptb: f64,
    winner: Option<String>,
    binance: Vec<TickPx>,
    asks: Vec<TickAsk>,
    #[serde(default)]
    shares: Option<f64>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let path = env::args()
        .nth(1)
        .context("usage: dry_replay <windows.jsonl>")?;
    let file = File::open(&path).with_context(|| format!("open {path}"))?;
    let reader = BufReader::new(file);

    let mut total = 0.0;
    let mut pairs = 0u64;
    let mut n = 0u64;
    let mut neg = 0u64;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let w: WindowDump = serde_json::from_str(&line).context("parse jsonl")?;
        let btc: Vec<(i64, f64)> = w.binance.iter().map(|x| (x.t, x.price)).collect();
        let asks: Vec<(i64, f64, f64)> = w.asks.iter().map(|x| (x.t, x.up, x.down)).collect();
        let winner = match w.winner.as_deref() {
            Some("UP") => Some(PositionSide::Up),
            Some("DOWN") => Some(PositionSide::Down),
            _ => None,
        };
        let shares = w.shares.unwrap_or(10.0);
        let res = run_gap_swing_window(
            w.open_ms,
            w.end_ms,
            Some(w.ptb),
            &btc,
            &asks,
            winner,
            shares,
            Some(w.end_ms),
            250, // dry/backtest taker delay
        );
        total += res.total_pnl;
        pairs += res.pairs as u64;
        n += 1;
        if res.total_pnl < -0.01 {
            neg += 1;
        }
        if n <= 5 || res.total_pnl.abs() > 5.0 {
            info!(
                "{} pnl={:.2} pairs={} trades={}",
                w.slug,
                res.total_pnl,
                res.pairs,
                res.trades.len()
            );
        }
    }

    println!(
        "DRY_REPLAY n={n} total_pnl={total:.2} pairs={pairs} neg={neg}"
    );
    Ok(())
}
