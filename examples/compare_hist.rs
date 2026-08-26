use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use serde::Deserialize;
use polybot::strategy::{run_gap_swing_window, PositionSide};

#[derive(Deserialize)]
struct TickPx { t: i64, price: f64 }
#[derive(Deserialize)]
struct TickAsk { t: i64, up: f64, down: f64 }
#[derive(Deserialize)]
struct WindowDump {
    slug: String,
    #[serde(rename = "openMs")] open_ms: i64,
    #[serde(rename = "endMs")] end_ms: i64,
    ptb: f64,
    winner: Option<String>,
    binance: Vec<TickPx>,
    asks: Vec<TickAsk>,
    #[serde(default)] shares: Option<f64>,
}

fn main() {
    let path = env::args().nth(1).expect("jsonl");
    let reader = BufReader::new(File::open(path).unwrap());
    for line in reader.lines() {
        let line = line.unwrap();
        if line.trim().is_empty() { continue; }
        let w: WindowDump = serde_json::from_str(&line).unwrap();
        let btc: Vec<_> = w.binance.iter().map(|x| (x.t, x.price)).collect();
        let asks: Vec<_> = w.asks.iter().map(|x| (x.t, x.up, x.down)).collect();
        let winner = match w.winner.as_deref() {
            Some("UP") => Some(PositionSide::Up),
            Some("DOWN") => Some(PositionSide::Down),
            _ => None,
        };
        let res = run_gap_swing_window(
            w.open_ms, w.end_ms, Some(w.ptb), &btc, &asks, winner,
            w.shares.unwrap_or(5.0), Some(w.end_ms), 250,
        );
        let legs: Vec<String> = res.trades.iter()
            .filter(|t| t.kind == "BUY" || t.kind == "SELL")
            .map(|t| format!("{:.1}s {} {:?} {}", (t.t - w.open_ms) as f64 / 1000.0, t.kind, t.side, t.reason))
            .collect();
        println!("{} pnl={:.2} | {}", w.slug, res.total_pnl, if legs.is_empty() { "(none)".into() } else { legs.join(" | ") });
    }
}
