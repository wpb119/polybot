//! One-window pairing replay JSON → PnL (for compare-pairing-bot.ts).

use std::env;
use std::fs;

use serde::Deserialize;

use polybot::strategy::run_pairing_window;

#[derive(Deserialize)]
struct TickPx {
    t: i64,
    price: f64,
}

#[derive(Deserialize)]
struct TickAsk {
    t: i64,
    up: f64,
    down: f64,
}

#[derive(Deserialize)]
struct Dump {
    #[serde(rename = "openMs")]
    open_ms: i64,
    #[serde(rename = "endMs")]
    end_ms: i64,
    ptb: f64,
    bn: Vec<TickPx>,
    cb: Vec<TickPx>,
    asks: Vec<TickAsk>,
    shares: f64,
}

fn main() {
    let path = env::args().nth(1).expect("json path");
    let raw = fs::read_to_string(path).expect("read");
    let w: Dump = serde_json::from_str(&raw).expect("parse");
    let bn: Vec<(i64, f64)> = w.bn.iter().map(|x| (x.t, x.price)).collect();
    let cb: Vec<(i64, f64)> = w.cb.iter().map(|x| (x.t, x.price)).collect();
    let asks: Vec<(i64, f64, f64)> = w
        .asks
        .iter()
        .map(|x| (x.t, x.up, x.down))
        .collect();
    let res = run_pairing_window(
        w.open_ms,
        w.end_ms,
        Some(w.ptb),
        &bn,
        &cb,
        &asks,
        w.shares,
    );
    println!(
        "{}",
        serde_json::json!({
            "net": res.total_pnl,
            "gross": res.gross,
            "fees": res.fees,
            "legs": res.legs,
        })
    );
}
