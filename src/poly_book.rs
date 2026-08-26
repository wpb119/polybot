use std::collections::HashMap;

use crate::strategy::PositionSide;

pub const TAKER_DELAY_MS: i64 = 250;
/// Always keep current + next window on the WS so the next book is warm before UTC open
/// (avoids T-5s resubscribe blackout that missed early buys).
pub const PRE_SUBSCRIBE_MS: i64 = 300_000;

#[derive(Clone, Debug, Default)]
pub struct MarketQuotes {
    pub t: i64,
    pub up_ask: f64,
    pub down_ask: f64,
}

/// Per-window poly ask history (causal fills at send + 250ms).
pub struct PolyBook {
    windows: HashMap<i64, Vec<(i64, f64, f64)>>,
    latest: HashMap<i64, MarketQuotes>,
}

impl PolyBook {
    pub fn new() -> Self {
        Self {
            windows: HashMap::new(),
            latest: HashMap::new(),
        }
    }

    pub fn push(&mut self, start_ts: i64, t: i64, up: f64, down: f64) {
        if up <= 0.0 {
            return;
        }
        let w = self.windows.entry(start_ts).or_default();
        w.push((t, up, down));
        // Keep enough history to seed the engine at window open (pre-open asks).
        let cutoff = t - 120_000;
        while w.first().is_some_and(|(pt, _, _)| *pt < cutoff) {
            w.remove(0);
        }
        self.latest.insert(
            start_ts,
            MarketQuotes {
                t,
                up_ask: up,
                down_ask: down,
            },
        );
    }

    pub fn latest(&self, start_ts: i64) -> Option<MarketQuotes> {
        self.latest.get(&start_ts).cloned()
    }

    /// Ask tape for a window (for seeding gap-swing at open).
    pub fn history(&self, start_ts: i64) -> Vec<(i64, f64, f64)> {
        self.windows.get(&start_ts).cloned().unwrap_or_default()
    }

    pub fn ask_before(&self, start_ts: i64, t: i64, side: PositionSide) -> Option<f64> {
        let w = self.windows.get(&start_ts)?;
        let mut found = None;
        for &(pt, up, down) in w {
            if pt >= t {
                break;
            }
            let px = match side {
                PositionSide::Up => up,
                PositionSide::Down => down,
            };
            if px > 0.0 && px <= 1.5 {
                found = Some(px);
            }
        }
        found
    }

    pub fn clear_window(&mut self, start_ts: i64) {
        self.windows.remove(&start_ts);
        self.latest.remove(&start_ts);
    }
}

/// Which window(s) to subscribe on Polymarket WS — always current + next so next
/// market asks are live for the full prior window (no late subscribe at T-5s).
pub fn subscribe_window_starts(now_ms: i64) -> Vec<i64> {
    let now_sec = now_ms / 1000;
    let current = (now_sec / 300) * 300;
    let next = current + 300;
    vec![current, next]
}

/// Active trading window (only after UTC open).
pub fn trading_window_start(now_ms: i64) -> i64 {
    let now_sec = now_ms / 1000;
    (now_sec / 300) * 300
}

pub fn window_open_ms(start_ts: i64) -> i64 {
    start_ts * 1000
}

pub fn window_end_ms(start_ts: i64) -> i64 {
    (start_ts + 300) * 1000
}
