//! Batch replay: pairing detector + engine on merged ticks (dry-run economics).

use crate::pnl::{resolve_winner, PnlTracker};
use super::detector::TrendDetector;
use super::pairing::{BotAction, PairingEngine, PositionSide};

const TAKER_DELAY_MS: i64 = 250;

#[derive(Clone, Debug, Default)]
pub struct PairingWindowResult {
    pub total_pnl: f64,
    pub gross: f64,
    pub fees: f64,
    pub legs: u32,
}

enum Event {
    Bn(i64, f64),
    Cb(i64, f64),
    Ask(i64, f64, f64),
}

fn ask_before(asks: &[(i64, f64, f64)], t: i64, side: PositionSide) -> Option<f64> {
    let mut found = None;
    for &(pt, up, down) in asks {
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

fn apply_action(
    action: &BotAction,
    asks: &[(i64, f64, f64)],
    pnl: &mut PnlTracker,
    shares: f64,
) {
    match action {
        BotAction::BuyEntry {
            side,
            signal_t,
            worst_ask,
            ..
        } => {
            let fill_t = *signal_t + TAKER_DELAY_MS;
            let fill = ask_before(asks, fill_t, *side).unwrap_or(*worst_ask);
            pnl.record_buy(*side, fill, shares, false);
        }
        BotAction::BuyPair {
            side,
            signal_t,
            worst_ask,
            ..
        } => {
            let fill_t = *signal_t + TAKER_DELAY_MS;
            let fill = ask_before(asks, fill_t, *side).unwrap_or(*worst_ask);
            pnl.record_buy(*side, fill, shares, true);
        }
        BotAction::SellFlatten { .. } => {}
    }
}

/// Replay one window with pairing strategy (mirrors live dry-run loop).
pub fn run_pairing_window(
    open_ms: i64,
    end_ms: i64,
    ptb: Option<f64>,
    bn: &[(i64, f64)],
    cb: &[(i64, f64)],
    asks: &[(i64, f64, f64)],
    shares: f64,
) -> PairingWindowResult {
    let mut out = PairingWindowResult::default();
    if asks.is_empty() {
        return out;
    }

    let mut events: Vec<Event> = Vec::new();
    for &(t, p) in bn {
        events.push(Event::Bn(t, p));
    }
    for &(t, p) in cb {
        events.push(Event::Cb(t, p));
    }
    for &(t, up, down) in asks {
        if up > 0.0 && down > 0.0 {
            events.push(Event::Ask(t, up, down));
        }
    }
    events.sort_by_key(|e| match e {
        Event::Bn(t, _) | Event::Cb(t, _) | Event::Ask(t, _, _) => *t,
    });

    let mut detector = TrendDetector::new(open_ms, end_ms, ptb);
    let mut engine = PairingEngine::new();
    let mut pnl = PnlTracker::new();
    pnl.open_window("replay".into(), shares);

    for ev in events {
        match ev {
            Event::Bn(t, p) => {
                if let Some(sig) = detector.on_bn(t, p) {
                    if engine.is_flat() {
                        engine.on_signal(&sig, t);
                    }
                }
            }
            Event::Cb(t, p) => {
                if let Some(sig) = detector.on_cb(t, p) {
                    if engine.is_flat() {
                        engine.on_signal(&sig, t);
                    }
                }
            }
            Event::Ask(t, up, down) => {
                detector.push_poly(t, up, down);
                engine.record_poly(t, up, down);
                if let Some(action) = engine.tick(t, up, down, end_ms) {
                    apply_action(&action, asks, &mut pnl, shares);
                }
            }
        }
    }

    let last_ask = asks.last().copied();
    if let Some((_, up, down)) = last_ask {
        if let Some(action) = engine
            .tick(end_ms - 1, up, down, end_ms)
            .or_else(|| engine.try_last_tick_flatten(end_ms))
        {
            apply_action(&action, asks, &mut pnl, shares);
        }
    }

    let (up_ask, down_ask) = last_ask.map(|(_, u, d)| (u, d)).unwrap_or((0.5, 0.5));
    let last_bn = bn.last().map(|(_, p)| *p);
    let winner = resolve_winner(up_ask, down_ask, last_bn, ptb);

    if let Some(close) = pnl.close_window(winner) {
        out.total_pnl = close.net;
        out.gross = close.gross;
        out.fees = close.fees;
        out.legs = close.legs;
    }

    let _ = open_ms;
    out
}
