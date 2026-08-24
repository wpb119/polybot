//! Live pairing state machine — entry pullback + pair exit.

use tracing::info;

use super::detector::CaptureSignal;
use super::gates::{other_side_ask, should_skip_gap_time, should_skip_token_trend, CaptureGateInput, Side};
use super::{
    DEAD_ASK, DEAD_LEFT_MS, HOLD_MFE, MAX_TOKEN_ASK, MIN_BOOK_LAG, PULLBACK, PULL_WAIT_MS,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionSide {
    Up,
    Down,
}

impl From<Side> for PositionSide {
    fn from(s: Side) -> Self {
        match s {
            Side::Up => PositionSide::Up,
            Side::Down => PositionSide::Down,
        }
    }
}

#[derive(Clone, Debug)]
pub enum BotAction {
    BuyEntry {
        side: PositionSide,
        worst_ask: f64,
        reason: String,
    },
    BuyPair {
        side: PositionSide,
        worst_ask: f64,
        reason: String,
    },
}

#[derive(Clone, Debug)]
enum State {
    Idle,
    PendingEntry {
        side: PositionSide,
        ref_ask: f64,
        deadline: i64,
    },
    Holding {
        side: PositionSide,
        entry_t: i64,
        entry_ask: f64,
        peak_ask: f64,
    },
    Cooldown { until: i64 },
}

pub struct PairingEngine {
    state: State,
    poly_history: Vec<(i64, f64, f64)>,
}

impl PairingEngine {
    pub fn new() -> Self {
        Self {
            state: State::Idle,
            poly_history: vec![],
        }
    }

    pub fn reset_window(&mut self) {
        self.state = State::Idle;
        self.poly_history.clear();
    }

    pub fn record_poly(&mut self, t: i64, up: f64, down: f64) {
        self.poly_history.push((t, up, down));
        let cutoff = t - 10_000;
        while self.poly_history.first().is_some_and(|(pt, _, _)| *pt < cutoff) {
            self.poly_history.remove(0);
        }
    }

    pub fn on_signal(&mut self, sig: &CaptureSignal, _now_ms: i64) -> Option<BotAction> {
        if !sig.tradable {
            return None;
        }
        if sig.token_ask > MAX_TOKEN_ASK {
            return None;
        }
        if sig.book_lag < MIN_BOOK_LAG {
            return None;
        }
        let gate_in = CaptureGateInput {
            direction: sig.direction,
            gap_usd: sig.gap_usd,
            seconds_left: sig.seconds_left,
            sigma_rem: super::gates::sigma_remaining(sig.seconds_left),
            token_ask: sig.token_ask,
            expected_dp: sig.expected_dp,
            book_lag: sig.book_lag,
        };
        if should_skip_gap_time(&gate_in) {
            info!("skip entry: gap×time");
            return None;
        }
        if should_skip_token_trend(sig.direction, &self.poly_history, sig.t) {
            info!("skip entry: token trend");
            return None;
        }

        match &self.state {
            State::Idle => {
                self.state = State::PendingEntry {
                    side: sig.direction.into(),
                    ref_ask: sig.token_ask,
                    deadline: sig.t + PULL_WAIT_MS,
                };
                None
            }
            _ => None,
        }
    }

    pub fn tick(
        &mut self,
        now_ms: i64,
        up_ask: f64,
        down_ask: f64,
        end_ms: i64,
    ) -> Option<BotAction> {
        self.record_poly(now_ms, up_ask, down_ask);

        if let State::Cooldown { until } = &self.state {
            if now_ms >= *until {
                self.state = State::Idle;
            } else {
                return None;
            }
        }

        if let State::PendingEntry {
            side,
            ref_ask,
            deadline,
            ..
        } = &self.state
        {
            let side = *side;
            let ref_ask = *ref_ask;
            let deadline = *deadline;
            let ask = side_ask(side, up_ask, down_ask);
            // Send to CLOB as soon as pullback/chase triggers — 250ms fill delay is on CLOB side.
            if ask <= ref_ask - PULLBACK {
                self.state = State::Holding {
                    side,
                    entry_t: now_ms,
                    entry_ask: ask,
                    peak_ask: ask,
                };
                return Some(BotAction::BuyEntry {
                    side,
                    worst_ask: clamp_price(ask + 0.02),
                    reason: "pullback fill".into(),
                });
            }
            if now_ms > deadline {
                self.state = State::Holding {
                    side,
                    entry_t: now_ms,
                    entry_ask: ask,
                    peak_ask: ask,
                };
                return Some(BotAction::BuyEntry {
                    side,
                    worst_ask: clamp_price(ask + 0.02),
                    reason: "chase fill".into(),
                });
            }
            return None;
        }

        if let State::Holding { peak_ask, side, .. } = &mut self.state {
            let ask = side_ask(*side, up_ask, down_ask);
            if ask > *peak_ask {
                *peak_ask = ask;
            }
        }

        let pair_signal = if let State::Holding {
            side,
            entry_t,
            entry_ask,
            peak_ask,
        } = &self.state
        {
            let ask = side_ask(*side, up_ask, down_ask);
            let peak = if ask > *peak_ask { ask } else { *peak_ask };
            let locked = peak - *entry_ask >= HOLD_MFE;
            let underwater = ask <= *entry_ask;
            let seconds_left = ((end_ms - now_ms) as f64 / 1000.0).max(0.0);
            let mut reason = None;
            if !locked && seconds_left * 1000.0 <= DEAD_LEFT_MS as f64 && ask <= DEAD_ASK {
                reason = Some("dead flatten");
            } else if !locked && underwater && now_ms - *entry_t > 400 && seconds_left < 5.0 {
                reason = Some("underwater flatten");
            }
            reason.map(|r| (*side, *entry_ask, ask, r))
        } else {
            None
        };

        if let Some((held, entry_ask, held_ask, reason)) = pair_signal {
            let held_side = match held {
                PositionSide::Up => Side::Up,
                PositionSide::Down => Side::Down,
            };
            let pair_ask = other_side_ask(held_side, held_ask);
            let pair_side = match held {
                PositionSide::Up => PositionSide::Down,
                PositionSide::Down => PositionSide::Up,
            };
            self.state = State::Cooldown {
                until: now_ms + super::COOLDOWN_AFTER_EXIT_MS,
            };
            let _ = entry_ask;
            return Some(BotAction::BuyPair {
                side: pair_side,
                worst_ask: clamp_price(pair_ask + 0.02),
                reason: reason.into(),
            });
        }

        None
    }

    pub fn is_flat(&self) -> bool {
        matches!(self.state, State::Idle | State::Cooldown { .. })
    }
}

fn side_ask(side: PositionSide, up: f64, down: f64) -> f64 {
    match side {
        PositionSide::Up => up,
        PositionSide::Down => down,
    }
}

fn clamp_price(n: f64) -> f64 {
    ((n * 100.0).round() / 100.0).clamp(0.01, 0.99)
}
