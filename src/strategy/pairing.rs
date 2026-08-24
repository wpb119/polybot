//! Live pairing state machine — mirrors poly-prices `captureSim` (DEFAULT_SIM_PARAMS).
//! Taker delay: submit on trigger; dry run fills at send+250ms via `PolyBook`.

use tracing::info;

use super::detector::CaptureSignal;
use super::gates::{other_side_ask, should_skip_gap_time, should_skip_token_trend, CaptureGateInput, Side};
use super::{
    DEAD_ASK, DEAD_LEFT_MS, HOLD_MFE, MAX_TOKEN_ASK, MIN_BOOK_LAG, PULLBACK, PULL_WAIT_MS,
    TAKER_DELAY_MS,
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
        signal_t: i64,
        side: PositionSide,
        ref_ask: Option<f64>,
        deadline: i64,
    },
    Holding {
        side: PositionSide,
        entry_t: i64,
        entry_ask: f64,
        peak_ask: f64,
        last_obs_t: i64,
        last_obs_ask: f64,
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
            impulse_usd: 0.0,
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
                    signal_t: sig.t,
                    side: sig.direction.into(),
                    ref_ask: None,
                    deadline: sig.t + TAKER_DELAY_MS + PULL_WAIT_MS,
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

        if matches!(&self.state, State::PendingEntry { .. }) {
            let pending = match &self.state {
                State::PendingEntry {
                    signal_t,
                    side,
                    ref_ask,
                    deadline,
                } => (*signal_t, *side, *ref_ask, *deadline),
                _ => unreachable!(),
            };
            let (signal_t, side, ref_ask_opt, deadline) = pending;
            let t0 = signal_t + TAKER_DELAY_MS;
            if now_ms < t0 {
                return None;
            }
            let ref_ask = ref_ask_opt.unwrap_or_else(|| {
                self.ask_before(t0, side).unwrap_or(side_ask(side, up_ask, down_ask))
            });
            if ref_ask_opt.is_none() {
                self.state = State::PendingEntry {
                    signal_t,
                    side,
                    ref_ask: Some(ref_ask),
                    deadline,
                };
            }
            let ask = side_ask(side, up_ask, down_ask);
            let obs_ask = self.ask_before(now_ms, side).unwrap_or(ask);

            if obs_ask <= ref_ask - PULLBACK {
                let buy_t = now_ms + TAKER_DELAY_MS;
                self.state = State::Holding {
                    side,
                    entry_t: buy_t,
                    entry_ask: obs_ask,
                    peak_ask: obs_ask,
                    last_obs_t: now_ms,
                    last_obs_ask: obs_ask,
                };
                return Some(BotAction::BuyEntry {
                    side,
                    worst_ask: clamp_price(ask + 0.02),
                    reason: "pullback fill".into(),
                });
            }
            if now_ms > deadline {
                let buy_t = now_ms + TAKER_DELAY_MS;
                self.state = State::Holding {
                    side,
                    entry_t: buy_t,
                    entry_ask: obs_ask,
                    peak_ask: obs_ask,
                    last_obs_t: now_ms,
                    last_obs_ask: obs_ask,
                };
                return Some(BotAction::BuyEntry {
                    side,
                    worst_ask: clamp_price(ask + 0.02),
                    reason: "chase fill".into(),
                });
            }
            return None;
        }

        if matches!(&self.state, State::Holding { .. }) {
            let holding = match &self.state {
                State::Holding {
                    side,
                    entry_t,
                    peak_ask,
                    ..
                } => (*side, *entry_t, *peak_ask),
                _ => unreachable!(),
            };
            let (side, entry_t, peak_ask) = holding;
            if now_ms >= entry_t + 400 {
                let ask = side_ask(side, up_ask, down_ask);
                let obs = self.ask_before(now_ms, side).unwrap_or(ask);
                if let State::Holding {
                    peak_ask: p,
                    last_obs_t,
                    last_obs_ask,
                    ..
                } = &mut self.state
                {
                    if obs > *p {
                        *p = obs;
                    }
                    *last_obs_t = now_ms;
                    *last_obs_ask = obs;
                }
            }
        }

        let pair_signal = if let State::Holding {
            side,
            entry_t,
            entry_ask,
            peak_ask,
            ..
        } = &self.state
        {
            if now_ms < *entry_t + 400 {
                return None;
            }
            let ask = side_ask(*side, up_ask, down_ask);
            let obs = self.ask_before(now_ms, *side).unwrap_or(ask);
            let peak = if obs > *peak_ask { obs } else { *peak_ask };
            let locked = peak - *entry_ask >= HOLD_MFE;
            let mut reason = None;
            // DEFAULT_SIM: skipDeadFlatten=false, skipCloseFlatten=true, skipTrail/Flip=true
            if !locked && end_ms - now_ms <= DEAD_LEFT_MS as i64 && obs <= DEAD_ASK {
                reason = Some("dead flatten");
            }
            reason.map(|r| (*side, obs, r))
        } else {
            None
        };

        if let Some((held, held_ask, reason)) = pair_signal {
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
            return Some(BotAction::BuyPair {
                side: pair_side,
                worst_ask: clamp_price(pair_ask + 0.02),
                reason: reason.into(),
            });
        }

        None
    }

    /// Last-tick underwater flatten (DEFAULT_SIM skipLastTickFlatten=false).
    pub fn try_last_tick_flatten(&mut self, end_ms: i64) -> Option<BotAction> {
        let State::Holding {
            side,
            entry_t,
            entry_ask,
            peak_ask,
            last_obs_t,
            last_obs_ask,
        } = &self.state
        else {
            return None;
        };

        if *entry_t + 400 > end_ms {
            return None;
        }
        let locked = *peak_ask - *entry_ask >= HOLD_MFE;
        if locked || *last_obs_ask > *entry_ask || *last_obs_t >= end_ms {
            return None;
        }

        let held_side = match *side {
            PositionSide::Up => Side::Up,
            PositionSide::Down => Side::Down,
        };
        let pair_ask = other_side_ask(held_side, *last_obs_ask);
        let pair_side = match *side {
            PositionSide::Up => PositionSide::Down,
            PositionSide::Down => PositionSide::Up,
        };

        self.state = State::Cooldown {
            until: end_ms + super::COOLDOWN_AFTER_EXIT_MS,
        };

        Some(BotAction::BuyPair {
            side: pair_side,
            worst_ask: clamp_price(pair_ask + 0.02),
            reason: "last tick flatten".into(),
        })
    }

    pub fn is_flat(&self) -> bool {
        matches!(self.state, State::Idle | State::Cooldown { .. })
    }

    fn ask_before(&self, t: i64, side: PositionSide) -> Option<f64> {
        let mut found = None;
        for &(pt, up, down) in &self.poly_history {
            if pt >= t {
                break;
            }
            let px = side_ask(side, up, down);
            if px > 0.0 && px <= 1.5 {
                found = Some(px);
            }
        }
        found
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
