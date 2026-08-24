//! Pairing strategy constants — mirrors poly-prices `captureSim.rs` defaults.
//! `TAKER_DELAY_MS` is CLOB-side only (fill ~250ms after submit); live bot sends immediately.

pub const TAKER_DELAY_MS: i64 = 250;
pub const PULLBACK: f64 = 0.04;
pub const PULL_WAIT_MS: i64 = 4_000;
pub const COOLDOWN_AFTER_EXIT_MS: i64 = 1_500;

pub const HOLD_MFE: f64 = 0.02;
pub const MIN_BOOK_LAG: f64 = 0.015;
pub const MAX_TOKEN_ASK: f64 = 0.75;
pub const DEAD_ASK: f64 = 0.20;
pub const DEAD_LEFT_MS: i64 = 90_000;

pub const GAP_TIME_Z_AT_MIN_SEC: f64 = 0.5;
pub const GAP_TIME_Z_AT_MAX_SEC: f64 = 2.0;
pub const TOKEN_TREND_LOOKBACK_MS: i64 = 2_000;
pub const MAX_TOKEN_FALL: f64 = 0.035;
pub const MAX_OPP_RISE: f64 = 0.04;

pub const BN_W: f64 = 0.6;
pub const CB_W: f64 = 0.4;

pub const MIN_R1: f64 = 2.4;
pub const Z_R1: f64 = 1.5;
pub const MIN_NOW_FRAC: f64 = 0.5;
pub const MIN_SIGNED_ACC: f64 = -1.5;
pub const COOLDOWN_MS: i64 = 12_000;
pub const MAX_CAPTURES: u32 = 3;

mod detector;
mod gates;
mod pairing;

pub use detector::{CaptureSignal, TrendDetector};
pub use pairing::{BotAction, PairingEngine, PositionSide};
