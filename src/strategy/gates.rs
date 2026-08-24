//! Entry filters: gap×time, token trend — mirrors poly-prices captureSim.

use super::{
    GAP_TIME_Z_AT_MAX_SEC, GAP_TIME_Z_AT_MIN_SEC, MAX_OPP_RISE, MAX_TOKEN_FALL,
    TOKEN_TREND_LOOKBACK_MS,
};

pub const MIN_LEFT_S: f64 = 25.0;
pub const MIN_TOKEN_ASK: f64 = 0.12;
pub const MAX_TOKEN_ASK_GATE: f64 = 0.88;
pub const MIN_ASK_ROOM: f64 = 0.15;
pub const MAX_ABS_Z: f64 = 1.5;

#[derive(Clone, Debug)]
pub struct CaptureGateInput {
    pub direction: Side,
    pub gap_usd: f64,
    pub seconds_left: f64,
    pub sigma_rem: f64,
    pub token_ask: f64,
    pub impulse_usd: f64,
    pub expected_dp: f64,
    pub book_lag: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Up,
    Down,
}

pub fn gap_against_trend(direction: Side, gap_usd: f64) -> bool {
    matches!(
        (direction, gap_usd > 0.0),
        (Side::Down, true) | (Side::Up, false)
    ) && gap_usd != 0.0
}

pub fn max_against_z(seconds_left: f64) -> f64 {
    let min_s = 25.0;
    let max_s = 300.0;
    let z_lo = GAP_TIME_Z_AT_MIN_SEC;
    let z_hi = GAP_TIME_Z_AT_MAX_SEC;
    let t = seconds_left.clamp(min_s, max_s);
    z_lo + ((t - min_s) / (max_s - min_s)) * (z_hi - z_lo)
}

pub fn should_skip_gap_time(input: &CaptureGateInput) -> bool {
    if !gap_against_trend(input.direction, input.gap_usd) {
        return false;
    }
    let z = input.gap_usd.abs() / input.sigma_rem.max(4.0);
    if z > max_against_z(input.seconds_left) {
        return true;
    }
    let base = 0.01;
    let z_scale = 0.016;
    let time_boost = (60.0 / input.seconds_left.max(20.0)).sqrt();
    let min_dp = base + z_scale * z * time_boost;
    input.expected_dp < min_dp
}

pub fn should_skip_token_trend(
    direction: Side,
    poly_asks: &[(i64, f64, f64)],
    signal_t: i64,
) -> bool {
    let side_delta = token_ask_change(poly_asks, signal_t, direction, TOKEN_TREND_LOOKBACK_MS);
    if let Some(d) = side_delta {
        if d < -MAX_TOKEN_FALL {
            return true;
        }
    }
    let opp = match direction {
        Side::Up => Side::Down,
        Side::Down => Side::Up,
    };
    let opp_delta = token_ask_change(poly_asks, signal_t, opp, TOKEN_TREND_LOOKBACK_MS);
    if let (Some(d), Some(od)) = (side_delta, opp_delta) {
        if od > MAX_OPP_RISE && d <= 0.0 {
            return true;
        }
    }
    false
}

fn token_ask_change(
    poly: &[(i64, f64, f64)],
    t: i64,
    side: Side,
    lookback_ms: i64,
) -> Option<f64> {
    let now = ask_before(poly, t, side)?;
    let past = ask_before(poly, t - lookback_ms, side)?;
    Some(now - past)
}

fn ask_before(poly: &[(i64, f64, f64)], t: i64, side: Side) -> Option<f64> {
    let mut found: Option<f64> = None;
    for &(pt, up, down) in poly {
        if pt >= t {
            break;
        }
        let px = match side {
            Side::Up => up,
            Side::Down => down,
        };
        if px > 0.0 && px <= 1.5 {
            found = Some(px);
        }
    }
    found
}

pub fn down_from_up(up: f64) -> f64 {
    (1.0 - up + 0.01).clamp(0.01, 0.99)
}

pub fn other_side_ask(held: Side, held_ask: f64) -> f64 {
    match held {
        Side::Up => down_from_up(held_ask),
        Side::Down => (1.0 - held_ask + 0.01).clamp(0.01, 0.99),
    }
}

pub fn sigma_remaining(seconds_left: f64) -> f64 {
    const TABLE: &[(f64, f64)] = &[
        (0.0, 6.0),
        (10.0, 11.2),
        (20.0, 15.9),
        (30.0, 18.2),
        (60.0, 34.1),
        (90.0, 37.8),
        (120.0, 41.4),
        (180.0, 57.7),
        (240.0, 72.2),
        (300.0, 77.4),
    ];
    let left = seconds_left.max(0.0);
    for i in 1..TABLE.len() {
        let (t1, s1) = TABLE[i];
        let (t0, s0) = TABLE[i - 1];
        if left <= t1 {
            let w = (left - t0) / (t1 - t0).max(1e-6);
            return s0 + w * (s1 - s0);
        }
    }
    77.4
}

/// Mirrors poly-prices `evaluateCaptureGate`.
pub fn evaluate_capture_gate(
    direction: Side,
    gap_usd: f64,
    impulse_usd: f64,
    seconds_left: f64,
    sigma_rem: f64,
    token_ask: f64,
) -> CaptureGateResult {
    let sigma = sigma_rem.max(4.0);
    let z = gap_usd / sigma;
    let p0 = normal_cdf(z);
    let p1 = normal_cdf((gap_usd + impulse_usd) / sigma);
    let dp = p1 - p0;
    let expected_dp = dp.abs();
    let fair_p = match direction {
        Side::Up => p1,
        Side::Down => 1.0 - p1,
    };
    let book_lag = fair_p - token_ask;
    let room = 1.0 - token_ask;
    let min_dp = min_expected_dp_for_ask(token_ask);
    let dp_sign_ok = match direction {
        Side::Up => dp > 0.0,
        Side::Down => dp < 0.0,
    };
    let ok = seconds_left >= MIN_LEFT_S
        && token_ask < MAX_TOKEN_ASK_GATE
        && token_ask >= MIN_TOKEN_ASK
        && room >= MIN_ASK_ROOM
        && z.abs() <= MAX_ABS_Z
        && dp_sign_ok
        && expected_dp >= min_dp
        && expected_dp <= room * 0.9;
    CaptureGateResult {
        ok,
        expected_dp,
        book_lag,
        fair_p,
    }
}

pub fn min_expected_dp_for_ask(token_ask: f64) -> f64 {
    0.014 + 0.09 * token_ask
}

#[derive(Clone, Debug)]
pub struct CaptureGateResult {
    pub ok: bool,
    pub expected_dp: f64,
    pub book_lag: f64,
    pub fair_p: f64,
}

/// Gate for START emission — mirrors `evaluateCaptureGate` (not minBookLag; that's in pairing).
pub fn capture_gate_ok(input: &CaptureGateInput) -> bool {
    evaluate_capture_gate(
        input.direction,
        input.gap_usd,
        input.impulse_usd,
        input.seconds_left,
        input.sigma_rem,
        input.token_ask,
    )
    .ok
}

pub fn fair_p(direction: Side, gap_usd: f64, sigma_rem: f64) -> f64 {
    let z = gap_usd / sigma_rem.max(1e-6);
    let p_up = normal_cdf(z);
    match direction {
        Side::Up => p_up,
        Side::Down => 1.0 - p_up,
    }
}

fn normal_cdf(z: f64) -> f64 {
    0.5 * (1.0 + erf(z / std::f64::consts::SQRT_2))
}

fn erf(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;
    let s = if x < 0.0 { -1.0 } else { 1.0 };
    let ax = x.abs();
    let t = 1.0 / (1.0 + p * ax);
    let y = 1.0 - ((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * (-ax * ax).exp();
    s * y
}
