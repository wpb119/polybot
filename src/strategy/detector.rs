//! Incremental BTC impulse START detector — mirrors poly-prices `trendCapture.rs`.

use super::gates::{capture_gate_ok, fair_p, sigma_remaining, CaptureGateInput, Side};
use super::{
    BN_W, CB_W, COOLDOWN_MS, MAX_CAPTURES, MIN_NOW_FRAC, MIN_R1, MIN_SIGNED_ACC, Z_R1,
};

#[derive(Clone, Debug)]
pub struct CaptureSignal {
    pub t: i64,
    pub direction: Side,
    pub token_ask: f64,
    pub gap_usd: f64,
    pub seconds_left: f64,
    pub book_lag: f64,
    pub expected_dp: f64,
    pub tradable: bool,
}

struct PriceBuf {
    points: Vec<(i64, f64)>,
}

impl PriceBuf {
    fn push(&mut self, t: i64, price: f64) {
        self.points.push((t, price));
        let cutoff = t - 60_050;
        while self.points.first().is_some_and(|(pt, _)| *pt < cutoff) {
            self.points.remove(0);
        }
    }

    fn usd_return(&self, now: i64, h: i64) -> f64 {
        let cur = self.points.last().map(|(_, p)| *p);
        let prev = self.price_at_or_before(now - h);
        match (cur, prev) {
            (Some(c), Some(p)) => c - p,
            _ => 0.0,
        }
    }

    fn price_at_or_before(&self, t: i64) -> Option<f64> {
        let mut found = None;
        for &(pt, p) in &self.points {
            if pt > t {
                break;
            }
            found = Some(p);
        }
        found
    }

    fn last(&self) -> Option<f64> {
        self.points.last().map(|(_, p)| *p)
    }
}

fn sign_of(x: f64, eps: f64) -> i8 {
    if x > eps {
        1
    } else if x < -eps {
        -1
    } else {
        0
    }
}

pub struct TrendDetector {
    bn: PriceBuf,
    cb: PriceBuf,
    norm: PriceBuf,
    poly: Vec<(i64, f64, f64)>,
    strike: Option<f64>,
    open_ms: i64,
    end_ms: i64,
    trend: Option<Side>,
    last_fire_ms: i64,
    start_count: u32,
    last_quality_ms: i64,
    prev_vel: Option<(i64, f64)>,
    r1_samples: Vec<(i64, f64)>,
}

impl TrendDetector {
    pub fn new(open_ms: i64, end_ms: i64, ptb: Option<f64>) -> Self {
        Self {
            bn: PriceBuf { points: vec![] },
            cb: PriceBuf { points: vec![] },
            norm: PriceBuf { points: vec![] },
            poly: vec![],
            strike: ptb,
            open_ms,
            end_ms,
            trend: None,
            last_fire_ms: i64::MIN / 2,
            start_count: 0,
            last_quality_ms: 0,
            prev_vel: None,
            r1_samples: vec![],
        }
    }

    pub fn reset_window(&mut self, open_ms: i64, end_ms: i64, ptb: Option<f64>) {
        self.open_ms = open_ms;
        self.end_ms = end_ms;
        self.strike = ptb;
        self.trend = None;
        self.last_fire_ms = i64::MIN / 2;
        self.start_count = 0;
        self.last_quality_ms = 0;
        self.prev_vel = None;
        self.r1_samples.clear();
        self.poly.clear();
    }

    pub fn strike(&self) -> Option<f64> {
        self.strike
    }

    pub fn push_poly(&mut self, t: i64, up: f64, down: f64) {
        if up > 0.0 {
            self.poly.push((t, up, down));
        }
    }

    pub fn on_bn(&mut self, t: i64, price: f64) -> Option<CaptureSignal> {
        self.bn.push(t, price);
        self.push_norm(t);
        self.step(t)
    }

    pub fn on_cb(&mut self, t: i64, price: f64) -> Option<CaptureSignal> {
        self.cb.push(t, price);
        self.push_norm(t);
        self.step(t)
    }

    fn push_norm(&mut self, t: i64) {
        let b = self.bn.last();
        let c = self.cb.last();
        let px = match (b, c) {
            (Some(b), Some(c)) => BN_W * b + CB_W * c,
            (Some(b), None) => b,
            (None, Some(c)) => c,
            _ => return,
        };
        self.norm.push(t, px);
        if self.strike.is_none() && t >= self.open_ms {
            self.strike = Some(px);
        }
    }

    fn realized_sigma1s(&self, now: i64) -> f64 {
        let xs: Vec<f64> = self
            .r1_samples
            .iter()
            .filter(|(pt, _)| now - *pt <= 30_000)
            .map(|(_, r)| *r)
            .collect();
        if xs.len() < 8 {
            return 8.0;
        }
        let mu = xs.iter().sum::<f64>() / xs.len() as f64;
        let var = xs.iter().map(|x| (x - mu).powi(2)).sum::<f64>() / xs.len() as f64;
        var.sqrt().max(3.0)
    }

    fn step(&mut self, tick_t: i64) -> Option<CaptureSignal> {
        if tick_t < self.open_ms || tick_t >= self.end_ms {
            return None;
        }
        let n = self.norm.last()?;
        let r250 = self.norm.usd_return(tick_t, 250);
        let r500 = self.norm.usd_return(tick_t, 500);
        let r1 = self.norm.usd_return(tick_t, 1000);
        self.r1_samples.push((tick_t, r1));
        let vel = r1;
        let mut acc = 0.0;
        if let Some((pt, pv)) = self.prev_vel {
            let dt = (tick_t - pt) as f64 / 1000.0;
            if dt > 0.04 {
                acc = (vel - pv) / dt;
            }
        }
        self.prev_vel = Some((tick_t, vel));

        let sig1 = self.realized_sigma1s(tick_t);
        let s = sign_of(r1, 0.4);
        if s == 0 {
            return None;
        }
        let aligned = sign_of(r250, 0.15) == s
            && sign_of(r500, 0.2) == s
            && r250.abs() >= MIN_NOW_FRAC * r1.abs();
        let bn1 = self.bn.usd_return(tick_t, 1000);
        let cb1 = self.cb.usd_return(tick_t, 1000);
        let both_venues = sign_of(bn1, 0.5) == s && sign_of(cb1, 0.5) == s;
        let r2 = self.norm.usd_return(tick_t, 2000);
        let bounce =
            sign_of(r2, 0.4) != 0 && sign_of(r2, 0.4) != s && r2.abs() > 0.85 * r1.abs();
        let big_enough = r1.abs() >= MIN_R1 && r1.abs() >= Z_R1 * sig1;
        let signed_acc = if s > 0 { acc } else { -acc };
        let quality = aligned && both_venues && !bounce && big_enough && signed_acc >= MIN_SIGNED_ACC;

        if !quality {
            return None;
        }

        let direction = if s > 0 { Side::Up } else { Side::Down };
        self.last_quality_ms = tick_t;

        if self.trend == Some(direction) {
            return None;
        }
        if tick_t - self.last_fire_ms < COOLDOWN_MS {
            return None;
        }
        if self.start_count >= MAX_CAPTURES {
            return None;
        }

        let seconds_left = ((self.end_ms - tick_t) as f64 / 1000.0).max(1.0);
        let gap_usd = n - self.strike.unwrap_or(n);
        let sigma_rem = sigma_remaining(seconds_left);
        let (token_ask, book_lag, expected_dp) = self.token_metrics(tick_t, direction, gap_usd, sigma_rem, r1);
        if token_ask <= 0.0 {
            return None;
        }

        let gate_in = CaptureGateInput {
            direction,
            gap_usd,
            seconds_left,
            sigma_rem,
            token_ask,
            expected_dp,
            book_lag,
        };
        if !capture_gate_ok(&gate_in) {
            return None;
        }

        self.trend = Some(direction);
        self.last_fire_ms = tick_t;
        self.start_count += 1;

        Some(CaptureSignal {
            t: tick_t,
            direction,
            token_ask,
            gap_usd,
            seconds_left,
            book_lag,
            expected_dp,
            tradable: true,
        })
    }

    fn token_metrics(
        &self,
        t: i64,
        direction: Side,
        gap_usd: f64,
        sigma_rem: f64,
        impulse: f64,
    ) -> (f64, f64, f64) {
        let mut token_ask = 0.0;
        for &(pt, up, down) in &self.poly {
            if pt > t {
                break;
            }
            token_ask = match direction {
                Side::Up => up,
                Side::Down => down,
            };
        }
        let fair = fair_p(direction, gap_usd, sigma_rem);
        let book_lag = fair - token_ask;
        let expected_dp = book_lag + impulse.abs() / 100.0; // simplified
        (token_ask, book_lag, expected_dp)
    }
}
