use tracing::info;

use crate::strategy::PositionSide;

pub const CRYPTO_TAKER_FEE_RATE: f64 = 0.07;

#[derive(Clone, Debug, Default)]
pub struct Totals {
    pub net: f64,
    pub gross: f64,
    pub fees: f64,
}

#[derive(Clone, Debug)]
pub struct WindowClose {
    pub slug: String,
    pub net: f64,
    pub gross: f64,
    pub fees: f64,
    pub legs: u32,
}

struct WindowLedger {
    slug: String,
    shares: f64,
    fees: f64,
    cost: f64,
    entry_side: Option<PositionSide>,
    paired: bool,
    legs: u32,
}

pub struct PnlTracker {
    totals: Totals,
    window: Option<WindowLedger>,
}

impl PnlTracker {
    pub fn new() -> Self {
        Self {
            totals: Totals::default(),
            window: None,
        }
    }

    pub fn totals(&self) -> &Totals {
        &self.totals
    }

    pub fn open_window(&mut self, slug: String, shares: f64) {
        self.window = Some(WindowLedger {
            slug,
            shares,
            ..Default::default()
        });
    }

    pub fn record_buy(&mut self, side: PositionSide, fill_px: f64, shares: f64, is_pair: bool) {
        let w = self.window.as_mut().expect("window ledger");
        let fee = taker_fee_usdc(shares, fill_px);
        let cost = shares * fill_px + fee;
        w.fees += fee;
        w.cost += cost;
        w.legs += 1;
        if is_pair {
            w.paired = true;
        } else {
            w.entry_side = Some(side);
        }
    }

    pub fn close_window(&mut self, winner: Option<PositionSide>) -> Option<WindowClose> {
        let w = self.window.take()?;
        if w.legs == 0 {
            return None;
        }

        let payout = if w.paired {
            w.shares
        } else if let Some(entry) = w.entry_side {
            if winner == Some(entry) {
                w.shares
            } else {
                0.0
            }
        } else {
            0.0
        };

        let gross = payout - (w.cost - w.fees);
        let net = payout - w.cost;

        self.totals.fees += w.fees;
        self.totals.net += net;
        self.totals.gross += gross;

        Some(WindowClose {
            slug: w.slug,
            net,
            gross,
            fees: w.fees,
            legs: w.legs,
        })
    }

    pub fn log_window_close(&self, close: &WindowClose) {
        info!(
            "── window finished {} ── net ${:.2} | gross ${:.2} | fees ${:.2} | legs {}",
            close.slug, close.net, close.gross, close.fees, close.legs
        );
        info!(
            "── session total ── net ${:.2} | gross ${:.2} | fees ${:.2}",
            self.totals.net, self.totals.gross, self.totals.fees
        );
    }
}

impl Default for WindowLedger {
    fn default() -> Self {
        Self {
            slug: String::new(),
            shares: 0.0,
            fees: 0.0,
            cost: 0.0,
            entry_side: None,
            paired: false,
            legs: 0,
        }
    }
}

pub fn taker_fee_usdc(shares: f64, price: f64) -> f64 {
    let p = price.clamp(0.01, 0.99);
    let raw = shares * CRYPTO_TAKER_FEE_RATE * p * (1.0 - p);
    (raw * 1e5).round() / 1e5
}

/// Mirrors poly-prices `resolveWinner`.
pub fn resolve_winner(
    up_ask: f64,
    down_ask: f64,
    last_btc: Option<f64>,
    ptb: Option<f64>,
) -> Option<PositionSide> {
    if up_ask >= 0.55 && down_ask <= 0.45 {
        return Some(PositionSide::Up);
    }
    if down_ask >= 0.55 && up_ask <= 0.45 {
        return Some(PositionSide::Down);
    }
    if let (Some(last), Some(strike)) = (last_btc, ptb) {
        if last != strike {
            return Some(if last > strike {
                PositionSide::Up
            } else {
                PositionSide::Down
            });
        }
    }
    None
}
