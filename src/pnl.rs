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
    fees: f64,
    buy_cost: f64,
    sell_proceeds: f64,
    paired_payout: f64,
    legs: u32,
    lots: Vec<OpenLot>,
}

#[derive(Clone, Debug)]
struct OpenLot {
    side: PositionSide,
    remaining: f64,
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

    pub fn open_window(&mut self, slug: String, _shares: f64) {
        self.window = Some(WindowLedger {
            slug,
            ..Default::default()
        });
    }

    /// `fill_px` is the actual CLOB fill (live) or dry-run modeled fill.
    pub fn record_buy(&mut self, side: PositionSide, fill_px: f64, shares: f64, _is_pair: bool) {
        let w = self.window.as_mut().expect("window ledger");
        let fee = taker_fee_usdc(shares, fill_px);
        w.fees += fee;
        w.buy_cost += shares * fill_px;
        w.legs += 1;
        w.lots.push(OpenLot {
            side,
            remaining: shares,
        });
        w.pair_open_lots();
    }

    /// Flatten sell proceeds (gap-swing emergency exit).
    pub fn record_sell(&mut self, side: PositionSide, fill_px: f64, shares: f64) {
        let w = self.window.as_mut().expect("window ledger");
        let fee = taker_fee_usdc(shares, fill_px);
        w.fees += fee;
        w.sell_proceeds += shares * fill_px;
        w.legs += 1;
        w.close_lots(side, shares);
    }

    pub fn close_window(&mut self, winner: Option<PositionSide>) -> Option<WindowClose> {
        let w = self.window.take()?;
        if w.legs == 0 {
            return None;
        }

        let settle_payout: f64 = w
            .lots
            .iter()
            .filter(|lot| Some(lot.side) == winner)
            .map(|lot| lot.remaining)
            .sum();
        let payout = w.paired_payout + w.sell_proceeds + settle_payout;

        let gross = payout - w.buy_cost;
        let net = gross - w.fees;

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

    pub fn add_window_net(&mut self, slug: &str, net: f64, pairs: u32, n_trades: usize) {
        self.window = None;
        self.totals.net += net;
        self.totals.gross += net;
        info!(
            "── window finished {slug} ── net ${:.2} | pairs {pairs} | trades {n_trades}",
            net
        );
        info!(
            "── session total ── net ${:.2}",
            self.totals.net
        );
    }
}

impl Default for WindowLedger {
    fn default() -> Self {
        Self {
            slug: String::new(),
            fees: 0.0,
            buy_cost: 0.0,
            sell_proceeds: 0.0,
            paired_payout: 0.0,
            legs: 0,
            lots: Vec::new(),
        }
    }
}

impl WindowLedger {
    fn pair_open_lots(&mut self) {
        loop {
            let up = self
                .lots
                .iter()
                .position(|lot| lot.side == PositionSide::Up && lot.remaining > 1e-12);
            let down = self
                .lots
                .iter()
                .position(|lot| lot.side == PositionSide::Down && lot.remaining > 1e-12);
            let (Some(up), Some(down)) = (up, down) else {
                break;
            };
            let q = self.lots[up].remaining.min(self.lots[down].remaining);
            if q <= 0.0 {
                break;
            }
            self.lots[up].remaining -= q;
            self.lots[down].remaining -= q;
            self.paired_payout += q;
        }
    }

    fn close_lots(&mut self, side: PositionSide, mut shares: f64) {
        for lot in &mut self.lots {
            if lot.side != side || shares <= 1e-12 {
                continue;
            }
            let q = lot.remaining.min(shares);
            lot.remaining -= q;
            shares -= q;
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
