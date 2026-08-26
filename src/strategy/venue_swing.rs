//! VENUE SWING — port of poly-history `strategy-venue-swing-final.js`
//! (spec: `VENUE_SWING_AGENT_GUIDE.md`). Best verified engine:
//! 7d @ 10 sh, fill t+70ms → ~+$4.2k (vs ~+$2.9k for PTB gap-swing).
//!
//! Signal: TWO independent zigzags — Binance − Binance window-open and
//! Coinbase − Coinbase window-open — union-merged (keep the more extreme of
//! consecutive same-kind swings). Peak→BUY DOWN, trough→BUY UP, pair when
//! YES+NO nets ≥ 0 after fees, flatten unpaired at T−25s.
//! Settlement winner stays official Chainlink PTB — never venue prices.
//!
//! Dry/backtest: fill = ask at signal+70ms. Live: delay 0 (post at signal).

use std::collections::HashSet;

use tracing::info;

use super::gap_swing::{CommittedLot, SimTrade, WindowResult};
use super::{BotAction, PositionSide};
use crate::pnl::taker_fee_usdc;

/// Venue-swing fill latency (guide §6.1). Live uses 0.
pub const TAKER_DELAY_MS: i64 = 70;
pub const GRID_MS: i64 = 250;
pub const TICK_MS: i64 = 500;

pub const MIN_SWING_USD: f64 = 40.0;
pub const CONFIRM_USD: f64 = 10.0;
pub const MAX_CHEAP_ASK: f64 = 0.78;
pub const MAX_SECOND_ASK: f64 = 0.76;
pub const MAX_EARLY_OPP_ASK: f64 = 0.88;
pub const MIN_ASK: f64 = 0.04;
pub const MIN_FIRST_ASK: f64 = 0.12;
pub const MIN_PEAK_DELTA: f64 = 40.0;
pub const MAX_TROUGH_DELTA: f64 = 52.0;
pub const EARLY_OPP_SWING_USD: f64 = 30.0;
pub const MIN_UP_FIRST_DELTA: f64 = -75.0;
pub const RAPID_FALL_USD: f64 = 55.0;
pub const RAPID_FALL_MS: i64 = 12_000;
pub const MIN_LEFT_MS: i64 = 10_000;
pub const RESTART_MIN_LEFT_MS: i64 = 45_000;
pub const PAIR_COOLDOWN_MS: i64 = 8_000;
pub const FIRST_COOLDOWN_MS: i64 = 400;
pub const MAX_PAIRS: u32 = 16;
pub const DELAY_BUFFER_PER_LEG: f64 = 0.005;
pub const MIN_RAW_PAIR_NET: f64 = 0.0;
pub const EMERGENCY_LEFT_MS: i64 = 25_000;
pub const FEE_RATE: f64 = 0.07;
pub const SELL_HAIRCUT: f64 = 0.01;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExtKind {
    Peak,
    Trough,
}

#[derive(Clone, Debug)]
struct Swing {
    t: i64,
    d: f64,
    kind: ExtKind,
    side: PositionSide,
    ask: f64,
}

struct Lot {
    side: PositionSide,
    shares: f64,
    fill: f64,
    fee: f64,
    paired: f64,
    anchor_d: f64,
}

/* ------------------------------------------------------------------ *
 *  Helpers (guide §2)                                                  *
 * ------------------------------------------------------------------ */

fn px_at(series: &[(i64, f64)], t: i64) -> Option<f64> {
    let mut found = None;
    for &(pt, px) in series {
        if pt > t {
            break;
        }
        found = Some(px);
    }
    found
}

fn ask_at(asks: &[(i64, f64, f64)], t: i64, side: PositionSide) -> Option<f64> {
    let mut found = None;
    for &(pt, up, down) in asks {
        if pt > t {
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

fn delta_drop_over(tape: &[(i64, f64)], t: i64, lookback_ms: i64) -> Option<f64> {
    let now = px_at(tape, t)?;
    let prev = px_at(tape, t - lookback_ms)?;
    Some(prev - now)
}

fn fee_share(price: f64) -> f64 {
    let p = price.clamp(0.01, 0.99);
    FEE_RATE * p * (1.0 - p)
}

fn net_gap_ok(a: f64, b: f64) -> bool {
    let fees = fee_share(a) + fee_share(b) + 2.0 * DELAY_BUFFER_PER_LEG;
    1.0 - a - b - fees >= MIN_RAW_PAIR_NET
}

fn clamp_price(n: f64) -> f64 {
    ((n * 100.0).round() / 100.0).clamp(0.01, 0.99)
}

/* ------------------------------------------------------------------ *
 *  Venue-open delta tapes (guide §3)                                   *
 * ------------------------------------------------------------------ */

fn sorted_tape(series: &[(i64, f64)]) -> Vec<(i64, f64)> {
    let mut out: Vec<(i64, f64)> = series
        .iter()
        .copied()
        .filter(|&(t, px)| t > 0 && px > 0.0)
        .collect();
    out.sort_by_key(|&(t, _)| t);
    out
}

/// Venue open = last print at/before openMs, else first print after.
pub fn venue_open_price(series: &[(i64, f64)], open_ms: i64) -> Option<f64> {
    let mut found = None;
    for &(t, px) in series {
        if px <= 0.0 {
            continue;
        }
        if t > open_ms {
            if found.is_none() {
                return Some(px);
            }
            break;
        }
        found = Some(px);
    }
    found
}

/// Delta tape: anchor {openMs, 0} + one point per print inside the window.
fn delta_tape(series: &[(i64, f64)], open_px: Option<f64>, open_ms: i64, end_ms: i64) -> Vec<(i64, f64)> {
    let Some(open_px) = open_px else {
        return vec![];
    };
    let mut tape = vec![(open_ms, 0.0)];
    for &(t, px) in series {
        if t < open_ms || t > end_ms {
            continue;
        }
        tape.push((t, px - open_px));
    }
    tape
}

/* ------------------------------------------------------------------ *
 *  Zigzag major-swing detector — per venue, strike 0 (guide §4)        *
 * ------------------------------------------------------------------ */

fn detect_major_swings(
    tape: &[(i64, f64)],
    asks: &[(i64, f64, f64)],
    open_ms: i64,
    end_ms: i64,
    market_end: i64,
) -> Vec<Swing> {
    let mut grid = Vec::new();
    let mut t = open_ms + 800;
    while t < end_ms - 800 {
        if let Some(px) = px_at(tape, t) {
            grid.push((t, px));
        }
        t += GRID_MS;
    }
    if grid.len() < 8 {
        return vec![];
    }

    // Pass 1 — raw zigzag with $10 confirmation.
    let mut raw: Vec<(i64, f64, ExtKind)> = Vec::new();
    let mut hunting = if grid[0].1 >= 0.0 {
        ExtKind::Peak
    } else {
        ExtKind::Trough
    };
    let mut extreme = grid[0];
    for &g in &grid {
        match hunting {
            ExtKind::Peak => {
                if g.1 >= extreme.1 {
                    extreme = g;
                } else if extreme.1 - g.1 >= CONFIRM_USD {
                    raw.push((extreme.0, extreme.1, ExtKind::Peak));
                    hunting = ExtKind::Trough;
                    extreme = g;
                }
            }
            ExtKind::Trough => {
                if g.1 <= extreme.1 {
                    extreme = g;
                } else if g.1 - extreme.1 >= CONFIRM_USD {
                    raw.push((extreme.0, extreme.1, ExtKind::Trough));
                    hunting = ExtKind::Peak;
                    extreme = g;
                }
            }
        }
    }

    // Pass 2 — alternating majors ≥ $40 apart (with bridge rescue).
    let mut major: Vec<(i64, f64, ExtKind)> = Vec::new();
    let mut last_idx: isize = -1;
    for (i, e) in raw.iter().enumerate() {
        let prev = major.last().copied();
        if prev.is_none() {
            major.push(*e);
            last_idx = i as isize;
            continue;
        }
        let prev = prev.unwrap();
        if prev.2 == e.2 {
            if i as isize == last_idx + 1 {
                if e.2 == ExtKind::Peak && e.1 >= prev.1 {
                    *major.last_mut().unwrap() = *e;
                    last_idx = i as isize;
                } else if e.2 == ExtKind::Trough && e.1 <= prev.1 {
                    *major.last_mut().unwrap() = *e;
                    last_idx = i as isize;
                }
                continue;
            }
            let between: Vec<_> = raw[(last_idx as usize + 1)..i]
                .iter()
                .copied()
                .filter(|x| x.2 != e.2)
                .collect();
            let mut best: Option<(i64, f64, ExtKind)> = None;
            for b in between {
                best = Some(match best {
                    None => b,
                    Some(cur) if b.2 == ExtKind::Peak && b.1 >= cur.1 => b,
                    Some(cur) if b.2 == ExtKind::Trough && b.1 <= cur.1 => b,
                    Some(cur) => cur,
                });
            }
            let bridge_ok = best.is_some_and(|b| {
                (b.1 - prev.1).abs() >= MIN_SWING_USD * 0.55 && (e.1 - b.1).abs() >= MIN_SWING_USD
            });
            if bridge_ok {
                major.push(best.unwrap());
                major.push(*e);
                last_idx = i as isize;
            } else if e.2 == ExtKind::Peak && e.1 >= prev.1 {
                *major.last_mut().unwrap() = *e;
                last_idx = i as isize;
            } else if e.2 == ExtKind::Trough && e.1 <= prev.1 {
                let grand = if major.len() >= 2 {
                    Some(major[major.len() - 2])
                } else {
                    None
                };
                if grand.is_some_and(|g| (prev.1 - g.1).abs() >= MIN_SWING_USD) {
                    continue;
                }
                *major.last_mut().unwrap() = *e;
                last_idx = i as isize;
            }
            continue;
        }
        if (e.1 - prev.1).abs() >= MIN_SWING_USD {
            major.push(*e);
            last_idx = i as isize;
        }
    }

    // Pass 3 — tradability filter.
    let mut out: Vec<Swing> = Vec::new();
    for e in major {
        let side = match e.2 {
            ExtKind::Peak => PositionSide::Down,
            ExtKind::Trough => PositionSide::Up,
        };
        let Some(ask) = ask_at(asks, e.0, side) else {
            continue;
        };
        if ask < MIN_ASK {
            continue;
        }
        if market_end - e.0 < MIN_LEFT_MS {
            continue;
        }
        let last = out.last();
        if last.is_some_and(|l| l.kind == e.2) {
            continue;
        }
        let big_from_last = last.is_some_and(|l| (e.1 - l.d).abs() >= MIN_SWING_USD * 1.15);
        match e.2 {
            ExtKind::Peak => {
                if e.1 < MIN_PEAK_DELTA && !big_from_last {
                    continue;
                }
            }
            ExtKind::Trough => {
                if e.1 > MAX_TROUGH_DELTA && !big_from_last {
                    continue;
                }
            }
        }
        if last.is_some_and(|l| (e.1 - l.d).abs() < MIN_SWING_USD) {
            continue;
        }
        out.push(Swing {
            t: e.0,
            d: e.1,
            kind: e.2,
            side,
            ask,
        });
    }
    out
}

/* ------------------------------------------------------------------ *
 *  Union merge of the two venues' majors (guide §5)                    *
 * ------------------------------------------------------------------ */

/// Stable-sorted union: consecutive same-kind → keep the more extreme;
/// alternation kept only when it moved ≥ minSwingUsd from the last kept.
fn merge_union_extreme(bn_sw: Vec<Swing>, cb_sw: Vec<Swing>) -> Vec<Swing> {
    let mut all: Vec<Swing> = bn_sw;
    all.extend(cb_sw);
    all.sort_by_key(|s| s.t); // stable: BN before CB on tie (JS parity)
    let mut out: Vec<Swing> = Vec::new();
    for e in all {
        let Some(last) = out.last() else {
            out.push(e);
            continue;
        };
        if last.kind == e.kind {
            let more_extreme = match e.kind {
                ExtKind::Peak => e.d >= last.d,
                ExtKind::Trough => e.d <= last.d,
            };
            if more_extreme {
                *out.last_mut().unwrap() = e;
            }
            continue;
        }
        if (e.d - last.d).abs() >= MIN_SWING_USD {
            out.push(e);
        }
    }
    out
}

/* ------------------------------------------------------------------ *
 *  Batch oracle — mirrors strategy-venue-swing-final.js runWindow      *
 * ------------------------------------------------------------------ */

/// Replay commits in fill order to recover `nextTradeT` after the last fill.
fn next_trade_t_from_commits(commits: &[CommittedLot], open_ms: i64) -> i64 {
    if commits.is_empty() {
        return open_ms;
    }
    let mut ordered: Vec<&CommittedLot> = commits.iter().collect();
    ordered.sort_by_key(|c| c.fill_t);
    let mut sides: Vec<(PositionSide, f64)> = Vec::new();
    let mut pairs_done: u32 = 0;
    let mut next_t = open_ms;
    for c in ordered {
        sides.push((c.side, c.shares));
        let before = pairs_done;
        loop {
            let up_i = sides
                .iter()
                .position(|(s, q)| *s == PositionSide::Up && *q > 1e-12);
            let dn_i = sides
                .iter()
                .position(|(s, q)| *s == PositionSide::Down && *q > 1e-12);
            let (Some(ui), Some(di)) = (up_i, dn_i) else {
                break;
            };
            let q = sides[ui].1.min(sides[di].1);
            if q <= 0.0 {
                break;
            }
            sides[ui].1 -= q;
            sides[di].1 -= q;
            pairs_done += 1;
        }
        next_t = if pairs_done > before {
            c.fill_t + PAIR_COOLDOWN_MS
        } else {
            c.fill_t + FIRST_COOLDOWN_MS
        };
    }
    next_t
}

#[allow(clippy::too_many_arguments)]
pub fn run_window(
    open_ms: i64,
    end_ms: i64,
    binance: &[(i64, f64)],
    coinbase: &[(i64, f64)],
    asks: &[(i64, f64, f64)],
    winner: Option<PositionSide>,
    shares: f64,
    market_end_ms: Option<i64>,
    taker_delay_ms: i64,
) -> WindowResult {
    run_window_with_commits(
        open_ms,
        end_ms,
        binance,
        coinbase,
        asks,
        winner,
        shares,
        market_end_ms,
        taker_delay_ms,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_window_with_commits(
    open_ms: i64,
    end_ms: i64,
    binance: &[(i64, f64)],
    coinbase: &[(i64, f64)],
    asks: &[(i64, f64, f64)],
    winner: Option<PositionSide>,
    shares: f64,
    market_end_ms: Option<i64>,
    taker_delay_ms: i64,
    commits: &[CommittedLot],
) -> WindowResult {
    let mut out = WindowResult::default();
    if asks.is_empty() {
        return out;
    }
    let market_end = market_end_ms.unwrap_or(end_ms);

    let bn = sorted_tape(binance);
    let cb = sorted_tape(coinbase);
    let bn_open = venue_open_price(&bn, open_ms);
    let cb_open = venue_open_price(&cb, open_ms);
    let bn_tape = delta_tape(&bn, bn_open, open_ms, end_ms);
    let cb_tape = delta_tape(&cb, cb_open, open_ms, end_ms);

    let bn_sw = if bn_tape.is_empty() {
        vec![]
    } else {
        detect_major_swings(&bn_tape, asks, open_ms, end_ms, market_end)
    };
    let cb_sw = if cb_tape.is_empty() {
        vec![]
    } else {
        detect_major_swings(&cb_tape, asks, open_ms, end_ms, market_end)
    };
    let swings = merge_union_extreme(bn_sw, cb_sw);

    // Interleaved tape: early-opposite dumps + rapid-fall guard (guide §6.4).
    let mut tape: Vec<(i64, f64)> = bn_tape;
    tape.extend(cb_tape);
    tape.sort_by_key(|&(t, _)| t);
    if tape.is_empty() {
        return out;
    }

    let last_ask = asks.last().copied();
    let mut lots: Vec<Lot> = commits
        .iter()
        .map(|c| Lot {
            side: c.side,
            shares: c.shares,
            fill: c.fill,
            fee: taker_fee_usdc(c.shares, c.fill),
            paired: c.paired,
            anchor_d: c.anchor_d,
        })
        .collect();
    let mut pairs_done: u32 = 0;
    {
        let up_p: f64 = commits
            .iter()
            .filter(|c| c.side == PositionSide::Up)
            .map(|c| c.paired)
            .sum();
        let dn_p: f64 = commits
            .iter()
            .filter(|c| c.side == PositionSide::Down)
            .map(|c| c.paired)
            .sum();
        if shares > 0.0 {
            pairs_done = (up_p.min(dn_p) / shares).floor() as u32;
        }
    }
    let mut missed: u32 = 0;
    let mut next_trade_t = next_trade_t_from_commits(commits, open_ms);
    let mut anchor: Option<(i64, f64, PositionSide, ExtKind)> = commits
        .iter()
        .rev()
        .find(|c| c.shares - c.paired > 1e-12)
        .map(|c| {
            (
                c.fill_t,
                c.anchor_d,
                c.side,
                if c.from_trough {
                    ExtKind::Trough
                } else {
                    ExtKind::Peak
                },
            )
        });

    let unpaired_lots = |lots: &Vec<Lot>, side: PositionSide| -> Vec<usize> {
        lots.iter()
            .enumerate()
            .filter(|(_, l)| l.side == side && l.shares - l.paired > 1e-12)
            .map(|(i, _)| i)
            .collect()
    };
    let unpaired_shares =
        |lots: &Vec<Lot>| -> f64 { lots.iter().map(|l| (l.shares - l.paired).max(0.0)).sum() };

    let pair_lots = |lots: &mut Vec<Lot>, trades: &mut Vec<SimTrade>, t: i64, pairs: &mut u32| {
        loop {
            let up_i = unpaired_lots(lots, PositionSide::Up).into_iter().next();
            let dn_i = unpaired_lots(lots, PositionSide::Down).into_iter().next();
            let (Some(ui), Some(di)) = (up_i, dn_i) else {
                break;
            };
            let q = (lots[ui].shares - lots[ui].paired).min(lots[di].shares - lots[di].paired);
            if q <= 0.0 {
                break;
            }
            lots[ui].paired += q;
            lots[di].paired += q;
            let fee_share_amt =
                (lots[ui].fee * q) / lots[ui].shares + (lots[di].fee * q) / lots[di].shares;
            let gross = q * (1.0 - lots[ui].fill - lots[di].fill);
            let net = gross - fee_share_amt;
            *pairs += 1;
            trades.push(SimTrade {
                kind: "PAIR",
                side: PositionSide::Up,
                t,
                fill: lots[ui].fill + lots[di].fill,
                shares: q,
                reason: "pair".into(),
                net,
                anchor_d: 0.0,
            });
        }
    };

    let try_fill = |asks: &[(i64, f64, f64)], t: i64, side: PositionSide| -> Option<(i64, f64)> {
        let fill_t = t + taker_delay_ms;
        // Live truncated horizon: never fill in the future with a present ask.
        if fill_t > end_ms {
            return None;
        }
        if fill_t >= market_end - 600 {
            return None;
        }
        let fill = ask_at(asks, fill_t, side)?;
        if fill < MIN_ASK || fill > 0.95 {
            return None;
        }
        Some((fill_t, fill))
    };

    let mut add_lot = |lots: &mut Vec<Lot>,
                       trades: &mut Vec<SimTrade>,
                       pairs: &mut u32,
                       next_t: &mut i64,
                       anchor_ref: &mut Option<(i64, f64, PositionSide, ExtKind)>,
                       side: PositionSide,
                       fill_t: i64,
                       fill: f64,
                       ext_kind: ExtKind,
                       is_flip: bool,
                       tag: &str,
                       anchor_d: f64| {
        let fee = taker_fee_usdc(shares, fill);
        lots.push(Lot {
            side,
            shares,
            fill,
            fee,
            paired: 0.0,
            anchor_d,
        });
        trades.push(SimTrade {
            kind: "BUY",
            side,
            t: fill_t,
            fill,
            shares,
            reason: tag.into(),
            net: 0.0,
            anchor_d,
        });
        let before = *pairs;
        pair_lots(lots, trades, fill_t, pairs);
        *next_t = if *pairs > before {
            fill_t + PAIR_COOLDOWN_MS
        } else {
            fill_t + FIRST_COOLDOWN_MS
        };
        if !is_flip {
            *anchor_ref = Some((fill_t, anchor_d, side, ext_kind));
        } else {
            *anchor_ref = None;
        }
    };

    let try_second_leg = |lots: &mut Vec<Lot>,
                          trades: &mut Vec<SimTrade>,
                          pairs: &mut u32,
                          next_t: &mut i64,
                          anchor_ref: &mut Option<(i64, f64, PositionSide, ExtKind)>,
                          missed: &mut u32,
                          t: i64,
                          side: PositionSide,
                          ext_kind: ExtKind,
                          tag: &str|
     -> bool {
        let need = match side {
            PositionSide::Up => PositionSide::Down,
            PositionSide::Down => PositionSide::Up,
        };
        let Some(first_i) = unpaired_lots(lots, need).into_iter().next() else {
            return false;
        };
        let first_fill = lots[first_i].fill;
        let first_anchor = lots[first_i].anchor_d;
        let Some(ask) = ask_at(asks, t, side) else {
            return false;
        };
        let max_ask = if tag.starts_with("EARLY_") {
            MAX_EARLY_OPP_ASK
        } else {
            MAX_SECOND_ASK
        };
        if ask < MIN_ASK || ask > max_ask {
            return false;
        }
        if !net_gap_ok(first_fill, ask) {
            return false;
        }
        let Some((fill_t, fill)) = try_fill(asks, t, side) else {
            *missed += 1;
            return false;
        };
        if !net_gap_ok(first_fill, fill) {
            *missed += 1;
            return false;
        }
        add_lot(
            lots, trades, pairs, next_t, anchor_ref, side, fill_t, fill, ext_kind, true, tag,
            first_anchor,
        );
        true
    };

    // Event stream: merged swings + interleaved-tape ticks (guide §6).
    #[derive(Clone)]
    enum Ev {
        Swing(Swing),
        Tick { t: i64, d: f64 },
    }
    let mut events: Vec<Ev> = swings.into_iter().map(Ev::Swing).collect();
    let mut t = open_ms + 1000;
    let tick_until = end_ms.min(market_end - MIN_LEFT_MS);
    while t < tick_until {
        if let Some(px) = px_at(&tape, t) {
            events.push(Ev::Tick { t, d: px });
        }
        t += TICK_MS;
    }
    events.sort_by(|a, b| {
        let ta = match a {
            Ev::Swing(s) => s.t,
            Ev::Tick { t, .. } => *t,
        };
        let tb = match b {
            Ev::Swing(s) => s.t,
            Ev::Tick { t, .. } => *t,
        };
        ta.cmp(&tb).then_with(|| match (a, b) {
            (Ev::Swing(_), Ev::Tick { .. }) => std::cmp::Ordering::Less,
            (Ev::Tick { .. }, Ev::Swing(_)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        })
    });

    for ev in events {
        if pairs_done >= MAX_PAIRS && unpaired_shares(&lots) <= 0.0 {
            break;
        }
        match ev {
            Ev::Tick { t, d } => {
                if unpaired_shares(&lots) <= 0.0 || anchor.is_none() {
                    continue;
                }
                if t < next_trade_t {
                    continue;
                }
                if let Some((_, ad, side, kind)) = anchor {
                    if kind == ExtKind::Peak
                        && side == PositionSide::Down
                        && d <= ad - EARLY_OPP_SWING_USD
                    {
                        try_second_leg(
                            &mut lots,
                            &mut out.trades,
                            &mut pairs_done,
                            &mut next_trade_t,
                            &mut anchor,
                            &mut missed,
                            t,
                            PositionSide::Up,
                            ExtKind::Trough,
                            "EARLY_OPP",
                        );
                    }
                }
            }
            Ev::Swing(sw) => {
                if sw.t < next_trade_t {
                    continue;
                }
                let need = match sw.side {
                    PositionSide::Up => PositionSide::Down,
                    PositionSide::Down => PositionSide::Up,
                };
                if !unpaired_lots(&lots, need).is_empty() {
                    try_second_leg(
                        &mut lots,
                        &mut out.trades,
                        &mut pairs_done,
                        &mut next_trade_t,
                        &mut anchor,
                        &mut missed,
                        sw.t,
                        sw.side,
                        sw.kind,
                        &format!(
                            "SWING_{}_2",
                            match sw.kind {
                                ExtKind::Peak => "PEAK",
                                ExtKind::Trough => "TROUGH",
                            }
                        ),
                    );
                    continue;
                }
                if unpaired_shares(&lots) > 0.0 {
                    continue;
                }
                // First leg (guide §6.2).
                if market_end - sw.t < MIN_LEFT_MS {
                    continue;
                }
                if pairs_done > 0 && market_end - sw.t < RESTART_MIN_LEFT_MS {
                    continue;
                }
                if sw.ask > MAX_CHEAP_ASK || sw.ask < MIN_FIRST_ASK {
                    continue;
                }
                if sw.kind == ExtKind::Trough && sw.side == PositionSide::Up {
                    if sw.d < MIN_UP_FIRST_DELTA && sw.ask > 0.2 {
                        continue;
                    }
                    if let Some(drop) = delta_drop_over(&tape, sw.t, RAPID_FALL_MS) {
                        if drop >= RAPID_FALL_USD && sw.d < 0.0 && sw.ask > 0.2 {
                            continue;
                        }
                    }
                }
                let Some((fill_t, fill)) = try_fill(asks, sw.t, sw.side) else {
                    missed += 1;
                    continue;
                };
                if sw.kind == ExtKind::Trough && sw.side == PositionSide::Up {
                    if let Some(fill_d) = px_at(&tape, fill_t) {
                        if fill_d < MIN_UP_FIRST_DELTA && fill > 0.2 {
                            missed += 1;
                            continue;
                        }
                    }
                }
                if fill > MAX_CHEAP_ASK + 0.04 {
                    missed += 1;
                    continue;
                }
                add_lot(
                    &mut lots,
                    &mut out.trades,
                    &mut pairs_done,
                    &mut next_trade_t,
                    &mut anchor,
                    sw.side,
                    fill_t,
                    fill,
                    sw.kind,
                    false,
                    &format!(
                        "SWING_{}",
                        match sw.kind {
                            ExtKind::Peak => "PEAK",
                            ExtKind::Trough => "TROUGH",
                        }
                    ),
                    sw.d,
                );
            }
        }
    }

    // Forced flatten at T−25s (guide §6.5).
    let last_t = market_end - EMERGENCY_LEFT_MS;
    if unpaired_shares(&lots) > 0.0 && last_t > open_ms && end_ms >= last_t {
        let fill_t = last_t + taker_delay_ms;
        if fill_t < market_end - 400 {
            for side in [PositionSide::Up, PositionSide::Down] {
                let idxs = unpaired_lots(&lots, side);
                if idxs.is_empty() {
                    continue;
                }
                let Some(ask) = ask_at(asks, fill_t, side) else {
                    continue;
                };
                if ask <= 0.0 {
                    continue;
                }
                let px = (ask - SELL_HAIRCUT).max(0.01);
                for i in idxs {
                    let q = lots[i].shares - lots[i].paired;
                    if q <= 0.0 {
                        continue;
                    }
                    let fee_s = taker_fee_usdc(q, px);
                    let fee_buy = (lots[i].fee * q) / lots[i].shares;
                    lots[i].paired = lots[i].shares;
                    out.trades.push(SimTrade {
                        kind: "SELL",
                        side,
                        t: fill_t,
                        fill: px,
                        shares: q,
                        reason: "flatten".into(),
                        net: q * px - (q * lots[i].fill + fee_buy) - fee_s,
                        anchor_d: 0.0,
                    });
                }
            }
        }
    }

    // Settle residue at official PTB winner (guide §6.6).
    for lot in &mut lots {
        let q = lot.shares - lot.paired;
        if q <= 0.0 {
            continue;
        }
        let mark = match winner {
            Some(PositionSide::Up) => {
                if lot.side == PositionSide::Up {
                    1.0
                } else {
                    0.0
                }
            }
            Some(PositionSide::Down) => {
                if lot.side == PositionSide::Down {
                    1.0
                } else {
                    0.0
                }
            }
            None => match lot.side {
                PositionSide::Up => last_ask.map(|(_, u, _)| u).unwrap_or(lot.fill),
                PositionSide::Down => last_ask.map(|(_, _, d)| d).unwrap_or(lot.fill),
            },
        };
        let fee_buy = (lot.fee * q) / lot.shares;
        lot.paired = lot.shares;
        out.trades.push(SimTrade {
            kind: "SETTLE",
            side: lot.side,
            t: end_ms - 1,
            fill: lot.fill,
            shares: q,
            reason: "settle".into(),
            net: q * mark - (q * lot.fill + fee_buy),
            anchor_d: 0.0,
        });
    }

    out.pairs = pairs_done;
    out.missed = missed;
    out.total_pnl = out.trades.iter().map(|t| t.net).sum();
    out
}

/* ------------------------------------------------------------------ *
 *  Live/dry engine (guide §7): buffers ticks, re-sims with committed   *
 *  fills, emits only NEW intents. Same proven harness as gap-swing.    *
 * ------------------------------------------------------------------ */

pub struct VenueSwingEngine {
    open_ms: i64,
    end_ms: i64,
    /// Official PTB — winner resolution ONLY. Never gates trading.
    ptb: Option<f64>,
    bn: Vec<(i64, f64)>,
    cb: Vec<(i64, f64)>,
    asks: Vec<(i64, f64, f64)>,
    emitted: HashSet<String>,
    pending: Vec<BotAction>,
    commits: Vec<CommittedLot>,
    shares: f64,
    /// 70 dry/backtest; **0** live (post immediately at signal).
    taker_delay_ms: i64,
    last_pnl: f64,
    last_pairs: u32,
}

impl VenueSwingEngine {
    pub fn new(open_ms: i64, end_ms: i64) -> Self {
        Self {
            open_ms,
            end_ms,
            ptb: None,
            bn: vec![],
            cb: vec![],
            asks: vec![],
            emitted: HashSet::new(),
            pending: vec![],
            commits: vec![],
            shares: 10.0,
            taker_delay_ms: TAKER_DELAY_MS,
            last_pnl: 0.0,
            last_pairs: 0,
        }
    }

    /// Keep pre-open prints (venue-open source) plus in-window ticks.
    pub fn reset_window(&mut self, open_ms: i64, end_ms: i64) {
        let shares = self.shares;
        let delay = self.taker_delay_ms;
        let keep = |v: &Vec<(i64, f64)>| -> Vec<(i64, f64)> {
            v.iter()
                .copied()
                .filter(|(t, _)| *t >= open_ms - 60_000 && *t < end_ms)
                .collect()
        };
        let bn = keep(&self.bn);
        let cb = keep(&self.cb);
        let asks: Vec<_> = self
            .asks
            .iter()
            .copied()
            .filter(|(t, _, _)| *t >= open_ms - 60_000 && *t < end_ms)
            .collect();
        *self = Self::new(open_ms, end_ms);
        self.shares = shares;
        self.taker_delay_ms = delay;
        self.bn = bn;
        self.cb = cb;
        self.asks = asks;
    }

    pub fn set_shares(&mut self, shares: f64) {
        self.shares = shares;
    }

    /// Live trading: 0 (immediate). Dry/backtest: 70.
    pub fn set_taker_delay_ms(&mut self, delay_ms: i64) {
        self.taker_delay_ms = delay_ms.max(0);
    }

    /// One-shot official PTB — winner resolution only (signal never uses it).
    pub fn set_strike(&mut self, ptb: f64) -> bool {
        if self.ptb.is_some() {
            return false;
        }
        if ptb > 0.0 {
            self.ptb = Some(ptb);
            return true;
        }
        false
    }

    pub fn strike(&self) -> Option<f64> {
        self.ptb
    }

    pub fn last_sim_pnl(&self) -> f64 {
        self.last_pnl
    }

    pub fn last_pairs(&self) -> u32 {
        self.last_pairs
    }

    pub fn unpaired_shares(&self) -> f64 {
        self.commits
            .iter()
            .map(|c| (c.shares - c.paired).max(0.0))
            .sum()
    }

    pub fn unpaired_on(&self, side: PositionSide) -> f64 {
        self.commits
            .iter()
            .filter(|c| c.side == side)
            .map(|c| (c.shares - c.paired).max(0.0))
            .sum()
    }

    pub fn fill_count(&self) -> usize {
        self.commits.len()
    }

    pub fn has_fills(&self) -> bool {
        !self.commits.is_empty()
    }

    /// (Binance − BN open, Coinbase − CB open) — logging.
    pub fn venue_deltas(&self) -> (Option<f64>, Option<f64>) {
        let bn = sorted_tape(&self.bn);
        let cb = sorted_tape(&self.cb);
        let d = |tape: &[(i64, f64)]| -> Option<f64> {
            let open = venue_open_price(tape, self.open_ms)?;
            tape.last().map(|&(_, px)| px - open)
        };
        (d(&bn), d(&cb))
    }

    /// Interleaved venue-open delta at t (anchor recovery for EARLY_OPP).
    fn venue_delta_at(&self, t: i64) -> Option<f64> {
        let t = t.max(self.open_ms);
        let bn = sorted_tape(&self.bn);
        let cb = sorted_tape(&self.cb);
        let bn_open = venue_open_price(&bn, self.open_ms);
        let cb_open = venue_open_price(&cb, self.open_ms);
        let last_at = |tape: &[(i64, f64)]| -> Option<(i64, f64)> {
            let mut found = None;
            for &(pt, px) in tape {
                if pt > t {
                    break;
                }
                found = Some((pt, px));
            }
            found
        };
        match (last_at(&bn), last_at(&cb)) {
            (Some((bt, bp)), Some((ct, cp))) => {
                if bt >= ct {
                    bn_open.map(|o| bp - o)
                } else {
                    cb_open.map(|o| cp - o)
                }
            }
            (Some((_, bp)), None) => bn_open.map(|o| bp - o),
            (None, Some((_, cp))) => cb_open.map(|o| cp - o),
            (None, None) => None,
        }
    }

    /// After a live/dry buy fill — lock inventory for re-sims.
    pub fn commit_buy(
        &mut self,
        side: PositionSide,
        shares: f64,
        fill: f64,
        fill_t: i64,
        is_pair: bool,
        anchor_d: f64,
    ) {
        let anchor_d = if anchor_d.abs() > 1e-6 {
            anchor_d
        } else {
            let sig_t = (fill_t - self.taker_delay_ms).max(self.open_ms);
            self.venue_delta_at(sig_t)
                .or_else(|| self.venue_delta_at(fill_t))
                .unwrap_or(0.0)
        };
        self.commits.push(CommittedLot {
            side,
            shares,
            fill,
            fill_t,
            paired: 0.0,
            anchor_d,
            from_trough: side == PositionSide::Up,
        });
        if is_pair {
            self.pair_commits();
        }
    }

    /// After flatten sell fill — mark side as closed.
    pub fn commit_sell(&mut self, side: PositionSide) {
        for c in &mut self.commits {
            if c.side == side {
                c.paired = c.shares;
            }
        }
    }

    fn pair_commits(&mut self) {
        loop {
            let up = self
                .commits
                .iter()
                .position(|c| c.side == PositionSide::Up && c.shares - c.paired > 1e-12);
            let dn = self
                .commits
                .iter()
                .position(|c| c.side == PositionSide::Down && c.shares - c.paired > 1e-12);
            let (Some(ui), Some(di)) = (up, dn) else {
                break;
            };
            let q = (self.commits[ui].shares - self.commits[ui].paired)
                .min(self.commits[di].shares - self.commits[di].paired);
            if q <= 0.0 {
                break;
            }
            self.commits[ui].paired += q;
            self.commits[di].paired += q;
        }
    }

    pub fn on_binance(&mut self, t: i64, price: f64) -> Vec<BotAction> {
        if !(price > 0.0) {
            return vec![];
        }
        if self
            .bn
            .last()
            .is_some_and(|(lt, lp)| *lt == t && (*lp - price).abs() < 1e-9)
        {
            return vec![];
        }
        self.bn.push((t, price));
        self.refresh(t);
        self.take_pending()
    }

    pub fn on_coinbase(&mut self, t: i64, price: f64) -> Vec<BotAction> {
        if !(price > 0.0) {
            return vec![];
        }
        if self
            .cb
            .last()
            .is_some_and(|(lt, lp)| *lt == t && (*lp - price).abs() < 1e-9)
        {
            return vec![];
        }
        self.cb.push((t, price));
        self.refresh(t);
        self.take_pending()
    }

    pub fn on_asks(&mut self, t: i64, up: f64, down: f64) {
        if up > 0.0 && down > 0.0 {
            if self.asks.last().is_some_and(|(lt, _, _)| *lt == t) {
                return;
            }
            self.asks.push((t, up, down));
        }
    }

    pub fn tick(&mut self, now_ms: i64, up_ask: f64, down_ask: f64) -> Vec<BotAction> {
        self.on_asks(now_ms, up_ask, down_ask);
        self.refresh(now_ms);
        self.take_pending()
    }

    /// Final window close — settle PnL with the official PTB winner.
    pub fn finalize(&mut self, winner: Option<PositionSide>) -> WindowResult {
        let res = run_window_with_commits(
            self.open_ms,
            self.end_ms,
            &self.bn,
            &self.cb,
            &self.asks,
            winner,
            self.shares,
            Some(self.end_ms),
            self.taker_delay_ms,
            &self.commits,
        );
        self.last_pnl = res.total_pnl;
        self.last_pairs = res.pairs;
        res
    }

    fn refresh(&mut self, now_ms: i64) {
        let end = self.end_ms.min(now_ms + 1);
        let res = run_window_with_commits(
            self.open_ms,
            end,
            &self.bn,
            &self.cb,
            &self.asks,
            None,
            self.shares,
            Some(self.end_ms),
            self.taker_delay_ms,
            &self.commits,
        );
        self.last_pnl = res.total_pnl;
        self.last_pairs = res.pairs;

        for tr in res.trades {
            if tr.kind != "BUY" && tr.kind != "SELL" {
                continue;
            }
            if tr.t > now_ms {
                continue;
            }
            let signal_t = tr.t - self.taker_delay_ms;
            let second = tr.reason.contains("_2")
                || tr.reason.starts_with("EARLY_")
                || tr.reason.starts_with("EMERGENCY");

            // Inventory checks BEFORE burning emitted keys.
            if tr.kind == "BUY" && !second && self.unpaired_shares() > 1e-12 {
                continue;
            }
            if tr.kind == "BUY"
                && second
                && self.unpaired_on(match tr.side {
                    PositionSide::Up => PositionSide::Down,
                    PositionSide::Down => PositionSide::Up,
                }) <= 1e-12
            {
                continue;
            }
            if tr.kind == "SELL" && self.unpaired_on(tr.side) <= 1e-12 {
                continue;
            }

            let key = if tr.reason.starts_with("EARLY_") || tr.reason.starts_with("EMERGENCY") {
                format!("{}:{:?}:{}", tr.kind, tr.side, tr.reason)
            } else {
                format!("{}:{:?}:{}:{}", tr.kind, tr.side, tr.t, tr.reason)
            };
            if !self.emitted.insert(key) {
                continue;
            }

            // Optimistic commit NOW — fills/ACKs lag the feed drain.
            if tr.kind == "BUY" {
                self.commit_buy(tr.side, tr.shares, tr.fill, tr.t, second, tr.anchor_d);
            } else if tr.kind == "SELL" {
                self.commit_sell(tr.side);
            }

            let worst = clamp_price(tr.fill + 0.02);
            let action = if tr.kind == "SELL" {
                BotAction::SellFlatten {
                    side: tr.side,
                    signal_t,
                    worst_ask: worst,
                    reason: tr.reason,
                }
            } else if second {
                BotAction::BuyPair {
                    side: tr.side,
                    signal_t,
                    worst_ask: worst,
                    reason: tr.reason,
                }
            } else {
                BotAction::BuyEntry {
                    side: tr.side,
                    signal_t,
                    worst_ask: worst,
                    reason: tr.reason,
                }
            };
            info!(
                "venue-swing intent {} reason={}",
                match &action {
                    BotAction::BuyEntry { side, .. } => format!("ENTRY {side:?}"),
                    BotAction::BuyPair { side, .. } => format!("PAIR {side:?}"),
                    BotAction::SellFlatten { side, .. } => format!("SELL {side:?}"),
                },
                match &action {
                    BotAction::BuyEntry { reason, .. }
                    | BotAction::BuyPair { reason, .. }
                    | BotAction::SellFlatten { reason, .. } => reason.as_str(),
                }
            );
            self.pending.push(action);
        }
    }

    pub fn take_pending(&mut self) -> Vec<BotAction> {
        std::mem::take(&mut self.pending)
    }
}
