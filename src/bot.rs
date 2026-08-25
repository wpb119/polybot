use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::config::{Config, StrategyKind};
use crate::clob::OrderClient;
use crate::feeds::{spawn_binance, spawn_coinbase, spawn_polymarket, BtcQuote, PolyQuote};
use crate::gamma::{resolve_btc5m_market, MarketInfo};
use crate::pnl::{resolve_winner, PnlTracker};
use crate::poly_book::{
    PolyBook, subscribe_window_starts, trading_window_start, window_end_ms, window_open_ms,
    TAKER_DELAY_MS,
};
use crate::strategy::{
    BotAction, CaptureSignal, GapSwingEngine, PairingEngine, PositionSide, TrendDetector,
};

#[derive(Clone, Copy, Debug)]
enum FillKind {
    Entry,
    Pair,
    Flatten,
}

struct PendingDryFill {
    send_t: i64,
    start_ts: i64,
    side: PositionSide,
    kind: FillKind,
    worst_ask: f64,
    reason: String,
}

enum ActiveEngine {
    Pairing {
        detector: TrendDetector,
        engine: PairingEngine,
    },
    GapSwing {
        engine: GapSwingEngine,
    },
}

pub struct Bot {
    cfg: Config,
    orders: OrderClient,
    subs_tx: watch::Sender<Vec<MarketInfo>>,
    bn_rx: watch::Receiver<Option<BtcQuote>>,
    cb_rx: watch::Receiver<Option<BtcQuote>>,
    poly_rx: watch::Receiver<Option<PolyQuote>>,
    market_cache: HashMap<i64, MarketInfo>,
    subscribed_starts: Vec<i64>,
    trading_start_ts: Option<i64>,
    trading_market: Option<MarketInfo>,
    active: ActiveEngine,
    poly_book: PolyBook,
    pnl: PnlTracker,
    pending_dry: Vec<PendingDryFill>,
}

impl Bot {
    pub fn new(cfg: Config) -> Result<Self> {
        let orders = OrderClient::new(&cfg)?;
        let (subs_tx, subs_rx) = watch::channel(vec![]);
        let bn_rx = spawn_binance();
        let cb_rx = spawn_coinbase();
        let poly_rx = spawn_polymarket(subs_rx);

        let now = now_ms();
        let trade_start = trading_window_start(now);
        let open_ms = window_open_ms(trade_start);
        let end_ms = window_end_ms(trade_start);

        let active = match cfg.strategy {
            StrategyKind::Pairing => ActiveEngine::Pairing {
                detector: TrendDetector::new(open_ms, end_ms, None),
                engine: PairingEngine::new(),
            },
            StrategyKind::GapSwing => {
                let mut engine = GapSwingEngine::new(open_ms, end_ms, None);
                // Live: post immediately (delay 0). Dry: model 250ms taker lag.
                engine.set_taker_delay_ms(if cfg.live_trading { 0 } else { TAKER_DELAY_MS });
                engine.set_shares(cfg.order_shares);
                ActiveEngine::GapSwing { engine }
            }
        };

        Ok(Self {
            cfg,
            orders,
            subs_tx,
            bn_rx,
            cb_rx,
            poly_rx,
            market_cache: HashMap::new(),
            subscribed_starts: vec![],
            trading_start_ts: None,
            trading_market: None,
            active,
            poly_book: PolyBook::new(),
            pnl: PnlTracker::new(),
            pending_dry: vec![],
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            "polybot starting strategy={} live_trading={} shares={} pre_subscribe=5s taker_delay={}ms",
            self.cfg.strategy.label(),
            self.cfg.live_trading,
            self.cfg.order_shares,
            if self.cfg.live_trading {
                0
            } else {
                TAKER_DELAY_MS
            }
        );
        if let Err(e) = self.orders.init_live(&self.cfg).await {
            error!("CLOB init: {:#}", e);
            if self.cfg.live_trading {
                return Err(e);
            }
        }

        let mut interval = tokio::time::interval(Duration::from_millis(50));
        loop {
            interval.tick().await;
            let now = now_ms();

            if let Err(e) = self.sync_subscriptions(now).await {
                error!("sync subscriptions: {:#}", e);
            }

            let trade_start = trading_window_start(now);
            if self.trading_start_ts != Some(trade_start) {
                if let Some(old) = self.trading_start_ts {
                    self.finalize_window(old, now).await;
                }
                if let Err(e) = self.start_trading_window(trade_start).await {
                    error!("start trading window: {:#}", e);
                }
            }

            let poly = self.poly_rx.borrow().clone();
            if let Some(p) = poly {
                if self.subscribed_starts.contains(&p.start_ts) {
                    self.poly_book.push(p.start_ts, p.t, p.up_ask, p.down_ask);
                }
            }

            self.process_pending_dry(now);

            if self.can_trade(now) {
                self.drive_strategy(now).await;
            }
        }
    }

    async fn drive_strategy(&mut self, now: i64) {
        match self.cfg.strategy {
            StrategyKind::Pairing => self.drive_pairing(now).await,
            StrategyKind::GapSwing => self.drive_gap_swing(now).await,
        }
    }

    async fn drive_pairing(&mut self, now: i64) {
        if let Some(m) = self.trading_market.clone() {
            if let Some(quotes) = self.poly_book.latest(m.start_ts) {
                if let ActiveEngine::Pairing { detector, .. } = &mut self.active {
                    detector.push_poly(quotes.t, quotes.up_ask, quotes.down_ask);
                }
            }
        }

        let bn_q = self.bn_rx.borrow().clone();
        if let Some(q) = bn_q {
            let sig = match &mut self.active {
                ActiveEngine::Pairing { detector, .. } => detector.on_bn(q.t, q.price),
                _ => None,
            };
            if let Some(sig) = sig {
                self.handle_pairing_signal(&sig, now).await;
            }
        }
        let cb_q = self.cb_rx.borrow().clone();
        if let Some(q) = cb_q {
            let sig = match &mut self.active {
                ActiveEngine::Pairing { detector, .. } => detector.on_cb(q.t, q.price),
                _ => None,
            };
            if let Some(sig) = sig {
                self.handle_pairing_signal(&sig, now).await;
            }
        }

        if let Some(m) = self.trading_market.clone() {
            if let Some(quotes) = self.poly_book.latest(m.start_ts) {
                let action = match &mut self.active {
                    ActiveEngine::Pairing { engine, .. } => {
                        engine.tick(now, quotes.up_ask, quotes.down_ask, m.end_ts * 1000)
                    }
                    _ => None,
                };
                if let Some(action) = action {
                    self.execute_action(&action, &m, now).await;
                }
            }
        }
    }

    async fn drive_gap_swing(&mut self, now: i64) {
        if let Some(m) = self.trading_market.clone() {
            if let Some(quotes) = self.poly_book.latest(m.start_ts) {
                if let ActiveEngine::GapSwing { engine } = &mut self.active {
                    engine.on_asks(quotes.t, quotes.up_ask, quotes.down_ask);
                }
            }
        }

        let bn_q = self.bn_rx.borrow().clone();
        if let Some(q) = bn_q {
            let actions = match &mut self.active {
                ActiveEngine::GapSwing { engine } => engine.on_binance(q.t, q.price),
                _ => vec![],
            };
            if let Some(m) = self.trading_market.clone() {
                for action in actions {
                    self.execute_action(&action, &m, now).await;
                }
            }
        }
        let cb_q = self.cb_rx.borrow().clone();
        if let Some(q) = cb_q {
            if let ActiveEngine::GapSwing { engine } = &mut self.active {
                let _ = engine.on_coinbase(q.t, q.price);
            }
        }

        if let Some(m) = self.trading_market.clone() {
            if let Some(quotes) = self.poly_book.latest(m.start_ts) {
                let actions = match &mut self.active {
                    ActiveEngine::GapSwing { engine } => {
                        engine.tick(now, quotes.up_ask, quotes.down_ask)
                    }
                    _ => vec![],
                };
                for action in actions {
                    self.execute_action(&action, &m, now).await;
                }
            }
        }
    }

    fn can_trade(&self, now: i64) -> bool {
        self.trading_market
            .as_ref()
            .is_some_and(|m| now >= window_open_ms(m.start_ts))
    }

    async fn sync_subscriptions(&mut self, now: i64) -> Result<()> {
        let desired = subscribe_window_starts(now);
        if desired == self.subscribed_starts {
            return Ok(());
        }

        for start in &desired {
            if !self.market_cache.contains_key(start) {
                let market = resolve_btc5m_market(*start).await?;
                info!(
                    "resolved {} (subscribe; trading opens at {})",
                    market.slug,
                    window_open_ms(market.start_ts)
                );
                self.market_cache.insert(*start, market);
            }
        }

        let markets: Vec<MarketInfo> = desired
            .iter()
            .map(|s| self.market_cache[s].clone())
            .collect();

        let new_tokens = market_token_ids(&markets);
        let old_markets: Vec<MarketInfo> = self
            .subscribed_starts
            .iter()
            .filter_map(|s| self.market_cache.get(s).cloned())
            .collect();
        let old_tokens = market_token_ids(&old_markets);

        let labels: Vec<&str> = markets.iter().map(|m| m.slug.as_str()).collect();

        // Next window was pre-subscribed 5s early — at UTC open only drop expired tokens.
        if !old_tokens.is_empty() && new_tokens.is_subset(&old_tokens) && new_tokens != old_tokens {
            info!(
                "window rolled — keep polymarket ws for {:?} (expired window unsubscribed logically)",
                labels
            );
            self.subscribed_starts = desired;
            return Ok(());
        }

        if markets.len() == 2 {
            info!("polymarket pre-subscribe (5s before next open) → {:?}", labels);
        } else {
            info!("polymarket subscribe → {:?}", labels);
        }

        self.subscribed_starts = desired.clone();
        self.subs_tx.send_replace(markets);

        if self.cfg.live_trading {
            for m in self.market_cache.values() {
                if desired.contains(&m.start_ts) {
                    if let Err(e) = self.orders.prepare_market(m).await {
                        warn!("presign {}: {:#}", m.slug, e);
                    }
                }
            }
        }
        Ok(())
    }

    async fn start_trading_window(&mut self, start_ts: i64) -> Result<()> {
        if !self.market_cache.contains_key(&start_ts) {
            let market = resolve_btc5m_market(start_ts).await?;
            self.market_cache.insert(start_ts, market);
        }
        let market = self.market_cache[&start_ts].clone();
        info!(
            "trading window {} ({}) strategy={}",
            market.slug,
            market.title,
            self.cfg.strategy.label()
        );

        let open_ms = window_open_ms(market.start_ts);
        let end_ms = window_end_ms(market.start_ts);
        let cb = self.cb_rx.borrow().clone().map(|q| q.price);
        let bn = self.bn_rx.borrow().clone().map(|q| q.price);
        let ptb = cb.or(bn);

        match &mut self.active {
            ActiveEngine::Pairing { detector, engine } => {
                detector.reset_window(open_ms, end_ms, ptb);
                engine.reset_window();
            }
            ActiveEngine::GapSwing { engine } => {
                engine.reset_window(open_ms, end_ms, ptb);
                engine.set_shares(self.cfg.order_shares);
                engine.set_taker_delay_ms(if self.cfg.live_trading {
                    0
                } else {
                    TAKER_DELAY_MS
                });
            }
        }
        self.pnl.open_window(market.slug.clone(), self.cfg.order_shares);
        self.trading_market = Some(market.clone());
        self.trading_start_ts = Some(start_ts);
        if self.cfg.live_trading {
            if let Err(e) = self.orders.prepare_market(&market).await {
                warn!("presign trading window {}: {:#}", market.slug, e);
            }
        }
        Ok(())
    }

    async fn finalize_window(&mut self, start_ts: i64, now: i64) {
        let end_ms = window_end_ms(start_ts);
        if let Some(m) = self.trading_market.clone() {
            if let Some(quotes) = self.poly_book.latest(start_ts) {
                match &mut self.active {
                    ActiveEngine::Pairing { engine, .. } => {
                        let action = engine
                            .tick(now, quotes.up_ask, quotes.down_ask, end_ms)
                            .or_else(|| engine.try_last_tick_flatten(end_ms));
                        if let Some(action) = action {
                            self.execute_action(&action, &m, now).await;
                        }
                    }
                    ActiveEngine::GapSwing { engine } => {
                        let actions = engine.tick(now, quotes.up_ask, quotes.down_ask);
                        for action in actions {
                            self.execute_action(&action, &m, now).await;
                        }
                    }
                }
            }
        }

        self.flush_dry_fills_for_window(start_ts);

        let quotes = self.poly_book.latest(start_ts);
        let (up_ask, down_ask) = quotes
            .map(|q| (q.up_ask, q.down_ask))
            .unwrap_or((0.5, 0.5));

        let bn = self.bn_rx.borrow().clone().map(|q| q.price);
        let cb = self.cb_rx.borrow().clone().map(|q| q.price);
        let last_btc = bn.or(cb);
        let ptb = match &self.active {
            ActiveEngine::Pairing { detector, .. } => detector.strike(),
            ActiveEngine::GapSwing { engine } => engine.strike(),
        };

        let winner = resolve_winner(up_ask, down_ask, last_btc, ptb);
        if let Some(close) = self.pnl.close_window(winner) {
            self.pnl.log_window_close(&close);
        } else if let Some(m) = self.market_cache.get(&start_ts) {
            info!(
                "── window finished {} ── no trades | session net ${:.2} | gross ${:.2}",
                m.slug,
                self.pnl.totals().net,
                self.pnl.totals().gross
            );
        }

        self.poly_book.clear_window(start_ts);
        let _ = now;
    }

    async fn handle_pairing_signal(&mut self, sig: &CaptureSignal, now: i64) {
        if !self.can_trade(now) {
            return;
        }
        let flat = match &self.active {
            ActiveEngine::Pairing { engine, .. } => engine.is_flat(),
            _ => false,
        };
        if !flat {
            return;
        }
        let action = match &mut self.active {
            ActiveEngine::Pairing { engine, .. } => engine.on_signal(sig, now),
            _ => None,
        };
        if let Some(action) = action {
            if let Some(m) = self.trading_market.clone() {
                self.execute_action(&action, &m, now).await;
            }
        }
    }

    async fn execute_action(&mut self, action: &BotAction, market: &MarketInfo, now: i64) {
        if !self.can_trade(now) {
            return;
        }

        match action {
            BotAction::BuyEntry {
                side,
                signal_t,
                worst_ask,
                reason,
            } => {
                if self.cfg.live_trading {
                    match self
                        .orders
                        .buy_side(market, *side, *worst_ask, reason)
                        .await
                    {
                        Err(e) => error!("entry order failed: {:#}", e),
                        Ok(fill_px) => {
                            self.pnl.record_buy(
                                *side,
                                fill_px,
                                self.cfg.order_shares,
                                false,
                            );
                            info!("PnL booked {:?} @ actual fill {:.3}", side, fill_px);
                        }
                    }
                } else {
                    self.queue_dry_fill(
                        *signal_t,
                        market.start_ts,
                        *side,
                        FillKind::Entry,
                        *worst_ask,
                        reason,
                    );
                }
            }
            BotAction::BuyPair {
                side,
                signal_t,
                worst_ask,
                reason,
            } => {
                if self.cfg.live_trading {
                    match self
                        .orders
                        .buy_side(market, *side, *worst_ask, reason)
                        .await
                    {
                        Err(e) => error!("pair order failed: {:#}", e),
                        Ok(fill_px) => {
                            self.pnl.record_buy(
                                *side,
                                fill_px,
                                self.cfg.order_shares,
                                true,
                            );
                            info!("PnL booked {:?} @ actual fill {:.3}", side, fill_px);
                        }
                    }
                } else {
                    self.queue_dry_fill(
                        *signal_t,
                        market.start_ts,
                        *side,
                        FillKind::Pair,
                        *worst_ask,
                        reason,
                    );
                }
            }
            BotAction::SellFlatten {
                side,
                signal_t,
                worst_ask,
                reason,
            } => {
                // Dry-run: mark sell at ask−1¢ after delay.
                // Live: no sell API yet — hold unpaired to settlement (log once per intent).
                if self.cfg.live_trading {
                    warn!(
                        "[LIVE] flatten {:?} @~{:.3} reason={} (sell not posted — hold to settle)",
                        side, worst_ask, reason
                    );
                } else {
                    self.queue_dry_fill(
                        *signal_t,
                        market.start_ts,
                        *side,
                        FillKind::Flatten,
                        (*worst_ask - 0.01).max(0.01),
                        reason,
                    );
                }
            }
        }
    }

    fn queue_dry_fill(
        &mut self,
        send_t: i64,
        start_ts: i64,
        side: PositionSide,
        kind: FillKind,
        worst_ask: f64,
        reason: &str,
    ) {
        self.pending_dry.push(PendingDryFill {
            send_t,
            start_ts,
            side,
            kind,
            worst_ask,
            reason: reason.to_string(),
        });
    }

    fn process_pending_dry(&mut self, now: i64) {
        let mut ready: Vec<PendingDryFill> = vec![];
        self.pending_dry.retain(|p| {
            if now >= p.send_t + TAKER_DELAY_MS {
                ready.push(PendingDryFill {
                    send_t: p.send_t,
                    start_ts: p.start_ts,
                    side: p.side,
                    kind: p.kind,
                    worst_ask: p.worst_ask,
                    reason: p.reason.clone(),
                });
                false
            } else {
                true
            }
        });

        for fill in ready {
            self.apply_dry_fill(&fill, false);
        }
    }

    fn flush_dry_fills_for_window(&mut self, start_ts: i64) {
        let mut ready: Vec<PendingDryFill> = vec![];
        self.pending_dry.retain(|p| {
            if p.start_ts == start_ts {
                ready.push(PendingDryFill {
                    send_t: p.send_t,
                    start_ts: p.start_ts,
                    side: p.side,
                    kind: p.kind,
                    worst_ask: p.worst_ask,
                    reason: p.reason.clone(),
                });
                false
            } else {
                true
            }
        });
        for fill in ready {
            self.apply_dry_fill(&fill, true);
        }
    }

    fn apply_dry_fill(&mut self, fill: &PendingDryFill, force: bool) {
        let fill_t = fill.send_t + TAKER_DELAY_MS;
        let fill_px = match fill.kind {
            FillKind::Flatten => fill.worst_ask, // already ask−1¢
            _ => self
                .poly_book
                .ask_before(fill.start_ts, fill_t, fill.side)
                .unwrap_or(fill.worst_ask),
        };

        let label = match fill.kind {
            FillKind::Entry => "ENTRY",
            FillKind::Pair => "PAIR",
            FillKind::Flatten => "SELL",
        };
        warn!(
            "[DRY] {} {:?} {:.0}sh @ {:.3} (signal+{}ms) reason={}",
            label,
            fill.side,
            self.cfg.order_shares,
            fill_px,
            TAKER_DELAY_MS,
            fill.reason
        );

        if force || self.trading_start_ts == Some(fill.start_ts) {
            match fill.kind {
                FillKind::Flatten => {
                    self.pnl
                        .record_sell(fill.side, fill_px, self.cfg.order_shares);
                }
                FillKind::Entry => {
                    self.pnl
                        .record_buy(fill.side, fill_px, self.cfg.order_shares, false);
                }
                FillKind::Pair => {
                    self.pnl
                        .record_buy(fill.side, fill_px, self.cfg.order_shares, true);
                }
            }
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn market_token_ids(markets: &[MarketInfo]) -> HashSet<String> {
    let mut ids = HashSet::new();
    for m in markets {
        ids.insert(m.up_token_id.clone());
        ids.insert(m.down_token_id.clone());
    }
    ids
}
