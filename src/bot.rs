use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use crate::config::{Config, StrategyKind};
use crate::clob::OrderClient;
use crate::feeds::{
    spawn_binance, spawn_chainlink, spawn_coinbase, spawn_polymarket, BtcQuote, PolyQuote,
};
use crate::gamma::{fetch_official_ptb, fetch_ptb_ready, resolve_btc5m_market, MarketInfo};
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
    bn_rx: mpsc::UnboundedReceiver<BtcQuote>,
    cb_rx: mpsc::UnboundedReceiver<BtcQuote>,
    cl_rx: mpsc::UnboundedReceiver<BtcQuote>,
    poly_rx: mpsc::UnboundedReceiver<PolyQuote>,
    /// Latest mids for settle (feeds are queues, not watch).
    last_bn: Option<f64>,
    last_cb: Option<f64>,
    /// Latest 60s TWAP print from RTDS (`crypto_prices_twap_sixty`). Never overwrites strike.
    last_cl: Option<(i64, f64)>,
    /// Last locked strike per window start (logging / pairing).
    ptb_cache: HashMap<i64, f64>,
    /// Pairing: REST/DB prefetch done. Gap-swing uses engine.strike() instead.
    ptb_site_confirmed: HashSet<i64>,
    market_cache: HashMap<i64, MarketInfo>,
    subscribed_starts: Vec<i64>,
    trading_start_ts: Option<i64>,
    trading_market: Option<MarketInfo>,
    active: ActiveEngine,
    poly_book: PolyBook,
    pnl: PnlTracker,
    pending_dry: Vec<PendingDryFill>,
    last_ptb_prefetch_ms: i64,
    last_ptb_429_ms: i64,
    /// Shared with REST fallback task — true once RTDS or REST locked this window.
    ptb_locked: Arc<AtomicBool>,
    rest_strike_tx: mpsc::UnboundedSender<(i64, f64)>,
    rest_strike_rx: mpsc::UnboundedReceiver<(i64, f64)>,
    last_tape_log_ms: i64,
}

impl Bot {
    pub fn new(cfg: Config) -> Result<Self> {
        let orders = OrderClient::new(&cfg)?;
        let (subs_tx, subs_rx) = watch::channel(vec![]);
        let bn_rx = spawn_binance();
        let cb_rx = spawn_coinbase();
        let cl_rx = spawn_chainlink();
        let poly_rx = spawn_polymarket(subs_rx);
        let (rest_strike_tx, rest_strike_rx) = mpsc::unbounded_channel();
        let ptb_locked = Arc::new(AtomicBool::new(false));

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
                // Live: fire at signal (delay 0). Dry/backtest: t+250 like poly-history.
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
            cl_rx,
            poly_rx,
            last_bn: None,
            last_cb: None,
            last_cl: None,
            ptb_cache: HashMap::new(),
            ptb_site_confirmed: HashSet::new(),
            market_cache: HashMap::new(),
            subscribed_starts: vec![],
            trading_start_ts: None,
            trading_market: None,
            active,
            poly_book: PolyBook::new(),
            pnl: PnlTracker::new(),
            pending_dry: vec![],
            last_ptb_prefetch_ms: 0,
            last_ptb_429_ms: 0,
            ptb_locked,
            rest_strike_tx,
            rest_strike_rx,
            last_tape_log_ms: 0,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            "polybot starting strategy={} live_trading={} shares={} poly_sub=current+next taker_delay={}ms tape=binance Δ=Binance−locked_PTB strike=RTDS-crypto_prices_twap_sixty (REST openPrice 60s TWAP fallback after 8s; first lock wins)",
            self.cfg.strategy.label(),
            self.cfg.live_trading,
            self.cfg.order_shares,
            if self.cfg.live_trading { 0 } else { TAKER_DELAY_MS },
        );
        if let Err(e) = self.orders.init_live(&self.cfg).await {
            error!("CLOB init: {:#}", e);
            if self.cfg.live_trading {
                return Err(e);
            }
        }

        // Fast poll so queued bookTicker/ask ticks are drained promptly (was 50ms + watch-latest).
        let mut interval = tokio::time::interval(Duration::from_millis(10));
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

            self.drain_poly_quotes();
            self.drain_rtds_and_maybe_lock_strike().await;
            self.drain_rest_ptb_fallback().await;

            self.process_pending_dry(now);

            self.ingest_gap_swing();
            if self.can_trade(now) {
                self.drive_strategy(now).await;
            }
            self.maybe_log_tape(now);
        }
    }

    fn drain_poly_quotes(&mut self) {
        while let Ok(p) = self.poly_rx.try_recv() {
            if self.subscribed_starts.contains(&p.start_ts) {
                self.poly_book.push(p.start_ts, p.t, p.up_ask, p.down_ask);
            }
        }
    }

    fn drain_bn_quotes(&mut self) -> Vec<BtcQuote> {
        let mut out = Vec::new();
        while let Ok(q) = self.bn_rx.try_recv() {
            self.last_bn = Some(q.price);
            out.push(q);
        }
        out
    }

    fn drain_cb_quotes(&mut self) -> Vec<BtcQuote> {
        let mut out = Vec::new();
        while let Ok(q) = self.cb_rx.try_recv() {
            self.last_cb = Some(q.price);
            out.push(q);
        }
        out
    }

    /// Drain RTDS 60s TWAP. First tick at/after window open locks strike once, then trade.
    async fn drain_rtds_and_maybe_lock_strike(&mut self) {
        while let Ok(q) = self.cl_rx.try_recv() {
            self.last_cl = Some((q.t, q.price));
            if !matches!(self.cfg.strategy, StrategyKind::GapSwing) {
                continue;
            }
            let Some(start) = self.trading_start_ts else {
                continue;
            };
            if q.t < window_open_ms(start) || q.price <= 0.0 {
                continue;
            }
            self.try_lock_gap_ptb(start, q.price, "rtds-twap-sixty").await;
        }
    }

    fn gap_ptb_locked(&self) -> bool {
        match &self.active {
            ActiveEngine::GapSwing { engine } => engine.strike().is_some(),
            _ => false,
        }
    }

    async fn try_lock_gap_ptb(&mut self, start: i64, ptb: f64, src: &str) {
        if self.gap_ptb_locked() {
            if src.starts_with("rest") {
                if let ActiveEngine::GapSwing { engine } = &self.active {
                    if let Some(have) = engine.strike() {
                        info!(
                            "REST openPrice ignored (PTB already locked) rest={:.2} have={:.2}",
                            ptb, have
                        );
                    }
                }
            }
            return;
        }
        self.apply_gap_strike(start, ptb, src).await;
    }

    async fn apply_gap_strike(&mut self, start: i64, ptb: f64, _src: &str) {
        let (up, down) = self
            .poly_book
            .latest(start)
            .map(|q| (q.up_ask, q.down_ask))
            .unwrap_or((0.0, 0.0));
        let now = now_ms();
        let locked = if let ActiveEngine::GapSwing { engine } = &mut self.active {
            engine.set_strike(ptb)
        } else {
            false
        };
        if !locked {
            return;
        }
        self.ptb_locked.store(true, Ordering::Relaxed);
        self.ptb_cache.insert(start, ptb);
        info!(ptb, "official PTB locked; Δ = Binance − PTB");
        let actions = if let ActiveEngine::GapSwing { engine } = &mut self.active {
            engine.tick(now, up, down)
        } else {
            vec![]
        };
        if self.can_trade(now) {
            if let Some(m) = self.trading_market.clone() {
                for action in actions {
                    self.execute_action(&action, &m, now).await;
                }
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

        for q in self.drain_bn_quotes() {
            let sig = match &mut self.active {
                ActiveEngine::Pairing { detector, .. } => detector.on_bn(q.t, q.price),
                _ => None,
            };
            if let Some(sig) = sig {
                self.handle_pairing_signal(&sig, now).await;
            }
        }
        for q in self.drain_cb_quotes() {
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

    async fn drain_rest_ptb_fallback(&mut self) {
        while let Ok((start, px)) = self.rest_strike_rx.try_recv() {
            if self.trading_start_ts != Some(start) {
                continue;
            }
            self.try_lock_gap_ptb(start, px, "rest-openPrice-stable").await;
        }
    }

    fn spawn_rest_ptb_fallback(&self, start: i64, twap_enabled: bool, twap_lookback_sec: i64) {
        let locked = self.ptb_locked.clone();
        let tx = self.rest_strike_tx.clone();
        tokio::spawn(async move {
            let wait_rtds = async {
                while !locked.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            };
            if tokio::time::timeout(Duration::from_secs(8), wait_rtds)
                .await
                .is_ok()
            {
                return;
            }
            if locked.load(Ordering::Relaxed) {
                return;
            }
            match fetch_ptb_ready(start, twap_enabled, twap_lookback_sec).await {
                Ok(px) => {
                    if locked.load(Ordering::Relaxed) {
                        return;
                    }
                    info!(
                        open = px,
                        start,
                        "ptb REST fallback (only if t0-twap missed)"
                    );
                    let _ = tx.send((start, px));
                }
                Err(e) => {
                    if !locked.load(Ordering::Relaxed) {
                        error!(error = %e, start, "ptb official lock");
                    }
                }
            }
        });
    }

    fn ptb_429_cooling(&self, now: i64) -> bool {
        self.last_ptb_429_ms > 0 && now.saturating_sub(self.last_ptb_429_ms) < 8_000
    }

    fn maybe_log_tape(&mut self, now: i64) {
        if !matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            return;
        }
        if now.saturating_sub(self.last_tape_log_ms) < 15_000 {
            return;
        }
        self.last_tape_log_ms = now;
        let ActiveEngine::GapSwing { engine } = &self.active else {
            return;
        };
        let delta = match (self.last_bn, engine.strike()) {
            (Some(bn), Some(ptb)) => Some(bn - ptb),
            _ => None,
        };
        info!(
            delta = ?delta,
            fills = engine.fill_count(),
            unpaired = engine.unpaired_shares(),
            pairs = engine.last_pairs(),
            "tape"
        );
    }

    fn ingest_gap_swing(&mut self) {
        if !matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            return;
        }
        if self.can_trade(now_ms()) {
            return;
        }
        // Ingest asks for the *next* trading window too (warm book before UTC open).
        for start in &self.subscribed_starts.clone() {
            if let Some(quotes) = self.poly_book.latest(*start) {
                if let ActiveEngine::GapSwing { engine } = &mut self.active {
                    // Only push into engine if this is the active trading window;
                    // otherwise poly_book already holds them for seed at open.
                    if self.trading_start_ts == Some(*start) {
                        engine.on_asks(quotes.t, quotes.up_ask, quotes.down_ask);
                    }
                }
            }
        }
        for q in self.drain_bn_quotes() {
            if let ActiveEngine::GapSwing { engine } = &mut self.active {
                let _ = engine.on_binance(q.t, q.price);
                engine.take_pending();
            }
        }
        // Coinbase: Raw chart uses Binance BTC; use CB only if BN is silent.
        let bn_fresh = self.last_bn.is_some();
        for q in self.drain_cb_quotes() {
            if !bn_fresh {
                if let ActiveEngine::GapSwing { engine } = &mut self.active {
                    let _ = engine.on_binance(q.t, q.price);
                    engine.take_pending();
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

        let mut got_bn = false;
        for q in self.drain_bn_quotes() {
            got_bn = true;
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
        for q in self.drain_cb_quotes() {
            // Same as Raw: primary tape is Binance. CB fills gaps only.
            if got_bn {
                continue;
            }
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
        let Some(m) = self.trading_market.as_ref() else {
            return false;
        };
        if now < window_open_ms(m.start_ts) {
            return false;
        }
        // Gap-swing: no orders until official 60s TWAP PTB is locked (RTDS first).
        if matches!(self.cfg.strategy, StrategyKind::GapSwing) && !self.gap_ptb_locked() {
            return false;
        }
        true
    }

    async fn sync_subscriptions(&mut self, now: i64) -> Result<()> {
        let desired = subscribe_window_starts(now);
        // Pairing only: optional REST prefetch. Gap-swing locks from RTDS at T0 (no REST spam).
        if matches!(self.cfg.strategy, StrategyKind::Pairing) {
        let near_open = desired.iter().any(|start| {
            if self.ptb_site_confirmed.contains(start) {
                return false;
            }
            let open = window_open_ms(*start);
            now + 10_000 >= open && now <= open + 45_000
        });
        let rest_cooling = self.ptb_429_cooling(now);
        let prefetch_every = if near_open {
            400
        } else if rest_cooling {
            8_000
        } else {
            5_000
        };
        if now.saturating_sub(self.last_ptb_prefetch_ms) >= prefetch_every {
            self.last_ptb_prefetch_ms = now;
            for start in &desired {
                if self.ptb_site_confirmed.contains(start) {
                    continue;
                }
                let open = window_open_ms(*start);
                if now + 10_000 < open || now > open + 45_000 {
                    continue;
                }
                if rest_cooling {
                    continue;
                }
                match fetch_official_ptb(*start).await {
                    Ok(Some(ptb)) => {
                        info!("prefetched official PTB for {} = {:.4}", start, ptb);
                        self.ptb_cache.insert(*start, ptb);
                        self.ptb_site_confirmed.insert(*start);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let msg = format!("{e:#}");
                        if msg.contains("429") {
                            self.last_ptb_429_ms = now;
                            warn!("crypto-price 429 on prefetch — REST backoff");
                        }
                    }
                }
            }
        }
        }

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
            info!("polymarket subscribe (current+next) → {:?}", labels);
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
            slug = %market.slug,
            "window ready (Δ = Binance − official PTB)"
        );

        let open_ms = window_open_ms(market.start_ts);
        let end_ms = window_end_ms(market.start_ts);
        // Gap-swing: do not wait for REST. Reset unlocked; first RTDS 60s TWAP tick locks.
        let (ptb, _ptb_src) = if matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            (None, "waiting-rtds-twap-sixty")
        } else {
            let mut official = self.ptb_cache.get(&market.start_ts).copied();
            if official.is_none() {
                for attempt in 0..8 {
                    official = fetch_official_ptb(market.start_ts).await.ok().flatten();
                    if let Some(p) = official {
                        self.ptb_cache.insert(market.start_ts, p);
                        break;
                    }
                    if attempt + 1 < 8 {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
            if let Some(p) = official {
                (Some(p), "official-ptb")
            } else {
                (self.last_bn.or(self.last_cb), "cex-fallback")
            }
        };

        match &mut self.active {
            ActiveEngine::Pairing { detector, engine } => {
                detector.reset_window(open_ms, end_ms, ptb.or(self.last_bn).or(self.last_cb));
                engine.reset_window();
            }
            ActiveEngine::GapSwing { engine } => {
                engine.reset_window(open_ms, end_ms, None);
                engine.set_shares(self.cfg.order_shares);
                let delay = if self.cfg.live_trading { 0 } else { TAKER_DELAY_MS };
                engine.set_taker_delay_ms(delay);
                self.ptb_locked.store(false, Ordering::Relaxed);
                // Seed pre-open asks collected while next window was subscribed (current+next).
                let seeded = self.poly_book.history(start_ts);
                for &(t, up, down) in &seeded {
                    engine.on_asks(t, up, down);
                }
                if let Some(q) = self.poly_book.latest(start_ts) {
                    engine.on_asks(q.t, q.up_ask, q.down_ask);
                }
                info!(slug = %market.slug, "window armed — waiting official PTB");
            }
        }
        self.pnl.open_window(market.slug.clone(), self.cfg.order_shares);
        self.trading_market = Some(market.clone());
        self.trading_start_ts = Some(start_ts);
        if matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            if let Some((t, px)) = self.last_cl {
                if t >= open_ms && px > 0.0 {
                    self.try_lock_gap_ptb(start_ts, px, "rtds-twap-sixty").await;
                }
            }
            self.spawn_rest_ptb_fallback(
                start_ts,
                market.twap_enabled,
                market.twap_lookback_sec,
            );
        }
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

        let bn = self.last_bn;
        let cb = self.last_cb;
        let last_btc = bn.or(cb);
        let ptb = match &self.active {
            ActiveEngine::Pairing { detector, .. } => detector.strike(),
            ActiveEngine::GapSwing { engine } => engine.strike(),
        };

        let winner = resolve_winner(up_ask, down_ask, last_btc, ptb);
        let slug = self
            .market_cache
            .get(&start_ts)
            .map(|m| m.slug.clone())
            .unwrap_or_else(|| format!("btc-updown-5m-{start_ts}"));

        if let ActiveEngine::GapSwing { engine } = &mut self.active {
            let res = engine.finalize(winner);
            self.pnl
                .add_window_net(&slug, res.total_pnl, res.pairs, res.trades.len());
        } else if let Some(close) = self.pnl.close_window(winner) {
            info!(
                "── window finished {} ── net ${:.2} | gross ${:.2} | fees ${:.2} | legs {}",
                close.slug, close.net, close.gross, close.fees, close.legs
            );
            info!(
                "── session total ── net ${:.2} | gross ${:.2} | fees ${:.2}",
                self.pnl.totals().net,
                self.pnl.totals().gross,
                self.pnl.totals().fees
            );
        } else {
            info!(
                "── window finished {slug} ── net $0.00 | no trades"
            );
            info!(
                "── session total ── net ${:.2}",
                self.pnl.totals().net
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

        // Gap-swing inventory is gated + optimistic-committed in engine.refresh.
        // Do not re-check here — commits already reflect the intent.

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
                            // Gap-swing commits at intent emit (optimistic Raw lots).
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
                            // Gap-swing commits at intent emit (optimistic Raw lots).
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
                let sell_shares = self.cfg.order_shares;
                if self.cfg.live_trading {
                    match self
                        .orders
                        .sell_side(market, *side, *worst_ask, reason, sell_shares)
                        .await
                    {
                        Err(e) => error!("flatten sell failed: {:#}", e),
                        Ok(fill_px) => {
                            self.pnl.record_sell(*side, fill_px, sell_shares);
                            // Gap-swing commits at intent emit.
                            info!(
                                "PnL booked SELL {:?} @ actual fill {:.3} ({:.0} sh)",
                                side, fill_px, sell_shares
                            );
                        }
                    }
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
            if matches!(self.cfg.strategy, StrategyKind::GapSwing) {
                // Gap-swing window PnL comes from engine.finalize (same as poly-history).
                return;
            }
            match fill.kind {
                FillKind::Flatten => {
                    self.pnl
                        .record_sell(fill.side, fill_px, self.cfg.order_shares);
                    // Gap-swing already commit_sell'd at intent emit.
                }
                FillKind::Entry => {
                    self.pnl
                        .record_buy(fill.side, fill_px, self.cfg.order_shares, false);
                    // Gap-swing already commit_buy'd at intent emit (avoids multi-tick
                    // first-leg stacks while dry fill was still pending).
                }
                FillKind::Pair => {
                    self.pnl
                        .record_buy(fill.side, fill_px, self.cfg.order_shares, true);
                    // Gap-swing already commit_buy'd at intent emit.
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
