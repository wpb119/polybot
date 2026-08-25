use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use crate::config::{Config, StrategyKind};
use crate::clob::OrderClient;
use crate::feeds::{
    spawn_binance, spawn_chainlink, spawn_coinbase, spawn_polymarket, BtcQuote, PolyQuote,
};
use crate::gamma::{fetch_official_ptb, resolve_btc5m_market, MarketInfo};
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
    /// Latest mids for PTB fallback / settle (feeds are queues, not watch).
    last_bn: Option<f64>,
    last_cb: Option<f64>,
    /// Latest Chainlink/TWAP print from RTDS.
    last_cl: Option<(i64, f64)>,
    /// Strike cache: site crypto-price TWAP open, or provisional RTDS TWAP lock.
    ptb_cache: HashMap<i64, f64>,
    /// Windows whose strike was confirmed from polymarket crypto-price (event-page PTB).
    ptb_site_confirmed: HashSet<i64>,
    market_cache: HashMap<i64, MarketInfo>,
    subscribed_starts: Vec<i64>,
    trading_start_ts: Option<i64>,
    trading_market: Option<MarketInfo>,
    active: ActiveEngine,
    poly_book: PolyBook,
    pnl: PnlTracker,
    pending_dry: Vec<PendingDryFill>,
    /// Live intents this window for close-time Raw comparison (elapsed_s, BUY/SELL, side, reason).
    live_intents: Vec<(f64, &'static str, PositionSide, String)>,
}

impl Bot {
    pub fn new(cfg: Config) -> Result<Self> {
        let orders = OrderClient::new(&cfg)?;
        let (subs_tx, subs_rx) = watch::channel(vec![]);
        let bn_rx = spawn_binance();
        let cb_rx = spawn_coinbase();
        let cl_rx = spawn_chainlink();
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
            live_intents: vec![],
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            "polybot starting strategy={} live_trading={} shares={} pre_subscribe=5s taker_delay={}ms tick_queue=drain-all strike=site-twap (crypto-price) + rtds-twap fallback (live=0; dry=250)",
            self.cfg.strategy.label(),
            self.cfg.live_trading,
            self.cfg.order_shares,
            if self.cfg.live_trading { 0 } else { TAKER_DELAY_MS }
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
            self.drain_chainlink_and_maybe_lock_strike().await;

            self.process_pending_dry(now);

            if let Err(e) = self.ensure_gap_swing_ptb().await {
                warn!("ptb retry: {:#}", e);
            }

            self.ingest_gap_swing();
            if self.can_trade(now) {
                self.drive_strategy(now).await;
            }
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

    /// Drain RTDS (TWAP preferred). Provisional lock at open until site crypto-price arrives.
    async fn drain_chainlink_and_maybe_lock_strike(&mut self) {
        let mut locked: Option<(i64, f64)> = None;
        while let Ok(q) = self.cl_rx.try_recv() {
            self.last_cl = Some((q.t, q.price));
            let Some(start) = self.trading_start_ts else {
                continue;
            };
            if self.ptb_site_confirmed.contains(&start) || self.ptb_cache.contains_key(&start) {
                continue;
            }
            let open_ms = window_open_ms(start);
            if q.t >= open_ms && q.t <= open_ms + 5_000 && q.price > 0.0 {
                locked = Some((start, q.price));
                while let Ok(more) = self.cl_rx.try_recv() {
                    self.last_cl = Some((more.t, more.price));
                }
                break;
            }
        }
        if let Some((start, ptb)) = locked {
            self.ptb_cache.insert(start, ptb);
            if matches!(self.cfg.strategy, StrategyKind::GapSwing) {
                if let ActiveEngine::GapSwing { engine } = &mut self.active {
                    if engine.strike().is_none() {
                        self.apply_gap_strike(start, ptb, "rtds-twap-provisional")
                            .await;
                    }
                }
            }
        }
    }

    async fn apply_gap_strike(&mut self, start: i64, ptb: f64, src: &str) {
        let (up, down) = self
            .poly_book
            .latest(start)
            .map(|q| (q.up_ask, q.down_ask))
            .unwrap_or((0.0, 0.0));
        let now = now_ms();
        let actions = if let ActiveEngine::GapSwing { engine } = &mut self.active {
            let had = engine.strike();
            engine.set_strike(ptb);
            if let Some(old) = had {
                if (old - ptb).abs() > 0.5 {
                    info!(
                        "gap-swing strike updated {:.4} → {:.4} source={}",
                        old, ptb, src
                    );
                } else {
                    info!("gap-swing strike set from {}={:.4}", src, ptb);
                }
            } else {
                info!("gap-swing strike set from {}={:.4}", src, ptb);
            }
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

    async fn ensure_gap_swing_ptb(&mut self) -> Result<()> {
        if !matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            return Ok(());
        }
        let Some(start) = self.trading_start_ts else {
            return Ok(());
        };

        // Site Price-to-Beat = crypto-price 60s TWAP open (matches polymarket.com).
        if !self.ptb_site_confirmed.contains(&start) {
            if let Ok(Some(site)) = fetch_official_ptb(start).await {
                let unset = match &self.active {
                    ActiveEngine::GapSwing { engine } => engine.strike().is_none(),
                    _ => true,
                };
                // Never change strike after any fill — mid-window PTB rewrite desyncs Raw path.
                let no_fills = match &self.active {
                    ActiveEngine::GapSwing { engine } => !engine.has_fills(),
                    _ => true,
                };
                if unset || (no_fills && !self.ptb_site_confirmed.contains(&start)) {
                    self.ptb_cache.insert(start, site);
                    self.ptb_site_confirmed.insert(start);
                    self.apply_gap_strike(start, site, "site-crypto-price-twap")
                        .await;
                    return Ok(());
                }
            }
        }

        let need = match &self.active {
            ActiveEngine::GapSwing { engine } => engine.strike().is_none(),
            _ => false,
        };
        if need {
            if let Some(ptb) = self.ptb_cache.get(&start).copied() {
                self.apply_gap_strike(start, ptb, "rtds-twap-provisional")
                    .await;
            }
        }
        Ok(())
    }

    fn ingest_gap_swing(&mut self) {
        if !matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            return;
        }
        if self.can_trade(now_ms()) {
            return;
        }
        if let Some(m) = self.trading_market.clone() {
            if let Some(quotes) = self.poly_book.latest(m.start_ts) {
                if let ActiveEngine::GapSwing { engine } = &mut self.active {
                    engine.on_asks(quotes.t, quotes.up_ask, quotes.down_ask);
                }
            }
        } else if let Some(start) = self.subscribed_starts.first() {
            if let Some(quotes) = self.poly_book.latest(*start) {
                if let ActiveEngine::GapSwing { engine } = &mut self.active {
                    engine.on_asks(quotes.t, quotes.up_ask, quotes.down_ask);
                }
            }
        }
        for q in self.drain_bn_quotes() {
            if let ActiveEngine::GapSwing { engine } = &mut self.active {
                let _ = engine.on_binance(q.t, q.price);
                engine.take_pending();
            }
        }
        let _ = self.drain_cb_quotes();
    }

    async fn drive_gap_swing(&mut self, now: i64) {
        if let Some(m) = self.trading_market.clone() {
            if let Some(quotes) = self.poly_book.latest(m.start_ts) {
                if let ActiveEngine::GapSwing { engine } = &mut self.active {
                    engine.on_asks(quotes.t, quotes.up_ask, quotes.down_ask);
                }
            }
        }

        for q in self.drain_bn_quotes() {
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
            // Prefetch site crypto-price TWAP for gap_swing + pairing.
            if !self.ptb_cache.contains_key(start) {
                if let Ok(Some(ptb)) = fetch_official_ptb(*start).await {
                    info!("prefetched site PTB for {} = {:.4}", start, ptb);
                    self.ptb_cache.insert(*start, ptb);
                    if matches!(self.cfg.strategy, StrategyKind::GapSwing) {
                        self.ptb_site_confirmed.insert(*start);
                    }
                }
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
        // Gap-swing: prefer site crypto-price TWAP open (event-page PTB); else RTDS TWAP lock.
        let cb = self.last_cb;
        let bn = self.last_bn;
        let (ptb, ptb_src) = if matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            let mut site = None;
            for attempt in 0..6 {
                site = fetch_official_ptb(market.start_ts).await.ok().flatten();
                if site.is_some() {
                    break;
                }
                if attempt + 1 < 6 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            if let Some(p) = site {
                self.ptb_cache.insert(market.start_ts, p);
                self.ptb_site_confirmed.insert(market.start_ts);
                (Some(p), "site-crypto-price-twap")
            } else if let Some(p) = self.ptb_cache.get(&market.start_ts).copied() {
                (Some(p), "rtds-twap-provisional")
            } else if let Some((t, px)) =
                self.last_cl.filter(|(t, _)| *t >= open_ms && *t <= open_ms + 5_000)
            {
                self.ptb_cache.insert(market.start_ts, px);
                let _ = t;
                (Some(px), "rtds-twap-provisional")
            } else {
                warn!(
                    "site PTB unavailable for {} — waiting crypto-price / RTDS TWAP (no CEX)",
                    market.slug
                );
                (None, "missing-ptb")
            }
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
                (bn.or(cb), "cex-fallback")
            }
        };

        match &mut self.active {
            ActiveEngine::Pairing { detector, engine } => {
                detector.reset_window(open_ms, end_ms, ptb.or(self.last_bn).or(self.last_cb));
                engine.reset_window();
            }
            ActiveEngine::GapSwing { engine } => {
                engine.reset_window(open_ms, end_ms, ptb);
                engine.set_shares(self.cfg.order_shares);
                let delay = if self.cfg.live_trading { 0 } else { TAKER_DELAY_MS };
                engine.set_taker_delay_ms(delay);
                info!(
                    "gap-swing strike={:?} source={} open={} end={} taker_delay={}ms fire={}",
                    ptb,
                    ptb_src,
                    open_ms,
                    end_ms,
                    delay,
                    if self.cfg.live_trading {
                        "immediate"
                    } else {
                        "signal+250ms"
                    }
                );
            }
        }
        self.pnl.open_window(market.slug.clone(), self.cfg.order_shares);
        self.trading_market = Some(market.clone());
        self.trading_start_ts = Some(start_ts);
        self.live_intents.clear();
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

        if matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            self.log_raw_history_compare(start_ts, ptb).await;
        }

        self.poly_book.clear_window(start_ts);
        self.live_intents.clear();
        let _ = now;
    }

    fn record_live_intent(
        &mut self,
        start_ts: i64,
        kind: &'static str,
        side: PositionSide,
        reason: &str,
        signal_t: i64,
    ) {
        let open = window_open_ms(start_ts);
        let elapsed = (signal_t.max(open) - open) as f64 / 1000.0;
        self.live_intents
            .push((elapsed, kind, side, reason.to_string()));
    }

    /// At every window close: Raw buys from poly-history tape vs our live intents.
    async fn log_raw_history_compare(&self, start_ts: i64, live_ptb: Option<f64>) {
        let slug = format!("btc-updown-5m-{}", start_ts);
        let url = format!(
            "http://127.0.0.1:3002/api/windows/{}",
            slug
        );
        let client = reqwest::Client::new();
        let detail: serde_json::Value = match client.get(&url).send().await {
            Ok(res) if res.status().is_success() => match res.json().await {
                Ok(v) => v,
                Err(e) => {
                    warn!("history compare {}: json {:#}", slug, e);
                    return;
                }
            },
            Ok(res) => {
                warn!("history compare {}: HTTP {}", slug, res.status());
                return;
            }
            Err(e) => {
                warn!("history compare {}: {:#} (is poly-history server up?)", slug, e);
                return;
            }
        };

        let open_ms = start_ts * 1000;
        let end_ms = (start_ts + 300) * 1000;
        let hist_ptb = detail
            .pointer("/market/ptb")
            .and_then(|v| v.as_f64())
            .filter(|p| *p > 0.0);
        let Some(hist_ptb) = hist_ptb else {
            warn!("history compare {}: no market.ptb", slug);
            return;
        };

        let btc: Vec<(i64, f64)> = detail
            .pointer("/ticks/btc")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| {
                        Some((x.get("t")?.as_i64()?, x.get("price")?.as_f64()?))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let asks: Vec<(i64, f64, f64)> = detail
            .pointer("/ticks/poly")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| {
                        Some((
                            x.get("t")?.as_i64()?,
                            x.get("up")?.as_f64()?,
                            x.get("down")?.as_f64()?,
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let winner = detail
            .pointer("/market/bnClose")
            .and_then(|v| v.as_f64())
            .and_then(|close| {
                if close >= hist_ptb {
                    Some(PositionSide::Up)
                } else {
                    Some(PositionSide::Down)
                }
            });

        let shares = self.cfg.order_shares;
        let hist = crate::strategy::run_gap_swing_window(
            open_ms,
            end_ms,
            Some(hist_ptb),
            &btc,
            &asks,
            winner,
            shares,
            Some(end_ms),
            250,
        );
        let hist_legs: Vec<String> = hist
            .trades
            .iter()
            .filter(|t| t.kind == "BUY" || t.kind == "SELL")
            .map(|t| {
                format!(
                    "{:.1}s {} {:?} {}",
                    (t.t - open_ms) as f64 / 1000.0,
                    t.kind,
                    t.side,
                    t.reason
                )
            })
            .collect();

        let live_legs: Vec<String> = self
            .live_intents
            .iter()
            .map(|(el, k, side, reason)| format!("{el:.1}s {k} {side:?} {reason}"))
            .collect();

        let hist_seq: Vec<String> = hist
            .trades
            .iter()
            .filter(|t| t.kind == "BUY" || t.kind == "SELL")
            .map(|t| format!("{}:{:?}:{}", t.kind, t.side, t.reason))
            .collect();
        let live_seq: Vec<String> = self
            .live_intents
            .iter()
            .map(|(_, k, side, reason)| format!("{k}:{side:?}:{reason}"))
            .collect();
        let seq_match = hist_seq == live_seq;

        info!(
            "── Raw compare {} ── histPtb={:.2} livePtb={:?} seq_match={}",
            slug,
            hist_ptb,
            live_ptb,
            seq_match
        );
        info!(
            "── HISTORY Raw: {}",
            if hist_legs.is_empty() {
                "(none)".into()
            } else {
                hist_legs.join(" | ")
            }
        );
        info!(
            "── LIVE bot:    {}",
            if live_legs.is_empty() {
                "(none)".into()
            } else {
                live_legs.join(" | ")
            }
        );

        if let Some(lp) = live_ptb {
            if (lp - hist_ptb).abs() > 1.0 {
                let live_raw = crate::strategy::run_gap_swing_window(
                    open_ms,
                    end_ms,
                    Some(lp),
                    &btc,
                    &asks,
                    winner,
                    shares,
                    Some(end_ms),
                    250,
                );
                let live_raw_legs: Vec<String> = live_raw
                    .trades
                    .iter()
                    .filter(|t| t.kind == "BUY" || t.kind == "SELL")
                    .map(|t| {
                        format!(
                            "{:.1}s {} {:?} {}",
                            (t.t - open_ms) as f64 / 1000.0,
                            t.kind,
                            t.side,
                            t.reason
                        )
                    })
                    .collect();
                info!(
                    "── Raw@livePTB: {}",
                    if live_raw_legs.is_empty() {
                        "(none)".into()
                    } else {
                        live_raw_legs.join(" | ")
                    }
                );
            }
        }
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

        // Raw gap-swing inventory (same as poly-history): one unpaired side at a time.
        if matches!(self.cfg.strategy, StrategyKind::GapSwing) {
            let (unpaired, on_side) = match &self.active {
                ActiveEngine::GapSwing { engine } => (
                    engine.unpaired_shares(),
                    match action {
                        BotAction::BuyEntry { side, .. }
                        | BotAction::BuyPair { side, .. }
                        | BotAction::SellFlatten { side, .. } => engine.unpaired_on(*side),
                    },
                ),
                _ => (0.0, 0.0),
            };
            match action {
                BotAction::BuyEntry { side, reason, .. } => {
                    if unpaired > 1e-12 {
                        warn!(
                            "skip ENTRY {:?} reason={} — unpaired inventory still open (raw rule)",
                            side, reason
                        );
                        return;
                    }
                }
                BotAction::BuyPair { side, reason, .. } => {
                    let need = match side {
                        PositionSide::Up => PositionSide::Down,
                        PositionSide::Down => PositionSide::Up,
                    };
                    let need_qty = match &self.active {
                        ActiveEngine::GapSwing { engine } => engine.unpaired_on(need),
                        _ => 0.0,
                    };
                    if need_qty <= 1e-12 {
                        warn!(
                            "skip PAIR {:?} reason={} — no opposite unpaired lot",
                            side, reason
                        );
                        return;
                    }
                }
                BotAction::SellFlatten { side, reason, .. } => {
                    if on_side <= 1e-12 {
                        warn!(
                            "skip SELL {:?} reason={} — nothing unpaired to flatten",
                            side, reason
                        );
                        return;
                    }
                }
            }
        }

        match action {
            BotAction::BuyEntry {
                side,
                signal_t,
                worst_ask,
                reason,
            } => {
                self.record_live_intent(market.start_ts, "BUY", *side, reason, *signal_t);
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
                            if let ActiveEngine::GapSwing { engine } = &mut self.active {
                                engine.commit_buy(
                                    *side,
                                    self.cfg.order_shares,
                                    fill_px,
                                    *signal_t,
                                    false,
                                    0.0,
                                );
                            }
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
                self.record_live_intent(market.start_ts, "BUY", *side, reason, *signal_t);
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
                            if let ActiveEngine::GapSwing { engine } = &mut self.active {
                                engine.commit_buy(
                                    *side,
                                    self.cfg.order_shares,
                                    fill_px,
                                    *signal_t,
                                    true,
                                    0.0,
                                );
                            }
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
                self.record_live_intent(market.start_ts, "SELL", *side, reason, *signal_t);
                let sell_shares = match &self.active {
                    ActiveEngine::GapSwing { engine } => {
                        let q = engine.unpaired_on(*side);
                        if q > 1e-12 {
                            q
                        } else {
                            self.cfg.order_shares
                        }
                    }
                    _ => self.cfg.order_shares,
                };
                if self.cfg.live_trading {
                    match self
                        .orders
                        .sell_side(market, *side, *worst_ask, reason, sell_shares)
                        .await
                    {
                        Err(e) => error!("flatten sell failed: {:#}", e),
                        Ok(fill_px) => {
                            self.pnl.record_sell(*side, fill_px, sell_shares);
                            if let ActiveEngine::GapSwing { engine } = &mut self.active {
                                engine.commit_sell(*side);
                            }
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
            match fill.kind {
                FillKind::Flatten => {
                    self.pnl
                        .record_sell(fill.side, fill_px, self.cfg.order_shares);
                    if let ActiveEngine::GapSwing { engine } = &mut self.active {
                        engine.commit_sell(fill.side);
                    }
                }
                FillKind::Entry => {
                    self.pnl
                        .record_buy(fill.side, fill_px, self.cfg.order_shares, false);
                    if let ActiveEngine::GapSwing { engine } = &mut self.active {
                        engine.commit_buy(
                            fill.side,
                            self.cfg.order_shares,
                            fill_px,
                            fill.send_t,
                            false,
                            0.0,
                        );
                    }
                }
                FillKind::Pair => {
                    self.pnl
                        .record_buy(fill.side, fill_px, self.cfg.order_shares, true);
                    if let ActiveEngine::GapSwing { engine } = &mut self.active {
                        engine.commit_buy(
                            fill.side,
                            self.cfg.order_shares,
                            fill_px,
                            fill.send_t,
                            true,
                            0.0,
                        );
                    }
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
