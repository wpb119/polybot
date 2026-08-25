use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Credentials, Normal};
use polymarket_client_sdk_v2::clob::client::Config as ClobConfig;
use polymarket_client_sdk_v2::clob::types::request::TradesRequest;
use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
use polymarket_client_sdk_v2::clob::types::{
    OrderStatusType, OrderType, Side as SdkSide, SignatureType, SignedOrder,
};
use polymarket_client_sdk_v2::clob::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::Config as AppConfig;
use crate::gamma::MarketInfo;
use crate::strategy::PositionSide;

const CLOB_HOST: &str = "https://clob.polymarket.com";

/// GTC buy ceiling. 0.99 crosses the book like a market buy so we get an actual fill.
pub fn default_max_limit() -> f64 {
    0.99
}

struct PresignedSlot {
    limit_px: f64,
    signed: SignedOrder,
}

struct LiveSession {
    client: Client<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    presigned: HashMap<String, PresignedSlot>,
}

pub struct OrderClient {
    live: bool,
    shares: f64,
    max_limit: f64,
    session: Option<LiveSession>,
}

impl OrderClient {
    pub fn new(cfg: &AppConfig) -> Result<Self> {
        cfg.validate_for_live()?;
        Ok(Self {
            live: cfg.live_trading,
            shares: cfg.order_shares,
            max_limit: default_max_limit(),
            session: None,
        })
    }

    /// Connect CLOB + presign when live trading is enabled.
    pub async fn init_live(&mut self, cfg: &AppConfig) -> Result<()> {
        if !self.live {
            return Ok(());
        }
        let pk = cfg
            .private_key
            .as_ref()
            .filter(|k| !k.is_empty())
            .context("POLYMARKET_PRIVATE_KEY required for live trading")?;
        let pk = if pk.starts_with("0x") {
            pk.clone()
        } else {
            format!("0x{}", pk)
        };
        let signer: PrivateKeySigner = PrivateKeySigner::from_str(&pk)
            .map_err(|e| anyhow!("invalid private key: {}", e))?
            .with_chain_id(Some(137));

        let client = Client::new(CLOB_HOST, ClobConfig::default()).context("clob client")?;

        let mut auth = client.authentication_builder(&signer);
        if let (Some(key), Some(secret), Some(pass)) = (
            cfg.api_key.as_ref(),
            cfg.api_secret.as_ref(),
            cfg.api_passphrase.as_ref(),
        ) {
            let uuid = Uuid::parse_str(key).context("POLYMARKET_API_KEY must be a UUID")?;
            auth = auth.credentials(Credentials::new(uuid, secret.clone(), pass.clone()));
        }
        if let Some(funder) = cfg.funder.as_ref().filter(|f| !f.is_empty()) {
            let addr = Address::from_str(funder).context("POLYMARKET_FUNDER address")?;
            auth = auth.funder(addr);
        }
        auth = auth.signature_type(signature_type_from_cfg(cfg));

        let client = auth.authenticate().await.context("clob auth")?;
        info!("CLOB authenticated — presigned GTC limit orders enabled");

        self.session = Some(LiveSession {
            client,
            signer,
            presigned: HashMap::new(),
        });
        Ok(())
    }

    /// Presign GTC limit buys for both legs (refreshed after each post).
    pub async fn prepare_market(&mut self, market: &MarketInfo) -> Result<()> {
        if !self.live {
            return Ok(());
        }
        let limit = self.max_limit;
        self.presign_token(&market.up_token_id, limit).await?;
        self.presign_token(&market.down_token_id, limit).await?;
        info!(
            "presigned GTC buys for {} @ limit {:.2} ({:.0} sh each)",
            market.slug,
            limit,
            self.shares
        );
        Ok(())
    }

    /// Posts a GTC buy at `max_limit` (0.99, market-like). Returns **actual fill** for PnL.
    pub async fn buy_side(
        &mut self,
        market: &MarketInfo,
        side: PositionSide,
        worst_ask: f64,
        reason: &str,
    ) -> Result<f64> {
        let intent_px = clamp_price(worst_ask);
        let limit_px = self.max_limit;
        let amount_usd = self.shares * limit_px;
        if amount_usd < 1.0 {
            return Err(anyhow!("minimum order is $1"));
        }

        let token_id = match side {
            PositionSide::Up => market.up_token_id.clone(),
            PositionSide::Down => market.down_token_id.clone(),
        };

        if !self.live {
            return Ok(intent_px);
        }

        let session = self
            .session
            .as_mut()
            .context("live session not initialized — call init_live()")?;

        let shares = self.shares;
        let max_limit = self.max_limit;

        let signed = match session.presigned.remove(&token_id) {
            Some(slot) if (slot.limit_px - limit_px).abs() < 1e-9 => slot.signed,
            _ => presign_limit_buy(&session.client, &session.signer, &token_id, shares, limit_px)
                .await?,
        };

        let resp = session.client.post_order(signed).await.context("post order")?;
        if !resp.success {
            return Err(anyhow!(
                "post order failed: {}",
                resp.error_msg.unwrap_or_else(|| resp.order_id.clone())
            ));
        }

        let fill_px = resolve_actual_fill(&session.client, &resp, &token_id, shares).await?;
        let _ = session.client.cancel_order(&resp.order_id).await;

        warn!(
            "[LIVE] GTC BUY {:?} {:.0}sh limit={:.3} fill={:.3} intent={:.3} reason={} order_id={} status={:?}",
            side,
            shares,
            limit_px,
            fill_px,
            intent_px,
            reason,
            resp.order_id,
            resp.status
        );

        if let Ok(signed) = presign_limit_buy(
            &session.client,
            &session.signer,
            &token_id,
            shares,
            max_limit,
        )
        .await
        {
            session.presigned.insert(
                token_id,
                PresignedSlot {
                    limit_px: max_limit,
                    signed,
                },
            );
        } else {
            warn!(
                "presign replenish {} failed",
                token_id.chars().take(12).collect::<String>()
            );
        }

        Ok(fill_px)
    }

    async fn presign_token(&mut self, token_id: &str, limit_px: f64) -> Result<()> {
        if !self.live {
            return Ok(());
        }
        let session = self
            .session
            .as_mut()
            .context("live session not initialized")?;
        let signed = presign_limit_buy(
            &session.client,
            &session.signer,
            token_id,
            self.shares,
            limit_px,
        )
        .await?;
        session.presigned.insert(
            token_id.to_string(),
            PresignedSlot {
                limit_px,
                signed,
            },
        );
        Ok(())
    }
}

async fn presign_limit_buy(
    client: &Client<Authenticated<Normal>>,
    signer: &PrivateKeySigner,
    token_id: &str,
    shares: f64,
    limit_px: f64,
) -> Result<SignedOrder> {
    let tid = token_id.parse().context("token id parse")?;
    let price = Decimal::from_str(&format!("{:.2}", limit_px)).context("price parse")?;
    let size = Decimal::from_str(&format!("{:.2}", shares)).context("size parse")?;

    let order = client
        .limit_order()
        .token_id(tid)
        .price(price)
        .size(size)
        .side(SdkSide::Buy)
        .order_type(OrderType::GTC)
        .build()
        .await
        .context("build GTC limit order")?;

    client.sign(signer, order).await.context("sign order")
}

fn signature_type_from_cfg(cfg: &AppConfig) -> SignatureType {
    match cfg.signature_type {
        Some(1) => SignatureType::Proxy,
        Some(2) => SignatureType::GnosisSafe,
        Some(3) => SignatureType::Poly1271,
        _ => SignatureType::Eoa,
    }
}

fn clamp_price(n: f64) -> f64 {
    ((n * 100.0).round() / 100.0).clamp(0.01, 0.99)
}

fn dec_f64(d: Decimal) -> f64 {
    d.to_f64().unwrap_or(0.0)
}

fn fill_from_amounts(making: Decimal, taking: Decimal, shares: f64) -> Option<f64> {
    let taking = dec_f64(taking);
    let making = dec_f64(making);
    if taking > 1e-9 {
        Some((making / taking).clamp(0.01, 0.99))
    } else if making > 1e-9 && shares > 0.0 {
        Some((making / shares).clamp(0.01, 0.99))
    } else {
        None
    }
}

/// Average fill from post_order amounts, else poll order/trades until matched.
async fn resolve_actual_fill(
    client: &Client<Authenticated<Normal>>,
    resp: &PostOrderResponse,
    token_id: &str,
    shares: f64,
) -> Result<f64> {
    if let Some(px) = fill_from_amounts(resp.making_amount, resp.taking_amount, shares) {
        return Ok(px);
    }

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
        - 30;
    let trades_req = match token_id.parse() {
        Ok(id) => TradesRequest::builder()
            .asset_id(id)
            .after(after)
            .build(),
        Err(_) => TradesRequest::builder().after(after).build(),
    };

    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        if let Ok(page) = client.trades(&trades_req, None).await {
            let mut notional = 0.0;
            let mut qty = 0.0;
            for tr in &page.data {
                if tr.taker_order_id == resp.order_id || resp.trade_ids.iter().any(|id| id == &tr.id)
                {
                    let sz = dec_f64(tr.size);
                    let px = dec_f64(tr.price);
                    if sz > 0.0 && px > 0.0 {
                        notional += sz * px;
                        qty += sz;
                    }
                }
            }
            if qty > 1e-9 {
                return Ok((notional / qty).clamp(0.01, 0.99));
            }
        }

        if let Ok(o) = client.order(&resp.order_id).await {
            let matched = dec_f64(o.size_matched);
            if matched > 0.0 {
                if matches!(
                    o.status,
                    OrderStatusType::Matched | OrderStatusType::Canceled
                ) {
                    // Limit field is the order price, not VWAP — keep polling trades.
                }
            }
        }
    }

    Err(anyhow!(
        "no actual fill for order {} after 0.99 GTC post",
        resp.order_id
    ))
}
