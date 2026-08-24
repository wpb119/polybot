use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct Config {
    pub live_trading: bool,
    pub private_key: Option<String>,
    pub funder: Option<String>,
    pub signature_type: Option<u8>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
    pub order_shares: f64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let live_trading = env_bool("LIVE_TRADING", false);
        Ok(Self {
            live_trading,
            private_key: env_opt("POLYMARKET_PRIVATE_KEY"),
            funder: env_opt("POLYMARKET_FUNDER"),
            signature_type: env_opt("POLYMARKET_SIGNATURE_TYPE").map(|s| s.parse().unwrap_or(2)),
            api_key: env_opt("POLYMARKET_API_KEY"),
            api_secret: env_opt("POLYMARKET_API_SECRET"),
            api_passphrase: env_opt("POLYMARKET_API_PASSPHRASE"),
            order_shares: env_f64("ORDER_SHARES", 10.0),
        })
    }

    pub fn validate_for_live(&self) -> Result<()> {
        if !self.live_trading {
            return Ok(());
        }
        self.private_key
            .as_ref()
            .filter(|k| !k.is_empty())
            .context("LIVE_TRADING=true requires POLYMARKET_PRIVATE_KEY")?;
        Ok(())
    }
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn env_bool(key: &str, default: bool) -> bool {
    match env_opt(key) {
        Some(v) => v == "true" || v == "1",
        None => default,
    }
}

fn env_f64(key: &str, default: f64) -> f64 {
    env_opt(key)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
