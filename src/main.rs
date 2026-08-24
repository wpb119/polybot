mod bot;
mod clob;
mod config;
mod feeds;
mod gamma;
mod pnl;
mod poly_book;
mod strategy;

use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("rustls crypto provider");

    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,polybot=debug")),
        )
        .init();

    let cfg = config::Config::from_env()?;
    info!(
        "polybot — Pairing strategy (BTC BN+CB impulse, pair exit)"
    );
    if !cfg.live_trading {
        info!("DRY RUN: set LIVE_TRADING=true and wallet env vars to post orders");
    }

    let mut bot = bot::Bot::new(cfg)?;
    bot.run().await?;
    Ok(())
}
