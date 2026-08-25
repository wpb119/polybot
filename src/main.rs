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

    let cfg = polybot::config::Config::from_env()?;
    info!(
        "polybot — strategy={} ({})",
        cfg.strategy.label(),
        match cfg.strategy {
            polybot::config::StrategyKind::Pairing => {
                "BTC BN+CB impulse, pullback entry, pair exit"
            }
            polybot::config::StrategyKind::GapSwing => {
                "major-swing gap capture peak→DOWN / trough→UP + early opposite"
            }
        }
    );
    if !cfg.live_trading {
        info!("DRY RUN: set LIVE_TRADING=true and wallet env vars to post orders");
    }

    let mut bot = polybot::bot::Bot::new(cfg)?;
    bot.run().await?;
    Ok(())
}
