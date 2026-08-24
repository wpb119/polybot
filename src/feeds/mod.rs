mod binance;
mod coinbase;
mod polymarket;

pub use binance::{spawn_binance, BtcQuote};
pub use coinbase::spawn_coinbase;
pub use polymarket::{spawn_polymarket, PolyQuote};

use tokio::sync::watch;

pub type PriceWatch = watch::Receiver<Option<BtcQuote>>;
