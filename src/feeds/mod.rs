mod binance;
mod chainlink;
mod coinbase;
mod polymarket;

pub use binance::{spawn_binance, BtcQuote};
pub use chainlink::spawn_chainlink;
pub use coinbase::spawn_coinbase;
pub use polymarket::{spawn_polymarket, PolyQuote};
