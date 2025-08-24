#![feature(thread_local)]

use std::str::FromStr;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{Layer, filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

mod error;
pub mod network;
mod node;
mod raft;
mod state_machine;
mod storage;
mod test;
mod utils;
pub use error::*;

#[compio::main]
async fn main() {
    let level = std::env::var("RUST_LOG")
        .ok()
        .map(|s| LevelFilter::from_str(&s).expect("Invalid log level"))
        .unwrap_or_else(|| LevelFilter::INFO);

    let target_filter = Targets::new()
        .with_default(level)
        .with_target("upgrid", LevelFilter::DEBUG)
        .with_target("rustls", LevelFilter::WARN);

    let fmt = tracing_subscriber::fmt::layer().with_filter(target_filter);

    tracing_subscriber::registry().with(fmt).init();
}
