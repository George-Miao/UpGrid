#![feature(thread_local)]

use std::str::FromStr;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{Layer, filter::Targets, layer::SubscriberExt, util::SubscriberInitExt};

mod app;
mod cluster;
mod domain;
mod durable;
mod error;
mod network;
mod node;
mod raft;
mod scheduler;
mod secret;
mod state_machine;
mod storage;
mod test;
mod utils;
mod web;
mod worker;
pub use error::*;

#[compio::main]
async fn main() {
    let level = std::env::var("RUST_LOG")
        .ok()
        .map(|s| LevelFilter::from_str(&s).expect("Invalid log level"))
        .unwrap_or_else(|| LevelFilter::INFO);

    let fmt = tracing_subscriber::fmt::layer().with_filter(log_filter(level));

    tracing_subscriber::registry().with(fmt).init();

    if let Err(error) = run().await {
        tracing::error!(%error, "UpGrid stopped");
        std::process::exit(1);
    }
}

fn log_filter(level: LevelFilter) -> Targets {
    Targets::new()
        .with_default(LevelFilter::WARN)
        .with_target("upgrid", level)
        .with_target("openraft", LevelFilter::OFF)
        .with_target("rustls", LevelFilter::WARN)
}

async fn run() -> app::AppResult<()> {
    let Some(mut config) = app::Config::from_env_and_args()? else {
        return Ok(());
    };
    std::fs::create_dir_all(&config.data_dir)?;
    let cipher = app::load_or_create_cipher(
        &config.data_dir,
        config.secret_key.as_deref(),
        config.join.is_some(),
    )?;
    config.secret_key = None;
    let node_id = app::load_or_create_node_id(&config.data_dir)?;
    let identity = raft::Identity::with_id(node_id, config.raft_url.as_str())?;
    let node = node::Node::open(identity, &config.data_dir, &cipher).await?;
    let bootstrapping = !node.has_membership() && config.join.is_none();
    if node.has_membership() {
        tracing::debug!(%node_id, "resuming persisted Cluster membership");
    } else {
        if let Some(join) = &config.join {
            node.join(
                join.as_str(),
                config
                    .join_token
                    .as_deref()
                    .expect("join token is validated with configuration"),
            )
            .await?;
        } else {
            node.start_cluster().await?;
        }
    }
    if let Some(retention_ms) = config
        .history_retention_ms
        .or_else(|| bootstrapping.then_some(domain::DEFAULT_HISTORY_RETENTION_MS))
    {
        node.write(raft::Req {
            operation_id: utils::uuid_v7_now(),
            submitted_at_ms: app::now_ms(),
            command: domain::Command::SetHistoryRetention { retention_ms },
        })
        .await
        .map_err(std::io::Error::other)?;
    }
    if config.username == "admin" && config.password == "upgrid" {
        tracing::warn!(
            "using default credentials; set UPGRID_USERNAME and UPGRID_PASSWORD before exposing the API"
        );
    }
    let (cluster, receiver) = cluster::Handle::new(node_id);
    web::start(config, cluster.clone(), cipher.clone())?;
    worker::start(cluster, cipher);
    receiver.run(node).await;
    Err(std::io::Error::other("cluster request channel stopped").into())
}

#[cfg(test)]
mod logging_tests {
    use tracing::Level;

    use super::*;

    #[test]
    fn default_filter_only_enables_upgrid_info_and_dependency_warnings() {
        let filter = log_filter(LevelFilter::INFO);
        assert!(filter.would_enable("upgrid::node", &Level::INFO));
        assert!(!filter.would_enable("upgrid::node", &Level::DEBUG));
        assert!(!filter.would_enable("tarpc::client", &Level::INFO));
    }

    #[test]
    fn openraft_is_fully_muted() {
        let filter = log_filter(LevelFilter::DEBUG);
        assert!(!filter.would_enable("openraft::replication", &Level::ERROR));
        assert!(!filter.would_enable("openraft_rt_compio", &Level::ERROR));
    }
}
