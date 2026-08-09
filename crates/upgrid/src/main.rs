//! UpGrid process lifecycle and orchestration.

use std::str::FromStr;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use upgrid_config::{Action, AppResult};
use upgrid_raft::domain::{Command, DEFAULT_HISTORY_RETENTION_MS};
use upgrid_raft::{Handle, Req};

mod bootstrap;
mod scheduler;
mod worker;

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
        .with_target("upgrid_api", level)
        .with_target("upgrid_notification", level)
        .with_target("upgrid_raft", level)
        .with_target("upgrid_transport", level)
        .with_target("openraft", LevelFilter::OFF)
        .with_target("rustls", LevelFilter::WARN)
}

async fn run() -> AppResult<()> {
    let Some(action) = Action::from_env_and_args()? else {
        return Ok(());
    };
    let Action::Run(config) = action else {
        print!("{}", upgrid_api::openapi_json()?);
        return Ok(());
    };
    let ready = bootstrap::prepare(*config).await?;
    let bootstrap::Ready {
        config,
        node,
        cipher,
        node_name,
        oobe,
        startup_warning,
        bootstrapping,
    } = ready;
    let node_id = node.node_id();
    node.write(Req {
        operation_id: uuid::Uuid::now_v7(),
        submitted_at_ms: upgrid_config::now_ms(),
        command: Command::SetNodeName {
            node_id,
            name: node_name,
        },
    })
    .await
    .map_err(std::io::Error::other)?;
    if let Some(retention_ms) = config
        .history_retention_ms
        .or_else(|| bootstrapping.then_some(DEFAULT_HISTORY_RETENTION_MS))
    {
        node.write(Req {
            operation_id: uuid::Uuid::now_v7(),
            submitted_at_ms: upgrid_config::now_ms(),
            command: Command::SetHistoryRetention { retention_ms },
        })
        .await
        .map_err(std::io::Error::other)?;
    }
    if config.username == "admin" && config.password == "upgrid" {
        tracing::warn!(
            "using default credentials; set UPGRID_USERNAME and UPGRID_PASSWORD before exposing \
             the API"
        );
    }
    let (cluster, receiver) = Handle::new(node_id);
    let notifications = upgrid_notification::start(cluster.clone(), cipher.clone());
    upgrid_api::start(
        config,
        cluster.clone(),
        cipher.clone(),
        notifications,
        oobe,
        startup_warning,
    )?;
    worker::start(cluster.clone(), cipher.clone());
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
        assert!(filter.would_enable("upgrid_raft::node", &Level::INFO));
        assert!(!filter.would_enable("upgrid_raft::node", &Level::DEBUG));
        assert!(!filter.would_enable("tarpc::client", &Level::INFO));
    }

    #[test]
    fn openraft_is_fully_muted() {
        let filter = log_filter(LevelFilter::DEBUG);
        assert!(!filter.would_enable("openraft::replication", &Level::ERROR));
        assert!(!filter.would_enable("openraft_rt_compio", &Level::ERROR));
    }
}
