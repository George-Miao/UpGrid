//! UpGrid process lifecycle and orchestration.

use std::str::FromStr;

use clap::{Parser, Subcommand};
use snafu::ResultExt;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use upgrid_config::{Config, ConfigArgs};
use upgrid_raft::Handle;
use upgrid_raft::domain::{Command, DEFAULT_HISTORY_RETENTION_MS};

mod bootstrap;
mod error;
mod scheduler;
mod worker;

use crate::error::{ClusterWriteSnafu, Error, Result};

#[derive(Debug, Parser)]
#[command(name = "upgrid", version, about = "Distributed service monitor")]
struct Cli {
    #[command(flatten)]
    config: ConfigArgs,

    #[command(subcommand)]
    command: Option<CliCommand>,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    #[command(name = "print-openapi")]
    PrintOpenApi,
}

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

async fn run() -> Result<()> {
    let Cli { config, command } = Cli::parse();
    if matches!(command, Some(CliCommand::PrintOpenApi)) {
        print!("{}", upgrid_api::openapi_json()?);
        return Ok(());
    }
    let config = Config::load(config)?;
    let ready = bootstrap::prepare(config).await?;
    let bootstrap::Ready {
        mut config,
        node,
        cipher,
        mut node_name,
        oobe,
        startup_warning,
        bootstrapping,
    } = ready;
    let node_id = node.node_id();
    if let Some(replicated) = node
        .local_application_state()
        .node_names
        .get(&node_id)
        .cloned()
    {
        node_name = replicated;
        config.node_name = Some(node_name.clone());
    } else {
        node.apply(Command::SetNodeName {
            node_id,
            name: node_name,
        })
        .await
        .context(ClusterWriteSnafu)?;
    }
    if let Some(retention_ms) = config
        .history_retention_ms
        .or_else(|| bootstrapping.then_some(DEFAULT_HISTORY_RETENTION_MS))
    {
        node.apply(Command::SetHistoryRetention { retention_ms })
            .await
            .context(ClusterWriteSnafu)?;
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
    Err(Error::ClusterStopped)
}

#[cfg(test)]
mod cli_tests {
    use clap::Parser as _;

    use super::{Cli, CliCommand};

    #[test]
    fn print_openapi_subcommand_is_selected() {
        let cli = Cli::try_parse_from(["upgrid", "print-openapi"]).unwrap();
        assert!(matches!(cli.command, Some(CliCommand::PrintOpenApi)));
    }

    #[test]
    fn legacy_print_openapi_flag_is_rejected() {
        assert!(Cli::try_parse_from(["upgrid", "--print-openapi"]).is_err());
    }

    #[test]
    fn run_flags_remain_at_the_root() {
        let cli =
            Cli::try_parse_from(["upgrid", "--bind", "127.0.0.1:9000", "--new-cluster"]).unwrap();
        assert!(cli.command.is_none());
    }
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
