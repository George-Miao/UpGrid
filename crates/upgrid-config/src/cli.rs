use std::path::PathBuf;

use clap::{ArgAction, Args};

/// Command-line arguments that configure ordinary Node startup.
#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// Read configuration from this TOML file.
    #[arg(long, value_name = "PATH")]
    pub(super) config: Option<PathBuf>,

    /// API listen address.
    #[arg(long, value_name = "ADDRESS")]
    pub(super) bind: Option<String>,

    /// Advertised inter-Node Raft URL.
    #[arg(long, value_name = "URL")]
    pub(super) raft_url: Option<String>,

    /// Join with an up:// invitation.
    #[arg(long, value_name = "JOIN_LINK", conflicts_with = "new_cluster")]
    pub(super) join: Option<String>,

    /// Create a new single-Node Cluster without opening OOBE.
    #[arg(long, action = ArgAction::SetTrue)]
    pub(super) new_cluster: bool,

    /// Persistent Node data directory.
    #[arg(long, value_name = "PATH")]
    pub(super) data_dir: Option<PathBuf>,

    /// Friendly name shown for this Node.
    #[arg(long, value_name = "NAME")]
    pub(super) node_name: Option<String>,

    /// Initial administrator username for unattended setup or one-time
    /// migration.
    #[arg(long, value_name = "USER")]
    pub(super) username: Option<String>,

    /// Initial administrator password for unattended setup or one-time
    /// migration.
    #[arg(long, value_name = "PASSWORD")]
    pub(super) password: Option<String>,

    /// Bootstrap or recovery deployment key.
    #[arg(long, value_name = "BASE64")]
    pub(super) secret_key: Option<String>,

    /// Raw Evaluation retention period.
    #[arg(long, value_name = "HOURS")]
    pub(super) history_retention_hours: Option<u64>,

    /// Long-term hourly Evaluation rollup retention period.
    #[arg(long, value_name = "DAYS")]
    pub(super) history_rollup_retention_days: Option<u64>,

    /// Deleted Target retention period.
    #[arg(long, value_name = "DAYS")]
    pub(super) target_trash_retention_days: Option<u64>,

    /// PEM certificate chain for API HTTPS.
    #[arg(long, value_name = "PATH", requires = "tls_key")]
    pub(super) tls_cert: Option<PathBuf>,

    /// PEM private key for API HTTPS.
    #[arg(long, value_name = "PATH", requires = "tls_cert")]
    pub(super) tls_key: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::ConfigArgs;

    #[derive(Debug, Parser)]
    #[command(name = "upgrid")]
    struct TestCli {
        #[command(flatten)]
        _config: ConfigArgs,
    }

    #[test]
    fn new_cluster_and_direct_join_conflict() {
        assert!(
            TestCli::try_parse_from(["upgrid", "--new-cluster", "--join", "up://node/token"])
                .is_err()
        );
    }

    #[test]
    fn new_cluster_flag_is_accepted() {
        assert!(TestCli::try_parse_from(["upgrid", "--new-cluster"]).is_ok());
    }

    #[test]
    fn setup_flag_is_removed() {
        assert!(TestCli::try_parse_from(["upgrid", "--setup"]).is_err());
    }

    #[test]
    fn tls_paths_are_a_pair() {
        assert!(TestCli::try_parse_from(["upgrid", "--tls-cert", "cert.pem"]).is_err());
        assert!(
            TestCli::try_parse_from(["upgrid", "--tls-cert", "cert.pem", "--tls-key", "key.pem",])
                .is_ok()
        );
    }
}
