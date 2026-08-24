use std::net::IpAddr;
use std::path::PathBuf;

use clap::{ArgAction, Args};
use url::Url;

/// Command-line arguments that configure ordinary node startup.
#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// Read configuration from this TOML file.
    #[arg(long, value_name = "PATH")]
    pub(super) config: Option<PathBuf>,

    /// API listen address.
    #[arg(long, value_name = "ADDRESS")]
    pub(super) bind: Option<String>,

    /// Local IP address for inter-node Raft traffic. Repeat for each address.
    #[arg(long = "local-address", value_name = "IP", action = ArgAction::Append)]
    pub(super) local_addresses: Option<Vec<IpAddr>>,

    /// Shared local port for inter-node Raft traffic.
    #[arg(long, value_name = "PORT")]
    pub(super) raft_port: Option<u16>,

    /// Reachable inter-node URL. Repeat for each address.
    #[arg(long = "reachable-address", value_name = "UP_URL", action = ArgAction::Append)]
    pub(super) reachable_addresses: Option<Vec<String>>,

    /// HTTP service URL that returns reachable address candidates. Repeat for
    /// each service.
    #[arg(long = "discovery-url", value_name = "HTTP_URL", action = ArgAction::Append)]
    pub(super) discovery_urls: Option<Vec<Url>>,

    /// Join with an up:// join link.
    #[arg(long, value_name = "JOIN_LINK", conflicts_with = "new_cluster")]
    pub(super) join: Option<String>,

    /// Create a new single-node cluster without opening OOBE.
    #[arg(long, action = ArgAction::SetTrue)]
    pub(super) new_cluster: bool,

    /// Persistent node data directory.
    #[arg(long, value_name = "PATH")]
    pub(super) data_dir: Option<PathBuf>,

    /// Friendly name shown for this node.
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
    pub(super) deployment_key: Option<String>,

    /// QUIC certificate-authority Ed25519 key seed.
    #[arg(long, value_name = "BASE64")]
    pub(super) quic_ca_key: Option<String>,

    /// Raw evaluation retention period.
    #[arg(long, value_name = "HOURS")]
    pub(super) history_retention_hours: Option<u64>,

    /// Long-term hourly evaluation rollup retention period.
    #[arg(long, value_name = "DAYS")]
    pub(super) history_rollup_retention_days: Option<u64>,

    /// Deleted target retention period.
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
    fn deployment_and_quic_ca_keys_are_explicit() {
        assert!(
            TestCli::try_parse_from([
                "upgrid",
                "--deployment-key",
                "deployment",
                "--quic-ca-key",
                "quic-ca",
            ])
            .is_ok()
        );
        assert!(TestCli::try_parse_from(["upgrid", "--secret-key", "legacy"]).is_err());
    }

    #[test]
    fn obsolete_raft_url_flag_is_rejected() {
        assert!(
            TestCli::try_parse_from(["upgrid", "--raft-url", "up://legacy.example:11451"]).is_err()
        );
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
