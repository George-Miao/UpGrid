use std::path::PathBuf;

use clap::{ArgAction, Parser};

#[derive(Debug, Parser)]
#[command(name = "upgrid", version, about = "Distributed service monitor")]
pub(super) struct Cli {
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
    #[arg(long, value_name = "JOIN_LINK", conflicts_with = "setup")]
    pub(super) join: Option<String>,

    /// Wait for a Join Link in the WebUI.
    #[arg(long, action = ArgAction::SetTrue)]
    pub(super) setup: bool,

    /// Persistent Node data directory.
    #[arg(long, value_name = "PATH")]
    pub(super) data_dir: Option<PathBuf>,

    /// Basic Auth username.
    #[arg(long, value_name = "USER")]
    pub(super) username: Option<String>,

    /// Basic Auth password.
    #[arg(long, value_name = "PASSWORD")]
    pub(super) password: Option<String>,

    /// Bootstrap or recovery deployment key.
    #[arg(long, value_name = "BASE64")]
    pub(super) secret_key: Option<String>,

    /// Raw Evaluation retention period.
    #[arg(long, value_name = "HOURS")]
    pub(super) history_retention_hours: Option<u64>,

    /// PEM certificate chain for API HTTPS.
    #[arg(long, value_name = "PATH", requires = "tls_key")]
    pub(super) tls_cert: Option<PathBuf>,

    /// PEM private key for API HTTPS.
    #[arg(long, value_name = "PATH", requires = "tls_cert")]
    pub(super) tls_key: Option<PathBuf>,

    /// Print generated OpenAPI JSON and exit.
    #[arg(long, action = ArgAction::SetTrue)]
    pub(super) print_openapi: bool,
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::Cli;

    #[test]
    fn setup_and_direct_join_conflict() {
        assert!(Cli::try_parse_from(["upgrid", "--setup", "--join", "up://node/token"]).is_err());
    }

    #[test]
    fn tls_paths_are_a_pair() {
        assert!(Cli::try_parse_from(["upgrid", "--tls-cert", "cert.pem"]).is_err());
        assert!(
            Cli::try_parse_from(["upgrid", "--tls-cert", "cert.pem", "--tls-key", "key.pem",])
                .is_ok()
        );
    }
}
