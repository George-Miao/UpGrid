//! Command-line and environment configuration.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs, io};

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use uuid::Uuid;

use crate::cli::ConfigArgs;
use crate::error::{LoadSnafu, NodeIdSnafu, ReadSnafu, WriteSnafu};
use crate::{Cipher, Error, JoinLink, Result, durable};

#[derive(Clone)]
pub enum JoinIntent {
    Valid(Box<JoinLink>),
    Invalid,
}

#[derive(Clone)]
pub struct Config {
    pub bind: String,
    pub raft_url: String,
    pub join: Option<JoinIntent>,
    pub new_cluster: bool,
    pub data_dir: PathBuf,
    pub node_name: Option<String>,
    pub username: String,
    pub password: String,
    pub secret_key: Option<String>,
    pub history_retention_ms: Option<u64>,
    pub history_rollup_retention_ms: Option<u64>,
    pub target_trash_retention_ms: Option<u64>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

impl Config {
    pub fn load(args: ConfigArgs) -> Result<Self> {
        load_with(args, true)
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct RawConfig {
    bind: String,
    raft_url: String,
    join: Option<String>,
    new_cluster: bool,
    data_dir: PathBuf,
    node_name: Option<String>,
    username: String,
    password: String,
    secret_key: Option<String>,
    history_retention_hours: Option<u64>,
    history_rollup_retention_days: Option<u64>,
    target_trash_retention_days: Option<u64>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
}

impl Default for RawConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".to_owned(),
            raft_url: "up://127.0.0.1:11451".to_owned(),
            join: None,
            new_cluster: false,
            data_dir: PathBuf::from("upgrid-data"),
            node_name: None,
            username: "admin".to_owned(),
            password: String::new(),
            secret_key: None,
            history_retention_hours: None,
            history_rollup_retention_days: None,
            target_trash_retention_days: None,
            tls_cert: None,
            tls_key: None,
        }
    }
}

fn load_with(args: ConfigArgs, load_env: bool) -> Result<Config> {
    let mut figment = Figment::from(Serialized::defaults(RawConfig::default()));
    if let Some(path) = args.config.or_else(|| {
        load_env
            .then(|| env::var_os("UPGRID_CONFIG"))
            .flatten()
            .map(PathBuf::from)
    }) {
        figment = figment.merge(Toml::file(path));
    }
    if load_env {
        figment = figment.merge(Env::prefixed("UPGRID_"));
    }
    macro_rules! override_value {
        ($field:ident) => {
            if let Some(value) = args.$field {
                figment = figment.merge((stringify!($field), value));
            }
        };
    }
    override_value!(bind);
    override_value!(raft_url);
    override_value!(join);
    override_value!(data_dir);
    override_value!(node_name);
    override_value!(username);
    override_value!(password);
    override_value!(secret_key);
    override_value!(history_retention_hours);
    override_value!(history_rollup_retention_days);
    override_value!(target_trash_retention_days);
    override_value!(tls_cert);
    override_value!(tls_key);
    if args.new_cluster {
        figment = figment.merge(("new_cluster", true));
    }
    RawConfig::try_into(figment.extract().context(LoadSnafu)?)
}

impl TryFrom<RawConfig> for Config {
    type Error = Error;

    fn try_from(raw: RawConfig) -> Result<Self, Self::Error> {
        if raw.new_cluster && raw.join.is_some() {
            return Err(Error::InvalidConfiguration {
                message: "new_cluster and join cannot be configured together",
            });
        }
        if raw.tls_cert.is_some() != raw.tls_key.is_some() {
            return Err(Error::InvalidConfiguration {
                message: "tls_cert and tls_key must be configured together",
            });
        }
        let history_retention_ms = raw
            .history_retention_hours
            .map(history_retention_ms)
            .transpose()?;
        let history_rollup_retention_ms = raw
            .history_rollup_retention_days
            .map(history_rollup_retention_ms)
            .transpose()?;
        let target_trash_retention_ms = raw
            .target_trash_retention_days
            .map(target_trash_retention_ms)
            .transpose()?;
        Ok(Self {
            bind: raw.bind,
            raft_url: raw.raft_url,
            join: raw.join.map(|value| match JoinLink::parse(&value) {
                Ok(link) => JoinIntent::Valid(Box::new(link)),
                Err(_) => JoinIntent::Invalid,
            }),
            new_cluster: raw.new_cluster,
            data_dir: raw.data_dir,
            node_name: raw.node_name,
            username: raw.username,
            password: raw.password,
            secret_key: raw.secret_key,
            history_retention_ms,
            history_rollup_retention_ms,
            target_trash_retention_ms,
            tls_cert: raw.tls_cert,
            tls_key: raw.tls_key,
        })
    }
}

fn history_retention_ms(hours: u64) -> Result<u64> {
    hours
        .checked_mul(60 * 60 * 1_000)
        .filter(|value| *value > 0)
        .ok_or(Error::InvalidConfiguration {
            message: "history retention is zero or too large",
        })
}

fn history_rollup_retention_ms(days: u64) -> Result<u64> {
    days.checked_mul(24 * 60 * 60 * 1_000)
        .filter(|value| *value > 0)
        .ok_or(Error::InvalidConfiguration {
            message: "history rollup retention is zero or too large",
        })
}

fn target_trash_retention_ms(days: u64) -> Result<u64> {
    days.checked_mul(24 * 60 * 60 * 1_000)
        .filter(|value| *value > 0)
        .ok_or(Error::InvalidConfiguration {
            message: "Target trash retention is zero or too large",
        })
}

pub fn load_or_create_cipher(
    data_dir: &Path,
    configured: Option<&Cipher>,
    joining: bool,
) -> Result<Cipher> {
    let path = data_dir.join("deployment-key");
    let stored = match fs::read_to_string(&path) {
        Ok(value) => Some(Cipher::parse(&value)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(source) => return Err(source).context(ReadSnafu { path }),
    };
    match (stored, configured) {
        (Some(stored), Some(configured)) if stored.encoded() != configured.encoded() => {
            Err(Error::DeploymentKeyMismatch)
        }
        (Some(stored), _) => Ok(stored),
        (None, Some(configured)) => {
            durable::replace_private(&path, configured.encoded().as_bytes())
                .context(WriteSnafu { path })?;
            Ok(configured.clone())
        }
        (None, None) if joining => Err(Error::JoinLinkRequired),
        (None, None) => {
            let generated = Cipher::generate()?;
            durable::replace_private(&path, generated.encoded().as_bytes())
                .context(WriteSnafu { path })?;
            Ok(generated)
        }
    }
}

pub fn load_or_create_node_id(data_dir: &Path) -> Result<Uuid> {
    let path = data_dir.join("node-id");
    match fs::read_to_string(&path) {
        Ok(value) => Uuid::parse_str(value.trim()).context(NodeIdSnafu { path }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let id = Uuid::now_v7();
            durable::replace(&path, id.to_string().as_bytes()).context(WriteSnafu { path })?;
            Ok(id)
        }
        Err(source) => Err(source).context(ReadSnafu { path }),
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    #[command(name = "upgrid")]
    struct TestCli {
        #[command(flatten)]
        config: ConfigArgs,
    }

    #[test]
    fn node_identity_survives_reopen() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let first = load_or_create_node_id(&directory).unwrap();
        let second = load_or_create_node_id(&directory).unwrap();
        assert_eq!(first, second);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn deployment_key_survives_reopen_and_is_required_for_join() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let first = load_or_create_cipher(&directory, None, false).unwrap();
        let second = load_or_create_cipher(&directory, None, true).unwrap();
        assert_eq!(first.encoded(), second.encoded());

        let joining = directory.join("joining");
        fs::create_dir_all(&joining).unwrap();
        assert!(load_or_create_cipher(&joining, None, true).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn tls_configuration_requires_a_certificate_and_key() {
        let cert_only = RawConfig {
            tls_cert: Some(PathBuf::from("cert.pem")),
            ..RawConfig::default()
        };
        assert!(Config::try_from(cert_only).is_err());

        let pair = RawConfig {
            tls_cert: Some(PathBuf::from("cert.pem")),
            tls_key: Some(PathBuf::from("key.pem")),
            ..RawConfig::default()
        };
        assert!(Config::try_from(pair).is_ok());
    }

    #[test]
    fn history_retention_windows_use_distinct_units() {
        let config = Config::try_from(RawConfig {
            history_retention_hours: Some(2),
            history_rollup_retention_days: Some(3),
            target_trash_retention_days: Some(4),
            ..RawConfig::default()
        })
        .unwrap();

        assert_eq!(config.history_retention_ms, Some(2 * 60 * 60 * 1_000));
        assert_eq!(
            config.history_rollup_retention_ms,
            Some(3 * 24 * 60 * 60 * 1_000)
        );
        assert_eq!(
            config.target_trash_retention_ms,
            Some(4 * 24 * 60 * 60 * 1_000)
        );
        assert!(
            Config::try_from(RawConfig {
                target_trash_retention_days: Some(0),
                ..RawConfig::default()
            })
            .is_err()
        );
    }

    #[test]
    fn cli_overrides_toml_configuration() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("upgrid.toml");
        fs::write(
            &path,
            "bind = \"127.0.0.1:9000\"\nusername = \"from-file\"\n",
        )
        .unwrap();
        let args = TestCli::try_parse_from([
            "upgrid",
            "--config",
            path.to_str().unwrap(),
            "--bind",
            "127.0.0.1:9001",
        ])
        .unwrap()
        .config;

        let config = load_with(args, false).unwrap();

        assert_eq!(config.bind, "127.0.0.1:9001");
        assert_eq!(config.username, "from-file");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    #[allow(clippy::result_large_err)] // Required by Figment's fixed Jail callback signature.
    fn environment_can_select_a_new_cluster() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("UPGRID_NEW_CLUSTER", "true");
            let raw: RawConfig = Figment::from(Serialized::defaults(RawConfig::default()))
                .merge(Env::prefixed("UPGRID_"))
                .extract()?;
            assert!(raw.new_cluster);
            Ok(())
        });
    }
}
