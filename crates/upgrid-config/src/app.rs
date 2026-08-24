//! Command-line and environment configuration.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs, io};

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use url::Url;
use uuid::Uuid;

use crate::cli::ConfigArgs;
use crate::error::{
    ConfiguredReachableAddressSnafu, LoadSnafu, NodeIdSnafu, ReadSnafu, WriteSnafu,
};
use crate::{Cipher, Error, JoinLink, QuicCaKey, ReachableAddress, Result, durable};

#[derive(Clone)]
pub enum JoinIntent {
    Valid(Box<JoinLink>),
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct LocalAddress {
    pub host: IpAddr,
    pub port: u16,
}

#[derive(Clone)]
pub struct Config {
    pub bind: String,
    pub local_addresses: BTreeSet<LocalAddress>,
    pub reachable_addresses: BTreeSet<ReachableAddress>,
    pub reachable_addresses_explicit: bool,
    pub discovery_urls: BTreeSet<Url>,
    pub discovery_urls_explicit: bool,
    pub join: Option<JoinIntent>,
    pub new_cluster: bool,
    pub data_dir: PathBuf,
    pub node_name: Option<String>,
    pub username: String,
    pub password: String,
    pub deployment_key: Option<String>,
    pub quic_ca_key: Option<String>,
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
    local_addresses: BTreeSet<IpAddr>,
    raft_port: u16,
    raft_url: Option<String>,
    reachable_addresses: Option<BTreeSet<String>>,
    discovery_urls: Option<BTreeSet<Url>>,
    join: Option<String>,
    new_cluster: bool,
    data_dir: PathBuf,
    node_name: Option<String>,
    username: String,
    password: String,
    deployment_key: Option<String>,
    quic_ca_key: Option<String>,
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
            local_addresses: BTreeSet::from([IpAddr::V4(Ipv4Addr::LOCALHOST)]),
            raft_port: 11451,
            raft_url: None,
            reachable_addresses: None,
            discovery_urls: None,
            join: None,
            new_cluster: false,
            data_dir: PathBuf::from("upgrid-data"),
            node_name: None,
            username: "admin".to_owned(),
            password: String::new(),
            deployment_key: None,
            quic_ca_key: None,
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
    override_value!(local_addresses);
    override_value!(raft_port);
    override_value!(reachable_addresses);
    override_value!(discovery_urls);
    override_value!(join);
    override_value!(data_dir);
    override_value!(node_name);
    override_value!(username);
    override_value!(password);
    override_value!(deployment_key);
    override_value!(quic_ca_key);
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
        if raw.raft_url.is_some() {
            return Err(Error::ObsoleteRaftUrl);
        }
        if raw.raft_port == 0 {
            return Err(Error::InvalidConfiguration {
                message: "raft_port must be nonzero",
            });
        }
        if raw.local_addresses.is_empty() {
            return Err(Error::InvalidConfiguration {
                message: "local_addresses must contain at least one IP address",
            });
        }
        if raw
            .discovery_urls
            .as_ref()
            .is_some_and(|urls| urls.len() > crate::MAX_DISCOVERY_SERVICES)
        {
            return Err(Error::InvalidConfiguration {
                message: "discovery_urls contains more than 8 services",
            });
        }
        if raw.discovery_urls.as_ref().is_some_and(|urls| {
            urls.iter()
                .any(|url| !crate::discovery::is_supported_discovery_url(url))
        }) {
            return Err(Error::InvalidConfiguration {
                message: "discovery_urls must use http or https and contain no credentials, \
                          query, or fragment",
            });
        }
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
        let reachable_addresses_explicit = raw.reachable_addresses.is_some();
        let reachable_addresses = raw
            .reachable_addresses
            .unwrap_or_default()
            .into_iter()
            .map(|value| {
                ReachableAddress::parse(&value).context(ConfiguredReachableAddressSnafu { value })
            })
            .collect::<Result<_>>()?;
        let discovery_urls_explicit = raw.discovery_urls.is_some();
        let discovery_urls = raw.discovery_urls.unwrap_or_default();
        let local_addresses = raw
            .local_addresses
            .into_iter()
            .map(|host| LocalAddress {
                host,
                port: raw.raft_port,
            })
            .collect();
        Ok(Self {
            bind: raw.bind,
            local_addresses,
            discovery_urls,
            discovery_urls_explicit,
            reachable_addresses,
            reachable_addresses_explicit,
            join: raw.join.map(|value| match JoinLink::parse(&value) {
                Ok(link) => JoinIntent::Valid(Box::new(link)),
                Err(_) => JoinIntent::Invalid,
            }),
            new_cluster: raw.new_cluster,
            data_dir: raw.data_dir,
            node_name: raw.node_name,
            username: raw.username,
            password: raw.password,
            deployment_key: raw.deployment_key,
            quic_ca_key: raw.quic_ca_key,
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

pub fn load_or_create_quic_ca_key(
    data_dir: &Path,
    configured: Option<&QuicCaKey>,
    cipher: &Cipher,
) -> Result<QuicCaKey> {
    let path = data_dir.join("quic-ca-key");
    let stored = match fs::read_to_string(&path) {
        Ok(value) => Some(QuicCaKey::parse(&value)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(source) => return Err(source).context(ReadSnafu { path }),
    };
    match (stored, configured) {
        (Some(stored), Some(configured)) if stored != *configured => Err(Error::QuicCaKeyMismatch),
        (Some(stored), _) => Ok(stored),
        (None, configured) => {
            let key = configured
                .cloned()
                .unwrap_or_else(|| QuicCaKey::derive(cipher));
            durable::replace_private(&path, key.encoded().as_bytes())
                .context(WriteSnafu { path })?;
            Ok(key)
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
mod tests;
