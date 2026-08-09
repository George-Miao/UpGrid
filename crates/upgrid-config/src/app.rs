//! Command-line and environment configuration.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs, io};

use uuid::Uuid;

use crate::{Cipher, JoinLink, durable};

pub type AppResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone)]
pub struct Config {
    pub bind: String,
    pub raft_url: String,
    pub join: Option<JoinLink>,
    pub data_dir: PathBuf,
    pub username: String,
    pub password: String,
    pub secret_key: Option<String>,
    pub history_retention_ms: Option<u64>,
}

impl Config {
    fn from_env() -> AppResult<Self> {
        let config = Self {
            bind: env::var("UPGRID_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned()),
            raft_url: env::var("UPGRID_RAFT_URL")
                .unwrap_or_else(|_| "up://127.0.0.1:11451".to_owned()),
            join: None,
            data_dir: env::var_os("UPGRID_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("upgrid-data")),
            username: env::var("UPGRID_USERNAME").unwrap_or_else(|_| "admin".to_owned()),
            password: env::var("UPGRID_PASSWORD").unwrap_or_else(|_| "upgrid".to_owned()),
            secret_key: env::var("UPGRID_SECRET_KEY").ok(),
            history_retention_ms: env::var("UPGRID_HISTORY_RETENTION_HOURS")
                .ok()
                .map(|value| parse_history_retention(&value))
                .transpose()?,
        };

        Ok(config)
    }
}

pub enum Action {
    Run(Box<Config>),
    PrintOpenApi,
}

impl Action {
    pub fn from_env_and_args() -> AppResult<Option<Self>> {
        let mut config = Config::from_env()?;
        let mut join = env::var("UPGRID_JOIN").ok();
        let mut args = env::args().skip(1);
        while let Some(argument) = args.next() {
            let value = match argument.as_str() {
                "-h" | "--help" => {
                    print_help();
                    return Ok(None);
                }
                "--print-openapi" => {
                    return Ok(Some(Self::PrintOpenApi));
                }
                "--bind"
                | "--raft-url"
                | "--join"
                | "--data-dir"
                | "--username"
                | "--password"
                | "--secret-key"
                | "--history-retention-hours" => args.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing value for {argument}"),
                    )
                })?,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown argument: {argument}"),
                    )
                    .into());
                }
            };
            match argument.as_str() {
                "--bind" => config.bind = value,
                "--raft-url" => config.raft_url = value,
                "--join" => join = Some(value),
                "--data-dir" => config.data_dir = PathBuf::from(value),
                "--username" => config.username = value,
                "--password" => config.password = value,
                "--secret-key" => config.secret_key = Some(value),
                "--history-retention-hours" => {
                    config.history_retention_ms = Some(parse_history_retention(&value)?)
                }
                _ => unreachable!(),
            }
        }
        config.join = join.map(|value| JoinLink::parse(&value)).transpose()?;
        Ok(Some(Self::Run(Box::new(config))))
    }
}

fn print_help() {
    println!(
        "UpGrid service monitor\n\nUsage: upgrid [OPTIONS]\n\nOptions:\n  --bind ADDRESS       \
         API address [default: 127.0.0.1:8080]\n  --raft-url URL       advertised Raft URL \
         [default: up://127.0.0.1:11451]\n  --join JOIN_LINK     join with a single-use up:// \
         invitation\n  --data-dir PATH      persistent data directory [default: upgrid-data]\n  \
         --username USER      Basic Auth username [default: admin]\n  --password PASSWORD  Basic \
         Auth password [default: upgrid]\n  --secret-key BASE64  bootstrap or recovery deployment \
         key\n  --history-retention-hours HOURS\n  retain raw evaluations [default: 24]\n  \
         --print-openapi      print generated OpenAPI JSON and exit\n  -h, --help           show \
         this help\n\nThe same settings are available as UPGRID_BIND, \
         UPGRID_RAFT_URL,\nUPGRID_JOIN, UPGRID_DATA_DIR, \
         UPGRID_USERNAME,\nUPGRID_PASSWORD,\nUPGRID_SECRET_KEY, and \
         UPGRID_HISTORY_RETENTION_HOURS."
    );
}

fn parse_history_retention(value: &str) -> io::Result<u64> {
    let hours = value.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "history retention must be a positive integer number of hours",
        )
    })?;
    hours
        .checked_mul(60 * 60 * 1_000)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "history retention is zero or too large",
            )
        })
}

pub fn load_or_create_cipher(
    data_dir: &Path,
    configured: Option<&Cipher>,
    joining: bool,
) -> AppResult<Cipher> {
    let path = data_dir.join("deployment-key");
    let stored = match fs::read_to_string(&path) {
        Ok(value) => Some(Cipher::parse(&value)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    match (stored, configured) {
        (Some(stored), Some(configured)) if stored.encoded() != configured.encoded() => {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "configured deployment key does not match the data directory",
            )
            .into())
        }
        (Some(stored), _) => Ok(stored),
        (None, Some(configured)) => {
            durable::replace_private(&path, configured.encoded().as_bytes())?;
            Ok(configured.clone())
        }
        (None, None) if joining => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "joining a Cluster requires a valid up:// invitation",
        )
        .into()),
        (None, None) => {
            let generated = Cipher::generate()?;
            durable::replace_private(&path, generated.encoded().as_bytes())?;
            Ok(generated)
        }
    }
}

pub fn load_or_create_node_id(data_dir: &Path) -> AppResult<Uuid> {
    let path = data_dir.join("node-id");
    match fs::read_to_string(&path) {
        Ok(value) => Ok(Uuid::parse_str(value.trim())?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let id = Uuid::now_v7();
            durable::replace(&path, id.to_string().as_bytes())?;
            Ok(id)
        }
        Err(error) => Err(error.into()),
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
    use super::*;

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
}
