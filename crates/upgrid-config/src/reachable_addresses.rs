use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::net::{IpAddr, Ipv6Addr};
use std::path::Path;
use std::{fs, io};

use serde::{Deserialize, Deserializer, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use url::Url;

use crate::error::{ReadSnafu, StoredReachableAddressSnafu, WriteSnafu};
use crate::{Result, durable};

#[derive(Debug, Snafu)]
pub enum ReachableAddressError {
    #[snafu(display("failed to parse reachable address: {source}"))]
    Parse { source: url::ParseError },
    #[snafu(display("invalid reachable address scheme in {url}; expected `up`"))]
    InvalidScheme { url: Url },
    #[snafu(display("reachable address has no host: {url}"))]
    InvalidHost { url: Url },
    #[snafu(display(
        "reachable address must contain only a routable host and nonzero port: {url}"
    ))]
    InvalidComponents { url: Url },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ReachableAddress {
    host: String,
    port: u16,
}

impl<'de> Deserialize<'de> for ReachableAddress {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "ReachableAddress")]
        struct Fields {
            host: String,
            port: u16,
        }

        let Fields { host, port } = Fields::deserialize(deserializer)?;
        Self::from_host_port(host, port)
            .ok_or_else(|| serde::de::Error::custom("invalid reachable address"))
    }
}

fn is_unicast_candidate(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !address.is_unspecified() && !address.is_multicast() && !address.is_broadcast()
        }
        IpAddr::V6(address) => !address.is_unspecified() && !address.is_multicast(),
    }
}

impl ReachableAddress {
    pub fn from_host_port(mut host: String, port: u16) -> Option<Self> {
        if let Some(address) = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .and_then(|host| host.parse::<Ipv6Addr>().ok())
        {
            host = address.to_string();
        }
        let value = if host.contains(':') {
            format!("up://[{host}]:{port}")
        } else {
            format!("up://{host}:{port}")
        };
        Self::parse(&value).ok()
    }

    pub fn parse(value: &str) -> std::result::Result<Self, ReachableAddressError> {
        let url = Url::parse(value).context(ParseSnafu)?;
        Self::new(url)
    }

    pub fn new(url: Url) -> std::result::Result<Self, ReachableAddressError> {
        if url.scheme() != "up" {
            return Err(ReachableAddressError::InvalidScheme { url });
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || !url.path().is_empty()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ReachableAddressError::InvalidComponents { url });
        }
        let Some(port) = url.port().filter(|port| *port != 0) else {
            return Err(ReachableAddressError::InvalidComponents { url });
        };
        let host = match url
            .host()
            .with_context(|| InvalidHostSnafu { url: url.clone() })?
        {
            url::Host::Domain(host)
                if host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| !is_unicast_candidate(address)) =>
            {
                return Err(ReachableAddressError::InvalidComponents { url });
            }
            url::Host::Domain(host) => host.to_ascii_lowercase(),
            url::Host::Ipv4(address) if is_unicast_candidate(address.into()) => address.to_string(),
            url::Host::Ipv6(address) if is_unicast_candidate(address.into()) => address.to_string(),
            url::Host::Ipv4(_) | url::Host::Ipv6(_) => {
                return Err(ReachableAddressError::InvalidComponents { url });
            }
        };
        Ok(Self { host, port })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Display for ReachableAddress {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "up://[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "up://{}:{}", self.host, self.port)
        }
    }
}

const FILE_NAME: &str = "reachable-addresses";

pub fn load(data_dir: &Path) -> Result<Option<BTreeSet<ReachableAddress>>> {
    let path = data_dir.join(FILE_NAME);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source).context(ReadSnafu { path }),
    };
    let addresses = contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|value| {
            ReachableAddress::parse(value).context(StoredReachableAddressSnafu {
                path: path.clone(),
                value: value.to_owned(),
            })
        })
        .collect::<Result<_>>()?;
    Ok(Some(addresses))
}

pub fn store(data_dir: &Path, addresses: &BTreeSet<ReachableAddress>) -> Result<()> {
    let path = data_dir.join(FILE_NAME);
    let mut contents = String::new();
    for address in addresses {
        contents.push_str(&address.to_string());
        contents.push('\n');
    }
    durable::replace(&path, contents.as_bytes()).context(WriteSnafu { path })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_address_set_survives_restart() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();

        store(&directory, &BTreeSet::new()).unwrap();

        assert_eq!(load(&directory).unwrap(), Some(BTreeSet::new()));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn address_set_survives_restart() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let addresses = BTreeSet::from([
            ReachableAddress::parse("up://first.example:11451").unwrap(),
            ReachableAddress::parse("up://second.example:11451").unwrap(),
        ]);

        store(&directory, &addresses).unwrap();

        assert_eq!(load(&directory).unwrap(), Some(addresses));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn malformed_stored_address_is_rejected() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join(FILE_NAME), "http://invalid.example:11451\n").unwrap();

        let error = load(&directory).unwrap_err();

        assert!(matches!(error, crate::Error::StoredReachableAddress { .. }));
        fs::remove_dir_all(directory).unwrap();
    }
}
