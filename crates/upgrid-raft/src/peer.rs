//! Validated Cluster node addresses.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use url::Url;

use crate::Result;
use crate::error::{UrlInvalidHostSnafu, UrlParseSnafu};

const DEFAULT_UP_PORT: u16 = 11451;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgridNode {
    host: String,
    port: u16,
}

impl UpgridNode {
    pub fn new<U, E>(url: U) -> Result<Self>
    where
        U: TryInto<Url, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let url = url
            .try_into()
            .map_err(|source| Box::new(source) as Box<dyn std::error::Error + Send + Sync>)
            .context(UrlParseSnafu)?;
        if url.scheme() != "up" {
            return Err(crate::Error::UrlInvalidScheme { url });
        }
        let host = url
            .host_str()
            .with_context(|| UrlInvalidHostSnafu { url: url.clone() })?
            .to_owned();
        let port = url.port().unwrap_or(DEFAULT_UP_PORT);
        Ok(Self { host, port })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Display for UpgridNode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "up://{}:{}", self.host, self.port)
    }
}
