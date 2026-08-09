use std::fmt::{Debug, Display, Formatter};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::network::UpgridNode;
use crate::secret::Cipher;

const VERSION: u8 = 1;

/// A short-lived bearer invitation containing everything a new Node needs to
/// join.
#[derive(Clone)]
pub struct JoinLink {
    url: Url,
    remote: Url,
    cipher: Cipher,
    token: String,
}

#[derive(Serialize, Deserialize)]
struct Payload {
    version: u8,
    deployment_key: String,
    token: String,
}

impl JoinLink {
    pub fn issue(raft_url: &str, cipher: &Cipher, token: String) -> Result<Self, Error> {
        let remote = parse_remote(raft_url)?;
        let payload = postcard::to_stdvec(&Payload {
            version: VERSION,
            deployment_key: cipher.encoded(),
            token: token.clone(),
        })
        .map_err(|_| Error::InvalidPayload)?;
        let invitation = URL_SAFE_NO_PAD.encode(payload);
        let mut url = remote.clone();
        url.set_path(&format!("/{invitation}"));

        Ok(Self {
            url,
            remote,
            cipher: cipher.clone(),
            token,
        })
    }

    pub fn parse(value: &str) -> Result<Self, Error> {
        let url = Url::parse(value).map_err(|_| Error::InvalidUrl)?;
        if url.scheme() != "up" {
            return Err(Error::InvalidScheme);
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::InvalidShape);
        }
        let invitation = url
            .path()
            .strip_prefix('/')
            .filter(|value| !value.is_empty() && !value.contains('/'))
            .ok_or(Error::InvalidShape)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(invitation)
            .map_err(|_| Error::InvalidPayload)?;
        let payload: Payload = postcard::from_bytes(&bytes).map_err(|_| Error::InvalidPayload)?;
        if payload.version != VERSION {
            return Err(Error::UnsupportedVersion(payload.version));
        }
        if payload.token.is_empty() {
            return Err(Error::InvalidPayload);
        }
        let cipher = Cipher::parse(&payload.deployment_key).map_err(|_| Error::InvalidPayload)?;
        let mut remote = url.clone();
        remote.set_path("");
        parse_remote(remote.as_str())?;

        Ok(Self {
            url,
            remote,
            cipher,
            token: payload.token,
        })
    }

    pub fn remote(&self) -> &Url {
        &self.remote
    }

    pub fn cipher(&self) -> &Cipher {
        &self.cipher
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

impl Display for JoinLink {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.url, formatter)
    }
}

impl Debug for JoinLink {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JoinLink")
            .field("url", &"[REDACTED]")
            .finish()
    }
}

fn parse_remote(value: &str) -> Result<Url, Error> {
    let mut url = Url::parse(value).map_err(|_| Error::InvalidUrl)?;
    if url.scheme() != "up"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(Error::InvalidShape);
    }
    UpgridNode::new(url.clone()).map_err(|_| Error::InvalidNode)?;
    url.set_path("");
    Ok(url)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidUrl,
    InvalidScheme,
    InvalidShape,
    InvalidPayload,
    UnsupportedVersion(u8),
    InvalidNode,
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUrl => formatter.write_str("join link is not a valid URL"),
            Self::InvalidScheme => formatter.write_str("join link must use the `up` scheme"),
            Self::InvalidShape => formatter.write_str("join link has an invalid shape"),
            Self::InvalidPayload => formatter.write_str("join link payload is invalid"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "join link version {version} is not supported")
            }
            Self::InvalidNode => formatter.write_str("join link contains an invalid Node address"),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher() -> Cipher {
        Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap()
    }

    #[test]
    fn join_link_round_trips_without_exposing_secret_in_debug() {
        let link = JoinLink::issue(
            "up://127.0.0.1:11451",
            &cipher(),
            "single-use-token".to_owned(),
        )
        .unwrap();
        let encoded = link.to_string();
        let parsed = JoinLink::parse(&encoded).unwrap();

        assert!(encoded.starts_with("up://127.0.0.1:11451/"));
        assert_eq!(parsed.remote().as_str(), "up://127.0.0.1:11451");
        assert_eq!(parsed.token(), "single-use-token");
        assert_eq!(parsed.cipher().encoded(), cipher().encoded());
        assert_eq!(format!("{parsed:?}"), "JoinLink { url: \"[REDACTED]\" }");
    }

    #[test]
    fn join_link_rejects_wrong_scheme_and_visible_parameters() {
        assert_eq!(
            JoinLink::parse("ups://127.0.0.1:11451/not-an-invite").unwrap_err(),
            Error::InvalidScheme
        );
        assert_eq!(
            JoinLink::parse("up://127.0.0.1:11451/invite?leak=true").unwrap_err(),
            Error::InvalidShape
        );
    }
}
