//! Reusable, revocable Cluster admission links.

use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::net::IpAddr;
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use snafu::ResultExt as _;
use url::{Host, Url};
use uuid::Uuid;

use crate::error::{RemoveSnafu, StoredJoinLinkSnafu, WriteSnafu};
use crate::{Cipher, QuicCaKey};

const VERSION: u8 = 3;
const PENDING_JOIN_FILE_NAME: &str = "pending-join-link";

/// A bearer join link containing everything a new node needs to join.
#[derive(Clone)]
pub struct JoinLink {
    url: Url,
    remote: Url,
    issuer_node_id: Uuid,
    cipher: Cipher,
    quic_ca_key: QuicCaKey,
    token: String,
}

#[derive(Serialize, Deserialize)]
struct Payload {
    version: u8,
    issuer_node_id: Uuid,
    deployment_key: String,
    quic_ca_key: String,
    token: String,
}

impl JoinLink {
    pub fn issue(
        reachable_address: &str,
        issuer_node_id: Uuid,
        cipher: &Cipher,
        quic_ca_key: &QuicCaKey,
        token: String,
    ) -> Result<Self, Error> {
        let remote = parse_remote(reachable_address)?;
        let payload = postcard::to_stdvec(&Payload {
            version: VERSION,
            issuer_node_id,
            deployment_key: cipher.encoded(),
            quic_ca_key: quic_ca_key.encoded(),
            token: token.clone(),
        })
        .map_err(|_| Error::InvalidPayload)?;
        let encoded_payload = URL_SAFE_NO_PAD.encode(payload);
        let mut url = remote.clone();
        url.set_path(&format!("/{encoded_payload}"));

        Ok(Self {
            url,
            issuer_node_id,
            remote,
            cipher: cipher.clone(),
            quic_ca_key: quic_ca_key.clone(),
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
        let encoded_payload = url
            .path()
            .strip_prefix('/')
            .filter(|value| !value.is_empty() && !value.contains('/'))
            .ok_or(Error::InvalidShape)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| Error::InvalidPayload)?;
        let payload: Payload = postcard::from_bytes(&bytes).map_err(|_| Error::InvalidPayload)?;
        if payload.version != VERSION {
            return Err(Error::UnsupportedVersion(payload.version));
        }
        if payload.token.is_empty() {
            return Err(Error::InvalidPayload);
        }
        let cipher = Cipher::parse(&payload.deployment_key).map_err(|_| Error::InvalidPayload)?;
        let quic_ca_key =
            QuicCaKey::parse(&payload.quic_ca_key).map_err(|_| Error::InvalidPayload)?;
        let mut remote = url.clone();
        remote.set_path("");
        parse_remote(remote.as_str())?;

        Ok(Self {
            url,
            remote,
            issuer_node_id: payload.issuer_node_id,
            cipher,
            quic_ca_key,
            token: payload.token,
        })
    }

    pub fn issuer_node_id(&self) -> Uuid {
        self.issuer_node_id
    }

    pub fn remote(&self) -> &Url {
        &self.remote
    }

    pub fn cipher(&self) -> &Cipher {
        &self.cipher
    }

    pub fn quic_ca_key(&self) -> &QuicCaKey {
        &self.quic_ca_key
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
    let valid_host = match url.host() {
        Some(Host::Ipv4(address)) => !address.is_unspecified(),
        Some(Host::Ipv6(address)) => !address.is_unspecified(),
        Some(Host::Domain(host)) => !host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_unspecified()),
        None => false,
    };
    if !valid_host || !matches!(url.port(), Some(1..=u16::MAX)) {
        return Err(Error::InvalidNode);
    }
    url.set_path("");
    Ok(url)
}

#[derive(Clone)]
pub struct PendingJoin {
    pub link: JoinLink,
    pub complete_oobe: bool,
}

pub fn load_pending_join(data_dir: &Path) -> crate::Result<Option<PendingJoin>> {
    let path = data_dir.join(PENDING_JOIN_FILE_NAME);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(crate::Error::Read { path, source }),
    };
    let (phase, encoded) =
        contents
            .split_once('\n')
            .ok_or_else(|| crate::Error::StoredJoinLink {
                path: path.clone(),
                source: Error::InvalidPayload,
            })?;
    let complete_oobe = match phase {
        "complete" => true,
        "continue" => false,
        _ => {
            return Err(crate::Error::StoredJoinLink {
                path,
                source: Error::InvalidPayload,
            });
        }
    };
    let link = JoinLink::parse(encoded.trim()).context(StoredJoinLinkSnafu { path })?;
    Ok(Some(PendingJoin {
        link,
        complete_oobe,
    }))
}

pub fn store_pending_join(
    data_dir: &Path,
    link: &JoinLink,
    complete_oobe: bool,
) -> crate::Result<()> {
    let path = data_dir.join(PENDING_JOIN_FILE_NAME);
    let phase = if complete_oobe {
        "complete"
    } else {
        "continue"
    };
    let contents = format!("{phase}\n{link}");
    crate::durable::replace_private(&path, contents.as_bytes()).context(WriteSnafu { path })
}

pub fn remove_pending_join(data_dir: &Path) -> crate::Result<()> {
    let path = data_dir.join(PENDING_JOIN_FILE_NAME);
    crate::durable::remove(&path).context(RemoveSnafu { path })
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
            Self::InvalidNode => formatter.write_str("join link contains an invalid node address"),
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

    fn quic_ca_key() -> QuicCaKey {
        QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap()
    }

    fn node_id() -> Uuid {
        Uuid::from_u128(1)
    }

    #[test]
    fn join_link_round_trips_without_exposing_secret_in_debug() {
        let link = JoinLink::issue(
            "up://127.0.0.1:11451",
            node_id(),
            &cipher(),
            &quic_ca_key(),
            "reusable-token".to_owned(),
        )
        .unwrap();
        let encoded = link.to_string();
        let parsed = JoinLink::parse(&encoded).unwrap();

        assert!(encoded.starts_with("up://127.0.0.1:11451/"));
        assert_eq!(parsed.remote().as_str(), "up://127.0.0.1:11451");
        assert_eq!(parsed.issuer_node_id(), node_id());
        assert_eq!(parsed.token(), "reusable-token");
        assert_eq!(parsed.cipher().encoded(), cipher().encoded());
        assert_eq!(parsed.quic_ca_key(), &quic_ca_key());
        assert_eq!(format!("{parsed:?}"), "JoinLink { url: \"[REDACTED]\" }");
    }

    #[test]
    fn pending_join_round_trips_in_a_private_file() {
        let directory =
            std::env::temp_dir().join(format!("upgrid-pending-join-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let link = JoinLink::issue(
            "up://127.0.0.1:11451",
            node_id(),
            &cipher(),
            &quic_ca_key(),
            "pending-token".to_owned(),
        )
        .unwrap();

        store_pending_join(&directory, &link, false).unwrap();
        let loaded = load_pending_join(&directory).unwrap().unwrap();
        assert_eq!(loaded.link.to_string(), link.to_string());
        assert!(!loaded.complete_oobe);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let mode = fs::metadata(directory.join(PENDING_JOIN_FILE_NAME))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        store_pending_join(&directory, &link, true).unwrap();
        let loaded = load_pending_join(&directory).unwrap().unwrap();
        assert!(loaded.complete_oobe);

        remove_pending_join(&directory).unwrap();
        assert!(load_pending_join(&directory).unwrap().is_none());
        fs::remove_dir_all(directory).unwrap();
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

    #[test]
    fn join_link_rejects_zero_port_authorities() {
        assert_eq!(
            JoinLink::issue(
                "up://127.0.0.1:0",
                node_id(),
                &cipher(),
                &quic_ca_key(),
                "token".to_owned(),
            )
            .unwrap_err(),
            Error::InvalidNode
        );
        let link = JoinLink::issue(
            "up://127.0.0.1:11451",
            node_id(),
            &cipher(),
            &quic_ca_key(),
            "token".to_owned(),
        )
        .unwrap()
        .to_string()
        .replacen(":11451/", ":0/", 1);
        assert_eq!(JoinLink::parse(&link).unwrap_err(), Error::InvalidNode);
    }

    #[test]
    fn join_link_rejects_unspecified_hosts() {
        for host in ["0.0.0.0", "[::]"] {
            assert_eq!(
                JoinLink::issue(
                    &format!("up://{host}:11451"),
                    node_id(),
                    &cipher(),
                    &quic_ca_key(),
                    "token".to_owned(),
                )
                .unwrap_err(),
                Error::InvalidNode
            );
        }

        let link = JoinLink::issue(
            "up://127.0.0.1:11451",
            node_id(),
            &cipher(),
            &quic_ca_key(),
            "token".to_owned(),
        )
        .unwrap()
        .to_string()
        .replacen("127.0.0.1", "0.0.0.0", 1);
        assert_eq!(JoinLink::parse(&link).unwrap_err(), Error::InvalidNode);
    }
}
