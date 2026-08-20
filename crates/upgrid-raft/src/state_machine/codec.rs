use std::{io, str};

use serde::Serialize;
use snafu::{OptionExt, ResultExt, Snafu};

use super::migrations::{self, CURRENT_VERSION};
use crate::domain::ApplicationState;

const VERSION_TERMINATOR: u8 = b'\n';

pub(super) struct Decoded<T> {
    pub(super) value: T,
    pub(super) migrated: bool,
}

pub(crate) fn encode_snapshot(value: &ApplicationState) -> io::Result<Vec<u8>> {
    encode(value)
}

pub(super) fn decode_snapshot(bytes: &[u8]) -> io::Result<Decoded<ApplicationState>> {
    decode(bytes).map_err(invalid_data)
}

fn decode(bytes: &[u8]) -> Result<Decoded<ApplicationState>, FormatError> {
    let (version, payload) = split(bytes)?;
    let value = migrations::snapshot(version, payload).context(DecodeSnafu)?;
    Ok(Decoded {
        value,
        migrated: version != CURRENT_VERSION,
    })
}

fn encode<T>(value: &T) -> io::Result<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let mut bytes = Vec::with_capacity(CURRENT_VERSION.len() + 1);
    bytes.extend_from_slice(CURRENT_VERSION.as_bytes());
    bytes.push(VERSION_TERMINATOR);
    postcard::to_extend(value, bytes)
        .context(EncodeSnafu)
        .map_err(io::Error::other)
}

fn split(bytes: &[u8]) -> Result<(&str, &[u8]), FormatError> {
    let terminator = bytes
        .iter()
        .position(|byte| *byte == VERSION_TERMINATOR)
        .context(MissingVersionSnafu)?;
    let (version, payload) = bytes.split_at(terminator);
    let version = str::from_utf8(version).context(InvalidVersionSnafu)?;
    Ok((version, &payload[1..]))
}

fn invalid_data(error: FormatError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[derive(Debug, Snafu)]
enum FormatError {
    #[snafu(display("state-machine data has no version string"))]
    MissingVersion,

    #[snafu(display("state-machine version is not UTF-8: {source}"))]
    InvalidVersion { source: str::Utf8Error },

    #[snafu(display("failed to decode state-machine data: {source}"))]
    Decode { source: migrations::Error },

    #[snafu(display("failed to encode state-machine data: {source}"))]
    Encode { source: postcard::Error },
}
