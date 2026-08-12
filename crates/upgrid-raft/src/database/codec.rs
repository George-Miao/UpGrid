use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::DatabaseError;

pub(super) fn encode_field<T>(
    table: &'static str,
    column: &'static str,
    value: &T,
) -> Result<Vec<u8>, DatabaseError>
where
    T: Serialize + ?Sized,
{
    serde_json::to_vec(value).map_err(|source| DatabaseError::FieldEncode {
        table,
        column,
        source,
    })
}

pub(super) fn decode_field<T>(
    table: &'static str,
    column: &'static str,
    bytes: &[u8],
) -> Result<T, DatabaseError>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(bytes).map_err(|source| DatabaseError::FieldDecode {
        table,
        column,
        source,
    })
}

pub(super) fn integer(
    table: &'static str,
    column: &'static str,
    value: u64,
) -> Result<i64, DatabaseError> {
    i64::try_from(value).map_err(|source| DatabaseError::IntegerRange {
        table,
        column,
        value,
        source,
    })
}
