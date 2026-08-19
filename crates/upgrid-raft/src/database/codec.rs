use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

pub(super) fn encode_application(
    value: &crate::domain::ApplicationState,
) -> Result<Vec<u8>, DatabaseError> {
    encode_field("state_machine", "application", &ApplicationRef(value))
}

pub(super) fn decode_application(
    bytes: &[u8],
) -> Result<crate::domain::ApplicationState, DatabaseError> {
    decode_field::<Application>("state_machine", "application", bytes).map(|value| value.0)
}

struct ApplicationRef<'a>(&'a crate::domain::ApplicationState);

impl Serialize for ApplicationRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize_database_json(serializer)
    }
}

struct Application(crate::domain::ApplicationState);

impl<'de> Deserialize<'de> for Application {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::domain::ApplicationState::deserialize_database_json(deserializer).map(Self)
    }
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
