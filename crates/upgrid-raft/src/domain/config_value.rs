use std::string::FromUtf8Error;

use snafu::{OptionExt, ResultExt, Snafu};
use upgrid_config::{Cipher, CipherError};

use super::{ApplicationState, ConfigValue, SecretId};

#[derive(Debug, Snafu)]
pub enum ConfigValueError {
    #[snafu(display("secret {} no longer exists", id.0))]
    MissingSecret { id: SecretId },
    #[snafu(display("could not decrypt secret {}: {source}", id.0))]
    Cipher { id: SecretId, source: CipherError },
    #[snafu(display("secret {} is not UTF-8", id.0))]
    InvalidUtf8 { id: SecretId, source: FromUtf8Error },
}

pub fn resolve_config_value(
    state: &ApplicationState,
    cipher: &Cipher,
    value: &ConfigValue,
) -> Result<String, ConfigValueError> {
    match value {
        ConfigValue::Literal(value) => Ok(value.clone()),
        ConfigValue::Secret(id) => {
            let secret = state
                .secrets
                .get(id)
                .context(MissingSecretSnafu { id: *id })?;
            let plaintext = cipher
                .open(&secret.ciphertext)
                .context(CipherSnafu { id: *id })?;
            String::from_utf8(plaintext).context(InvalidUtf8Snafu { id: *id })
        }
    }
}
