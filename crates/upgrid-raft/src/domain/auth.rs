use std::num::NonZeroU32;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::digest::{SHA256, digest};
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ApplicationState, CommandResult, DomainError};

const PASSWORD_ITERATIONS: u32 = 210_000;
const SALT_LEN: usize = 16;
const HASH_LEN: usize = 32;
const TOKEN_BYTES: usize = 32;
const TOKEN_PREFIX: &str = "upgrid_";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IdentityId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ApiTokenId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ApiTokenHash(pub [u8; HASH_LEN]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PasswordVerifier {
    iterations: u32,
    salt: [u8; SALT_LEN],
    hash: [u8; HASH_LEN],
}

impl PasswordVerifier {
    pub fn create(password: &str) -> Result<Self, AuthenticationError> {
        validate_password(password)?;
        let mut salt = [0; SALT_LEN];
        SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| AuthenticationError::Random)?;
        let mut hash = [0; HASH_LEN];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(PASSWORD_ITERATIONS).expect("password iterations are non-zero"),
            &salt,
            password.as_bytes(),
            &mut hash,
        );
        Ok(Self {
            iterations: PASSWORD_ITERATIONS,
            salt,
            hash,
        })
    }

    pub fn verify(&self, password: &str) -> bool {
        let Some(iterations) = NonZeroU32::new(self.iterations) else {
            return false;
        };
        pbkdf2::verify(
            pbkdf2::PBKDF2_HMAC_SHA256,
            iterations,
            &self.salt,
            password.as_bytes(),
            &self.hash,
        )
        .is_ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorIdentity {
    pub id: IdentityId,
    pub username: String,
    pub password: PasswordVerifier,
    pub auth_version: u64,
    pub created_at_ms: u64,
}

impl OperatorIdentity {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_username(&self.username)?;
        if self.auth_version == 0 {
            return Err(DomainError::InvalidIdentity(
                "identity authentication version must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiToken {
    pub id: ApiTokenId,
    pub identity_id: IdentityId,
    pub name: String,
    pub hash: ApiTokenHash,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
}

impl ApiToken {
    pub fn validate(&self) -> Result<(), DomainError> {
        let name = self.name.trim();
        if name.is_empty() || name.len() > 64 || name.chars().any(char::is_control) {
            return Err(DomainError::InvalidApiToken(
                "API token name must contain 1 to 64 printable characters".to_owned(),
            ));
        }
        if self
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= self.created_at_ms)
        {
            return Err(DomainError::InvalidApiToken(
                "API token expiry must be after its creation time".to_owned(),
            ));
        }
        Ok(())
    }
}

pub fn generate_api_token() -> Result<(String, ApiTokenHash), AuthenticationError> {
    let mut secret = [0; TOKEN_BYTES];
    SystemRandom::new()
        .fill(&mut secret)
        .map_err(|_| AuthenticationError::Random)?;
    let token = format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(secret));
    Ok((token.clone(), hash_api_token(&token)))
}

pub fn hash_api_token(token: &str) -> ApiTokenHash {
    ApiTokenHash(
        digest(&SHA256, token.as_bytes())
            .as_ref()
            .try_into()
            .expect("SHA-256 output is always 32 bytes"),
    )
}

pub fn validate_username(username: &str) -> Result<(), DomainError> {
    let username = username.trim();
    if username.is_empty()
        || username.len() > 64
        || username.chars().any(|character| {
            character.is_control() || character.is_whitespace() || character == ':'
        })
    {
        return Err(DomainError::InvalidIdentity(
            "username must contain 1 to 64 non-whitespace printable characters excluding ':'"
                .to_owned(),
        ));
    }
    Ok(())
}

pub fn validate_password(password: &str) -> Result<(), AuthenticationError> {
    if password.len() < 12 || password.len() > 1_024 {
        return Err(AuthenticationError::InvalidPassword);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationError {
    InvalidPassword,
    Random,
}

impl std::fmt::Display for AuthenticationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPassword => "password must contain 12 to 1024 bytes",
            Self::Random => "secure random generation failed",
        })
    }
}

impl std::error::Error for AuthenticationError {}

impl ApplicationState {
    pub(super) fn create_identity(
        &mut self,
        identity: OperatorIdentity,
    ) -> Result<CommandResult, DomainError> {
        identity.validate()?;
        if self.identities.contains_key(&identity.id) {
            return Err(DomainError::IdentityAlreadyExists(identity.id));
        }
        if self
            .identities
            .values()
            .any(|current| current.username.eq_ignore_ascii_case(&identity.username))
        {
            return Err(DomainError::InvalidIdentity(
                "username is already in use".to_owned(),
            ));
        }
        let id = identity.id;
        self.identities.insert(id, identity);
        Ok(CommandResult::IdentityCreated(id))
    }

    pub(super) fn update_identity(
        &mut self,
        identity: OperatorIdentity,
    ) -> Result<CommandResult, DomainError> {
        identity.validate()?;
        let current = self
            .identities
            .get(&identity.id)
            .ok_or(DomainError::IdentityNotFound(identity.id))?;
        let expected_version = if identity.password == current.password {
            current.auth_version
        } else {
            current.auth_version.saturating_add(1)
        };
        if identity.created_at_ms != current.created_at_ms
            || identity.auth_version != expected_version
        {
            return Err(DomainError::InvalidIdentity(
                "identity update has stale authentication metadata".to_owned(),
            ));
        }
        if self.identities.values().any(|candidate| {
            candidate.id != identity.id
                && candidate.username.eq_ignore_ascii_case(&identity.username)
        }) {
            return Err(DomainError::InvalidIdentity(
                "username is already in use".to_owned(),
            ));
        }
        let id = identity.id;
        self.identities.insert(id, identity);
        Ok(CommandResult::IdentityUpdated(id))
    }

    pub(super) fn delete_identity(&mut self, id: IdentityId) -> Result<CommandResult, DomainError> {
        if !self.identities.contains_key(&id) {
            return Err(DomainError::IdentityNotFound(id));
        }
        if self.identities.len() == 1 {
            return Err(DomainError::InvalidIdentity(
                "the final administrator identity cannot be deleted".to_owned(),
            ));
        }
        self.identities.remove(&id);
        self.api_tokens.retain(|_, token| token.identity_id != id);
        Ok(CommandResult::IdentityDeleted(id))
    }

    pub(super) fn create_api_token(
        &mut self,
        token: ApiToken,
    ) -> Result<CommandResult, DomainError> {
        token.validate()?;
        if !self.identities.contains_key(&token.identity_id) {
            return Err(DomainError::IdentityNotFound(token.identity_id));
        }
        if self.api_tokens.contains_key(&token.id) {
            return Err(DomainError::ApiTokenAlreadyExists(token.id));
        }
        if self
            .api_tokens
            .values()
            .any(|current| current.hash == token.hash)
        {
            return Err(DomainError::InvalidApiToken(
                "API token verifier is already in use".to_owned(),
            ));
        }
        let id = token.id;
        self.api_tokens.insert(id, token);
        Ok(CommandResult::ApiTokenCreated(id))
    }
}
