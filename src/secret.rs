use std::fmt::{Debug, Display, Formatter};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::digest::{SHA256, digest};
use ring::rand::{SecureRandom, SystemRandom};
use uuid::Uuid;

const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const VERSION: u8 = 1;

pub fn generate_join_token() -> Result<String, Error> {
    let mut token = [0; 32];
    SystemRandom::new()
        .fill(&mut token)
        .map_err(|_| Error::Random)?;
    Ok(URL_SAFE_NO_PAD.encode(token))
}

pub fn hash_join_token(token: &str) -> crate::domain::JoinTokenHash {
    crate::domain::JoinTokenHash(
        digest(&SHA256, token.as_bytes())
            .as_ref()
            .try_into()
            .expect("SHA-256 output is always 32 bytes"),
    )
}

pub fn join_operation_id(token: &str, node_id: Uuid) -> Uuid {
    let hash = hash_join_token(token);
    let mut material = Vec::with_capacity(hash.0.len() + node_id.as_bytes().len());
    material.extend_from_slice(&hash.0);
    material.extend_from_slice(node_id.as_bytes());
    let digest = digest(&SHA256, &material);
    let bytes = digest.as_ref()[..16]
        .try_into()
        .expect("SHA-256 output is at least 16 bytes");
    Uuid::from_bytes(bytes)
}

#[derive(Clone)]
pub struct Cipher {
    key: [u8; KEY_LEN],
}

impl Debug for Cipher {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Cipher([REDACTED])")
    }
}

impl Cipher {
    pub fn generate() -> Result<Self, Error> {
        let mut key = [0; KEY_LEN];
        SystemRandom::new()
            .fill(&mut key)
            .map_err(|_| Error::Random)?;
        Ok(Self { key })
    }

    pub fn parse(encoded: &str) -> Result<Self, Error> {
        let decoded = STANDARD
            .decode(encoded.trim())
            .map_err(|_| Error::InvalidKey)?;
        let key = decoded.try_into().map_err(|_| Error::InvalidKey)?;
        Ok(Self { key })
    }

    pub fn encoded(&self) -> String {
        STANDARD.encode(self.key)
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        digest(&SHA256, &self.key)
            .as_ref()
            .try_into()
            .expect("SHA-256 output is always 32 bytes")
    }

    pub fn derive(&self, purpose: &[u8]) -> [u8; 32] {
        let mut material = Vec::with_capacity(self.key.len() + purpose.len());
        material.extend_from_slice(&self.key);
        material.extend_from_slice(purpose);
        digest(&SHA256, &material)
            .as_ref()
            .try_into()
            .expect("SHA-256 output is always 32 bytes")
    }

    pub fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let mut nonce = [0; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| Error::Random)?;
        let mut ciphertext = plaintext.to_vec();
        self.key()?
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut ciphertext,
            )
            .map_err(|_| Error::Seal)?;

        let mut output = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        output.push(VERSION);
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    pub fn open(&self, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
        let (&version, rest) = ciphertext.split_first().ok_or(Error::InvalidCiphertext)?;
        if version != VERSION || rest.len() <= NONCE_LEN {
            return Err(Error::InvalidCiphertext);
        }
        let (nonce, sealed) = rest.split_at(NONCE_LEN);
        let nonce: [u8; NONCE_LEN] = nonce.try_into().map_err(|_| Error::InvalidCiphertext)?;
        let mut plaintext = sealed.to_vec();
        let opened = self
            .key()?
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut plaintext,
            )
            .map_err(|_| Error::InvalidCiphertext)?;
        let len = opened.len();
        plaintext.truncate(len);
        Ok(plaintext)
    }

    fn key(&self) -> Result<LessSafeKey, Error> {
        UnboundKey::new(&AES_256_GCM, &self.key)
            .map(LessSafeKey::new)
            .map_err(|_| Error::InvalidKey)
    }

    #[cfg(test)]
    fn from_bytes(key: [u8; KEY_LEN]) -> Self {
        Self { key }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidKey,
    Random,
    Seal,
    InvalidCiphertext,
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidKey => "deployment key must be 32 bytes encoded as base64",
            Self::Random => "secure random generation failed",
            Self::Seal => "secret encryption failed",
            Self::InvalidCiphertext => {
                "secret ciphertext is invalid or uses another deployment key"
            }
        })
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{Cipher, generate_join_token, hash_join_token, join_operation_id};

    #[test]
    fn ciphertext_is_authenticated_and_randomized() {
        let cipher = Cipher::from_bytes([7; 32]);

        let first = cipher.seal(b"telegram-token").unwrap();
        let second = cipher.seal(b"telegram-token").unwrap();

        assert_ne!(first, second);
        assert!(!first.windows(14).any(|part| part == b"telegram-token"));
        assert_eq!(cipher.open(&first).unwrap(), b"telegram-token");
        assert_ne!(
            cipher.fingerprint(),
            Cipher::from_bytes([8; 32]).fingerprint()
        );

        let mut tampered = first;
        *tampered.last_mut().unwrap() ^= 1;
        assert!(cipher.open(&tampered).is_err());
    }

    #[test]
    fn join_tokens_are_random_and_hash_stably() {
        let first = generate_join_token().unwrap();
        let second = generate_join_token().unwrap();

        assert_ne!(first, second);
        assert_eq!(hash_join_token(&first), hash_join_token(&first));
        assert_ne!(hash_join_token(&first), hash_join_token(&second));
    }

    #[test]
    fn join_operation_is_stable_only_for_the_same_token_and_node() {
        let first_node = Uuid::from_u128(1);
        let second_node = Uuid::from_u128(2);

        assert_eq!(
            join_operation_id("token", first_node),
            join_operation_id("token", first_node)
        );
        assert_ne!(
            join_operation_id("token", first_node),
            join_operation_id("token", second_node)
        );
        assert_ne!(
            join_operation_id("token", first_node),
            join_operation_id("other", first_node)
        );
    }
}
