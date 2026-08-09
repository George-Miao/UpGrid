use ring::digest::{SHA256, digest};

use crate::domain::JoinTokenHash;

pub fn hash_join_token(token: &str) -> JoinTokenHash {
    JoinTokenHash(
        digest(&SHA256, token.as_bytes())
            .as_ref()
            .try_into()
            .expect("SHA-256 output is always 32 bytes"),
    )
}
