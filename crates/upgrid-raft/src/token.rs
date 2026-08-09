use ring::digest::{SHA256, digest};
use uuid::Uuid;

use crate::domain::JoinTokenHash;

pub fn hash_join_token(token: &str) -> JoinTokenHash {
    JoinTokenHash(
        digest(&SHA256, token.as_bytes())
            .as_ref()
            .try_into()
            .expect("SHA-256 output is always 32 bytes"),
    )
}

pub(crate) fn join_operation_id(token: &str, node_id: Uuid) -> Uuid {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_operation_is_stable_only_for_the_same_token_and_node() {
        let first = Uuid::from_u128(1);
        let second = Uuid::from_u128(2);

        assert_eq!(
            join_operation_id("token", first),
            join_operation_id("token", first)
        );
        assert_ne!(
            join_operation_id("token", first),
            join_operation_id("token", second)
        );
        assert_ne!(
            join_operation_id("token", first),
            join_operation_id("other", first)
        );
    }
}
