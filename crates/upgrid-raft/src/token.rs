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

pub(crate) fn admission_operation_id(node_id: uuid::Uuid, hash: JoinTokenHash) -> uuid::Uuid {
    let mut input = [0_u8; 48];
    input[..16].copy_from_slice(node_id.as_bytes());
    input[16..].copy_from_slice(&hash.0);
    let value = digest(&SHA256, &input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&value.as_ref()[..16]);
    uuid::Uuid::from_bytes(bytes)
}

pub(crate) fn admission_acceptance_id(operation_id: uuid::Uuid) -> uuid::Uuid {
    let mut input = [0_u8; 36];
    input[..20].copy_from_slice(b"UpGrid join accepted");
    input[20..].copy_from_slice(operation_id.as_bytes());
    let value = digest(&SHA256, &input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&value.as_ref()[..16]);
    uuid::Uuid::from_bytes(bytes)
}
