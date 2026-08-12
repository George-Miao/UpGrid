use super::evaluation::{id, state_with_target};
use super::*;

#[test]
fn cleanup_deletes_only_secrets_unreferenced_at_commit() {
    let (mut state, target_id, _) = state_with_target();
    let trashed_target_secret = SecretId(id(10));
    let orphan = SecretId(id(11));
    for (id, name) in [
        (trashed_target_secret, "trashed-target"),
        (orphan, "orphan"),
    ] {
        state
            .apply(Command::PutSecret(Secret {
                id,
                name: name.to_owned(),
                ciphertext: vec![1],
            }))
            .unwrap();
    }
    state.targets.get_mut(&target_id).unwrap().target.http.body =
        Some(ConfigValue::Secret(trashed_target_secret));
    state
        .apply(Command::TrashTarget {
            target_id,
            deleted_at_ms: 1_000,
        })
        .unwrap();

    assert_eq!(
        state.referenced_secret_ids(),
        [SecretId(id(1)), trashed_target_secret]
            .into_iter()
            .collect()
    );
    assert_eq!(
        state.apply(Command::DeleteUnreferencedSecrets).unwrap(),
        CommandResult::UnreferencedSecretsDeleted(vec![orphan])
    );
    assert!(state.secrets.contains_key(&SecretId(id(1))));
    assert!(state.secrets.contains_key(&trashed_target_secret));
    assert!(!state.secrets.contains_key(&orphan));
}
