use super::*;

fn identity(id: u128, username: &str, password: &str) -> OperatorIdentity {
    OperatorIdentity {
        id: IdentityId(Uuid::from_u128(id)),
        username: username.to_owned(),
        password: PasswordVerifier::create(password).unwrap(),
        auth_version: 1,
        created_at_ms: id as u64,
    }
}

#[test]
fn password_verifiers_authenticate_without_storing_plaintext() {
    let verifier = PasswordVerifier::create("correct horse battery").unwrap();

    assert!(verifier.verify("correct horse battery"));
    assert!(!verifier.verify("incorrect horse battery"));
    let encoded = postcard::to_stdvec(&verifier).unwrap();
    assert!(
        !encoded
            .windows("correct horse battery".len())
            .any(|window| window == b"correct horse battery")
    );
}

#[test]
fn identities_are_unique_and_the_final_administrator_is_retained() {
    let mut state = ApplicationState::default();
    let first = identity(1, "admin", "first administrator");
    state.apply(Command::CreateIdentity(first.clone())).unwrap();

    let duplicate = identity(2, "ADMIN", "second administrator");
    assert!(matches!(
        state.apply(Command::CreateIdentity(duplicate)),
        Err(DomainError::InvalidIdentity(_))
    ));
    assert!(matches!(
        state.apply(Command::DeleteIdentity(first.id)),
        Err(DomainError::InvalidIdentity(_))
    ));
}

#[test]
fn password_change_requires_and_invalidates_the_authentication_version() {
    let mut state = ApplicationState::default();
    let current = identity(1, "admin", "first administrator");
    state
        .apply(Command::CreateIdentity(current.clone()))
        .unwrap();

    let mut changed = current.clone();
    changed.password = PasswordVerifier::create("replacement password").unwrap();
    assert!(matches!(
        state.apply(Command::UpdateIdentity(changed.clone())),
        Err(DomainError::InvalidIdentity(_))
    ));
    changed.auth_version += 1;
    state.apply(Command::UpdateIdentity(changed)).unwrap();

    assert_eq!(state.identities[&current.id].auth_version, 2);
}

#[test]
fn deleting_an_identity_revokes_its_api_tokens() {
    let mut state = ApplicationState::default();
    let first = identity(1, "admin", "first administrator");
    let second = identity(2, "operator", "second administrator");
    state.apply(Command::CreateIdentity(first.clone())).unwrap();
    state.apply(Command::CreateIdentity(second)).unwrap();
    let (plaintext, hash) = generate_api_token().unwrap();
    let token = ApiToken {
        id: ApiTokenId(Uuid::from_u128(3)),
        identity_id: first.id,
        name: "automation".to_owned(),
        hash,
        created_at_ms: 10,
        expires_at_ms: Some(20),
    };
    state.apply(Command::CreateApiToken(token)).unwrap();

    assert_eq!(hash_api_token(&plaintext), hash);
    state.apply(Command::DeleteIdentity(first.id)).unwrap();
    assert!(state.api_tokens.is_empty());
}
