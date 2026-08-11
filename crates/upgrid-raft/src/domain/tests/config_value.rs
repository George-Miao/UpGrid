use upgrid_config::Cipher;

use super::*;

fn cipher() -> Cipher {
    Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap()
}

fn secret(id: SecretId, ciphertext: Vec<u8>) -> Secret {
    Secret {
        id,
        name: "test-secret".to_owned(),
        ciphertext,
    }
}

#[test]
fn literal_resolves_without_state() {
    let value = ConfigValue::Literal("literal-value".to_owned());

    assert_eq!(
        resolve_config_value(&ApplicationState::default(), &cipher(), &value).unwrap(),
        "literal-value"
    );
}

#[test]
fn missing_secret_is_distinct() {
    let id = SecretId(Uuid::from_u128(1));
    let error = resolve_config_value(
        &ApplicationState::default(),
        &cipher(),
        &ConfigValue::Secret(id),
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("secret {} no longer exists", id.0)
    );
    assert!(matches!(error, ConfigValueError::MissingSecret { id: actual } if actual == id));
}

#[test]
fn encrypted_secret_resolves_to_plaintext() {
    let cipher = cipher();
    let id = SecretId(Uuid::from_u128(2));
    let mut state = ApplicationState::default();
    state
        .secrets
        .insert(id, secret(id, cipher.seal(b"decrypted-value").unwrap()));

    assert_eq!(
        resolve_config_value(&state, &cipher, &ConfigValue::Secret(id)).unwrap(),
        "decrypted-value"
    );
}

#[test]
fn cipher_failure_is_distinct() {
    let id = SecretId(Uuid::from_u128(3));
    let mut state = ApplicationState::default();
    state.secrets.insert(id, secret(id, Vec::new()));

    let error = resolve_config_value(&state, &cipher(), &ConfigValue::Secret(id)).unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "could not decrypt secret {}: secret ciphertext is invalid or uses another deployment \
             key",
            id.0
        )
    );
    assert!(matches!(error, ConfigValueError::Cipher { id: actual, .. } if actual == id));
}

#[test]
fn non_utf8_plaintext_is_distinct() {
    let cipher = cipher();
    let id = SecretId(Uuid::from_u128(4));
    let mut state = ApplicationState::default();
    state
        .secrets
        .insert(id, secret(id, cipher.seal(&[0xff]).unwrap()));

    let error = resolve_config_value(&state, &cipher, &ConfigValue::Secret(id)).unwrap_err();

    assert_eq!(error.to_string(), format!("secret {} is not UTF-8", id.0));
    assert!(matches!(error, ConfigValueError::InvalidUtf8 { id: actual, .. } if actual == id));
}
