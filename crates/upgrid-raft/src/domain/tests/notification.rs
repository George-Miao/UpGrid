use super::evaluation::id;
use super::*;

#[test]
fn smtp_channel_requires_valid_delivery_and_authentication_configuration() {
    let mut state = ApplicationState::default();
    let channel_id = NotificationChannelId(id(11));
    let password_id = SecretId(id(12));
    let channel = |host: &str, username: Option<&str>, password| NotificationChannel {
        id: channel_id,
        name: "Email".to_owned(),
        kind: NotificationChannelKind::Smtp {
            host: host.to_owned(),
            port: 587,
            security: SmtpSecurity::StartTls,
            username: username.map(str::to_owned),
            password,
            from: "upgrid@example.com".to_owned(),
            to: "on-call@example.com".to_owned(),
        },
    };

    assert!(matches!(
        state.apply(Command::CreateNotificationChannel {
            channel: channel("", None, None),
            generated_secret: None,
            is_default: false,
        }),
        Err(DomainError::InvalidNotificationChannel(_))
    ));
    assert!(matches!(
        state.apply(Command::CreateNotificationChannel {
            channel: channel("smtp.example.com", Some("upgrid"), None),
            generated_secret: None,
            is_default: false,
        }),
        Err(DomainError::InvalidNotificationChannel(_))
    ));
    assert_eq!(
        state
            .apply(Command::CreateNotificationChannel {
                channel: channel("smtp.example.com", Some("upgrid"), Some(password_id)),
                generated_secret: Some(Secret {
                    id: password_id,
                    name: "smtp-password".to_owned(),
                    ciphertext: vec![1, 2, 3],
                }),
                is_default: true,
            })
            .unwrap(),
        CommandResult::NotificationChannelStored(channel_id)
    );
    assert!(state.secrets.contains_key(&password_id));
}
