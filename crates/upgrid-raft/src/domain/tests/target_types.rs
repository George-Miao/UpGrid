use std::collections::BTreeSet;

use serde::Serialize;
use url::Url;

use super::evaluation::id;
use super::*;

fn target(id_value: u128, endpoint: &str) -> Target {
    Target {
        id: TargetId(id(id_value)),
        name: endpoint.to_owned(),
        http: HttpTarget::get(Url::parse(endpoint).unwrap()),
        policy: EvaluationPolicy::default(),
        notification_channels: BTreeSet::new(),
    }
}

#[test]
fn accepts_each_supported_target_kind() {
    let mut state = ApplicationState::default();
    let cases = [
        ("http://127.0.0.1/", TargetKind::Http),
        ("tcp://127.0.0.1:443", TargetKind::Tcp),
        ("dns://localhost", TargetKind::Dns),
        ("icmp://127.0.0.1", TargetKind::Icmp),
        ("tls://example.com:443", TargetKind::Tls),
    ];

    for (index, (endpoint, kind)) in cases.into_iter().enumerate() {
        let target = target(index as u128 + 1, endpoint);
        assert_eq!(target.kind(), kind);
        state
            .apply(Command::CreateTarget {
                target,
                use_default_notifications: true,
            })
            .unwrap();
    }
}

#[test]
fn rejects_malformed_non_http_endpoints() {
    let mut state = ApplicationState::default();
    for (index, endpoint) in [
        "tcp://localhost",
        "dns://localhost:53",
        "icmp://localhost/path",
        "tls://localhost",
        "tcp://user:password@localhost:443",
    ]
    .into_iter()
    .enumerate()
    {
        assert!(matches!(
            state.apply(Command::CreateTarget {
                target: target(index as u128 + 10, endpoint),
                use_default_notifications: true,
            }),
            Err(DomainError::InvalidTarget(_))
        ));
    }
}

#[test]
fn validates_http_assertions_before_replication() {
    let valid = [
        HttpAssertion::BodyContains {
            value: "healthy".to_owned(),
        },
        HttpAssertion::BodyRegex {
            pattern: r#""healthy"\s*:\s*true"#.to_owned(),
        },
        HttpAssertion::JsonPath {
            path: "$.services[0].healthy".to_owned(),
            expected: Some("true".to_owned()),
        },
        HttpAssertion::ResponseHeader {
            name: "content-type".to_owned(),
            value: Some("application/json".to_owned()),
        },
        HttpAssertion::Latency { max_ms: 500 },
        HttpAssertion::Script {
            source: "status == 200 && latency_ms < 500".to_owned(),
        },
    ];
    let mut valid_target = target(30, "https://example.com/health");
    valid_target.http.assertions = valid.into();
    let mut state = ApplicationState::default();
    state
        .apply(Command::CreateTarget {
            target: valid_target,
            use_default_notifications: true,
        })
        .unwrap();

    for assertion in [
        HttpAssertion::BodyRegex {
            pattern: "(".to_owned(),
        },
        HttpAssertion::JsonPath {
            path: "$[".to_owned(),
            expected: None,
        },
        HttpAssertion::Latency { max_ms: 0 },
        HttpAssertion::Script {
            source: "let =".to_owned(),
        },
        HttpAssertion::Script {
            source: "while true {}".to_owned(),
        },
    ] {
        let mut invalid = target(31, "https://example.com/health");
        invalid.http.assertions.push(assertion);
        assert!(matches!(
            ApplicationState::default().apply(Command::CreateTarget {
                target: invalid,
                use_default_notifications: true,
            }),
            Err(DomainError::InvalidTarget(_))
        ));
    }

    let mut too_many = target(32, "https://example.com/health");
    too_many.http.assertions = vec![HttpAssertion::Latency { max_ms: 1 }; MAX_HTTP_ASSERTIONS + 1];
    assert!(matches!(
        ApplicationState::default().apply(Command::CreateTarget {
            target: too_many,
            use_default_notifications: true,
        }),
        Err(DomainError::InvalidTarget(_))
    ));
}

#[test]
fn validates_custom_tls_secret_references_before_replication() {
    let ca_id = SecretId(id(40));
    let certificate_id = SecretId(id(41));
    let private_key_id = SecretId(id(42));
    let mut state = ApplicationState::default();
    for (id, name) in [
        (ca_id, "ca"),
        (certificate_id, "certificate"),
        (private_key_id, "private-key"),
    ] {
        state
            .apply(Command::PutSecret(Secret {
                id,
                name: name.to_owned(),
                ciphertext: vec![1],
            }))
            .unwrap();
    }

    let mut valid = target(43, "https://example.com/health");
    valid.http.tls_ca_secret = Some(ca_id);
    valid.http.tls_client_certificate_secret = Some(certificate_id);
    valid.http.tls_client_private_key_secret = Some(private_key_id);
    state
        .apply(Command::CreateTarget {
            target: valid,
            use_default_notifications: true,
        })
        .unwrap();

    let mut partial = target(44, "https://example.com/health");
    partial.http.tls_client_certificate_secret = Some(certificate_id);
    assert!(matches!(
        state.apply(Command::CreateTarget {
            target: partial,
            use_default_notifications: true,
        }),
        Err(DomainError::InvalidTarget(_))
    ));

    let mut insecure = target(45, "https://example.com/health");
    insecure.http.tls_ca_secret = Some(ca_id);
    insecure.http.skip_tls_verification = true;
    assert!(matches!(
        state.apply(Command::CreateTarget {
            target: insecure,
            use_default_notifications: true,
        }),
        Err(DomainError::InvalidTarget(_))
    ));

    let mut plaintext = target(46, "http://example.com/health");
    plaintext.http.tls_ca_secret = Some(ca_id);
    assert!(matches!(
        state.apply(Command::CreateTarget {
            target: plaintext,
            use_default_notifications: true,
        }),
        Err(DomainError::InvalidTarget(_))
    ));

    let mut missing = target(47, "https://example.com/health");
    missing.http.tls_ca_secret = Some(SecretId(id(404)));
    assert!(matches!(
        state.apply(Command::CreateTarget {
            target: missing,
            use_default_notifications: true,
        }),
        Err(DomainError::SecretNotFound(_))
    ));
}

#[test]
fn target_kind_addition_preserves_legacy_http_encoding() {
    #[derive(Serialize)]
    struct LegacyTarget {
        id: TargetId,
        name: String,
        http: HttpTarget,
        policy: EvaluationPolicy,
        notification_channels: BTreeSet<NotificationChannelId>,
    }

    let current = target(42, "https://example.com/health");
    let legacy = LegacyTarget {
        id: current.id,
        name: current.name.clone(),
        http: current.http.clone(),
        policy: current.policy.clone(),
        notification_channels: current.notification_channels.clone(),
    };

    assert_eq!(
        postcard::to_stdvec(&current).unwrap(),
        postcard::to_stdvec(&legacy).unwrap()
    );
}
