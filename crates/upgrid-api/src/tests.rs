use super::targets::target_from_input;
use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_view_never_serializes_ciphertext() {
        let secret = Secret {
            id: SecretId(Uuid::from_u128(1)),
            name: "Telegram".to_owned(),
            ciphertext: b"must-not-leak".to_vec(),
        };

        let json = serde_json::to_string(&SecretView::from(&secret)).unwrap();

        assert!(json.contains("Telegram"));
        assert!(!json.contains("must-not-leak"));
        assert!(!json.contains("ciphertext"));
    }

    #[test]
    fn target_input_preserves_extension_method_case() {
        let input = PutTargetRequest {
            name: "Example".to_owned(),
            kind: TargetKindInput::Http,
            url: "https://example.com".to_owned(),
            method: "Example-Method".to_owned(),
            headers: BTreeMap::new(),
            body: None,
            accepted_statuses: vec![StatusRangeInput {
                start: 200,
                end: 299,
            }],
            follow_redirects: true,
            max_redirects: 5,
            assertions: Vec::new(),
            skip_tls_verification: false,
            tls_ca_secret_id: None,
            tls_client_certificate_secret_id: None,
            tls_client_private_key_secret_id: None,
            interval_seconds: 60,
            timeout_seconds: 10,
            failure_threshold: 3,
            notification_channel_ids: BTreeSet::new(),
            use_default_channels: true,
        };

        let Ok(target) = target_from_input(TargetId(Uuid::from_u128(1)), input) else {
            panic!("valid extension method should be accepted");
        };

        assert_eq!(target.http.method, "Example-Method");
    }

    #[test]
    fn webui_build_is_embeddable() {
        let html = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/index.html"
        ));
        let script = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/assets/upgrid.js"
        ));
        let logo = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../frontend/dist/favicon.svg"
        ));

        assert!(html.contains("rel=\"icon\" href=\"/favicon.svg\""));
        assert!(html.contains("src=\"/assets/upgrid.js\""));
        assert!(!script.is_empty());
        assert!(logo.starts_with("<svg"));
        assert!(logo.contains("<title id=\"title\">UpGrid</title>"));
    }

    #[test]
    fn published_openapi_matches_routes() {
        assert_eq!(
            openapi_json().unwrap(),
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../docs/openapi.json"
            ))
        );
    }
}
