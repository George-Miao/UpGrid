use super::*;

fn local_addresses() -> BTreeSet<LocalAddress> {
    BTreeSet::from([LocalAddress {
        host: "127.0.0.1".parse().unwrap(),
        port: 11451,
    }])
}

#[compio::test]
async fn accepting_choice_notifies_shutdown_waiters() {
    let accepted = Arc::new(Event::new());
    let accepted_signal = accepted.listen();
    let deadline = Arc::new(Event::new());
    let deadline_signal = deadline.listen();
    let state = SetupState {
        data_dir: std::path::PathBuf::new(),
        local_addresses: local_addresses(),
        reachable_addresses: vec!["up://127.0.0.1:11451".to_owned()],
        discovery_urls: Vec::new(),
        reachable_addresses_explicit: false,
        discovery_urls_explicit: false,
        node_name: Arc::new(Mutex::new(String::new())),
        result: Arc::new(Mutex::new(None)),
        accepted,
        deadline,
    };

    accept(
        &state,
        OobeChoice::NewCluster {
            node_name: "node".to_owned(),
            admin_username: "admin".to_owned(),
            admin_password: "password".to_owned(),
            network: OobeNetworkSources {
                reachable_addresses: BTreeSet::new(),
                reachable_addresses_explicit: true,
                discovery_urls: BTreeSet::new(),
                discovery_urls_explicit: true,
            },
        },
    )
    .unwrap();

    accepted_signal.await;
    deadline_signal.await;
}

#[test]
fn accepting_join_persists_the_link_before_shutdown() {
    let directory =
        std::env::temp_dir().join(format!("upgrid-setup-test-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&directory).unwrap();
    let state = SetupState {
        data_dir: directory.clone(),
        local_addresses: local_addresses(),
        reachable_addresses: Vec::new(),
        discovery_urls: Vec::new(),
        reachable_addresses_explicit: false,
        discovery_urls_explicit: false,
        node_name: Arc::new(Mutex::new(String::new())),
        result: Arc::new(Mutex::new(None)),
        accepted: Arc::new(Event::new()),
        deadline: Arc::new(Event::new()),
    };
    let cipher =
        upgrid_config::Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
    let quic_ca_key =
        upgrid_config::QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
    let link = JoinLink::issue(
        "up://127.0.0.1:11451",
        uuid::Uuid::from_u128(1),
        &cipher,
        &quic_ca_key,
        "pending-token".to_owned(),
    )
    .unwrap();

    accept(
        &state,
        OobeChoice::Join {
            node_name: "node".to_owned(),
            link: Box::new(link.clone()),
            network: OobeNetworkSources {
                reachable_addresses: BTreeSet::new(),
                reachable_addresses_explicit: true,
                discovery_urls: BTreeSet::new(),
                discovery_urls_explicit: true,
            },
        },
    )
    .unwrap();

    let pending = upgrid_config::load_pending_join(&directory)
        .unwrap()
        .unwrap();
    assert_eq!(pending.link.to_string(), link.to_string());
    assert!(!pending.complete_oobe);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn setup_auth_accepts_only_the_configured_basic_credentials() {
    let auth = SetupAuth::new("setup-admin", "setup-password");
    let mut headers = HeaderMap::new();
    let credentials = STANDARD.encode("setup-admin:setup-password");
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Basic {credentials}")).unwrap(),
    );
    assert!(auth.accepts(&headers));

    let credentials = STANDARD.encode("setup-admin:wrong-password");
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Basic {credentials}")).unwrap(),
    );
    assert!(!auth.accepts(&headers));
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer setup-password"),
    );
    assert!(!auth.accepts(&headers));
}

#[test]
fn setup_authentication_challenges_the_browser() {
    let response = setup_unauthorized();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(header::WWW_AUTHENTICATE),
        Some(&HeaderValue::from_static(
            r#"Basic realm="UpGrid setup", charset="UTF-8""#
        ))
    );
}

#[test]
fn reachable_addresses_accept_a_translated_port() {
    let addresses = parse_reachable_addresses(vec!["up://node.example:12000".to_owned()]).unwrap();

    assert_eq!(
        addresses,
        BTreeSet::from([ReachableAddress::parse("up://node.example:12000").unwrap(),])
    );
}

#[test]
fn discovery_urls_reject_sensitive_components() {
    for url in [
        "https://user:password@discovery.example/nodes",
        "https://discovery.example/nodes?token=secret",
        "https://discovery.example/nodes#secret",
    ] {
        assert!(parse_discovery_urls(vec![url.to_owned()]).is_err());
    }
}

#[test]
fn discovery_service_count_is_bounded() {
    let urls = (0..=MAX_DISCOVERY_SERVICES)
        .map(|index| format!("https://discovery-{index}.example/nodes"))
        .collect();

    assert!(parse_discovery_urls(urls).is_err());
}
#[test]
fn accepted_network_sources_are_durable_before_shutdown() {
    let directory =
        std::env::temp_dir().join(format!("upgrid-setup-test-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&directory).unwrap();
    let state = SetupState {
        data_dir: directory.clone(),
        local_addresses: local_addresses(),
        reachable_addresses: vec!["up://127.0.0.1:11451".to_owned()],
        discovery_urls: vec!["https://existing.example/nodes".to_owned()],
        reachable_addresses_explicit: false,
        discovery_urls_explicit: true,
        node_name: Arc::new(Mutex::new(String::new())),
        result: Arc::new(Mutex::new(None)),
        accepted: Arc::new(Event::new()),
        deadline: Arc::new(Event::new()),
    };
    let additions = OobeNetworkSources {
        reachable_addresses: BTreeSet::from([ReachableAddress::parse(
            "up://translated.example:443",
        )
        .unwrap()]),
        reachable_addresses_explicit: true,
        discovery_urls: BTreeSet::from(["https://added.example/nodes".parse().unwrap()]),
        discovery_urls_explicit: true,
    };

    persist_network_sources(&state, &additions).unwrap();

    assert_eq!(
        upgrid_config::load_reachable_addresses(&directory)
            .unwrap()
            .unwrap(),
        additions.reachable_addresses,
    );
    assert_eq!(
        upgrid_config::load_discovery_urls(&directory).unwrap(),
        Some(BTreeSet::from([
            "https://added.example/nodes".parse().unwrap(),
            "https://existing.example/nodes".parse().unwrap(),
        ])),
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn empty_network_selection_is_durable() {
    let directory =
        std::env::temp_dir().join(format!("upgrid-setup-test-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&directory).unwrap();
    let state = SetupState {
        data_dir: directory.clone(),
        local_addresses: local_addresses(),
        reachable_addresses: vec!["up://127.0.0.1:11451".to_owned()],
        discovery_urls: Vec::new(),
        reachable_addresses_explicit: false,
        discovery_urls_explicit: false,
        node_name: Arc::new(Mutex::new(String::new())),
        result: Arc::new(Mutex::new(None)),
        accepted: Arc::new(Event::new()),
        deadline: Arc::new(Event::new()),
    };
    let selection = OobeNetworkSources {
        reachable_addresses: BTreeSet::new(),
        reachable_addresses_explicit: true,
        discovery_urls: BTreeSet::new(),
        discovery_urls_explicit: true,
    };

    persist_network_sources(&state, &selection).unwrap();

    assert_eq!(
        upgrid_config::load_reachable_addresses(&directory).unwrap(),
        Some(BTreeSet::new()),
    );
    assert_eq!(
        upgrid_config::load_discovery_urls(&directory).unwrap(),
        Some(BTreeSet::new()),
    );
    std::fs::remove_dir_all(directory).unwrap();
}
