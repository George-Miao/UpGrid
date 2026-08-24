use super::*;

#[test]
fn merged_discovery_limit_fails_before_network_writes() {
    let directory =
        std::env::temp_dir().join(format!("upgrid-setup-test-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&directory).unwrap();
    let state = SetupState {
        data_dir: directory.clone(),
        node_name: Arc::new(Mutex::new(String::new())),
        local_addresses: BTreeSet::new(),
        reachable_addresses: Vec::new(),
        discovery_urls: (0..MAX_DISCOVERY_SERVICES)
            .map(|index| format!("https://discovery-{index}.example/nodes"))
            .collect(),
        reachable_addresses_explicit: false,
        discovery_urls_explicit: true,
        result: Arc::new(Mutex::new(None)),
        accepted: Arc::new(Event::new()),
        deadline: Arc::new(Event::new()),
    };
    let additions = OobeNetworkSources {
        reachable_addresses: BTreeSet::from([ReachableAddress::parse(
            "up://reachable.example:11451",
        )
        .unwrap()]),
        reachable_addresses_explicit: true,
        discovery_urls: BTreeSet::from(["https://extra.example/nodes".parse().unwrap()]),
        discovery_urls_explicit: true,
    };

    let error = persist_network_sources(&state, &additions).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        upgrid_config::load_reachable_addresses(&directory).unwrap(),
        None
    );
    assert_eq!(
        upgrid_config::load_discovery_urls(&directory).unwrap(),
        None
    );
    std::fs::remove_dir_all(directory).unwrap();
}
