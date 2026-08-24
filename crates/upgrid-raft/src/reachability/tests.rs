use super::*;

fn address(value: &str) -> ReachableAddress {
    ReachableAddress::parse(value).unwrap()
}

#[test]
fn rejects_non_unicast_ip_literals() {
    for address in [
        "up://0.0.0.0:11451",
        "up://224.0.0.1:11451",
        "up://255.255.255.255:11451",
        "up://[::]:11451",
        "up://[ff02::1]:11451",
    ] {
        assert!(ReachableAddress::parse(address).is_err(), "{address}");
    }
}

#[test]
fn ipv6_addresses_have_one_bracket_pair() {
    let address = address("up://[::1]:11451");

    assert_eq!(address.host(), "::1");
    assert_eq!(address.to_string(), "up://[::1]:11451");
}

#[test]
fn legacy_bracketed_ipv6_host_is_canonicalized_when_deserialized() {
    let address: ReachableAddress =
        serde_json::from_str(r#"{"host":"[2001:0db8:0:0:0:0:0:1]","port":11451}"#).unwrap();

    assert_eq!(address.host(), "2001:db8::1");
    assert_eq!(address.to_string(), "up://[2001:db8::1]:11451");
    assert_eq!(
        serde_json::to_value(address).unwrap(),
        serde_json::json!({"host": "2001:db8::1", "port": 11451})
    );
}

#[test]
fn persisted_addresses_use_the_same_validation_as_parsed_urls() {
    for value in [
        serde_json::json!({"host": "", "port": 11451}),
        serde_json::json!({"host": "example.com/path", "port": 11451}),
        serde_json::json!({"host": "0.0.0.0", "port": 11451}),
        serde_json::json!({"host": "::", "port": 11451}),
        serde_json::json!({"host": "example.com", "port": 0}),
    ] {
        assert!(
            serde_json::from_value::<ReachableAddress>(value.clone()).is_err(),
            "{value}"
        );
    }
}

#[test]
fn legacy_reachability_lease_defaults_the_discovery_time() {
    let lease: ReachableAddressLease = serde_json::from_value(serde_json::json!({
        "node_id": Uuid::nil(),
        "address": {"host": "legacy.example", "port": 11451},
        "source": {"Service": {"url": "https://discovery.example/nodes"}},
        "expires_at_ms": 20
    }))
    .unwrap();

    assert_eq!(lease.discovered_at_ms, 0);
}

#[test]
fn rejects_non_address_url_components() {
    for value in [
        "up://user@example.com:11451",
        "up://example.com:11451/path",
        "up://example.com:11451?query=value",
        "up://example.com:11451#fragment",
        "up://example.com:0",
    ] {
        assert!(ReachableAddress::parse(value).is_err(), "{value}");
    }
}

#[test]
fn rejects_missing_port() {
    assert!(ReachableAddress::parse("up://example.com").is_err());
    assert!(ReachableAddress::parse("up://[::1]").is_err());
}

#[test]
fn configured_reachable_addresses_replace_the_owned_set() {
    let first = address("up://first.example:11451");
    let second = address("up://second.example:11451");
    let mut reachability = NodeReachability::configured(BTreeSet::from([first]));

    reachability.replace_configured(BTreeSet::from([second.clone()]));

    assert_eq!(
        reachability.configured_reachable_addresses(),
        &BTreeSet::from([second.clone()])
    );
    assert_eq!(
        reachability.preferred_reachable_address(Uuid::nil(), 1),
        Some(&second),
    );
}

#[test]
fn moving_an_address_to_a_new_discovery_lease_requires_new_verification() {
    let candidate = address("up://moved.example:11451");
    let mut reachability = NodeReachability::configured(BTreeSet::from([candidate.clone()]));
    reachability.verify(&candidate, 10);
    reachability.renew(
        candidate.clone(),
        DiscoverySource::Service {
            url: "https://discovery.example/nodes".to_owned(),
        },
        20,
        30,
    );

    reachability.replace_configured(BTreeSet::new());

    assert!(!reachability.verified_addresses().contains_key(&candidate));
    assert!(!reachability.is_reachable(&candidate, Uuid::nil(), 20));
}

#[test]
fn ordered_candidates_prefer_configured_reachable_addresses() {
    let configured = address("up://z-configured.example:11451");
    let discovered = address("up://a-discovered.example:11451");
    let mut reachability = NodeReachability::configured(BTreeSet::from([configured.clone()]));
    reachability.renew(
        discovered.clone(),
        DiscoverySource::Service {
            url: "https://discovery.example/nodes".to_owned(),
        },
        10,
        20,
    );

    assert_eq!(
        reachability.ordered_candidates(Uuid::nil(), 10),
        vec![configured, discovered]
    );
}

#[test]
fn service_candidates_can_seed_join_links_but_need_verification_for_routes() {
    let candidate = address("up://discovered.example:11451");
    let mut reachability = NodeReachability::default();
    reachability.renew(
        candidate.clone(),
        DiscoverySource::Service {
            url: "https://discovery.example/nodes".to_owned(),
        },
        10,
        20,
    );

    assert_eq!(
        reachability.preferred_reachable_address(Uuid::nil(), 10),
        Some(&candidate),
    );
    assert!(reachability.reachable(Uuid::nil(), 10).is_empty());
    assert!(!reachability.is_reachable(&candidate, Uuid::nil(), 10));
    reachability.verify(&candidate, 11);
    assert_eq!(reachability.verified_addresses().get(&candidate), Some(&11));
    assert_eq!(
        reachability.reachable(Uuid::nil(), 10),
        BTreeSet::from([candidate.clone()])
    );
    assert!(reachability.is_reachable(&candidate, Uuid::nil(), 10));
    assert_eq!(
        reachability.preferred_reachable_address(Uuid::nil(), 10),
        Some(&candidate),
    );

    reachability.expire(20);
    assert!(reachability.candidates(Uuid::nil(), 20).is_empty());
    assert!(reachability.verified_addresses().is_empty());
    assert!(!reachability.is_reachable(&candidate, Uuid::nil(), 20));
}

#[test]
fn one_active_source_retains_a_discovered_address() {
    let candidate = address("up://shared.example:11451");
    let mut reachability = NodeReachability::default();
    reachability.renew(
        candidate.clone(),
        DiscoverySource::Service {
            url: "https://first.example/nodes".to_owned(),
        },
        1,
        10,
    );
    reachability.renew(
        candidate.clone(),
        DiscoverySource::Node {
            discovering_node_id: Uuid::nil(),
        },
        2,
        30,
    );

    reachability.expire(10);

    assert_eq!(
        reachability.candidates(Uuid::nil(), 20),
        BTreeSet::from([candidate])
    );
}

#[test]
fn older_renewal_does_not_shorten_an_active_lease() {
    let candidate = address("up://shared.example:11451");
    let source = DiscoverySource::Service {
        url: "https://discovery.example/nodes".to_owned(),
    };
    let mut reachability = NodeReachability::default();
    reachability.renew(candidate.clone(), source.clone(), 10, 100);
    reachability.renew(candidate.clone(), source, 5, 50);

    assert_eq!(
        reachability.candidates(Uuid::nil(), 75),
        BTreeSet::from([candidate])
    );
}

#[test]
fn expired_lease_requires_new_verification() {
    let candidate = address("up://renewed.example:11451");
    let source = DiscoverySource::Service {
        url: "https://discovery.example/nodes".to_owned(),
    };
    let mut reachability = NodeReachability::default();
    reachability.renew(candidate.clone(), source.clone(), 1, 10);
    reachability.verify(&candidate, 5);
    assert_eq!(reachability.verified_addresses().get(&candidate), Some(&5));

    reachability.renew(candidate.clone(), source, 20, 30);

    assert!(!reachability.verified_addresses().contains_key(&candidate));
    reachability.verify(&candidate, 19);
    reachability.verify(&candidate, 30);
    assert!(!reachability.verified_addresses().contains_key(&candidate));
    reachability.verify(&candidate, 21);
    assert_eq!(reachability.verified_addresses().get(&candidate), Some(&21));
}
#[test]
fn active_service_can_seed_a_join_link_without_node_verification() {
    let service_address = address("up://service.example:11451");
    let reachable_address_candidate = address("up://node.example:11451");
    let source_node_id = Uuid::from_u128(7);
    let mut reachability = NodeReachability::default();
    reachability.renew(
        service_address.clone(),
        DiscoverySource::Service {
            url: "https://discovery.example/nodes".to_owned(),
        },
        1,
        30,
    );
    reachability.renew(
        reachable_address_candidate,
        DiscoverySource::Node {
            discovering_node_id: source_node_id,
        },
        1,
        30,
    );

    assert_eq!(
        reachability.preferred_reachable_address(source_node_id, 10),
        Some(&service_address)
    );
    assert_eq!(
        reachability.preferred_published_address(10),
        Some(&service_address)
    );
    assert_eq!(
        reachability.preferred_reachable_address(source_node_id, 30),
        None,
    );
}

#[test]
fn peer_verified_address_can_publish_a_join_link() {
    let candidate = address("up://peer-verified.example:11451");
    let peer = Uuid::from_u128(7);
    let local = Uuid::from_u128(8);
    let mut reachability = NodeReachability::default();
    reachability.renew(
        candidate.clone(),
        DiscoverySource::Node {
            discovering_node_id: peer,
        },
        1,
        30,
    );
    reachability.verify(&candidate, 2);

    assert_eq!(reachability.preferred_reachable_address(local, 10), None);
    assert_eq!(
        reachability.preferred_published_address(10),
        Some(&candidate)
    );
    assert_eq!(reachability.preferred_published_address(30), None);
}

#[test]
fn configured_reachable_address_is_preferred_for_join_links() {
    let configured = address("up://configured.example:11451");
    let discovered = address("up://discovered.example:11451");
    let source = DiscoverySource::Service {
        url: "https://discovery.example/nodes".to_owned(),
    };
    let mut reachability = NodeReachability::configured(BTreeSet::from([configured.clone()]));
    reachability.renew(discovered, source, 1, 30);

    assert_eq!(
        reachability.preferred_reachable_address(Uuid::nil(), 10),
        Some(&configured),
    );
    assert_eq!(
        reachability.preferred_published_address(10),
        Some(&configured),
    );
}

#[test]
fn reachable_address_candidates_are_scoped_to_the_source_node() {
    let source_a = Uuid::from_u128(1);
    let source_b = Uuid::from_u128(2);
    let source_reachable_address_candidate = address("up://node.example:11451");
    let service_address = address("up://service.example:11451");
    let configured_address = address("up://configured.example:11451");
    let mut reachability =
        NodeReachability::configured(BTreeSet::from([configured_address.clone()]));
    reachability.renew(
        source_reachable_address_candidate.clone(),
        DiscoverySource::Node {
            discovering_node_id: source_a,
        },
        1,
        20,
    );
    reachability.renew(
        service_address.clone(),
        DiscoverySource::Service {
            url: "https://discovery.example/nodes".to_owned(),
        },
        1,
        20,
    );
    reachability.verify(&source_reachable_address_candidate, 1);
    reachability.verify(&service_address, 1);

    assert_eq!(
        reachability.candidates(source_a, 10),
        BTreeSet::from([
            configured_address.clone(),
            source_reachable_address_candidate.clone(),
            service_address.clone(),
        ])
    );
    assert_eq!(
        reachability.candidates(source_b, 10),
        BTreeSet::from([configured_address.clone(), service_address.clone()])
    );
    assert_eq!(
        reachability.reachable(source_b, 10),
        BTreeSet::from([configured_address, service_address])
    );
}

#[test]
fn reachability_round_trips_through_json() {
    let candidate = address("up://discovered.example:11451");
    let mut reachability = NodeReachability::default();
    reachability.renew(
        candidate.clone(),
        DiscoverySource::Node {
            discovering_node_id: Uuid::nil(),
        },
        1,
        30,
    );
    reachability.verify(&candidate, 10);

    let encoded = serde_json::to_vec(&reachability).unwrap();
    let decoded: NodeReachability = serde_json::from_slice(&encoded).unwrap();

    assert_eq!(decoded, reachability);
}
