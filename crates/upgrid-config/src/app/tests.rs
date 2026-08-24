use clap::Parser;

use super::*;

#[derive(Debug, Parser)]
#[command(name = "upgrid")]
struct TestCli {
    #[command(flatten)]
    config: ConfigArgs,
}

#[test]
fn node_identity_survives_reopen() {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    let first = load_or_create_node_id(&directory).unwrap();
    let second = load_or_create_node_id(&directory).unwrap();
    assert_eq!(first, second);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn deployment_key_survives_reopen_and_is_required_for_join() {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    let first = load_or_create_cipher(&directory, None, false).unwrap();
    let second = load_or_create_cipher(&directory, None, true).unwrap();
    assert_eq!(first.encoded(), second.encoded());

    let joining = directory.join("joining");
    fs::create_dir_all(&joining).unwrap();
    assert!(load_or_create_cipher(&joining, None, true).is_err());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn configured_deployment_and_quic_ca_keys_survive_reopen() {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
    let quic_ca = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();

    let stored_cipher = load_or_create_cipher(&directory, Some(&cipher), false).unwrap();
    let stored_quic_ca =
        load_or_create_quic_ca_key(&directory, Some(&quic_ca), &stored_cipher).unwrap();

    assert_eq!(stored_cipher.encoded(), cipher.encoded());
    assert_eq!(stored_quic_ca, quic_ca);
    assert!(
        load_or_create_quic_ca_key(&directory, Some(&QuicCaKey::derive(&cipher)), &cipher,)
            .is_err()
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn tls_configuration_requires_a_certificate_and_key() {
    let cert_only = RawConfig {
        tls_cert: Some(PathBuf::from("cert.pem")),
        ..RawConfig::default()
    };
    assert!(Config::try_from(cert_only).is_err());

    let pair = RawConfig {
        tls_cert: Some(PathBuf::from("cert.pem")),
        tls_key: Some(PathBuf::from("key.pem")),
        ..RawConfig::default()
    };
    assert!(Config::try_from(pair).is_ok());
}

#[test]
fn zero_raft_port_is_rejected() {
    let error = Config::try_from(RawConfig {
        raft_port: 0,
        ..RawConfig::default()
    });

    assert!(matches!(
        error,
        Err(Error::InvalidConfiguration {
            message: "raft_port must be nonzero",
        })
    ));
}

#[test]
fn history_retention_windows_use_distinct_units() {
    let config = Config::try_from(RawConfig {
        history_retention_hours: Some(2),
        history_rollup_retention_days: Some(3),
        target_trash_retention_days: Some(4),
        ..RawConfig::default()
    })
    .unwrap();

    assert_eq!(config.history_retention_ms, Some(2 * 60 * 60 * 1_000));
    assert_eq!(
        config.history_rollup_retention_ms,
        Some(3 * 24 * 60 * 60 * 1_000)
    );
    assert_eq!(
        config.target_trash_retention_ms,
        Some(4 * 24 * 60 * 60 * 1_000)
    );
    assert!(
        Config::try_from(RawConfig {
            target_trash_retention_days: Some(0),
            ..RawConfig::default()
        })
        .is_err()
    );
}

#[test]
fn cli_overrides_toml_configuration() {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("upgrid.toml");
    fs::write(
        &path,
        "bind = \"127.0.0.1:9000\"\nusername = \"from-file\"\nlocal_addresses = [\"10.0.0.1\"]\nraft_port = 12000\nreachable_addresses = [\"up://file.example:12000\"]\ndiscovery_urls = [\"https://file.example/nodes\"]\n",
    )
    .unwrap();
    let args = TestCli::try_parse_from([
        "upgrid",
        "--config",
        path.to_str().unwrap(),
        "--bind",
        "127.0.0.1:9001",
        "--local-address",
        "10.0.0.2",
        "--local-address",
        "10.0.0.3",
        "--raft-port",
        "13000",
        "--reachable-address",
        "up://cli-a.example:13000",
        "--reachable-address",
        "up://cli-b.example:13000",
        "--discovery-url",
        "https://cli.example/nodes",
    ])
    .unwrap()
    .config;

    let config = load_with(args, false).unwrap();

    assert_eq!(config.bind, "127.0.0.1:9001");
    assert_eq!(config.username, "from-file");
    assert_eq!(
        config.local_addresses,
        BTreeSet::from([
            LocalAddress {
                host: "10.0.0.2".parse().unwrap(),
                port: 13000,
            },
            LocalAddress {
                host: "10.0.0.3".parse().unwrap(),
                port: 13000,
            },
        ])
    );
    assert_eq!(
        config.reachable_addresses,
        BTreeSet::from([
            ReachableAddress::parse("up://cli-a.example:13000").unwrap(),
            ReachableAddress::parse("up://cli-b.example:13000").unwrap(),
        ])
    );
    assert!(config.reachable_addresses_explicit);
    assert_eq!(
        config.discovery_urls,
        BTreeSet::from(["https://cli.example/nodes".parse().unwrap()])
    );
    assert!(config.discovery_urls_explicit);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn empty_toml_discovery_urls_are_explicit() {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("upgrid.toml");
    fs::write(&path, "discovery_urls = []\n").unwrap();
    let args = TestCli::try_parse_from(["upgrid", "--config", path.to_str().unwrap()])
        .unwrap()
        .config;

    let config = load_with(args, false).unwrap();

    assert!(config.discovery_urls.is_empty());
    assert!(config.discovery_urls_explicit);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn obsolete_toml_raft_url_is_rejected() {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("upgrid.toml");
    fs::write(&path, "raft_url = \"up://legacy.example:11451\"\n").unwrap();
    let args = TestCli::try_parse_from(["upgrid", "--config", path.to_str().unwrap()])
        .unwrap()
        .config;

    let Err(error) = load_with(args, false) else {
        panic!("obsolete raft_url was accepted");
    };

    assert!(matches!(&error, Error::ObsoleteRaftUrl));
    assert!(error.to_string().contains("reachable_addresses"));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
#[allow(clippy::result_large_err)] // Required by Figment's fixed Jail callback signature.
fn obsolete_environment_raft_url_is_rejected() {
    figment::Jail::expect_with(|jail| {
        jail.set_env("UPGRID_RAFT_URL", "up://legacy.example:11451");
        let args = TestCli::try_parse_from(["upgrid"]).unwrap().config;

        let Err(error) = load_with(args, true) else {
            panic!("obsolete UPGRID_RAFT_URL was accepted");
        };

        assert!(matches!(&error, Error::ObsoleteRaftUrl));
        assert!(error.to_string().contains("raft_port"));
        Ok(())
    });
}

#[test]
#[allow(clippy::result_large_err)] // Required by Figment's fixed Jail callback signature.
fn environment_can_select_a_new_cluster() {
    figment::Jail::expect_with(|jail| {
        jail.set_env("UPGRID_NEW_CLUSTER", "true");
        jail.set_env("UPGRID_LOCAL_ADDRESSES", "[\"10.0.0.4\",\"10.0.0.5\"]");
        jail.set_env("UPGRID_RAFT_PORT", "14000");
        jail.set_env(
            "UPGRID_REACHABLE_ADDRESSES",
            "[\"up://env-a.example:14000\",\"up://env-b.example:14000\"]",
        );
        jail.set_env(
            "UPGRID_DISCOVERY_URLS",
            "[\"https://discovery.example/nodes\"]",
        );
        let raw: RawConfig = Figment::from(Serialized::defaults(RawConfig::default()))
            .merge(Env::prefixed("UPGRID_"))
            .extract()?;
        assert!(raw.new_cluster);
        assert_eq!(
            raw.local_addresses,
            BTreeSet::from(["10.0.0.4".parse().unwrap(), "10.0.0.5".parse().unwrap()])
        );
        assert_eq!(raw.raft_port, 14000);
        assert_eq!(
            raw.reachable_addresses,
            Some(BTreeSet::from([
                "up://env-a.example:14000".to_owned(),
                "up://env-b.example:14000".to_owned(),
            ]))
        );
        assert_eq!(
            raw.discovery_urls,
            Some(BTreeSet::from(["https://discovery.example/nodes"
                .parse()
                .unwrap()]))
        );
        Ok(())
    });
}

#[test]
fn default_cluster_transport_binds_only_to_loopback() {
    let config = Config::try_from(RawConfig::default()).unwrap();
    assert_eq!(
        config.local_addresses,
        BTreeSet::from([LocalAddress {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 11451,
        }])
    );
    assert!(config.reachable_addresses.is_empty());
    assert!(!config.reachable_addresses_explicit);
    assert!(config.discovery_urls.is_empty());
    assert!(!config.discovery_urls_explicit);
}

#[test]
fn discovery_service_count_is_bounded() {
    let discovery_urls = (0..=crate::MAX_DISCOVERY_SERVICES)
        .map(|index| {
            format!("https://discovery-{index}.example/nodes")
                .parse()
                .unwrap()
        })
        .collect();
    let error = Config::try_from(RawConfig {
        discovery_urls: Some(discovery_urls),
        ..RawConfig::default()
    });

    assert!(matches!(
        error,
        Err(Error::InvalidConfiguration {
            message: "discovery_urls contains more than 8 services"
        })
    ));
}

#[test]
fn reachable_addresses_may_use_translated_ports() {
    let raw = RawConfig {
        reachable_addresses: Some(BTreeSet::from(["up://translated.example:443".to_owned()])),
        ..RawConfig::default()
    };

    let config = Config::try_from(raw).unwrap();
    assert_eq!(
        config.local_addresses,
        BTreeSet::from([LocalAddress {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 11451,
        }])
    );

    assert_eq!(
        config.reachable_addresses,
        BTreeSet::from([ReachableAddress::parse("up://translated.example:443").unwrap()])
    );
}

#[test]
fn invalid_reachable_address_is_rejected_during_config_loading() {
    let raw = RawConfig {
        reachable_addresses: Some(BTreeSet::from(["http://node.example:11451".to_owned()])),
        ..RawConfig::default()
    };

    let Err(error) = Config::try_from(raw) else {
        panic!("invalid reachable address was accepted");
    };

    assert!(matches!(error, Error::ConfiguredReachableAddress { .. }));
}
