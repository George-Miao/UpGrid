use uuid::Uuid;

use super::*;
use crate::{DiscoverySource, ReachableAddressCandidate};

#[test]
fn reachable_address_candidates_do_not_become_configured_reachable_addresses() {
    let candidate = ReachableAddress::parse("up://discovered.example:11451").unwrap();
    let network = NodeNetworkConfig::new(
        BTreeSet::from([LocalAddress {
            host: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            port: 11451,
        }]),
        BTreeSet::new(),
        false,
        vec![ReachableAddressCandidate {
            address: candidate.clone(),
            source: DiscoverySource::Service {
                url: "https://discovery.example/nodes".to_owned(),
            },
        }],
    );

    assert!(network.configured.is_empty());
    assert_eq!(
        bootstrap_address(Uuid::nil(), &network.configured, &network.candidates).unwrap(),
        candidate,
    );
}

#[test]
fn explicit_empty_addresses_do_not_create_a_local_fallback() {
    let network = NodeNetworkConfig::new(
        BTreeSet::from([LocalAddress {
            host: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            port: 11451,
        }]),
        BTreeSet::new(),
        true,
        Vec::new(),
    );

    assert!(network.configured.is_empty());
}

#[compio::test]
async fn local_probe_succeeds_without_an_advertised_route() {
    let socket = std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let directory = std::env::temp_dir().join(format!("upgrid-node-test-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&directory).unwrap();
    let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
    let quic_ca_key = QuicCaKey::derive(&cipher);
    let node_id = Uuid::from_u128(9);
    let network = NodeNetworkConfig::new(
        BTreeSet::from([LocalAddress {
            host: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            port,
        }]),
        BTreeSet::new(),
        true,
        Vec::new(),
    );
    let node = Node::open(node_id, network, &directory, &cipher, &quic_ca_key)
        .await
        .unwrap();

    let address = node.probe_node(node_id).await.unwrap();

    assert_eq!(
        address,
        ReachableAddress::parse(&format!("up://127.0.0.1:{port}")).unwrap()
    );
    drop(node);
    std::fs::remove_dir_all(directory).unwrap();
}
