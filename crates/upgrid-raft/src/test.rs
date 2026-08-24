#![cfg(test)]

//! Multi-Node manual fixtures.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use compio::io::AsyncWriteExt;
use compio::runtime::spawn;
use compio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use upgrid_config::{LocalAddress, QuicCaKey, now_ms};
use upgrid_transport::secure_endpoints;

use crate::domain::Command;
use crate::node::Node;
use crate::raft::NodeRegistration;
use crate::token::hash_join_token;

fn init_tracing() {
    let target_filter = Targets::new()
        .with_default(LevelFilter::INFO)
        .with_target("upgrid", LevelFilter::DEBUG)
        .with_target("rustls", LevelFilter::WARN)
        .with_target("compio", LevelFilter::TRACE);

    let fmt = tracing_subscriber::fmt::layer().with_filter(target_filter);
    tracing_subscriber::registry().with(fmt).try_init().ok();
}

#[compio::test]
#[ignore = "manual multi-node fixture uses fixed ports 8080 and 8081"]
async fn master() {
    init_tracing();

    spawn(async move {
        loop {
            info!("Tick");
            sleep(Duration::from_secs(1)).await;
        }
    })
    .detach();

    let node = Node::new("up://127.0.0.1:8080").await.unwrap();
    sleep(Duration::from_secs(1)).await;

    node.start_cluster().await.unwrap();
    node.apply(Command::PutJoinToken {
        hash: hash_join_token("test-join-token"),
        expires_at_ms: now_ms().saturating_add(60_000),
    })
    .await
    .unwrap();

    info!("Node 1 started");

    sleep(Duration::from_secs(114514)).await
}

#[compio::test]
#[ignore = "manual multi-node fixture uses fixed ports 8080 and 8081"]
async fn worker() {
    init_tracing();

    let node = Node::new("up://127.0.0.1:8081").await.unwrap();
    sleep(Duration::from_secs(1)).await;
    info!("Node 2 started");

    node.join(
        uuid::Uuid::from_u128(1),
        "up://127.0.0.1:8080",
        "test-join-token",
    )
    .await
    .unwrap();

    info!("Node 2 joined");
}

#[compio::test]
#[ignore = "manual multi-node fixture uses fixed ports 8080 and 8081"]
async fn master_raw() {
    init_tracing();

    let id = NodeRegistration::new("up://127.0.0.1:8080").unwrap();

    let quic_ca_key = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
    let mut endpoints = secure_endpoints(
        &BTreeSet::from([LocalAddress {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: id.bootstrap.port(),
        }]),
        id.id,
        &quic_ca_key,
    )
    .await
    .unwrap();
    let endpoint = endpoints.pop().unwrap();

    while let Some(incoming) = endpoint.wait_incoming().await {
        info!("Received incoming connection: {:?}", incoming);
        spawn(async move {
            let conn = incoming.await.unwrap();
            let (_, mut bi_rx) = conn.accept_bi().await.unwrap();
            info!("Connection accepted");
            let (_, buf) = bi_rx.read_to_end(Vec::new()).await.unwrap();
            info!("Received data: {:?}", unsafe {
                String::from_utf8_unchecked(buf)
            });
        })
        .detach();
    }
}

#[compio::test]
#[ignore = "manual multi-node fixture uses fixed ports 8080 and 8081"]
async fn worker_raw() {
    init_tracing();

    let id = NodeRegistration::new("up://127.0.0.1:8081").unwrap();

    let quic_ca_key = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
    let mut endpoints = secure_endpoints(
        &BTreeSet::from([LocalAddress {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: id.bootstrap.port(),
        }]),
        id.id,
        &quic_ca_key,
    )
    .await
    .unwrap();
    let endpoint = endpoints.pop().unwrap();

    let conn = endpoint
        .connect("127.0.0.1:8080".parse().unwrap(), "upgrid-node", None)
        .unwrap()
        .await
        .unwrap();

    let (mut bi_tx, _) = conn.open_bi_wait().await.unwrap();

    let _ = bi_tx.write_all(b"Hello, world!").await.unwrap();

    bi_tx.finish().unwrap();

    sleep(Duration::from_secs(1)).await;
}
