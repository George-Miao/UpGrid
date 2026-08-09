#![cfg(test)]

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

use crate::app::now_ms;
use crate::domain::Command;
use crate::node::Node;
use crate::raft::{Identity, Req};
use crate::secret::hash_join_token;
use crate::utils::{unsafe_endpoint, uuid_v7_now};

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
    node.write(Req {
        operation_id: uuid_v7_now(),
        submitted_at_ms: now_ms(),
        command: Command::PutJoinToken {
            hash: hash_join_token("test-join-token"),
            expires_at_ms: now_ms().saturating_add(60_000),
        },
    })
    .await
    .unwrap();

    info!("Node 1 started");

    sleep(Duration::from_secs(114514)).await
}

#[compio::test]
async fn worker() {
    init_tracing();

    let node = Node::new("up://127.0.0.1:8081").await.unwrap();
    sleep(Duration::from_secs(1)).await;
    info!("Node 2 started");

    node.join("up://127.0.0.1:8080", "test-join-token")
        .await
        .unwrap();

    info!("Node 2 joined");
}

#[compio::test]
async fn master_raw() {
    init_tracing();

    let id = Identity::new("up://127.0.0.1:8080").unwrap();

    let endpoint = unsafe_endpoint(id.node.host().to_owned(), id.node.port())
        .await
        .unwrap();

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
async fn worker_raw() {
    init_tracing();

    let id = Identity::new("up://127.0.0.1:8081").unwrap();

    let endpoint = unsafe_endpoint(id.node.host().to_owned(), id.node.port())
        .await
        .unwrap();

    let conn = endpoint
        .connect("127.0.0.1:8080".parse().unwrap(), "127.0.0.1", None)
        .unwrap()
        .await
        .unwrap();

    let (mut bi_tx, _) = conn.open_bi_wait().await.unwrap();

    let _ = bi_tx.write_all(b"Hello, world!").await.unwrap();

    bi_tx.finish().unwrap();

    sleep(Duration::from_secs(1)).await;
}
