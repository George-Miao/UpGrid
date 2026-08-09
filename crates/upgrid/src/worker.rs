//! Target assignment and probe execution.

mod http;
mod node;
mod probe;
mod schedule;

use std::sync::Arc;

use compio::runtime::spawn;
use cyper::Client;
use upgrid_config::Cipher;
use upgrid_raft::Handle;
use upgrid_transport::SkipServerVerification;

#[derive(Clone)]
struct Clients {
    verified: Client,
    insecure: Client,
}

pub fn start(cluster: Handle, cipher: Cipher) {
    let insecure_tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    let clients = Clients {
        verified: Client::builder()
            .use_rustls_default()
            .build()
            .expect("default HTTP client configuration should be valid"),
        insecure: Client::builder()
            .use_rustls(Arc::new(insecure_tls))
            .build()
            .expect("insecure HTTP client configuration should be valid"),
    };
    spawn(schedule::run(cluster.clone())).detach();
    spawn(probe::run(cluster.clone(), clients, cipher)).detach();
    spawn(node::run(cluster)).detach();
}
