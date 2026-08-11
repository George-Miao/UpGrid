//! Monitor client setup and worker startup.

use std::sync::Arc;

use compio::runtime::spawn;
use cyper::Client;
use upgrid_config::Cipher;
use upgrid_raft::Handle;
use upgrid_transport::SkipServerVerification;

use crate::{node, probe, schedule};

#[derive(Clone)]
pub(super) struct Clients {
    pub(super) verified: Client,
    pub(super) insecure: Client,
}

/// Starts Target scheduling and probe workers in the current Compio runtime.
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
