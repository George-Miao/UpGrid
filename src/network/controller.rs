use std::{
    cell::{OnceCell, RefCell},
    pin::pin,
    rc::Rc,
};

use compio::{
    net::ToSocketAddrsAsync,
    runtime::{JoinHandle, spawn},
};
use compio_quic::{Connection, Endpoint, Incoming};
use openraft::alias::NodeOf;
use openraft_rt_compio::futures::{FutureExt, StreamExt, lock::Mutex};
use quick_cache::{Equivalent, unsync::Cache};
use snafu::{OptionExt, ResultExt, futures::TryFutureExt as SnafuTryFutureExt};
use tap::Pipe;
use tarpc::{
    client::{Config, NewClient},
    server::{BaseChannel, Channel},
};
use tracing::debug;

use crate::{
    QuicConnectSnafu, QuicConnectionSnafu, QuicIncomingStreamSnafu, ResolveEmptySnafu,
    ResolveSnafu, Result,
    network::{
        UpgridServer,
        rpc::{UpgridService, UpgridServiceClient},
        transport::{accept_framed, bi_stream_framed},
    },
    raft::{Raft, TC},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnectionKey {
    host: String,
    port: u16,
}

#[derive(Debug)]
struct ConnectionEntry {
    _conn: Connection,
    client: UpgridServiceClient,
    _server_handle: JoinHandle<()>,
}

impl Equivalent<ConnectionKey> for (&str, u16) {
    fn equivalent(&self, other: &ConnectionKey) -> bool {
        self.0 == other.host && self.1 == other.port
    }
}

type SharedCache<K, V> = Rc<RefCell<Cache<K, V>>>;

#[derive(Clone)]
pub struct Controller {
    raft: Rc<OnceCell<Raft>>,
    endpoint: Endpoint,
    connections: SharedCache<ConnectionKey, ConnectionEntry>,
    // Serializes cache misses so concurrent heartbeats create one peer session.
    connection_changes: Rc<Mutex<()>>,
    membership_changes: Rc<Mutex<()>>,
    deployment_key_fingerprint: [u8; 32],
}

impl Controller {
    pub fn new(endpoint: Endpoint, deployment_key_fingerprint: [u8; 32]) -> Self {
        Self {
            raft: Rc::new(OnceCell::new()),
            endpoint,
            connections: Rc::new(RefCell::new(Cache::new(64))),
            connection_changes: Rc::new(Mutex::new(())),
            membership_changes: Rc::new(Mutex::new(())),
            deployment_key_fingerprint,
        }
    }

    pub fn init_raft(&self, raft: Raft) {
        if self.raft.set(raft).is_err() {
            panic!("Raft should be initialized only once");
        }
    }

    fn raft(&self) -> &Raft {
        self.raft.get().expect("Raft should be initialized")
    }

    pub fn invalidate_client(&self, node: &NodeOf<TC>) {
        if self
            .connections
            .borrow_mut()
            .remove(&(node.host(), node.port()))
            .is_some()
        {
            debug!(
                host = node.host(),
                port = node.port(),
                "invalidated RPC client"
            );
        }
    }

    pub async fn accept(&self, incoming: Incoming) {
        if !incoming.remote_address_validated() {
            incoming
                .retry()
                .expect("Un-validated connection should be retryable");
            return;
        }

        let conn = match incoming.await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!("Failed to establish connection: {}", e);
                return;
            }
        };

        self.serve_conn(conn.clone()).await

        // let id = match Controller::get_id_from_conn(&conn).await {
        //     Ok(id) => id,
        //     Err(e) => {
        //         tracing::warn!("Failed to get identity from connection: {}",
        // e);         return;
        //     }
        // };

        // let entry = ConnectionEntry {
        //     conn: conn.clone(),
        //     _server_handle: handle,
        // };

        // self.connections
        //     .borrow_mut()
        //     .insert(id.connection_key(), entry);
    }

    pub async fn get_client(&self, node: &NodeOf<TC>) -> Result<UpgridServiceClient> {
        if let Some(entry) = self.connections.borrow().get(&(node.host(), node.port())) {
            debug!(
                "Cache hit for RPC client to {}:{}",
                node.host(),
                node.port()
            );
            return Ok(entry.client.clone());
        }

        let _connection_change = self.connection_changes.lock().await;
        if let Some(entry) = self.connections.borrow().get(&(node.host(), node.port())) {
            debug!(
                "Cache hit for RPC client to {}:{}",
                node.host(),
                node.port()
            );
            return Ok(entry.client.clone());
        }

        debug!(
            "Cache miss for RPC client to {}:{}",
            node.host(),
            node.port()
        );

        let addr = (node.host(), node.port())
            .to_socket_addrs_async()
            .context(ResolveSnafu { host: node.host() })
            .await?
            .next()
            .context(ResolveEmptySnafu { host: node.host() })?;

        let conn = self
            .endpoint
            .connect(addr, node.host(), None)
            .context(QuicConnectSnafu {
                host: node.host(),
                port: node.port(),
            })?
            .context(QuicConnectionSnafu {
                host: node.host(),
                port: node.port(),
            })
            .await?;
        let transport = conn
            .open_bi_wait()
            .context(QuicIncomingStreamSnafu)
            .await?
            .pipe(|(s, r)| bi_stream_framed(s, r));

        let mut config = Config::default();
        config.max_in_flight_requests = 1024;
        config.pending_request_buffer = 8;

        let NewClient { client, dispatch } = UpgridServiceClient::new(config, transport);
        spawn(dispatch.map(|res| {
            if let Err(e) = res {
                debug!(error=%e, "client transport error")
            }
        }))
        .detach();

        let key = ConnectionKey {
            host: node.host().to_string(),
            port: node.port(),
        };

        let entry = ConnectionEntry {
            _conn: conn.clone(),
            client: client.clone(),
            _server_handle: spawn(self.serve_conn(conn.clone())),
        };

        self.connections.borrow_mut().insert(key, entry);

        Ok(client)
    }

    pub fn serve_conn(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        let server = UpgridServer::new(
            self.raft().clone(),
            self.membership_changes.clone(),
            self.deployment_key_fingerprint,
        );
        let peer = conn.remote_address();

        async move {
            let mut framed = pin!(accept_framed(conn));

            while let Some(conn) = framed.next().await {
                match conn {
                    Ok(conn) => {
                        let server = server.clone();
                        spawn(async move {
                            let mut requests = pin!(BaseChannel::with_defaults(conn).requests());
                            while let Some(request) = requests.next().await {
                                match request {
                                    Ok(request) => {
                                        spawn(request.execute(server.clone().serve())).detach()
                                    }
                                    Err(error) => {
                                        tracing::warn!(%peer, ?error, "Node RPC stream failed");
                                        break;
                                    }
                                }
                            }

                            debug!("RPC stream disconnected");
                        })
                        .detach()
                    }
                    Err(e) => {
                        debug!(error = %e, "Failed to accept connection");
                    }
                }
            }

            debug!("QUIC connection closed");
        }
    }
}
