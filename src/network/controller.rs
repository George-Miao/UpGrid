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
use openraft_rt_compio::futures::{FutureExt, StreamExt};
use quick_cache::{Equivalent, unsync::Cache};
use snafu::{OptionExt, ResultExt, futures::TryFutureExt as SnafuTryFutureExt};
use tap::Pipe;
use tarpc::{
    client::{Config, NewClient},
    server::{BaseChannel, Channel},
};
use tracing::{debug, info};

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
    conn: Connection,
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
    // TODO: Use connecting state and waker queue to prevent multiple simultaneous connection
    // effort
    connections: SharedCache<ConnectionKey, ConnectionEntry>,
}

impl Controller {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            raft: Rc::new(OnceCell::new()),
            endpoint,
            connections: Rc::new(RefCell::new(Cache::new(64))),
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
        let conn = self.get_conn(node.host(), node.port()).await?;
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

        Ok(client)
    }

    /// Get connection to another node. If the connection does not exist, create
    /// a new one and spawn its server task.
    pub async fn get_conn(&self, host: &str, port: u16) -> Result<Connection> {
        if let Some(conn) = self.connections.borrow().get(&(host, port)) {
            debug!("Cache hit for connection to {host}:{port}");
            return Ok(conn.conn.clone());
        }

        debug!("Cache miss for connection to {host}:{port}");

        let addr = (host, port)
            .to_socket_addrs_async()
            .context(ResolveSnafu { host })
            .await?
            .next()
            .context(ResolveEmptySnafu { host })?;

        let conn = self
            .endpoint
            .connect(addr, host, None)
            .context(QuicConnectSnafu { host, port })?
            .context(QuicConnectionSnafu { host, port })
            .await?;

        let key = ConnectionKey {
            host: host.to_string(),
            port,
        };

        let entry = ConnectionEntry {
            conn: conn.clone(),
            _server_handle: spawn(self.serve_conn(conn.clone())),
        };

        self.connections.borrow_mut().insert(key, entry);

        Ok(conn)
    }

    pub fn serve_conn(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        let server = UpgridServer::new(self.raft().clone());

        async move {
            let mut framed = pin!(accept_framed(conn));

            while let Some(conn) = framed.next().await {
                match conn {
                    Ok(conn) => {
                        let server = server.clone();
                        spawn(async move {
                            BaseChannel::with_defaults(conn)
                                .requests()
                                .execute(server.serve())
                                .for_each(|fut| async move {
                                    spawn(fut).detach();
                                })
                                .await;

                            info!("Disconnected");
                        })
                        .detach()
                    }
                    Err(e) => {
                        debug!(error = %e, "Failed to accept connection");
                    }
                }
            }

            info!("Connection closed");
        }
    }
}
