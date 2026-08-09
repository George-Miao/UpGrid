use std::cell::RefCell;
use std::rc::Rc;

use compio::net::ToSocketAddrsAsync;
use compio_quic::{Connection, Endpoint};
use futures_core::Stream;
use quick_cache::unsync::Cache;
use snafu::futures::TryFutureExt as _;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    QuicConnectSnafu, QuicConnectionSnafu, QuicIncomingSnafu, ResolveEmptySnafu, ResolveSnafu,
};
use crate::{FramedConn, Result, accept_framed, bi_stream_framed};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    host: String,
    port: u16,
}

/// A reusable QUIC transport that yields typed tarpc-compatible channels.
#[derive(Clone)]
pub struct RpcTransport {
    endpoint: Endpoint,
    peers: Rc<RefCell<Cache<Key, Connection>>>,
}

impl RpcTransport {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            peers: Rc::new(RefCell::new(Cache::new(64))),
        }
    }

    pub async fn connect<In, Out>(&self, host: &str, port: u16) -> Result<FramedConn<In, Out>> {
        let connection = if let Some(connection) = self.peers.borrow().get(&Key {
            host: host.to_owned(),
            port,
        }) {
            connection.clone()
        } else {
            let addr = (host, port)
                .to_socket_addrs_async()
                .context(ResolveSnafu { host })
                .await?
                .next()
                .context(ResolveEmptySnafu { host })?;
            let connection = self
                .endpoint
                .connect(addr, host, None)
                .context(QuicConnectSnafu { host, port })?
                .context(QuicConnectionSnafu { host, port })
                .await?;
            self.peers.borrow_mut().insert(
                Key {
                    host: host.to_owned(),
                    port,
                },
                connection.clone(),
            );
            connection
        };
        let (send, recv) = connection.open_bi_wait().context(QuicIncomingSnafu).await?;
        Ok(bi_stream_framed(send, recv))
    }

    pub fn invalidate(&self, host: &str, port: u16) {
        self.peers.borrow_mut().remove(&Key {
            host: host.to_owned(),
            port,
        });
    }

    pub async fn accept(&self) -> Option<Result<RpcSession>> {
        loop {
            let incoming = self.endpoint.wait_incoming().await?;
            if !incoming.remote_address_validated() {
                let _ = incoming.retry();
                continue;
            }
            return Some(
                incoming
                    .await
                    .map(|connection| RpcSession { connection })
                    .map_err(|source| crate::Error::QuicIncoming { source }),
            );
        }
    }
}

pub struct RpcSession {
    connection: Connection,
}

impl RpcSession {
    pub fn remote_address(&self) -> std::net::SocketAddr {
        self.connection.remote_address()
    }

    pub fn channels<In, Out>(self) -> impl Stream<Item = Result<FramedConn<In, Out>>> {
        accept_framed(self.connection)
    }
}
