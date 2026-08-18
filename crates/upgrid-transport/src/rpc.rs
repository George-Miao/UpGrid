use std::cell::RefCell;
use std::rc::Rc;

use compio::net::ToSocketAddrsAsync;
use compio_quic::{Connection, Endpoint};
use futures_core::Stream;
use quick_cache::unsync::Cache;
use snafu::futures::TryFutureExt as _;
use snafu::{OptionExt, ResultExt};
use upgrid_config::QuicCaKey;

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
    /// Binds a mutual-TLS endpoint that validates peer certificates against the
    /// deployment CA.
    pub async fn bind(host: &str, port: u16, quic_ca_key: &QuicCaKey) -> Result<Self> {
        let endpoint = crate::tls::secure_endpoint(host.to_owned(), port, quic_ca_key).await?;
        Ok(Self::new(endpoint))
    }

    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            peers: Rc::new(RefCell::new(Cache::new(64))),
        }
    }

    pub async fn connect<In, Out>(&self, host: &str, port: u16) -> Result<FramedConn<In, Out>> {
        let key = Key {
            host: host.to_owned(),
            port,
        };
        let cached = self
            .peers
            .borrow()
            .get(&key)
            .filter(|connection| connection.close_reason().is_none())
            .cloned();
        let connection = match cached {
            Some(connection) => connection,
            None => {
                self.peers.borrow_mut().remove(&key);
                self.establish(&key).await?
            }
        };
        let (send, recv) = match connection.open_bi_wait().await {
            Ok(streams) => streams,
            Err(_) => {
                self.peers.borrow_mut().remove(&key);
                self.establish(&key)
                    .await?
                    .open_bi_wait()
                    .context(QuicIncomingSnafu)
                    .await?
            }
        };
        Ok(bi_stream_framed(send, recv))
    }

    async fn establish(&self, key: &Key) -> Result<Connection> {
        let addr = (key.host.as_str(), key.port)
            .to_socket_addrs_async()
            .context(ResolveSnafu { host: &key.host })
            .await?
            .next()
            .context(ResolveEmptySnafu { host: &key.host })?;
        let connection = self
            .endpoint
            .connect(addr, &key.host, None)
            .context(QuicConnectSnafu {
                host: &key.host,
                port: key.port,
            })?
            .context(QuicConnectionSnafu {
                host: &key.host,
                port: key.port,
            })
            .await?;
        self.peers
            .borrow_mut()
            .insert(key.clone(), connection.clone());
        Ok(connection)
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

#[cfg(test)]
mod tests {
    use compio::runtime::spawn;
    use upgrid_config::Cipher;

    use super::*;

    #[compio::test]
    async fn reconnects_after_cached_connection_closes() {
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
        let quic_ca_key = QuicCaKey::derive(&cipher);
        let server = RpcTransport::bind("127.0.0.1", 0, &quic_ca_key)
            .await
            .unwrap();
        let server_port = server.endpoint.local_addr().unwrap().port();
        let client = RpcTransport::bind("127.0.0.1", 0, &quic_ca_key)
            .await
            .unwrap();

        let first_accept = spawn({
            let server = server.clone();
            async move { server.accept().await.unwrap().unwrap() }
        });
        let first = client
            .connect::<u8, u8>("127.0.0.1", server_port)
            .await
            .unwrap();
        let _first_session = first_accept.await.unwrap();
        drop(first);

        let key = Key {
            host: "127.0.0.1".to_owned(),
            port: server_port,
        };
        client
            .peers
            .borrow()
            .get(&key)
            .unwrap()
            .close(0_u32.into(), b"test reconnect");

        let second_accept = spawn({
            let server = server.clone();
            async move { server.accept().await.unwrap().unwrap() }
        });
        client
            .connect::<u8, u8>("127.0.0.1", server_port)
            .await
            .unwrap();
        let _second_session = second_accept.await.unwrap();
    }
}
