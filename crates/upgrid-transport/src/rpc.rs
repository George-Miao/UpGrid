use std::cell::RefCell;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::rc::Rc;

use compio::net::ToSocketAddrsAsync;
use compio::runtime::spawn;
use compio_quic::{Connection, Endpoint, Incoming};
use futures_channel::mpsc::{UnboundedReceiver, unbounded};
use futures_core::Stream;
use futures_util::lock::Mutex;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use quick_cache::unsync::Cache;
use snafu::ResultExt;
use snafu::futures::TryFutureExt as _;
use upgrid_config::{LocalAddress, QuicCaKey};

use crate::error::{
    NoLocalAddressFamilySnafu, PeerIdentitySnafu, QuicConnectSnafu, QuicConnectionSnafu,
    QuicIncomingSnafu, ResolveEmptySnafu, ResolveSnafu,
};
use crate::{FramedConn, Result, accept_framed, bi_stream_framed};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    host: String,
    port: u16,
    node_id: uuid::Uuid,
}

fn is_unicast(address: std::net::SocketAddr) -> bool {
    match address.ip() {
        IpAddr::V4(address) => {
            !address.is_unspecified() && !address.is_multicast() && !address.is_broadcast()
        }
        IpAddr::V6(address) => !address.is_unspecified() && !address.is_multicast(),
    }
}

fn validated_socket_addresses(
    addresses: impl IntoIterator<Item = std::net::SocketAddr>,
) -> BTreeSet<std::net::SocketAddr> {
    addresses
        .into_iter()
        .filter(|address| is_unicast(*address))
        .collect()
}

/// A reusable QUIC transport that yields typed UpGrid RPC channels.
#[derive(Clone)]
pub struct RpcTransport {
    endpoints: Rc<[Endpoint]>,
    quic_ca_key: Option<QuicCaKey>,
    peers: Rc<RefCell<Cache<Key, Connection>>>,
    incoming: Rc<Mutex<UnboundedReceiver<Incoming>>>,
}

impl RpcTransport {
    /// Binds a mutual-TLS endpoint that validates peer certificates against the
    /// deployment CA.
    pub async fn bind(
        local_addresses: &BTreeSet<LocalAddress>,
        node_id: uuid::Uuid,
        quic_ca_key: &QuicCaKey,
    ) -> Result<Self> {
        let endpoints = crate::tls::secure_endpoints(local_addresses, node_id, quic_ca_key).await?;
        Ok(Self::new(endpoints, Some(quic_ca_key.clone())))
    }

    pub(crate) fn new(endpoints: Vec<Endpoint>, quic_ca_key: Option<QuicCaKey>) -> Self {
        let endpoints: Rc<[Endpoint]> = endpoints.into();
        let (incoming_sender, incoming) = unbounded();
        for endpoint in endpoints.iter() {
            let endpoint = endpoint.clone();
            let incoming_sender = incoming_sender.clone();
            spawn(async move {
                while let Some(incoming) = endpoint.wait_incoming().await {
                    if incoming_sender.unbounded_send(incoming).is_err() {
                        break;
                    }
                }
            })
            .detach();
        }
        drop(incoming_sender);
        Self {
            quic_ca_key,
            endpoints,
            peers: Rc::new(RefCell::new(Cache::new(64))),
            incoming: Rc::new(Mutex::new(incoming)),
        }
    }

    pub async fn connect<In, Out>(
        &self,
        host: &str,
        port: u16,
        node_id: uuid::Uuid,
    ) -> Result<FramedConn<In, Out>> {
        let key = Key {
            host: host.to_owned(),
            port,
            node_id,
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
        let addresses = (key.host.as_str(), key.port)
            .to_socket_addrs_async()
            .context(ResolveSnafu { host: &key.host })
            .await?;
        self.establish_resolved(key, addresses).await
    }

    async fn establish_resolved(
        &self,
        key: &Key,
        addresses: impl IntoIterator<Item = std::net::SocketAddr>,
    ) -> Result<Connection> {
        let addresses = validated_socket_addresses(addresses);
        if addresses.is_empty() {
            return ResolveEmptySnafu { host: &key.host }.fail();
        }
        let attempts = FuturesUnordered::new();
        for address in addresses {
            for endpoint in self.endpoints.iter().filter(|endpoint| {
                endpoint
                    .local_addr()
                    .is_ok_and(|local| local.is_ipv4() == address.is_ipv4())
            }) {
                let host = key.host.clone();
                let port = key.port;
                attempts.push(async move {
                    let connecting = endpoint
                        .connect(address, crate::tls::CLUSTER_SERVER_NAME, None)
                        .context(QuicConnectSnafu { host: &host, port })?;
                    connecting
                        .context(QuicConnectionSnafu { host: &host, port })
                        .await
                });
            }
        }
        if attempts.is_empty() {
            return NoLocalAddressFamilySnafu {
                host: &key.host,
                port: key.port,
            }
            .fail();
        }
        let mut last_error = None;
        let mut attempts = attempts;
        while let Some(result) = attempts.next().await {
            match result {
                Ok(connection) => match self.verify_peer_identity(&connection, key.node_id) {
                    Ok(()) => {
                        self.peers
                            .borrow_mut()
                            .insert(key.clone(), connection.clone());
                        return Ok(connection);
                    }
                    Err(error) => last_error = Some(error),
                },
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("a compatible local endpoint ensures one connection attempt"))
    }

    fn verify_peer_identity(
        &self,
        connection: &Connection,
        expected_node_id: uuid::Uuid,
    ) -> Result<()> {
        let Some(quic_ca_key) = &self.quic_ca_key else {
            return Ok(());
        };
        if !PeerIdentity::new(connection, Some(quic_ca_key.clone())).matches(expected_node_id) {
            return PeerIdentitySnafu { expected_node_id }.fail();
        }
        Ok(())
    }

    pub fn invalidate(&self, host: &str, port: u16, node_id: uuid::Uuid) {
        self.peers.borrow_mut().remove(&Key {
            host: host.to_owned(),
            port,
            node_id,
        });
    }

    pub async fn accept(&self) -> Option<Result<RpcSession>> {
        loop {
            let incoming = self.incoming.lock().await.next().await?;
            if !incoming.remote_address_validated() {
                let _ = incoming.retry();
                continue;
            }
            return Some(
                incoming
                    .await
                    .map(|connection| RpcSession {
                        peer_identity: PeerIdentity::new(&connection, self.quic_ca_key.clone()),
                        connection,
                    })
                    .map_err(|source| crate::Error::QuicIncoming { source }),
            );
        }
    }
}

#[derive(Clone)]
pub struct PeerAddress {
    connection: Connection,
}

impl PeerAddress {
    pub fn current(&self) -> std::net::SocketAddr {
        self.connection.remote_address()
    }
}
impl std::fmt::Display for PeerAddress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.current().fmt(formatter)
    }
}

#[derive(Clone)]
pub struct PeerIdentity {
    certificate: Option<rustls::pki_types::CertificateDer<'static>>,
    quic_ca_key: Option<QuicCaKey>,
}

impl PeerIdentity {
    fn new(connection: &Connection, quic_ca_key: Option<QuicCaKey>) -> Self {
        Self {
            certificate: connection
                .peer_identity()
                .as_deref()
                .and_then(|certificates| certificates.first())
                .cloned(),
            quic_ca_key,
        }
    }

    pub fn matches(&self, node_id: uuid::Uuid) -> bool {
        let Some(quic_ca_key) = &self.quic_ca_key else {
            return false;
        };
        crate::tls::expected_node_certificate(node_id, quic_ca_key)
            .is_ok_and(|expected| self.certificate.as_ref() == Some(&expected))
    }
}

pub struct RpcSession {
    connection: Connection,
    peer_identity: PeerIdentity,
}

impl RpcSession {
    pub fn peer_address(&self) -> PeerAddress {
        PeerAddress {
            connection: self.connection.clone(),
        }
    }

    pub fn peer_identity(&self) -> PeerIdentity {
        self.peer_identity.clone()
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

    fn local_addresses() -> BTreeSet<LocalAddress> {
        BTreeSet::from([LocalAddress {
            host: IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            port: 0,
        }])
    }

    #[compio::test]
    async fn reconnects_after_cached_connection_closes() {
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
        let quic_ca_key = QuicCaKey::derive(&cipher);
        let addresses = local_addresses();
        let server = RpcTransport::bind(&addresses, uuid::Uuid::from_u128(1), &quic_ca_key)
            .await
            .unwrap();
        let server_port = server.endpoints[0].local_addr().unwrap().port();
        let client = RpcTransport::bind(&addresses, uuid::Uuid::from_u128(2), &quic_ca_key)
            .await
            .unwrap();

        let first_accept = spawn({
            let server = server.clone();
            async move { server.accept().await.unwrap().unwrap() }
        });
        let first = client
            .connect::<u8, u8>("127.0.0.1", server_port, uuid::Uuid::from_u128(1))
            .await
            .unwrap();
        let _first_session = first_accept.await.unwrap();
        drop(first);

        let key = Key {
            host: "127.0.0.1".to_owned(),
            port: server_port,
            node_id: uuid::Uuid::from_u128(1),
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
            .connect::<u8, u8>("127.0.0.1", server_port, uuid::Uuid::from_u128(1))
            .await
            .unwrap();
        let _second_session = second_accept.await.unwrap();
    }

    #[compio::test]
    async fn rejects_a_peer_with_a_different_node_identity() {
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
        let quic_ca_key = QuicCaKey::derive(&cipher);
        let addresses = local_addresses();
        let server_id = uuid::Uuid::from_u128(1);
        let server = RpcTransport::bind(&addresses, server_id, &quic_ca_key)
            .await
            .unwrap();
        let server_port = server.endpoints[0].local_addr().unwrap().port();
        let client_id = uuid::Uuid::from_u128(2);
        let client = RpcTransport::bind(&addresses, client_id, &quic_ca_key)
            .await
            .unwrap();
        let accept = spawn({
            let server = server.clone();
            async move { server.accept().await }
        });

        let expected_node_id = uuid::Uuid::from_u128(3);
        let Err(error) = client
            .connect::<u8, u8>("127.0.0.1", server_port, expected_node_id)
            .await
        else {
            panic!("connection accepted an unexpected node identity");
        };

        assert!(matches!(
            error,
            crate::Error::PeerIdentity {
                expected_node_id: actual
            } if actual == expected_node_id
        ));
        let _ = accept.await.unwrap();
    }

    #[compio::test]
    async fn tries_the_next_compatible_local_endpoint() {
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
        let quic_ca_key = QuicCaKey::derive(&cipher);
        let server_addresses = local_addresses();
        let server = RpcTransport::bind(&server_addresses, uuid::Uuid::from_u128(1), &quic_ca_key)
            .await
            .unwrap();
        let server_port = server.endpoints[0].local_addr().unwrap().port();
        let mut client_endpoints =
            crate::tls::secure_endpoints(&server_addresses, uuid::Uuid::from_u128(2), &quic_ca_key)
                .await
                .unwrap();
        client_endpoints.extend(
            crate::tls::secure_endpoints(&server_addresses, uuid::Uuid::from_u128(2), &quic_ca_key)
                .await
                .unwrap(),
        );
        let client = RpcTransport::new(client_endpoints, Some(quic_ca_key.clone()));
        client.endpoints[0].close(0_u32.into(), b"test fallback");
        let accept = spawn({
            let server = server.clone();
            async move { server.accept().await.unwrap().unwrap() }
        });

        client
            .connect::<u8, u8>("127.0.0.1", server_port, uuid::Uuid::from_u128(1))
            .await
            .unwrap();

        let _session = accept.await.unwrap();
    }

    #[compio::test]
    async fn tries_every_resolved_socket_address() {
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
        let quic_ca_key = QuicCaKey::derive(&cipher);
        let addresses = local_addresses();
        let server = RpcTransport::bind(&addresses, uuid::Uuid::from_u128(1), &quic_ca_key)
            .await
            .unwrap();
        let server_port = server.endpoints[0].local_addr().unwrap().port();
        let client = RpcTransport::bind(&addresses, uuid::Uuid::from_u128(2), &quic_ca_key)
            .await
            .unwrap();
        let accept = spawn({
            let server = server.clone();
            async move { server.accept().await.unwrap().unwrap() }
        });
        let key = Key {
            host: "multiple.example".to_owned(),
            port: server_port,
            node_id: uuid::Uuid::from_u128(1),
        };

        let _connection = client
            .establish_resolved(
                &key,
                [
                    std::net::SocketAddr::from(([127, 0, 0, 2], server_port)),
                    std::net::SocketAddr::from(([127, 0, 0, 1], server_port)),
                ],
            )
            .await
            .unwrap();

        let _session = accept.await.unwrap();
    }

    #[test]
    fn resolved_addresses_are_unicast_and_unique() {
        let unicast = std::net::SocketAddr::from(([127, 0, 0, 1], 11451));
        let multicast = std::net::SocketAddr::from(([224, 0, 0, 1], 11451));

        assert_eq!(
            validated_socket_addresses([unicast, unicast, multicast]),
            BTreeSet::from([unicast])
        );
    }
}
