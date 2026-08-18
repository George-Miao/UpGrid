use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use compio::buf::{BufResult, IoBuf, IoBufMut};
use compio::io::{AsyncRead, AsyncWrite};
use compio::net::{TcpListener, TcpStream};
use compio_tls::{TlsAcceptor, TlsStream};
use futures_util::FutureExt;
use futures_util::future::LocalBoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
type SecureStream = TlsStream<TcpStream>;
type Accept = LocalBoxFuture<'static, io::Result<(TcpStream, SocketAddr)>>;
type Handshake = LocalBoxFuture<'static, (io::Result<SecureStream>, SocketAddr)>;

enum ListenerEvent {
    Connection,
    Handshake,
}

pub(crate) struct TlsRead(futures_util::io::ReadHalf<SecureStream>);

impl AsyncRead for TlsRead {
    async fn read<B: IoBufMut>(&mut self, mut buf: B) -> BufResult<usize, B> {
        let result = futures_util::AsyncReadExt::read(&mut self.0, buf.ensure_init()).await;
        let result = match result {
            Ok(length) => {
                unsafe { buf.advance_to(length) };
                Ok(length)
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(0),
            Err(error) => Err(error),
        };
        BufResult(result, buf)
    }
}

pub(crate) struct TlsWrite(futures_util::io::WriteHalf<SecureStream>);

impl AsyncWrite for TlsWrite {
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        let result = futures_util::AsyncWriteExt::write(&mut self.0, buf.as_init()).await;
        BufResult(result, buf)
    }

    async fn flush(&mut self) -> io::Result<()> {
        futures_util::AsyncWriteExt::flush(&mut self.0).await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        futures_util::AsyncWriteExt::close(&mut self.0).await
    }
}

pub(crate) struct TlsListener {
    inner: TcpListener,
    accepting: Accept,
    acceptor: TlsAcceptor,
    handshakes: FuturesUnordered<Handshake>,
}

impl TlsListener {
    pub(crate) fn new(inner: TcpListener, acceptor: TlsAcceptor) -> Self {
        Self {
            accepting: accepting(inner.clone()),
            inner,
            acceptor,
            handshakes: FuturesUnordered::new(),
        }
    }

    fn queue_handshake(&mut self, stream: TcpStream, address: SocketAddr) {
        let acceptor = self.acceptor.clone();
        self.handshakes.push(
            async move {
                let result =
                    compio::time::timeout(Duration::from_secs(10), acceptor.accept(stream)).await;
                let result = result.unwrap_or_else(|_| {
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "API TLS handshake timed out",
                    ))
                });
                (result, address)
            }
            .boxed_local(),
        );
    }
}

impl cyper_axum::Listener for TlsListener {
    type Addr = SocketAddr;
    type Io = (TlsRead, TlsWrite);

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let mut connection = None;
            let mut handshake = None;
            let event = futures_util::future::poll_fn(|context| {
                if let Poll::Ready(Some(result)) = self.handshakes.poll_next_unpin(context) {
                    handshake = Some(result);
                    return Poll::Ready(ListenerEvent::Handshake);
                }
                if let Poll::Ready(result) = self.accepting.as_mut().poll(context) {
                    connection = Some(result);
                    return Poll::Ready(ListenerEvent::Connection);
                }
                Poll::Pending
            })
            .await;
            match event {
                ListenerEvent::Connection => {
                    self.accepting = accepting(self.inner.clone());
                    match connection.expect("connection event has a result") {
                        Ok((stream, address)) => self.queue_handshake(stream, address),
                        Err(error) => handle_accept_error(error).await,
                    }
                }
                ListenerEvent::Handshake => {
                    match handshake.expect("handshake event has a result") {
                        (Ok(stream), address) => {
                            let (read, write) = futures_util::AsyncReadExt::split(stream);
                            return ((TlsRead(read), TlsWrite(write)), address);
                        }
                        (Err(error), address) => {
                            tracing::debug!(%error, %address, "API TLS handshake failed");
                        }
                    }
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}
fn accepting(listener: TcpListener) -> Accept {
    async move { listener.accept().await }.boxed_local()
}

async fn handle_accept_error(error: io::Error) {
    tracing::warn!(%error, "API listener could not accept a connection");
    compio::time::sleep(Duration::from_secs(1)).await;
}

pub(crate) fn tls_acceptor(cert_path: &Path, key_path: &Path) -> io::Result<TlsAcceptor> {
    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;
    let certificates = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid_data)?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TLS certificate file contains no certificates",
        ));
    }
    let private_key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(invalid_data)?;
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(invalid_data)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
