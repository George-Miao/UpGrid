use std::io;
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use compio::net::{TcpStream, ToSocketAddrsAsync};
use compio::runtime::spawn_blocking;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore};
use snafu::Snafu;
use upgrid_raft::domain::{HttpTarget, TargetKind};

#[derive(Debug, Snafu)]
pub(super) enum Error {
    #[snafu(display("invalid {} target endpoint: {detail}", kind.as_str()))]
    InvalidEndpoint { kind: TargetKind, detail: String },

    #[snafu(display("DNS resolution failed for {host}: {source}"))]
    Resolve { host: String, source: io::Error },

    #[snafu(display("TCP connection to {endpoint} failed: {source}"))]
    Connect { endpoint: String, source: io::Error },

    #[snafu(display("ICMP echo to {host} failed: {source}"))]
    Icmp { host: String, source: IcmpError },

    #[snafu(display("TLS certificate check for {endpoint} failed: {source}"))]
    Tls { endpoint: String, source: TlsError },

    #[snafu(display("request timed out"))]
    RequestTimeout,
}

#[derive(Debug, Snafu)]
pub(super) enum IcmpError {
    #[snafu(display("{source}"))]
    Ping { source: ping::Error },
}

#[derive(Debug, Snafu)]
pub(super) enum TlsError {
    #[snafu(display("resolver returned no addresses"))]
    MissingAddresses,

    #[snafu(display("request timed out"))]
    DeadlineElapsed,

    #[snafu(display("timeout exceeds the supported clock range"))]
    DeadlineOverflow,

    #[snafu(display("connection failed: {source}"))]
    SocketConnect { source: io::Error },

    #[snafu(display("invalid server name: {source}"))]
    InvalidServerName {
        source: rustls::pki_types::InvalidDnsNameError,
    },

    #[snafu(display("TLS setup failed: {source}"))]
    ClientSetup { source: rustls::Error },

    #[snafu(display("TLS handshake failed: {source}"))]
    Handshake { source: io::Error },
}
pub(super) async fn probe(
    target: &HttpTarget,
    kind: TargetKind,
    timeout: Duration,
) -> Result<(), Error> {
    match kind {
        TargetKind::Http => unreachable!("HTTP probes use the HTTP client"),
        TargetKind::Tcp => tcp(target).await,
        TargetKind::Dns => dns(target).await,
        TargetKind::Icmp => icmp(target, timeout).await,
        TargetKind::Tls => tls(target, timeout).await,
    }
}

async fn tcp(target: &HttpTarget) -> Result<(), Error> {
    let (host, port) = host_and_port(target, TargetKind::Tcp)?;
    TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|source| Error::Connect {
            endpoint: endpoint(&host, port),
            source,
        })?;
    Ok(())
}

async fn dns(target: &HttpTarget) -> Result<(), Error> {
    let host = host(target, TargetKind::Dns)?;
    let mut addresses = (host.as_str(), 0)
        .to_socket_addrs_async()
        .await
        .map_err(|source| Error::Resolve {
            host: host.clone(),
            source,
        })?;
    addresses.next().ok_or_else(|| Error::Resolve {
        host,
        source: io::Error::new(io::ErrorKind::NotFound, "resolver returned no addresses"),
    })?;
    Ok(())
}

async fn icmp(target: &HttpTarget, timeout: Duration) -> Result<(), Error> {
    let host = host(target, TargetKind::Icmp)?;
    let address = resolve_one(&host).await?;
    let result = spawn_blocking(move || {
        ping::new(address.ip())
            .timeout(timeout)
            .send()
            .map(|_| ())
            .map_err(|source| IcmpError::Ping { source })
    })
    .await
    .expect("ICMP probe task should run to completion");
    result.map_err(|source| Error::Icmp { host, source })
}

async fn resolve_one(host: &str) -> Result<SocketAddr, Error> {
    (host, 0)
        .to_socket_addrs_async()
        .await
        .map_err(|source| Error::Resolve {
            host: host.to_owned(),
            source,
        })?
        .next()
        .ok_or_else(|| Error::Resolve {
            host: host.to_owned(),
            source: io::Error::new(io::ErrorKind::NotFound, "resolver returned no addresses"),
        })
}

async fn tls(target: &HttpTarget, timeout: Duration) -> Result<(), Error> {
    let (host, port) = host_and_port(target, TargetKind::Tls)?;
    let endpoint = endpoint(&host, port);
    let addresses = (host.as_str(), port)
        .to_socket_addrs_async()
        .await
        .map_err(|source| Error::Resolve {
            host: host.clone(),
            source,
        })?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(Error::Resolve {
            host,
            source: io::Error::new(io::ErrorKind::NotFound, "resolver returned no addresses"),
        });
    }
    let result = spawn_blocking(move || tls_blocking(&host, &addresses, timeout))
        .await
        .expect("TLS probe task should run to completion");
    result.map_err(|source| Error::Tls { endpoint, source })
}

fn tls_blocking(host: &str, addresses: &[SocketAddr], timeout: Duration) -> Result<(), TlsError> {
    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    tls_with_roots(host, addresses, timeout, roots)
}

fn tls_with_roots(
    host: &str,
    addresses: &[SocketAddr],
    timeout: Duration,
    roots: RootCertStore,
) -> Result<(), TlsError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(TlsError::DeadlineOverflow)?;
    let mut last_error = None;
    let mut socket = None;
    for address in addresses {
        let remaining = remaining(deadline)?;
        match StdTcpStream::connect_timeout(address, remaining) {
            Ok(connected) => {
                socket = Some(connected);
                break;
            }
            Err(error) => last_error = Some(error),
        }
    }
    let mut socket = match socket {
        Some(socket) => socket,
        None => {
            return Err(last_error.map_or(TlsError::MissingAddresses, |source| {
                TlsError::SocketConnect { source }
            }));
        }
    };

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = ServerName::try_from(host.to_owned())
        .map_err(|source| TlsError::InvalidServerName { source })?;
    let mut connection = ClientConnection::new(Arc::new(config), server_name)
        .map_err(|source| TlsError::ClientSetup { source })?;
    while connection.is_handshaking() {
        let remaining = remaining(deadline)?;
        socket
            .set_read_timeout(Some(remaining))
            .and_then(|()| socket.set_write_timeout(Some(remaining)))
            .map_err(|source| TlsError::Handshake { source })?;
        connection
            .complete_io(&mut socket)
            .map_err(|source| TlsError::Handshake { source })?;
    }
    Ok(())
}

fn remaining(deadline: Instant) -> Result<Duration, TlsError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(TlsError::DeadlineElapsed)
}

fn host(target: &HttpTarget, kind: TargetKind) -> Result<String, Error> {
    target
        .url
        .host()
        .map(|host| match host {
            url::Host::Domain(host) => host.to_owned(),
            url::Host::Ipv4(host) => host.to_string(),
            url::Host::Ipv6(host) => host.to_string(),
        })
        .ok_or_else(|| Error::InvalidEndpoint {
            kind,
            detail: "host is missing".to_owned(),
        })
}

fn host_and_port(target: &HttpTarget, kind: TargetKind) -> Result<(String, u16), Error> {
    let host = host(target, kind)?;
    let port = target.url.port().ok_or_else(|| Error::InvalidEndpoint {
        kind,
        detail: "port is missing".to_owned(),
    })?;
    Ok((host, port))
}

fn endpoint(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::TcpListener as StdTcpListener;
    use std::thread;

    use compio::net::TcpListener;
    use url::Url;

    use super::*;

    #[test]
    fn normalizes_ipv6_endpoint_host() {
        let target = HttpTarget::get(Url::parse("tcp://[::1]:443").unwrap());

        let (host, port) = host_and_port(&target, TargetKind::Tcp).unwrap();

        assert_eq!(host, "::1");
        assert_eq!(endpoint(&host, port), "[::1]:443");
    }

    #[compio::test]
    async fn tcp_probe_connects_to_local_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("tcp://{}", listener.local_addr().unwrap());
        let target = HttpTarget::get(Url::parse(&endpoint).unwrap());

        tcp(&target).await.unwrap();
    }

    #[compio::test]
    async fn dns_probe_resolves_localhost() {
        let target = HttpTarget::get(Url::parse("dns://localhost").unwrap());

        dns(&target).await.unwrap();
    }

    #[compio::test]
    async fn icmp_probe_reports_echo_failure() {
        let target = HttpTarget::get(Url::parse("icmp://192.0.2.1").unwrap());

        let error = icmp(&target, Duration::from_millis(50)).await.unwrap_err();

        assert!(matches!(error, Error::Icmp { .. }));
        assert!(error.to_string().contains("192.0.2.1"));
    }

    #[test]
    fn tls_probe_accepts_trusted_local_certificate() {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = cert.der().clone();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], key)
            .unwrap();
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut connection = rustls::ServerConnection::new(Arc::new(config)).unwrap();
            while connection.is_handshaking() {
                connection.complete_io(&mut stream).unwrap();
            }
        });
        let mut roots = RootCertStore::empty();
        roots.add(certificate).unwrap();

        tls_with_roots("localhost", &[address], Duration::from_secs(2), roots).unwrap();
        server.join().unwrap();
    }

    #[compio::test]
    async fn tls_probe_rejects_plaintext_server() {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("tls://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.write_all(b"not tls").unwrap();
        });
        let target = HttpTarget::get(Url::parse(&endpoint).unwrap());

        let error = tls(&target, Duration::from_secs(2)).await.unwrap_err();
        server.join().unwrap();

        assert!(matches!(error, Error::Tls { .. }));
        assert!(error.to_string().contains("TLS certificate check"));
    }
}
