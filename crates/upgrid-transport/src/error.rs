use ::rustls::server::VerifierBuilderError;
use compio_quic::crypto::rustls;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("incoming QUIC stream was lost: {source}"))]
    QuicIncoming {
        source: compio_quic::ConnectionError,
    },

    #[snafu(display("failed to resolve {host}: {source}"))]
    Resolve {
        host: String,
        source: std::io::Error,
    },

    #[snafu(display("no address found for {host}"))]
    ResolveEmpty { host: String },

    #[snafu(display("no local address family can connect to {host}:{port}"))]
    NoLocalAddressFamily { host: String, port: u16 },

    #[snafu(display("failed to connect to {host}:{port}: {source}"))]
    QuicConnect {
        host: String,
        port: u16,
        source: compio_quic::ConnectError,
    },

    #[snafu(display("failed to establish connection to {host}:{port}: {source}"))]
    QuicConnection {
        host: String,
        port: u16,
        source: compio_quic::ConnectionError,
    },

    #[snafu(display("authenticated peer is not Node {expected_node_id}"))]
    PeerIdentity { expected_node_id: uuid::Uuid },

    #[snafu(display("TLS configuration failed: {source}"))]
    Tls { source: rustls::Error },

    #[snafu(display("certificate generation failed: {source}"))]
    Certificate { source: rcgen::Error },

    #[snafu(display("client certificate verification failed: {source}"))]
    CertificateVerifier { source: VerifierBuilderError },

    #[snafu(display("QUIC TLS configuration has no usable initial cipher suite"))]
    QuicCipherSuite {
        source: compio_quic::crypto::rustls::NoInitialCipherSuite,
    },

    #[snafu(display("failed to create QUIC endpoint: {source}"))]
    EndpointCreation { source: std::io::Error },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Failure {
    Incoming {
        diagnostic: String,
    },
    Resolve {
        host: String,
        diagnostic: String,
    },
    ResolveEmpty {
        host: String,
    },
    NoLocalAddressFamily {
        host: String,
        port: u16,
    },
    Connect {
        host: String,
        port: u16,
        diagnostic: String,
    },
    Connection {
        host: String,
        port: u16,
        diagnostic: String,
    },
    PeerIdentity {
        expected_node_id: uuid::Uuid,
    },
    Tls {
        diagnostic: String,
    },
    Certificate {
        diagnostic: String,
    },
    CertificateVerifier {
        diagnostic: String,
    },
    QuicCipherSuite {
        diagnostic: String,
    },
    EndpointCreation {
        diagnostic: String,
    },
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
            Error::QuicIncoming { source } => Self::Incoming {
                diagnostic: source.to_string(),
            },
            Error::Resolve { host, source } => Self::Resolve {
                host,
                diagnostic: source.to_string(),
            },
            Error::ResolveEmpty { host } => Self::ResolveEmpty { host },
            Error::NoLocalAddressFamily { host, port } => Self::NoLocalAddressFamily { host, port },
            Error::QuicConnect { host, port, source } => Self::Connect {
                host,
                port,
                diagnostic: source.to_string(),
            },
            Error::QuicConnection { host, port, source } => Self::Connection {
                host,
                port,
                diagnostic: source.to_string(),
            },
            Error::PeerIdentity { expected_node_id } => Self::PeerIdentity { expected_node_id },
            Error::Tls { source } => Self::Tls {
                diagnostic: source.to_string(),
            },
            Error::Certificate { source } => Self::Certificate {
                diagnostic: source.to_string(),
            },
            Error::CertificateVerifier { source } => Self::CertificateVerifier {
                diagnostic: source.to_string(),
            },
            Error::QuicCipherSuite { source } => Self::QuicCipherSuite {
                diagnostic: source.to_string(),
            },
            Error::EndpointCreation { source } => Self::EndpointCreation {
                diagnostic: source.to_string(),
            },
        }
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incoming { diagnostic } => {
                write!(formatter, "incoming QUIC stream was lost: {diagnostic}")
            }
            Self::Resolve { host, diagnostic } => {
                write!(formatter, "failed to resolve {host}: {diagnostic}")
            }
            Self::ResolveEmpty { host } => write!(formatter, "no address found for {host}"),
            Self::NoLocalAddressFamily { host, port } => {
                write!(
                    formatter,
                    "no local address family can connect to {host}:{port}"
                )
            }
            Self::Connect {
                host,
                port,
                diagnostic,
            } => write!(
                formatter,
                "failed to connect to {host}:{port}: {diagnostic}"
            ),
            Self::Connection {
                host,
                port,
                diagnostic,
            } => write!(
                formatter,
                "failed to establish connection to {host}:{port}: {diagnostic}"
            ),
            Self::PeerIdentity { expected_node_id } => {
                write!(
                    formatter,
                    "authenticated peer is not Node {expected_node_id}"
                )
            }
            Self::Tls { diagnostic } => {
                write!(formatter, "TLS configuration failed: {diagnostic}")
            }
            Self::Certificate { diagnostic } => {
                write!(formatter, "certificate generation failed: {diagnostic}")
            }
            Self::CertificateVerifier { diagnostic } => {
                write!(
                    formatter,
                    "client certificate verification failed: {diagnostic}"
                )
            }
            Self::QuicCipherSuite { diagnostic } => write!(
                formatter,
                "QUIC TLS configuration has no usable initial cipher suite: {diagnostic}"
            ),
            Self::EndpointCreation { diagnostic } => {
                write!(formatter, "failed to create QUIC endpoint: {diagnostic}")
            }
        }
    }
}

impl std::error::Error for Failure {}

pub type Result<T, E = Error> = std::result::Result<T, E>;
