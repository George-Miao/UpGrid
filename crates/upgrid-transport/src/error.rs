use ::rustls::server::VerifierBuilderError;
use compio_quic::crypto::rustls;
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

pub type Result<T, E = Error> = std::result::Result<T, E>;
