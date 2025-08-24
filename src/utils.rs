use std::{sync::Arc, time::Duration};

use compio::{
    net::UdpSocket,
    runtime::{JoinHandle, spawn},
};
use compio_quic::{Endpoint, EndpointConfig, ServerConfig, crypto::rustls::QuicClientConfig};
use rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger,
    crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use snafu::ResultExt;
use uuid::{ContextV7, Timestamp, Uuid};

use crate::{EndpointCreationSnafu, Result, TLSSnafu};

#[thread_local]
static CONTEXT: ContextV7 = ContextV7::new();

pub fn uuid_v7_now() -> Uuid {
    Uuid::new_v7(Timestamp::now(&CONTEXT))
}

#[derive(Debug)]
pub struct SkipServerVerification(CryptoProvider);

impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(rustls::crypto::ring::default_provider()))
    }
}

impl danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<danger::ServerCertVerified, rustls::Error> {
        Ok(danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<danger::HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<danger::HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

pub async fn unsafe_endpoint(host: String, port: u16) -> Result<Endpoint> {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![host]).unwrap();
    let cert = cert.der().clone();
    let key_der = signing_key.serialize_der().try_into().unwrap();
    let config = EndpointConfig::default();
    let socket = UdpSocket::bind(("0.0.0.0", port))
        .await
        .context(EndpointCreationSnafu)?;
    let server_config = ServerConfig::with_single_cert(vec![cert], key_der).context(TLSSnafu)?;
    let rustls_client_config =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();
    let quic_client_config = QuicClientConfig::try_from(rustls_client_config)
        .expect("ring should provide TLS13_AES_128_GCM_SHA256 suite");
    let client_config = compio_quic::ClientConfig::new(Arc::new(quic_client_config));

    Endpoint::new(socket, config, Some(server_config), Some(client_config))
        .context(EndpointCreationSnafu)
}

pub fn delay<F: Future<Output = ()> + 'static>(duration: Duration, fut: F) -> JoinHandle<()> {
    spawn(async move {
        compio::time::sleep(duration).await;
        fut.await;
    })
}
