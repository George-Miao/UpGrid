//! Mutual-TLS QUIC endpoints.

use std::sync::Arc;

use compio::net::UdpSocket;
use compio_quic::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use compio_quic::{Endpoint, EndpointConfig, ServerConfig};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    PKCS_ED25519,
};
use rustls::client::danger;
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use snafu::ResultExt;
use upgrid_config::Cipher;

use crate::Result;
use crate::error::{
    CertificateSnafu, CertificateVerifierSnafu, EndpointCreationSnafu, QuicCipherSuiteSnafu,
    TlsSnafu,
};

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

pub async fn secure_endpoint(host: String, port: u16, cipher: &Cipher) -> Result<Endpoint> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).context(CertificateSnafu)?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let mut ca_key_der = vec![
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    ca_key_der.extend_from_slice(&cipher.derive(b"upgrid deployment ca"));
    let ca_key_der = PrivatePkcs8KeyDer::from(ca_key_der);
    let ca_key = KeyPair::from_pkcs8_der_and_sign_algo(&ca_key_der, &PKCS_ED25519)
        .context(CertificateSnafu)?;
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).context(CertificateSnafu)?;

    let mut node_params = CertificateParams::new(vec![host.clone()]).context(CertificateSnafu)?;
    node_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let node_key = KeyPair::generate().context(CertificateSnafu)?;
    let node_cert = node_params
        .signed_by(&node_key, &ca)
        .context(CertificateSnafu)?;
    let mut roots = RootCertStore::empty();
    roots.add(ca.der().clone()).context(TlsSnafu)?;

    let config = EndpointConfig::default();
    let socket = UdpSocket::bind(("0.0.0.0", port))
        .await
        .context(EndpointCreationSnafu)?;
    let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
        .build()
        .context(CertificateVerifierSnafu)?;
    let rustls_server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(
                vec![node_cert.der().clone()],
                node_key
                    .serialize_der()
                    .try_into()
                    .expect("rcgen emits PKCS#8"),
            )
            .context(TlsSnafu)?;
    let server_config = ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(rustls_server_config).context(QuicCipherSuiteSnafu)?,
    ));
    let rustls_client_config =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![node_cert.der().clone()],
                node_key
                    .serialize_der()
                    .try_into()
                    .expect("rcgen emits PKCS#8"),
            )
            .context(TlsSnafu)?;
    let quic_client_config = QuicClientConfig::try_from(rustls_client_config)
        .expect("ring should provide TLS13_AES_128_GCM_SHA256 suite");
    let client_config = compio_quic::ClientConfig::new(Arc::new(quic_client_config));

    Endpoint::new(socket, config, Some(server_config), Some(client_config))
        .context(EndpointCreationSnafu)
}

#[cfg(test)]
mod tests {
    use compio::runtime::spawn;

    use super::*;
    use crate::RpcTransport;

    async fn untrusted_server_endpoint(host: &str) -> Endpoint {
        let mut params = CertificateParams::new(vec![host.to_owned()]).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "untrusted");
        let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let server_config = ServerConfig::with_single_cert(
            vec![cert.der().clone()],
            key.serialize_der().try_into().unwrap(),
        )
        .unwrap();
        let socket = UdpSocket::bind(("0.0.0.0", 0)).await.unwrap();

        Endpoint::new(socket, EndpointConfig::default(), Some(server_config), None).unwrap()
    }

    #[compio::test]
    async fn rejects_untrusted_server_certificate() {
        let cipher = Cipher::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
        let server_endpoint = untrusted_server_endpoint("127.0.0.1").await;
        let server_port = server_endpoint.local_addr().unwrap().port();
        let server = RpcTransport::new(server_endpoint);
        let client = RpcTransport::bind("127.0.0.1", 0, &cipher).await.unwrap();
        let _accept = spawn(async move { server.accept().await });

        let error = match client.connect::<u8, u8>("127.0.0.1", server_port).await {
            Ok(_) => panic!("untrusted server certificate was accepted"),
            Err(error) => error,
        };
        let message = format!("{error:#?}");
        assert!(message.contains("UnknownIssuer"), "{message}");
    }
}
