//! Mutual-TLS QUIC endpoints.

use std::collections::BTreeSet;
use std::net::IpAddr;
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
use socket2::{Domain, Protocol, Socket, Type};
use upgrid_config::{LocalAddress, QuicCaKey};

use crate::Result;
use crate::error::{
    CertificateSnafu, CertificateVerifierSnafu, EndpointCreationSnafu, QuicCipherSuiteSnafu,
    TlsSnafu,
};

pub(crate) const CLUSTER_SERVER_NAME: &str = "upgrid-node";

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

fn bind_is_subsumed(address: &LocalAddress, local_addresses: &BTreeSet<LocalAddress>) -> bool {
    let wildcard = match address.host {
        IpAddr::V4(host) if !host.is_unspecified() => IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(host) if !host.is_unspecified() => IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        _ => return false,
    };
    local_addresses.contains(&LocalAddress {
        host: wildcard,
        port: address.port,
    })
}

async fn bind_udp(address: IpAddr, port: u16, only_v6: bool) -> std::io::Result<UdpSocket> {
    if !only_v6 {
        return UdpSocket::bind((address, port)).await;
    }
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_only_v6(true)?;
    socket.bind(&std::net::SocketAddr::new(address, port).into())?;
    UdpSocket::from_std(socket.into())
}

struct NodeCredentials {
    ca: CertificateDer<'static>,
    cert: CertificateDer<'static>,
    key: Vec<u8>,
}

fn ed25519_key(seed: &[u8]) -> Result<KeyPair> {
    let mut key_der = vec![
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    key_der.extend_from_slice(seed);
    let key_der = PrivatePkcs8KeyDer::from(key_der);
    KeyPair::from_pkcs8_der_and_sign_algo(&key_der, &PKCS_ED25519).context(CertificateSnafu)
}

fn node_credentials(node_id: uuid::Uuid, quic_ca_key: &QuicCaKey) -> Result<NodeCredentials> {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).context(CertificateSnafu)?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = ed25519_key(quic_ca_key.as_bytes())?;
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).context(CertificateSnafu)?;

    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(b"UpGrid node certificate key");
    digest.update(quic_ca_key.as_bytes());
    digest.update(node_id.as_bytes());
    let node_key = ed25519_key(digest.finish().as_ref())?;
    let mut node_params = CertificateParams::new(vec![
        CLUSTER_SERVER_NAME.to_owned(),
        format!("node-{node_id}.upgrid"),
    ])
    .context(CertificateSnafu)?;
    node_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let node_cert = node_params
        .signed_by(&node_key, &ca)
        .context(CertificateSnafu)?;
    Ok(NodeCredentials {
        ca: ca.der().clone(),
        cert: node_cert.der().clone(),
        key: node_key.serialize_der(),
    })
}

pub(crate) fn expected_node_certificate(
    node_id: uuid::Uuid,
    quic_ca_key: &QuicCaKey,
) -> Result<CertificateDer<'static>> {
    Ok(node_credentials(node_id, quic_ca_key)?.cert)
}

pub async fn secure_endpoints(
    local_addresses: &BTreeSet<LocalAddress>,
    node_id: uuid::Uuid,
    quic_ca_key: &QuicCaKey,
) -> Result<Vec<Endpoint>> {
    let credentials = node_credentials(node_id, quic_ca_key)?;
    let mut roots = RootCertStore::empty();
    roots.add(credentials.ca).context(TlsSnafu)?;

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
        .build()
        .context(CertificateVerifierSnafu)?;
    let rustls_server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(
                vec![credentials.cert.clone()],
                credentials
                    .key
                    .clone()
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
                vec![credentials.cert],
                credentials.key.try_into().expect("rcgen emits PKCS#8"),
            )
            .context(TlsSnafu)?;
    let quic_client_config = QuicClientConfig::try_from(rustls_client_config)
        .expect("ring should provide TLS13_AES_128_GCM_SHA256 suite");
    let client_config = compio_quic::ClientConfig::new(Arc::new(quic_client_config));

    let mut endpoints = Vec::with_capacity(local_addresses.len());
    for address in local_addresses
        .iter()
        .filter(|address| !bind_is_subsumed(address, local_addresses))
    {
        let only_v6 = matches!(address.host, IpAddr::V6(host) if host.is_unspecified());
        let socket = bind_udp(address.host, address.port, only_v6)
            .await
            .context(EndpointCreationSnafu)?;
        endpoints.push(
            Endpoint::new(
                socket,
                EndpointConfig::default(),
                Some(server_config.clone()),
                Some(client_config.clone()),
            )
            .context(EndpointCreationSnafu)?,
        );
    }
    Ok(endpoints)
}

#[cfg(test)]
mod tests {
    use compio::runtime::spawn;

    use super::*;
    use crate::RpcTransport;

    fn local(host: IpAddr, port: u16) -> LocalAddress {
        LocalAddress { host, port }
    }

    fn assert_bound_addresses(endpoints: &[Endpoint], local_addresses: &BTreeSet<LocalAddress>) {
        let bound_addresses = endpoints
            .iter()
            .map(|endpoint| endpoint.local_addr().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            bound_addresses
                .iter()
                .map(std::net::SocketAddr::ip)
                .collect::<BTreeSet<_>>(),
            local_addresses
                .iter()
                .map(|address| address.host)
                .collect::<BTreeSet<_>>(),
        );
        assert!(bound_addresses.iter().all(|address| address.port() != 0));
    }

    #[test]
    fn wildcard_bind_subsumes_only_same_family_and_port() {
        let addresses = BTreeSet::from([
            local(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 11451),
            local(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 11451),
            local(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 11452),
            local(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST), 11451),
        ]);
        let effective = addresses
            .iter()
            .filter(|address| !bind_is_subsumed(address, &addresses))
            .copied()
            .collect::<BTreeSet<_>>();

        assert_eq!(
            effective,
            BTreeSet::from([
                local(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 11451),
                local(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 11452),
                local(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST), 11451),
            ])
        );
    }

    #[test]
    fn node_certificates_are_stable_and_identity_specific() {
        let key = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
        let first = expected_node_certificate(uuid::Uuid::from_u128(1), &key).unwrap();
        let repeated = expected_node_certificate(uuid::Uuid::from_u128(1), &key).unwrap();
        let second = expected_node_certificate(uuid::Uuid::from_u128(2), &key).unwrap();

        assert_eq!(first, repeated);
        assert_ne!(first, second);
    }

    #[compio::test]
    async fn dual_stack_wildcards_bind_each_local_address() {
        let key = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
        let addresses = BTreeSet::from([
            local(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0),
            local(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0),
        ]);

        let endpoints = secure_endpoints(&addresses, uuid::Uuid::nil(), &key)
            .await
            .unwrap();

        assert_bound_addresses(&endpoints, &addresses);
    }

    #[compio::test]
    async fn ipv6_wildcard_and_ipv4_address_bind_each_local_address() {
        let key = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
        let addresses = BTreeSet::from([
            local(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            local(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0),
        ]);

        let endpoints = secure_endpoints(&addresses, uuid::Uuid::nil(), &key)
            .await
            .unwrap();

        assert_bound_addresses(&endpoints, &addresses);
    }

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
        let quic_ca_key = QuicCaKey::parse("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap();
        let server_endpoint = untrusted_server_endpoint("127.0.0.1").await;
        let server_port = server_endpoint.local_addr().unwrap().port();
        let server = RpcTransport::new(vec![server_endpoint], None);
        let addresses = BTreeSet::from([local(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0)]);
        let client = RpcTransport::bind(&addresses, uuid::Uuid::from_u128(2), &quic_ca_key)
            .await
            .unwrap();
        let _accept = spawn(async move { server.accept().await });

        let error = match client
            .connect::<u8, u8>("127.0.0.1", server_port, uuid::Uuid::from_u128(1))
            .await
        {
            Ok(_) => panic!("untrusted server certificate was accepted"),
            Err(error) => error,
        };
        let message = format!("{error:#?}");
        assert!(message.contains("UnknownIssuer"), "{message}");
    }
}
