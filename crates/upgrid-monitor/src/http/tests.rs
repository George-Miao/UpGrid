use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use http::Method;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use url::Url;

use super::*;

struct Credentials {
    ca_pem: String,
    client_certificate_pem: String,
    client_private_key_pem: String,
    server_certificate: CertificateDer<'static>,
    server_private_key: PrivateKeyDer<'static>,
    ca_certificate: CertificateDer<'static>,
}

#[test]
fn cross_origin_redirect_strips_secret_backed_headers() {
    let mut headers = BTreeMap::from([
        ("x-api-key".to_owned(), "secret".to_owned()),
        ("x-public".to_owned(), "visible".to_owned()),
    ]);
    let sensitive = BTreeSet::from(["x-api-key".to_owned()]);

    strip_cross_origin_headers(&mut headers, &sensitive);

    assert!(!headers.contains_key("x-api-key"));
    assert_eq!(headers["x-public"], "visible");
}

#[test]
fn rejects_malformed_and_mismatched_tls_credentials() {
    let malformed = custom_tls_config(Some("not PEM"), None, None).unwrap_err();
    assert!(malformed.to_string().contains("custom CA bundle"));

    let credentials = credentials();
    let other_key = KeyPair::generate().unwrap().serialize_pem();
    let mismatched = custom_tls_config(
        Some(&credentials.ca_pem),
        Some(&credentials.client_certificate_pem),
        Some(&other_key),
    )
    .unwrap_err();
    assert!(
        mismatched
            .to_string()
            .contains("client certificate and private key")
    );
}

#[compio::test]
async fn custom_ca_and_client_identity_complete_https_request() {
    let credentials = credentials();
    let (url, server) = serve_https(&credentials, true);
    let custom_tls = custom_tls_config(
        Some(&credentials.ca_pem),
        Some(&credentials.client_certificate_pem),
        Some(&credentials.client_private_key_pem),
    )
    .unwrap();

    let response = send(&clients(), request(url, Some(custom_tls)))
        .await
        .unwrap();
    server.join().unwrap();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, b"ok");
}

#[compio::test]
async fn private_ca_is_rejected_without_custom_trust() {
    let credentials = credentials();
    let (url, server) = serve_https(&credentials, false);

    let Err(error) = send(&clients(), request(url, None)).await else {
        panic!("private CA unexpectedly trusted");
    };
    server.join().unwrap();

    assert!(matches!(error, Error::Send { .. }));
    assert!(error.to_string().contains("HTTP request failed"));
}

fn credentials() -> Credentials {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();

    let mut server_params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().unwrap();
    let server = server_params.signed_by(&server_key, &ca).unwrap();

    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = KeyPair::generate().unwrap();
    let client = client_params.signed_by(&client_key, &ca).unwrap();

    Credentials {
        ca_pem: ca.pem(),
        client_certificate_pem: client.pem(),
        client_private_key_pem: client_key.serialize_pem(),
        server_certificate: server.der().clone(),
        server_private_key: PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
        ca_certificate: ca.der().clone(),
    }
}

fn serve_https(credentials: &Credentials, require_client: bool) -> (Url, JoinHandle<()>) {
    let config = if require_client {
        let mut roots = RootCertStore::empty();
        roots.add(credentials.ca_certificate.clone()).unwrap();
        let verifier = WebPkiClientVerifier::builder(roots.into()).build().unwrap();
        ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![credentials.server_certificate.clone()],
                credentials.server_private_key.clone_key(),
            )
            .unwrap()
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![credentials.server_certificate.clone()],
                credentials.server_private_key.clone_key(),
            )
            .unwrap()
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let connection = ServerConnection::new(Arc::new(config)).unwrap();
        let mut stream = StreamOwned::new(connection, stream);
        let mut request = [0_u8; 1_024];
        if stream.read(&mut request).is_ok() {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                .unwrap();
        }
    });
    (
        Url::parse(&format!("https://localhost:{}/", address.port())).unwrap(),
        server,
    )
}

fn clients() -> Clients {
    let verified = cyper::Client::builder()
        .use_rustls_default()
        .build()
        .unwrap();
    Clients {
        insecure: verified.clone(),
        verified,
        network_runtime: Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        ),
    }
}

fn request(url: Url, custom_tls: Option<Arc<ClientConfig>>) -> Request {
    Request {
        method: Method::GET,
        url,
        headers: BTreeMap::new(),
        sensitive_headers: BTreeSet::new(),
        body: Vec::new(),
        follow_redirects: false,
        redirects_left: 0,
        skip_tls_verification: false,
        custom_tls,
    }
}
