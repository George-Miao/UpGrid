use std::collections::BTreeSet;
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use upgrid_config::now_ms;
use upgrid_raft::domain::Command;
use upgrid_raft::{
    DiscoverySource, Handle, ReachableAddress, ReachableAddressCandidate, ReachableAddressLease,
};
use uuid::Uuid;

const MAX_DISCOVERY_BYTES: u64 = 64 * 1_024;
const MAX_CONCURRENT_REQUESTS: usize = 4;

const MAX_DISCOVERY_ADDRESSES: usize = 32;

#[derive(Debug, Snafu)]
enum DiscoveryError {
    #[snafu(display("could not create a request for {url}: {source}"))]
    Request { url: String, source: cyper::Error },
    #[snafu(display("request to discovery service {url} timed out"))]
    Timeout { url: String },
    #[snafu(display("request to discovery service {url} failed: {source}"))]
    Send { url: String, source: cyper::Error },
    #[snafu(display("discovery service {url} returned {status}"))]
    Status {
        url: String,
        status: http::StatusCode,
    },
    #[snafu(display("discovery service {url} response is larger than {limit} bytes"))]
    ResponseTooLarge { url: String, limit: u64 },
    #[snafu(display("discovery service {url} returned {count} addresses; the limit is {limit}"))]
    TooManyAddresses {
        url: String,
        count: usize,
        limit: usize,
    },
    #[snafu(display("could not read discovery service {url}: {source}"))]
    Body { url: String, source: cyper::Error },
    #[snafu(display("could not decode discovery service {url}: {source}"))]
    Decode {
        url: String,
        source: serde_json::Error,
    },
    #[snafu(display("discovery service {url} returned invalid address {address}: {source}"))]
    Address {
        url: String,
        address: String,
        #[snafu(source(from(upgrid_config::ReachableAddressError, Box::new)))]
        source: Box<upgrid_config::ReachableAddressError>,
    },
}

#[derive(Deserialize)]
struct DiscoveryDocument {
    addresses: Vec<String>,
}

pub(crate) fn start(cluster: Handle, node_id: Uuid, services: BTreeSet<url::Url>) {
    if services.is_empty() {
        return;
    }
    compio::runtime::spawn(async move {
        loop {
            compio::time::sleep(Duration::from_secs(20)).await;
            let Ok(status) = cluster.status().await else {
                continue;
            };
            if !status.member_ids.is_empty() && !status.member_ids.contains(&node_id) {
                break;
            }
            let discovered = addresses(&services).await;
            let discovered_at_ms = now_ms();
            let expires_at_ms = discovered_at_ms.saturating_add(upgrid_raft::REACHABILITY_LEASE_MS);
            let leases = discovered
                .into_iter()
                .map(|candidate| ReachableAddressLease {
                    node_id,
                    address: candidate.address,
                    source: candidate.source,
                    discovered_at_ms,
                    expires_at_ms,
                })
                .collect::<Vec<_>>();
            if leases.is_empty() {
                continue;
            }
            let Ok(status) = cluster.status().await else {
                continue;
            };
            if !status.member_ids.is_empty() && !status.member_ids.contains(&node_id) {
                break;
            }
            if let Err(error) = cluster
                .apply(Command::RenewReachabilityLeases(leases))
                .await
            {
                tracing::warn!(%error, "could not renew discovery service addresses");
            }
        }
    })
    .detach();
}

pub(super) async fn addresses(services: &BTreeSet<url::Url>) -> Vec<ReachableAddressCandidate> {
    let client = match cyper::Client::builder().use_rustls_default().build() {
        Ok(client) => client,
        Err(source) => {
            tracing::warn!(%source, "could not create discovery HTTP client");
            return Vec::new();
        }
    };
    addresses_with_client(&client, services).await
}

async fn addresses_with_client(
    client: &cyper::Client,
    services: &BTreeSet<url::Url>,
) -> Vec<ReachableAddressCandidate> {
    let mut discovered = BTreeSet::new();
    let requests = futures_util::stream::iter(services)
        .map(|url| async move { (url, fetch(client, url).await) })
        .buffer_unordered(MAX_CONCURRENT_REQUESTS);
    futures_util::pin_mut!(requests);
    while let Some((url, result)) = requests.next().await {
        match result {
            Ok(addresses) => {
                let source = DiscoverySource::Service {
                    url: url.to_string(),
                };
                discovered.extend(
                    addresses
                        .into_iter()
                        .map(|address| ReachableAddressCandidate {
                            address,
                            source: source.clone(),
                        }),
                );
            }
            Err(error) => tracing::warn!(%error, "could not refresh discovery service"),
        }
    }
    discovered.into_iter().collect()
}
fn decode_addresses(
    url: &str,
    body: &[u8],
) -> std::result::Result<Vec<ReachableAddress>, DiscoveryError> {
    let document: DiscoveryDocument = serde_json::from_slice(body).context(DecodeSnafu {
        url: url.to_owned(),
    })?;
    if document.addresses.len() > MAX_DISCOVERY_ADDRESSES {
        return Err(DiscoveryError::TooManyAddresses {
            url: url.to_owned(),
            count: document.addresses.len(),
            limit: MAX_DISCOVERY_ADDRESSES,
        });
    }
    document
        .addresses
        .into_iter()
        .map(|address| {
            ReachableAddress::parse(&address).context(AddressSnafu {
                url: url.to_owned(),
                address,
            })
        })
        .collect()
}

async fn fetch(
    client: &cyper::Client,
    url: &url::Url,
) -> std::result::Result<Vec<ReachableAddress>, DiscoveryError> {
    let url_text = url.to_string();
    let request = client
        .request(http::Method::GET, url.clone())
        .context(RequestSnafu {
            url: url_text.clone(),
        })?;
    let timeout_url = url_text.clone();
    compio::time::timeout(Duration::from_secs(3), async move {
        let response = request.send().await.context(SendSnafu {
            url: url_text.clone(),
        })?;
        if !response.status().is_success() {
            return Err(DiscoveryError::Status {
                url: url_text,
                status: response.status(),
            });
        }
        let content_length = response.content_length();
        if content_length.is_some_and(|length| length > MAX_DISCOVERY_BYTES) {
            return Err(DiscoveryError::ResponseTooLarge {
                url: url_text,
                limit: MAX_DISCOVERY_BYTES,
            });
        }
        let mut body = Vec::with_capacity(content_length.unwrap_or_default() as usize);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context(BodySnafu {
                url: url_text.clone(),
            })?;
            if body.len().saturating_add(chunk.len()) > MAX_DISCOVERY_BYTES as usize {
                return Err(DiscoveryError::ResponseTooLarge {
                    url: url_text,
                    limit: MAX_DISCOVERY_BYTES,
                });
            }
            body.extend_from_slice(&chunk);
        }
        decode_addresses(&url_text, &body)
    })
    .await
    .map_err(|_| DiscoveryError::Timeout { url: timeout_url })?
}
#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use super::*;
    fn service(index: usize) -> (url::Url, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut chunk = [0_u8; 256];
                let read = stream.read(&mut chunk).unwrap();
                assert_ne!(read, 0, "request ended before the HTTP headers");
                request.extend_from_slice(&chunk[..read]);
            }
            thread::sleep(Duration::from_millis(750));
            let body = format!(r#"{{"addresses":["up://node-{index}.example:11451"]}}"#);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: \
                 {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        (
            format!("http://127.0.0.1:{}/", address.port())
                .parse()
                .unwrap(),
            handle,
        )
    }

    #[compio::test]
    async fn fetches_discovery_services_concurrently() {
        let mut services = BTreeSet::new();
        let mut handles = Vec::new();
        for index in 0..4 {
            let (url, handle) = service(index);
            services.insert(url);
            handles.push(handle);
        }

        let client = cyper::Client::builder()
            .use_rustls_default()
            .no_proxy()
            .build()
            .unwrap();
        let started = Instant::now();
        let discovered = addresses_with_client(&client, &services).await;
        let elapsed = started.elapsed();
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(discovered.len(), 4);
        assert!(elapsed < Duration::from_secs(2), "elapsed: {elapsed:?}");
    }

    #[test]
    fn rejects_too_many_discovery_addresses() {
        let addresses = (0..=MAX_DISCOVERY_ADDRESSES)
            .map(|index| format!("up://node-{index}.example:11451"))
            .collect::<Vec<_>>();
        let body = serde_json::to_vec(&serde_json::json!({ "addresses": addresses })).unwrap();

        let error = decode_addresses("https://discovery.example/nodes", &body).unwrap_err();

        assert!(matches!(
            error,
            DiscoveryError::TooManyAddresses {
                count,
                limit: MAX_DISCOVERY_ADDRESSES,
                ..
            } if count == MAX_DISCOVERY_ADDRESSES + 1
        ));
    }
}
