//! Telegram and webhook alert delivery for an UpGrid Cluster.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use compio::runtime::spawn;
use compio::time::{sleep, timeout};
use cyper::Client;
use http::{Method, StatusCode, header};
use upgrid_config::{Cipher, now_ms};
use upgrid_raft::Handle;
use upgrid_raft::domain::{
    Alert, AlertDelivery, AlertId, Command, MAX_DIAGNOSTIC_BYTES, MAX_RESPONSE_BYTES,
};

mod channel;

#[cfg(test)]
#[path = "tests.rs"]
mod notification_tests;

use channel::Request;

const RETRY_WINDOW_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Clone)]
struct Notifier {
    client: Client,
    cipher: Cipher,
}

struct Response {
    status: StatusCode,
    body: Vec<u8>,
    retry_after_ms: Option<u64>,
}

struct Attempt {
    response: Response,
    accepted: bool,
}

enum DeliveryError {
    Transient(String),
    Permanent(String),
}

/// Starts leader-only alert delivery in the current Compio runtime.
pub fn start(cluster: Handle, cipher: Cipher) {
    let notifier = Notifier {
        client: Client::builder()
            .use_rustls_default()
            .build()
            .expect("default HTTP client configuration should be valid"),
        cipher,
    };
    spawn(run(cluster, notifier)).detach();
}

async fn run(cluster: Handle, notifier: Notifier) {
    let active = Rc::new(RefCell::new(BTreeSet::<AlertId>::new()));
    loop {
        if !cluster.is_leader().await {
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        let now = now_ms();
        let state = match cluster.read().await {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(%error, "notification worker could not establish a read barrier");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let due = state
            .alerts
            .values()
            .filter(|alert| {
                matches!(alert.delivery, AlertDelivery::Pending { next_attempt_at_ms, .. } if next_attempt_at_ms <= now)
                    && !active.borrow().contains(&alert.id)
            })
            .cloned()
            .collect::<Vec<_>>();
        for alert in due {
            active.borrow_mut().insert(alert.id);
            let active = active.clone();
            let cluster = cluster.clone();
            let notifier = notifier.clone();
            spawn(async move {
                deliver(&cluster, &notifier, alert.clone()).await;
                active.borrow_mut().remove(&alert.id);
            })
            .detach();
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn deliver(cluster: &Handle, notifier: &Notifier, alert: Alert) {
    let attempted_at_ms = now_ms();
    let result = attempt(cluster, notifier, &alert).await;
    let command = match result {
        Ok(Attempt { accepted: true, .. }) => Command::MarkAlertDelivered {
            alert_id: alert.id,
            delivered_at_ms: now_ms(),
        },
        Ok(Attempt { response, .. }) => Command::RecordAlertFailure {
            alert_id: alert.id,
            attempted_at_ms,
            retry_at_ms: retry_at_for_response(&alert, attempted_at_ms, &response),
            diagnostic: truncate(format!(
                "notification endpoint returned HTTP {}",
                response.status.as_u16()
            )),
        },
        Err(DeliveryError::Transient(message)) => Command::RecordAlertFailure {
            alert_id: alert.id,
            attempted_at_ms,
            retry_at_ms: retry_at_with_delay(&alert, attempted_at_ms, retry_delay_ms(&alert)),
            diagnostic: truncate(message),
        },
        Err(DeliveryError::Permanent(message)) => Command::RecordAlertFailure {
            alert_id: alert.id,
            attempted_at_ms,
            retry_at_ms: None,
            diagnostic: truncate(message),
        },
    };
    if let Err(error) = cluster.apply(command).await {
        tracing::error!(target_id = %alert.id.target_id.0, %error, "could not update alert delivery");
    }
}

async fn attempt(
    cluster: &Handle,
    notifier: &Notifier,
    alert: &Alert,
) -> Result<Attempt, DeliveryError> {
    let state = cluster.read().await.map_err(DeliveryError::Permanent)?;
    let configured = state
        .notification_channels
        .get(&alert.id.channel_id)
        .ok_or_else(|| {
            DeliveryError::Permanent(format!(
                "channel {} no longer exists",
                alert.id.channel_id.0
            ))
        })?;
    let target = channel::target(&configured.kind);
    let request = target
        .request(&state, &notifier.cipher, alert)
        .map_err(DeliveryError::Permanent)?;
    let response = timeout(Duration::from_secs(15), send(&notifier.client, request))
        .await
        .map_err(|_| DeliveryError::Transient("notification timed out".to_owned()))?
        .map_err(DeliveryError::Transient)?;
    let accepted = target.accepts(response.status, &response.body);
    Ok(Attempt { response, accepted })
}

async fn send(client: &Client, request: Request) -> Result<Response, String> {
    let mut builder = client
        .request(Method::POST, request.url)
        .map_err(|error| error.to_string())?;
    for (name, value) in request.headers {
        builder = builder
            .header(name.as_str(), value.as_str())
            .map_err(|error| error.to_string())?;
    }
    let response = builder
        .body(request.body)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    let retry_after_ms = response
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| parse_retry_after(value, SystemTime::now()));
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES)
    {
        return Err(format!("response body exceeds {MAX_RESPONSE_BYTES} bytes"));
    }
    let body = response.bytes().await.map_err(|error| error.to_string())?;
    if body.len() > MAX_RESPONSE_BYTES as usize {
        return Err(format!("response body exceeds {MAX_RESPONSE_BYTES} bytes"));
    }
    Ok(Response {
        status,
        body: body.to_vec(),
        retry_after_ms,
    })
}

fn parse_retry_after(value: &str, now: SystemTime) -> Option<u64> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds.saturating_mul(1_000));
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    Some(
        retry_at
            .duration_since(now)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    )
}

fn retry_delay_ms(alert: &Alert) -> u64 {
    let attempts = match alert.delivery {
        AlertDelivery::Pending { attempts, .. } => attempts,
        _ => 0,
    };
    5_000_u64
        .saturating_mul(1_u64 << attempts.min(6))
        .min(300_000)
}

fn retry_at_for_response(alert: &Alert, attempted_at_ms: u64, response: &Response) -> Option<u64> {
    let retryable = matches!(
        response.status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
    );
    if !retryable && response.retry_after_ms.is_none() {
        return None;
    }
    retry_at_with_delay(
        alert,
        attempted_at_ms,
        response
            .retry_after_ms
            .unwrap_or_else(|| retry_delay_ms(alert)),
    )
}

fn retry_at_with_delay(alert: &Alert, attempted_at_ms: u64, delay_ms: u64) -> Option<u64> {
    let deadline = alert
        .evaluation
        .recorded_at_ms
        .saturating_add(RETRY_WINDOW_MS);
    let retry_at_ms = attempted_at_ms.saturating_add(delay_ms);
    (retry_at_ms <= deadline).then_some(retry_at_ms)
}

fn truncate(value: String) -> String {
    if value.len() <= MAX_DIAGNOSTIC_BYTES {
        return value;
    }
    let mut end = MAX_DIAGNOSTIC_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}
