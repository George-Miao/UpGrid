use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use axum::http::{Method, StatusCode, header};
use compio::runtime::spawn;
use compio::time::{sleep, timeout};
use cyper::Client;
use serde_json::json;
use url::Url;

use crate::app::now_ms;
use crate::cluster::Handle;
use crate::domain::{
    Alert, AlertDelivery, AlertId, AlertKind, Command, ConfigValue, Evaluation,
    EvaluationAssignment, EvaluationId, HttpEvaluationMetadata, HttpTarget, MAX_DIAGNOSTIC_BYTES,
    MAX_RESPONSE_BYTES, NotificationChannelKind, Target,
};
use crate::scheduler::{select_executor, slot_at_or_before_ms};
use crate::secret::Cipher;
use crate::utils::SkipServerVerification;

const ASSIGNMENT_GRACE_MS: u64 = 1_000;
const ASSIGNMENT_BATCH_SIZE: usize = 128;

#[derive(Clone)]
struct Clients {
    verified: Client,
    insecure: Client,
}

pub fn start(cluster: Handle, cipher: Cipher) {
    let insecure_tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    let clients = Clients {
        verified: Client::builder()
            .use_rustls_default()
            .build()
            .expect("default HTTP client configuration should be valid"),
        insecure: Client::builder()
            .use_rustls(Arc::new(insecure_tls))
            .build()
            .expect("insecure HTTP client configuration should be valid"),
    };
    spawn(run_scheduler(cluster.clone())).detach();
    spawn(run_evaluations(
        cluster.clone(),
        clients.clone(),
        cipher.clone(),
    ))
    .detach();
    spawn(run_alerts(cluster, clients, cipher)).detach();
}

async fn run_scheduler(cluster: Handle) {
    loop {
        if !cluster.is_leader().await {
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        let now = now_ms();
        let state = match cluster.read().await {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(%error, "scheduler could not establish a read barrier");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let voters = match cluster.voters().await {
            Ok(voters) => voters,
            Err(error) => {
                tracing::warn!(%error, "scheduler could not read Cluster membership");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let assignments = plan_assignments(&state, &voters, now);
        for batch in assignments.chunks(ASSIGNMENT_BATCH_SIZE) {
            if let Err(error) = cluster
                .apply(Command::AssignEvaluations(batch.to_vec()))
                .await
            {
                tracing::error!(%error, "could not assign evaluation");
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn plan_assignments(
    state: &crate::domain::ApplicationState,
    voters: &BTreeSet<uuid::Uuid>,
    now_ms: u64,
) -> Vec<EvaluationAssignment> {
    state
        .targets
        .values()
        .filter(|target_state| !target_state.paused)
        .filter_map(|target_state| {
            let existing = state
                .assignments
                .values()
                .find(|assignment| assignment.id.target_id == target_state.target.id);
            if let Some(existing) = existing {
                if existing.expires_at_ms > now_ms {
                    return None;
                }
                let mut candidates = voters.clone();
                if candidates.len() > 1 {
                    candidates.remove(&existing.executor_node_id);
                }
                let executor_node_id = select_executor(existing.id, candidates)?;
                return Some(EvaluationAssignment {
                    id: existing.id,
                    executor_node_id,
                    assigned_at_ms: now_ms,
                    expires_at_ms: assignment_expiry(&target_state.target, now_ms),
                    attempt: existing.attempt.saturating_add(1),
                });
            }

            let scheduled_at_ms = slot_at_or_before_ms(
                target_state.target.id,
                target_state.target.policy.interval_ms,
                now_ms,
            )?;
            if target_state
                .latest_evaluation
                .as_ref()
                .is_some_and(|latest| latest.id.scheduled_at_ms >= scheduled_at_ms)
            {
                return None;
            }
            let id = EvaluationId {
                target_id: target_state.target.id,
                scheduled_at_ms,
            };
            Some(EvaluationAssignment {
                id,
                executor_node_id: select_executor(id, voters.iter().copied())?,
                assigned_at_ms: now_ms,
                expires_at_ms: assignment_expiry(&target_state.target, now_ms),
                attempt: 1,
            })
        })
        .collect()
}

fn assignment_expiry(target: &Target, assigned_at_ms: u64) -> u64 {
    assigned_at_ms
        .saturating_add(target.policy.timeout_ms)
        .saturating_add(ASSIGNMENT_GRACE_MS)
}

async fn run_evaluations(cluster: Handle, clients: Clients, cipher: Cipher) {
    let active = Rc::new(RefCell::new(BTreeSet::<EvaluationId>::new()));
    loop {
        let state = match cluster.read().await {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(%error, "executor could not establish a read barrier");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let due = state
            .assignments
            .values()
            .filter(|assignment| {
                assignment.executor_node_id == cluster.node_id
                    && !active.borrow().contains(&assignment.id)
            })
            .filter_map(|assignment| {
                state.targets.get(&assignment.id.target_id).map(|target| {
                    (
                        assignment.id,
                        assignment.assigned_at_ms,
                        target.target.clone(),
                    )
                })
            })
            .collect::<Vec<_>>();

        for (evaluation_id, recorded_at_ms, target) in due {
            active.borrow_mut().insert(evaluation_id);
            let active = active.clone();
            let cluster = cluster.clone();
            let clients = clients.clone();
            let cipher = cipher.clone();
            spawn(async move {
                let evaluation = evaluate(
                    &cluster,
                    &clients,
                    &cipher,
                    target.clone(),
                    evaluation_id.scheduled_at_ms,
                    recorded_at_ms,
                )
                .await;
                if let Err(error) = cluster.apply(Command::RecordEvaluation(evaluation)).await {
                    tracing::error!(target_id = %target.id.0, target_name = %target.name, %error, "could not record evaluation");
                }
                active.borrow_mut().remove(&evaluation_id);
            })
            .detach();
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn evaluate(
    cluster: &Handle,
    clients: &Clients,
    cipher: &Cipher,
    target: Target,
    scheduled_at_ms: u64,
    recorded_at_ms: u64,
) -> Evaluation {
    let started = Instant::now();
    let resolved = resolve_target(cluster, cipher, &target.http).await;
    let outcome = match resolved {
        Ok(request) => timeout(
            Duration::from_millis(target.policy.timeout_ms),
            send_with_redirects(clients, request),
        )
        .await
        .map_err(|_| "request timed out".to_owned())
        .and_then(|result| result),
        Err(error) => Err(error),
    };
    let latency_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    let (succeeded, status_code, received_bytes, final_url, diagnostic) = match outcome {
        Ok(response) => {
            let status_ok = target
                .http
                .accepted_statuses
                .iter()
                .any(|range| range.contains(response.status.as_u16()));
            let body_ok = target
                .http
                .body_contains
                .as_ref()
                .is_none_or(|needle| String::from_utf8_lossy(&response.body).contains(needle));
            let succeeded = status_ok && body_ok;
            let diagnostic = if !status_ok {
                Some(format!(
                    "HTTP status {} is outside accepted ranges",
                    response.status.as_u16()
                ))
            } else if !body_ok {
                Some("response body does not contain the required text".to_owned())
            } else {
                None
            };
            (
                succeeded,
                Some(response.status.as_u16()),
                response.body.len() as u64,
                response.url,
                diagnostic,
            )
        }
        Err(error) => (false, None, 0, target.http.url.clone(), Some(error)),
    };
    Evaluation {
        id: EvaluationId {
            target_id: target.id,
            scheduled_at_ms,
        },
        recorded_at_ms,
        executor_node_id: cluster.node_id,
        succeeded,
        http: HttpEvaluationMetadata {
            status_code,
            latency_ms,
            received_bytes,
            final_url,
        },
        diagnostic: diagnostic.map(truncate_diagnostic),
    }
}

#[derive(Clone)]
struct ResolvedRequest {
    method: Method,
    url: Url,
    headers: BTreeMap<String, String>,
    sensitive_headers: BTreeSet<String>,
    body: Vec<u8>,
    follow_redirects: bool,
    redirects_left: u8,
    skip_tls_verification: bool,
    telegram_response: bool,
}

struct ProbeResponse {
    status: StatusCode,
    body: Vec<u8>,
    url: Url,
    telegram_response: bool,
    retry_after_ms: Option<u64>,
}

async fn resolve_target(
    cluster: &Handle,
    cipher: &Cipher,
    target: &HttpTarget,
) -> Result<ResolvedRequest, String> {
    let state = cluster.read().await?;
    let sensitive_headers = target
        .headers
        .iter()
        .filter(|(_, value)| matches!(value, ConfigValue::Secret(_)))
        .map(|(name, _)| name.to_ascii_lowercase())
        .collect();
    let headers = target
        .headers
        .iter()
        .map(|(name, value)| {
            resolve_value(&state, cipher, value).map(|value| (name.clone(), value))
        })
        .collect::<Result<_, _>>()?;
    let body = target
        .body
        .as_ref()
        .map(|value| resolve_value(&state, cipher, value))
        .transpose()?
        .unwrap_or_default()
        .into_bytes();
    Ok(ResolvedRequest {
        method: Method::from_bytes(target.method.as_bytes()).map_err(|error| error.to_string())?,
        url: target.url.clone(),
        headers,
        sensitive_headers,
        body,
        follow_redirects: target.follow_redirects,
        redirects_left: target.max_redirects,
        skip_tls_verification: target.skip_tls_verification,
        telegram_response: false,
    })
}

fn resolve_value(
    state: &crate::domain::ApplicationState,
    cipher: &Cipher,
    value: &ConfigValue,
) -> Result<String, String> {
    match value {
        ConfigValue::Literal(value) => Ok(value.clone()),
        ConfigValue::Secret(id) => state
            .secrets
            .get(id)
            .ok_or_else(|| format!("secret {} no longer exists", id.0))
            .and_then(|secret| {
                cipher
                    .open(&secret.ciphertext)
                    .map_err(|error| format!("could not decrypt secret {}: {error}", id.0))
                    .and_then(|plaintext| {
                        String::from_utf8(plaintext)
                            .map_err(|_| format!("secret {} is not UTF-8", id.0))
                    })
            }),
    }
}

async fn send_with_redirects(
    clients: &Clients,
    mut request: ResolvedRequest,
) -> Result<ProbeResponse, String> {
    loop {
        let client = if request.skip_tls_verification {
            &clients.insecure
        } else {
            &clients.verified
        };
        let mut builder = client
            .request(request.method.clone(), request.url.clone())
            .map_err(|error| error.to_string())?;
        for (name, value) in &request.headers {
            builder = builder
                .header(name.as_str(), value.as_str())
                .map_err(|error| error.to_string())?;
        }
        if !request.body.is_empty() {
            builder = builder.body(request.body.clone());
        }
        let response = builder.send().await.map_err(|error| error.to_string())?;
        let status = response.status();
        let retry_after_ms = response
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse_retry_after(value, SystemTime::now()));
        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if request.follow_redirects && status.is_redirection() && location.is_some() {
            if request.redirects_left == 0 {
                return Err("redirect limit exceeded".to_owned());
            }
            request.redirects_left -= 1;
            let next_url = request
                .url
                .join(location.as_deref().unwrap_or_default())
                .map_err(|error| format!("invalid redirect: {error}"))?;
            if request.url.origin() != next_url.origin() {
                strip_cross_origin_headers(&mut request.headers, &request.sensitive_headers);
            }
            if status == StatusCode::SEE_OTHER
                || ((status == StatusCode::MOVED_PERMANENTLY || status == StatusCode::FOUND)
                    && request.method != Method::GET
                    && request.method != Method::HEAD)
            {
                request.method = Method::GET;
                request.body.clear();
            }
            request.url = next_url;
            continue;
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES)
        {
            return Err(format!("response body exceeds {MAX_RESPONSE_BYTES} bytes"));
        }
        let url = request.url;
        let telegram_response = request.telegram_response;
        let body = response.bytes().await.map_err(|error| error.to_string())?;
        if body.len() > MAX_RESPONSE_BYTES as usize {
            return Err(format!("response body exceeds {MAX_RESPONSE_BYTES} bytes"));
        }
        return Ok(ProbeResponse {
            status,
            body: body.to_vec(),
            url,
            telegram_response,
            retry_after_ms,
        });
    }
}

fn strip_cross_origin_headers(
    headers: &mut BTreeMap<String, String>,
    sensitive_headers: &BTreeSet<String>,
) {
    headers.retain(|name, _| {
        let name = name.to_ascii_lowercase();
        !sensitive_headers.contains(&name)
            && !matches!(
                name.as_str(),
                "authorization" | "cookie" | "proxy-authorization"
            )
    });
}

async fn run_alerts(cluster: Handle, clients: Clients, cipher: Cipher) {
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
                tracing::warn!(%error, "alert worker could not establish a read barrier");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let due = state.alerts.values().filter(|alert| {
            matches!(alert.delivery, AlertDelivery::Pending { next_attempt_at_ms, .. } if next_attempt_at_ms <= now)
                && !active.borrow().contains(&alert.id)
        }).cloned().collect::<Vec<_>>();
        for alert in due {
            active.borrow_mut().insert(alert.id);
            let active = active.clone();
            let cluster = cluster.clone();
            let clients = clients.clone();
            let cipher = cipher.clone();
            spawn(async move {
                deliver_alert(&cluster, &clients, &cipher, alert.clone()).await;
                active.borrow_mut().remove(&alert.id);
            })
            .detach();
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn deliver_alert(cluster: &Handle, clients: &Clients, cipher: &Cipher, alert: Alert) {
    let attempted_at_ms = now_ms();
    let result = build_alert_request(cluster, cipher, &alert).await;
    let result = match result {
        Ok(request) => timeout(
            Duration::from_secs(15),
            send_with_redirects(clients, request),
        )
        .await
        .map_err(|_| DeliveryError::Transient("notification timed out".to_owned()))
        .and_then(|result| result.map_err(DeliveryError::from_probe)),
        Err(error) => Err(DeliveryError::Permanent(error)),
    };
    let command = match result {
        Ok(response)
            if response.status.is_success()
                && (!response.telegram_response || telegram_response_ok(&response.body)) =>
        {
            Command::MarkAlertDelivered {
                alert_id: alert.id,
                delivered_at_ms: now_ms(),
            }
        }
        Ok(response) => {
            let message = format!(
                "notification endpoint returned HTTP {}",
                response.status.as_u16()
            );
            let retry_at_ms = retry_at_for_response(&alert, attempted_at_ms, &response);
            Command::RecordAlertFailure {
                alert_id: alert.id,
                attempted_at_ms,
                retry_at_ms,
                diagnostic: truncate_diagnostic(message),
            }
        }
        Err(DeliveryError::Transient(message)) => Command::RecordAlertFailure {
            alert_id: alert.id,
            attempted_at_ms,
            retry_at_ms: retry_at_for_transient_failure(&alert, attempted_at_ms),
            diagnostic: truncate_diagnostic(message),
        },
        Err(DeliveryError::Permanent(message)) => Command::RecordAlertFailure {
            alert_id: alert.id,
            attempted_at_ms,
            retry_at_ms: None,
            diagnostic: truncate_diagnostic(message),
        },
    };
    if let Err(error) = cluster.apply(command).await {
        tracing::error!(target_id = %alert.id.target_id.0, %error, "could not update alert delivery");
    }
}

enum DeliveryError {
    Transient(String),
    Permanent(String),
}

impl DeliveryError {
    fn from_probe(message: String) -> Self {
        Self::Transient(message)
    }
}

async fn build_alert_request(
    cluster: &Handle,
    cipher: &Cipher,
    alert: &Alert,
) -> Result<ResolvedRequest, String> {
    let state = cluster.read().await?;
    let channel = state
        .notification_channels
        .get(&alert.id.channel_id)
        .ok_or_else(|| format!("channel {} no longer exists", alert.id.channel_id.0))?;
    let text = format!(
        "UpGrid: {} is {}\n{}\n{}",
        alert.target_name,
        match alert.id.kind {
            AlertKind::Down => "DOWN",
            AlertKind::Recovered => "UP again",
        },
        alert.target_url,
        alert
            .evaluation
            .diagnostic
            .as_deref()
            .unwrap_or("evaluation succeeded"),
    );
    let (url, headers, body, telegram_response) = match &channel.kind {
        NotificationChannelKind::Telegram { bot_token, chat_id } => {
            let token = resolve_value(&state, cipher, &ConfigValue::Secret(*bot_token))?;
            let url = Url::parse(&format!("https://api.telegram.org/bot{token}/sendMessage"))
                .map_err(|error| error.to_string())?;
            (
                url,
                BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
                serde_json::to_vec(&json!({"chat_id": chat_id, "text": text}))
                    .map_err(|error| error.to_string())?,
                true,
            )
        }
        NotificationChannelKind::Webhook { url, headers } => {
            let mut resolved = headers
                .iter()
                .map(|(name, value)| {
                    resolve_value(&state, cipher, value).map(|value| (name.clone(), value))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            resolved
                .entry("content-type".to_owned())
                .or_insert_with(|| "application/json".to_owned());
            let payload = json!({"alert_id": stable_alert_id(alert), "event": match alert.id.kind { AlertKind::Down => "down", AlertKind::Recovered => "recovered" }, "target_id": alert.id.target_id.0, "target_name": alert.target_name, "target_url": alert.target_url, "evaluation": {"scheduled_at_ms": alert.evaluation.id.scheduled_at_ms, "succeeded": alert.evaluation.succeeded, "status_code": alert.evaluation.http.status_code, "diagnostic": alert.evaluation.diagnostic}});
            (
                url.clone(),
                resolved,
                serde_json::to_vec(&payload).map_err(|error| error.to_string())?,
                false,
            )
        }
    };
    Ok(ResolvedRequest {
        method: Method::POST,
        url,
        headers,
        sensitive_headers: BTreeSet::new(),
        body,
        follow_redirects: false,
        redirects_left: 0,
        skip_tls_verification: false,
        telegram_response,
    })
}

fn telegram_response_ok(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("ok").and_then(|ok| ok.as_bool()))
        .unwrap_or(true)
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

const ALERT_RETRY_WINDOW_MS: u64 = 24 * 60 * 60 * 1_000;

fn retry_at_for_response(
    alert: &Alert,
    attempted_at_ms: u64,
    response: &ProbeResponse,
) -> Option<u64> {
    let is_retryable_status = matches!(
        response.status,
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
    );
    if !is_retryable_status && response.retry_after_ms.is_none() {
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

fn retry_at_for_transient_failure(alert: &Alert, attempted_at_ms: u64) -> Option<u64> {
    retry_at_with_delay(alert, attempted_at_ms, retry_delay_ms(alert))
}

fn retry_at_with_delay(alert: &Alert, attempted_at_ms: u64, delay_ms: u64) -> Option<u64> {
    let retry_deadline = alert
        .evaluation
        .recorded_at_ms
        .saturating_add(ALERT_RETRY_WINDOW_MS);
    let retry_at_ms = attempted_at_ms.saturating_add(delay_ms);
    (retry_at_ms <= retry_deadline).then_some(retry_at_ms)
}

fn stable_alert_id(alert: &Alert) -> String {
    format!(
        "{}:{}:{}:{}",
        alert.id.target_id.0,
        alert.id.channel_id.0,
        alert.id.evaluation_scheduled_at_ms,
        match alert.id.kind {
            AlertKind::Down => "down",
            AlertKind::Recovered => "recovered",
        }
    )
}

fn truncate_diagnostic(value: String) -> String {
    if value.len() <= MAX_DIAGNOSTIC_BYTES {
        return value;
    }
    let mut end = MAX_DIAGNOSTIC_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::domain::{
        AlertDelivery, AlertId, ApplicationState, EvaluationId, EvaluationPolicy,
        HttpEvaluationMetadata, HttpTarget, NotificationChannelId, TargetId,
    };

    fn alert(recorded_at_ms: u64) -> Alert {
        let target_id = TargetId(Uuid::from_u128(1));
        Alert {
            id: AlertId {
                target_id,
                channel_id: NotificationChannelId(Uuid::from_u128(2)),
                evaluation_scheduled_at_ms: recorded_at_ms,
                kind: AlertKind::Down,
            },
            target_name: "API".to_owned(),
            target_url: Url::parse("https://example.com/health").unwrap(),
            evaluation: Evaluation {
                id: EvaluationId {
                    target_id,
                    scheduled_at_ms: recorded_at_ms,
                },
                recorded_at_ms,
                executor_node_id: Uuid::from_u128(3),
                succeeded: false,
                http: HttpEvaluationMetadata {
                    status_code: Some(500),
                    latency_ms: 10,
                    received_bytes: 0,
                    final_url: Url::parse("https://example.com/health").unwrap(),
                },
                diagnostic: None,
            },
            delivery: AlertDelivery::Pending {
                attempts: 0,
                next_attempt_at_ms: recorded_at_ms,
            },
        }
    }

    fn response(status: StatusCode, retry_after_ms: Option<u64>) -> ProbeResponse {
        ProbeResponse {
            status,
            body: Vec::new(),
            url: Url::parse("https://hooks.example.com/upgrid").unwrap(),
            telegram_response: false,
            retry_after_ms,
        }
    }

    #[test]
    fn unqualified_server_errors_are_terminal() {
        let alert = alert(1_000);
        assert_eq!(
            retry_at_for_response(&alert, 2_000, &response(StatusCode::BAD_GATEWAY, None)),
            None
        );
    }

    #[test]
    fn retry_after_qualifies_a_response_but_retries_are_bounded() {
        let alert = alert(1_000);
        assert_eq!(
            retry_at_for_response(
                &alert,
                2_000,
                &response(StatusCode::BAD_GATEWAY, Some(30_000))
            ),
            Some(32_000)
        );
        assert_eq!(
            retry_at_for_response(
                &alert,
                1_000 + ALERT_RETRY_WINDOW_MS,
                &response(StatusCode::TOO_MANY_REQUESTS, None)
            ),
            None
        );
    }

    #[test]
    fn retry_after_accepts_delta_seconds_and_http_dates() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let later = now + Duration::from_secs(30);

        assert_eq!(parse_retry_after("30", now), Some(30_000));
        assert_eq!(
            parse_retry_after(&httpdate::fmt_http_date(later), now),
            Some(30_000)
        );
        assert_eq!(parse_retry_after("not a date", now), None);
    }

    #[test]
    fn webhook_alert_id_is_stable() {
        let alert = alert(1_000);
        assert_eq!(stable_alert_id(&alert), stable_alert_id(&alert));
        assert!(stable_alert_id(&alert).ends_with(":down"));
    }

    #[test]
    fn expired_assignment_moves_to_another_voter() {
        let target_id = TargetId(Uuid::from_u128(42));
        let target = Target {
            id: target_id,
            name: "API".to_owned(),
            http: HttpTarget::get(Url::parse("https://example.com/health").unwrap()),
            policy: EvaluationPolicy {
                interval_ms: 1_000,
                timeout_ms: 100,
                failure_threshold: 3,
            },
            notification_channels: BTreeSet::new(),
        };
        let mut state = ApplicationState::default();
        state.apply(Command::CreateTarget(target)).unwrap();
        let voters = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]);
        let phase = crate::scheduler::phase_offset_ms(target_id, 1_000).unwrap();
        let first = plan_assignments(&state, &voters, phase + 10_000)
            .pop()
            .unwrap();
        state
            .apply(Command::AssignEvaluation(first.clone()))
            .unwrap();

        let second = plan_assignments(&state, &voters, first.expires_at_ms)
            .pop()
            .unwrap();

        assert_eq!(second.id, first.id);
        assert_eq!(second.attempt, 2);
        assert_ne!(second.executor_node_id, first.executor_node_id);
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
}
