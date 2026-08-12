use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use compio::runtime::spawn;
use compio::time::{sleep, timeout};
use upgrid_config::Cipher;
use upgrid_raft::Handle;
use upgrid_raft::domain::{
    Command, Evaluation, EvaluationId, HttpEvaluationMetadata, MAX_DIAGNOSTIC_BYTES, Target,
    TargetKind,
};

use super::http::{self, resolve, send};
use super::network;
use super::runtime::Clients;

pub(super) async fn run(cluster: Handle, clients: Clients, cipher: Cipher) {
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
                    && !state
                        .evaluation_results(assignment.id)
                        .is_some_and(|results| results.contains_key(&cluster.node_id))
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

        for (id, recorded_at_ms, target) in due {
            active.borrow_mut().insert(id);
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
                    id.scheduled_at_ms,
                    recorded_at_ms,
                )
                .await;
                if let Err(error) = cluster.apply(Command::RecordEvaluation(evaluation)).await {
                    tracing::error!(target_id = %target.id.0, target_name = %target.name, %error, "could not record evaluation");
                }
                active.borrow_mut().remove(&id);
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
    let timeout_duration = Duration::from_millis(target.policy.timeout_ms);
    let outcome = match target.kind() {
        TargetKind::Http => {
            ProbeOutcome::Http(match resolve(cluster, cipher, &target.http).await {
                Ok(request) => match timeout(timeout_duration, send(clients, request)).await {
                    Ok(result) => result,
                    Err(_) => Err(http::Error::RequestTimeout),
                },
                Err(error) => Err(error),
            })
        }
        kind => ProbeOutcome::Network(
            match timeout(
                timeout_duration,
                network::probe(
                    &clients.network_runtime,
                    &target.http,
                    kind,
                    timeout_duration,
                ),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(network::Error::RequestTimeout),
            },
        ),
    };
    let latency_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    let (succeeded, status_code, received_bytes, final_url, diagnostic) = match outcome {
        ProbeOutcome::Http(Ok(response)) => {
            let status_ok = target
                .http
                .accepted_statuses
                .iter()
                .any(|range| range.contains(response.status.as_u16()));
            let assertion_diagnostic =
                crate::assertion::evaluate(&target.http.assertions, &response, latency_ms);
            let diagnostic = if !status_ok {
                Some(format!(
                    "HTTP status {} is outside accepted ranges",
                    response.status.as_u16()
                ))
            } else {
                assertion_diagnostic
            };
            let succeeded = status_ok && diagnostic.is_none();
            (
                succeeded,
                Some(response.status.as_u16()),
                response.body.len() as u64,
                response.url,
                diagnostic,
            )
        }
        ProbeOutcome::Http(Err(error)) => (
            false,
            None,
            0,
            target.http.url.clone(),
            Some(error.to_string()),
        ),
        ProbeOutcome::Network(Ok(())) => (true, None, 0, target.http.url.clone(), None),
        ProbeOutcome::Network(Err(error)) => (
            false,
            None,
            0,
            target.http.url.clone(),
            Some(error.to_string()),
        ),
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
        diagnostic: diagnostic.map(truncate),
    }
}

enum ProbeOutcome {
    Http(Result<http::Response, http::Error>),
    Network(Result<(), network::Error>),
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
