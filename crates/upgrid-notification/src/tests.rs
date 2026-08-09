use upgrid_raft::domain::{
    Alert, AlertDelivery, AlertId, AlertKind, Evaluation, EvaluationId, HttpEvaluationMetadata,
    NotificationChannelId, TargetId,
};
use url::Url;
use uuid::Uuid;

use super::*;

fn alert(delivery: AlertDelivery) -> Alert {
    let target_id = TargetId(Uuid::from_u128(1));
    Alert {
        id: AlertId {
            target_id,
            channel_id: NotificationChannelId(Uuid::from_u128(2)),
            evaluation_scheduled_at_ms: 1_000,
            kind: AlertKind::Down,
        },
        target_name: "API".to_owned(),
        target_url: Url::parse("https://example.com/health").unwrap(),
        evaluation: Evaluation {
            id: EvaluationId {
                target_id,
                scheduled_at_ms: 1_000,
            },
            recorded_at_ms: 1_100,
            executor_node_id: Uuid::from_u128(3),
            succeeded: false,
            http: HttpEvaluationMetadata {
                status_code: Some(500),
                latency_ms: 50,
                received_bytes: 0,
                final_url: Url::parse("https://example.com/health").unwrap(),
            },
            diagnostic: Some("unexpected status".to_owned()),
        },
        delivery,
    }
}

fn response(status: StatusCode, retry_after_ms: Option<u64>) -> Response {
    Response {
        status,
        body: Vec::new(),
        retry_after_ms,
    }
}

#[test]
fn server_errors_are_permanent_without_retry_after() {
    let alert = alert(AlertDelivery::Pending {
        attempts: 0,
        next_attempt_at_ms: 0,
    });

    assert_eq!(
        retry_at_for_response(&alert, 2_000, &response(StatusCode::BAD_GATEWAY, None)),
        None,
    );
}

#[test]
fn rate_limits_and_explicit_retry_after_are_retried() {
    let alert = alert(AlertDelivery::Pending {
        attempts: 0,
        next_attempt_at_ms: 0,
    });

    assert_eq!(
        retry_at_for_response(
            &alert,
            2_000,
            &response(StatusCode::TOO_MANY_REQUESTS, Some(3_000)),
        ),
        Some(5_000),
    );
    assert_eq!(
        retry_at_for_response(
            &alert,
            2_000,
            &response(StatusCode::BAD_GATEWAY, Some(3_000)),
        ),
        Some(5_000),
    );
}

#[test]
fn numeric_retry_after_is_seconds() {
    assert_eq!(parse_retry_after("7", SystemTime::UNIX_EPOCH), Some(7_000));
}

#[test]
fn alert_text_is_compact_and_status_focused() {
    let mut down = alert(AlertDelivery::Pending {
        attempts: 0,
        next_attempt_at_ms: 0,
    });
    down.evaluation.http.status_code = Some(502);
    assert_eq!(
        channel::alert_text(&down),
        "[API] [🔴 Down] Request failed with status code 502",
    );

    let mut recovered = down;
    recovered.id.kind = AlertKind::Recovered;
    recovered.evaluation.succeeded = true;
    recovered.evaluation.http.status_code = Some(200);
    recovered.evaluation.diagnostic = None;
    assert_eq!(channel::alert_text(&recovered), "[API] [✅ Up] 200 - OK",);
}
