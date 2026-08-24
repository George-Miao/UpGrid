use crate::domain::{ApplicationState, decode_v2026_08_19_connectivity_alerts_application_state};

pub(super) const VERSION: &str = "v2026_08_19_connectivity_alerts";

pub(super) fn snapshot(payload: &[u8]) -> Result<ApplicationState, postcard::Error> {
    decode_v2026_08_19_connectivity_alerts_application_state(payload).map(|mut state| {
        state.normalize_connectivity_state();
        state.normalize_alert_ids();
        state
    })
}

#[cfg(test)]
mod tests {
    use url::Url;
    use uuid::Uuid;

    use super::{VERSION, snapshot};
    use crate::domain::{
        Alert, AlertDelivery, AlertId, AlertKind, ApplicationState, AvailabilityTransition,
        Evaluation, EvaluationId, HttpEvaluationMetadata, NotificationChannelId, TargetId,
        encode_v2026_08_19_connectivity_alerts_application_state,
    };

    #[test]
    fn version_matches_file_name() {
        assert_eq!(VERSION, module_path!().rsplit("::").nth(1).unwrap());
    }

    #[test]
    fn snapshot_restores_binary_alert_kind() {
        let target_id = TargetId(Uuid::from_u128(1));
        let evaluation_id = EvaluationId {
            target_id,
            scheduled_at_ms: 10,
        };
        let url = Url::parse("https://example.com").unwrap();
        let evaluation = Evaluation {
            id: evaluation_id,
            recorded_at_ms: 11,
            executor_node_id: Uuid::from_u128(2),
            succeeded: true,
            http: HttpEvaluationMetadata {
                status_code: Some(200),
                latency_ms: 1,
                received_bytes: 0,
                final_url: url.clone(),
            },
            diagnostic: None,
        };
        let alert_id = AlertId {
            target_id,
            channel_id: NotificationChannelId(Uuid::from_u128(3)),
            evaluation_scheduled_at_ms: 10,
            kind: AlertKind::Recovered,
        };
        let mut state = ApplicationState::default();
        state.availability_transitions.insert(
            evaluation_id,
            AvailabilityTransition {
                kind: AlertKind::Recovered,
                target_name: "Example".to_owned(),
                target_url: url.clone(),
                evaluation: evaluation.clone(),
            },
        );
        state.alerts.insert(
            alert_id,
            Alert {
                id: alert_id,
                target_name: "Example".to_owned(),
                target_url: url,
                evaluation,
                delivery: AlertDelivery::Pending {
                    attempts: 0,
                    next_attempt_at_ms: 10,
                },
            },
        );

        let payload = encode_v2026_08_19_connectivity_alerts_application_state(state).unwrap();
        let decoded = snapshot(&payload).unwrap();

        assert_eq!(
            decoded.alerts.get(&alert_id).map(|alert| alert.id),
            Some(alert_id)
        );
    }
}
