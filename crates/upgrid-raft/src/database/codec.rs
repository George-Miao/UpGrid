use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::DatabaseError;

pub(super) fn encode_field<T>(
    table: &'static str,
    column: &'static str,
    value: &T,
) -> Result<Vec<u8>, DatabaseError>
where
    T: Serialize + ?Sized,
{
    serde_json::to_vec(value).map_err(|source| DatabaseError::FieldEncode {
        table,
        column,
        source,
    })
}

pub(super) fn decode_field<T>(
    table: &'static str,
    column: &'static str,
    bytes: &[u8],
) -> Result<T, DatabaseError>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(bytes).map_err(|source| DatabaseError::FieldDecode {
        table,
        column,
        source,
    })
}

pub(super) fn encode_application(
    value: &crate::domain::ApplicationState,
) -> Result<Vec<u8>, DatabaseError> {
    encode_field("state_machine", "application", &ApplicationRef(value))
}

pub(super) fn decode_application(
    bytes: &[u8],
) -> Result<crate::domain::ApplicationState, DatabaseError> {
    decode_field::<Application>("state_machine", "application", bytes).map(|value| value.0)
}

struct ApplicationRef<'a>(&'a crate::domain::ApplicationState);

impl Serialize for ApplicationRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize_database_json(serializer)
    }
}

struct Application(crate::domain::ApplicationState);

impl<'de> Deserialize<'de> for Application {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::domain::ApplicationState::deserialize_database_json(deserializer).map(Self)
    }
}

pub(super) fn integer(
    table: &'static str,
    column: &'static str,
    value: u64,
) -> Result<i64, DatabaseError> {
    i64::try_from(value).map_err(|source| DatabaseError::IntegerRange {
        table,
        column,
        value,
        source,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::DirectedRoute;
    use crate::domain::{
        Alert, AlertId, AlertKind, ApplicationState, AvailabilityTransition, Command, EvaluationId,
    };

    const TARGET_ID: &str = "00000000-0000-0000-0000-000000000001";
    const CHANNEL_ID: &str = "00000000-0000-0000-0000-000000000002";
    const EXECUTOR_ID: &str = "00000000-0000-0000-0000-000000000003";
    const EVALUATION_OPERATION_ID: &str = "00000000-0000-0000-0000-000000000004";
    const NODE_OPERATION_ID: &str = "00000000-0000-0000-0000-000000000005";

    fn application_json() -> Value {
        serde_json::to_value(ApplicationRef(&ApplicationState::default())).unwrap()
    }

    fn alert_id_json() -> Value {
        json!({
            "target_id": TARGET_ID,
            "channel_id": CHANNEL_ID,
            "evaluation_scheduled_at_ms": 1_000,
            "kind": "Down",
        })
    }

    fn legacy_alert_id_json() -> Value {
        let mut id = alert_id_json();
        id.as_object_mut().unwrap().remove("kind");
        id
    }

    fn evaluation_json() -> Value {
        json!({
            "id": {
                "target_id": TARGET_ID,
                "scheduled_at_ms": 1_000,
            },
            "recorded_at_ms": 1_050,
            "executor_node_id": EXECUTOR_ID,
            "succeeded": false,
            "http": {
                "status_code": null,
                "latency_ms": 50,
                "received_bytes": 0,
                "final_url": "https://example.com/health",
            },
            "diagnostic": "connection refused",
        })
    }

    #[test]
    fn legacy_alert_state_fields_decode_and_encode_with_current_names() {
        let alert_id = legacy_alert_id_json();
        let evaluation = evaluation_json();
        let alert = json!({
            "id": alert_id,
            "target_name": "Example",
            "target_url": "https://example.com/health",
            "evaluation": evaluation,
            "delivery": {
                "Pending": {
                    "attempts": 1,
                    "next_attempt_at_ms": 2_000,
                },
            },
        });
        let transition = json!({
            "kind": "Down",
            "target_name": "Example",
            "target_url": "https://example.com/health",
            "evaluation": evaluation_json(),
        });
        let expected_id: AlertId = serde_json::from_value(alert_id_json()).unwrap();
        let expected_alert: Alert = serde_json::from_value(alert.clone()).unwrap();
        let expected_evaluation_id: EvaluationId =
            serde_json::from_value(evaluation_json()["id"].clone()).unwrap();
        let expected_transition: AvailabilityTransition =
            serde_json::from_value(transition.clone()).unwrap();

        for (alerts_field, transitions_field) in [
            ("alert_deliveries", "alert_events"),
            ("alerts", "transitions"),
        ] {
            let mut row = application_json();
            let fields = row.as_object_mut().unwrap();
            fields.remove("alerts");
            fields.remove("availability_transitions");
            fields.insert(
                alerts_field.to_owned(),
                json!([[legacy_alert_id_json(), alert.clone()]]),
            );
            fields.insert(
                transitions_field.to_owned(),
                json!([[evaluation_json()["id"].clone(), transition.clone()]]),
            );

            let decoded = decode_application(&serde_json::to_vec(&row).unwrap()).unwrap();
            assert_eq!(decoded.alerts.get(&expected_id), Some(&expected_alert));
            assert_eq!(
                decoded
                    .availability_transitions
                    .get(&expected_evaluation_id),
                Some(&expected_transition)
            );

            let current: Value =
                serde_json::from_slice(&encode_application(&decoded).unwrap()).unwrap();
            assert_eq!(
                current["alerts"],
                json!([[expected_id, expected_alert.clone()]])
            );
            assert_eq!(
                current["availability_transitions"],
                json!([[expected_evaluation_id, expected_transition.clone()]])
            );
            assert!(current.get("alert_deliveries").is_none());
            assert!(current.get("alert_events").is_none());
            assert!(current.get("transitions").is_none());
        }
    }

    #[test]
    fn legacy_processed_alert_results_decode_and_encode_with_current_names() {
        let mut row = application_json();
        row["processed_operations"] = json!({
            "00000000-0000-0000-0000-000000000004": {
                "submitted_at_ms": 1_100,
                "result": {
                    "Ok": {
                        "EvaluationAccepted": {
                            "availability": "Down",
                            "alert_deliveries": [legacy_alert_id_json()],
                        },
                    },
                },
            },
            "00000000-0000-0000-0000-000000000005": {
                "submitted_at_ms": 1_200,
                "result": {
                    "Ok": {
                        "NodeEvaluationAccepted": {
                            "availability": "Up",
                            "alert_deliveries": [legacy_alert_id_json()],
                        },
                    },
                },
            },
        });

        let decoded = decode_application(&serde_json::to_vec(&row).unwrap()).unwrap();
        let current: Value =
            serde_json::from_slice(&encode_application(&decoded).unwrap()).unwrap();

        for (operation_id, variant, expected_kind) in [
            (EVALUATION_OPERATION_ID, "EvaluationAccepted", "Down"),
            (NODE_OPERATION_ID, "NodeEvaluationAccepted", "Recovered"),
        ] {
            let payload = &current["processed_operations"][operation_id]["result"]["Ok"][variant];
            assert_eq!(
                payload["alerts"],
                json!([{
                    "target_id": TARGET_ID,
                    "channel_id": CHANNEL_ID,
                    "evaluation_scheduled_at_ms": 1_000,
                    "kind": expected_kind,
                }])
            );
            assert!(payload.get("alert_deliveries").is_none());
        }
    }

    #[test]
    fn persisted_connectivity_failures_restore_confirmed_counts() {
        let route = DirectedRoute {
            source: uuid::Uuid::from_u128(1),
            destination: uuid::Uuid::from_u128(2),
        };
        let mut row = application_json();
        let fields = row.as_object_mut().unwrap();
        fields.insert(
            "connectivity_failures".to_owned(),
            serde_json::to_value(std::collections::BTreeSet::from([route])).unwrap(),
        );
        fields.remove("connectivity_failure_counts");

        let mut decoded = decode_application(&serde_json::to_vec(&row).unwrap()).unwrap();
        assert!(decoded.connectivity_degraded());
        decoded
            .apply(Command::RecordConnectivity {
                leases: Vec::new(),
                verified: Some(std::collections::BTreeMap::new()),
                checked_at_ms: 1_000,
                failures: std::collections::BTreeSet::from([route]),
            })
            .unwrap();

        assert_eq!(
            decoded.connectivity_route_state(route.source).0,
            std::collections::BTreeSet::from([route])
        );
        assert!(decoded.availability_transitions.is_empty());
    }

    #[test]
    fn legacy_healthy_success_counts_do_not_decode_as_degraded() {
        let mut row = application_json();
        row.as_object_mut()
            .unwrap()
            .insert("connectivity_success_count".to_owned(), json!(u8::MAX));

        let decoded = decode_application(&serde_json::to_vec(&row).unwrap()).unwrap();

        assert!(!decoded.connectivity_degraded());
    }

    #[test]
    fn current_healthy_marker_overrides_a_stale_down_transition() {
        let route = DirectedRoute {
            source: uuid::Uuid::from_u128(1),
            destination: uuid::Uuid::from_u128(2),
        };
        let mut state = ApplicationState::default();
        for (checked_at_ms, failed) in [
            (1_000, true),
            (2_000, true),
            (3_000, true),
            (4_000, false),
            (5_000, false),
            (6_000, false),
        ] {
            state
                .apply(Command::RecordConnectivity {
                    leases: Vec::new(),
                    verified: Some(Default::default()),
                    checked_at_ms,
                    failures: if failed {
                        std::collections::BTreeSet::from([route])
                    } else {
                        Default::default()
                    },
                })
                .unwrap();
        }
        assert!(!state.connectivity_degraded());
        state
            .availability_transitions
            .retain(|_, transition| transition.kind == AlertKind::Down);

        let bytes = serde_json::to_vec(&ApplicationRef(&state)).unwrap();
        let decoded = decode_application(&bytes).unwrap();

        assert!(!decoded.connectivity_degraded());
    }
}
