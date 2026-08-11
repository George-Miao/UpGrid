use super::evaluation::{evaluation, state_with_target};
use super::*;
#[test]
fn delivered_alerts_cannot_regress_to_pending() {
    let (mut state, target_id, channel_id) = state_with_target();
    for scheduled_at_ms in [1_000, 2_000, 3_000] {
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id,
                scheduled_at_ms,
                false,
            )))
            .unwrap();
    }
    let alert_id = AlertId {
        target_id,
        channel_id,
        evaluation_scheduled_at_ms: 3_000,
        kind: AlertKind::Down,
    };
    state
        .apply(Command::MarkAlertDelivered {
            alert_id,
            delivered_at_ms: 3_100,
        })
        .unwrap();
    state
        .apply(Command::RecordAlertFailure {
            alert_id,
            attempted_at_ms: 3_200,
            retry_at_ms: Some(4_000),
            diagnostic: "late failure".to_owned(),
        })
        .unwrap();

    assert_eq!(
        state.alerts[&alert_id].delivery,
        AlertDelivery::Delivered {
            delivered_at_ms: 3_100
        }
    );
    assert!(matches!(
        state.apply(Command::RetryAlert {
            alert_id,
            retry_at_ms: 3_300,
        }),
        Err(DomainError::InvalidAlert(_))
    ));
}

#[test]
fn failed_alerts_can_be_acknowledged_and_retried() {
    let (mut state, target_id, channel_id) = state_with_target();
    for scheduled_at_ms in [1_000, 2_000, 3_000] {
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id,
                scheduled_at_ms,
                false,
            )))
            .unwrap();
    }
    let alert_id = AlertId {
        target_id,
        channel_id,
        evaluation_scheduled_at_ms: 3_000,
        kind: AlertKind::Down,
    };
    state
        .apply(Command::RecordAlertFailure {
            alert_id,
            attempted_at_ms: 3_100,
            retry_at_ms: None,
            diagnostic: "permanent failure".to_owned(),
        })
        .unwrap();

    state
        .apply(Command::AcknowledgeAlert {
            alert_id,
            acknowledged_at_ms: 3_200,
        })
        .unwrap();
    state
        .apply(Command::AcknowledgeAlert {
            alert_id,
            acknowledged_at_ms: 3_300,
        })
        .unwrap();
    state
        .apply(Command::RetryAlert {
            alert_id,
            retry_at_ms: 3_400,
        })
        .unwrap();

    assert_eq!(state.alert_acknowledgements[&alert_id], 3_200);
    assert_eq!(
        state.alerts[&alert_id].delivery,
        AlertDelivery::Pending {
            attempts: 0,
            next_attempt_at_ms: 3_400,
        }
    );
}
