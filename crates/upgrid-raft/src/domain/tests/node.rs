use super::evaluation::{evaluation, id, state_with_target};
use super::*;

fn node(node_id: Uuid, name: &str) -> NodeTarget {
    NodeTarget {
        node_id,
        name: name.to_owned(),
        url: Url::parse(&format!(
            "up://127.0.0.1:{}",
            11_451 + node_id.as_u128() as u16
        ))
        .unwrap(),
        policy: EvaluationPolicy {
            interval_ms: 10_000,
            timeout_ms: 2_000,
            failure_threshold: 1,
        },
    }
}

#[test]
fn node_transitions_use_default_channels() {
    let (mut state, _, channel_id) = state_with_target();
    state
        .apply(Command::SetNotificationChannelDefault {
            channel_id,
            is_default: true,
        })
        .unwrap();
    let node_id = id(20);
    let node = node(node_id, "green-anchor");
    state
        .apply(Command::SyncNodeTargets(vec![node.clone()]))
        .unwrap();
    let mut failed = evaluation(node.id(), 1_000, false);
    failed.http.final_url = node.url.clone();

    let result = state
        .apply(Command::RecordNodeEvaluation(failed.clone()))
        .unwrap();

    let CommandResult::NodeEvaluationAccepted {
        availability,
        alerts,
    } = result
    else {
        panic!("Node Evaluation should be accepted");
    };
    assert_eq!(availability, AvailabilityState::Down);
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].channel_id, channel_id);
    assert_eq!(state.transitions[&failed.id].target_name, "green-anchor");
    assert_eq!(state.alerts[&alerts[0]].target_url, node.url);
}

#[test]
fn syncing_membership_preserves_health_and_removes_departed_nodes() {
    let mut state = ApplicationState::default();
    let node_id = id(30);
    let mut current = node(node_id, "green-anchor");
    state
        .apply(Command::SyncNodeTargets(vec![current.clone()]))
        .unwrap();
    let mut passed = evaluation(current.id(), 1_000, true);
    passed.http.final_url = current.url.clone();
    state.apply(Command::RecordNodeEvaluation(passed)).unwrap();

    current.name = "renamed-anchor".to_owned();
    state
        .apply(Command::SyncNodeTargets(vec![current.clone()]))
        .unwrap();
    let retained = &state.node_targets[&current.id()];
    assert_eq!(retained.target.name, "renamed-anchor");
    assert_eq!(retained.availability, AvailabilityState::Up);
    assert_eq!(retained.history.len(), 1);

    state.apply(Command::SyncNodeTargets(Vec::new())).unwrap();
    assert!(state.node_targets.is_empty());
}
