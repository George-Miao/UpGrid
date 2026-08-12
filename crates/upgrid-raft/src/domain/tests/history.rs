use super::evaluation::{evaluation, id, state_with_target};
use super::*;

#[test]
fn target_evaluations_accumulate_hourly_rollups() {
    let (mut state, target_id, _) = state_with_target();
    let mut first = evaluation(target_id, HISTORY_ROLLUP_INTERVAL_MS + 1_000, true);
    first.http.latency_ms = 40;
    let mut second = evaluation(target_id, HISTORY_ROLLUP_INTERVAL_MS + 2_000, false);
    second.http.latency_ms = 120;

    state.apply(Command::RecordEvaluation(first)).unwrap();
    state.apply(Command::RecordEvaluation(second)).unwrap();

    assert_eq!(
        state.history_rollups[&target_id][&HISTORY_ROLLUP_INTERVAL_MS],
        EvaluationRollup {
            bucket_start_ms: HISTORY_ROLLUP_INTERVAL_MS,
            samples: 2,
            successes: 1,
            failures: 1,
            latency_total_ms: 160,
            latency_min_ms: 40,
            latency_max_ms: 120,
        }
    );
}

#[test]
fn node_evaluations_accumulate_hourly_rollups() {
    let mut state = ApplicationState::default();
    let node = NodeTarget {
        node_id: id(80),
        name: "edge".to_owned(),
        url: Url::parse("up://127.0.0.1:11451").unwrap(),
        policy: EvaluationPolicy::default(),
    };
    let target_id = node.id();
    state.apply(Command::SyncNodeTargets(vec![node])).unwrap();

    state
        .apply(Command::RecordNodeEvaluation(evaluation(
            target_id, 1_000, true,
        )))
        .unwrap();

    let rollup = &state.history_rollups[&target_id][&0];
    assert_eq!(rollup.samples, 1);
    assert_eq!(rollup.successes, 1);
    assert_eq!(rollup.failures, 0);
}

#[test]
fn rollup_retention_prunes_complete_old_buckets() {
    let (mut state, target_id, _) = state_with_target();
    state
        .apply(Command::SetHistoryRollupRetention {
            retention_ms: HISTORY_ROLLUP_INTERVAL_MS,
        })
        .unwrap();
    state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 1_000, true,
        )))
        .unwrap();
    state
        .apply(Command::RecordEvaluation(evaluation(
            target_id,
            2 * HISTORY_ROLLUP_INTERVAL_MS + 1_000,
            true,
        )))
        .unwrap();

    assert_eq!(
        state.history_rollups[&target_id]
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![2 * HISTORY_ROLLUP_INTERVAL_MS]
    );
}
