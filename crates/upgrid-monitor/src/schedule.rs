use std::collections::BTreeSet;
use std::time::Duration;

use compio::time::sleep;
use upgrid_config::now_ms;
use upgrid_raft::Handle;
use upgrid_raft::domain::{ApplicationState, Command, EvaluationAssignment, EvaluationId, Target};

use crate::scheduler::{select_executor, select_executors, slot_at_or_before_ms};

const ASSIGNMENT_GRACE_MS: u64 = 1_000;
const ASSIGNMENT_BATCH_SIZE: usize = 128;

pub(super) async fn run(cluster: Handle) {
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
        let assignments = plan(&state, &voters, now);
        for batch in assignment_batches(&assignments) {
            if let Err(error) = cluster.apply(Command::AssignEvaluations(batch)).await {
                tracing::error!(%error, "could not assign evaluation");
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn plan(
    state: &ApplicationState,
    voters: &BTreeSet<uuid::Uuid>,
    now_ms: u64,
) -> Vec<EvaluationAssignment> {
    let eligible = voters
        .difference(&state.draining_nodes)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut planned = Vec::new();
    for target in state.targets.values().filter(|target| !target.paused) {
        let existing = state
            .assignments
            .values()
            .filter(|assignment| assignment.id.target_id == target.target.id)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(first) = existing.first() {
            let completed = state
                .evaluation_results(first.id)
                .map(|results| results.keys().copied().collect())
                .unwrap_or_default();
            let occupied = existing
                .iter()
                .filter(|assignment| assignment.expires_at_ms > now_ms)
                .map(|assignment| assignment.executor_node_id)
                .collect::<BTreeSet<_>>();
            let mut candidates = eligible
                .difference(&completed)
                .copied()
                .collect::<BTreeSet<_>>();
            for node_id in &occupied {
                candidates.remove(node_id);
            }
            for expired in existing
                .iter()
                .filter(|assignment| assignment.expires_at_ms <= now_ms)
            {
                let mut preferred = candidates.clone();
                if preferred.len() > 1 {
                    preferred.remove(&expired.executor_node_id);
                }
                let Some(executor_node_id) = select_executor(expired.id, preferred)
                    .or_else(|| select_executor(expired.id, candidates.iter().copied()))
                else {
                    continue;
                };
                candidates.remove(&executor_node_id);
                planned.push(EvaluationAssignment {
                    id: expired.id,
                    executor_node_id,
                    assigned_at_ms: now_ms,
                    expires_at_ms: expiry(&target.target, now_ms),
                    attempt: expired.attempt.saturating_add(1),
                });
            }
            continue;
        }

        let Some(scheduled_at_ms) =
            slot_at_or_before_ms(target.target.id, target.target.policy.interval_ms, now_ms)
        else {
            continue;
        };
        if target
            .latest_evaluation
            .as_ref()
            .is_some_and(|latest| latest.id.scheduled_at_ms >= scheduled_at_ms)
        {
            continue;
        }
        let id = EvaluationId {
            target_id: target.target.id,
            scheduled_at_ms,
        };
        let completed = state
            .evaluation_results(id)
            .map(|results| results.keys().copied().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let expected = state.expected_evaluation_results(id).unwrap_or_else(|| {
            state
                .target_location_count(target.target.id)
                .min(eligible.len().try_into().unwrap_or(u16::MAX))
        });
        let missing = usize::from(expected).saturating_sub(completed.len());
        let candidates = eligible.difference(&completed).copied();
        planned.extend(select_executors(id, candidates, missing).into_iter().map(
            |executor_node_id| EvaluationAssignment {
                id,
                executor_node_id,
                assigned_at_ms: now_ms,
                expires_at_ms: expiry(&target.target, now_ms),
                attempt: 1,
            },
        ));
    }
    planned
}

fn assignment_batches(assignments: &[EvaluationAssignment]) -> Vec<Vec<EvaluationAssignment>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    for group in assignments.chunk_by(|left, right| left.id == right.id) {
        if !current.is_empty() && current.len() + group.len() > ASSIGNMENT_BATCH_SIZE {
            batches.push(std::mem::take(&mut current));
        }
        current.extend_from_slice(group);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

fn expiry(target: &Target, assigned_at_ms: u64) -> u64 {
    assigned_at_ms
        .saturating_add(target.policy.timeout_ms)
        .saturating_add(ASSIGNMENT_GRACE_MS)
}

#[cfg(test)]
mod tests {
    use upgrid_raft::domain::{EvaluationPolicy, HttpTarget, TargetId};
    use url::Url;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn expired_assignment_moves_to_another_voter() {
        let id = TargetId(Uuid::from_u128(42));
        let target = Target {
            id,
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
        state
            .apply(Command::CreateTarget {
                target,
                use_default_notifications: true,
            })
            .unwrap();
        let voters = BTreeSet::from([Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)]);
        let phase = crate::scheduler::phase_offset_ms(id, 1_000).unwrap();
        let first = plan(&state, &voters, phase + 10_000).pop().unwrap();
        state
            .apply(Command::AssignEvaluation(first.clone()))
            .unwrap();
        let second = plan(&state, &voters, first.expires_at_ms).pop().unwrap();

        assert_eq!(second.id, first.id);
        assert_eq!(second.attempt, 2);
        assert_ne!(second.executor_node_id, first.executor_node_id);
        state
            .apply(Command::AssignEvaluation(second.clone()))
            .unwrap();
        assert_eq!(state.assignments.len(), 1);
    }

    #[test]
    fn draining_nodes_receive_no_new_assignments() {
        let id = TargetId(Uuid::from_u128(42));
        let target = Target {
            id,
            name: "API".to_owned(),
            http: HttpTarget::get(Url::parse("https://example.com/health").unwrap()),
            policy: EvaluationPolicy {
                interval_ms: 1_000,
                timeout_ms: 100,
                failure_threshold: 3,
            },
            notification_channels: BTreeSet::new(),
        };
        let draining = Uuid::from_u128(1);
        let eligible = Uuid::from_u128(2);
        let mut state = ApplicationState::default();
        state
            .apply(Command::CreateTarget {
                target,
                use_default_notifications: true,
            })
            .unwrap();
        state.draining_nodes.insert(draining);

        let assignment = plan(
            &state,
            &BTreeSet::from([draining, eligible]),
            crate::scheduler::phase_offset_ms(id, 1_000).unwrap() + 10_000,
        )
        .pop()
        .unwrap();

        assert_eq!(assignment.executor_node_id, eligible);
    }

    #[test]
    fn plan_assigns_one_distinct_voter_per_requested_location() {
        let id = TargetId(Uuid::from_u128(43));
        let target = Target {
            id,
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
        state
            .apply(Command::CreateTargetWithLocations {
                target,
                use_default_notifications: true,
                locations: 3,
            })
            .unwrap();
        let voters = BTreeSet::from([
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
        ]);
        let assignments = plan(
            &state,
            &voters,
            crate::scheduler::phase_offset_ms(id, 1_000).unwrap() + 10_000,
        );

        assert_eq!(assignments.len(), 3);
        assert_eq!(
            assignments
                .iter()
                .map(|assignment| assignment.executor_node_id)
                .collect::<BTreeSet<_>>()
                .len(),
            3,
        );
        assert!(
            assignments
                .iter()
                .all(|assignment| assignment.id == assignments[0].id)
        );
    }

    #[test]
    fn assignment_batches_do_not_split_an_evaluation() {
        let assignments = [31_u128, 32, 32, 32, 2]
            .into_iter()
            .enumerate()
            .flat_map(|(group, count)| {
                (0..count).map(move |index| EvaluationAssignment {
                    id: EvaluationId {
                        target_id: TargetId(Uuid::from_u128(group as u128 + 1)),
                        scheduled_at_ms: 1_000,
                    },
                    executor_node_id: Uuid::from_u128(group as u128 * 100 + index + 1),
                    assigned_at_ms: 1_000,
                    expires_at_ms: 2_000,
                    attempt: 1,
                })
            })
            .collect::<Vec<_>>();

        let batches = assignment_batches(&assignments);

        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![127, 2]
        );
    }
}
