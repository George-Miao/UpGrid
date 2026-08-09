use std::collections::BTreeSet;
use std::time::Duration;

use compio::time::sleep;
use upgrid_config::now_ms;
use upgrid_raft::Handle;
use upgrid_raft::domain::{ApplicationState, Command, EvaluationAssignment, EvaluationId, Target};

use crate::scheduler::{select_executor, slot_at_or_before_ms};

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

fn plan(
    state: &ApplicationState,
    voters: &BTreeSet<uuid::Uuid>,
    now_ms: u64,
) -> Vec<EvaluationAssignment> {
    state
        .targets
        .values()
        .filter(|target| !target.paused)
        .filter_map(|target| {
            let existing = state
                .assignments
                .values()
                .find(|assignment| assignment.id.target_id == target.target.id);
            if let Some(existing) = existing {
                if existing.expires_at_ms > now_ms {
                    return None;
                }
                let mut candidates = voters.clone();
                if candidates.len() > 1 {
                    candidates.remove(&existing.executor_node_id);
                }
                return Some(EvaluationAssignment {
                    id: existing.id,
                    executor_node_id: select_executor(existing.id, candidates)?,
                    assigned_at_ms: now_ms,
                    expires_at_ms: expiry(&target.target, now_ms),
                    attempt: existing.attempt.saturating_add(1),
                });
            }

            let scheduled_at_ms =
                slot_at_or_before_ms(target.target.id, target.target.policy.interval_ms, now_ms)?;
            if target
                .latest_evaluation
                .as_ref()
                .is_some_and(|latest| latest.id.scheduled_at_ms >= scheduled_at_ms)
            {
                return None;
            }
            let id = EvaluationId {
                target_id: target.target.id,
                scheduled_at_ms,
            };
            Some(EvaluationAssignment {
                id,
                executor_node_id: select_executor(id, voters.iter().copied())?,
                assigned_at_ms: now_ms,
                expires_at_ms: expiry(&target.target, now_ms),
                attempt: 1,
            })
        })
        .collect()
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
        state.apply(Command::CreateTarget(target)).unwrap();
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
    }
}
