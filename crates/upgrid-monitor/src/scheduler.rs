//! Deterministic Target evaluation scheduling.

use std::collections::BTreeSet;

use upgrid_raft::domain::{EvaluationId, TargetId};
use uuid::Uuid;

/// Returns the stable phase assigned to a Target within an interval.
pub fn phase_offset_ms(target_id: TargetId, interval_ms: u64) -> Option<u64> {
    if interval_ms == 0 {
        return None;
    }
    let value = target_id.0.as_u128();
    let folded = (value as u64) ^ ((value >> 64) as u64);
    Some(folded % interval_ms)
}

/// Returns the first scheduled slot strictly after `after_ms`.
///
/// Missed slots are not enumerated, which prevents catch-up bursts after leader
/// failover or Cluster downtime.
#[cfg(test)]
pub fn next_slot_after_ms(target_id: TargetId, interval_ms: u64, after_ms: u64) -> Option<u64> {
    let phase = phase_offset_ms(target_id, interval_ms)?;
    if after_ms < phase {
        return Some(phase);
    }

    let completed_intervals = (after_ms - phase) / interval_ms;
    phase.checked_add(
        completed_intervals
            .checked_add(1)?
            .checked_mul(interval_ms)?,
    )
}

/// Returns the latest scheduled slot at or before the supplied cluster time.
///
/// Selecting one current slot instead of enumerating earlier slots prevents a
/// catch-up burst after downtime or a leader change.
pub fn slot_at_or_before_ms(target_id: TargetId, interval_ms: u64, now_ms: u64) -> Option<u64> {
    let phase = phase_offset_ms(target_id, interval_ms)?;
    if now_ms < phase {
        return None;
    }
    phase.checked_add(((now_ms - phase) / interval_ms).checked_mul(interval_ms)?)
}

/// Selects up to `count` distinct executors from already-filtered eligible
/// Nodes.
///
/// Sorting and a stable rotation make the result independent of membership
/// iteration order while spreading the first executor across the Cluster.
pub fn select_executors(
    evaluation_id: EvaluationId,
    eligible_nodes: impl IntoIterator<Item = Uuid>,
    count: usize,
) -> BTreeSet<Uuid> {
    let mut nodes = eligible_nodes.into_iter().collect::<Vec<_>>();
    nodes.sort_unstable();
    nodes.dedup();
    if nodes.is_empty() || count == 0 {
        return BTreeSet::new();
    }

    let target = evaluation_id.target_id.0.as_u128();
    let scheduled = evaluation_id.scheduled_at_ms as u128;
    let mixed = target ^ scheduled.rotate_left(37);
    let folded = (mixed as u64) ^ ((mixed >> 64) as u64);
    let start = (folded as usize) % nodes.len();
    (0..count.min(nodes.len()))
        .map(|offset| nodes[(start + offset) % nodes.len()])
        .collect()
}

pub fn select_executor(
    evaluation_id: EvaluationId,
    eligible_nodes: impl IntoIterator<Item = Uuid>,
) -> Option<Uuid> {
    select_executors(evaluation_id, eligible_nodes, 1)
        .into_iter()
        .next()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_is_stable_and_within_interval() {
        let target_id = TargetId(Uuid::from_u128(0x1234_5678_90ab_cdef));
        let first = phase_offset_ms(target_id, 60_000).unwrap();
        let second = phase_offset_ms(target_id, 60_000).unwrap();

        assert_eq!(first, second);
        assert!(first < 60_000);
    }

    #[test]
    fn next_slot_skips_missed_intervals() {
        let target_id = TargetId(Uuid::from_u128(42));
        let phase = phase_offset_ms(target_id, 1_000).unwrap();

        assert_eq!(
            next_slot_after_ms(target_id, 1_000, phase + 10_250),
            Some(phase + 11_000)
        );
    }

    #[test]
    fn current_slot_does_not_replay_missed_intervals() {
        let target_id = TargetId(Uuid::from_u128(42));
        let phase = phase_offset_ms(target_id, 1_000).unwrap();

        assert_eq!(
            slot_at_or_before_ms(target_id, 1_000, phase + 10_250),
            Some(phase + 10_000)
        );
    }

    #[test]
    fn executor_selection_is_independent_of_input_order() {
        let evaluation_id = EvaluationId {
            target_id: TargetId(Uuid::from_u128(7)),
            scheduled_at_ms: 12_000,
        };
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let c = Uuid::from_u128(3);

        assert_eq!(
            select_executor(evaluation_id, [a, b, c]),
            select_executor(evaluation_id, [c, a, b])
        );
    }

    #[test]
    fn invalid_interval_and_empty_membership_have_no_schedule() {
        let target_id = TargetId(Uuid::from_u128(1));
        let evaluation_id = EvaluationId {
            target_id,
            scheduled_at_ms: 0,
        };

        assert_eq!(next_slot_after_ms(target_id, 0, 0), None);
        assert_eq!(select_executor(evaluation_id, []), None);
    }
}
