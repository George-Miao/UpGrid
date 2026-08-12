use serde::{Deserialize, Serialize};

use super::{ApplicationState, Evaluation};

pub const HISTORY_ROLLUP_INTERVAL_MS: u64 = 60 * 60 * 1_000;
pub const DEFAULT_HISTORY_ROLLUP_RETENTION_MS: u64 = 365 * 24 * HISTORY_ROLLUP_INTERVAL_MS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationRollup {
    pub bucket_start_ms: u64,
    pub samples: u64,
    pub successes: u64,
    pub failures: u64,
    pub latency_total_ms: u64,
    pub latency_min_ms: u64,
    pub latency_max_ms: u64,
}

impl EvaluationRollup {
    pub(super) fn from_evaluation(evaluation: &Evaluation) -> Self {
        let latency_ms = evaluation.http.latency_ms;
        Self {
            bucket_start_ms: bucket_start(evaluation.id.scheduled_at_ms),
            samples: 1,
            successes: u64::from(evaluation.succeeded),
            failures: u64::from(!evaluation.succeeded),
            latency_total_ms: latency_ms,
            latency_min_ms: latency_ms,
            latency_max_ms: latency_ms,
        }
    }

    pub(super) fn record(&mut self, evaluation: &Evaluation) {
        self.samples = self.samples.saturating_add(1);
        if evaluation.succeeded {
            self.successes = self.successes.saturating_add(1);
        } else {
            self.failures = self.failures.saturating_add(1);
        }
        self.latency_total_ms = self
            .latency_total_ms
            .saturating_add(evaluation.http.latency_ms);
        self.latency_min_ms = self.latency_min_ms.min(evaluation.http.latency_ms);
        self.latency_max_ms = self.latency_max_ms.max(evaluation.http.latency_ms);
    }
}

pub(super) fn bucket_start(timestamp_ms: u64) -> u64 {
    timestamp_ms / HISTORY_ROLLUP_INTERVAL_MS * HISTORY_ROLLUP_INTERVAL_MS
}

impl ApplicationState {
    pub(super) fn record_history_rollup(&mut self, evaluation: &Evaluation) {
        let bucket_start_ms = bucket_start(evaluation.id.scheduled_at_ms);
        let rollups = self
            .history_rollups
            .entry(evaluation.id.target_id)
            .or_default();
        rollups
            .entry(bucket_start_ms)
            .and_modify(|rollup| rollup.record(evaluation))
            .or_insert_with(|| EvaluationRollup::from_evaluation(evaluation));

        let cutoff = bucket_start(
            evaluation
                .recorded_at_ms
                .saturating_sub(self.history_rollup_retention_ms),
        );
        rollups.retain(|bucket_start_ms, _| *bucket_start_ms >= cutoff);
    }
}
