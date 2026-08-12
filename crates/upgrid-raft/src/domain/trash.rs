use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{EvaluationRollup, TargetId, TargetState};

pub const DEFAULT_TARGET_TRASH_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrashedTarget {
    pub state: TargetState,
    pub locations: u16,
    pub use_default_notifications: bool,
    pub history_rollups: BTreeMap<u64, EvaluationRollup>,
    pub deleted_at_ms: u64,
}

impl TrashedTarget {
    pub fn id(&self) -> TargetId {
        self.state.target.id
    }

    pub fn purge_at_ms(&self, retention_ms: u64) -> u64 {
        self.deleted_at_ms.saturating_add(retention_ms)
    }

    pub(super) fn expired(&self, retention_ms: u64, now_ms: u64) -> bool {
        self.purge_at_ms(retention_ms) <= now_ms
    }
}
