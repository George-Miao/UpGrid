use super::super::*;

impl ApplicationState {
    pub(super) fn hard_delete_target(
        &mut self,
        target_id: TargetId,
    ) -> Result<CommandResult, DomainError> {
        self.targets
            .remove(&target_id)
            .ok_or(DomainError::TargetNotFound(target_id))?;
        self.release_target_runtime(target_id);
        self.target_locations.remove(&target_id);
        self.history_rollups.remove(&target_id);
        self.default_notifications_disabled.remove(&target_id);
        Ok(CommandResult::TargetDeleted(target_id))
    }

    pub(super) fn trash_target(
        &mut self,
        target_id: TargetId,
        deleted_at_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        self.remove_expired_target_trash(deleted_at_ms);
        let state = self
            .targets
            .remove(&target_id)
            .ok_or(DomainError::TargetNotFound(target_id))?;
        self.release_target_runtime(target_id);
        let target = TrashedTarget {
            state,
            locations: self.target_locations.remove(&target_id).unwrap_or(1),
            use_default_notifications: !self.default_notifications_disabled.remove(&target_id),
            history_rollups: self.history_rollups.remove(&target_id).unwrap_or_default(),
            deleted_at_ms,
        };
        self.trashed_targets.insert(target_id, target);
        Ok(CommandResult::TargetTrashed(target_id))
    }

    pub(super) fn restore_target(
        &mut self,
        target_id: TargetId,
        restored_at_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        self.remove_expired_target_trash(restored_at_ms);
        if self.targets.contains_key(&target_id) {
            return Err(DomainError::TargetAlreadyExists(target_id));
        }
        let target = self
            .trashed_targets
            .remove(&target_id)
            .ok_or(DomainError::TrashedTargetNotFound(target_id))?;
        self.targets.insert(target_id, target.state);
        self.target_locations.insert(target_id, target.locations);
        if !target.use_default_notifications {
            self.default_notifications_disabled.insert(target_id);
        }
        if !target.history_rollups.is_empty() {
            self.history_rollups
                .insert(target_id, target.history_rollups);
        }
        Ok(CommandResult::TargetRestored(target_id))
    }

    pub(super) fn purge_target(
        &mut self,
        target_id: TargetId,
    ) -> Result<CommandResult, DomainError> {
        self.trashed_targets
            .remove(&target_id)
            .ok_or(DomainError::TrashedTargetNotFound(target_id))?;
        Ok(CommandResult::TargetPurged(target_id))
    }

    pub(super) fn set_target_trash_retention(
        &mut self,
        retention_ms: u64,
        now_ms: u64,
    ) -> Result<CommandResult, DomainError> {
        if retention_ms == 0 {
            return Err(DomainError::InvalidTarget(
                "Target trash retention must be greater than zero".to_owned(),
            ));
        }
        self.target_trash_retention_ms = retention_ms;
        self.remove_expired_target_trash(now_ms);
        Ok(CommandResult::TargetTrashRetentionSet(retention_ms))
    }

    fn release_target_runtime(&mut self, target_id: TargetId) {
        self.assignments
            .retain(|key, _| key.id.target_id != target_id);
        self.evaluation_batches
            .retain(|id, _| id.target_id != target_id);
    }

    pub(super) fn prune_target_trash(&mut self, now_ms: u64) -> Result<CommandResult, DomainError> {
        let removed = self.remove_expired_target_trash(now_ms);
        Ok(CommandResult::TargetTrashPruned(removed as u64))
    }

    fn remove_expired_target_trash(&mut self, now_ms: u64) -> usize {
        let retention_ms = self.target_trash_retention_ms;
        let before = self.trashed_targets.len();
        self.trashed_targets
            .retain(|_, target| !target.expired(retention_ms, now_ms));
        before - self.trashed_targets.len()
    }
}
