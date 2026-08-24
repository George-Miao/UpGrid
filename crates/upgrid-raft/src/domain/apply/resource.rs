use std::collections::BTreeSet;

use crate::domain::{
    ApplicationState, CommandResult, DomainError, MAX_EVALUATION_LOCATIONS, NodeTarget,
    NodeTargetState, NotificationChannel, Secret, Target, TargetId, TargetState,
};

impl ApplicationState {
    pub(super) fn create_notification_channel(
        &mut self,
        channel: NotificationChannel,
        generated_secret: Option<Secret>,
        is_default: bool,
    ) -> Result<CommandResult, DomainError> {
        if let Some(secret) = &generated_secret {
            secret.validate()?;
        }
        channel.validate()?;
        for secret_id in channel.secret_ids() {
            let is_generated = generated_secret
                .as_ref()
                .is_some_and(|secret| secret.id == secret_id);
            if !is_generated && !self.secrets.contains_key(&secret_id) {
                return Err(DomainError::SecretNotFound(secret_id));
            }
        }

        let id = channel.id;
        if let Some(secret) = generated_secret {
            self.secrets.insert(secret.id, secret);
        }
        self.notification_channels.insert(id, channel);
        if is_default {
            self.default_notification_channels.insert(id);
        } else {
            self.default_notification_channels.remove(&id);
        }
        Ok(CommandResult::NotificationChannelStored(id))
    }

    pub(super) fn update_notification_channel(
        &mut self,
        channel: NotificationChannel,
        generated_secret: Option<Secret>,
        is_default: bool,
    ) -> Result<CommandResult, DomainError> {
        let id = channel.id;
        if !self.notification_channels.contains_key(&id) {
            return Err(DomainError::NotificationChannelNotFound(id));
        }
        if let Some(secret) = &generated_secret {
            secret.validate()?;
        }
        channel.validate()?;
        for secret_id in channel.secret_ids() {
            let is_generated = generated_secret
                .as_ref()
                .is_some_and(|secret| secret.id == secret_id);
            if !is_generated && !self.secrets.contains_key(&secret_id) {
                return Err(DomainError::SecretNotFound(secret_id));
            }
        }

        if let Some(secret) = generated_secret {
            self.secrets.insert(secret.id, secret);
        }
        self.notification_channels.insert(id, channel);
        if is_default {
            self.default_notification_channels.insert(id);
        } else {
            self.default_notification_channels.remove(&id);
        }
        Ok(CommandResult::NotificationChannelUpdated(id))
    }

    pub(super) fn create_target(
        &mut self,
        target: Target,
        use_default_notifications: bool,
    ) -> Result<CommandResult, DomainError> {
        target.validate()?;
        if self.targets.contains_key(&target.id) || self.trashed_targets.contains_key(&target.id) {
            return Err(DomainError::TargetAlreadyExists(target.id));
        }
        self.validate_target_references(&target)?;

        let id = target.id;
        self.targets.insert(id, TargetState::new(target));
        self.set_target_default_notifications(id, use_default_notifications);
        Ok(CommandResult::TargetCreated(id))
    }

    pub(super) fn update_target(
        &mut self,
        target: Target,
        use_default_notifications: bool,
    ) -> Result<CommandResult, DomainError> {
        target.validate()?;
        self.validate_target_references(&target)?;
        let target_state = self
            .targets
            .get_mut(&target.id)
            .ok_or(DomainError::TargetNotFound(target.id))?;

        let id = target.id;
        target_state.target = target;
        self.set_target_default_notifications(id, use_default_notifications);
        Ok(CommandResult::TargetUpdated(id))
    }

    pub(super) fn create_target_with_locations(
        &mut self,
        target: Target,
        use_default_notifications: bool,
        locations: u16,
    ) -> Result<CommandResult, DomainError> {
        validate_locations(locations)?;
        let id = target.id;
        let result = self.create_target(target, use_default_notifications)?;
        self.target_locations.insert(id, locations);
        Ok(result)
    }

    pub(super) fn update_target_with_locations(
        &mut self,
        target: Target,
        use_default_notifications: bool,
        locations: u16,
    ) -> Result<CommandResult, DomainError> {
        validate_locations(locations)?;
        let id = target.id;
        let result = self.update_target(target, use_default_notifications)?;
        self.target_locations.insert(id, locations);
        Ok(result)
    }

    fn set_target_default_notifications(&mut self, id: TargetId, enabled: bool) {
        if enabled {
            self.default_notifications_disabled.remove(&id);
        } else {
            self.default_notifications_disabled.insert(id);
        }
    }

    fn validate_target_references(&self, target: &Target) -> Result<(), DomainError> {
        for channel_id in &target.notification_channels {
            if !self.notification_channels.contains_key(channel_id) {
                return Err(DomainError::NotificationChannelNotFound(*channel_id));
            }
        }
        for secret_id in target.http.secret_ids() {
            if !self.secrets.contains_key(&secret_id) {
                return Err(DomainError::SecretNotFound(secret_id));
            }
        }
        Ok(())
    }

    pub(super) fn sync_node_targets(
        &mut self,
        targets: Vec<NodeTarget>,
    ) -> Result<CommandResult, DomainError> {
        let mut ids = BTreeSet::new();
        for target in &targets {
            target.validate()?;
            if !ids.insert(target.id()) {
                return Err(DomainError::InvalidTarget(
                    "Node Target list contains duplicate Nodes".to_owned(),
                ));
            }
        }
        self.node_targets.retain(|id, _| ids.contains(id));
        for target in targets {
            let id = target.id();
            if let Some(state) = self.node_targets.get_mut(&id) {
                state.target = target;
            } else {
                self.node_targets.insert(id, NodeTargetState::new(target));
            }
        }
        Ok(CommandResult::NodeTargetsSynced)
    }
}

fn validate_locations(locations: u16) -> Result<(), DomainError> {
    if !(1..=MAX_EVALUATION_LOCATIONS).contains(&locations) {
        return Err(DomainError::InvalidTarget(format!(
            "evaluation locations must be between 1 and {MAX_EVALUATION_LOCATIONS}",
        )));
    }
    Ok(())
}
