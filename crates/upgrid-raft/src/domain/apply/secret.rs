use super::super::*;

impl ApplicationState {
    pub(super) fn delete_secret(
        &mut self,
        secret_id: SecretId,
    ) -> Result<CommandResult, DomainError> {
        if self.referenced_secret_ids().contains(&secret_id) {
            return Err(DomainError::InvalidSecret(
                "secret is still referenced by a Target or Notification Channel".to_owned(),
            ));
        }
        self.secrets
            .remove(&secret_id)
            .ok_or(DomainError::SecretNotFound(secret_id))?;
        Ok(CommandResult::SecretDeleted(secret_id))
    }

    pub(super) fn delete_unreferenced_secrets(&mut self) -> Result<CommandResult, DomainError> {
        let referenced = self.referenced_secret_ids();
        let deleted = self
            .secrets
            .keys()
            .filter(|id| !referenced.contains(id))
            .copied()
            .collect::<Vec<_>>();
        self.secrets.retain(|id, _| referenced.contains(id));
        Ok(CommandResult::UnreferencedSecretsDeleted(deleted))
    }
}
