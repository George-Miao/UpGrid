use std::collections::BTreeSet;

use super::{ApplicationState, SecretId};

impl ApplicationState {
    pub fn referenced_secret_ids(&self) -> BTreeSet<SecretId> {
        let mut referenced = BTreeSet::new();
        for target in self.targets.values() {
            referenced.extend(target.target.http.secret_ids());
        }
        for target in self.trashed_targets.values() {
            referenced.extend(target.state.target.http.secret_ids());
        }
        for channel in self.notification_channels.values() {
            referenced.extend(channel.secret_ids());
        }
        referenced
    }
}
