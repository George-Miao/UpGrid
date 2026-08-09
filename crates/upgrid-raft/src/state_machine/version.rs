use openraft::{LogId, StoredMembership};
use serde::{Deserialize, Serialize};

use super::core::StoredSnapshot;
use crate::domain::{
    DefaultChannelApplicationState, LegacyApplicationState, NamedApplicationState,
    PreviousApplicationState, TokenApplicationState, TransitionApplicationState,
};
use crate::raft::TC;

macro_rules! version {
    ($data:ident, $persisted:ident, $application:ty) => {
        #[derive(Serialize, Deserialize)]
        pub(super) struct $data {
            pub(super) last_applied_log: Option<LogId<TC>>,
            pub(super) last_membership: StoredMembership<TC>,
            pub(super) application: $application,
        }

        #[derive(Serialize, Deserialize)]
        pub(super) struct $persisted {
            pub(super) state_machine: $data,
            pub(super) current_snapshot: Option<StoredSnapshot>,
            pub(super) snapshot_idx: u64,
        }
    };
}

version!(
    DefaultChannelStateMachineData,
    DefaultChannelPersistedStateMachine,
    DefaultChannelApplicationState
);
version!(
    TransitionStateMachineData,
    TransitionPersistedStateMachine,
    TransitionApplicationState
);
version!(
    NamedStateMachineData,
    NamedPersistedStateMachine,
    NamedApplicationState
);
version!(
    LegacyStateMachineData,
    LegacyPersistedStateMachine,
    LegacyApplicationState
);
version!(
    PreviousStateMachineData,
    PreviousPersistedStateMachine,
    PreviousApplicationState
);
version!(
    TokenStateMachineData,
    TokenPersistedStateMachine,
    TokenApplicationState
);
