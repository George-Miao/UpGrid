use std::io;

use super::core::*;
use super::version::*;
use crate::domain::{
    ApplicationState, DefaultChannelApplicationState, LegacyApplicationState,
    NamedApplicationState, PreAcknowledgementApplicationState, PreAssertionApplicationState,
    PreAuthApplicationState, PreDrainApplicationState, PreLocationApplicationState,
    PreRollupApplicationState, PreTlsApplicationState, PreTrashApplicationState,
    PreviousApplicationState, TokenApplicationState, TransitionApplicationState,
};

pub(super) fn decode_persisted(bytes: &[u8]) -> io::Result<PersistedStateMachine> {
    if let Some(bytes) = bytes.strip_prefix(STATE_MAGIC) {
        return postcard::from_bytes(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()));
    }
    if let Some(bytes) = bytes.strip_prefix(PRE_TRASH_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreTrashPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PRE_ROLLUP_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreRollupPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PRE_LOCATION_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreLocationPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PRE_TLS_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreTlsPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PRE_ASSERTION_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreAssertionPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PRE_ACKNOWLEDGEMENT_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreAcknowledgementPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PRE_DRAIN_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreDrainPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PRE_AUTH_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreAuthPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(DEFAULT_CHANNEL_STATE_MAGIC) {
        let previous = postcard::from_bytes::<DefaultChannelPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(TRANSITION_STATE_MAGIC) {
        let previous = postcard::from_bytes::<TransitionPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(NAMED_STATE_MAGIC) {
        let previous = postcard::from_bytes::<NamedPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(TOKEN_STATE_MAGIC) {
        let previous = postcard::from_bytes::<TokenPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    if let Some(bytes) = bytes.strip_prefix(PREVIOUS_STATE_MAGIC) {
        let previous = postcard::from_bytes::<PreviousPersistedStateMachine>(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        return Ok(PersistedStateMachine {
            state_machine: StateMachineData {
                last_applied_log: previous.state_machine.last_applied_log,
                last_membership: previous.state_machine.last_membership,
                application: previous.state_machine.application.into(),
            },
            current_snapshot: previous.current_snapshot,
            snapshot_idx: previous.snapshot_idx,
        });
    }
    let legacy = postcard::from_bytes::<LegacyPersistedStateMachine>(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    Ok(PersistedStateMachine {
        state_machine: StateMachineData {
            last_applied_log: legacy.state_machine.last_applied_log,
            last_membership: legacy.state_machine.last_membership,
            application: legacy.state_machine.application.into(),
        },
        current_snapshot: legacy.current_snapshot,
        snapshot_idx: legacy.snapshot_idx,
    })
}

pub(super) fn decode_application(bytes: &[u8]) -> Result<ApplicationState, postcard::Error> {
    if let Some(bytes) = bytes.strip_prefix(SNAPSHOT_MAGIC) {
        postcard::from_bytes(bytes)
    } else if let Some(bytes) = bytes.strip_prefix(PRE_TRASH_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreTrashApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PRE_ROLLUP_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreRollupApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PRE_LOCATION_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreLocationApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PRE_TLS_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreTlsApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PRE_ASSERTION_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreAssertionApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PRE_ACKNOWLEDGEMENT_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreAcknowledgementApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PRE_DRAIN_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreDrainApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PRE_AUTH_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreAuthApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(DEFAULT_CHANNEL_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<DefaultChannelApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(TRANSITION_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<TransitionApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(NAMED_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<NamedApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(TOKEN_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<TokenApplicationState>(bytes).map(Into::into)
    } else if let Some(bytes) = bytes.strip_prefix(PREVIOUS_SNAPSHOT_MAGIC) {
        postcard::from_bytes::<PreviousApplicationState>(bytes).map(Into::into)
    } else {
        postcard::from_bytes::<LegacyApplicationState>(bytes).map(Into::into)
    }
}
