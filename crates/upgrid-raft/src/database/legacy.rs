use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::{fs, io, str};

use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, StoredMembershipOf, VoteOf};
use redb::{ReadOnlyDatabase, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};

use crate::domain::{ApplicationState, ApplicationStateV20260812};
use crate::error::DatabaseError;
use crate::raft::TC;
use crate::state_machine::{StateMachineData, StoredSnapshot};

const LOG_REDB_FILE: &str = "raft-log.redb";
const LOG_POSTCARD_FILE: &str = "raft-log.postcard";
const STATE_POSTCARD_FILE: &str = "raft-state.postcard";
const VERSION_TERMINATOR: u8 = b'\n';
const CURRENT_STATE_VERSION: &str = "v2026_08_12_initial";
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META_TABLE: TableDefinition<u8, &[u8]> = TableDefinition::new("raft_meta");
const META_KEY: u8 = 0;

pub(super) struct Recovered {
    pub(super) log: LegacyInMemStore,
    pub(super) state: LegacyPersistedStateMachine,
}

#[derive(Default, Deserialize)]
pub(super) struct LegacyStoreMeta {
    pub(super) last_purged_log_id: Option<LogIdOf<TC>>,
    pub(super) committed: Option<LogIdOf<TC>>,
    pub(super) vote: Option<VoteOf<TC>>,
}

#[derive(Default, Deserialize)]
pub(super) struct LegacyInMemStore {
    pub(super) last_purged_log_id: Option<LogIdOf<TC>>,
    pub(super) log: BTreeMap<u64, EntryOf<TC>>,
    pub(super) committed: Option<LogIdOf<TC>>,
    pub(super) vote: Option<VoteOf<TC>>,
}

#[derive(Default, Deserialize)]
pub(super) struct LegacyStateMachineData {
    pub(super) last_applied_log: Option<LogIdOf<TC>>,
    pub(super) last_membership: StoredMembershipOf<TC>,
    pub(super) application: ApplicationStateV20260812,
}

#[derive(Deserialize)]
pub(super) struct LegacyStoredSnapshot {
    pub(super) meta: SnapshotMetaOf<TC>,
    pub(super) data: Vec<u8>,
}

#[derive(Default, Deserialize)]
pub(super) struct LegacyPersistedStateMachine {
    pub(super) state_machine: LegacyStateMachineData,
    pub(super) current_snapshot: Option<LegacyStoredSnapshot>,
    pub(super) snapshot_idx: u64,
}

impl LegacyPersistedStateMachine {
    pub(super) fn runtime(self) -> (StateMachineData, Option<StoredSnapshot>, u64) {
        let mut application = ApplicationState::from(self.state_machine.application);
        application.normalize_alert_ids();
        (
            StateMachineData {
                last_applied_log: self.state_machine.last_applied_log,
                last_membership: self.state_machine.last_membership,
                application,
            },
            self.current_snapshot.map(|snapshot| StoredSnapshot {
                meta: snapshot.meta,
                data: snapshot.data,
            }),
            self.snapshot_idx,
        )
    }
}

pub(super) fn recover(data_dir: &Path) -> Result<Recovered, DatabaseError> {
    let redb_path = data_dir.join(LOG_REDB_FILE);
    let postcard_log_path = data_dir.join(LOG_POSTCARD_FILE);
    let state_path = data_dir.join(STATE_POSTCARD_FILE);
    let log = if redb_path.exists() {
        read_redb(&redb_path)?
    } else if postcard_log_path.exists() {
        read_postcard_log(&postcard_log_path)?
    } else {
        LegacyInMemStore::default()
    };
    let state = if state_path.exists() {
        read_state(&state_path)?
    } else {
        LegacyPersistedStateMachine::default()
    };

    Ok(Recovered { log, state })
}

fn read_redb(path: &Path) -> Result<LegacyInMemStore, DatabaseError> {
    let database =
        ReadOnlyDatabase::open(path).map_err(|source| redb_error(path, source.into()))?;
    let transaction = database
        .begin_read()
        .map_err(|source| redb_error(path, source.into()))?;
    let meta_table = transaction
        .open_table(META_TABLE)
        .map_err(|source| redb_error(path, source.into()))?;
    let meta = match meta_table
        .get(META_KEY)
        .map_err(|source| redb_error(path, source.into()))?
    {
        Some(value) => {
            postcard::from_bytes::<LegacyStoreMeta>(value.value()).map_err(|source| {
                DatabaseError::LegacyPostcard {
                    path: path.to_path_buf(),
                    source,
                }
            })?
        }
        None => LegacyStoreMeta::default(),
    };
    let log_table = transaction
        .open_table(LOG_TABLE)
        .map_err(|source| redb_error(path, source.into()))?;
    let mut log = BTreeMap::new();
    for item in log_table
        .iter()
        .map_err(|source| redb_error(path, source.into()))?
    {
        let (index, value) = item.map_err(|source| redb_error(path, source.into()))?;
        let entry = postcard::from_bytes(value.value()).map_err(|source| {
            DatabaseError::LegacyPostcard {
                path: path.to_path_buf(),
                source,
            }
        })?;
        log.insert(index.value(), entry);
    }

    Ok(LegacyInMemStore {
        last_purged_log_id: meta.last_purged_log_id,
        log,
        committed: meta.committed,
        vote: meta.vote,
    })
}

fn read_postcard_log(path: &Path) -> Result<LegacyInMemStore, DatabaseError> {
    let bytes = read_file(path)?;
    postcard::from_bytes(&bytes).map_err(|source| DatabaseError::LegacyPostcard {
        path: path.to_path_buf(),
        source,
    })
}

fn read_state(path: &Path) -> Result<LegacyPersistedStateMachine, DatabaseError> {
    let bytes = read_file(path)?;
    let (version, payload) = split(&bytes).map_err(|source| DatabaseError::LegacyRead {
        path: path.to_path_buf(),
        source: io::Error::new(io::ErrorKind::InvalidData, source),
    })?;
    if version != CURRENT_STATE_VERSION {
        return Err(DatabaseError::LegacyRead {
            path: path.to_path_buf(),
            source: io::Error::new(
                io::ErrorKind::InvalidData,
                FormatError::Unsupported {
                    version: version.to_owned(),
                },
            ),
        });
    }
    postcard::from_bytes(payload).map_err(|source| DatabaseError::LegacyPostcard {
        path: path.to_path_buf(),
        source,
    })
}

fn read_file(path: &Path) -> Result<Vec<u8>, DatabaseError> {
    fs::read(path).map_err(|source| DatabaseError::LegacyRead {
        path: path.to_path_buf(),
        source,
    })
}

fn split(bytes: &[u8]) -> Result<(&str, &[u8]), FormatError> {
    let terminator = bytes
        .iter()
        .position(|byte| *byte == VERSION_TERMINATOR)
        .context(MissingHeaderSnafu)?;
    let (version, payload) = bytes.split_at(terminator);
    let version = str::from_utf8(version).context(InvalidUtf8Snafu)?;
    Ok((version, &payload[1..]))
}

fn redb_error(path: &Path, source: redb::Error) -> DatabaseError {
    DatabaseError::LegacyRedb {
        path: PathBuf::from(path),
        source,
    }
}

#[derive(Debug, Snafu)]
enum FormatError {
    #[snafu(display("legacy state-machine data has no version string"))]
    MissingHeader,

    #[snafu(display("legacy state-machine version is not UTF-8: {source}"))]
    InvalidUtf8 { source: str::Utf8Error },

    #[snafu(display("unsupported legacy state-machine version `{version}`"))]
    Unsupported { version: String },
}
