use std::collections::BTreeSet;
use std::ops::Range;
use std::path::PathBuf;
use std::rc::Rc;
use std::{fs, io};

use openraft::EntryPayload;
use openraft::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, SnapshotMetaOf, VoteOf};
use url::Url;
use uuid::Uuid;

use crate::database::{LogRepository, RaftDatabase, StateRepository};
use crate::domain::{ApplicationState, Command, EvaluationPolicy, HttpTarget, Target, TargetId};
use crate::raft::TC;
use crate::state_machine::{StateMachineData, StoredSnapshot, encode_snapshot};

#[doc(hidden)]
pub struct PersistenceBench {
    directory: PathBuf,
    log: Option<LogRepository>,
    state: Option<StateRepository>,
    database: Option<Rc<RaftDatabase>>,
    leader: CommittedLeaderIdOf<TC>,
    state_value: StateMachineData,
    snapshot: StoredSnapshot,
}

impl PersistenceBench {
    pub fn new(target_count: usize) -> io::Result<Self> {
        let directory = std::env::temp_dir().join(format!("upgrid-bench-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory)?;
        let database = Rc::new(RaftDatabase::open(&directory).map_err(io::Error::from)?);
        let leader = CommittedLeaderIdOf::<TC> {
            term: 1,
            node_id: Uuid::now_v7(),
        };
        let mut application = ApplicationState::default();
        for index in 0..target_count {
            application
                .apply(Command::CreateTarget {
                    target: Target {
                        id: TargetId(Uuid::now_v7()),
                        name: format!("Target {index}"),
                        http: HttpTarget::get(
                            Url::parse(&format!("https://target-{index}.example.com/health"))
                                .expect("benchmark URL must be valid"),
                        ),
                        policy: EvaluationPolicy::default(),
                        notification_channels: BTreeSet::new(),
                    },
                    use_default_notifications: true,
                })
                .expect("benchmark Target must be valid");
        }
        let state_value = StateMachineData {
            last_applied_log: Some(LogIdOf::<TC>::new(leader, 256)),
            application,
            ..StateMachineData::default()
        };
        let snapshot = StoredSnapshot {
            meta: SnapshotMetaOf::<TC> {
                last_log_id: state_value.last_applied_log,
                last_membership: state_value.last_membership.clone(),
            },
            data: encode_snapshot(&state_value.application)?,
        };

        Ok(Self {
            directory,
            log: Some(LogRepository::new(database.clone())),
            state: Some(StateRepository::new(database.clone())),
            database: Some(database),
            leader,
            state_value,
            snapshot,
        })
    }

    pub fn seed_log(&self, count: u64) -> io::Result<()> {
        self.log()
            .append::<TC>(&self.entries(count))
            .map_err(io::Error::from)
    }

    pub fn append(&self, count: u64) -> io::Result<()> {
        self.seed_log(count)
    }

    pub fn range_read(&self, range: Range<u64>) -> io::Result<usize> {
        let store = self.log().load::<TC>().map_err(io::Error::from)?;
        Ok(store.log.range(range).count())
    }

    pub fn conflict_delete(&self, last_index: u64) -> io::Result<()> {
        self.log()
            .truncate_after::<TC>(Some(LogIdOf::<TC>::new(self.leader, last_index)))
            .map_err(io::Error::from)
    }

    pub fn purge(&self, index: u64) -> io::Result<()> {
        self.log()
            .purge::<TC>(&LogIdOf::<TC>::new(self.leader, index))
            .map_err(io::Error::from)
    }

    pub fn update_vote(&self) -> io::Result<()> {
        self.log()
            .save_vote::<TC>(&VoteOf::<TC>::new(2, self.leader.node_id))
            .map_err(io::Error::from)
    }

    pub fn update_committed(&self) -> io::Result<()> {
        self.log()
            .save_committed::<TC>(Some(&LogIdOf::<TC>::new(self.leader, 128)))
            .map_err(io::Error::from)
    }

    pub fn write_checkpoint(&self) -> io::Result<()> {
        self.state()
            .replace(&self.state_value, None, 1)
            .map_err(io::Error::from)
    }

    pub fn replace_snapshot(&self) -> io::Result<()> {
        self.state()
            .replace(&self.state_value, Some(&self.snapshot), 2)
            .map_err(io::Error::from)
    }

    pub fn read_state(&self) -> io::Result<usize> {
        let (state, snapshot, _) = self.state().load().map_err(io::Error::from)?;
        Ok(state.application.targets.len() + snapshot.map_or(0, |snapshot| snapshot.data.len()))
    }

    fn entries(&self, count: u64) -> Vec<EntryOf<TC>> {
        (1..=count)
            .map(|index| EntryOf::<TC> {
                log_id: LogIdOf::<TC>::new(self.leader, index),
                payload: EntryPayload::Blank,
            })
            .collect()
    }

    fn log(&self) -> &LogRepository {
        self.log
            .as_ref()
            .expect("benchmark repository must remain open")
    }

    fn state(&self) -> &StateRepository {
        self.state
            .as_ref()
            .expect("benchmark repository must remain open")
    }
}

impl Drop for PersistenceBench {
    fn drop(&mut self) {
        self.log.take();
        self.state.take();
        self.database.take();
        let _ = fs::remove_dir_all(&self.directory);
    }
}
