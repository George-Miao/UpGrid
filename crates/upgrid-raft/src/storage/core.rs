use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
use std::{fs, io};

use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::{LogState, RaftTypeConfig};
use openraft_rt_compio::futures::lock::Mutex;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META_TABLE: TableDefinition<u8, &[u8]> = TableDefinition::new("raft_meta");
const META_KEY: u8 = 0;

/// A durable Raft log backed by redb and mirrored in memory for fast reads.
#[derive(Clone, Debug, Default)]
pub struct InMemStore<C: RaftTypeConfig> {
    pub(super) inner: Arc<Mutex<InMemStoreInner<C>>>,
    database: Option<Arc<Database>>,
}

impl<C: RaftTypeConfig> InMemStore<C> {
    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemStoreInner::default())),
            database: None,
        }
    }

    pub fn open(path: impl AsRef<Path>, legacy_path: impl AsRef<Path>) -> io::Result<Self>
    where
        C::Entry: DeserializeOwned + Serialize,
        LogIdOf<C>: DeserializeOwned + Serialize,
        VoteOf<C>: DeserializeOwned + Serialize,
    {
        let database = Arc::new(Database::create(path).map_err(io_error)?);
        initialize_tables(&database)?;
        let (mut inner, initialized) = load(&database)?;

        if !initialized {
            match fs::read(legacy_path) {
                Ok(bytes) => {
                    inner = postcard::from_bytes(&bytes).map_err(invalid_data)?;
                    persist_all(&database, &inner)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    persist_all(&database, &inner)?;
                }
                Err(error) => return Err(error),
            }
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            database: Some(database),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "C::Entry: Serialize, LogIdOf<C>: Serialize, VoteOf<C>: Serialize",
    deserialize = "C::Entry: DeserializeOwned, LogIdOf<C>: DeserializeOwned, VoteOf<C>: \
                   DeserializeOwned"
))]
pub(super) struct InMemStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    pub(super) last_purged_log_id: Option<LogIdOf<C>>,

    /// The Raft log.
    pub(super) log: BTreeMap<u64, C::Entry>,

    /// The commit log id.
    pub(super) committed: Option<LogIdOf<C>>,

    /// The current granted vote.
    pub(super) vote: Option<VoteOf<C>>,
}

impl<C: RaftTypeConfig> Default for InMemStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> InMemStoreInner<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<C::Entry>>
    where
        C::Entry: Clone,
    {
        Ok(self
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    async fn get_log_state(&mut self) -> io::Result<LogState<C>> {
        let last_log_id = self
            .log
            .iter()
            .next_back()
            .map(|(_, entry)| entry.log_id())
            .or_else(|| self.last_purged_log_id.clone());

        Ok(LogState {
            last_purged_log_id: self.last_purged_log_id.clone(),
            last_log_id,
        })
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<C>>> {
        Ok(self.committed.clone())
    }

    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<C>>> {
        Ok(self.vote.clone())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "LogIdOf<C>: Serialize, VoteOf<C>: Serialize",
    deserialize = "LogIdOf<C>: DeserializeOwned, VoteOf<C>: DeserializeOwned"
))]
struct StoreMeta<C: RaftTypeConfig> {
    last_purged_log_id: Option<LogIdOf<C>>,
    committed: Option<LogIdOf<C>>,
    vote: Option<VoteOf<C>>,
}

impl<C: RaftTypeConfig> Default for StoreMeta<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> From<&InMemStoreInner<C>> for StoreMeta<C> {
    fn from(inner: &InMemStoreInner<C>) -> Self {
        Self {
            last_purged_log_id: inner.last_purged_log_id.clone(),
            committed: inner.committed.clone(),
            vote: inner.vote.clone(),
        }
    }
}

fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn initialize_tables(database: &Database) -> io::Result<()> {
    let transaction = database.begin_write().map_err(io_error)?;
    transaction.open_table(LOG_TABLE).map_err(io_error)?;
    transaction.open_table(META_TABLE).map_err(io_error)?;
    transaction.commit().map_err(io_error)
}

fn load<C>(database: &Database) -> io::Result<(InMemStoreInner<C>, bool)>
where
    C: RaftTypeConfig,
    C::Entry: DeserializeOwned,
    LogIdOf<C>: DeserializeOwned,
    VoteOf<C>: DeserializeOwned,
{
    let transaction = database.begin_read().map_err(io_error)?;
    let meta_table = transaction.open_table(META_TABLE).map_err(io_error)?;
    let (meta, mut initialized) = match meta_table.get(META_KEY).map_err(io_error)? {
        Some(value) => (
            postcard::from_bytes::<StoreMeta<C>>(value.value()).map_err(invalid_data)?,
            true,
        ),
        None => (StoreMeta::default(), false),
    };
    let log_table = transaction.open_table(LOG_TABLE).map_err(io_error)?;
    let mut log = BTreeMap::new();
    for item in log_table.iter().map_err(io_error)? {
        let (index, value) = item.map_err(io_error)?;
        let entry = postcard::from_bytes(value.value()).map_err(invalid_data)?;
        log.insert(index.value(), entry);
    }
    initialized |= !log.is_empty();

    Ok((
        InMemStoreInner {
            last_purged_log_id: meta.last_purged_log_id,
            log,
            committed: meta.committed,
            vote: meta.vote,
        },
        initialized,
    ))
}

fn encode_meta<C>(meta: &StoreMeta<C>) -> io::Result<Vec<u8>>
where
    C: RaftTypeConfig,
    LogIdOf<C>: Serialize,
    VoteOf<C>: Serialize,
{
    postcard::to_stdvec(meta).map_err(io_error)
}

fn persist_all<C>(database: &Database, inner: &InMemStoreInner<C>) -> io::Result<()>
where
    C: RaftTypeConfig,
    C::Entry: Serialize,
    LogIdOf<C>: Serialize,
    VoteOf<C>: Serialize,
{
    let entries = inner
        .log
        .iter()
        .map(|(index, entry)| Ok((*index, postcard::to_stdvec(entry).map_err(io_error)?)))
        .collect::<io::Result<Vec<_>>>()?;
    let meta = encode_meta(&StoreMeta::from(inner))?;
    let transaction = database.begin_write().map_err(io_error)?;
    {
        let mut table = transaction.open_table(LOG_TABLE).map_err(io_error)?;
        for (index, entry) in &entries {
            table.insert(index, entry.as_slice()).map_err(io_error)?;
        }
    }
    transaction
        .open_table(META_TABLE)
        .map_err(io_error)?
        .insert(META_KEY, meta.as_slice())
        .map_err(io_error)?;
    transaction.commit().map_err(io_error)
}

fn persist_meta<C>(database: &Database, meta: &StoreMeta<C>) -> io::Result<()>
where
    C: RaftTypeConfig,
    LogIdOf<C>: Serialize,
    VoteOf<C>: Serialize,
{
    let bytes = encode_meta(meta)?;
    let transaction = database.begin_write().map_err(io_error)?;
    transaction
        .open_table(META_TABLE)
        .map_err(io_error)?
        .insert(META_KEY, bytes.as_slice())
        .map_err(io_error)?;
    transaction.commit().map_err(io_error)
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::io;
    use std::ops::RangeBounds;

    use openraft::alias::{LogIdOf, VoteOf};
    use openraft::entry::RaftEntry;
    use openraft::storage::{IOFlushed, RaftLogStorage};
    use openraft::{LogState, RaftLogReader, RaftTypeConfig};
    use serde::Serialize;

    use super::{InMemStore, LOG_TABLE, META_TABLE, StoreMeta, io_error, persist_meta};

    impl<C: RaftTypeConfig> RaftLogReader<C> for InMemStore<C>
    where
        C::Entry: Clone + Serialize,
        LogIdOf<C>: Serialize,
        VoteOf<C>: Serialize,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> io::Result<Vec<C::Entry>> {
            self.inner.lock().await.try_get_log_entries(range).await
        }

        async fn read_vote(&mut self) -> io::Result<Option<VoteOf<C>>> {
            self.inner.lock().await.read_vote().await
        }
    }

    impl<C: RaftTypeConfig> RaftLogStorage<C> for InMemStore<C>
    where
        C::Entry: Clone + Serialize,
        LogIdOf<C>: Serialize,
        VoteOf<C>: Serialize,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> io::Result<LogState<C>> {
            self.inner.lock().await.get_log_state().await
        }

        async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> io::Result<()> {
            let mut inner = self.inner.lock().await;
            let mut meta = StoreMeta::from(&*inner);
            meta.committed = committed.clone();
            if let Some(database) = &self.database {
                persist_meta(database, &meta)?;
            }
            inner.committed = committed;
            Ok(())
        }

        async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<C>>> {
            self.inner.lock().await.read_committed().await
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> io::Result<()> {
            let mut inner = self.inner.lock().await;
            let mut meta = StoreMeta::from(&*inner);
            meta.vote = Some(vote.clone());
            if let Some(database) = &self.database {
                persist_meta(database, &meta)?;
            }
            inner.vote = Some(vote.clone());
            Ok(())
        }

        async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> io::Result<()>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let entries = entries.into_iter().collect::<Vec<_>>();
            let encoded = entries
                .iter()
                .map(|entry| {
                    postcard::to_stdvec(entry)
                        .map(|bytes| (entry.index(), bytes))
                        .map_err(io_error)
                })
                .collect::<io::Result<Vec<_>>>();

            let mut inner = self.inner.lock().await;
            let result = encoded.and_then(|encoded| {
                if let Some(database) = &self.database {
                    let transaction = database.begin_write().map_err(io_error)?;
                    {
                        let mut table = transaction.open_table(LOG_TABLE).map_err(io_error)?;
                        for (index, bytes) in &encoded {
                            table.insert(index, bytes.as_slice()).map_err(io_error)?;
                        }
                    }
                    transaction.commit().map_err(io_error)?;
                }
                for entry in entries {
                    inner.log.insert(entry.index(), entry);
                }
                Ok(())
            });
            drop(inner);
            callback.io_completed(result);
            Ok(())
        }

        async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> io::Result<()> {
            let mut inner = self.inner.lock().await;
            let start = last_log_id.map_or(0, |log_id| log_id.index() + 1);
            let keys = inner
                .log
                .range(start..)
                .map(|(index, _)| *index)
                .collect::<Vec<_>>();
            if let Some(database) = &self.database {
                let transaction = database.begin_write().map_err(io_error)?;
                {
                    let mut table = transaction.open_table(LOG_TABLE).map_err(io_error)?;
                    for key in &keys {
                        table.remove(key).map_err(io_error)?;
                    }
                }
                transaction.commit().map_err(io_error)?;
            }
            for key in keys {
                inner.log.remove(&key);
            }
            Ok(())
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> io::Result<()> {
            let mut inner = self.inner.lock().await;
            assert!(inner.last_purged_log_id.as_ref() <= Some(&log_id));
            let keys = inner
                .log
                .range(..=log_id.index())
                .map(|(index, _)| *index)
                .collect::<Vec<_>>();
            let mut meta = StoreMeta::from(&*inner);
            meta.last_purged_log_id = Some(log_id.clone());

            if let Some(database) = &self.database {
                let meta = super::encode_meta(&meta)?;
                let transaction = database.begin_write().map_err(io_error)?;
                {
                    let mut table = transaction.open_table(LOG_TABLE).map_err(io_error)?;
                    for key in &keys {
                        table.remove(key).map_err(io_error)?;
                    }
                }
                transaction
                    .open_table(META_TABLE)
                    .map_err(io_error)?
                    .insert(super::META_KEY, meta.as_slice())
                    .map_err(io_error)?;
                transaction.commit().map_err(io_error)?;
            }

            inner.last_purged_log_id = Some(log_id);
            for key in keys {
                inner.log.remove(&key);
            }
            Ok(())
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
