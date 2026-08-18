use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::rc::Rc;
use std::sync::Arc;

use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use openraft::{LogState, RaftTypeConfig};
use openraft_rt_compio::futures::lock::Mutex;
use serde::de::DeserializeOwned;

use crate::database::{LogRepository, RaftDatabase};

/// A durable Raft log backed by SQLite and mirrored in memory for fast reads.
#[derive(Clone, Debug)]
pub struct InMemStore<C: RaftTypeConfig> {
    pub(super) inner: Arc<Mutex<InMemStoreInner<C>>>,
    repository: Option<LogRepository>,
}

impl<C: RaftTypeConfig> InMemStore<C> {
    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemStoreInner::default())),
            repository: None,
        }
    }

    pub(crate) fn open(database: Rc<RaftDatabase>) -> io::Result<Self>
    where
        C::Entry: DeserializeOwned,
        LogIdOf<C>: DeserializeOwned,
        VoteOf<C>: DeserializeOwned,
    {
        let repository = LogRepository::new(database);
        let inner = repository.load()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            repository: Some(repository),
        })
    }
}

#[cfg(test)]
impl<C: RaftTypeConfig> Default for InMemStore<C> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct InMemStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    pub(crate) last_purged_log_id: Option<LogIdOf<C>>,

    /// The Raft log.
    pub(crate) log: BTreeMap<u64, C::Entry>,

    /// The commit log id.
    pub(crate) committed: Option<LogIdOf<C>>,

    /// The current granted vote.
    pub(crate) vote: Option<VoteOf<C>>,
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

mod impl_log_store {
    use std::fmt::Debug;
    use std::io;
    use std::ops::RangeBounds;

    use openraft::alias::{LogIdOf, VoteOf};
    use openraft::entry::RaftEntry;
    use openraft::storage::{IOFlushed, RaftLogStorage};
    use openraft::{LogState, RaftLogReader, RaftTypeConfig};
    use serde::Serialize;

    use super::InMemStore;

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
            if let Some(repository) = &self.repository {
                repository.save_committed::<C>(committed.as_ref())?;
            }
            inner.committed = committed;
            Ok(())
        }

        async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<C>>> {
            self.inner.lock().await.read_committed().await
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> io::Result<()> {
            let mut inner = self.inner.lock().await;
            if let Some(repository) = &self.repository {
                repository.save_vote::<C>(vote)?;
            }
            inner.vote = Some(vote.clone());
            Ok(())
        }

        async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> io::Result<()>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let entries = entries.into_iter().collect::<Vec<_>>();
            let mut inner = self.inner.lock().await;
            let result = if let Some(repository) = &self.repository {
                repository.append::<C>(&entries).map_err(io::Error::from)
            } else {
                Ok(())
            };
            if result.is_ok() {
                for entry in entries {
                    inner.log.insert(entry.index(), entry);
                }
            }
            drop(inner);
            callback.io_completed(result);
            Ok(())
        }

        async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> io::Result<()> {
            let mut inner = self.inner.lock().await;
            let start = match last_log_id.as_ref() {
                Some(log_id) => log_id.index().checked_add(1),
                None => Some(0),
            };
            if let Some(repository) = &self.repository {
                repository.truncate_after::<C>(last_log_id)?;
            }
            let Some(start) = start else {
                return Ok(());
            };
            let keys = inner
                .log
                .range(start..)
                .map(|(index, _)| *index)
                .collect::<Vec<_>>();
            for key in keys {
                inner.log.remove(&key);
            }
            Ok(())
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> io::Result<()> {
            let mut inner = self.inner.lock().await;
            assert!(inner.last_purged_log_id.as_ref() <= Some(&log_id));
            if let Some(repository) = &self.repository {
                repository.purge::<C>(&log_id)?;
            }
            let keys = inner
                .log
                .range(..=log_id.index())
                .map(|(index, _)| *index)
                .collect::<Vec<_>>();
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
