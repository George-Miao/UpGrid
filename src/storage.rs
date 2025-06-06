use std::{collections::BTreeMap, fmt::Debug, ops::RangeBounds, sync::Arc};

use openraft::{
    LogState, RaftTypeConfig, StorageError,
    alias::{LogIdOf, VoteOf},
    entry::RaftEntry,
    storage::IOFlushed,
};
use openraft_rt_compio::futures::lock::Mutex;

/// RaftLogStore implementation with a in-memory storage
#[derive(Clone, Debug, Default)]
pub struct InMemStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<InMemStoreInner<C>>>,
}

impl<C: RaftTypeConfig> InMemStore<C> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemStoreInner::default())),
        }
    }
}

#[derive(Debug)]
pub struct InMemStoreInner<C: RaftTypeConfig> {
    /// The last purged log id.
    last_purged_log_id: Option<LogIdOf<C>>,

    /// The Raft log.
    log: BTreeMap<u64, C::Entry>,

    /// The commit log id.
    committed: Option<LogIdOf<C>>,

    /// The current granted vote.
    vote: Option<VoteOf<C>>,
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
    ) -> Result<Vec<C::Entry>, StorageError<C>>
    where
        C::Entry: Clone,
    {
        let response = self
            .log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.log_id());

        let last_purged = self.last_purged_log_id.clone();

        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<C>>,
    ) -> Result<(), StorageError<C>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, StorageError<C>> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), StorageError<C>> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, StorageError<C>> {
        Ok(self.vote.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), StorageError<C>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        // Simple implementation that calls the flush-before-return `append_to_log`.
        for entry in entries {
            self.log.insert(entry.index(), entry);
        }
        callback.io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
        let keys = self
            .log
            .range(log_id.index()..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }

        {
            let keys = self
                .log
                .range(..=log_id.index())
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                self.log.remove(&key);
            }
        }

        Ok(())
    }
}

mod impl_log_store {
    use std::{fmt::Debug, ops::RangeBounds};

    use openraft::{
        LogState, RaftLogReader, RaftTypeConfig, StorageError,
        alias::{LogIdOf, VoteOf},
        storage::{IOFlushed, RaftLogStorage},
    };

    use crate::storage::InMemStore;

    impl<C: RaftTypeConfig> RaftLogReader<C> for InMemStore<C>
    where
        C::Entry: Clone,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.try_get_log_entries(range).await
        }

        async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.read_vote().await
        }
    }

    impl<C: RaftTypeConfig> RaftLogStorage<C> for InMemStore<C>
    where
        C::Entry: Clone,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.get_log_state().await
        }

        async fn save_committed(
            &mut self,
            committed: Option<LogIdOf<C>>,
        ) -> Result<(), StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.save_committed(committed).await
        }

        async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.read_committed().await
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.save_vote(vote).await
        }

        async fn append<I>(
            &mut self,
            entries: I,
            callback: IOFlushed<C>,
        ) -> Result<(), StorageError<C>>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let mut inner = self.inner.lock().await;
            inner.append(entries, callback).await
        }

        async fn truncate(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.truncate(log_id).await
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), StorageError<C>> {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
