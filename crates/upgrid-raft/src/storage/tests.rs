use super::core::*;

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::{self, Cursor};
    use std::rc::Rc;

    use openraft::alias::{
        CommittedLeaderIdOf, EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf,
    };
    use openraft::storage::{RaftStateMachine, Snapshot};
    use openraft::testing::log::{StoreBuilder, Suite};
    use openraft::{
        BasicNode, Entry, EntryPayload, OptionalSend, RaftSnapshotBuilder, StorageError,
        declare_raft_types,
    };
    use openraft_rt_compio::CompioRuntime;
    use openraft_rt_compio::futures::{Stream, StreamExt};
    use uuid::Uuid;

    use super::*;
    use crate::raft::TC;

    declare_raft_types! {
        TestTC:
            D = String,
            R = String,
            Node = BasicNode,
            NodeId = u64,
            AsyncRuntime = CompioRuntime,
    }

    type AppliedState = (Option<LogIdOf<TestTC>>, StoredMembershipOf<TestTC>);
    type TestSnapshot = SnapshotOf<TestTC, Cursor<Vec<u8>>>;

    #[derive(Clone, Default)]
    struct TestStateMachine {
        applied: Rc<RefCell<AppliedState>>,
        snapshot: Rc<RefCell<Option<TestSnapshot>>>,
    }

    impl RaftSnapshotBuilder<TestTC> for TestStateMachine {
        type SnapshotData = Cursor<Vec<u8>>;

        async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<TestTC, Self::SnapshotData>> {
            let (last_log_id, last_membership) = self.applied.borrow().clone();
            let snapshot = Snapshot {
                meta: SnapshotMetaOf::<TestTC> {
                    last_log_id,
                    last_membership,
                },
                snapshot: Cursor::new(Vec::new()),
            };
            *self.snapshot.borrow_mut() = Some(snapshot.clone());
            Ok(snapshot)
        }
    }

    impl RaftStateMachine<TestTC> for TestStateMachine {
        type SnapshotBuilder = Self;
        type SnapshotData = Cursor<Vec<u8>>;

        async fn applied_state(
            &mut self,
        ) -> io::Result<(Option<LogIdOf<TestTC>>, StoredMembershipOf<TestTC>)> {
            Ok(self.applied.borrow().clone())
        }

        async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
        where
            Strm: Stream<
                    Item = io::Result<(
                        EntryOf<TestTC>,
                        Option<openraft::storage::ApplyResponder<TestTC>>,
                    )>,
                > + Unpin
                + OptionalSend,
        {
            while let Some(item) = entries.next().await {
                let (entry, responder) = item?;
                let membership = match &entry.payload {
                    EntryPayload::Membership(membership) => {
                        StoredMembershipOf::<TestTC>::new(Some(entry.log_id), membership.clone())
                    }
                    _ => self.applied.borrow().1.clone(),
                };
                *self.applied.borrow_mut() = (Some(entry.log_id), membership);
                if let Some(responder) = responder {
                    responder.send(String::new());
                }
            }
            Ok(())
        }

        async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
            self.clone()
        }

        async fn install_snapshot(
            &mut self,
            meta: &SnapshotMetaOf<TestTC>,
            snapshot: Self::SnapshotData,
        ) -> io::Result<()> {
            *self.applied.borrow_mut() = (meta.last_log_id, meta.last_membership.clone());
            *self.snapshot.borrow_mut() = Some(Snapshot {
                meta: meta.clone(),
                snapshot,
            });
            Ok(())
        }

        async fn get_current_snapshot(
            &mut self,
        ) -> io::Result<Option<SnapshotOf<TestTC, Self::SnapshotData>>> {
            Ok(self.snapshot.borrow().clone())
        }
    }

    struct TestStoreBuilder;

    impl StoreBuilder<TestTC, InMemStore<TestTC>, TestStateMachine> for TestStoreBuilder {
        async fn build(
            &self,
        ) -> Result<((), InMemStore<TestTC>, TestStateMachine), StorageError<TestTC>> {
            Ok(((), InMemStore::new(), TestStateMachine::default()))
        }
    }

    #[compio::test]
    async fn conforms_to_openraft_storage_contracts() {
        Suite::test_all(TestStoreBuilder).await.unwrap();
    }

    #[compio::test]
    async fn migrates_legacy_log_and_reopens_redb() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-log.redb");
        let legacy_path = directory.join("raft-log.postcard");
        let leader = CommittedLeaderIdOf::<TC> {
            term: 1,
            node_id: Uuid::now_v7(),
        };
        let entry = Entry {
            log_id: LogIdOf::<TC>::new(leader, 1),
            payload: EntryPayload::Blank,
        };
        let legacy = InMemStoreInner::<TC> {
            log: BTreeMap::from([(1, entry)]),
            committed: Some(LogIdOf::<TC>::new(leader, 1)),
            ..Default::default()
        };
        fs::write(&legacy_path, postcard::to_stdvec(&legacy).unwrap()).unwrap();

        let store = InMemStore::<TC>::open(&path, &legacy_path).unwrap();
        let inner = store.inner.lock().await;
        assert_eq!(inner.log.len(), 1);
        assert_eq!(inner.committed, Some(LogIdOf::<TC>::new(leader, 1)));
        drop(inner);
        drop(store);
        fs::remove_file(&legacy_path).unwrap();

        let reopened = InMemStore::<TC>::open(&path, &legacy_path).unwrap();
        let inner = reopened.inner.lock().await;
        assert_eq!(inner.log.len(), 1);
        assert_eq!(inner.committed, Some(LogIdOf::<TC>::new(leader, 1)));
        drop(inner);
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[compio::test]
    async fn initialized_redb_does_not_import_a_late_legacy_file() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-log.redb");
        let legacy_path = directory.join("raft-log.postcard");
        let store = InMemStore::<TC>::open(&path, &legacy_path).unwrap();
        drop(store);

        let leader = CommittedLeaderIdOf::<TC> {
            term: 1,
            node_id: Uuid::now_v7(),
        };
        let legacy = InMemStoreInner::<TC> {
            log: BTreeMap::from([(
                1,
                Entry {
                    log_id: LogIdOf::<TC>::new(leader, 1),
                    payload: EntryPayload::Blank,
                },
            )]),
            ..Default::default()
        };
        fs::write(&legacy_path, postcard::to_stdvec(&legacy).unwrap()).unwrap();

        let reopened = InMemStore::<TC>::open(&path, &legacy_path).unwrap();
        assert!(reopened.inner.lock().await.log.is_empty());
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }
}
