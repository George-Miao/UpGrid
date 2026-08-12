use std::cell::RefCell;
use std::io::{self, Cursor};
use std::rc::Rc;

use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::testing::log::{StoreBuilder, Suite};
use openraft::{
    BasicNode, EntryPayload, OptionalSend, RaftSnapshotBuilder, StorageError, declare_raft_types,
};
use openraft_rt_compio::CompioRuntime;
use openraft_rt_compio::futures::{Stream, StreamExt};

use super::core::*;

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
