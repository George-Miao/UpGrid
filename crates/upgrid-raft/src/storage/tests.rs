use super::core::*;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use openraft::vote::leader_id_adv::CommittedLeaderId;
    use openraft::{Entry, EntryPayload, LogId};
    use uuid::Uuid;

    use super::*;
    use crate::raft::TC;

    #[compio::test]
    async fn migrates_legacy_log_and_reopens_redb() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-log.redb");
        let legacy_path = directory.join("raft-log.postcard");
        let leader = CommittedLeaderId::<TC> {
            term: 1,
            node_id: Uuid::now_v7(),
        };
        let entry = Entry {
            log_id: LogId::new(leader, 1),
            payload: EntryPayload::Blank,
        };
        let legacy = InMemStoreInner::<TC> {
            log: BTreeMap::from([(1, entry)]),
            committed: Some(LogId::new(leader, 1)),
            ..Default::default()
        };
        fs::write(&legacy_path, postcard::to_stdvec(&legacy).unwrap()).unwrap();

        let store = InMemStore::<TC>::open(&path, &legacy_path).unwrap();
        let inner = store.inner.lock().await;
        assert_eq!(inner.log.len(), 1);
        assert_eq!(inner.committed, Some(LogId::new(leader, 1)));
        drop(inner);
        drop(store);
        fs::remove_file(&legacy_path).unwrap();

        let reopened = InMemStore::<TC>::open(&path, &legacy_path).unwrap();
        let inner = reopened.inner.lock().await;
        assert_eq!(inner.log.len(), 1);
        assert_eq!(inner.committed, Some(LogId::new(leader, 1)));
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

        let leader = CommittedLeaderId::<TC> {
            term: 1,
            node_id: Uuid::now_v7(),
        };
        let legacy = InMemStoreInner::<TC> {
            log: BTreeMap::from([(
                1,
                Entry {
                    log_id: LogId::new(leader, 1),
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
