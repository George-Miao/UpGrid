use sea_query::Iden;

#[derive(Clone, Copy, Debug, Iden)]
pub(super) enum RaftLog {
    Table,
    LogIndex,
    Entry,
}

#[derive(Clone, Copy, Debug, Iden)]
pub(super) enum RaftMeta {
    Table,
    Id,
    LastPurgedLogId,
    Committed,
    Vote,
}

#[derive(Clone, Copy, Debug, Iden)]
pub(super) enum StateMachine {
    Table,
    Id,
    LastAppliedLog,
    LastMembership,
    Application,
    SnapshotIdx,
}

#[derive(Clone, Copy, Debug, Iden)]
pub(super) enum Snapshot {
    Table,
    Id,
    Meta,
    Data,
}
