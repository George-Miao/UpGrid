use sea_query::{
    ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, SqliteQueryBuilder, Table,
    TableCreateStatement,
};

use super::schema::{RaftLog, RaftMeta, Snapshot, StateMachine};

pub(super) fn migration() -> String {
    let statements = [
        Table::create()
            .table(RaftLog::Table)
            .col(
                ColumnDef::new(RaftLog::LogIndex)
                    .big_integer()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(RaftLog::Entry).binary().not_null())
            .to_owned(),
        Table::create()
            .table(RaftMeta::Table)
            .col(
                ColumnDef::new(RaftMeta::Id)
                    .integer()
                    .not_null()
                    .primary_key()
                    .check(Expr::col(RaftMeta::Id).eq(1)),
            )
            .col(ColumnDef::new(RaftMeta::LastPurgedLogId).binary())
            .col(ColumnDef::new(RaftMeta::Committed).binary())
            .col(ColumnDef::new(RaftMeta::Vote).binary())
            .to_owned(),
        Table::create()
            .table(StateMachine::Table)
            .col(
                ColumnDef::new(StateMachine::Id)
                    .integer()
                    .not_null()
                    .primary_key()
                    .check(Expr::col(StateMachine::Id).eq(1)),
            )
            .col(ColumnDef::new(StateMachine::LastAppliedLog).binary())
            .col(
                ColumnDef::new(StateMachine::LastMembership)
                    .binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(StateMachine::Application)
                    .binary()
                    .not_null(),
            )
            .col(
                ColumnDef::new(StateMachine::SnapshotIdx)
                    .big_integer()
                    .not_null()
                    .check(Expr::col(StateMachine::SnapshotIdx).gte(0)),
            )
            .to_owned(),
        Table::create()
            .table(Snapshot::Table)
            .col(
                ColumnDef::new(Snapshot::Id)
                    .integer()
                    .not_null()
                    .primary_key()
                    .check(Expr::col(Snapshot::Id).eq(1)),
            )
            .col(ColumnDef::new(Snapshot::Meta).binary().not_null())
            .col(ColumnDef::new(Snapshot::Data).binary().not_null())
            .foreign_key(
                ForeignKey::create()
                    .from(Snapshot::Table, Snapshot::Id)
                    .to(StateMachine::Table, StateMachine::Id)
                    .on_delete(ForeignKeyAction::Cascade),
            )
            .to_owned(),
    ];

    statements
        .iter()
        .map(|statement: &TableCreateStatement| statement.to_string(SqliteQueryBuilder))
        .collect::<Vec<_>>()
        .join(";\n")
        + ";"
}
