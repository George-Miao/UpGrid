#![feature(thread_local)]

use std::{collections::HashMap, net::ToSocketAddrs, rc::Rc, sync::Arc};

use openraft::{
    Config,
    alias::NodeOf,
    error::{Fatal, InitializeError, RaftError},
};
use uuid::Uuid;

use crate::{
    network::TarpcNetwork,
    raft::{Raft, TC},
    state_machine::StateMachine,
    storage::InMemStore,
    utils::uuid_v7_now,
};

pub mod network;
mod raft;
mod state_machine;
mod storage;
mod utils;

#[compio::main]
async fn main() {
    let app1 = Node::new().await.unwrap();
    let app2 = Node::new().await.unwrap();
    let app3 = Node::new().await.unwrap();

    app1.init_self().await.unwrap();
}

struct Node {
    id: Uuid,
    node: NodeOf<TC>,
    raft: Raft,
}

impl Node {
    fn id(&self) -> Uuid {
        self.id
    }

    async fn init_self(&self) -> Result<(), RaftError<TC, InitializeError<TC>>> {
        let mut map = HashMap::with_capacity(1);
        map.insert(self.id, self.node);
        let res = self.raft.initialize(map).await;

        if let Err(RaftError::APIError(InitializeError::NotAllowed(_))) = &res {
            Ok(())
        } else {
            res
        }
    }

    async fn new() -> Result<Self, Fatal<TC>> {
        let id = uuid_v7_now();
        let network = TarpcNetwork::new(id);
        let storage = InMemStore::new();
        let state_machine = StateMachine::default();
        let config = Config {
            cluster_name: "UpGrid".to_string(),
            ..Config::default()
        };

        let raft = Raft::new(
            id,
            Arc::new(config),
            network,
            storage,
            Rc::new(state_machine),
        )
        .await?;

        Ok(Self { id, raft })
    }
}
