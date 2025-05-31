#![feature(thread_local)]

use std::{collections::BTreeMap, error::Error as StdError, rc::Rc, sync::Arc};

use compio::runtime::spawn;
use openraft::{
    Config,
    alias::NodeOf,
    error::{Fatal, InitializeError, RaftError},
};
use snafu::{ResultExt, Snafu};
use url::Url;
use uuid::Uuid;

use crate::{
    network::{UpgridNetwork, UpgridServer, UrlNode},
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

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create Raft instance: {}", source))]
    RaftCreation { source: Fatal<TC> },

    #[snafu(display("Failed to initialize Raft: {}", source))]
    RaftInitialize {
        source: RaftError<TC, InitializeError<TC>>,
    },

    #[snafu(display("Failed to parse input url: {}", source))]
    UrlParse { source: Box<dyn StdError> },

    #[snafu(display("Invalid scheme: {}, expected `up` or `ups`", url.scheme()))]
    UrlInvalidScheme { url: Url },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[compio::main]
async fn main() {
    let app1 = Node::new("up://127.0.0.1:8080").await.unwrap();
    let app2 = Node::new("up://127.0.0.1:8081").await.unwrap();
    let app3 = Node::new("up://127.0.0.1:8082").await.unwrap();

    app1.init_self().await.unwrap();
}

struct Node {
    id: Uuid,
    node: NodeOf<TC>,
    raft: Raft,
    server: JoinHandle<()>,
}

impl Node {
    async fn new<U, E>(advertise_url: U) -> Result<Self>
    where
        U: TryInto<Url, Error = E>,
        E: StdError + 'static,
    {
        let url = advertise_url
            .try_into()
            .map_err(|e| Box::new(e) as Box<dyn StdError>)
            .context(UrlParseSnafu)?;
        let node = UrlNode::new(url)?;
        let id = uuid_v7_now();
        let network = UpgridNetwork::new(id);
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
        .await
        .context(RaftCreationSnafu)?;

        let server = UpgridServer::new(raft.clone());
        let url = node.url().clone();

        spawn(async move { server.serve_bind(url.as_str()).await }).detach();

        Ok(Self { id, node, raft })
    }

    async fn init_self(&self) -> Result<()> {
        let mut map = BTreeMap::new();
        map.insert(self.id, self.node.clone());
        let res = self.raft.initialize(map).await;

        if let Err(RaftError::APIError(InitializeError::NotAllowed(_))) = &res {
            Ok(())
        } else {
            res.context(RaftInitializeSnafu)
        }
    }
}
