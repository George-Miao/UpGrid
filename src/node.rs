use std::{collections::BTreeMap, error::Error as StdError, rc::Rc, sync::Arc};

use compio::runtime::{JoinHandle, spawn};
use openraft::{
    Config,
    error::{InitializeError, RaftError},
};
use snafu::ResultExt;
use tarpc::context::Context;
use tracing::{error, info};
use url::Url;

use crate::{
    error::*,
    network::{Controller, UpgridNetwork, UpgridNode},
    raft::{Identity, Raft},
    state_machine::StateMachine,
    storage::InMemStore,
    utils::unsafe_endpoint,
};

pub struct Node {
    id: Identity,
    raft: Raft,
    controller: Controller,
    _server_handle: JoinHandle<()>,
}

impl Node {
    pub async fn new<U, E>(advertise_url: U) -> Result<Self>
    where
        U: TryInto<Url, Error = E>,
        E: StdError + 'static,
    {
        let id = Identity::new(advertise_url)?;

        let endpoint = unsafe_endpoint(id.node.host().to_owned(), id.node.port()).await?;

        let controller = Controller::new(endpoint.clone());
        let network = UpgridNetwork::new(id.clone(), controller.clone());
        let storage = InMemStore::new();
        let state_machine = StateMachine::default();
        let config = Config {
            cluster_name: "UpGrid".to_string(),
            ..Config::default()
        };

        let raft = Raft::new(
            id.id,
            Arc::new(config),
            network,
            storage,
            Rc::new(state_machine),
        )
        .await
        .context(RaftCreationSnafu)?;

        controller.init_raft(raft.clone());

        let con = controller.clone();

        let server_handle = spawn(async move {
            while let Some(incoming) = endpoint.wait_incoming().await {
                info!("New incoming connection");
                let controller = con.clone();
                spawn(async move { controller.accept(incoming).await }).detach();
                info!("Income handled");
            }

            error!("Endpoint closed");
        });

        Ok(Self {
            id,
            raft,
            controller,
            _server_handle: server_handle,
        })
    }

    pub async fn join<U, E>(&self, remote: U) -> Result<()>
    where
        U: TryInto<Url, Error = E>,
        E: std::error::Error + 'static,
    {
        let remote = UpgridNode::new(remote)?;
        info!("Joining {remote:?}");

        self.controller
            .get_client(&remote)
            .await?
            .ask_to_join(Context::current(), self.id.clone())
            .await
            .context(RpcSnafu)?
            .context(RaftJoinSnafu)?;

        Ok(())
    }

    pub async fn start_cluster(&self) -> Result<()> {
        let mut map = BTreeMap::new();
        map.insert(self.id.id, self.id.node.clone());
        let res = self.raft.initialize(map).await;

        if let Err(RaftError::APIError(InitializeError::NotAllowed(_))) = &res {
            Ok(())
        } else {
            res.context(RaftInitializeSnafu)
        }
    }
}
