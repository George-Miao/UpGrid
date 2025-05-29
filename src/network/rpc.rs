use std::{cell::RefCell, collections::HashMap, io, net::SocketAddr, rc::Rc};

use compio::runtime::JoinHandle;
use openraft::{
    alias::NodeIdOf,
    error::{InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use openraft_rt_compio::futures::FutureExt;
use tarpc::{
    client::{Config, NewClient},
    context::Context,
};
use tracing::{debug, warn};

use crate::{TC, network::transport::connect_framed, raft::Raft};

#[tarpc::service]
pub trait UpgridService {
    async fn install_snapshot(
        req: InstallSnapshotRequest<TC>,
    ) -> Result<InstallSnapshotResponse<TC>, RaftError<TC, InstallSnapshotError>>;

    async fn append_entries(
        req: AppendEntriesRequest<TC>,
    ) -> Result<AppendEntriesResponse<TC>, RaftError<TC>>;

    async fn vote(req: VoteRequest<TC>) -> Result<VoteResponse<TC>, RaftError<TC>>;
}

#[derive(Debug, Default, Clone)]
pub struct ClientPool {
    clients: Rc<RefCell<HashMap<NodeIdOf<TC>, (UpgridServiceClient, JoinHandle<()>)>>>,
}

impl ClientPool {
    pub fn drop_client(&self, node_id: &NodeIdOf<TC>) {
        self.clients.borrow_mut().remove(&node_id);
    }

    pub async fn get_client(
        &self,
        node_id: NodeIdOf<TC>,
        addr: SocketAddr,
    ) -> io::Result<UpgridServiceClient> {
        if let Some((client, _)) = self.clients.borrow().get(&node_id) {
            return Ok(client.clone());
        }

        let mut config = Config::default();
        config.max_in_flight_requests = 1024;
        config.pending_request_buffer = 8;

        let transport = connect_framed(addr).await?;
        let NewClient { client, dispatch } = UpgridServiceClient::new(config, transport);
        let handle = compio::runtime::spawn(dispatch.map(|res| {
            if let Err(e) = res {
                debug!(error=%e, "client transport error")
            }
        }));

        self.clients
            .borrow_mut()
            .insert(node_id, (client.clone(), handle));

        Ok(client)
    }
}

struct UpgridServer {
    raft: Raft,
}

impl UpgridService for UpgridServer {
    async fn install_snapshot(
        self,
        _: Context,
        req: InstallSnapshotRequest<TC>,
    ) -> Result<InstallSnapshotResponse<TC>, RaftError<TC, InstallSnapshotError>> {
        self.raft.install_snapshot(req).await
    }

    async fn append_entries(
        self,
        _: Context,
        req: AppendEntriesRequest<TC>,
    ) -> Result<AppendEntriesResponse<TC>, RaftError<TC>> {
        self.raft.append_entries(req).await
    }

    async fn vote(
        self,
        _: Context,
        req: VoteRequest<TC>,
    ) -> Result<VoteResponse<TC>, RaftError<TC>> {
        self.raft.vote(req).await
    }
}
