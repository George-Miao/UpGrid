use std::{cell::RefCell, collections::HashMap, io, rc::Rc};

use compio::{
    net::ToSocketAddrsAsync,
    runtime::{JoinHandle, spawn},
};
use openraft::{
    alias::NodeIdOf,
    error::{InstallSnapshotError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use openraft_rt_compio::futures::{FutureExt, StreamExt, pin_mut};
use tarpc::{
    client::{Config, NewClient},
    context::Context,
    server::{BaseChannel, Channel},
};
use tracing::debug;
use url::Url;

use crate::{
    TC,
    network::transport::{connect_framed, listen_framed},
    raft::Raft,
};

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

#[derive(Debug)]
struct ClientEntry {
    client: UpgridServiceClient,
    handle: JoinHandle<()>,
}

#[derive(Debug, Default, Clone)]
pub struct ClientPool {
    clients: Rc<RefCell<HashMap<NodeIdOf<TC>, ClientEntry>>>,
}

impl ClientPool {
    pub fn drop_client(&self, node_id: &NodeIdOf<TC>) {
        self.clients.borrow_mut().remove(node_id);
    }

    pub async fn get_client(
        &self,
        node_id: NodeIdOf<TC>,
        url: &Url,
    ) -> io::Result<UpgridServiceClient> {
        if let Some(entry) = self.clients.borrow().get(&node_id) {
            return Ok(entry.client.clone());
        }

        let mut config = Config::default();
        config.max_in_flight_requests = 1024;
        config.pending_request_buffer = 8;

        let transport = connect_framed(url.as_str()).await?;
        let NewClient { client, dispatch } = UpgridServiceClient::new(config, transport);
        let handle = compio::runtime::spawn(dispatch.map(|res| {
            if let Err(e) = res {
                debug!(error=%e, "client transport error")
            }
        }));

        let entry = ClientEntry {
            client: client.clone(),
            handle,
        };

        self.clients.borrow_mut().insert(node_id, entry);

        Ok(client)
    }
}

#[derive(Clone)]
pub struct UpgridServer {
    raft: Raft,
    pool: ClientPool,
}

impl UpgridServer {
    pub fn new(raft: Raft, pool: ClientPool) -> Self {
        Self { raft, pool }
    }

    pub async fn serve_bind(&self, addr: impl ToSocketAddrsAsync) -> io::Result<()> {
        let streams = listen_framed(addr);
        pin_mut!(streams);

        while let Some(stream) = streams.next().await {
            let stream = stream?;
            let this = self.clone();

            spawn(async move {
                let serve = UpgridService::serve(this);
                let stream = BaseChannel::with_defaults(stream).execute(serve);
                pin_mut!(stream);
                while let Some(resp) = stream.next().await {
                    spawn(resp).detach();
                }
            })
            .detach();
        }

        Ok(())
    }

    pub async fn serve(self) -> io::Result<()> {
        self.serve_bind("0.0.0.0:11451").await
    }
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
