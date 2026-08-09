//! Adapts generic transport channels to the UpGrid tarpc service.

use std::cell::OnceCell;
use std::pin::pin;
use std::rc::Rc;

use compio::runtime::spawn;
use openraft::alias::NodeOf;
use openraft_rt_compio::futures::lock::Mutex;
use openraft_rt_compio::futures::{FutureExt, StreamExt};
use tarpc::client::{Config, NewClient};
use tarpc::server::{BaseChannel, Channel};
use tracing::debug;
use upgrid_transport::{RpcSession, RpcTransport};

use super::service::{UpgridServer, UpgridService, UpgridServiceClient};
use crate::Result;
use crate::raft::{Raft, TC};

#[derive(Clone)]
pub(crate) struct Rpc {
    raft: Rc<OnceCell<Raft>>,
    transport: RpcTransport,
    membership_changes: Rc<Mutex<()>>,
    deployment_key_fingerprint: [u8; 32],
}

impl Rpc {
    pub(crate) fn new(transport: RpcTransport, deployment_key_fingerprint: [u8; 32]) -> Self {
        Self {
            raft: Rc::new(OnceCell::new()),
            transport,
            membership_changes: Rc::new(Mutex::new(())),
            deployment_key_fingerprint,
        }
    }

    pub(crate) fn init_raft(&self, raft: Raft) {
        assert!(
            self.raft.set(raft).is_ok(),
            "Raft should be initialized only once"
        );
    }

    fn raft(&self) -> &Raft {
        self.raft.get().expect("Raft should be initialized")
    }

    pub(crate) async fn client(&self, node: &NodeOf<TC>) -> Result<UpgridServiceClient> {
        let channel = self.transport.connect(node.host(), node.port()).await?;
        let mut config = Config::default();
        config.max_in_flight_requests = 1024;
        config.pending_request_buffer = 8;
        let NewClient { client, dispatch } = UpgridServiceClient::new(config, channel);
        spawn(dispatch.map(|result| {
            if let Err(error) = result {
                debug!(%error, "client RPC stream closed");
            }
        }))
        .detach();
        Ok(client)
    }

    pub(crate) fn invalidate(&self, node: &NodeOf<TC>) {
        self.transport.invalidate(node.host(), node.port());
    }

    pub(crate) async fn run(self) {
        while let Some(session) = self.transport.accept().await {
            match session {
                Ok(session) => spawn(self.clone().serve(session)).detach(),
                Err(error) => tracing::warn!(%error, "could not accept Node RPC session"),
            }
        }
        tracing::error!("Node RPC endpoint closed");
    }

    async fn serve(self, session: RpcSession) {
        let peer = session.remote_address();
        let server = UpgridServer::new(
            self.raft().clone(),
            self.membership_changes.clone(),
            self.deployment_key_fingerprint,
        );
        let mut channels = pin!(session.channels());
        while let Some(channel) = channels.next().await {
            match channel {
                Ok(channel) => {
                    let server = server.clone();
                    spawn(async move {
                        let mut requests = pin!(BaseChannel::with_defaults(channel).requests());
                        while let Some(request) = requests.next().await {
                            match request {
                                Ok(request) => {
                                    spawn(request.execute(server.clone().serve())).detach()
                                }
                                Err(error) => {
                                    tracing::warn!(%peer, ?error, "Node RPC stream failed");
                                    break;
                                }
                            }
                        }
                        debug!(%peer, "Node RPC stream disconnected");
                    })
                    .detach();
                }
                Err(error) => debug!(%peer, %error, "could not accept RPC channel"),
            }
        }
        debug!(%peer, "Node RPC session disconnected");
    }
}
