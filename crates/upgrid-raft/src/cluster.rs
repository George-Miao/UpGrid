//! Process-local handle for the replicated Cluster.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use snafu::{ResultExt, Snafu};
use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

use crate::domain::{ApplicationState, Command, CommandResult, DomainError};
use crate::node::Node;
use crate::raft::Req;

enum Request {
    Read {
        reply: oneshot::Sender<Result<ApplicationState, String>>,
    },
    LocalRead {
        reply: oneshot::Sender<ApplicationState>,
    },
    Write {
        request: Box<Req>,
        reply: oneshot::Sender<Result<CommandResult, ClusterError>>,
    },
    IsLeader {
        reply: oneshot::Sender<bool>,
    },
    Voters {
        reply: oneshot::Sender<BTreeSet<Uuid>>,
    },
    Status {
        reply: oneshot::Sender<Status>,
    },
    ProbeNode {
        node_id: Uuid,
        url: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Debug, Clone)]
pub struct Status {
    pub local_node_id: Uuid,
    pub leader_node_id: Option<Uuid>,
    pub members: std::collections::BTreeMap<Uuid, String>,
}

#[derive(Clone)]
pub struct Handle {
    sender: mpsc::UnboundedSender<Request>,
    version: Arc<AtomicU64>,
    events: watch::Sender<u64>,
    pub node_id: Uuid,
}

impl Handle {
    pub fn new(node_id: Uuid) -> (Self, Receiver) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (events, _) = watch::channel(0);
        let version = Arc::new(AtomicU64::new(0));
        (
            Self {
                sender,
                version: version.clone(),
                events: events.clone(),
                node_id,
            },
            Receiver {
                requests: receiver,
                version: version.clone(),
                events: events.clone(),
            },
        )
    }

    pub async fn read(&self) -> Result<ApplicationState, String> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Read { reply })
            .map_err(|_| "cluster runtime stopped".to_owned())?;
        response
            .await
            .map_err(|_| "cluster runtime stopped before responding".to_owned())?
    }

    pub async fn local_read(&self) -> Result<ApplicationState, String> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::LocalRead { reply })
            .map_err(|_| "cluster runtime stopped".to_owned())?;
        response
            .await
            .map_err(|_| "cluster runtime stopped before responding".to_owned())
    }

    pub async fn apply(&self, command: Command) -> Result<CommandResult, ClusterError> {
        let (reply, response) = oneshot::channel();
        if self
            .sender
            .send(Request::Write {
                request: Box::new(Req::new(command)),
                reply,
            })
            .is_err()
        {
            return Err(ClusterError::Unavailable {
                message: "cluster runtime stopped".to_owned(),
            });
        }
        match response.await {
            Ok(result) => result,
            Err(_) => Err(ClusterError::Unavailable {
                message: "cluster runtime stopped before responding".to_owned(),
            }),
        }
    }

    pub async fn is_leader(&self) -> bool {
        let (reply, response) = oneshot::channel();
        if self.sender.send(Request::IsLeader { reply }).is_err() {
            return false;
        }
        response.await.unwrap_or(false)
    }

    pub async fn voters(&self) -> Result<BTreeSet<Uuid>, String> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Voters { reply })
            .map_err(|_| "cluster runtime stopped".to_owned())?;
        response
            .await
            .map_err(|_| "cluster runtime stopped before responding".to_owned())
    }

    pub async fn status(&self) -> Result<Status, String> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Status { reply })
            .map_err(|_| "cluster runtime stopped".to_owned())?;
        response
            .await
            .map_err(|_| "cluster runtime stopped before responding".to_owned())
    }

    pub async fn probe_node(&self, node_id: Uuid, url: String) -> Result<(), String> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::ProbeNode {
                node_id,
                url,
                reply,
            })
            .map_err(|_| "cluster runtime stopped".to_owned())?;
        response
            .await
            .map_err(|_| "cluster runtime stopped before responding".to_owned())?
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.events.subscribe()
    }
}

fn changed(version: &AtomicU64, events: &watch::Sender<u64>) {
    let version = version.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    events.send_replace(version);
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ClusterError {
    #[snafu(display("{message}"))]
    Unavailable { message: String },

    #[snafu(display("{source}"))]
    Domain { source: DomainError },
}

pub struct Receiver {
    requests: mpsc::UnboundedReceiver<Request>,
    version: Arc<AtomicU64>,
    events: watch::Sender<u64>,
}

impl Receiver {
    pub async fn run(mut self, node: Node) {
        let mut applied = node.applied_index();
        loop {
            let request =
                match compio::time::timeout(Duration::from_millis(250), self.requests.recv()).await
                {
                    Ok(Some(request)) => Some(request),
                    Ok(None) => break,
                    Err(_) => None,
                };
            match request {
                Some(Request::Read { reply }) => {
                    let _ = reply.send(node.read().await);
                }
                Some(Request::LocalRead { reply }) => {
                    let _ = reply.send(node.local_application_state());
                }
                Some(Request::Write { request, reply }) => {
                    let result = match node.write(*request).await {
                        Ok(response) => response.result.context(DomainSnafu),
                        Err(message) => Err(ClusterError::Unavailable { message }),
                    };
                    let _ = reply.send(result);
                }
                Some(Request::IsLeader { reply }) => {
                    let _ = reply.send(node.is_leader().await);
                }
                Some(Request::Voters { reply }) => {
                    let _ = reply.send(node.voters());
                }
                Some(Request::Status { reply }) => {
                    let (leader_node_id, members) = node.cluster_topology();
                    let _ = reply.send(Status {
                        local_node_id: node.node_id(),
                        leader_node_id,
                        members,
                    });
                }
                Some(Request::ProbeNode {
                    node_id,
                    url,
                    reply,
                }) => {
                    let _ = reply.send(node.probe_node(node_id, &url).await);
                }
                None => {}
            }
            let current = node.applied_index();
            if current != applied {
                applied = current;
                changed(&self.version, &self.events);
            }
        }
    }
}
