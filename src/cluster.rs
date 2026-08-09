use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

use crate::app::now_ms;
use crate::domain::{ApplicationState, Command, CommandResult, DomainError};
use crate::node::Node;
use crate::raft::Req;

enum Request {
    Read {
        reply: oneshot::Sender<Result<ApplicationState, String>>,
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

    pub async fn apply(&self, command: Command) -> Result<CommandResult, ClusterError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Request::Write {
                request: Box::new(Req {
                    operation_id: Uuid::now_v7(),
                    submitted_at_ms: now_ms(),
                    command,
                }),
                reply,
            })
            .map_err(|_| ClusterError::Unavailable("cluster runtime stopped".to_owned()))?;
        response.await.map_err(|_| {
            ClusterError::Unavailable("cluster runtime stopped before responding".to_owned())
        })?
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

#[derive(Debug)]
pub enum ClusterError {
    Unavailable(String),
    Domain(DomainError),
}

impl std::fmt::Display for ClusterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(message) => formatter.write_str(message),
            Self::Domain(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ClusterError {}

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
                Some(Request::Write { request, reply }) => {
                    let result = match node.write(*request).await {
                        Ok(response) => response.result.map_err(ClusterError::Domain),
                        Err(error) => Err(ClusterError::Unavailable(error)),
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
