use std::fmt;
use std::time::Duration;

use compio::runtime::spawn;
use compio::time::timeout;
use tokio::sync::{mpsc, oneshot};

use super::{Notifier, channel, send};

struct Call {
    channel: channel::TestChannel,
    reply: oneshot::Sender<Result<(), TestError>>,
}

/// Sends one-off channel tests through the Compio notification runtime.
#[derive(Clone)]
pub struct Tester {
    sender: mpsc::UnboundedSender<Call>,
}

impl Tester {
    /// Sends a test message without storing the supplied channel configuration.
    pub async fn send(&self, channel: TestChannel) -> Result<(), TestError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(Call { channel, reply })
            .map_err(|_| TestError::Unavailable)?;
        response.await.map_err(|_| TestError::Unavailable)?
    }
}

/// Failure to execute or accept a channel test.
#[derive(Debug)]
pub enum TestError {
    Unavailable,
    Failed(String),
}

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("notification runtime unavailable"),
            Self::Failed(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for TestError {}

pub(crate) struct Receiver {
    receiver: mpsc::UnboundedReceiver<Call>,
}

impl Receiver {
    pub(crate) fn drain(&mut self, notifier: &Notifier) {
        while let Ok(Call { channel, reply }) = self.receiver.try_recv() {
            let notifier = notifier.clone();
            spawn(async move {
                let _ = reply.send(attempt(&notifier, &channel).await);
            })
            .detach();
        }
    }
}

pub(crate) fn channel() -> (Tester, Receiver) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (Tester { sender }, Receiver { receiver })
}

async fn attempt(notifier: &Notifier, channel: &TestChannel) -> Result<(), TestError> {
    let request = channel::test_request(channel).map_err(TestError::Failed)?;
    let response = timeout(Duration::from_secs(15), send(&notifier.client, request))
        .await
        .map_err(|_| TestError::Failed("notification test timed out".to_owned()))?
        .map_err(TestError::Failed)?;
    if channel::test_accepts(channel, response.status, &response.body) {
        return Ok(());
    }
    Err(TestError::Failed(format!(
        "notification endpoint rejected test with HTTP {}",
        response.status.as_u16(),
    )))
}

pub use channel::TestChannel;
