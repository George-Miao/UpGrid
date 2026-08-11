use std::time::Duration;

use compio::runtime::spawn;
use compio::time::timeout;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{mpsc, oneshot};

use super::{ChannelError, Notifier, SendError, channel, send};

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
            .ok()
            .context(UnavailableSnafu)?;
        response.await.ok().context(UnavailableSnafu)?
    }
}

/// Failure to execute or accept a channel test.
#[derive(Debug, Snafu)]
pub enum TestError {
    #[snafu(display("notification runtime unavailable"))]
    Unavailable,

    #[snafu(display("{source}"))]
    Channel { source: ChannelError },

    #[snafu(display("notification test timed out"))]
    Timeout,

    #[snafu(display("{source}"))]
    Send { source: SendError },

    #[snafu(display("notification endpoint rejected test with HTTP {status}"))]
    Rejected { status: u16 },
}

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
    let request = channel::test_request(channel).context(ChannelSnafu)?;
    let response = match timeout(Duration::from_secs(15), send(&notifier.client, request)).await {
        Ok(result) => result.context(SendSnafu)?,
        Err(_) => return Err(TestError::Timeout),
    };
    if channel::test_accepts(channel, response.status, &response.body) {
        return Ok(());
    }
    Err(TestError::Rejected {
        status: response.status.as_u16(),
    })
}

pub use channel::TestChannel;
