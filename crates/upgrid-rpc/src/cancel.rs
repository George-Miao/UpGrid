use std::pin::Pin;
use std::task::{Context, Poll};

use futures_channel::mpsc;
use futures_util::{Stream, StreamExt};

#[derive(Clone, Debug)]
pub(crate) struct Cancellation(mpsc::UnboundedSender<u64>);

#[derive(Debug)]
pub(crate) struct Cancellations(mpsc::UnboundedReceiver<u64>);

pub(crate) fn channel() -> (Cancellation, Cancellations) {
    let (sender, receiver) = mpsc::unbounded();
    (Cancellation(sender), Cancellations(receiver))
}

impl Cancellation {
    pub(crate) fn send(&self, request_id: u64) {
        let _ = self.0.unbounded_send(request_id);
    }
}

impl Stream for Cancellations {
    type Item = u64;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(context)
    }
}
