//! Multiplexed RPC client.

mod dispatch;
mod in_flight;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub use dispatch::Dispatch;
use futures_channel::{mpsc, oneshot};
use futures_util::SinkExt;
use tracing::Span;

use crate::cancel::{Cancellation, channel as cancellation_channel};
use crate::{CallError, Context, RequestName, Transport};

const MAX_IN_FLIGHT_REQUESTS: usize = 1_024;
const PENDING_REQUEST_BUFFER: usize = 8;

/// A cloneable handle that sends typed requests through one RPC channel.
#[derive(Debug)]
pub struct Client<Request, Response> {
    dispatch: mpsc::Sender<DispatchRequest<Request, Response>>,
    cancellation: Cancellation,
    next_request_id: Arc<AtomicU64>,
}

impl<Request, Response> Clone for Client<Request, Response> {
    fn clone(&self) -> Self {
        Self {
            dispatch: self.dispatch.clone(),
            cancellation: self.cancellation.clone(),
            next_request_id: self.next_request_id.clone(),
        }
    }
}

/// Creates a client and the task that drives its transport.
///
/// The caller must poll or spawn the returned [`Dispatch`].
pub fn new<Request, Response, T>(
    transport: T,
) -> (Client<Request, Response>, Dispatch<Request, Response, T>)
where
    T: Transport<crate::ClientMessage<Request>, crate::Response<Response>>,
{
    let (dispatch, pending_requests) = mpsc::channel(PENDING_REQUEST_BUFFER);
    let (cancellation, cancellations) = cancellation_channel();
    let client = Client {
        dispatch,
        cancellation,
        next_request_id: Arc::new(AtomicU64::new(0)),
    };
    let dispatch = Dispatch::new(
        transport,
        pending_requests,
        cancellations,
        MAX_IN_FLIGHT_REQUESTS,
    );
    (client, dispatch)
}

impl<Request, Response> Client<Request, Response>
where
    Request: RequestName,
{
    /// Sends one request and waits for its response or deadline.
    #[tracing::instrument(name = "rpc.client", skip_all, fields(rpc.method = request.name()))]
    pub async fn call(&self, context: Context, request: Request) -> Result<Response, CallError> {
        let request_id = self
            .next_request_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| CallError::RequestIdExhausted)?;
        let timeout = context.remaining();
        let (reply, response) = oneshot::channel();
        let response = ResponseGuard {
            response,
            cancellation: self.cancellation.clone(),
            request_id,
            cancel: true,
        };
        let request = DispatchRequest {
            context,
            span: Span::current(),
            request_id,
            request,
            reply,
        };
        let mut dispatch = self.dispatch.clone();
        let call = async move {
            dispatch
                .send(request)
                .await
                .map_err(|_| CallError::Shutdown)?;
            response.wait().await
        };

        compio::time::timeout(timeout, call)
            .await
            .map_err(|_| CallError::DeadlineExceeded)?
    }
}

struct ResponseGuard<Response> {
    response: oneshot::Receiver<Result<Response, CallError>>,
    cancellation: Cancellation,
    request_id: u64,
    cancel: bool,
}

impl<Response> ResponseGuard<Response> {
    async fn wait(mut self) -> Result<Response, CallError> {
        let response = (&mut self.response).await;
        self.cancel = false;
        response.map_err(|_| CallError::Shutdown)?
    }
}

impl<Response> Drop for ResponseGuard<Response> {
    fn drop(&mut self) {
        self.response.close();
        if self.cancel {
            self.cancellation.send(self.request_id);
        }
    }
}

#[derive(Debug)]
pub(super) struct DispatchRequest<Request, Response> {
    context: Context,
    span: Span,
    request_id: u64,
    request: Request,
    reply: oneshot::Sender<Result<Response, CallError>>,
}
