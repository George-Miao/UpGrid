//! Multiplexed RPC server.

mod in_flight;

use std::error::Error;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use futures_util::future::{Abortable, LocalBoxFuture, select};
use futures_util::stream::{Fuse, FuturesUnordered};
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tracing::{Instrument, Span};

use self::in_flight::InFlightRequests;
use crate::{ClientMessage, Context, RequestName, Response, Transport, TransportError};

/// A typed RPC service.
pub trait Service: Clone + 'static {
    type Request: RequestName + 'static;
    type Response: 'static;

    /// Handles one request.
    async fn serve(&self, context: Context, request: Self::Request) -> Self::Response;
}

/// A failure produced by the RPC server before a service returns.
#[derive(Clone, Debug, Deserialize, Serialize, Snafu)]
pub enum ServerError {
    #[snafu(display("RPC deadline exceeded"))]
    DeadlineExceeded,
}

pin_project! {
    /// Adapts a typed transport into a stream of RPC requests.
    #[derive(Debug)]
    pub struct Channel<T, Request, ResponseBody> {
        #[pin]
        transport: Fuse<T>,
        in_flight: InFlightRequests,
        protocol: PhantomData<fn(Request) -> ResponseBody>,
    }
}

impl<T, Request, ResponseBody> Channel<T, Request, ResponseBody> {
    /// Creates a server channel over a transport.
    pub fn new(transport: T) -> Self
    where
        T: Stream,
    {
        Self {
            transport: transport.fuse(),
            in_flight: InFlightRequests::default(),
            protocol: PhantomData,
        }
    }
}

impl<T, Request, ResponseBody> Stream for Channel<T, Request, ResponseBody>
where
    Request: RequestName,
    T: Transport<Response<Result<ResponseBody, ServerError>>, ClientMessage<Request>>,
{
    type Item = Result<RequestHandler<Request>, TransportError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.transport.as_mut().poll_next(context) {
                Poll::Ready(Some(Ok(ClientMessage::Request(request)))) => {
                    let span = tracing::info_span!(
                        "rpc.server",
                        rpc.method = request.message.name(),
                        rpc.request_id = request.id,
                    );
                    return Poll::Ready(Some(Ok(RequestHandler {
                        context: request.context,
                        request_id: request.id,
                        request: request.message,
                        span,
                    })));
                }
                Poll::Ready(Some(Ok(ClientMessage::Cancel { request_id }))) => {
                    if let Some(span) = this.in_flight.cancel(request_id) {
                        let _entered = span.enter();
                        tracing::debug!("canceled RPC request");
                    }
                }
                Poll::Ready(Some(Err(source))) => {
                    return Poll::Ready(Some(Err(TransportError::read(source))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T, Request, ResponseBody> Sink<ResponseGuard<ResponseBody>>
    for Channel<T, Request, ResponseBody>
where
    T: Stream + Sink<Response<Result<ResponseBody, ServerError>>>,
    T::Error: Error + Send + Sync + 'static,
{
    type Error = TransportError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project()
            .transport
            .poll_ready(context)
            .map_err(TransportError::ready)
    }

    fn start_send(
        self: Pin<&mut Self>,
        response: ResponseGuard<ResponseBody>,
    ) -> Result<(), Self::Error> {
        let this = self.project();
        let Some(span) = this.in_flight.remove(response.request_id) else {
            return Ok(());
        };
        let _entered = span.enter();
        this.transport
            .start_send(Response {
                request_id: response.request_id,
                message: response.message,
            })
            .map_err(TransportError::write)?;
        tracing::debug!("sent RPC response");
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project()
            .transport
            .poll_flush(context)
            .map_err(TransportError::flush)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.project()
            .transport
            .poll_close(context)
            .map_err(TransportError::close)
    }
}

impl<T, Request, ResponseBody> Channel<T, Request, ResponseBody> {
    fn start_request(
        &mut self,
        request: &RequestHandler<Request>,
    ) -> Option<futures_util::future::AbortRegistration> {
        self.in_flight
            .insert(request.request_id, request.span.clone())
    }
}

/// One request received by a server channel.
#[derive(Debug)]
pub struct RequestHandler<Request> {
    context: Context,
    request_id: u64,
    request: Request,
    span: Span,
}

/// A response that retains request lifecycle state until it is sent.
#[derive(Debug)]
pub struct ResponseGuard<ResponseBody> {
    request_id: u64,
    message: Result<ResponseBody, ServerError>,
}

impl<ResponseBody> ResponseGuard<ResponseBody> {
    fn success(request_id: u64, message: ResponseBody) -> Self {
        Self {
            request_id,
            message: Ok(message),
        }
    }

    fn deadline_exceeded(request_id: u64) -> Self {
        Self {
            request_id,
            message: Err(ServerError::DeadlineExceeded),
        }
    }
}

/// Runs a service on a server channel.
#[derive(Debug)]
#[must_use = "the RPC server does not make progress unless it is polled"]
pub struct Server<T, S>
where
    S: Service,
{
    channel: Channel<T, S::Request, S::Response>,
    service: S,
    responses: FuturesUnordered<LocalBoxFuture<'static, Option<ResponseGuard<S::Response>>>>,
}

impl<T, Request, ResponseBody> Channel<T, Request, ResponseBody> {
    /// Creates a server future that executes all requests with `service`.
    pub fn execute<S>(self, service: S) -> Server<T, S>
    where
        S: Service<Request = Request, Response = ResponseBody>,
    {
        Server {
            channel: self,
            service,
            responses: FuturesUnordered::new(),
        }
    }
}

impl<T, S> Server<T, S>
where
    S: Service,
    T: Transport<Response<Result<S::Response, ServerError>>, ClientMessage<S::Request>>
        + Unpin
        + 'static,
{
    fn respond(&mut self, request: RequestHandler<S::Request>) {
        let Some(registration) = self.channel.start_request(&request) else {
            tracing::warn!(
                request_id = request.request_id,
                "ignored duplicate RPC request identifier"
            );
            return;
        };
        let RequestHandler {
            context,
            request_id,
            request,
            span,
        } = request;
        let service = self.service.clone();
        let call_span = span.clone();
        let call = async move {
            let response = service.serve(context, request).instrument(call_span).await;
            ResponseGuard::success(request_id, response)
        };
        let deadline = async move {
            compio::time::sleep_until(context.deadline()).await;
            ResponseGuard::deadline_exceeded(request_id)
        };
        let response = async move {
            futures_util::pin_mut!(call, deadline);
            match select(call, deadline).await {
                futures_util::future::Either::Left((response, _))
                | futures_util::future::Either::Right((response, _)) => response,
            }
        };
        self.responses.push(
            Abortable::new(response, registration)
                .map(Result::ok)
                .boxed_local(),
        );
    }

    /// Runs until the transport closes or fails.
    pub async fn run(mut self) -> Result<(), TransportError> {
        loop {
            if self.responses.is_empty() {
                match self.channel.next().await {
                    Some(Ok(request)) => self.respond(request),
                    Some(Err(error)) => return Err(error),
                    None => return Ok(()),
                }
                continue;
            }

            let request = self.channel.next().fuse();
            let response = self.responses.next().fuse();
            futures_util::pin_mut!(request, response);
            futures_util::select! {
                request = request => match request {
                    Some(Ok(request)) => self.respond(request),
                    Some(Err(error)) => return Err(error),
                    None => return Ok(()),
                },
                response = response => {
                    if let Some(Some(response)) = response {
                        self.channel.send(response).await?;
                    }
                },
            }
        }
    }
}
