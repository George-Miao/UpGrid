use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_channel::mpsc;
use futures_util::stream::Fuse;
use futures_util::{Sink, Stream, StreamExt};
use pin_project_lite::pin_project;

use super::DispatchRequest;
use super::in_flight::InFlightRequests;
use crate::cancel::Cancellations;
use crate::{CallError, ClientMessage, Request, Response, Transport, TransportError};

pin_project! {
    /// Drives one client transport and all requests sent through it.
    #[derive(Debug)]
    #[must_use = "the RPC client does not make progress unless its dispatch is polled"]
    pub struct Dispatch<Req, Resp, T> {
        #[pin]
        transport: Fuse<T>,
        pending_requests: mpsc::Receiver<DispatchRequest<Req, Resp>>,
        cancellations: Cancellations,
        in_flight: InFlightRequests<Result<Resp, CallError>>,
        max_in_flight: usize,
        terminal_error: Option<TransportError>,
    }
}

impl<Req, Resp, T> Dispatch<Req, Resp, T>
where
    T: Transport<ClientMessage<Req>, Response<Resp>>,
{
    pub(super) fn new(
        transport: T,
        pending_requests: mpsc::Receiver<DispatchRequest<Req, Resp>>,
        cancellations: Cancellations,
        max_in_flight: usize,
    ) -> Self {
        Self {
            transport: transport.fuse(),
            pending_requests,
            cancellations,
            in_flight: InFlightRequests::default(),
            max_in_flight,
            terminal_error: None,
        }
    }

    fn transport<'a>(self: &'a mut Pin<&mut Self>) -> Pin<&'a mut Fuse<T>> {
        self.as_mut().project().transport
    }

    fn pending_requests<'a>(
        self: &'a mut Pin<&mut Self>,
    ) -> &'a mut mpsc::Receiver<DispatchRequest<Req, Resp>> {
        self.as_mut().project().pending_requests
    }

    fn cancellations<'a>(self: &'a mut Pin<&mut Self>) -> &'a mut Cancellations {
        self.as_mut().project().cancellations
    }

    fn in_flight<'a>(
        self: &'a mut Pin<&mut Self>,
    ) -> &'a mut InFlightRequests<Result<Resp, CallError>> {
        self.as_mut().project().in_flight
    }

    fn poll_ready(
        self: &mut Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        self.transport()
            .poll_ready(context)
            .map_err(TransportError::ready)
    }

    fn poll_flush(
        self: &mut Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        self.transport()
            .poll_flush(context)
            .map_err(TransportError::flush)
    }

    fn poll_close(
        self: &mut Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        self.transport()
            .poll_close(context)
            .map_err(TransportError::close)
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<(), TransportError>>> {
        match self.transport().poll_next(context) {
            Poll::Ready(Some(Ok(response))) => {
                self.complete(response);
                Poll::Ready(Some(Ok(())))
            }
            Poll::Ready(Some(Err(source))) => Poll::Ready(Some(Err(TransportError::read(source)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<(), TransportError>>> {
        #[derive(Clone, Copy)]
        enum InputStatus {
            Pending,
            Closed,
        }

        let requests = match self.as_mut().poll_send_request(context)? {
            Poll::Ready(Some(())) => return Poll::Ready(Some(Ok(()))),
            Poll::Ready(None) => InputStatus::Closed,
            Poll::Pending => InputStatus::Pending,
        };
        let cancellations = match self.as_mut().poll_send_cancellation(context)? {
            Poll::Ready(Some(())) => return Poll::Ready(Some(Ok(()))),
            Poll::Ready(None) => InputStatus::Closed,
            Poll::Pending => InputStatus::Pending,
        };

        match (requests, cancellations) {
            (InputStatus::Closed, InputStatus::Closed) => {
                ready!(self.poll_close(context))?;
                Poll::Ready(None)
            }
            _ => {
                ready!(self.poll_flush(context))?;
                Poll::Pending
            }
        }
    }

    fn poll_next_request(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<DispatchRequest<Req, Resp>, TransportError>>> {
        if self.in_flight().len() >= *self.as_mut().project().max_in_flight {
            return Poll::Pending;
        }
        ready!(self.as_mut().ensure_writable(context))?;

        loop {
            match ready!(self.pending_requests().poll_next_unpin(context)) {
                Some(request) if request.reply.is_canceled() => {
                    let _entered = request.span.enter();
                    tracing::debug!("RPC request was canceled before dispatch");
                }
                Some(request) => return Poll::Ready(Some(Ok(request))),
                None => return Poll::Ready(None),
            }
        }
    }

    fn poll_next_cancellation(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<(tracing::Span, u64), TransportError>>> {
        ready!(self.as_mut().ensure_writable(context))?;

        loop {
            match ready!(self.cancellations().poll_next_unpin(context)) {
                Some(request_id) => {
                    if let Some(span) = self.in_flight().cancel(request_id) {
                        return Poll::Ready(Some(Ok((span, request_id))));
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }

    fn ensure_writable(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        loop {
            match self.as_mut().poll_ready(context) {
                Poll::Ready(result) => return Poll::Ready(result),
                Poll::Pending => ready!(self.as_mut().poll_flush(context))?,
            }
        }
    }

    fn poll_send_request(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<(), TransportError>>> {
        let DispatchRequest {
            context: call_context,
            span,
            request_id,
            request,
            reply,
        } = match ready!(self.as_mut().poll_next_request(context)?) {
            Some(request) => request,
            None => return Poll::Ready(None),
        };
        let message = ClientMessage::Request(Request {
            context: call_context,
            id: request_id,
            message: request,
        });
        if let Err(reply) = self.in_flight().insert(request_id, span.clone(), reply) {
            let _ = reply.send(Err(CallError::RequestIdExhausted));
            return Poll::Ready(Some(Ok(())));
        }

        let _entered = span.enter();
        match self.transport().start_send(message) {
            Ok(()) => tracing::debug!("sent RPC request"),
            Err(source) => {
                let error = TransportError::write(source);
                self.in_flight()
                    .complete(request_id, Err(CallError::Transport { source: error }));
            }
        }
        Poll::Ready(Some(Ok(())))
    }

    fn poll_send_cancellation(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<(), TransportError>>> {
        let (span, request_id) = match ready!(self.as_mut().poll_next_cancellation(context)?) {
            Some(cancellation) => cancellation,
            None => return Poll::Ready(None),
        };
        let _entered = span.enter();
        self.transport()
            .start_send(ClientMessage::Cancel { request_id })
            .map_err(TransportError::write)?;
        tracing::debug!("sent RPC cancellation");
        Poll::Ready(Some(Ok(())))
    }

    fn complete(mut self: Pin<&mut Self>, response: Response<Resp>) {
        if let Some(span) = self
            .in_flight()
            .complete(response.request_id, Ok(response.message))
        {
            let _entered = span.enter();
            tracing::debug!("received RPC response");
        }
    }

    fn shut_down(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        error: TransportError,
    ) -> Poll<()> {
        self.pending_requests().close();
        for span in self.in_flight().drain(|| {
            Err(CallError::Transport {
                source: error.clone(),
            })
        }) {
            let _entered = span.enter();
            tracing::debug!("RPC request stopped after transport failure");
        }

        loop {
            match ready!(self.pending_requests().poll_next_unpin(context)) {
                Some(request) if request.reply.is_canceled() => {}
                Some(request) => {
                    let _ = request.reply.send(Err(CallError::Transport {
                        source: error.clone(),
                    }));
                }
                None => return Poll::Ready(()),
            }
        }
    }

    fn run(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        loop {
            match (
                self.as_mut().poll_read(context)?,
                self.as_mut().poll_write(context)?,
            ) {
                (Poll::Ready(None), _) => return Poll::Ready(Ok(())),
                (read, Poll::Ready(None)) => {
                    if self.in_flight().is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    if matches!(read, Poll::Ready(Some(()))) {
                        continue;
                    }
                    return Poll::Pending;
                }
                (Poll::Ready(Some(())), _) | (_, Poll::Ready(Some(()))) => {}
                _ => return Poll::Pending,
            }
        }
    }
}

impl<Req, Resp, T> Future for Dispatch<Req, Resp, T>
where
    T: Transport<ClientMessage<Req>, Response<Resp>>,
{
    type Output = Result<(), TransportError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(error) = self.as_mut().project().terminal_error.clone() {
                ready!(self.as_mut().shut_down(context, error.clone()));
                return Poll::Ready(Err(error));
            }
            match ready!(self.as_mut().run(context)) {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(error) => *self.as_mut().project().terminal_error = Some(error),
            }
        }
    }
}
