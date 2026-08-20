use std::collections::HashMap;
use std::collections::hash_map::Entry;

use futures_channel::oneshot;
use tracing::Span;

#[derive(Debug)]
pub(super) struct InFlightRequests<Response> {
    requests: HashMap<u64, RequestData<Response>>,
}

#[derive(Debug)]
struct RequestData<Response> {
    span: Span,
    reply: oneshot::Sender<Response>,
}

impl<Response> Default for InFlightRequests<Response> {
    fn default() -> Self {
        Self {
            requests: HashMap::new(),
        }
    }
}

impl<Response> InFlightRequests<Response> {
    pub(super) fn len(&self) -> usize {
        self.requests.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub(super) fn insert(
        &mut self,
        request_id: u64,
        span: Span,
        reply: oneshot::Sender<Response>,
    ) -> Result<(), oneshot::Sender<Response>> {
        match self.requests.entry(request_id) {
            Entry::Vacant(entry) => {
                entry.insert(RequestData { span, reply });
                Ok(())
            }
            Entry::Occupied(_) => Err(reply),
        }
    }

    pub(super) fn complete(&mut self, request_id: u64, response: Response) -> Option<Span> {
        let request = self.requests.remove(&request_id)?;
        let _ = request.reply.send(response);
        Some(request.span)
    }

    pub(super) fn cancel(&mut self, request_id: u64) -> Option<Span> {
        self.requests
            .remove(&request_id)
            .map(|request| request.span)
    }

    pub(super) fn drain<'a>(
        &'a mut self,
        mut response: impl FnMut() -> Response + 'a,
    ) -> impl Iterator<Item = Span> + 'a {
        self.requests.drain().map(move |(_, request)| {
            let _ = request.reply.send(response());
            request.span
        })
    }
}
