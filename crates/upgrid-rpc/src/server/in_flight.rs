use std::collections::HashMap;
use std::collections::hash_map::Entry;

use futures_util::future::{AbortHandle, AbortRegistration};
use tracing::Span;

#[derive(Debug, Default)]
pub(super) struct InFlightRequests {
    requests: HashMap<u64, RequestData>,
}

#[derive(Debug)]
struct RequestData {
    abort: AbortHandle,
    span: Span,
}

impl InFlightRequests {
    pub(super) fn insert(&mut self, request_id: u64, span: Span) -> Option<AbortRegistration> {
        match self.requests.entry(request_id) {
            Entry::Vacant(entry) => {
                let (abort, registration) = AbortHandle::new_pair();
                entry.insert(RequestData { abort, span });
                Some(registration)
            }
            Entry::Occupied(_) => None,
        }
    }

    pub(super) fn remove(&mut self, request_id: u64) -> Option<Span> {
        self.requests
            .remove(&request_id)
            .map(|request| request.span)
    }

    pub(super) fn cancel(&mut self, request_id: u64) -> Option<Span> {
        let request = self.requests.remove(&request_id)?;
        request.abort.abort();
        Some(request.span)
    }
}

impl Drop for InFlightRequests {
    fn drop(&mut self) {
        for request in self.requests.values() {
            request.abort.abort();
        }
    }
}
