//! Typed, multiplexed RPC over a caller-supplied transport.

#![allow(async_fn_in_trait)]

mod cancel;
pub mod client;
mod context;
mod error;
mod macros;
pub mod server;
mod transport;

pub use context::Context;
pub use error::{CallError, CallFailure, TransportError, TransportStage};
#[doc(hidden)]
pub use serde as __serde;
use serde::{Deserialize, Serialize};
pub use transport::Transport;

/// A message from an RPC client to a server.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientMessage<T> {
    /// A new request.
    Request(Request<T>),

    /// A request cancellation.
    Cancel { request_id: u64 },
}

/// A request sent over one transport channel.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request<T> {
    pub context: Context,
    pub id: u64,
    pub message: T,
}

/// Provides the stable name of an RPC request.
pub trait RequestName {
    fn name(&self) -> &'static str;
}

/// A response sent over one transport channel.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response<T> {
    pub request_id: u64,
    pub message: T,
}
