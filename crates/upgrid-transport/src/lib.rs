//! Network transport primitives shared by UpGrid crates.

mod error;
mod framing;
mod rpc;
mod tls;

pub use error::{Error, Result};
pub use framing::{FramedConn, accept_framed, bi_stream_framed};
pub use rpc::{RpcSession, RpcTransport};
#[cfg(feature = "test-util")]
pub use tls::insecure_endpoint;
pub use tls::{SkipServerVerification, secure_endpoint};
