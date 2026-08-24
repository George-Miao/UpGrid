//! Network transport primitives shared by UpGrid crates.

mod error;
mod framing;
mod rpc;
mod tls;

pub use error::{Error, Failure, Result};
pub use framing::{FramedConn, accept_framed, bi_stream_framed};
pub use rpc::{PeerAddress, PeerIdentity, RpcSession, RpcTransport};
pub use tls::{SkipServerVerification, secure_endpoints};
