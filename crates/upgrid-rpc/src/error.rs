use std::error::Error;
use std::sync::Arc;
use std::{fmt, io};

use snafu::Snafu;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Read,
    Ready,
    Write,
    Flush,
    Close,
}

impl fmt::Display for Operation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Read => "read",
            Self::Ready => "write readiness",
            Self::Write => "write",
            Self::Flush => "flush",
            Self::Close => "close",
        };
        formatter.write_str(name)
    }
}

/// A transport failure that stops one RPC channel.
#[derive(Clone, Debug, Snafu)]
#[snafu(display("RPC transport {operation} failed: {source}"))]
pub struct TransportError {
    operation: Operation,
    source: Arc<io::Error>,
}

impl TransportError {
    fn new(operation: Operation, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            operation,
            source: Arc::new(io::Error::other(source)),
        }
    }

    pub(crate) fn read(source: impl Error + Send + Sync + 'static) -> Self {
        Self::new(Operation::Read, source)
    }

    pub(crate) fn ready(source: impl Error + Send + Sync + 'static) -> Self {
        Self::new(Operation::Ready, source)
    }

    pub(crate) fn write(source: impl Error + Send + Sync + 'static) -> Self {
        Self::new(Operation::Write, source)
    }

    pub(crate) fn flush(source: impl Error + Send + Sync + 'static) -> Self {
        Self::new(Operation::Flush, source)
    }

    pub(crate) fn close(source: impl Error + Send + Sync + 'static) -> Self {
        Self::new(Operation::Close, source)
    }
}

/// A failure reported to one RPC caller.
#[derive(Debug, Snafu)]
pub enum CallError {
    #[snafu(display("RPC client is shut down"))]
    Shutdown,

    #[snafu(transparent)]
    Transport { source: TransportError },

    #[snafu(display("RPC deadline exceeded"))]
    DeadlineExceeded,

    #[snafu(display("RPC request identifier space is exhausted"))]
    RequestIdExhausted,

    #[snafu(display("RPC response does not match request `{expected}`; received `{received}`"))]
    UnexpectedResponse {
        expected: &'static str,
        received: &'static str,
    },
}
