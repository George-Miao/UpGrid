use std::error::Error;
use std::sync::Arc;
use std::{fmt, io};

use serde::{Deserialize, Serialize};
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportStage {
    Read,
    Ready,
    Write,
    Flush,
    Close,
}

impl From<Operation> for TransportStage {
    fn from(operation: Operation) -> Self {
        match operation {
            Operation::Read => Self::Read,
            Operation::Ready => Self::Ready,
            Operation::Write => Self::Write,
            Operation::Flush => Self::Flush,
            Operation::Close => Self::Close,
        }
    }
}

impl fmt::Display for TransportStage {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallFailure {
    Shutdown,
    Transport {
        stage: TransportStage,
        diagnostic: String,
    },
    DeadlineExceeded,
    RequestIdExhausted,
    UnexpectedResponse {
        expected: String,
        received: String,
    },
}

impl From<CallError> for CallFailure {
    fn from(error: CallError) -> Self {
        match error {
            CallError::Shutdown => Self::Shutdown,
            CallError::Transport { source } => Self::Transport {
                stage: source.operation.into(),
                diagnostic: source.source.to_string(),
            },
            CallError::DeadlineExceeded => Self::DeadlineExceeded,
            CallError::RequestIdExhausted => Self::RequestIdExhausted,
            CallError::UnexpectedResponse { expected, received } => Self::UnexpectedResponse {
                expected: expected.to_owned(),
                received: received.to_owned(),
            },
        }
    }
}

impl fmt::Display for CallFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => formatter.write_str("RPC client is shut down"),
            Self::Transport { stage, diagnostic } => {
                write!(formatter, "RPC transport {stage} failed: {diagnostic}")
            }
            Self::DeadlineExceeded => formatter.write_str("RPC deadline exceeded"),
            Self::RequestIdExhausted => {
                formatter.write_str("RPC request identifier space is exhausted")
            }
            Self::UnexpectedResponse { expected, received } => write!(
                formatter,
                "RPC response does not match request `{expected}`; received `{received}`"
            ),
        }
    }
}

impl std::error::Error for CallFailure {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_failure_preserves_the_transport_stage() {
        let failure = CallFailure::from(CallError::Transport {
            source: TransportError::read(io::Error::other("closed")),
        });

        assert!(matches!(
            &failure,
            CallFailure::Transport {
                stage: TransportStage::Read,
                diagnostic,
            } if diagnostic == "closed"
        ));
        let encoded = postcard::to_stdvec(&failure).unwrap();
        assert_eq!(
            postcard::from_bytes::<CallFailure>(&encoded).unwrap(),
            failure
        );
    }
}
