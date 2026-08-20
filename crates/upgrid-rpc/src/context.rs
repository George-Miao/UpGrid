//! Request deadline context.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Request-scoped RPC data that crosses the transport.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Context {
    #[serde(with = "deadline")]
    deadline: Instant,
}

impl Context {
    /// Creates a context with the default deadline.
    pub fn current() -> Self {
        Self::default()
    }

    /// Creates a context with an explicit deadline.
    pub const fn with_deadline(deadline: Instant) -> Self {
        Self { deadline }
    }

    /// Returns the request deadline.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::with_deadline(Instant::now() + DEFAULT_TIMEOUT)
    }
}

mod deadline {
    use std::time::{Duration, Instant};

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(deadline: &Instant, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        deadline
            .saturating_duration_since(Instant::now())
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Instant, D::Error>
    where
        D: Deserializer<'de>,
    {
        let duration = Duration::deserialize(deserializer)?;
        Instant::now()
            .checked_add(duration)
            .ok_or_else(|| serde::de::Error::custom("RPC deadline is outside the supported range"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_deadline_as_remaining_time() {
        let context = Context::with_deadline(Instant::now() + Duration::from_secs(5));
        let bytes = postcard::to_stdvec(&context).expect("context must serialize");
        let decoded: Context = postcard::from_bytes(&bytes).expect("context must deserialize");

        assert!(decoded.remaining() <= Duration::from_secs(5));
        assert!(decoded.remaining() > Duration::from_secs(4));
    }

    #[test]
    fn serializes_elapsed_deadline_without_panicking() {
        let context = Context::with_deadline(Instant::now() - Duration::from_secs(1));
        let bytes = postcard::to_stdvec(&context).expect("elapsed context must serialize");
        let decoded: Context = postcard::from_bytes(&bytes).expect("context must deserialize");

        assert!(decoded.remaining() < Duration::from_millis(10));
    }
}
