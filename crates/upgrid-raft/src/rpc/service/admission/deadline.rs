use std::future::Future;
use std::time::{Duration, Instant};

use compio::time::timeout;
use upgrid_rpc::Context;

use super::super::JoinError;

const SETTLEMENT_GRACE: Duration = Duration::from_secs(1);

pub(super) async fn before_deadline<T>(
    context: &Context,
    future: impl Future<Output = T>,
) -> Result<T, JoinError> {
    let remaining = context.deadline().saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(JoinError::Deadline);
    }
    timeout(remaining, future)
        .await
        .map_err(|_| JoinError::Deadline)
}

pub(super) async fn settle_before_deadline<T>(
    context: &Context,
    future: impl Future<Output = T>,
) -> Result<T, JoinError> {
    settle_with_grace(context, future, SETTLEMENT_GRACE).await
}

async fn settle_with_grace<T>(
    context: &Context,
    future: impl Future<Output = T>,
    grace: Duration,
) -> Result<T, JoinError> {
    let remaining = context.deadline().saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(JoinError::Deadline);
    }
    timeout(remaining.saturating_add(grace), future)
        .await
        .map_err(|_| JoinError::Deadline)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[compio::test]
    async fn started_operation_settles_during_the_grace_period() {
        let context = Context::with_deadline(Instant::now() + Duration::from_millis(1));
        let result = settle_with_grace(
            &context,
            async {
                compio::time::sleep(Duration::from_millis(10)).await;
                42
            },
            Duration::from_millis(20),
        )
        .await;

        assert_eq!(result.unwrap(), 42);
    }

    #[compio::test]
    async fn stalled_operation_stops_after_the_grace_period() {
        let context = Context::with_deadline(Instant::now() + Duration::from_millis(1));
        let result = settle_with_grace(
            &context,
            std::future::pending::<()>(),
            Duration::from_millis(1),
        )
        .await;

        assert!(matches!(result, Err(JoinError::Deadline)));
    }
}
