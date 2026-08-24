use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::{Context, JoinError, before_deadline, operation_context};

#[test]
fn admission_keeps_the_callers_earlier_deadline() {
    let deadline = Instant::now() + Duration::from_secs(1);

    let context = operation_context(Context::with_deadline(deadline));

    assert_eq!(context.deadline(), deadline);
}

#[compio::test]
async fn elapsed_deadline_does_not_start_admission_work() {
    let polled = Rc::new(Cell::new(false));
    let mark_polled = polled.clone();
    let work = async move {
        mark_polled.set(true);
    };

    let result = before_deadline(&Context::with_deadline(Instant::now()), work).await;

    assert!(matches!(result, Err(JoinError::Deadline)));
    assert!(!polled.get());
}

#[compio::test]
async fn pending_admission_work_stops_at_deadline() {
    let context = Context::with_deadline(Instant::now() + Duration::from_millis(20));
    let started = Instant::now();

    let result = before_deadline(&context, std::future::pending::<()>()).await;

    assert!(matches!(result, Err(JoinError::Deadline)));
    assert!(started.elapsed() < Duration::from_secs(1));
}
