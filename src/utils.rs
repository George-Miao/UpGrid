use uuid::{ContextV7, Timestamp, Uuid};

#[thread_local]
static CONTEXT: ContextV7 = ContextV7::new();

pub fn uuid_v7_now() -> Uuid {
    Uuid::new_v7(Timestamp::now(&CONTEXT))
}
