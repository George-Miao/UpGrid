use std::error::Error;

use futures_util::{Sink, Stream};

/// A bidirectional stream of typed RPC messages.
pub trait Transport<Out, In>
where
    Self: Stream<Item = Result<In, <Self as Sink<Out>>::Error>>,
    Self: Sink<Out, Error = <Self as Transport<Out, In>>::TransportError>,
    <Self as Sink<Out>>::Error: Error,
{
    type TransportError: Error + Send + Sync + 'static;
}

impl<T, Out, In, E> Transport<Out, In> for T
where
    T: ?Sized + Stream<Item = Result<In, E>> + Sink<Out, Error = E>,
    E: Error + Send + Sync + 'static,
{
    type TransportError = E;
}
