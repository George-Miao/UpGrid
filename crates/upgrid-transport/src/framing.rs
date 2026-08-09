//! Length-delimited Postcard frames over QUIC streams.

use std::io;

use compio::buf::{IoBuf, IoBufMut, IoBufMutExt, Slice};
use compio::io::framed::Framed;
use compio::io::framed::codec::{Decoder, Encoder};
use compio::io::framed::frame::LengthDelimited;
use compio_quic::{Connection, RecvStream, SendStream};
use futures_core::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::prelude::*;
use tarpc::Transport;

use crate::Result;
use crate::error::QuicIncomingSnafu;

const _: () = {
    const fn assert_is_transport<T: Transport<(), ()>>() {}

    assert_is_transport::<FramedConn<(), ()>>();
};

#[derive(Debug, Snafu)]
pub enum PostcardCodecError {
    #[snafu(display("Postcard error: {source}"))]
    PostcardError { source: postcard::Error },
    #[snafu(display("IO error: {source}"))]
    IoError { source: io::Error },
}

impl From<io::Error> for PostcardCodecError {
    fn from(source: io::Error) -> Self {
        PostcardCodecError::IoError { source }
    }
}

pub struct PostcardCodec {}

impl<Item: Serialize, B: IoBufMut> Encoder<Item, B> for PostcardCodec {
    type Error = PostcardCodecError;

    fn encode(&mut self, item: Item, buf: &mut B) -> Result<(), PostcardCodecError> {
        postcard::to_io(&item, buf.as_writer())
            .context(PostcardSnafu)
            .map(|_| ())
    }
}

impl<Item: DeserializeOwned, B: IoBuf> Decoder<Item, B> for PostcardCodec {
    type Error = PostcardCodecError;

    fn decode(&mut self, buf: &Slice<B>) -> Result<Item, PostcardCodecError> {
        postcard::from_bytes(buf).context(PostcardSnafu)
    }
}

pub type FramedConn<In, Out> =
    Framed<RecvStream, SendStream, PostcardCodec, LengthDelimited, In, Out>;

pub fn bi_stream_framed<In, Out>(send: SendStream, recv: RecvStream) -> FramedConn<In, Out> {
    Framed::new(PostcardCodec {}, LengthDelimited::new())
        .with_reader(recv)
        .with_writer(send)
}

pub fn accept_framed<In, Out>(
    connection: Connection,
) -> impl Stream<Item = Result<FramedConn<In, Out>>> {
    async_stream::try_stream! {
        loop {
            let (send, recv) = connection.accept_bi().await.context(QuicIncomingSnafu)?;
            let framed = bi_stream_framed(send, recv);
            yield framed;
        }
    }
}
