use std::io;

use compio::io::framed::{
    Framed,
    codec::{Decoder, Encoder},
    frame::LengthDelimited,
};
use compio_quic::{Connection, RecvStream, SendStream};
use openraft_rt_compio::futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use snafu::prelude::*;
use tarpc::Transport;

use crate::{QuicIncomingStreamSnafu, Result};

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

impl<Item: Serialize> Encoder<Item> for PostcardCodec {
    type Error = PostcardCodecError;

    fn encode(&mut self, item: Item, buf: &mut Vec<u8>) -> Result<(), PostcardCodecError> {
        postcard::to_io(&item, buf)
            .context(PostcardSnafu)
            .map(|_| ())
    }
}

impl<Item: DeserializeOwned> Decoder<Item> for PostcardCodec {
    type Error = PostcardCodecError;

    fn decode(&mut self, buf: &[u8]) -> Result<Item, PostcardCodecError> {
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
            let (send, recv) = connection.accept_bi().await.context(QuicIncomingStreamSnafu)?;
            let framed = bi_stream_framed(send, recv);
            yield framed;
        }
    }
}
