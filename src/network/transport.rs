use std::io;

use compio::{
    io::framed::{
        Framed,
        codec::{Decoder, Encoder},
        frame::LengthDelimited,
    },
    net::{TcpListener, TcpStream, ToSocketAddrsAsync},
};
use openraft_rt_compio::futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use snafu::prelude::*;
use tarpc::Transport;
use tracing::debug;

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

    fn encode(&mut self, item: Item, buf: &mut Vec<u8>) -> Result<(), Self::Error> {
        postcard::to_io(&item, buf)
            .context(PostcardSnafu)
            .map(|_| ())
    }
}

impl<Item: DeserializeOwned> Decoder<Item> for PostcardCodec {
    type Error = PostcardCodecError;

    fn decode(&mut self, buf: &[u8]) -> Result<Item, Self::Error> {
        postcard::from_bytes(buf).context(PostcardSnafu)
    }
}

pub type FramedConn<In, Out> = Framed<TcpStream, PostcardCodec, LengthDelimited, In, Out>;

pub async fn connect_framed<In, Out>(
    addr: impl ToSocketAddrsAsync,
) -> io::Result<FramedConn<In, Out>> {
    let stream = TcpStream::connect(addr).await?;
    Ok(Framed::new(
        stream,
        PostcardCodec {},
        LengthDelimited::new(),
    ))
}

pub fn listen_framed<Io, Out>(
    addr: impl ToSocketAddrsAsync,
) -> impl Stream<Item = io::Result<Framed<TcpStream, PostcardCodec, LengthDelimited, Io, Out>>> {
    async_stream::try_stream! {
        let listener = TcpListener::bind(addr).await?;
        debug!(?listener, "Listening for connections");

        loop {
            let (stream, addr) = listener.accept().await?;
            debug!(?addr, "Accepted connection");
            let framed = Framed::new(stream, PostcardCodec {}, LengthDelimited::new());
            yield framed;
        }
    }
}
