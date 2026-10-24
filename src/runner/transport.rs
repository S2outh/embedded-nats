use embedded_io_async::Write;
use embedded_nal_async::TcpConnect;

use crate::{
    AUTH_JSON_MAX_LEN, BytesBuf, CapacityError, DELIM, NatsAuthenticator, NatsCollections, StrBuf,
    U32_MAX_STR_LEN, runner::transport::framer::Framer,
};

mod framer;
pub use framer::Frame;

macro_rules! defmt {
    ($($t:tt)*) => {{
        #[cfg(feature = "defmt")]
        defmt::$($t)*;
    }};
}

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    #[error("Disconnected")]
    Disconnected,
    #[error("Tcp error: {0}")]
    Tcp(
        #[from]
        #[cfg_attr(feature = "defmt", defmt(Debug2Format))]
        E,
    ),
    #[error("Capacity of collection {0} not sufficient for operation")]
    Capacity(CapacityError),
    #[error("Json serialization error {0}")]
    Ser(serde_json_core::ser::Error),
    #[error("Json deserialization error {0}")]
    Deser(serde_json_core::de::Error),
    #[error("Invalid utf8")]
    Utf8,
    #[error("Invalid header")]
    Header,
}

impl<E> From<framer::Error<E>> for Error<E> {
    fn from(value: framer::Error<E>) -> Self {
        match value {
            framer::Error::Disconnected => Error::Disconnected,
            framer::Error::Read(e) => Error::Tcp(e),
            framer::Error::Capacity(e) => Error::Capacity(e),
            framer::Error::Deser(e) => Error::Deser(e),
            framer::Error::Utf8 => Error::Utf8,
            framer::Error::Header => Error::Header,
        }
    }
}

type Result<T, E> = core::result::Result<T, Error<E>>;

pub struct NatsTransport<'a, S: TcpConnect + 'a, C: NatsCollections, A: NatsAuthenticator> {
    socket: S::Connection<'a>,
    auth: &'a A,
    framer: Framer<C>,
}

impl<'a, S: TcpConnect, C: NatsCollections, A: NatsAuthenticator> NatsTransport<'a, S, C, A> {
    pub fn new(socket: S::Connection<'a>, auth: &'a A) -> Self {
        let framer = Framer::new();
        Self {
            socket,
            auth,
            framer,
        }
    }

    pub async fn pong(&mut self) -> Result<(), S::Error> {
        self.socket.write_all("PONG".as_bytes()).await?;
        self.socket.write_all(&DELIM).await?;
        Ok(())
    }

    pub async fn connect(&mut self) -> Result<(), S::Error> {
        // serialize the connect message body into AuthBuf
        let mut connect_msg = C::AuthBuf::default();
        let mut connect_msg_slice = connect_msg
            .reserve_slice(AUTH_JSON_MAX_LEN)
            .map_err(|e| Error::<S::Error>::Capacity(e))?;
        let len = serde_json_core::to_slice(&self.auth, &mut connect_msg_slice)
            .map_err(|e| Error::<S::Error>::Ser(e))?;
        connect_msg_slice.confirm(len);

        self.socket.write_all(b"CONNECT ").await?;
        self.socket.write_all(connect_msg.as_bytes()).await?;
        self.socket.write_all(&DELIM).await?;
        Ok(())
    }

    pub async fn subscribe(&mut self, sid: usize, topic: C::Topic) -> Result<(), S::Error> {
        defmt!(info!("Subscribing {}, topic: {}", sid, topic.as_str()));

        self.socket.write_all(b"SUB ").await?;
        self.socket.write_all(topic.as_str().as_bytes()).await?;
        self.socket.write_all(b" ").await?;
        self.socket
            .write_all(
                heapless::format!(U32_MAX_STR_LEN; "{}", sid)
                    .unwrap()
                    .as_bytes(),
            )
            .await?;
        self.socket.write_all(&DELIM).await?;
        Ok(())
    }

    pub async fn unsubscribe(&mut self, sid: usize) -> Result<(), S::Error> {
        defmt!(info!("Unsubscribing {}", sid));

        self.socket.write_all(b"UNSUB ").await?;
        self.socket
            .write_all(
                heapless::format!(U32_MAX_STR_LEN; "{}", sid)
                    .unwrap()
                    .as_bytes(),
            )
            .await?;
        self.socket.write_all(&DELIM).await?;
        Ok(())
    }

    pub async fn publish(&mut self, topic: C::Topic, data: C::MsgBuf) -> Result<(), S::Error> {
        let data = data.as_bytes();

        // Header
        self.socket.write_all(b"PUB ").await?;
        self.socket.write_all(topic.as_str().as_bytes()).await?;
        self.socket.write_all(b" ").await?;
        self.socket
            .write_all(
                heapless::format!(U32_MAX_STR_LEN; "{}", data.len())
                    .unwrap()
                    .as_bytes(),
            )
            .await?;
        self.socket.write_all(&DELIM).await?;

        // Body
        self.socket.write_all(data).await?;
        self.socket.write_all(&DELIM).await?;

        Ok(())
    }

    pub async fn frame(&mut self) -> Result<Option<Frame<C>>, S::Error> {
        Ok(self.framer.frame(&mut self.socket).await?)
    }

    pub fn reset(&mut self) {
        self.framer = Framer::new();
    }
}
