use core::net::SocketAddr;

use embassy_futures::select::{Either, select};
use embassy_net::tcp::ConnectError;
use embassy_net::tcp::{self, TcpSocket};
use embedded_io_async::Write;
use thiserror::Error;

mod framer;
use framer::Frame;
use framer::Framer;

use crate::runner::Error::Disconnected;
use crate::runner::framer::FramerError;
use crate::{
    AUTH_JSON_MAX_LEN, BytesBuf, CapacityError, CmdReceiver, DELIM, InfoSender, InternalCmd,
    MsgSender, NatsAuthenticator, NatsCollections, StrBuf, U32_MAX_STR_LEN,
};

macro_rules! defmt {
    ($($t:tt)*) => {{
        #[cfg(feature = "defmt")]
        defmt::$($t)*;
    }};
}

enum State {
    Disconnected,
    Authenticating,
    Connected,
}

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Error)]
enum Error {
    #[error("Auth")]
    Auth,
    #[error("Disconnected")]
    Disconnected,
    #[error("Tcp error: {0}")]
    Tcp(#[from] tcp::Error),
    #[error("Could not connect: {0}")]
    Connect(#[from] ConnectError),
    #[error("Capacity of collection {0} not sufficient for operation")]
    Capacity(#[from] CapacityError),
    #[error("Json serialization error {0}")]
    Ser(#[from] serde_json_core::ser::Error),
    #[error("Json deserialization error {0}")]
    Deser(#[from] serde_json_core::de::Error),
    #[error("Invalid utf8")]
    Utf8,
    #[error("Invalid header")]
    Header,
}

type Result<T> = core::result::Result<T, Error>;

impl From<FramerError<tcp::Error>> for Error {
    fn from(value: FramerError<tcp::Error>) -> Self {
        match value {
            FramerError::Disconnected => Error::Disconnected,
            FramerError::Read(e) => Error::Tcp(e),
            FramerError::Capacity(e) => Error::Capacity(e),
            FramerError::Deser(e) => Error::Deser(e),
            FramerError::Utf8 => Error::Utf8,
            FramerError::Header => Error::Header,
        }
    }
}

struct Subscription<'a, C: NatsCollections> {
    sid: usize,
    topic: C::Topic,
    channel: MsgSender<'a, C>,
}

pub struct Runner<'a, C: NatsCollections, A: NatsAuthenticator, const N: usize> {
    auth: A,
    state: State,
    address: SocketAddr,
    socket: TcpSocket<'a>,

    info_watch: InfoSender<'a>,
    cmd_channel: CmdReceiver<'a, C>,

    subs: heapless::Vec<Subscription<'a, C>, N>,
    framer: Framer<C>,
}
impl<'a, C: NatsCollections, A: NatsAuthenticator, const N: usize> Runner<'a, C, A, N> {
    pub(crate) fn new(
        auth: A,
        address: SocketAddr,
        socket: TcpSocket<'a>,
        info_watch: InfoSender<'a>,
        cmd_channel: CmdReceiver<'a, C>,
    ) -> Self {
        let state = State::Disconnected;
        Self {
            auth,
            state,
            address,
            socket,

            info_watch,
            cmd_channel,

            subs: heapless::Vec::new(),
            framer: Framer::new(),
        }
    }
    async fn disconnect(&mut self) {
        self.socket.abort();
        let _ = self.socket.flush().await;
        self.state = State::Disconnected;
    }
    async fn handle_frame(&mut self, frame: Frame<C>) -> Result<()> {
        match frame {
            Frame::Ping => {
                self.socket.write_all("PONG".as_bytes()).await?;
                self.socket.write_all(&DELIM).await?;
            }
            Frame::Info(info) => {
                defmt!(info!("connected to nats: {}", info.server_name.as_str()));
                self.info_watch.send(info);

                // serialize the connect message body into AuthBuf
                let mut connect_msg = C::AuthBuf::default();
                let mut connect_msg_slice = connect_msg.reserve_slice(AUTH_JSON_MAX_LEN)?;
                let len = serde_json_core::to_slice(&self.auth, &mut connect_msg_slice)?;
                connect_msg_slice.confirm(len);

                self.socket.write_all(b"CONNECT ").await?;
                self.socket.write_all(connect_msg.as_bytes()).await?;
                self.socket.write_all(&DELIM).await?;

                // resubscribe to all existing subscriptions
                for i in 0..self.subs.len() {
                    let Subscription {
                        sid,
                        topic,
                        channel,
                    } = &self.subs[i];
                    if channel.is_open() {
                        self.subscribe(*sid, topic.clone()).await?;
                    }
                }

                // Update state
                self.state = State::Connected;
            }
            Frame::Err => {
                self.disconnect().await;
            }
            Frame::Ok => (),
            Frame::Msg(nats_msg) => {
                if let Some((i, Subscription { sid, channel, .. })) = self
                    .subs
                    .iter()
                    .enumerate()
                    .find(|(_, s)| s.sid == nats_msg.sid)
                {
                    if channel.send(nats_msg).await.is_err() {
                        let sid = *sid;
                        self.subs.swap_remove(i);
                        self.unsubscribe(sid).await?;
                    }
                } else {
                    defmt!(error!(
                        "Receiving message with no endpoint, unsubscribing..."
                    ));
                    self.unsubscribe(nats_msg.sid).await?;
                }
            }
        }

        Ok(())
    }
    async fn subscribe(&mut self, sid: usize, topic: C::Topic) -> Result<()> {
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
    async fn unsubscribe(&mut self, sid: usize) -> Result<()> {
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
    async fn publish(&mut self, topic: C::Topic, data: C::MsgBuf) -> Result<()> {
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
    async fn register_sub(&mut self, topic: C::Topic, channel: MsgSender<'a, C>) -> Result<()> {
        self.sync().await?;

        if self.subs.iter().any(|s| s.topic == topic) {
            panic!("can't subscribe to a topic twice");
        }

        self.subs.sort_unstable_by_key(|s| s.sid);

        let sid = self
            .subs
            .iter()
            .enumerate()
            .position(|(i, s)| i != s.sid)
            .unwrap_or(self.subs.len());

        self.subs
            .push(Subscription {
                sid,
                topic: topic.clone(),
                channel,
            })
            .map_err(|_| CapacityError::Subscriptions)?;

        self.subscribe(sid, topic).await?;

        Ok(())
    }
    // clear unused subs
    async fn sync(&mut self) -> Result<()> {
        let sids: heapless::Vec<_, N> = self
            .subs
            .iter()
            .filter(|s| !s.channel.is_open())
            .map(|v| v.sid)
            .collect();
        for sid in sids {
            self.unsubscribe(sid).await?;
        }
        self.subs
            .retain(|Subscription { channel, .. }| channel.is_open());
        Ok(())
    }
    async fn run_connected(&mut self) -> Result<()> {
        match select(
            self.framer.frame(&mut self.socket),
            self.cmd_channel.receive(),
        )
        .await
        {
            Either::First(framer_result) => {
                if let Some(frame) = framer_result? {
                    self.handle_frame(frame).await?
                }
            }
            Either::Second(cmd) => match cmd {
                InternalCmd::Sub(topic, ch) => self.register_sub(topic, ch).await?,
                InternalCmd::Pub(topic, data) => self.publish(topic, data).await?,
                InternalCmd::Sync => self.sync().await?,
            },
        }
        Ok(())
    }
    async fn run_auth_step(&mut self) -> Result<()> {
        let Ok(framer_result) = self.framer.frame(&mut self.socket).await else {
            // errors here should always be handled as auth and cause a reconnect
            return Err(Error::Auth);
        };
        if let Some(frame) = framer_result {
            self.handle_frame(frame).await?
        }
        Ok(())
    }
    async fn try_connect(&mut self) -> Result<()> {
        self.socket.connect(self.address).await?;
        self.framer = Framer::new();
        self.state = State::Authenticating;
        Ok(())
    }
    /// Mainloop entry for the runner
    pub async fn run(&mut self) -> ! {
        loop {
            if let Err(e) = match self.state {
                State::Connected => self.run_connected().await,
                State::Authenticating => self.run_auth_step().await,
                State::Disconnected => self.try_connect().await,
            } {
                defmt!(error!("nats error: {}", e));
                match e {
                    // If we had a connection issue disconnect
                    Error::Auth | Error::Disconnected | Error::Tcp(_) | Error::Connect(_) => {
                        self.disconnect().await
                    }
                    // otherwise reset framer
                    _ => self.framer = Framer::new(),
                }
            }
        }
    }
}
impl<'a, C: NatsCollections, A: NatsAuthenticator, const N: usize> Drop for Runner<'a, C, A, N> {
    fn drop(&mut self) {
        self.socket.abort();
    }
}
