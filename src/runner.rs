use core::marker::PhantomData;
use core::net::SocketAddr;

use embassy_futures::select::{Either, select};
use embedded_io_async::Write;
use embedded_nal_async::TcpConnect;
use thiserror::Error;

mod framer;
use framer::Frame;
use framer::Framer;

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
enum Error<E> {
    #[error("Auth")]
    Auth,
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

impl<E> From<FramerError<E>> for Error<E> {
    fn from(value: FramerError<E>) -> Self {
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

type Result<T, E> = core::result::Result<T, Error<E>>;

struct Subscription<'a, C: NatsCollections> {
    sid: usize,
    topic: C::Topic,
    channel: MsgSender<'a, C>,
}

pub struct Runner<'a, S: TcpConnect, C: NatsCollections, A: NatsAuthenticator, const N: usize> {
    address: SocketAddr,
    client: S,
    inner: Inner<'a, S, C, A, N>,
}

pub struct Inner<'a, S: TcpConnect, C: NatsCollections, A: NatsAuthenticator, const N: usize> {
    state: State,
    auth: A,

    info_watch: InfoSender<'a, C>,
    cmd_channel: CmdReceiver<'a, C>,

    subs: heapless::Vec<Subscription<'a, C>, N>,
    framer: Framer<C>,

    _s: PhantomData<S>,
}

impl<'a, S: TcpConnect, C: NatsCollections, A: NatsAuthenticator, const N: usize>
    Runner<'a, S, C, A, N>
{
    pub(crate) fn new(
        auth: A,
        address: SocketAddr,
        client: S,
        info_watch: InfoSender<'a, C>,
        cmd_channel: CmdReceiver<'a, C>,
    ) -> Self {
        let state = State::Disconnected;
        let inner = Inner {
            state,
            auth,

            info_watch,
            cmd_channel,

            subs: heapless::Vec::new(),
            framer: Framer::new(),

            _s: PhantomData,
        };
        Self {
            address,
            client,
            inner,
        }
    }

    /// Mainloop entry for the runner
    pub async fn run(&mut self) -> ! {
        loop {
            let Ok(mut socket) = self.client.connect(self.address).await else {
                defmt!(error!("could not connect"));
                continue;
            };
            self.inner.run(&mut socket).await;
        }
    }
}

impl<'a, S: TcpConnect, C: NatsCollections, A: NatsAuthenticator, const N: usize>
    Inner<'a, S, C, A, N>
{
    async fn disconnect(&mut self, socket: &mut S::Connection<'_>) {
        let _ = socket.flush().await;
        self.state = State::Disconnected;
    }
    async fn handle_frame(
        &mut self,
        socket: &mut S::Connection<'_>,
        frame: Frame<C>,
    ) -> Result<(), S::Error> {
        match frame {
            Frame::Ping => {
                socket.write_all("PONG".as_bytes()).await?;
                socket.write_all(&DELIM).await?;
            }
            Frame::Info(info) => {
                defmt!(info!("connected to nats: {}", info.server_name.as_str()));
                self.info_watch.send(info);

                // serialize the connect message body into AuthBuf
                let mut connect_msg = C::AuthBuf::default();
                let mut connect_msg_slice = connect_msg
                    .reserve_slice(AUTH_JSON_MAX_LEN)
                    .map_err(|e| Error::Capacity(e))?;
                let len = serde_json_core::to_slice(&self.auth, &mut connect_msg_slice)
                    .map_err(|e| Error::Ser(e))?;
                connect_msg_slice.confirm(len);

                socket.write_all(b"CONNECT ").await?;
                socket.write_all(connect_msg.as_bytes()).await?;
                socket.write_all(&DELIM).await?;

                // resubscribe to all existing subscriptions
                for i in 0..self.subs.len() {
                    let Subscription {
                        sid,
                        topic,
                        channel,
                    } = &self.subs[i];
                    if channel.is_open() {
                        self.subscribe(socket, *sid, topic.clone()).await?;
                    }
                }

                // Update state
                self.state = State::Connected;
            }
            Frame::Err => {
                self.disconnect(socket).await;
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
                        self.unsubscribe(socket, sid).await?;
                    }
                } else {
                    defmt!(error!(
                        "Receiving message with no endpoint, unsubscribing..."
                    ));
                    self.unsubscribe(socket, nats_msg.sid).await?;
                }
            }
        }

        Ok(())
    }
    async fn subscribe(
        &mut self,
        socket: &mut S::Connection<'_>,
        sid: usize,
        topic: C::Topic,
    ) -> Result<(), S::Error> {
        defmt!(info!("Subscribing {}, topic: {}", sid, topic.as_str()));

        socket.write_all(b"SUB ").await?;
        socket.write_all(topic.as_str().as_bytes()).await?;
        socket.write_all(b" ").await?;
        socket
            .write_all(
                heapless::format!(U32_MAX_STR_LEN; "{}", sid)
                    .unwrap()
                    .as_bytes(),
            )
            .await?;
        socket.write_all(&DELIM).await?;
        Ok(())
    }
    async fn unsubscribe(
        &mut self,
        socket: &mut S::Connection<'_>,
        sid: usize,
    ) -> Result<(), S::Error> {
        defmt!(info!("Unsubscribing {}", sid));

        socket.write_all(b"UNSUB ").await?;
        socket
            .write_all(
                heapless::format!(U32_MAX_STR_LEN; "{}", sid)
                    .unwrap()
                    .as_bytes(),
            )
            .await?;
        socket.write_all(&DELIM).await?;
        Ok(())
    }
    async fn publish(
        &mut self,
        socket: &mut S::Connection<'_>,
        topic: C::Topic,
        data: C::MsgBuf,
    ) -> Result<(), S::Error> {
        let data = data.as_bytes();

        // Header
        socket.write_all(b"PUB ").await?;
        socket.write_all(topic.as_str().as_bytes()).await?;
        socket.write_all(b" ").await?;
        socket
            .write_all(
                heapless::format!(U32_MAX_STR_LEN; "{}", data.len())
                    .unwrap()
                    .as_bytes(),
            )
            .await?;
        socket.write_all(&DELIM).await?;

        // Body
        socket.write_all(data).await?;
        socket.write_all(&DELIM).await?;

        Ok(())
    }
    async fn register_sub(
        &mut self,
        socket: &mut S::Connection<'_>,
        topic: C::Topic,
        channel: MsgSender<'a, C>,
    ) -> Result<(), S::Error> {
        self.sync(socket).await?;

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
            .map_err(|_| Error::Capacity(CapacityError::Subscriptions))?;

        self.subscribe(socket, sid, topic).await?;

        Ok(())
    }
    // clear unused subs
    async fn sync(&mut self, socket: &mut S::Connection<'_>) -> Result<(), S::Error> {
        let sids: heapless::Vec<_, N> = self
            .subs
            .iter()
            .filter(|s| !s.channel.is_open())
            .map(|v| v.sid)
            .collect();
        for sid in sids {
            self.unsubscribe(socket, sid).await?;
        }
        self.subs
            .retain(|Subscription { channel, .. }| channel.is_open());
        Ok(())
    }
    async fn run_connected(&mut self, socket: &mut S::Connection<'_>) -> Result<(), S::Error> {
        match select(self.framer.frame(socket), self.cmd_channel.receive()).await {
            Either::First(framer_result) => {
                if let Some(frame) = framer_result? {
                    self.handle_frame(socket, frame).await?
                }
            }
            Either::Second(cmd) => match cmd {
                InternalCmd::Sub(topic, ch) => self.register_sub(socket, topic, ch).await?,
                InternalCmd::Pub(topic, data) => self.publish(socket, topic, data).await?,
                InternalCmd::Sync => self.sync(socket).await?,
            },
        }
        Ok(())
    }
    async fn run_auth_step(&mut self, socket: &mut S::Connection<'_>) -> Result<(), S::Error> {
        let Ok(framer_result) = self.framer.frame(socket).await else {
            // errors here should always be handled as auth and cause a reconnect
            return Err(Error::Auth);
        };
        if let Some(frame) = framer_result {
            self.handle_frame(socket, frame).await?
        }
        Ok(())
    }

    async fn run(&mut self, socket: &mut S::Connection<'_>) {
        self.framer = Framer::new();
        self.state = State::Authenticating;
        loop {
            if let Err(e) = match self.state {
                State::Connected => self.run_connected(socket).await,
                State::Authenticating => self.run_auth_step(socket).await,
                State::Disconnected => return,
            } {
                defmt!(error!("nats error: {}", e));
                match e {
                    // If we had a connection issue disconnect
                    Error::Auth | Error::Disconnected | Error::Tcp(_) => {
                        self.disconnect(socket).await
                    }
                    // otherwise reset framer
                    _ => self.framer = Framer::new(),
                }
            }
        }
    }
}
