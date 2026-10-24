use core::marker::PhantomData;
use core::net::SocketAddr;

use embassy_futures::select::{Either, select};
use embedded_nal_async::TcpConnect;

mod transport;
use transport::NatsTransport;

use crate::{
    CapacityError, CmdReceiver, InfoSender, InternalCmd, MsgSender, NatsAuthenticator,
    NatsCollections, runner::transport::Frame,
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
#[derive(Debug, thiserror::Error)]
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

impl<E> From<transport::Error<E>> for Error<E> {
    fn from(value: transport::Error<E>) -> Self {
        match value {
            transport::Error::Disconnected => Error::Disconnected,
            transport::Error::Tcp(e) => Error::Tcp(e),
            transport::Error::Capacity(e) => Error::Capacity(e),
            transport::Error::Ser(e) => Error::Ser(e),
            transport::Error::Deser(e) => Error::Deser(e),
            transport::Error::Utf8 => Error::Utf8,
            transport::Error::Header => Error::Header,
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
    auth: A,
    inner: Inner<'a, S, C, A, N>,
}

pub struct Inner<'a, S: TcpConnect, C: NatsCollections, A: NatsAuthenticator, const N: usize> {
    state: State,

    info_watch: InfoSender<'a, C>,
    cmd_channel: CmdReceiver<'a, C>,

    subs: heapless::Vec<Subscription<'a, C>, N>,

    _s: PhantomData<S>,
    _a: PhantomData<A>,
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

            info_watch,
            cmd_channel,

            subs: heapless::Vec::new(),

            _s: PhantomData,
            _a: PhantomData,
        };
        Self {
            address,
            client,
            auth,
            inner,
        }
    }

    /// Mainloop entry for the runner
    pub async fn run(&mut self) -> ! {
        loop {
            let Ok(socket) = self.client.connect(self.address).await else {
                defmt!(error!("could not connect"));
                continue;
            };
            let transport = NatsTransport::new(socket, &self.auth);
            self.inner.run(transport).await;
        }
    }
}

impl<'a, S: TcpConnect, C: NatsCollections, A: NatsAuthenticator, const N: usize>
    Inner<'a, S, C, A, N>
{
    async fn handle_frame(
        &mut self,
        transport: &mut NatsTransport<'_, S, C, A>,
        frame: Frame<C>,
    ) -> Result<(), S::Error> {
        match frame {
            Frame::Ping => {
                transport.pong().await?;
            }
            Frame::Info(info) => {
                defmt!(info!("connected to nats: {}", info.server_name.as_str()));
                self.info_watch.send(info);

                transport.connect().await?;

                // resubscribe to all existing subscriptions
                for i in 0..self.subs.len() {
                    let Subscription {
                        sid,
                        topic,
                        channel,
                    } = &self.subs[i];
                    if channel.is_open() {
                        transport.subscribe(*sid, topic.clone()).await?;
                    }
                }

                // Update state
                self.state = State::Connected;
            }
            Frame::Err => {
                self.state = State::Disconnected;
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
                        transport.unsubscribe(sid).await?;
                    }
                } else {
                    defmt!(error!(
                        "Receiving message with no endpoint, unsubscribing..."
                    ));
                    transport.unsubscribe(nats_msg.sid).await?;
                }
            }
        }

        Ok(())
    }
    async fn register_sub(
        &mut self,
        transport: &mut NatsTransport<'_, S, C, A>,
        topic: C::Topic,
        channel: MsgSender<'a, C>,
    ) -> Result<(), S::Error> {
        self.sync(transport).await?;

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
            .map_err(|_| Error::<S::Error>::Capacity(CapacityError::Subscriptions))?;

        transport.subscribe(sid, topic).await?;

        Ok(())
    }
    // clear unused subs
    async fn sync(&mut self, transport: &mut NatsTransport<'_, S, C, A>) -> Result<(), S::Error> {
        let sids: heapless::Vec<_, N> = self
            .subs
            .iter()
            .filter(|s| !s.channel.is_open())
            .map(|v| v.sid)
            .collect();
        for sid in sids {
            transport.unsubscribe(sid).await?;
        }
        self.subs
            .retain(|Subscription { channel, .. }| channel.is_open());
        Ok(())
    }
    async fn run_connected(
        &mut self,
        transport: &mut NatsTransport<'_, S, C, A>,
    ) -> Result<(), S::Error> {
        match select(transport.frame(), self.cmd_channel.receive()).await {
            Either::First(framer_result) => {
                if let Some(frame) = framer_result? {
                    self.handle_frame(transport, frame).await?
                }
            }
            Either::Second(cmd) => match cmd {
                InternalCmd::Sub(topic, ch) => self.register_sub(transport, topic, ch).await?,
                InternalCmd::Pub(topic, data) => transport.publish(topic, data).await?,
                InternalCmd::Sync => self.sync(transport).await?,
            },
        }
        Ok(())
    }
    async fn run_auth_step(
        &mut self,
        transport: &mut NatsTransport<'_, S, C, A>,
    ) -> Result<(), S::Error> {
        let Ok(framer_result) = transport.frame().await else {
            // errors here should always be handled as auth and cause a reconnect
            return Err(Error::Auth);
        };
        if let Some(frame) = framer_result {
            self.handle_frame(transport, frame).await?
        }
        Ok(())
    }

    async fn run(&mut self, mut transport: NatsTransport<'_, S, C, A>) {
        self.state = State::Authenticating;
        loop {
            if let Err(e) = match self.state {
                State::Connected => self.run_connected(&mut transport).await,
                State::Authenticating => self.run_auth_step(&mut transport).await,
                State::Disconnected => return,
            } {
                defmt!(error!("nats error: {}", e));
                match e {
                    // If we had a connection issue disconnect
                    Error::Auth | Error::Disconnected | Error::Tcp(_) => {
                        self.state = State::Disconnected;
                    }
                    // otherwise reset framer
                    _ => transport.reset(),
                }
            }
        }
    }
}
