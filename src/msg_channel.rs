use core::{future::poll_fn, sync::atomic::Ordering, task::Poll};

use embassy_sync::{
    channel::{self, DynamicReceiveFuture, TryReceiveError},
    waitqueue::AtomicWaker,
};
use portable_atomic::AtomicU8;

use crate::{NatsCollections, NatsMsg};

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, thiserror::Error)]
#[error("channel is already in use by another subscription")]
pub struct AlreadyOpen;

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, thiserror::Error)]
#[error("channel is closed")]
pub struct Closed;

enum ChannelState {
    Ready,
    Open,
    Closing,
}

struct AtomicState {
    state: AtomicU8,
}
impl AtomicState {
    const fn to_u8(state: ChannelState) -> u8 {
        match state {
            ChannelState::Ready => 0,
            ChannelState::Open => 1,
            ChannelState::Closing => 2,
        }
    }
    pub const fn new(state: ChannelState) -> Self {
        Self {
            state: AtomicU8::new(Self::to_u8(state)),
        }
    }
    pub fn store(&self, state: ChannelState, order: Ordering) {
        self.state.store(Self::to_u8(state), order);
    }
    pub fn load(&self, order: Ordering) -> ChannelState {
        match self.state.load(order) {
            0 => ChannelState::Ready,
            1 => ChannelState::Open,
            2 => ChannelState::Closing,
            _ => unreachable!(),
        }
    }
}

pub struct MsgChannel<C: NatsCollections, const N: usize> {
    channel: channel::Channel<C::RawMutex, NatsMsg<C>, N>,
    state: AtomicState,
    waker: AtomicWaker,
}

impl<C: NatsCollections, const N: usize> MsgChannel<C, N> {
    pub const fn new() -> Self {
        Self {
            channel: channel::Channel::new(),
            state: AtomicState::new(ChannelState::Ready),
            waker: AtomicWaker::new(),
        }
    }

    async fn wait_for_ready(&self) {
        poll_fn(|cx| {
            self.waker.register(cx.waker());
            let state = self.state.load(Ordering::Acquire);
            if matches!(state, ChannelState::Ready) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    pub async fn open(&self) -> Result<(MsgSender<'_, C>, MsgReceiver<'_, C>), AlreadyOpen> {
        match self.state.load(Ordering::Acquire) {
            ChannelState::Ready => (),
            ChannelState::Open => return Err(AlreadyOpen),
            ChannelState::Closing => self.wait_for_ready().await,
        }
        self.channel.clear();
        self.state.store(ChannelState::Open, Ordering::Release);
        Ok((
            MsgSender {
                sender: self.channel.sender().into(),
                state: &self.state,
                waker: &self.waker,
            },
            MsgReceiver {
                receiver: self.channel.receiver().into(),
                state: &self.state,
            },
        ))
    }
}

pub struct MsgSender<'a, C: NatsCollections> {
    sender: channel::SendDynamicSender<'a, NatsMsg<C>>,
    state: &'a AtomicState,
    waker: &'a AtomicWaker,
}

impl<'a, C: NatsCollections> MsgSender<'a, C> {
    pub async fn send(&self, msg: NatsMsg<C>) -> Result<(), Closed> {
        if !matches!(self.state.load(Ordering::Acquire), ChannelState::Open) {
            return Err(Closed);
        }
        self.sender.send(msg).await;
        Ok(())
    }
    pub fn is_open(&self) -> bool {
        matches!(self.state.load(Ordering::Acquire), ChannelState::Open)
    }
}

impl<'a, C: NatsCollections> Drop for MsgSender<'a, C> {
    fn drop(&mut self) {
        if !matches!(self.state.load(Ordering::Acquire), ChannelState::Closing) {
            panic!("msg sender should never be dropped while a client is still listening")
        }
        self.state.store(ChannelState::Ready, Ordering::Release);
        self.waker.wake();
    }
}

pub struct MsgReceiver<'a, C: NatsCollections> {
    receiver: channel::SendDynamicReceiver<'a, NatsMsg<C>>,
    state: &'a AtomicState,
}

impl<'a, C: NatsCollections> MsgReceiver<'a, C> {
    pub fn receive(&self) -> DynamicReceiveFuture<'_, NatsMsg<C>> {
        if !matches!(self.state.load(Ordering::Acquire), ChannelState::Open) {
            panic!("While a receiver exists channel must be open")
        }
        self.receiver.receive()
    }

    pub fn try_receive(&self) -> Result<NatsMsg<C>, TryReceiveError> {
        if !matches!(self.state.load(Ordering::Acquire), ChannelState::Open) {
            panic!("While a receiver exists channel must be open")
        }
        self.receiver.try_receive()
    }
}

impl<'a, C: NatsCollections> Drop for MsgReceiver<'a, C> {
    fn drop(&mut self) {
        if !matches!(self.state.load(Ordering::Acquire), ChannelState::Open) {
            panic!("While a receiver exists channel must be open")
        }
        loop {
            if self.receiver.try_receive().is_err() {
                break;
            }
        }
        self.state.store(ChannelState::Closing, Ordering::Release);
    }
}
