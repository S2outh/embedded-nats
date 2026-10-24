use core::{pin::Pin, sync::atomic::Ordering};

use embassy_futures::select::select_slice;
use embassy_sync::channel::DynamicReceiveFuture;

use crate::{
    CapacityError, CmdSender, InfoReceiver, InternalCmd, MsgChannel, MsgReceiver, NatsCollections,
    NatsInfoMsg, NatsMsg, Storage, msg_channel,
};

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, thiserror::Error)]
#[error("Not subscribed to topic")]
pub struct NotSubscribed;

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, thiserror::Error)]
pub enum SubscribingError {
    #[error("Max capacity for subscriptions reached")]
    CapacityError(#[from] CapacityError),
    #[error("Channel in use")]
    ChannelError(#[from] msg_channel::AlreadyOpen),
}

struct Subscription<'a, C: NatsCollections> {
    topic: C::Topic,
    channel: MsgReceiver<'a, C>,
}

/// The client struct can be used to interface with the runner.
/// it can be cloned to receive new interfaces to the same runner.
pub struct Client<'a, C, const N: usize>
where
    C: NatsCollections,
{
    storage: &'a Storage<'a, C>,
    info_watch: InfoReceiver<'a>,
    cmd_channel: CmdSender<'a, C>,

    sub_vec: heapless::Vec<Subscription<'a, C>, N>,
}
impl<'a, C, const N: usize> Client<'a, C, N>
where
    C: NatsCollections,
{
    pub(crate) fn new(storage: &'a Storage<'a, C>) -> Self {
        let info_watch = storage.info_watch.dyn_anon_receiver();
        let cmd_channel = storage.cmd_channel.sender();
        Self {
            storage,
            info_watch,
            cmd_channel,
            sub_vec: heapless::Vec::new(),
        }
    }

    /// Publish a message with a given topic
    pub async fn publish(&mut self, topic: C::Topic, bytes: C::MsgBuf) {
        self.cmd_channel.send(InternalCmd::Pub(topic, bytes)).await;
    }

    /// Subscribe to a given topic. Since the lifetime of the message channel needs to outlive
    /// the entire NATS stack (which in practice almost always means 'static) the user will need to
    /// provide one.
    ///
    /// Returns a capacity error if the number of subscriptions the runner can handle are exausted
    ///
    /// This Future should never be cancelled
    pub async fn subscribe<const S: usize>(
        &mut self,
        topic: C::Topic,
        channel: &'a MsgChannel<C, S>,
    ) -> Result<(), SubscribingError> {
        if self.storage.sub_count.fetch_add(1, Ordering::SeqCst) >= N {
            self.storage.sub_count.sub(1, Ordering::SeqCst);
            Err(CapacityError::Subscriptions)?;
        }

        self.cmd_channel.send(InternalCmd::Sync).await;
        let (sender, receiver) = channel.open().await.map_err(|e| {
            self.storage.sub_count.sub(1, Ordering::SeqCst);
            e
        })?;

        self.sub_vec
            .push(Subscription {
                topic: topic.clone(),
                channel: receiver,
            })
            .map_err(|_| {
                self.storage.sub_count.sub(1, Ordering::SeqCst);
                CapacityError::Subscriptions
            })?;

        self.cmd_channel.send(InternalCmd::Sub(topic, sender)).await;
        Ok(())
    }

    /// Unsubscribe from a topic, freeing the channel for future use
    pub async fn unsub(&mut self, topic: C::Topic) -> Result<(), NotSubscribed> {
        let i = self
            .sub_vec
            .iter()
            .position(|s| s.topic == topic)
            .ok_or(NotSubscribed)?;
        self.sub_vec.swap_remove(i);
        self.storage.sub_count.sub(1, Ordering::Release);
        Ok(())
    }

    /// Unsubscribe from all topics
    pub async fn unsub_all(&mut self) {
        self.storage
            .sub_count
            .sub(self.sub_vec.len(), Ordering::Release);
        self.sub_vec.clear();
    }

    /// Awaiting receive will await any message from all subscriptions of this client.
    /// If there are no subscriptions this will hang forever
    pub async fn receive(&mut self) -> NatsMsg<C> {
        let mut futs = self
            .sub_vec
            .iter()
            .map(|s| s.channel.receive())
            .collect::<heapless::Vec<DynamicReceiveFuture<'_, _>, N>>();
        select_slice(Pin::new(&mut futs[..])).await.0
    }

    /// Try to return any currently pending messages in the
    /// receive buffer
    pub fn try_receive(&mut self) -> Option<NatsMsg<C>> {
        self.sub_vec
            .iter()
            .find_map(|s| s.channel.try_receive().ok())
    }

    /// Get the contents of the latest INFO message received from the server
    pub async fn get_info(&mut self) -> Option<NatsInfoMsg> {
        self.info_watch.try_get()
    }
}
impl<'a, C, const N: usize> Clone for Client<'a, C, N>
where
    C: NatsCollections,
{
    fn clone(&self) -> Self {
        Client::new(self.storage)
    }
}

impl<'a, C, const N: usize> Drop for Client<'a, C, N>
where
    C: NatsCollections,
{
    fn drop(&mut self) {
        self.storage
            .sub_count
            .sub(self.sub_vec.len(), Ordering::Release);
    }
}
