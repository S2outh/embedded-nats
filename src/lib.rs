//! This crate provides an embedded embassy net based implementation of the
//! Nats client protocol: (https://docs.nats.io/reference/protocols/client).
//! It allows for either the usage of [`alloc`] types if a global allocator is present
//! (requires the *alloc* feature to be active) or using fixed size collections
//! from the [`heapless`] crate. Furthermore defmt print statements are supported
//! and can be enabled using the *defmt* feature.
//!
//! The entrypoint for this crate is one of three new() functions that can be used
//! to connect to a Nats server using one of three supported authentication methods:
//! No authentication, User/Password authentication and Token authentication.
//! These new() functions will return a runner and a client. The runner only exposes an
//! async run function that should be called in a dedicated task. To interface with
//! the runner the client type exposes a set of methods for subscribing, publishing
//! and receiving messages. If more than one client is necessary the client can safely be cloned.
//! Each client keeps a separate list of subsciptions.
//!
//! For types that need to be statically available this crate exposes a [`Storage`]
//! type that can be created in a const context. An immutable reference to such a storage
//! should be provided to the respective new() function. Each separate runner will require
//! a separate Storage.
#![no_std]

mod client;
mod msg_channel;
mod runner;

use core::{
    net::SocketAddr,
    ops::{Deref, DerefMut},
};

pub use client::*;
use embassy_net::tcp::TcpSocket;
use embassy_sync::{channel, watch};
use msg_channel::*;
pub use msg_channel::{AlreadyOpen, MsgChannel};
use portable_atomic::AtomicUsize;
pub use runner::Runner;
use thiserror::Error;

// Private module to seal traits
mod sealed {
    pub trait Sealed {}
}

// constant implementation of usize::max
const fn max(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

// These constants set sane but addmitedly arbitrary upper bounds
// for the heapless string types in CONNECT and INFO messages

// Maximum length of user and password for authentication
const USR_PASS_STR_SIZE: usize = 20;

// Maximum length of an auth token
const TOKEN_STR_SIZE: usize = 32;

// server_id and server_name are up to 56 chars.
// 64 is therefore a good upper bound for this metadata.
const INFO_METADATA_STR_SIZE: usize = 64;

// The maximum length of the (json) messages received by nats are important
// in order to allocate the correct heapless:: type lengths.
// Here is a collection of necessary constants and calculated sizes

// The NATS ending delimiter
const DELIM: [u8; 2] = *b"\r\n";

// for UserPasswordAuth (trivially the longest one with empty filelds),
// this equals to 112 bytes base (string with empty user / pwd / name / version)
// + max(user and password max length, token max length)
// + project name and version length:
const AUTH_JSON_MAX_LEN: usize = 112
    + max(USR_PASS_STR_SIZE * 2, TOKEN_STR_SIZE)
    + env!("CARGO_PKG_NAME").len()
    + env!("CARGO_PKG_VERSION").len();

// The base (empty strings, i32::MIN) length for NatsInfoMsg equals to 142.
// The total length is prefix + base + 5 * max metadata string length + delimeter.
// This information is also used to compute the required size of the Header receive buffer
const INFO_MSG_MAX_LEN: usize = "INFO ".len() + 145 + 5 * INFO_METADATA_STR_SIZE + DELIM.len();

// The size of U32 MAX in decimal is 10 bytes
const U32_MAX_STR_LEN: usize = 10;

// Mutex type used by all the embassy channels
#[cfg(not(feature = "critical-section-mutex"))]
type RawMutex = embassy_sync::blocking_mutex::raw::ThreadModeRawMutex;
#[cfg(feature = "critical-section-mutex")]
type RawMutex = embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Error)]
pub enum CapacityError {
    #[error("C: StrBuf")]
    Str,
    #[error("C: BytesBuf")]
    Bytes,
    #[error("N: Subscriptions")]
    Subscriptions,
    #[error("Heapless capacity error")]
    Other(#[from] heapless::CapacityError),
}

/// This trait is a container for collections used througout this library,
/// and can be used to switch between heapless and alloc types.
pub trait NatsCollections: sealed::Sealed {
    type Topic: StrBuf;
    type MsgBuf: BytesBuf;
    type SyncBuf: BytesBuf;
    type AuthBuf: BytesBuf;
}

/// A String buffer, containing methods to convert from and to &str
pub trait StrBuf: Sized + Clone + PartialEq {
    fn try_from_str(s: &str) -> Result<Self, CapacityError>;
    fn as_str(&self) -> &str;
}

/// A reserved mutable reference to a slice. If dropped without calling confirm,
/// the referenced collections length will be restored.
pub struct ReservedSlice<'a, T: BytesBuf> {
    collection: &'a mut T,
    base_ptr: usize,
    reserved_len: usize,
}
impl<'a, T: BytesBuf> ReservedSlice<'a, T> {
    /// confirm a set length of this slice as used
    fn confirm(mut self, len: usize) {
        self.base_ptr += len;
    }
}
impl<'a, T: BytesBuf> Drop for ReservedSlice<'a, T> {
    fn drop(&mut self) {
        self.collection.truncate(self.base_ptr);
    }
}

impl<'a, T: BytesBuf> Deref for ReservedSlice<'a, T> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.collection.as_bytes()[self.base_ptr..(self.base_ptr + self.reserved_len)]
    }
}

impl<'a, T: BytesBuf> DerefMut for ReservedSlice<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.collection.as_bytes_mutable()[self.base_ptr..(self.base_ptr + self.reserved_len)]
    }
}

/// A Bytes buffer, containing methods to extend, clear and retreive info from a buffer
pub trait BytesBuf: Sized + Default {
    /// This method tries to take a slice of length `len` at the end of the buffer,
    /// potentially increasing the size of the underlying collection.
    fn reserve_slice(&mut self, len: usize) -> Result<ReservedSlice<'_, Self>, CapacityError>;
    /// keep only the first len bytes, or less if the collection is not large enough
    fn truncate(&mut self, len: usize);
    fn clear(&mut self);
    fn as_bytes(&self) -> &[u8];
    fn as_bytes_mutable(&mut self) -> &mut [u8];
    fn len(&self) -> usize;
}

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "alloc")]
pub struct Alloc;

#[cfg(feature = "alloc")]
impl sealed::Sealed for Alloc {}

#[cfg(feature = "alloc")]
impl NatsCollections for Alloc {
    type Topic = alloc::string::String;
    type MsgBuf = alloc::vec::Vec<u8>;
    type SyncBuf = alloc::vec::Vec<u8>;
    type AuthBuf = alloc::vec::Vec<u8>;
}

#[cfg(feature = "alloc")]
impl StrBuf for alloc::string::String {
    fn try_from_str(s: &str) -> Result<Self, CapacityError> {
        Ok(alloc::string::String::from(s))
    }
    fn as_str(&self) -> &str {
        <&str>::from(self)
    }
}

#[cfg(feature = "alloc")]
impl BytesBuf for alloc::vec::Vec<u8> {
    fn reserve_slice(
        &mut self,
        reserved_len: usize,
    ) -> Result<ReservedSlice<'_, Self>, CapacityError> {
        let base_ptr = self.len();
        self.extend(core::iter::repeat_n(0, reserved_len));

        Ok(ReservedSlice {
            collection: self,
            base_ptr,
            reserved_len,
        })
    }
    fn truncate(&mut self, len: usize) {
        self.truncate(len);
    }
    fn clear(&mut self) {
        self.clear();
    }
    fn as_bytes(&self) -> &[u8] {
        self
    }
    fn as_bytes_mutable(&mut self) -> &mut [u8] {
        self
    }
    fn len(&self) -> usize {
        self.as_bytes().len()
    }
}

pub struct Heapless<const TOPIC: usize, const PAYLOAD: usize>;

impl<const TOPIC: usize, const PAYLOAD: usize> sealed::Sealed for Heapless<TOPIC, PAYLOAD> {}

impl<const TOPIC: usize, const PAYLOAD: usize> NatsCollections for Heapless<TOPIC, PAYLOAD> {
    type Topic = heapless::String<TOPIC>;
    type MsgBuf = heapless::Vec<u8, PAYLOAD>;
    type SyncBuf = heapless::Vec<u8, INFO_MSG_MAX_LEN>;
    type AuthBuf = heapless::Vec<u8, AUTH_JSON_MAX_LEN>;
}

impl<const TOPIC: usize> StrBuf for heapless::String<TOPIC> {
    fn try_from_str(s: &str) -> Result<Self, CapacityError> {
        heapless::String::try_from(s).map_err(|_| CapacityError::Str)
    }
    fn as_str(&self) -> &str {
        <&str>::from(self)
    }
}

impl<const PAYLOAD: usize> BytesBuf for heapless::Vec<u8, PAYLOAD> {
    fn reserve_slice(
        &mut self,
        reserved_len: usize,
    ) -> Result<ReservedSlice<'_, Self>, CapacityError> {
        if self.capacity() - self.len() < reserved_len {
            return Err(CapacityError::Bytes);
        }

        let base_ptr = self.len();
        self.extend(core::iter::repeat_n(0, reserved_len));

        Ok(ReservedSlice {
            collection: self,
            base_ptr,
            reserved_len,
        })
    }
    fn truncate(&mut self, len: usize) {
        self.truncate(len);
    }
    fn clear(&mut self) {
        self.clear();
    }
    fn as_bytes(&self) -> &[u8] {
        self
    }
    fn as_bytes_mutable(&mut self) -> &mut [u8] {
        self
    }
    fn len(&self) -> usize {
        self.as_bytes().len()
    }
}

pub struct NatsMsg<C>
where
    C: NatsCollections,
{
    pub sid: usize,
    pub topic: C::Topic,
    pub data: C::MsgBuf,
}

#[derive(serde::Deserialize, Clone)]
pub struct NatsInfoMsg {
    pub server_id: heapless::String<INFO_METADATA_STR_SIZE>,
    pub server_name: heapless::String<INFO_METADATA_STR_SIZE>,
    pub version: heapless::String<INFO_METADATA_STR_SIZE>,
    pub go: heapless::String<INFO_METADATA_STR_SIZE>,
    pub host: heapless::String<INFO_METADATA_STR_SIZE>,
    pub port: i32,
    pub headers: bool,
    pub max_payload: i32,
    pub proto: i32,
}

type InfoWatch = watch::Watch<RawMutex, NatsInfoMsg, 0>;
type InfoSender<'a> = watch::Sender<'a, RawMutex, NatsInfoMsg, 0>;
type InfoReceiver<'a> = watch::DynAnonReceiver<'a, NatsInfoMsg>;

const CMD_CHANNEL_SIZE: usize = 3;
type CmdChannel<'a, C> = channel::Channel<RawMutex, InternalCmd<'a, C>, CMD_CHANNEL_SIZE>;
type CmdSender<'a, C> = channel::Sender<'a, RawMutex, InternalCmd<'a, C>, CMD_CHANNEL_SIZE>;
type CmdReceiver<'a, C> = channel::Receiver<'a, RawMutex, InternalCmd<'a, C>, CMD_CHANNEL_SIZE>;

enum InternalCmd<'a, C>
where
    C: NatsCollections,
{
    Pub(C::Topic, C::MsgBuf),
    Sub(C::Topic, MsgSender<'a, C>),
    Sync,
}

pub trait NatsAuthenticator: serde::Serialize + sealed::Sealed {}

#[derive(serde::Serialize)]
pub struct NoopAuthenticator {
    verbose: bool,
    pedantic: bool,
    tls_required: bool,
    lang: &'static str,
    name: &'static str,
    version: &'static str,
}
impl NoopAuthenticator {
    fn new() -> Self {
        Self {
            verbose: false,
            pedantic: false,
            tls_required: false,
            name: env!("CARGO_PKG_NAME"),
            lang: "rust",
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

impl sealed::Sealed for NoopAuthenticator {}
impl NatsAuthenticator for NoopAuthenticator {}

#[derive(serde::Serialize)]
pub struct UserPwdAuthenticator {
    verbose: bool,
    pedantic: bool,
    tls_required: bool,
    user: heapless::String<USR_PASS_STR_SIZE>,
    pass: heapless::String<USR_PASS_STR_SIZE>,
    lang: &'static str,
    name: &'static str,
    version: &'static str,
}
impl UserPwdAuthenticator {
    fn new(user: &str, pwd: &str) -> Result<Self, CapacityError> {
        Ok(Self {
            verbose: false,
            pedantic: false,
            tls_required: false,
            user: user.try_into()?,
            pass: pwd.try_into()?,
            name: env!("CARGO_PKG_NAME"),
            lang: "rust",
            version: env!("CARGO_PKG_VERSION"),
        })
    }
}
impl sealed::Sealed for UserPwdAuthenticator {}
impl NatsAuthenticator for UserPwdAuthenticator {}

#[derive(serde::Serialize)]
pub struct TokenAuthenticator {
    verbose: bool,
    pedantic: bool,
    tls_required: bool,
    token: heapless::String<TOKEN_STR_SIZE>,
    lang: &'static str,
    name: &'static str,
    version: &'static str,
}
impl TokenAuthenticator {
    fn new(auth_token: &str) -> Result<Self, CapacityError> {
        Ok(Self {
            verbose: false,
            pedantic: false,
            tls_required: false,
            token: auth_token.try_into()?,
            name: env!("CARGO_PKG_NAME"),
            lang: "rust",
            version: env!("CARGO_PKG_VERSION"),
        })
    }
}
impl sealed::Sealed for TokenAuthenticator {}
impl NatsAuthenticator for TokenAuthenticator {}

/// Static storage of sync types used by an nats runner instance
pub struct Storage<'a, C>
where
    C: NatsCollections,
{
    info_watch: InfoWatch,
    cmd_channel: CmdChannel<'a, C>,
    sub_count: AtomicUsize,
}
impl<'a, C> Storage<'a, C>
where
    C: NatsCollections,
{
    /// Create a new instance of the storage
    pub const fn new() -> Self {
        let info_watch = InfoWatch::new();
        let cmd_channel = CmdChannel::new();
        let sub_count = AtomicUsize::new(0);

        Self {
            info_watch,
            cmd_channel,
            sub_count,
        }
    }
}

/// Create a new embassy runner/client pair without authentication
pub fn new_no_auth<'a, C, const N: usize>(
    address: SocketAddr,
    socket: TcpSocket<'a>,
    storage: &'a Storage<'a, C>,
) -> (Client<'a, C, N>, Runner<'a, C, NoopAuthenticator, N>)
where
    C: NatsCollections,
{
    let auth = NoopAuthenticator::new();

    let runner = Runner::new(
        auth,
        address,
        socket,
        storage.info_watch.sender(),
        storage.cmd_channel.receiver(),
    );
    let client = Client::new(storage);

    (client, runner)
}

/// Create a new embassy runner/client pair with username / password authentication
pub fn new_with_user_pwd<'a, C, const N: usize>(
    user: &str,
    pwd: &str,
    address: SocketAddr,
    socket: TcpSocket<'a>,
    storage: &'a Storage<'a, C>,
) -> Result<(Client<'a, C, N>, Runner<'a, C, UserPwdAuthenticator, N>), CapacityError>
where
    C: NatsCollections,
{
    let auth = UserPwdAuthenticator::new(user, pwd)?;

    let runner = Runner::new(
        auth,
        address,
        socket,
        storage.info_watch.sender(),
        storage.cmd_channel.receiver(),
    );
    let client = Client::new(storage);

    Ok((client, runner))
}

/// Create a new embassy runner/client pair with token authentication
pub fn new_with_auth_token<'a, C, const N: usize>(
    auth_token: &str,
    address: SocketAddr,
    socket: TcpSocket<'a>,
    storage: &'a Storage<'a, C>,
) -> Result<(Client<'a, C, N>, Runner<'a, C, TokenAuthenticator, N>), CapacityError>
where
    C: NatsCollections,
{
    let auth = TokenAuthenticator::new(auth_token)?;

    let runner = Runner::new(
        auth,
        address,
        socket,
        storage.info_watch.sender(),
        storage.cmd_channel.receiver(),
    );
    let client = Client::new(storage);

    Ok((client, runner))
}
