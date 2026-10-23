use embedded_io_async::Read;
use thiserror::Error;

use crate::{BytesBuf, CapacityError, DELIM, NatsCollections, NatsInfoMsg, NatsMsg, StrBuf};

macro_rules! defmt {
    ($($t:tt)*) => {{
        #[cfg(feature = "defmt")]
        defmt::$($t)*;
    }};
}

#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Error)]
pub enum FramerError<E> {
    #[error("Disconnected")]
    Disconnected,
    #[error("Read error: {0}")]
    Read(E),
    #[error("Capacity of collection {0} not sufficient for operation")]
    Capacity(#[from] CapacityError),
    #[error("Json deserialization error {0}")]
    Deser(#[from] serde_json_core::de::Error),
    #[error("Invalid utf8")]
    Utf8,
    #[error("Invalid header")]
    Header,
}

impl<R> From<core::str::Utf8Error> for FramerError<R> {
    fn from(_: core::str::Utf8Error) -> Self {
        FramerError::Utf8
    }
}

/// A Received Nats protocol frame
pub enum Frame<C: NatsCollections> {
    Ping,
    Info(NatsInfoMsg),
    Err,
    Ok,
    Msg(NatsMsg<C>),
}

/// A result enum from an internal frame
enum InternalFrame<C: NatsCollections> {
    MsgHeader {
        topic: C::Topic,
        len: usize,
        sid: usize,
    },
    MsgDone,
    Ping,
    Info(NatsInfoMsg),
    Err,
    Ok,
    None,
}

/// This framer type is used to sync and read frames from a provided "read" function.
/// This read function is expected to follow the embedded_io_async contract of always
/// reading at least one, but up to buf.len() bytes. Since the embassy_runner also needs to perform
/// other operations besides reading from the wire this framer is itself non blocking:
/// the frame() function will call read() once, and return Option::None or Option::Some(frame)
/// depending on whether or not the read bytes are sufficient to complete a frame.
/// Conversely calling frame() does not guarantee that all of the currently ready bytes are beeing
/// read.
pub enum Framer<C: NatsCollections> {
    Sync(SyncFramer<C>),
    Msg(MsgFramer<C>),
}

pub struct SyncFramer<C: NatsCollections> {
    buffer: C::SyncBuf,
    magic_pos: usize,
}

pub struct MsgFramer<C: NatsCollections> {
    buffer: C::MsgBuf,
    topic: C::Topic,
    len: usize,
    sid: usize,
}

impl<C: NatsCollections> Framer<C> {
    /// create a new framer in sync state
    pub fn new() -> Self {
        Self::Sync(SyncFramer::new())
    }

    /// Call the provided reader.read() function exactly once,
    /// and return a frame if it can be finished.
    /// On error the state of the framer is not defined, and
    /// a reset of the framer is expected.
    pub async fn frame<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<Frame<C>>, FramerError<R::Error>> {
        let internal_frame = match self {
            Self::Sync(framer) => framer.frame(reader).await,
            Self::Msg(framer) => framer.frame(reader).await,
        }?;
        Ok(match internal_frame {
            InternalFrame::MsgHeader { topic, len, sid } => {
                *self = Self::Msg(MsgFramer::new(topic, len, sid));
                None
            }
            InternalFrame::MsgDone => {
                let Self::Msg(framer) = core::mem::replace(self, Self::Sync(SyncFramer::new()))
                else {
                    panic!()
                };
                Some(framer.finalize())
            }
            InternalFrame::Ok => Some(Frame::Ok),
            InternalFrame::Err => Some(Frame::Err),
            InternalFrame::Ping => Some(Frame::Ping),
            InternalFrame::Info(info) => Some(Frame::Info(info)),
            InternalFrame::None => None,
        })
    }
}

impl<C: NatsCollections> SyncFramer<C> {
    fn new() -> Self {
        Self {
            buffer: C::SyncBuf::default(),
            magic_pos: 0,
        }
    }

    async fn frame<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> Result<InternalFrame<C>, FramerError<R::Error>> {
        let mut slice = self.buffer.reserve_slice(1)?;
        let n = reader
            .read(&mut slice)
            .await
            .map_err(|e| FramerError::Read(e))?;
        slice.confirm(n);
        if n == 0 {
            return Err(FramerError::Disconnected);
        }

        if self.buffer.as_bytes().last().unwrap() == &DELIM[self.magic_pos] {
            self.magic_pos += 1;
            if self.magic_pos == DELIM.len() {
                self.magic_pos = 0;
                let header = self.parse_header();
                self.buffer.clear();
                header
            } else {
                Ok(InternalFrame::None)
            }
        } else {
            self.magic_pos = 0;
            Ok(InternalFrame::None)
        }
    }

    fn parse_header<E>(&mut self) -> Result<InternalFrame<C>, FramerError<E>> {
        fn parse_msg_header<E, C: NatsCollections>(
            msg: &str,
        ) -> Result<InternalFrame<C>, FramerError<E>> {
            let Some((topic, msg)) = msg.split_once(' ') else {
                defmt!(error!("nats msg header parsing error (1)"));
                return Err(FramerError::Header);
            };
            let Some((sid, msg)) = msg.split_once(' ') else {
                defmt!(error!("nats msg header parsing error (2)"));
                return Err(FramerError::Header);
            };
            let (_reply_to, len) = msg.split_once(' ').unwrap_or(("", msg));
            let Ok(sid) = sid.parse::<usize>() else {
                defmt!(error!("nats sid parsing error: '{}'", sid));
                return Err(FramerError::Header);
            };
            let Ok(len) = len.parse::<usize>() else {
                defmt!(error!("nats msg len parsing error: '{}'", len));
                return Err(FramerError::Header);
            };

            Ok(InternalFrame::MsgHeader {
                len,
                sid,
                topic: C::Topic::try_from_str(topic)?,
            })
        }

        let mut packet_str = core::str::from_utf8(&self.buffer.as_bytes())?;
        packet_str = &packet_str[..(packet_str.len() - DELIM.len())];

        let (cmd, msg) = packet_str
            .trim()
            .split_once(' ')
            .unwrap_or((&packet_str.trim(), ""));

        // match first word to one of the Nats messages supported by this
        // implementations
        match cmd {
            "PING" => Ok(InternalFrame::Ping),
            "INFO" => match serde_json_core::from_str::<NatsInfoMsg>(msg) {
                Ok((info, _)) => Ok(InternalFrame::Info(info)),
                Err(e) => {
                    defmt!(warn!("could not decode nats INFO"));
                    Err(FramerError::Deser(e))
                }
            },
            "-ERR" => {
                defmt!(error!("nats disconnected ({})", msg));
                Ok(InternalFrame::Err)
            }
            "+OK" => Ok(InternalFrame::Ok),
            "MSG" => parse_msg_header(msg),
            _default => {
                defmt!(warn!("unknown nats cmd {}", _default));
                Err(FramerError::Header)
            }
        }
    }
}

impl<C: NatsCollections> MsgFramer<C> {
    fn new(topic: C::Topic, len: usize, sid: usize) -> Self {
        Self {
            buffer: C::MsgBuf::default(),
            topic,
            len,
            sid,
        }
    }
    async fn frame<R: Read>(
        &mut self,
        reader: &mut R,
    ) -> Result<InternalFrame<C>, FramerError<R::Error>> {
        let pos = self.buffer.len();
        let mut slice = self.buffer.reserve_slice((self.len + DELIM.len()) - pos)?;
        let n = reader
            .read(&mut slice)
            .await
            .map_err(|e| FramerError::Read(e))?;
        slice.confirm(n);
        if n == 0 {
            return Err(FramerError::Disconnected);
        }

        if self.buffer.len() >= (self.len + DELIM.len()) {
            // drop the delimeter from the end
            self.buffer.truncate(self.len);
            Ok(InternalFrame::MsgDone)
        } else {
            Ok(InternalFrame::None)
        }
    }
    fn finalize(self) -> Frame<C> {
        Frame::Msg(NatsMsg {
            sid: self.sid,
            topic: self.topic,
            data: self.buffer,
        })
    }
}

#[cfg(test)]
mod tests {

    extern crate std;
    use super::*;
    use crate::Heapless;
    use embassy_futures::block_on;
    use embassy_sync::blocking_mutex::raw::ThreadModeRawMutex;
    use std::collections::VecDeque;
    use std::vec::Vec;

    type C = Heapless<ThreadModeRawMutex, 32, 64>;

    /// A reader yielding the provided chunks. A single read never crosses a chunk
    /// boundary, which simulates data arriving in separate packets.
    /// Once all chunks are consumed, reads return 0 (disconnected).
    struct Provider {
        data: VecDeque<Vec<u8>>,
    }

    impl Provider {
        fn new(chunks: &[&[u8]]) -> Self {
            Self {
                data: chunks.iter().map(|c| c.to_vec()).collect(),
            }
        }
    }

    impl embedded_io_async::ErrorType for Provider {
        type Error = core::convert::Infallible;
    }

    impl Read for Provider {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            let Some(data) = self.data.front_mut() else {
                return Ok(0);
            };
            let len = buf.len().min(data.len());
            buf[..len].copy_from_slice(&data[..len]);
            data.drain(..len);
            if data.is_empty() {
                self.data.pop_front();
            }
            Ok(len)
        }
    }

    /// Call frame() until a frame is produced or an error occurs.
    fn next_frame(
        framer: &mut Framer<C>,
        provider: &mut Provider,
    ) -> Result<Frame<C>, FramerError<core::convert::Infallible>> {
        loop {
            if let Some(frame) = block_on(framer.frame(provider))? {
                return Ok(frame);
            }
        }
    }

    #[test]
    fn ping_split() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[b"PI", b"NG\r", b"\n"]);
        assert!(matches!(
            next_frame(&mut framer, &mut provider),
            Ok(Frame::Ping)
        ));
    }

    #[test]
    fn ok_and_err() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[b"+OK\r\n-ERR 'Unknown Protocol Operation'\r\n"]);
        assert!(matches!(
            next_frame(&mut framer, &mut provider),
            Ok(Frame::Ok)
        ));
        assert!(matches!(
            next_frame(&mut framer, &mut provider),
            Ok(Frame::Err)
        ));
    }

    #[test]
    fn info() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[
            br#"INFO {"server_id":"id","server_name":"name","version":"1.2.3","go":"go1.2","host":"0.0.0.0","port":1234,"headers":true,"max_payload":1048576,"proto":1}"#,
            b"\r\n",
        ]);
        let Ok(Frame::Info(info)) = next_frame(&mut framer, &mut provider) else {
            panic!("expected INFO frame");
        };
        assert_eq!(info.server_name.as_str(), "name");
        assert_eq!(info.port, 1234);
        assert_eq!(info.max_payload, 1048576);
    }

    #[test]
    fn msg() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[b"MSG foo.bar 1 _INBOX.abc 4\r\na\r\nb\r\n"]);
        let Ok(Frame::Msg(msg)) = next_frame(&mut framer, &mut provider) else {
            panic!("expected MSG frame");
        };
        assert_eq!(msg.topic.as_str(), "foo.bar");
        assert_eq!(msg.sid, 1);
        assert_eq!(msg.data.as_bytes(), b"a\r\nb");
    }

    #[test]
    fn empty_msg() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[b"MSG foo 2 0\r\n\r\n"]);
        let Ok(Frame::Msg(msg)) = next_frame(&mut framer, &mut provider) else {
            panic!("expected MSG frame");
        };
        assert_eq!(msg.data.as_bytes(), b"");
    }

    #[test]
    fn msg_too_large() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[b"MSG foo 2 100\r\n"]);
        assert!(matches!(
            next_frame(&mut framer, &mut provider),
            Err(FramerError::Capacity(CapacityError::Bytes))
        ));
    }

    #[test]
    fn unknown_cmd() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[b"FOO bar\r\n"]);
        assert!(matches!(
            next_frame(&mut framer, &mut provider),
            Err(FramerError::Header)
        ));
    }

    #[test]
    fn invalid_msg_header() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[b"MSG foo x 3\r\n"]);
        assert!(matches!(
            next_frame(&mut framer, &mut provider),
            Err(FramerError::Header)
        ));
    }

    #[test]
    fn disconnected() {
        let mut framer = Framer::new();
        let mut provider = Provider::new(&[b"PIN"]);
        assert!(matches!(
            next_frame(&mut framer, &mut provider),
            Err(FramerError::Disconnected)
        ));
    }
}
