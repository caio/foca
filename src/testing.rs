/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use alloc::vec::Vec;
use core::time::Duration;

use bytes::{Buf, BufMut, Bytes};

use crate::{Codec, Header, Identity, Member, Message, Notification, Runtime, State, Timer};

#[derive(Debug, Clone, Copy, PartialOrd, Ord)]
pub(crate) struct ID {
    id: u8,
    bump: u8,
    rejoinable: bool,
}

impl PartialEq for ID {
    fn eq(&self, other: &Self) -> bool {
        // Ignoring `rejoinable` field
        self.id == other.id && self.bump == other.bump
    }
}

impl core::hash::Hash for ID {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.bump.hash(state);
    }
}

impl Eq for ID {}

impl ID {
    pub(crate) fn new(id: u8) -> Self {
        ID::new_with_bump(id, 0)
    }

    pub(crate) fn new_with_bump(id: u8, bump: u8) -> Self {
        Self {
            id,
            bump,
            rejoinable: false,
        }
    }

    pub(crate) fn rejoinable(mut self) -> Self {
        self.rejoinable = true;
        self
    }

    pub(crate) fn serialize_into(&self, mut buf: impl BufMut) -> Result<(), BadCodecError> {
        if buf.remaining_mut() >= 2 {
            buf.put_u8(self.id);
            buf.put_u8(self.bump);
            Ok(())
        } else {
            Err(BadCodecError::SerializeInto)
        }
    }

    pub(crate) fn deserialize_from(mut buf: impl Buf) -> Result<Self, BadCodecError> {
        if buf.remaining() >= 2 {
            Ok(Self {
                id: buf.get_u8(),
                bump: buf.get_u8(),
                // Only the identity held by foca cares about this
                rejoinable: false,
            })
        } else {
            Err(BadCodecError::DeserializeFrom)
        }
    }
}

impl Identity<ID> for ID {
    fn has_same_prefix(&self, other: &Self) -> bool {
        self.id == other.id
    }

    fn renew(&self) -> Option<Self> {
        if self.rejoinable {
            Some(ID::new_with_bump(self.id, self.bump.wrapping_add(1)).rejoinable())
        } else {
            None
        }
    }

    fn addr(&self) -> ID {
        *self
    }
}

pub(crate) struct BadCodec;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BadCodecError {
    BufTooSmall,
    BadMessageID(u8),
    BadStateByte(u8),
    SerializeInto,
    DeserializeFrom,
    EncodeHeader,
    DecodeHeader,
    DecodeMessage,
}

impl core::fmt::Display for BadCodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl BadCodec {
    fn encode_header(
        &self,
        header: &Header<ID>,
        mut buf: impl BufMut,
    ) -> Result<(), BadCodecError> {
        if buf.remaining_mut() >= 2 + 2 + 2 {
            header.src.serialize_into(&mut buf)?;
            buf.put_u16(header.src_incarnation);
            header.dst.serialize_into(&mut buf)?;
            self.encode_message(&header.message, &mut buf)?;
            Ok(())
        } else {
            Err(BadCodecError::EncodeHeader)
        }
    }

    fn decode_header(&self, mut buf: impl Buf) -> Result<Header<ID>, BadCodecError> {
        if buf.remaining() > 2 + 2 + 2 {
            let src = ID::deserialize_from(&mut buf)?;
            let src_incarnation = buf.get_u16();
            let dst = ID::deserialize_from(&mut buf)?;
            let message = self.decode_message(&mut buf)?;

            Ok(Header {
                src,
                src_incarnation,
                dst,
                message,
            })
        } else {
            Err(BadCodecError::DecodeHeader)
        }
    }

    fn decode_message(&self, mut buf: impl Buf) -> Result<Message<ID>, BadCodecError> {
        if !buf.has_remaining() {
            return Err(BadCodecError::BufTooSmall);
        }

        let message_id = buf.get_u8();

        if message_id < 7 && !buf.has_remaining() {
            return Err(BadCodecError::DecodeMessage);
        }

        let message = match message_id {
            1 => Message::Ping(buf.get_u8()),
            2 => Message::Ack(buf.get_u8()),
            3 => {
                let target = ID::deserialize_from(&mut buf)?;
                let probe_number = buf.get_u8();
                Message::PingReq {
                    target,
                    probe_number,
                }
            }
            4 => {
                let origin = ID::deserialize_from(&mut buf)?;
                let probe_number = buf.get_u8();
                Message::IndirectPing {
                    origin,
                    probe_number,
                }
            }
            5 => {
                let target = ID::deserialize_from(&mut buf)?;
                let probe_number = buf.get_u8();
                Message::IndirectAck {
                    target,
                    probe_number,
                }
            }
            6 => {
                let origin = ID::deserialize_from(&mut buf)?;
                let probe_number = buf.get_u8();
                Message::ForwardedAck {
                    origin,
                    probe_number,
                }
            }
            7 => Message::Gossip,
            8 => Message::Announce,
            9 => Message::Feed,
            10 => Message::Broadcast,
            11 => Message::TurnUndead,
            other => return Err(BadCodecError::BadMessageID(other)),
        };

        Ok(message)
    }

    fn encode_message(
        &self,
        message: &Message<ID>,
        mut buf: impl BufMut,
    ) -> Result<(), BadCodecError> {
        if buf.remaining_mut() < 2 {
            return Err(BadCodecError::BufTooSmall);
        }
        match message {
            Message::Ping(ping_nr) => {
                buf.put_u8(1);
                buf.put_u8(*ping_nr);
            }
            Message::Ack(ping_nr) => {
                buf.put_u8(2);
                buf.put_u8(*ping_nr);
            }
            Message::PingReq {
                target,
                probe_number,
            } => {
                buf.put_u8(3);
                target.serialize_into(&mut buf)?;
                buf.put_u8(*probe_number);
            }
            Message::IndirectPing {
                origin,
                probe_number,
            } => {
                buf.put_u8(4);
                origin.serialize_into(&mut buf)?;
                buf.put_u8(*probe_number);
            }
            Message::IndirectAck {
                target,
                probe_number,
            } => {
                buf.put_u8(5);
                target.serialize_into(&mut buf)?;
                buf.put_u8(*probe_number);
            }
            Message::ForwardedAck {
                origin,
                probe_number,
            } => {
                buf.put_u8(6);
                origin.serialize_into(&mut buf)?;
                buf.put_u8(*probe_number);
            }
            Message::Gossip => {
                buf.put_u8(7);
            }
            Message::Announce => {
                buf.put_u8(8);
            }
            Message::Feed => {
                buf.put_u8(9);
            }
            Message::Broadcast => {
                buf.put_u8(10);
            }
            Message::TurnUndead => {
                buf.put_u8(11);
            }
        }

        Ok(())
    }

    fn decode_member(&self, mut buf: impl Buf) -> Result<Member<ID>, BadCodecError> {
        let id = ID::deserialize_from(&mut buf)?;
        let incarnation = buf.get_u16();
        let state = match buf.get_u8() {
            1 => State::Alive,
            2 => State::Suspect,
            3 => State::Down,
            other => return Err(BadCodecError::BadStateByte(other)),
        };

        Ok(Member::new(id, incarnation, state))
    }

    fn try_put_u8(&self, mut buf: impl BufMut, num: u8) -> Result<(), BadCodecError> {
        if buf.remaining_mut() > 0 {
            buf.put_u8(num);
            Ok(())
        } else {
            Err(BadCodecError::BufTooSmall)
        }
    }

    fn try_put_u16(&self, mut buf: impl BufMut, num: u16) -> Result<(), BadCodecError> {
        if buf.remaining_mut() > 1 {
            buf.put_u16(num);
            Ok(())
        } else {
            Err(BadCodecError::BufTooSmall)
        }
    }

    fn encode_member(
        &self,
        member: &Member<ID>,
        mut buf: impl BufMut,
    ) -> Result<(), BadCodecError> {
        member.id().serialize_into(&mut buf)?;
        self.try_put_u16(&mut buf, member.incarnation())?;
        match member.state() {
            State::Alive => self.try_put_u8(&mut buf, 1)?,
            State::Suspect => self.try_put_u8(&mut buf, 2)?,
            State::Down => self.try_put_u8(&mut buf, 3)?,
        }
        Ok(())
    }
}

// More like PlzDontFuzzMeCodec amirite
impl Codec<ID> for BadCodec {
    type Error = BadCodecError;

    fn encode_header(
        &mut self,
        header: &Header<ID>,
        mut buf: impl BufMut,
    ) -> Result<(), Self::Error> {
        BadCodec::encode_header(self, header, &mut buf)?;
        Ok(())
    }

    fn decode_header(&mut self, mut buf: impl Buf) -> Result<Header<ID>, Self::Error> {
        BadCodec::decode_header(self, &mut buf)
    }

    fn encode_member(
        &mut self,
        member: &Member<ID>,
        mut buf: impl BufMut,
    ) -> Result<(), Self::Error> {
        BadCodec::encode_member(self, member, &mut buf)
    }

    fn decode_member(&mut self, mut buf: impl Buf) -> Result<Member<ID>, Self::Error> {
        BadCodec::decode_member(self, &mut buf)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InMemoryRuntime {
    notifications: Vec<Notification<ID>>,
    to_send: Vec<(ID, Bytes)>,
    to_schedule: Vec<(Timer<ID>, Duration)>,
}

impl InMemoryRuntime {
    pub(crate) fn new() -> Self {
        Self {
            notifications: Vec::new(),
            to_send: Vec::new(),
            to_schedule: Vec::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.notifications.clear();
        self.to_send.clear();
        self.to_schedule.clear();
    }

    pub(crate) fn take_all_data(&mut self) -> Vec<(ID, Bytes)> {
        core::mem::take(&mut self.to_send)
    }

    pub(crate) fn take_data(&mut self, dst: ID) -> Option<Bytes> {
        let position = self.to_send.iter().position(|(to, _data)| to == &dst)?;

        let taken = self.to_send.swap_remove(position);
        Some(taken.1)
    }

    pub(crate) fn take_notification(
        &mut self,
        wanted: Notification<ID>,
    ) -> Option<Notification<ID>> {
        let position = self
            .notifications
            .iter()
            .position(|notification| notification == &wanted)?;

        let taken = self.notifications.swap_remove(position);
        Some(taken)
    }

    pub(crate) fn take_scheduling(&mut self, timer: Timer<ID>) -> Option<Duration> {
        let position = self
            .to_schedule
            .iter()
            .position(|(event, _when)| event == &timer)?;

        let taken = self.to_schedule.swap_remove(position);
        Some(taken.1)
    }

    pub(crate) fn find_scheduling<F>(&self, predicate: F) -> Option<&Timer<ID>>
    where
        F: Fn(&Timer<ID>) -> bool,
    {
        self.to_schedule
            .iter()
            .find(|(timer, _)| predicate(timer))
            .map(|(timer, _)| timer)
    }
}

impl Runtime<ID, ID> for InMemoryRuntime {
    fn notify(&mut self, notification: Notification<ID>) {
        self.notifications.push(notification);
    }

    fn send_to(&mut self, to: ID, data: &[u8]) {
        self.to_send.push((to, Bytes::copy_from_slice(data)));
    }

    fn submit_after(&mut self, event: Timer<ID>, after: Duration) {
        self.to_schedule.push((event, after));
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct TrivialID(u64);

#[cfg(any(feature = "bincode-codec", feature = "postcard-codec"))]
pub(crate) fn verify_codec_roundtrip<C: Codec<TrivialID>>(mut codec: C) -> Result<(), C::Error> {
    let mut buf = Vec::new();

    let payload = Header {
        src: TrivialID(1),
        src_incarnation: 0,
        dst: TrivialID(2),
        message: Message::PingReq {
            target: TrivialID(3),
            probe_number: 1,
        },
    };

    codec.encode_header(&payload, &mut buf)?;
    let decoded = codec.decode_header(&buf[..])?;

    assert_eq!(payload, decoded);

    buf.clear();
    let member = Member::new(TrivialID(42), 12, State::Down);

    codec.encode_member(&member, &mut buf)?;
    let decoded = codec.decode_member(&buf[..])?;

    assert_eq!(member, decoded);

    Ok(())
}

mod tests {
    use super::*;

    #[test]
    fn message_roundtrip() {
        let messages = [
            Message::Ping(1),
            Message::Ack(2),
            Message::PingReq {
                target: ID::new(3),
                probe_number: 4,
            },
            Message::IndirectPing {
                origin: ID::new_with_bump(5, 6),
                probe_number: 7,
            },
            Message::IndirectAck {
                target: ID::new_with_bump(8, 9),
                probe_number: 10,
            },
            Message::ForwardedAck {
                origin: ID::new_with_bump(11, 12),
                probe_number: 13,
            },
            Message::Gossip,
            Message::Announce,
            Message::Feed,
        ];

        let codec = BadCodec;
        let mut buf = alloc::vec![0; 1500];

        for msg in messages.iter() {
            codec.encode_message(msg, &mut buf[..]).unwrap();
            let decoded = codec.decode_message(&buf[..]).unwrap();
            assert_eq!(msg, &decoded);
        }
    }

    #[test]
    fn header_roundtrip() {
        let header = Header {
            src: ID::new(0),
            src_incarnation: 710,
            dst: ID::new_with_bump(1, 2),
            message: Message::ForwardedAck {
                origin: ID::new_with_bump(2, 254),
                probe_number: 11,
            },
        };

        let codec = BadCodec;
        let mut buf = Vec::new();

        codec.encode_header(&header, &mut buf).unwrap();
        let decoded = codec.decode_header(&buf[..]).unwrap();

        assert_eq!(header, decoded);
    }

    #[test]
    fn member_roundtrip() {
        for state in [State::Alive, State::Suspect, State::Down] {
            let member = Member::new(ID::new_with_bump(7, 13), 420, state);
            let codec = BadCodec;
            let mut buf = Vec::new();

            codec.encode_member(&member, &mut buf).unwrap();
            let decoded = codec.decode_member(&buf[..]).unwrap();

            assert_eq!(member, decoded);
        }
    }
}
