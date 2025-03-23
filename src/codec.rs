/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use core::fmt;

use bytes::{Buf, BufMut};

use crate::{Header, Member};

#[cfg(feature = "bincode-codec")]
pub(crate) mod bincode_impl;

#[cfg(feature = "postcard-codec")]
pub(crate) mod postcard_impl;

/// A Codec is responsible to encoding and decoding the data that
/// is sent between cluster members.
///
/// So you can paint your bike shed however you like.
pub trait Codec<T> {
    /// The codec error type. Will be wrapped by [`crate::Error`].
    type Error: fmt::Debug + fmt::Display + Send + Sync + 'static;

    /// Encodes a `foca::Header` into the given buffer.
    fn encode_header(&mut self, header: &Header<T>, buf: impl BufMut) -> Result<(), Self::Error>;

    /// Decode a [`Header`] from the given buffer.
    ///
    /// Implementations MUST read a single item from the buffer and
    /// advance the cursor accordingly.
    ///
    /// Implementations may assume the data in the buffer is contiguous.
    fn decode_header(&mut self, buf: impl Buf) -> Result<Header<T>, Self::Error>;

    /// Encodes a [`Member`] into the given buffer.
    ///
    /// Implementations MUST NOT leave the buffer dirty when there's
    /// not enough space to encode the item.
    fn encode_member(&mut self, member: &Member<T>, buf: impl BufMut) -> Result<(), Self::Error>;

    /// Decode a [`Member`] from the given buffer.
    ///
    /// Implementations MUST read a single item from the buffer and
    /// advance the cursor accordingly.
    ///
    /// Implementations may assume the data in the buffer is contiguous.
    fn decode_member(&mut self, buf: impl Buf) -> Result<Member<T>, Self::Error>;
}

impl<C, T> Codec<T> for &mut C
where
    C: Codec<T>,
{
    type Error = C::Error;

    fn encode_header(&mut self, header: &Header<T>, buf: impl BufMut) -> Result<(), Self::Error> {
        C::encode_header(self, header, buf)
    }

    fn decode_header(&mut self, buf: impl Buf) -> Result<Header<T>, Self::Error> {
        C::decode_header(self, buf)
    }

    fn encode_member(&mut self, member: &Member<T>, buf: impl BufMut) -> Result<(), Self::Error> {
        C::encode_member(self, member, buf)
    }

    fn decode_member(&mut self, buf: impl Buf) -> Result<Member<T>, Self::Error> {
        C::decode_member(self, buf)
    }
}
