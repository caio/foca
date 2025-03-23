/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use crate::{Codec, Header, Member};

/// `BincodeCodec` encodes/decodes messages using [`bincode`].
///
/// This struct simply wraps a [`bincode::config::Config`] type:
///
/// ~~~rust
/// let codec =
///     foca::BincodeCodec(bincode::config::standard());
/// ~~~
#[derive(Debug, Clone, Copy)]
pub struct BincodeCodec<O: bincode::config::Config>(pub O);

/// `Error` wraps [`bincode::error`] error enums
#[derive(Debug)]
pub enum Error {
    /// See [`bincode::error::EncodeError`]
    Encode(bincode::error::EncodeError),
    /// See [`bincode::error::DecodeError`]
    Decode(bincode::error::DecodeError),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl core::error::Error for Error {}

impl<T, O> Codec<T> for BincodeCodec<O>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    O: bincode::config::Config + Copy,
{
    type Error = Error;

    fn encode_header(
        &mut self,
        payload: &Header<T>,
        buf: impl bytes::BufMut,
    ) -> Result<(), Self::Error> {
        let mut writer = buf.writer();
        bincode::serde::encode_into_std_write(payload, &mut writer, self.0)
            .map(|_| ())
            .map_err(Error::Encode)
    }

    fn decode_header(&mut self, buf: impl bytes::Buf) -> Result<Header<T>, Self::Error> {
        let mut reader = buf.reader();
        bincode::serde::decode_from_std_read(&mut reader, self.0).map_err(Error::Decode)
    }

    fn encode_member(
        &mut self,
        member: &Member<T>,
        buf: impl bytes::BufMut,
    ) -> Result<(), Self::Error> {
        let mut writer = buf.writer();
        bincode::serde::encode_into_std_write(member, &mut writer, self.0)
            .map(|_| ())
            .map_err(Error::Encode)
    }

    fn decode_member(&mut self, buf: impl bytes::Buf) -> Result<Member<T>, Self::Error> {
        let mut reader = buf.reader();
        bincode::serde::decode_from_std_read(&mut reader, self.0).map_err(Error::Decode)
    }
}

#[cfg(test)]
mod tests {
    use super::{BincodeCodec, Error};

    #[test]
    fn bincode_roundtrip() -> Result<(), Error> {
        crate::testing::verify_codec_roundtrip(BincodeCodec(bincode::config::standard()))
    }
}
