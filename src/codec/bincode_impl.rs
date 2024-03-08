/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use crate::{Codec, Header, Member};

/// `BincodeCodec` encodes/decodes messages using [`bincode`].
///
/// This struct simply wraps a [`bincode::Options`] type:
///
/// ~~~rust
/// let codec =
///     foca::BincodeCodec(bincode::DefaultOptions::new());
/// ~~~
#[derive(Debug, Clone, Copy)]
pub struct BincodeCodec<O: bincode::Options>(pub O);

impl<T, O> Codec<T> for BincodeCodec<O>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    O: bincode::Options + Copy,
{
    type Error = bincode::Error;

    fn encode_header(
        &mut self,
        payload: &Header<T>,
        buf: impl bytes::BufMut,
    ) -> Result<(), Self::Error> {
        self.0.serialize_into(buf.writer(), payload)
    }

    fn decode_header(&mut self, buf: impl bytes::Buf) -> Result<Header<T>, Self::Error> {
        self.0.deserialize_from(buf.reader())
    }

    fn encode_member(
        &mut self,
        member: &Member<T>,
        buf: impl bytes::BufMut,
    ) -> Result<(), Self::Error> {
        self.0.serialize_into(buf.writer(), member)
    }

    fn decode_member(&mut self, buf: impl bytes::Buf) -> Result<Member<T>, Self::Error> {
        self.0.deserialize_from(buf.reader())
    }
}

#[cfg(test)]
mod tests {
    use super::BincodeCodec;

    #[test]
    fn bincode_roundtrip() -> Result<(), bincode::Error> {
        crate::testing::verify_codec_roundtrip(BincodeCodec(bincode::DefaultOptions::new()))
    }
}
