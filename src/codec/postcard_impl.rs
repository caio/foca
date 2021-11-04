/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use bytes::{Buf, BufMut};
use postcard::flavors::SerFlavor;

use crate::{Codec, Header, Member};

/// PostcardCodec encodes/decodes packets using [`postcard`].
#[derive(Debug, Clone, Copy)]
pub struct PostcardCodec;

// XXX We can use Buf::chunk here because Foca guarantees the buffer is
//     contiguous... Maybe a marker trait like `trait ContiguousBuf: Buf {}`
//     or some other form of making it explicit in the type would be
//     helpful?
impl<T> Codec<T> for PostcardCodec
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    type Error = postcard::Error;

    fn encode_header(&mut self, payload: &Header<T>, buf: impl BufMut) -> Result<(), Self::Error> {
        postcard::serialize_with_flavor(payload, WrappedBuf(buf))
    }

    fn decode_header(&mut self, mut buf: impl Buf) -> Result<Header<T>, Self::Error> {
        let remaining = buf.remaining();
        debug_assert_eq!(remaining, buf.chunk().len());
        let (payload, rest) = postcard::take_from_bytes(buf.chunk())?;
        let after = rest.len();
        buf.advance(remaining - after);
        Ok(payload)
    }

    fn encode_member(&mut self, member: &Member<T>, buf: impl BufMut) -> Result<(), Self::Error> {
        postcard::serialize_with_flavor(member, WrappedBuf(buf))
    }

    fn decode_member(&mut self, mut buf: impl Buf) -> Result<Member<T>, Self::Error> {
        let remaining = buf.remaining();
        debug_assert_eq!(remaining, buf.chunk().len());
        let (member, rest) = postcard::take_from_bytes(buf.chunk())?;
        let after = rest.remaining();
        buf.advance(remaining - after);
        Ok(member)
    }
}

struct WrappedBuf<B>(B);

impl<B: BufMut> SerFlavor for WrappedBuf<B> {
    type Output = ();

    fn try_push(&mut self, data: u8) -> Result<(), ()> {
        if self.0.has_remaining_mut() {
            self.0.put_u8(data);
            Ok(())
        } else {
            Err(())
        }
    }

    fn release(self) -> Result<Self::Output, ()> {
        Ok(())
    }

    fn try_extend(&mut self, data: &[u8]) -> Result<(), ()> {
        if self.0.remaining_mut() >= data.len() {
            self.0.put_slice(data);
            Ok(())
        } else {
            Err(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::PostcardCodec;

    #[test]
    fn postcard_roundtrip() -> Result<(), postcard::Error> {
        crate::testing::verify_codec_roundtrip(PostcardCodec)
    }
}
