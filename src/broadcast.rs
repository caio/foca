/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use alloc::vec::Vec;
use core::{cmp::Ordering, fmt};

use bytes::BufMut;

/// A type capable of decoding a (associated) broadcast from a buffer
/// and deciding whether to keep disseminating it for other members
/// of the cluster (when it's new information) or to discard it (when
/// its outdated/stale).
pub trait BroadcastHandler<T> {
    /// Concrete type that will be disseminated to all cluster members.
    ///
    /// It should be able to compare itself against an arbitrary number
    /// of other [`Self::Broadcast`] instances and decide wether it
    /// replaces it or not so conflicting/stale information isn't
    /// disseminated.
    ///
    /// The `AsRef<[u8]>` part is what gets sent over the wire, which
    /// [`Self::receive_item`] is supposed to decode.
    type Key: Invalidates;

    /// The error type that `receive_item` may emit. Will be wrapped
    /// by [`crate::Error`].
    type Error: fmt::Debug + fmt::Display + Send + Sync + 'static;

    /// Decodes a [`Self::Broadcast`] from a buffer and either discards
    /// it or tells Foca to persist and disseminate it.
    ///
    /// `Sender` is `None` when you're adding broadcast data directly,
    /// via [`crate::Foca::add_broadcast`], otherwise it will be the
    /// address of the member that sent the data. Notice that Foca
    /// doesn't track the origin of packets- if you need it you
    /// have to add it to the data you're broadcasting.
    ///
    /// When you receive a broadcast you have to decide whether it's
    /// new information that needs to be disseminated (`Ok(Some(...))`)
    /// or not (`Ok(None)`).
    ///
    /// Always yielding `Some(...)` is wrong because Foca will never
    /// know when to stop sending this information to other members.
    ///
    /// Example: Assume your custom broadcast is a simple Set-Key-Value
    /// operation. When you receive it you should check if your map
    /// contains the Key-Value pair; If it didn't, you yield
    /// `Some`, otherwise the operation is stale, so you yield `None`.
    ///
    /// Implementations MUST read a single [`Self::Broadcast`] from the
    /// buffer and advance the cursor accordingly.
    ///
    /// Implementations may assume the data in the buffer is contiguous.
    fn receive_item(
        &mut self,
        data: &[u8],
        sender: Option<&T>,
    ) -> Result<Option<Self::Key>, Self::Error>;

    /// Decides whether Foca should add broadcast data to the message
    /// it's about to send to active member `T`.
    ///
    /// Normally when Foca sends a message it always tries to include
    /// custom broadcasts alongside the information it actually
    /// cares about; This allows implementations to override this
    /// logic with something else.
    ///
    /// Example: You are running a heterogeneous cluster and some nodes
    /// are always very busy and you'd rather they never have to deal
    /// with the extra cpu/bandwidth cost of receiving/sending
    /// your custom broadcasts.
    ///
    /// Returning `true` tells Foca to proceed as it would normally,
    /// including broadcasts in the messages it sends when it can.
    ///
    /// Returning `false` tells Foca to not include such broadcasts
    /// in the message. It does *not* prevent the message from being
    /// sent, just keeps Foca from attaching extra data to them.
    fn should_add_broadcast_data(&self, _member: &T) -> bool {
        true
    }
}

/// A type that's able to look at another and decide wether it's
/// newer/fresher (i.e. invalidates) than it.
///
/// As you send/receive broadcasts, Foca will hold them for a while
/// as it disseminates the data to other cluster members. This trait
/// helps with replacing data that hasn't been fully disseminated
/// yet but you already know it's stale.
///
/// Example: Assume a versioned broadcast like `{key,version,...}`;
/// After you receive `{K, 0, ...}` and keep it, Foca will be
/// disseminating it. Soon after you receive `{K, 1, ...}` which
/// is a newer version for this broadcast. This trait enables
/// Foca to immediately stop disseminating the previous version,
/// even if it hasn't sent it to everyone it can yet.
pub trait Invalidates {
    /// When `item.invalidates(&other)` it means that Foca will
    /// keep `item` and discard `other` from its dissemination
    /// backlog.
    fn invalidates(&self, other: &Self) -> bool;
}

impl<'a> Invalidates for &'a [u8] {
    fn invalidates(&self, other: &Self) -> bool {
        self.eq(other)
    }
}

#[allow(dead_code)]
pub(crate) struct Broadcasts<V> {
    flip: alloc::collections::BinaryHeap<Entry<V>>,
    flop: alloc::collections::BinaryHeap<Entry<V>>,
}

impl<T> Broadcasts<T>
where
    T: Invalidates,
{
    pub(crate) fn new() -> Self {
        Self {
            flip: Default::default(),
            flop: Default::default(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.flip.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.flip.is_empty()
    }

    pub(crate) fn add_or_replace(&mut self, item: T, data: Vec<u8>, max_tx: usize) {
        debug_assert!(max_tx > 0);
        self.flip.retain(|node| !item.invalidates(&node.item));
        self.flip.push(Entry {
            remaining_tx: max_tx,
            item,
            data,
        });
    }

    pub(crate) fn fill(&mut self, mut buffer: impl BufMut, max_items: usize) -> usize {
        if self.flip.is_empty() {
            return 0;
        }
        debug_assert!(self.flop.is_empty());

        let mut num_taken = 0;
        let mut remaining = max_items;

        while buffer.has_remaining_mut() && remaining > 0 {
            let Some(mut node) = self.flip.pop() else {
                break;
            };
            debug_assert!(node.remaining_tx > 0);

            if buffer.remaining_mut() >= node.data.len() {
                num_taken += 1;
                remaining -= 1;

                buffer.put_slice(&node.data);
                node.remaining_tx -= 1;
            }

            if node.remaining_tx > 0 {
                self.flop.push(node);
            }
        }

        self.flip.append(&mut self.flop);

        num_taken
    }

    pub(crate) fn fill_with_len_prefix(
        &mut self,
        mut buffer: impl BufMut,
        max_items: usize,
    ) -> usize {
        if self.flip.is_empty() {
            return 0;
        }
        debug_assert!(self.flop.is_empty());

        let mut num_taken = 0;
        let mut remaining = max_items;

        while buffer.has_remaining_mut() && remaining > 0 {
            let Some(mut node) = self.flip.pop() else {
                break;
            };
            debug_assert!(node.remaining_tx > 0);

            if buffer.remaining_mut() >= node.data.len() + 2 {
                num_taken += 1;
                remaining -= 1;

                debug_assert!(node.data.len() <= core::u16::MAX as usize);
                buffer.put_u16(node.data.len() as u16);
                buffer.put_slice(&node.data);
                node.remaining_tx -= 1;
            }

            if node.remaining_tx > 0 {
                self.flop.push(node);
            }
        }

        self.flip.append(&mut self.flop);

        num_taken
    }
}

#[derive(Debug, Clone)]
struct Entry<T> {
    remaining_tx: usize,
    // XXX could be Bytes, or keep a pool in parent
    data: Vec<u8>,
    // ignored for eq/ord. sorting is unstable
    item: T,
}

impl<T> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<T> Eq for Entry<T> {}

impl<T> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.remaining_tx
            .cmp(&other.remaining_tx)
            .then_with(|| self.data.len().cmp(&other.data.len()))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    struct Key(&'static str);

    impl Invalidates for Key {
        fn invalidates(&self, other: &Self) -> bool {
            self.0 == other.0
        }
    }

    #[test]
    fn piggyback_behaviour() {
        let max_tx = 5;
        let mut piggyback = Broadcasts::new();

        assert!(piggyback.is_empty(), "Piggyback starts empty");

        piggyback.add_or_replace(Key("AA"), b"AAabc".to_vec(), max_tx);

        assert_eq!(1, piggyback.len());

        piggyback.add_or_replace(Key("AA"), b"AAcba".to_vec(), max_tx);

        assert_eq!(
            1,
            piggyback.len(),
            "add_or_replace with same key should replace"
        );

        let mut buf = Vec::new();

        for _i in 0..max_tx {
            buf.clear();
            let num_items = piggyback.fill(&mut buf, usize::MAX);
            assert_eq!(1, num_items);
            assert_eq!(
                b"AAcba",
                &buf[..],
                "Should transmit an item at most max_tx times"
            );
        }

        assert!(
            piggyback.is_empty(),
            "Should remove item after being used max_tx times"
        );
    }

    #[test]
    fn fill_does_nothing_if_buffer_full() {
        let mut piggyback = Broadcasts::new();
        piggyback.add_or_replace(Key("a "), b"a super long value".to_vec(), 1);

        let buf = bytes::BytesMut::new();
        let mut limited = buf.limit(5);

        let num_items = piggyback.fill(&mut limited, usize::MAX);

        assert_eq!(0, num_items);
        assert_eq!(5, limited.remaining_mut());
        assert_eq!(1, piggyback.len());
    }

    #[test]
    fn piggyback_consumes_largest_first() {
        let max_tx = 10;
        let mut piggyback = Broadcasts::new();

        piggyback.add_or_replace(Key("00"), b"00hi".to_vec(), max_tx);
        piggyback.add_or_replace(Key("01"), b"01hello".to_vec(), max_tx);
        piggyback.add_or_replace(Key("02"), b"02hey".to_vec(), max_tx);

        let mut buf = Vec::new();
        let num_items = piggyback.fill(&mut buf, usize::MAX);

        assert_eq!(3, num_items);
        assert_eq!(b"01hello02hey00hi", &buf[..]);
    }

    #[test]
    fn highest_max_tx_is_consumed_first() {
        let mut piggyback = Broadcasts::new();

        // 3 items, same byte size, distinct max_tx
        piggyback.add_or_replace(Key("10"), b"100".to_vec(), 1);
        piggyback.add_or_replace(Key("20"), b"200".to_vec(), 2);
        piggyback.add_or_replace(Key("30"), b"300".to_vec(), 3);

        let mut buf = Vec::new();
        piggyback.fill(&mut buf, usize::MAX);
        assert_eq!(b"300200100", &buf[..]);

        buf.clear();
        piggyback.fill(&mut buf, usize::MAX);
        assert_eq!(b"300200", &buf[..]);

        buf.clear();
        piggyback.fill(&mut buf, usize::MAX);
        assert_eq!(b"300", &buf[..]);

        assert_eq!(0, piggyback.len());
    }

    #[test]
    fn piggyback_respects_limit() {
        let max_tx = 10;
        let mut piggyback = Broadcasts::new();

        piggyback.add_or_replace(Key("fo"), b"foo".to_vec(), max_tx);
        piggyback.add_or_replace(Key("ba"), b"bar".to_vec(), max_tx);
        piggyback.add_or_replace(Key("ba"), b"baz".to_vec(), max_tx);

        let mut buf = Vec::new();
        let num_items = piggyback.fill(&mut buf, 0);

        assert_eq!(0, num_items);
        assert!(buf.is_empty());

        let num_items = piggyback.fill(&mut buf, 2);
        assert_eq!(2, num_items);
    }

    #[test]
    fn fill_with_len_prefix() {
        let mut bcs = Broadcasts::new();

        bcs.add_or_replace(Key("fo"), b"foo".to_vec(), 10);
        bcs.add_or_replace(Key("ba"), b"barr".to_vec(), 10);
        bcs.add_or_replace(Key("ba"), b"bazz".to_vec(), 10);

        let mut buf = Vec::new();
        let num_items = bcs.fill_with_len_prefix(&mut buf, 0);

        assert_eq!(0, num_items);
        assert!(buf.is_empty());

        let num_items = bcs.fill_with_len_prefix(&mut buf, 2);
        assert_eq!(2, num_items);

        use bytes::Buf;
        let mut buf = &buf[..];
        assert_eq!(4, buf.get_u16());
        assert_eq!(&b"bazz"[..], &buf[..4]);
        buf.advance(4);
        assert_eq!(3, buf.get_u16());
        assert_eq!(&b"foo"[..], &buf[..3]);
        buf.advance(3);
        assert!(buf.is_empty());
    }
}
