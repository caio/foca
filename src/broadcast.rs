/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use alloc::vec::Vec;
use core::{cmp::Ordering, fmt};

use bytes::{Buf, BufMut};

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
    type Broadcast: Invalidates + AsRef<[u8]>;

    /// The error type that `receive_item` may emit. Will be wrapped
    /// by [`crate::Error`].
    type Error: fmt::Debug + fmt::Display + Send + Sync + 'static;

    /// Decodes a [`Self::Broadcast`] from a buffer and either discards
    /// it or tells Foca to persist and disseminate it.
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
    fn receive_item(&mut self, data: impl Buf) -> Result<Option<Self::Broadcast>, Self::Error>;

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

pub(crate) struct Broadcasts<V> {
    storage: Vec<Entry<V>>,
}

impl<T> Broadcasts<T>
where
    T: Invalidates + AsRef<[u8]>,
{
    pub fn new() -> Self {
        Self {
            storage: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn add_or_replace(&mut self, value: T, max_tx: usize) {
        let new_node = Entry {
            remaining_tx: max_tx,
            value,
        };

        // Can I be smarter here?
        if let Some(position) = self
            .storage
            .iter()
            .position(|node| new_node.value.invalidates(&node.value))
        {
            self.storage.remove(position);
        }

        // Find where to insert whilst keeping the storage sorted
        // Searching from the right may be better since there is a
        // bound and default value for `remaining_tx`
        let position = self
            .storage
            .binary_search(&new_node)
            .unwrap_or_else(|pos| pos);
        self.storage.insert(position, new_node);
    }

    pub fn fill(&mut self, mut buffer: impl BufMut, max_items: usize) -> usize {
        if self.storage.is_empty() {
            return 0;
        }

        let mut num_taken = 0;
        let mut num_removed = 0;
        let starting_len = self.storage.len();
        let mut remaining = max_items;

        // We fill the buffer giving priority to the largest
        // least sent items.
        for idx in (0..starting_len).rev() {
            if !buffer.has_remaining_mut() || remaining == 0 {
                break;
            }

            let node = &mut self.storage[idx];
            let value_len = node.value.as_ref().len();
            debug_assert!(node.remaining_tx > 0);

            if buffer.remaining_mut() >= value_len {
                num_taken += 1;
                remaining -= 1;

                buffer.put_slice(node.value.as_ref());

                if node.remaining_tx == 1 {
                    // Last transmission, gotta remove the node.
                    // It's ok to swap_remove because we're walking
                    // the storage from the right to the left
                    self.storage.swap_remove(idx);
                    num_removed += 1;
                } else {
                    node.remaining_tx -= 1;
                }
            }
        }

        if num_removed > 0 {
            self.storage.truncate(starting_len - num_removed);
        }

        // XXX Any other easy "bail out" scenario?
        let skip_resort = {
            // If we took all the nodes without removing any
            (num_taken == starting_len && num_removed == 0)
                // Or ignored them all
                || num_taken == 0
        };

        if !skip_resort {
            self.storage.sort_unstable();
        }

        debug_assert!(!skip_resort || self.is_sorted());

        num_taken
    }

    pub fn is_sorted(&self) -> bool {
        // Future: `is_sorted` from https://github.com/rust-lang/rfcs/pull/2351
        self.storage[..]
            .windows(2)
            .all(|w| w[0].remaining_tx <= w[1].remaining_tx)
    }
}

#[derive(Debug, Clone)]
struct Entry<T> {
    remaining_tx: usize,
    value: T,
}

impl<T: AsRef<[u8]>> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.remaining_tx == other.remaining_tx
            && self.value.as_ref().len() == other.value.as_ref().len()
    }
}

impl<T: AsRef<[u8]>> Eq for Entry<T> {}

impl<T: AsRef<[u8]>> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = self.remaining_tx.cmp(&other.remaining_tx);

        if ordering == Ordering::Equal {
            self.value.as_ref().len().cmp(&other.value.as_ref().len())
        } else {
            ordering
        }
    }
}

impl<T: AsRef<[u8]>> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
impl<T> Broadcasts<T> {
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    struct TwoByteKey(Vec<u8>);

    impl TwoByteKey {
        fn new(data: impl AsRef<[u8]>) -> Self {
            assert!(
                data.as_ref().len() > 2,
                "first two bytes are used as key for invalidation"
            );
            Self(Vec::from(data.as_ref()))
        }
    }

    impl Invalidates for TwoByteKey {
        fn invalidates(&self, other: &Self) -> bool {
            self.0[..2] == other.0[..2]
        }
    }

    impl AsRef<[u8]> for TwoByteKey {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }

    #[test]
    fn piggyback_behaviour() {
        let max_tx = 5;
        let mut piggyback = Broadcasts::new();

        assert!(piggyback.is_empty(), "Piggyback starts empty");

        piggyback.add_or_replace(TwoByteKey::new(b"AAabc"), max_tx);

        assert_eq!(1, piggyback.len());

        piggyback.add_or_replace(TwoByteKey::new(b"AAcba"), max_tx);

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
        piggyback.add_or_replace(TwoByteKey::new(b"a super long value"), 1);

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

        piggyback.add_or_replace(TwoByteKey::new(b"00hi"), max_tx);
        piggyback.add_or_replace(TwoByteKey::new(b"01hello"), max_tx);
        piggyback.add_or_replace(TwoByteKey::new(b"02hey"), max_tx);

        let mut buf = Vec::new();
        let num_items = piggyback.fill(&mut buf, usize::MAX);

        assert_eq!(3, num_items);
        assert_eq!(b"01hello02hey00hi", &buf[..]);
    }

    #[test]
    fn highest_max_tx_is_consumed_first() {
        let mut piggyback = Broadcasts::new();

        // 3 items, same byte size, distinct max_tx
        piggyback.add_or_replace(TwoByteKey::new(b"100"), 1);
        piggyback.add_or_replace(TwoByteKey::new(b"200"), 2);
        piggyback.add_or_replace(TwoByteKey::new(b"300"), 3);

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

        piggyback.add_or_replace(TwoByteKey::new(b"foo"), max_tx);
        piggyback.add_or_replace(TwoByteKey::new(b"bar"), max_tx);
        piggyback.add_or_replace(TwoByteKey::new(b"baz"), max_tx);

        let mut buf = Vec::new();
        let num_items = piggyback.fill(&mut buf, 0);

        assert_eq!(0, num_items);
        assert!(buf.is_empty());

        let num_items = piggyback.fill(&mut buf, 2);
        assert_eq!(2, num_items);
    }
}
