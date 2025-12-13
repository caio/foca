/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use core::fmt;

use alloc::boxed::Box;

#[derive(Debug)]
/// This type represents all possible errors operating a Foca instance.
pub enum Error {
    /// Emitted whenever Foca receives a byte slice larger than
    /// the configured limit ([`crate::Config::max_packet_size`]).
    ///
    /// Doesn't affect Foca's state.
    DataTooBig,

    /// Attempt to [`crate::Foca::reuse_down_identity`] when not needed.
    ///
    /// Doesn't affect Foca's state.
    NotUndead,

    /// Attempt to [`crate::Foca::change_identity`] with the same identity.
    ///
    /// Doesn't affect Foca's state.
    SameIdentity,

    /// Expected to be connected when reaching this point.
    /// Sentinel error to detect integration bugs.
    ///
    /// Must not happen under normal circumstances.
    NotConnected,

    /// Reached the end of the probe cycle but expected
    /// steps didn't happen. Bug in the runtime/scheduling
    /// mechanism most likely.
    ///
    /// Foca tries to resume normal operations after emitting this
    /// error, but any occurance of it is a sign that something is
    /// not behaving as expected
    ///
    /// Must not happen under normal circumstances.
    IncompleteProbeCycle,

    /// Received data where the sender has the same
    /// id as ourselves.
    ///
    /// There's likely a member submitting wrong/manually-crafted
    /// packets.
    DataFromOurselves,

    /// Data contains a message supposed to reach us via indirect
    /// means.
    ///
    /// There's likely a member submitting wrong/manually-crafted
    /// packets.
    IndirectForOurselves,

    /// Data is in an unexpected format.
    ///
    /// Doesn't affect Foca's state.
    MalformedPacket,

    /// Wraps [`crate::Codec`]'s `encode_*` failures.
    /// Shouldn't happen under normal circumstances unless using a broken
    /// codec.
    ///
    /// Might have left Foca in a inconsistent state.
    Encode(Box<dyn core::error::Error + Send + Sync>),

    /// Wraps [`crate::Codec`]'s `decode_*` failures.
    ///
    /// Can happen during normal operation when receiving junk data.
    Decode(Box<dyn core::error::Error + Send + Sync>),

    /// Wraps [`crate::BroadcastHandler`] failures.
    ///
    /// Doesn't affect Foca's state.
    CustomBroadcast(Box<dyn core::error::Error + Send + Sync>),

    /// Configuration change not allowed.
    ///
    /// Doesn't affact Foca's state.
    ///
    /// See [`crate::Foca::set_config`].
    InvalidConfig,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        use alloc::string::ToString;
        #[allow(clippy::match_same_arms)]
        match (self, other) {
            // Wrapped errors have to allocate to compare :(
            // But PartialEq on an error type is mostly useful for tests
            (Self::Encode(a), Self::Encode(b)) => a.to_string().eq(&b.to_string()),
            (Self::Decode(a), Self::Decode(b)) => a.to_string().eq(&b.to_string()),
            (Self::CustomBroadcast(a), Self::CustomBroadcast(b)) => {
                a.to_string().eq(&b.to_string())
            }

            (Self::DataTooBig, Self::DataTooBig) => true,
            (Self::NotConnected, Self::NotConnected) => true,
            (Self::NotUndead, Self::NotUndead) => true,
            (Self::SameIdentity, Self::SameIdentity) => true,
            (Self::IncompleteProbeCycle, Self::IncompleteProbeCycle) => true,
            (Self::DataFromOurselves, Self::DataFromOurselves) => true,
            (Self::IndirectForOurselves, Self::IndirectForOurselves) => true,
            (Self::MalformedPacket, Self::MalformedPacket) => true,
            (Self::InvalidConfig, Self::InvalidConfig) => true,

            // Instead of a catch-all here, we explicitly enumerate our variants
            // so that when/if new errors are added we don't silently introduce
            // a bug
            (Self::Encode(_), _) => false,
            (Self::Decode(_), _) => false,
            (Self::CustomBroadcast(_), _) => false,
            (Self::DataTooBig, _) => false,
            (Self::NotConnected, _) => false,
            (Self::NotUndead, _) => false,
            (Self::SameIdentity, _) => false,
            (Self::IncompleteProbeCycle, _) => false,
            (Self::DataFromOurselves, _) => false,
            (Self::IndirectForOurselves, _) => false,
            (Self::MalformedPacket, _) => false,
            (Self::InvalidConfig, _) => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[allow(clippy::match_same_arms)]
        match self {
            Self::DataTooBig => {
                formatter.write_str("Received data larger than maximum configured limit")
            }
            Self::NotUndead => formatter.write_str("Useless attempt to reuse a functioning Foca"),
            Self::SameIdentity => {
                formatter.write_str("New identity is the same as the current one")
            }
            Self::NotConnected => formatter.write_str("BUG! Expected to be connected, but wasn't"),
            Self::IncompleteProbeCycle => {
                formatter.write_str("BUG! Probe cycle finished without running its full course")
            }
            Self::DataFromOurselves => formatter.write_str(concat!(
                "Received data from something claiming to have ",
                "an identity equal to our own"
            )),
            Self::IndirectForOurselves => formatter.write_str(concat!(
                "Received message that was supposed to reach us only ",
                "via indirect means"
            )),
            Self::MalformedPacket => formatter.write_str("Payload with more data than expected"),
            Self::Encode(err) => err.fmt(formatter),
            Self::Decode(err) => err.fmt(formatter),
            Self::CustomBroadcast(err) => err.fmt(formatter),
            Self::InvalidConfig => formatter.write_str("Invalid configuration"),
        }
    }
}

impl core::error::Error for Error {}

#[cfg(test)]
mod tests {
    fn ensure_send_sync<T: Send + Sync>(_val: T) {}

    #[test]
    fn errors_are_sync() {
        ensure_send_sync(super::Error::InvalidConfig);
    }
}
