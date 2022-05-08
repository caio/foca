/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use core::fmt;

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
    Encode(anyhow::Error),

    /// Wraps [`crate::Codec`]'s `decode_*` failures.
    ///
    /// Can happen during normal operation when receiving junk data.
    Decode(anyhow::Error),

    /// Wraps [`crate::BroadcastHandler`] failures.
    ///
    /// Doesn't affect Foca's state.
    CustomBroadcast(anyhow::Error),

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
        match (self, other) {
            // Wrapped errors have to allocate to compare :(
            // But PartialEq on an error type is mostly useful for tests
            (Error::Encode(a), Error::Encode(b)) => a.to_string().eq(&b.to_string()),
            (Error::Decode(a), Error::Decode(b)) => a.to_string().eq(&b.to_string()),
            (Error::CustomBroadcast(a), Error::CustomBroadcast(b)) => {
                a.to_string().eq(&b.to_string())
            }

            (Error::DataTooBig, Error::DataTooBig) => true,
            (Error::NotConnected, Error::NotConnected) => true,
            (Error::NotUndead, Error::NotUndead) => true,
            (Error::SameIdentity, Error::SameIdentity) => true,
            (Error::IncompleteProbeCycle, Error::IncompleteProbeCycle) => true,
            (Error::DataFromOurselves, Error::DataFromOurselves) => true,
            (Error::IndirectForOurselves, Error::IndirectForOurselves) => true,
            (Error::MalformedPacket, Error::MalformedPacket) => true,
            (Error::InvalidConfig, Error::InvalidConfig) => true,

            // Instead of a catch-all here, we explicitly enumerate our variants
            // so that when/if new errors are added we don't silently introduce
            // a bug
            (Error::Encode(_), _) => false,
            (Error::Decode(_), _) => false,
            (Error::CustomBroadcast(_), _) => false,
            (Error::DataTooBig, _) => false,
            (Error::NotConnected, _) => false,
            (Error::NotUndead, _) => false,
            (Error::SameIdentity, _) => false,
            (Error::IncompleteProbeCycle, _) => false,
            (Error::DataFromOurselves, _) => false,
            (Error::IndirectForOurselves, _) => false,
            (Error::MalformedPacket, _) => false,
            (Error::InvalidConfig, _) => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::DataTooBig => {
                formatter.write_str("Received data larger than maximum configured limit")
            }
            Error::NotUndead => formatter.write_str("Useless attempt to reuse a functioning Foca"),
            Error::SameIdentity => {
                formatter.write_str("New identity is the same as the current one")
            }
            Error::NotConnected => formatter.write_str("BUG! Expected to be connected, but wasn't"),
            Error::IncompleteProbeCycle => {
                formatter.write_str("BUG! Probe cycle finished without running its full course")
            }
            Error::DataFromOurselves => formatter.write_str(concat!(
                "Received data from something claiming to have ",
                "an identity equal to our own"
            )),
            Error::IndirectForOurselves => formatter.write_str(concat!(
                "Received message that was supposed to reach us only ",
                "via indirect means"
            )),
            Error::MalformedPacket => formatter.write_str("Payload with more data than expected"),
            Error::Encode(err) => err.fmt(formatter),
            Error::Decode(err) => err.fmt(formatter),
            Error::CustomBroadcast(err) => err.fmt(formatter),
            Error::InvalidConfig => formatter.write_str("Invalid configuration"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}
