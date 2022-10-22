/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use crate::Incarnation;

/// The preamble of every datagram sent by Foca.
///
/// A foca packet is always:
///
/// - A Header. Optionally followed by:
/// - A `u16` in network byte order to signal how many updates are
///   expected;
/// - A sequence of said `u16` updates (`foca::Member`);
/// - And finally a tail of custom broadcasts, if at all used.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Header<T> {
    /// The identity of the sender
    pub src: T,
    /// The sender's incarnation, so that other cluster members
    /// may keep it up-to-date
    pub src_incarnation: Incarnation,
    /// The target of the message
    pub dst: T,
    /// The actual message
    pub message: Message<T>,
}

/// Messages are how members request interaction from each other.
///
/// There are a few different kind of interactions that may occur:
///
/// ## Direct Probe Cycle
///
/// Foca will periodically check if members are still active. It
/// sends a `Ping` to said member and expects an `Ack` in return.
///
/// If `B` takes too long to reply with an Ack, the indirect
/// probe cycle starts.
///
/// ## Indirect Probe Cycle
///
/// A previously pinged member may be too busy, its reply may have been
/// dropped by an unreliable network or maybe it's actually down.
///
/// The indirect probe cycle helps with getting more certainty about
/// its current state by asking other members to execute a ping
/// on our behalf.
///
/// Here, member `A` will ask member `C` to ping `B` on their
/// behalf:
///
/// ~~~txt
/// A ->[PingReq(B)]      C
/// C ->[IndirectPing(A)] B
/// B ->[IndirectAck(A)]  C
/// C ->[ForwardedAck(B)] A
/// ~~~
///
/// If by the end of the full probe cycle (direct and indirect) Foca
/// has received either an `Ack` or a `ForwardedAck`, the member is
/// considered active. Otherwise the member is declared `State::Suspect`
/// and will need to refute it before the configured deadline
/// else it will be declared `State::Down`.
///
/// ## "Join" sub-protocol
///
/// Foca instances can join a cluster by sending `Announce` messages
/// to one or more identities. If a recipient decides to accept it,
/// it replies with a `Feed` message, containing other active cluster
/// members.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Message<T> {
    /// A Ping message. Replied with `Ack`.
    Ping(ProbeNumber),
    /// Acknowledment of a Ping. Reply to `Ping`.
    Ack(ProbeNumber),

    /// Receiver is being asked to Ping `target` on their behalf
    /// and should emit `IndirectPing(sender)` to `target`
    PingReq {
        /// The identity that failed to reply to the original
        /// `Ping` in a timely manner.
        target: T,
        /// See `foca::ProbeNumber`
        probe_number: ProbeNumber,
    },

    /// Analogous to `Ping`, with added metadata about the original
    /// requesting member.
    /// Recipient should reply `IndirectAck(origin)` to sender
    IndirectPing {
        /// The identity that started the indirect cycle. I.e.:
        /// whoever sent the unanswered `Ping`.
        origin: T,
        /// See `foca::ProbeNumber`
        probe_number: ProbeNumber,
    },

    /// Analogous to `Ack`, with added metadta about the final
    /// destination.
    /// Recipient should emit `ForwardedAck(sender)` to `target`
    IndirectAck {
        /// The identity that started the indirect cycle. I.e.:
        /// whoever sent the unanswered `Ping`.
        target: T,
        /// See `foca::ProbeNumber`
        probe_number: ProbeNumber,
    },

    /// The result of a successful indirect probe cycle. Sender
    /// is indicating that they've managed to ping and receive
    /// an ack from `origin`
    ForwardedAck {
        /// The identity that failed to reply to the original
        /// `Ping` in a timely manner.
        origin: T,
        /// See `foca::ProbeNumber`
        probe_number: ProbeNumber,
    },

    /// Request to join a cluster. Replied with `Feed`.
    Announce,
    /// Response to a Announce, signals that the remaining bytes in the
    /// payload will be a sequence of active members, instead of just
    /// cluster updates. Reply to `Announce`.
    Feed,

    /// Deliberate dissemination of cluster updates.
    /// Non-interactive, doesn't expect a reply.
    Gossip,

    /// Deliberate dissemination of custom broadcasts. Broadcast
    /// messages do not contain cluster updates.
    Broadcast,
}

/// ProbeNumber is simply a bookkeeping mechanism to try and prevent
/// incorrect sequencing of protocol messages.
///
/// Similar in spirit to `foca::TimerToken`.
pub type ProbeNumber = u8;
