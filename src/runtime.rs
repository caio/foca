/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use core::{cmp::Ordering, time::Duration};

use crate::{Identity, Incarnation};

/// A Runtime is Foca's gateway to the real world: here is where
/// implementations decide how to interact with the network, the
/// hardware timer and the user.
///
/// Implementations may react directly to it for a fully synchronous
/// behavior or accumulate-then-drain when dispatching via fancier
/// mechanisms like async.
pub trait Runtime<T>
where
    T: Identity,
{
    /// Whenever something changes Foca's state significantly a
    /// notification is emitted.
    ///
    /// It's the best mechanism to watch for membership changes
    /// and allows implementors to keep track of the cluster state
    /// without having direct access to the running Foca instance.
    ///
    /// Implementations may completely disregard this if desired.
    fn notify(&mut self, notification: Notification<T>);

    /// This is how Foca connects to an actual transport.
    ///
    /// Implementations are responsible for the actual delivery.
    fn send_to(&mut self, to: T, data: &[u8]);

    /// Request to schedule the delivery of a given event after
    /// a specified duration.
    ///
    /// Implementations MUST ensure that every event is delivered.
    /// Foca is very tolerant to delays, but non-delivery will
    /// cause errors.
    fn submit_after(&mut self, event: Timer<T>, after: Duration);
}

// A mutable reference to a Runtime is a Runtime too
impl<T, R> Runtime<T> for &mut R
where
    T: Identity,
    R: Runtime<T>,
{
    fn notify(&mut self, notification: Notification<T>) {
        R::notify(self, notification);
    }

    fn send_to(&mut self, to: T, data: &[u8]) {
        R::send_to(self, to, data);
    }

    fn submit_after(&mut self, event: Timer<T>, after: Duration) {
        R::submit_after(self, event, after);
    }
}

/// A Notification contains information about high-level relevant
/// state changes in the cluster or Foca itself.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Notification<T> {
    /// Foca discovered a new active member with identity T.
    MemberUp(T),
    /// A previously active member has been declared down by the cluster.
    ///
    /// If Foca detects a down member but didn't know about its activity
    /// before, this notification will not be emitted.
    ///
    /// Can only happen if `MemberUp(T)` happened before.
    MemberDown(T),

    /// FIXME docs. better name?
    Renamed(T, T),

    /// Foca's current identity is known by at least one active member
    /// of the cluster.
    ///
    /// Fired when successfully joining a cluster for the first time and
    /// every time after a successful identity change.
    Active,

    /// All known active members have either left the cluster or been
    /// declared down.
    Idle,

    /// Foca's current identity has been declared down.
    ///
    /// Manual intervention via `Foca::change_identity` or
    /// `Foca::reuse_down_identity` is required to return to a functioning
    /// state.
    Defunct,

    /// Foca automatically changed its identity and rejoined the cluster
    /// after being declared down.
    ///
    /// This happens instead of `Defunct` when identities opt-in on
    /// `Identity::renew()` functionality.
    Rejoin(T),
}

/// Timer is an event that's scheduled by a [`Runtime`]. You won't need
/// to construct or understand these, just ensure a timely delivery.
///
/// **Warning:** This type implements [`Ord`] to facilitate correcting
/// for out-of-order delivery due to the runtime lagging for whatever
/// reason. It assumes the events being sorted come from the same foca
/// instance and are not being persisted after being handled
/// via [`crate::Foca::handle_timer`]. Any use outside of this scenario
/// will likely lead to unintended consequences.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Timer<T> {
    /// Pick a random active member and initiate the probe cycle.
    ProbeRandomMember(TimerToken),

    /// Send indirect probes if the direct one hasn't completed yet.
    SendIndirectProbe {
        /// The current member being probed
        probed_id: T,
        /// See `TimerToken`
        token: TimerToken,
    },

    /// Transitions member T from Suspect to Down if the incarnation is
    /// still the same.
    ChangeSuspectToDown {
        /// Target member identity
        member_id: T,
        /// Its Incarnation the moment the suspicion was raised. If the
        /// member refutes the suspicion (by increasing its Incarnation),
        /// this won't match and it won't be declared Down.
        incarnation: Incarnation,
        /// See `TimerToken`
        token: TimerToken,
    },

    /// Sends a [`crate::Message::Announce`] to randomly chosen members as
    /// specified by [`crate::Config::periodic_announce`]
    PeriodicAnnounce(TimerToken),

    /// Sends a [`crate::Message::Gossip`] to randomly chosen members as
    /// specified by [`crate::Config::periodic_gossip`]
    PeriodicGossip(TimerToken),

    /// Forgets about dead member `T`, allowing them to join the
    /// cluster again with the same identity.
    RemoveDown(T),
}

impl<T> Timer<T> {
    fn seq(&self) -> u8 {
        match self {
            Timer::SendIndirectProbe {
                probed_id: _,
                token: _,
            } => 0,
            Timer::ProbeRandomMember(_) => 1,
            Timer::ChangeSuspectToDown {
                member_id: _,
                incarnation: _,
                token: _,
            } => 2,
            Timer::PeriodicAnnounce(_) => 3,
            Timer::PeriodicGossip(_) => 4,
            Timer::RemoveDown(_) => 5,
        }
    }
}

impl<T: PartialEq> core::cmp::PartialOrd for Timer<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.seq().partial_cmp(&other.seq())
    }
}

impl<T: Eq> core::cmp::Ord for Timer<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("total ordering")
    }
}

/// `TimerToken` is simply a bookkeeping mechanism to try and prevent
/// reacting to events dispatched that aren't relevant anymore.
///
/// Certain interactions may cause Foca to decide to disregard every
/// event it scheduled previously- so it changes the token in order
/// to drop everything that doesn't match.
///
/// Similar in spirit to [`crate::ProbeNumber`].
pub type TimerToken = u8;

#[cfg(test)]
mod tests {
    use super::Timer;

    #[test]
    fn timers_sort() {
        // What we really care about is SendIndirectProbe
        // appearing before ProbeRandomMember,
        // Foca tolerates other events in arbitrary order
        // without emitting scary errors / traces.
        let mut out_of_order = alloc::vec![
            Timer::<u8>::RemoveDown(0),
            Timer::<u8>::ChangeSuspectToDown {
                member_id: 0,
                incarnation: 0,
                token: 0,
            },
            Timer::<u8>::ProbeRandomMember(0),
            Timer::<u8>::SendIndirectProbe {
                probed_id: 0,
                token: 0
            },
        ];

        out_of_order.sort_unstable();

        assert_eq!(
            alloc::vec![
                Timer::<u8>::SendIndirectProbe {
                    probed_id: 0,
                    token: 0
                },
                Timer::<u8>::ProbeRandomMember(0),
                Timer::<u8>::ChangeSuspectToDown {
                    member_id: 0,
                    incarnation: 0,
                    token: 0,
                },
                Timer::<u8>::RemoveDown(0),
            ],
            out_of_order
        );
    }
}
