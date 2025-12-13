/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use alloc::collections::VecDeque;
use core::{cmp::Ordering, time::Duration};

use bytes::{Bytes, BytesMut};

#[cfg(feature = "unstable-notifications")]
use crate::{Header, ProbeNumber};
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
    fn notify(&mut self, notification: Notification<'_, T>);

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
    fn notify(&mut self, notification: Notification<'_, T>) {
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
#[derive(Debug, PartialEq, Eq)]
pub enum Notification<'a, T> {
    /// Foca discovered a new active member with identity T.
    MemberUp(&'a T),
    /// A previously active member has been declared down by the cluster.
    ///
    /// If Foca detects a down member but didn't know about its activity
    /// before, this notification will not be emitted.
    ///
    /// Can only happen if `MemberUp(T)` happened before.
    MemberDown(&'a T),

    /// Foca has learned that there's a more recent identity with
    /// the same address and chose to use it instead of the previous
    /// one.
    ///
    /// So `Notification::Rename(A,B)` means that we knew about a member
    /// `A` but now there's a `B` with the same `Identity::Addr` and
    /// foca chose to keep it. i.e. `B.win_addr_conflict(A) == true`.
    ///
    /// This happens naturally when a member rejoins the cluster after
    /// any event (maybe they were declared down and `Identity::renew`d
    /// themselves, maybe it's a restart/upgrade process)
    ///
    /// Example:
    ///
    /// If `A` was considered Down and `B` is Alive, you'll get
    /// two notifications, in order:
    //
    ///  1. `Notification::Rename(A,B)`
    ///  2. `Notification::MemberUp(B)`
    ///
    /// However, if there's no liveness change (both are active
    /// or both are down), you'll only get the `Rename` notification
    Rename(&'a T, &'a T),

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
    Rejoin(&'a T),

    #[cfg(feature = "unstable-notifications")]
    /// Foca successfully decoded data received via [`crate::Foca::handle_data`]
    ///
    /// See [`Header`]
    DataReceived(&'a Header<T>),

    #[cfg(feature = "unstable-notifications")]
    /// Foca sent data to a peer
    ///
    /// See [`Header`]
    DataSent(&'a Header<T>),

    #[cfg(feature = "unstable-notifications")]
    /// Foca has probed a member and didn't receive a timely reply
    /// from it nor from any other member asked to indirectly ping it
    ///
    /// See [`crate::Message`]
    ProbeFailed(ProbeNumber, &'a T),
}

impl<T> Notification<'_, T>
where
    T: Clone,
{
    /// Converts self into a [`OwnedNotification`]
    pub fn to_owned(self) -> OwnedNotification<T> {
        match self {
            Notification::MemberUp(m) => OwnedNotification::MemberUp(m.clone()),
            Notification::MemberDown(m) => OwnedNotification::MemberDown(m.clone()),
            Notification::Rename(before, after) => {
                OwnedNotification::Rename(before.clone(), after.clone())
            }
            Notification::Active => OwnedNotification::Active,
            Notification::Idle => OwnedNotification::Idle,
            Notification::Defunct => OwnedNotification::Defunct,
            Notification::Rejoin(id) => OwnedNotification::Rejoin(id.clone()),
            #[cfg(feature = "unstable-notifications")]
            Notification::DataReceived(h) => OwnedNotification::DataReceived(h.clone()),
            #[cfg(feature = "unstable-notifications")]
            Notification::DataSent(h) => OwnedNotification::DataSent(h.clone()),
            #[cfg(feature = "unstable-notifications")]
            Notification::ProbeFailed(n, m) => OwnedNotification::ProbeFailed(n, m.clone()),
        }
    }
}

/// An owned `Notification`, for convenience.
///
/// See [`Notification`] for details
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedNotification<T> {
    /// See [`Notification::MemberUp`]
    MemberUp(T),
    /// See [`Notification::MemberDown`]
    MemberDown(T),
    /// See [`Notification::Rename`]
    Rename(T, T),
    /// See [`Notification::Active`]
    Active,
    /// See [`Notification::Idle`]
    Idle,
    /// See [`Notification::Defunct`]
    Defunct,
    /// See [`Notification::Rejoin`]
    Rejoin(T),
    #[cfg(feature = "unstable-notifications")]
    /// See [`Notification::DataReceived`]
    DataReceived(Header<T>),

    #[cfg(feature = "unstable-notifications")]
    /// See [`Notification::DataSent`]
    DataSent(Header<T>),

    #[cfg(feature = "unstable-notifications")]
    /// See [`Notification::ProbeFailed`]
    ProbeFailed(ProbeNumber, T),
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

    /// Sends a [`crate::Message::Announce`] to randomly chosen members
    /// that are condidered [`crate::State::Down`] as specified by
    /// [`crate::Config::periodic_announce_to_down_members`]
    PeriodicAnnounceDown(TimerToken),

    /// Sends a [`crate::Message::Gossip`] to randomly chosen members as
    /// specified by [`crate::Config::periodic_gossip`]
    PeriodicGossip(TimerToken),

    /// Forgets about dead member `T`, allowing them to join the
    /// cluster again with the same identity.
    RemoveDown(T),
}

impl<T> Timer<T> {
    const fn seq(&self) -> u8 {
        match self {
            Self::SendIndirectProbe {
                probed_id: _,
                token: _,
            } => 0,
            Self::ProbeRandomMember(_) => 1,
            Self::ChangeSuspectToDown {
                member_id: _,
                incarnation: _,
                token: _,
            } => 2,
            Self::PeriodicAnnounce(_) => 3,
            Self::PeriodicGossip(_) => 4,
            Self::RemoveDown(_) => 5,
            Self::PeriodicAnnounceDown(_) => 6,
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

/// A `Runtime` implementation that's good enough for simple use-cases.
///
/// It accumulates all events that happen during an interaction with
/// [`crate::Foca`] and users must drain those and react accordingly.
///
/// Better runtimes would react directly to the events, intead of
/// needlessly storing the events in a queue.
///
/// Users must drain the runtime immediately after interacting with
/// foca. Example:
///
/// See it in use at `examples/foca_insecure_udp_agent.rs`
pub struct AccumulatingRuntime<T> {
    to_send: VecDeque<(T, Bytes)>,
    to_schedule: VecDeque<(Duration, Timer<T>)>,
    notifications: VecDeque<OwnedNotification<T>>,
    buf: BytesMut,
}

impl<T> Default for AccumulatingRuntime<T> {
    fn default() -> Self {
        Self {
            to_send: Default::default(),
            to_schedule: Default::default(),
            notifications: Default::default(),
            buf: Default::default(),
        }
    }
}

impl<T: Identity> Runtime<T> for AccumulatingRuntime<T> {
    fn notify(&mut self, notification: Notification<'_, T>) {
        self.notifications.push_back(notification.to_owned());
    }

    fn send_to(&mut self, to: T, data: &[u8]) {
        self.buf.extend_from_slice(data);
        let packet = self.buf.split().freeze();
        self.to_send.push_back((to, packet));
    }

    fn submit_after(&mut self, event: Timer<T>, after: Duration) {
        self.to_schedule.push_back((after, event));
    }
}

impl<T> AccumulatingRuntime<T> {
    /// Create a new [`AccumulatingRuntime`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Yields data to be sent to a cluster member `T` in the
    /// order they've happened.
    ///
    /// Users are expected to drain it until it yields `None`
    /// after every interaction with [`crate::Foca`]
    pub fn to_send(&mut self) -> Option<(T, Bytes)> {
        self.to_send.pop_front()
    }

    /// Yields timer events and how far in the future they
    /// must be given back to the foca instance that produced it
    ///
    /// Users are expected to drain it until it yields `None`
    /// after every interaction with [`crate::Foca`]
    pub fn to_schedule(&mut self) -> Option<(Duration, Timer<T>)> {
        self.to_schedule.pop_front()
    }

    /// Yields event notifications in the order they've happened
    ///
    /// Users are expected to drain it until it yields `None`
    /// after every interaction with [`crate::Foca`]
    pub fn to_notify(&mut self) -> Option<OwnedNotification<T>> {
        self.notifications.pop_front()
    }

    /// Returns how many unhandled events are left in this runtime
    ///
    /// Should be brought down to zero after every interaction with
    /// [`crate::Foca`]
    pub fn backlog(&self) -> usize {
        self.to_send.len() + self.to_schedule.len() + self.notifications.len()
    }
}

#[cfg(test)]
impl<T: PartialEq> AccumulatingRuntime<T> {
    pub(crate) fn clear(&mut self) {
        self.notifications.clear();
        self.to_send.clear();
        self.to_schedule.clear();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.notifications.is_empty() && self.to_send.is_empty() && self.to_schedule.is_empty()
    }

    pub(crate) fn take_all_data(&mut self) -> VecDeque<(T, Bytes)> {
        core::mem::take(&mut self.to_send)
    }

    pub(crate) fn take_data(&mut self, dst: T) -> Option<Bytes> {
        let position = self.to_send.iter().position(|(to, _data)| to == &dst)?;

        self.to_send.remove(position).map(|(_, data)| data)
    }

    pub(crate) fn take_notification(
        &mut self,
        wanted: OwnedNotification<T>,
    ) -> Option<OwnedNotification<T>> {
        let position = self
            .notifications
            .iter()
            .position(|notification| notification == &wanted)?;

        self.notifications.remove(position)
    }

    pub(crate) fn take_scheduling(&mut self, timer: Timer<T>) -> Option<Duration> {
        let position = self
            .to_schedule
            .iter()
            .position(|(_when, event)| event == &timer)?;

        self.to_schedule.remove(position).map(|(when, _)| when)
    }

    pub(crate) fn find_scheduling<F>(&self, predicate: F) -> Option<&Timer<T>>
    where
        F: Fn(&Timer<T>) -> bool,
    {
        self.to_schedule
            .iter()
            .find(|(_, timer)| predicate(timer))
            .map(|(_, timer)| timer)
    }
}

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
