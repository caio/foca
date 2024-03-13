/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
extern crate alloc;
use alloc::vec::Vec;

use rand::{
    prelude::{IteratorRandom, SliceRandom},
    Rng,
};

/// State describes how a Foca instance perceives a member of the cluster.
///
/// This is part of the Suspicion Mechanism described in section 4.2 of the
/// original SWIM paper.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum State {
    /// Member is active.
    Alive,
    /// Member is active, but at least one cluster member
    /// suspects its down. For all purposes, a `Suspect` member
    /// is treated as if it were `Alive` until either it
    /// refutes the suspicion (becoming `Alive`) or fails to
    /// do so (being declared `Down`).
    Suspect,
    /// Confirmed Down.
    /// A member that reaches this state can't join the cluster
    /// with the same identity until the cluster forgets
    /// this knowledge.
    Down,
}

/// Incarnation is a member-controlled cluster-global number attached
/// to a member identity.
/// A member M's incarnation starts with zero and can only be incremented
/// by said member M when refuting suspicion.
pub type Incarnation = u16;

/// A Cluster Member. Also often called "cluster update".
///
/// A [`Member`] represents Foca's snapshot knowledge about an
/// [`crate::Identity`]. An individual cluster update is simply a
/// serialized Member which other Foca instances receive and use to
/// update their own cluster state representation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Member<T> {
    id: T,
    incarnation: Incarnation,
    state: State,
}

impl<T> Member<T> {
    /// Initializes a new member.
    ///
    /// `id` is an identity used to uniquely identify an individual
    /// cluster member (say, a primary key).
    pub fn new(id: T, incarnation: Incarnation, state: State) -> Self {
        Self {
            id,
            incarnation,
            state,
        }
    }

    /// Shortcut for initializing a member as [`State::Alive`].
    pub fn alive(id: T) -> Self {
        Self::new(id, Incarnation::default(), State::Alive)
    }

    #[cfg(test)]
    pub(crate) fn suspect(id: T) -> Self {
        Self::new(id, Incarnation::default(), State::Suspect)
    }

    pub(crate) fn down(id: T) -> Self {
        Self::new(id, Incarnation::default(), State::Down)
    }

    /// Getter for the member's Incarnation
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Getter for the member's State
    pub fn state(&self) -> State {
        self.state
    }

    /// Getter for the member's identity
    pub fn id(&self) -> &T {
        &self.id
    }

    pub(crate) fn is_active(&self) -> bool {
        match self.state {
            State::Alive | State::Suspect => true,
            State::Down => false,
        }
    }

    pub(crate) fn change_state(&mut self, incarnation: Incarnation, state: State) -> bool {
        if self.can_change(incarnation, state) {
            self.state = state;
            self.incarnation = incarnation;
            true
        } else {
            false
        }
    }

    fn can_change(&self, other_incarnation: Incarnation, other: State) -> bool {
        // This implements the order of preference of the Suspicion subprotocol
        // outlined on section 4.2 of the paper.
        match self.state {
            State::Alive => match other {
                State::Alive => other_incarnation > self.incarnation,
                State::Suspect => other_incarnation >= self.incarnation,
                State::Down => true,
            },
            State::Suspect => match other {
                State::Alive | State::Suspect => other_incarnation > self.incarnation,
                State::Down => true,
            },
            State::Down => false,
        }
    }

    pub(crate) fn into_identity(self) -> T {
        self.id
    }
}

pub(crate) struct Members<T> {
    pub(crate) inner: Vec<Member<T>>,
    cursor: usize,
    num_active: usize,
}

#[cfg(test)]
impl<T> Members<T> {
    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T> Members<T>
where
    T: PartialEq + Clone + crate::Identity,
{
    pub(crate) fn num_active(&self) -> usize {
        self.num_active
    }

    pub(crate) fn new(inner: Vec<Member<T>>) -> Self {
        // XXX This doesn't prevent someone initializing with
        //     duplicated members... Not a problem (yet?) since
        //     inner is always empty outside of tests
        let num_active = inner.iter().filter(|member| member.is_active()).count();

        Self {
            cursor: 0,
            num_active,
            inner,
        }
    }

    // Next member that's considered active
    // Chosen at random (shuffle + round-robin)
    pub(crate) fn next(&mut self, mut rng: impl Rng) -> Option<&Member<T>> {
        // Round-robin with a shuffle at the end
        if self.cursor >= self.inner.len() {
            self.inner.shuffle(&mut rng);
            self.cursor = 0;
        }

        // Find an active member from cursor..len()
        let position = self
            .inner
            .iter()
            .skip(self.cursor)
            .position(|m| m.is_active())
            // Since we skip(), position() will start counting from zero
            // this ensures it's actually the index of the chosen member
            .map(|pos| pos + self.cursor);

        // And if we don't find any: try from 0..cursor
        let position = position.or_else(|| {
            self.inner
                .iter()
                .take(self.cursor)
                .position(|m| m.is_active())
        });

        if let Some(pos) = position {
            if pos < self.cursor {
                // We wrapped around the list to find a member. A shuffle
                // is needed, so we set it to MAX. Any other value could
                // cause the shuffle to not happen since members may join
                // in-between probes
                self.cursor = core::usize::MAX;
            } else {
                self.cursor = pos.saturating_add(1);
            }
            self.inner.get(pos)
        } else {
            None
        }
    }

    /// XXX This used to be a `next_members()` which would make use of the
    ///     already shuffled state and then simply advance the cursor
    ///     to trigger the next shuffle-after-round-robin that `next()`
    ///     does. However I'm not sure it was a good idea: the point
    ///     of what `next()` does is giving some sort of determinism giving
    ///     a high chance that every member will be *pinged* periodically
    ///     and using the same logic for other "pick random member"
    ///     mechanisms might break the math.
    pub(crate) fn choose_active_members<F>(
        &self,
        wanted: usize,
        output: &mut Vec<Member<T>>,
        mut rng: impl Rng,
        picker: F,
    ) where
        F: Fn(&T) -> bool,
    {
        // Basic reservoir sampling
        let mut num_chosen = 0;
        let mut num_seen = 0;

        for member in self.iter_active() {
            if !picker(member.id()) {
                continue;
            }

            num_seen += 1;
            if num_chosen < wanted {
                num_chosen += 1;
                output.push(member.clone());
            } else {
                let replace_at = rng.gen_range(0..num_seen);
                if replace_at < wanted {
                    output[replace_at] = member.clone();
                }
            }
        }
    }

    pub(crate) fn remove_if_down(&mut self, id: &T) -> Option<Member<T>> {
        let position = self
            .inner
            .iter()
            .position(|member| &member.id == id && member.state == State::Down);

        position.map(|pos| self.inner.swap_remove(pos))
    }

    pub(crate) fn iter_active(&self) -> impl Iterator<Item = &Member<T>> {
        self.inner.iter().filter(|m| m.is_active())
    }

    pub(crate) fn apply_existing_if<F: Fn(&Member<T>) -> bool>(
        &mut self,
        update: Member<T>,
        condition: F,
    ) -> Option<ApplySummary> {
        if let Some(known_member) = self
            .inner
            .iter_mut()
            .find(|member| member.id.addr() == update.id().addr())
        {
            if !condition(known_member) {
                return Some(ApplySummary {
                    is_active_now: known_member.is_active(),
                    apply_successful: false,
                    changed_active_set: false,
                });
            }
            let was_active = known_member.is_active();
            let apply_successful = known_member.change_state(update.incarnation(), update.state());
            let is_active_now = known_member.is_active();
            let changed_active_set = is_active_now != was_active;

            if changed_active_set {
                // XXX Overzealous checking
                if is_active_now {
                    self.num_active = self.num_active.saturating_add(1);
                } else {
                    self.num_active = self.num_active.saturating_sub(1);
                }
            }

            Some(ApplySummary {
                is_active_now,
                apply_successful,
                changed_active_set,
            })
        } else {
            None
        }
    }

    pub(crate) fn apply(&mut self, update: Member<T>, mut rng: impl Rng) -> ApplySummary {
        self.apply_existing_if(update.clone(), |_member| true)
            .unwrap_or_else(|| {
                // Unknown member, we'll register it
                let is_active_now = update.is_active();

                // Insert at the end and swap with a random position.
                self.inner.push(update);
                let inserted_at = self.inner.len() - 1;

                let swap_idx = (0..self.inner.len())
                    .choose(&mut rng)
                    .unwrap_or(inserted_at);

                self.inner.swap(swap_idx, inserted_at);

                if is_active_now {
                    self.num_active = self.num_active.saturating_add(1);
                }

                ApplySummary {
                    is_active_now,
                    apply_successful: true,
                    // Registering a new active member changes the active set
                    changed_active_set: is_active_now,
                }
            })
    }
}

#[derive(Debug, Clone, PartialEq)]
#[must_use]
pub(crate) struct ApplySummary {
    pub(crate) is_active_now: bool,
    pub(crate) apply_successful: bool,
    pub(crate) changed_active_set: bool,
}

#[cfg(test)]
mod tests {

    use super::*;

    use alloc::vec;
    use rand::{rngs::SmallRng, SeedableRng};

    #[derive(Clone, Debug, PartialEq, Eq, Copy, PartialOrd, Ord)]
    struct Id(&'static str);
    impl crate::Identity for Id {
        type Addr = &'static str;

        fn renew(&self) -> Option<Self> {
            None
        }

        fn addr(&self) -> Self::Addr {
            self.0
        }
    }

    use State::*;

    #[test]
    fn alive_transitions() {
        let mut member = Member::new(Id("a"), 0, Alive);

        // Alive => Alive
        assert!(
            member.change_state(member.incarnation + 1, Alive),
            "can transition to a higher incarnation"
        );

        assert_eq!(1, member.incarnation);
        assert_eq!(Alive, member.state);

        assert!(
            !member.change_state(member.incarnation - 1, Alive),
            "cannot transition to a lower incarnation"
        );

        assert!(
            !member.change_state(member.incarnation, Alive),
            "cannot transition to same state and incarnation {:?}",
            &member
        );

        // Alive => Suspect
        assert!(
            !member.change_state(member.incarnation - 1, Suspect),
            "lower suspect incarnation shouldn't transition"
        );

        assert!(
            member.change_state(member.incarnation, Suspect),
            "transition to suspect with same incarnation"
        );
        assert_eq!(Suspect, member.state);

        member = Member::new(Id("b"), 0, Alive);
        assert!(
            member.change_state(member.incarnation + 1, Suspect),
            "transition to suspect with higher incarnation"
        );
        assert_eq!(1, member.incarnation);
        assert_eq!(Suspect, member.state);

        // Alive => Down, always works
        assert!(
            Member::new("c", 1, Alive).change_state(0, Down),
            "transitions to down on lower incarnation"
        );
        assert!(
            Member::new("c", 0, Alive).change_state(0, Down),
            "transitions to down on same incarnation"
        );
        assert!(
            Member::new("c", 0, Alive).change_state(1, Down),
            "transitions to down on higher incarnation"
        );
    }

    #[test]
    fn suspect_transitions() {
        let mut member = Member::new(Id("a"), 0, Suspect);

        // Suspect => Suspect
        assert!(
            member.change_state(member.incarnation + 1, Suspect),
            "can transition to a higher incarnation"
        );

        assert_eq!(1, member.incarnation);
        assert_eq!(Suspect, member.state);

        assert!(
            !member.change_state(member.incarnation - 1, Suspect),
            "cannot transition to a lower incarnation"
        );

        assert!(
            !member.change_state(member.incarnation, Suspect),
            "cannot transition to same state and incarnation {:?}",
            &member
        );

        // Suspect => Alive
        assert!(
            !member.change_state(member.incarnation - 1, Alive),
            "lower alive incarnation shouldn't transition"
        );
        assert!(
            !member.change_state(member.incarnation, Alive),
            "same alive incarnation shouldn't transition"
        );

        assert!(
            member.change_state(member.incarnation + 1, Alive),
            "can transition to alive with higher incarnation"
        );
        assert_eq!(Alive, member.state);

        // Suspect => Down, always works
        assert!(
            Member::new("c", 1, Suspect).change_state(0, Down),
            "transitions to down on lower incarnation"
        );
        assert!(
            Member::new("c", 0, Suspect).change_state(0, Down),
            "transitions to down on same incarnation"
        );
        assert!(
            Member::new("c", 0, Suspect).change_state(1, Down),
            "transitions to down on higher incarnation"
        );
    }

    #[test]
    fn down_never_transitions() {
        let mut member = Member::new("dead", 1, Down);

        for incarnation in 0..=2 {
            assert!(!member.change_state(incarnation, Alive));
            assert!(!member.change_state(incarnation, Suspect));
            assert!(!member.change_state(incarnation, Down));
        }
    }

    #[test]
    fn next_walks_sequentially_then_shuffles() {
        let ordered_ids = vec![Id("1"), Id("2"), Id("3"), Id("4"), Id("5")];
        let mut members = Members::new(ordered_ids.iter().cloned().map(Member::alive).collect());

        let mut rng = SmallRng::seed_from_u64(0xF0CA);

        for wanted in ordered_ids.iter() {
            let got = members
                .next(&mut rng)
                .expect("Non-empty set of Alive members should always yield Some()")
                .id;
            assert_eq!(wanted, &got);
        }

        // By now we walked through all known live members so
        // the internal state should've shuffled.
        // We'll verify that by calling `next()` multiple
        // times and comparing with the original `ordered_ids`
        let mut after_shuffle = (0..ordered_ids.len())
            .map(|_| members.next(&mut rng).unwrap().id)
            .collect::<Vec<_>>();
        assert_ne!(ordered_ids, after_shuffle);

        // The shuffle only happens once the cursor walks
        // through the whole set, so `after_shuffle` should
        // contain every member, like `ordered_ids`, but in
        // a distinct order
        after_shuffle.sort_unstable();
        assert_eq!(ordered_ids, after_shuffle);
    }

    #[test]
    fn apply_existing_if_behaviour() {
        let mut members = Members::new(Vec::new());

        assert_eq!(
            None,
            members.apply_existing_if(Member::alive(Id("1")), |_member| true),
            "Only yield None only if member is not found"
        );

        let mut rng = SmallRng::seed_from_u64(0xF0CA);
        let _ = members.apply(Member::alive(Id("1")), &mut rng);

        assert_ne!(
            None,
            members.apply_existing_if(Member::alive(Id("1")), |_member| true),
            "Must yield Some() if existing, regardless of condition"
        );

        assert_ne!(
            None,
            members.apply_existing_if(Member::alive(Id("1")), |_member| false),
            "Must yield Some() if existing, regardless of condition"
        );
    }

    #[test]
    fn apply_summary_behaviour() {
        let mut members = Members::new(Vec::new());
        let mut rng = SmallRng::seed_from_u64(0xF0CA);

        // New and active member
        let res = members.apply(Member::suspect(Id("1")), &mut rng);
        assert_eq!(
            ApplySummary {
                is_active_now: true,
                apply_successful: true,
                changed_active_set: true
            },
            res,
        );
        assert_eq!(1, members.len());
        assert_eq!(1, members.num_active());

        // Failed attempt to change member id=1 to alive
        // (since it's already suspect with same incarnation)
        let res = members.apply(Member::alive(Id("1")), &mut rng);
        assert_eq!(
            ApplySummary {
                is_active_now: true,
                apply_successful: false,
                changed_active_set: false
            },
            res,
        );
        assert_eq!(1, members.len());

        // Successful attempt at changing member id=1 to
        // alive by using a higher incarnation
        let res = members.apply(Member::new(Id("1"), 1, State::Alive), &mut rng);
        assert_eq!(
            ApplySummary {
                is_active_now: true,
                apply_successful: true,
                changed_active_set: false
            },
            res,
        );
        assert_eq!(1, members.len());

        // Change existing member to down
        let res = members.apply(Member::down(Id("1")), &mut rng);
        assert_eq!(
            ApplySummary {
                is_active_now: false,
                apply_successful: true,
                changed_active_set: true
            },
            res,
        );
        assert_eq!(1, members.len());
        assert_eq!(0, members.num_active());

        // New and inactive member
        let res = members.apply(Member::down(Id("2")), &mut rng);
        assert_eq!(
            ApplySummary {
                is_active_now: false,
                apply_successful: true,
                changed_active_set: false
            },
            res,
        );
        assert_eq!(2, members.len());
        assert_eq!(0, members.num_active());
    }

    #[test]
    fn remove_if_down_works() {
        let mut members = Members::new(Vec::new());
        let mut rng = SmallRng::seed_from_u64(0xF0CA);

        assert_eq!(
            None,
            members.remove_if_down(&Id("1")),
            "cant remove member that does not exist"
        );
        let _ = members.apply(Member::alive(Id("1")), &mut rng);

        assert_eq!(
            None,
            members.remove_if_down(&Id("1")),
            "cant remove member that isnt down"
        );
        let _ = members.apply(Member::down(Id("1")), &mut rng);

        assert_eq!(
            Some(Member::down(Id("1"))),
            members.remove_if_down(&Id("1")),
            "must return the removed member"
        );
    }

    #[test]
    fn next_yields_none_with_no_active_members() {
        let mut members = Members::new(Vec::new());
        let mut rng = SmallRng::seed_from_u64(0xF0CA);

        assert_eq!(
            None,
            members.next(&mut rng),
            "next() should yield None when there are no members"
        );

        let _ = members.apply(Member::down(Id("-1")), &mut rng);
        let _ = members.apply(Member::down(Id("-2")), &mut rng);
        let _ = members.apply(Member::down(Id("-3")), &mut rng);

        assert_eq!(
            None,
            members.next(&mut rng),
            "next() should yield None when there are no active members"
        );

        let _ = members.apply(Member::alive(Id("1")), &mut rng);

        for _i in 0..10 {
            assert_eq!(
                Some(Id("1")),
                members.next(&mut rng).map(|m| m.id),
                "next() should yield the same member if its the only active"
            );
        }
    }

    #[test]
    fn choose_active_members_behaviour() {
        let members = Members::new(Vec::from([
            // 5 active members
            Member::alive(Id("1")),
            Member::alive(Id("2")),
            Member::alive(Id("3")),
            Member::suspect(Id("4")),
            Member::suspect(Id("5")),
            // 2 down
            Member::down(Id("6")),
            Member::down(Id("7")),
        ]));

        assert_eq!(7, members.len());
        assert_eq!(5, members.num_active());

        let mut out = Vec::new();
        let mut rng = SmallRng::seed_from_u64(0xF0CA);

        out.clear();
        members.choose_active_members(0, &mut out, &mut rng, |_| true);
        assert_eq!(0, out.len(), "Can pointlessly choose 0 members");

        out.clear();
        members.choose_active_members(10, &mut out, &mut rng, |_| false);
        assert_eq!(0, out.len(), "Filtering works");

        out.clear();
        members.choose_active_members(members.len(), &mut out, &mut rng, |_| true);
        assert_eq!(
            members.num_active(),
            out.len(),
            "Only chooses active members"
        );

        out.clear();
        members.choose_active_members(2, &mut out, &mut rng, |_| true);
        assert_eq!(2, out.len(), "Respects `wanted` even if we have more");

        out.clear();
        members.choose_active_members(usize::MAX, &mut out, &mut rng, |&member_id| {
            member_id.0.parse::<usize>().expect("number") > 4
        });
        assert_eq!(vec![Member::suspect(Id("5"))], out);
    }
}
