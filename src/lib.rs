/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
//! Foca is a building block for your gossip-based cluster discovery. It's
//! a small library-first crate that implements the SWIM protocol along
//! with its useful extensions (`SWIM+Inf.+Susp.`).
//!
//! * It's a `no_std` + `alloc` crate by default. There's an optional
//!   `std` feature that simply brings compatibility with some types
//!   and the `std::error::Error` trait
//!
//! * Bring Your Own Everything: Foca doesn't care about anything that
//!   isn't part of the cluster membership functionality:
//!
//!   * Pluggable, renewable identities: Using a fixed port number?
//!     No need to send it all the time. Want to attach extra crucial
//!     information (shard id, deployment version, etc)? Easy.
//!     Always have a lookup table mapping `u16` to hostnames? Use
//!     that instead of a socket address!  Bring your own type,
//!     implement [`Identity`] and enjoy.
//!
//!   * Write your own wire format by implementing [`Codec`]; Like
//!     serde? There is `bincode-codec` and `postcard-codec` features,
//!     or just use the `serde` feature and pick your favorite format.
//!
//!   * Use any transport you want, it's up to you how messages
//!     reach each member: Foca will tell you "Send these bytes to
//!     member M", how that happens is not its business.
//!
//! * Custom Broadcasts: Foca can attach arbitrary data to its messages
//!   and disseminate them the same way it distributes cluster updates.
//!   Send CRDT operations, take a stab at implementing metadata-heavy
//!   service discovery system, anything really. Give it something
//!   that implements [`BroadcastHandler`] and Foca will ship it.
//!
//! * No runtime crashes: Apart from `alloc`-related aborts, Foca should
//!   only crash inside something you provided: a [`Codec`], [`Runtime`]
//!   or a [`BroadcastHandler`]- so long as those are solid, Foca is too.
//!
//! * Doesn't force you to choose between `sync` and `async`. It's as
//!   easy to plug it in an evented runtime as it is to go old-school.
//!
#![forbid(unsafe_code)]
#![no_std]
#![deny(missing_docs, unreachable_pub)]
#![deny(rustdoc::broken_intra_doc_links)]
#![warn(
    clippy::all,
    clippy::await_holding_lock,
    clippy::char_lit_as_u8,
    clippy::checked_conversions,
    clippy::dbg_macro,
    clippy::debug_assert_with_mut_call,
    clippy::doc_markdown,
    clippy::empty_enum,
    clippy::enum_glob_use,
    clippy::exit,
    clippy::expl_impl_clone_on_copy,
    clippy::explicit_deref_methods,
    clippy::explicit_into_iter_loop,
    clippy::fallible_impl_from,
    clippy::filter_map_next,
    clippy::flat_map_option,
    clippy::float_cmp_const,
    clippy::fn_params_excessive_bools,
    clippy::from_iter_instead_of_collect,
    clippy::if_let_mutex,
    clippy::implicit_clone,
    clippy::imprecise_flops,
    clippy::inefficient_to_string,
    clippy::invalid_upcast_comparisons,
    clippy::large_digit_groups,
    clippy::large_stack_arrays,
    clippy::large_types_passed_by_value,
    clippy::let_unit_value,
    clippy::linkedlist,
    clippy::lossy_float_literal,
    clippy::macro_use_imports,
    clippy::manual_ok_or,
    clippy::map_err_ignore,
    clippy::map_flatten,
    clippy::map_unwrap_or,
    clippy::match_on_vec_items,
    clippy::match_same_arms,
    clippy::match_wild_err_arm,
    clippy::match_wildcard_for_single_variants,
    clippy::mem_forget,
    clippy::mismatched_target_os,
    clippy::missing_enforced_import_renames,
    clippy::mut_mut,
    clippy::mutex_integer,
    clippy::needless_borrow,
    clippy::needless_continue,
    clippy::needless_for_each,
    clippy::option_option,
    clippy::path_buf_push_overwrite,
    clippy::ptr_as_ptr,
    clippy::rc_mutex,
    clippy::ref_option_ref,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::same_functions_in_if_condition,
    clippy::semicolon_if_nothing_returned,
    clippy::single_match_else,
    clippy::string_add_assign,
    clippy::string_add,
    clippy::string_lit_as_bytes,
    clippy::string_to_string,
    clippy::trait_duplication_in_bounds,
    clippy::unimplemented,
    clippy::unnested_or_patterns,
    clippy::useless_transmute,
    clippy::verbose_file_reads,
    clippy::zero_sized_map_values,
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms
)]

extern crate alloc;
use alloc::vec::Vec;

#[cfg(feature = "std")]
extern crate std;

use core::{cmp::Ordering, convert::TryFrom, fmt, iter::ExactSizeIterator, mem};

use bytes::{Buf, BufMut};
use rand::Rng;

mod broadcast;
mod codec;
mod config;
mod error;
mod identity;
mod member;
mod payload;
mod probe;
mod runtime;
#[cfg(test)]
mod testing;

use crate::{
    broadcast::Broadcasts,
    member::{ApplySummary, Members},
    probe::Probe,
};

pub use crate::{
    broadcast::{BroadcastHandler, Invalidates},
    codec::Codec,
    config::{Config, PeriodicParams},
    error::Error,
    identity::Identity,
    member::{Incarnation, Member, State},
    payload::{Header, Message, ProbeNumber},
    runtime::{AccumulatingRuntime, Notification, Runtime, Timer, TimerToken},
};

#[cfg(feature = "postcard-codec")]
pub use crate::codec::postcard_impl::PostcardCodec;

#[cfg(feature = "bincode-codec")]
pub use crate::codec::bincode_impl::BincodeCodec;

type Result<T> = core::result::Result<T, Error>;

/// Foca is the main interaction point of this crate.
///
/// It manages the cluster members and executes the SWIM protocol. It's
/// intended as a low-level guts-exposed safe view into the protocol
/// allowing any kind of Identity and transport to be used.
///
/// Most interactions with Foca require the caller to provide a
/// [`Runtime`] type, which is simply a way to turn the result of an
/// operation inside out (think callbacks, or an out parameter like
/// `void* out`). This allows Foca to avoid deciding anything related
/// to how it interacts with the operating system.
pub struct Foca<T: Identity, C, RNG, B: BroadcastHandler<T>> {
    identity: T,
    codec: C,
    rng: RNG,

    incarnation: Incarnation,
    config: Config,
    connection_state: ConnectionState,
    timer_token: TimerToken,

    members: Members<T>,
    probe: Probe<T>,

    // Used to buffer up members/updates when receiving and
    // sending data
    member_buf: Vec<Member<T>>,

    send_buf: Vec<u8>,

    // Holds (serialized) cluster updates, which may live for a
    // while until they get disseminated `Config::max_transmissions`
    // times or replaced by fresher updates.
    updates: Broadcasts<Addr<T::Addr>>,

    broadcast_handler: B,
    custom_broadcasts: Broadcasts<B::Key>,
}

impl<T, C, RNG> Foca<T, C, RNG, NoCustomBroadcast>
where
    T: Identity,
    C: Codec<T>,
    RNG: Rng,
{
    /// Create a new Foca instance with custom broadcasts disabled.
    ///
    /// This is a simple shortcut for [`Foca::with_custom_broadcast`]
    /// using the [`NoCustomBroadcast`] type to deny any form of custom
    /// broadcast.
    pub fn new(identity: T, config: Config, rng: RNG, codec: C) -> Self {
        Self::with_custom_broadcast(identity, config, rng, codec, NoCustomBroadcast)
    }
}

#[cfg(feature = "tracing")]
impl<T, C, RNG, B> fmt::Debug for Foca<T, C, RNG, B>
where
    T: Identity,
    B: BroadcastHandler<T>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Assuming that when tracing comes into play the cluster is actually
        // uniform. Meaning: everything is configured the same, including
        // codec and broadcast handler.
        // So the actually interesting thing is the identity.
        formatter.debug_tuple("Foca").field(&self.identity).finish()
    }
}

// XXX Does it make sense to have different associated type restrictions
//     based on a feature flag? Say: when using `std` we would enforce
//     that `Codec::Error` and `BroadcastHandler::Error` both implement
//     `std::error::Error`, thus instead of wrapping these errors via
//     `anyhow::Error::msg` we can use `anyhow::Error::new`.
impl<T, C, RNG, B> Foca<T, C, RNG, B>
where
    T: Identity,
    C: Codec<T>,
    RNG: Rng,
    B: BroadcastHandler<T>,
{
    /// Initialize a new Foca instance.
    pub fn with_custom_broadcast(
        identity: T,
        config: Config,
        rng: RNG,
        codec: C,
        broadcast_handler: B,
    ) -> Self {
        let max_indirect_probes = config.num_indirect_probes.get();
        let max_bytes = config.max_packet_size.get();
        Self {
            identity,
            config,
            rng,
            codec,
            incarnation: Incarnation::default(),
            timer_token: TimerToken::default(),
            members: Members::new(Vec::new()),
            probe: Probe::new(Vec::with_capacity(max_indirect_probes)),
            member_buf: Vec::new(),
            connection_state: ConnectionState::Disconnected,
            updates: Broadcasts::new(),
            send_buf: Vec::with_capacity(max_bytes),
            custom_broadcasts: Broadcasts::new(),
            broadcast_handler,
        }
    }

    /// Getter for the current identity.
    pub fn identity(&self) -> &T {
        &self.identity
    }

    /// Re-enable joining a cluster with the same identity after being
    /// declared Down.
    ///
    /// This is intended to be use by implementations that decide not to
    /// opt-in on auto-rejoining: once Foca detects its Down you'll
    /// only be able to receive messages (which will likely stop after
    /// a short while since the cluster things you are down).
    ///
    /// Whatever is controlling the running Foca will then have to wait
    /// for at least [`Config::remove_down_after`] before attempting a
    /// rejoin. Then you can call this method followed by a
    /// [`Foca::announce(T)`] to go back to the cluster.
    pub fn reuse_down_identity(&mut self) -> Result<()> {
        if self.connection_state != ConnectionState::Undead {
            Err(Error::NotUndead)
        } else {
            self.reset();
            Ok(())
        }
    }

    /// Change the current identity.
    ///
    /// Foca will declare its previous identity as Down and immediatelly
    /// notify the cluster about the changes.
    ///
    /// Notice that changing your identity does not guarantee a
    /// successful (re)join. After changing it and disseminating the updates
    /// Foca will only know it's actually accepted after receiving a
    /// message addressed to it.
    ///
    /// Watch for [`Notification::Active`] if you want more confidence about
    /// a successful (re)join.
    ///
    /// Intended to be used when identities carry metadata that occasionally
    /// changes.
    pub fn change_identity(&mut self, new_id: T, runtime: impl Runtime<T>) -> Result<()> {
        if self.identity == new_id {
            Err(Error::SameIdentity)
        } else {
            let previous_is_down = self.connection_state == ConnectionState::Undead;
            let previous_id = mem::replace(&mut self.identity, new_id);

            self.reset();

            #[cfg(feature = "tracing")]
            tracing::debug!(
                self = tracing::field::debug(&self),
                previous_id = tracing::field::debug(&previous_id),
                "changed identity"
            );

            // If our previous identity wasn't known as Down already,
            // we'll declare it ourselves
            if !previous_is_down {
                let addr = Addr(previous_id.addr());
                let data = self.serialize_member(Member::down(previous_id))?;
                self.updates
                    .add_or_replace(addr, data, self.config.max_transmissions.get().into());
            }

            self.gossip(runtime)?;

            Ok(())
        }
    }

    /// Iterate over the currently active cluster members.
    pub fn iter_members(&self) -> impl Iterator<Item = &Member<T>> {
        self.members.iter_active()
    }

    /// Returns the number of active members in the cluster.
    ///
    /// May only be used as a bound for [`Foca::iter_members`] if no
    /// Foca method that takes `&mut self` is called in-between.
    pub fn num_members(&self) -> usize {
        self.members.num_active()
    }

    /// Iterates over the *full* membership state, including members
    /// that have been declared down.
    ///
    /// This is for advanced usage, to be used in tandem with
    /// [`Foca::apply_many`]. The main use-case for this is
    /// state replication:
    ///
    /// 1. You may want to send it to another node so that it knows
    ///    all you do; if said member sends you their state as an
    ///    immediate reply, both states will be exactly the same.
    ///    The reply can be a lot smaller than the full state in
    ///    most cases, if payload size if a concern.
    ///
    /// 2. You might want to save the full state to disk before
    ///    restarting a process running Foca so that you can get
    ///    back up quickly with low risk of accepting stale
    ///    knowledge as truthful
    pub fn iter_membership_state(&self) -> impl ExactSizeIterator<Item = &Member<T>> {
        self.members.inner.iter()
    }

    /// Applies cluster updates to this foca instance.
    ///
    /// This is for advanced usage. It's intended as a way to unlock
    /// more elaborate synchronization protocols: implementations may
    /// choose to unify their cluster knowledge (say: a streaming
    /// join protocol or a periodic sync) and use [`Foca::apply_many`]
    /// as a way to feed Foca this new (external) knowledge.
    pub fn apply_many(
        &mut self,
        updates: impl Iterator<Item = Member<T>>,
        mut runtime: impl Runtime<T>,
    ) -> Result<()> {
        for update in updates {
            if update.id() == &self.identity {
                self.handle_self_update(update.incarnation(), update.state(), &mut runtime)?;
            } else if self.identity.addr() == update.id().addr() {
                // We received an update that's about an identity that *could*
                // have been ours but definitely isn't (the branch right above,
                // where we check equality)
                //
                // This can happen naturally: an instance rejoins the cluster
                // while the cluster actively talking about its previous identity
                // going down.
                //
                // Any non-Down state, however, is questionable: maybe there are
                // multiple instances using the same id; Maybe our own instance
                // has been restarted many times at once as the cluster still
                // hasn't figured out the correct state yet.
                //
                // So we assume that this is always delayed/stale information and
                // declare this previous identity as Down.
                //
                // NOTE If there are multiple nodes claiming to have the same
                //      identity, this will lead to a looping scenario where
                //      node A declares B down, then B changes identity and
                //      declares A down; nonstop
                #[cfg(feature = "tracing")]
                if update.is_active() {
                    tracing::trace!(
                        self = tracing::field::debug(&self),
                        update = tracing::field::debug(&update),
                        "update about identity with same prefix as ours, declaring it down"
                    );
                }
                self.apply_update(Member::down(update.into_identity()), &mut runtime)?;
            } else {
                self.apply_update(update, &mut runtime)?;
            }
        }

        self.adjust_connection_state(runtime);

        Ok(())
    }

    fn adjust_connection_state(&mut self, runtime: impl Runtime<T>) {
        match self.connection_state {
            ConnectionState::Disconnected => {
                if self.members.num_active() > 0 {
                    self.become_connected(runtime);
                }
            }
            ConnectionState::Connected => {
                if self.members.num_active() == 0 {
                    self.become_disconnected(runtime);
                }
            }
            ConnectionState::Undead => {
                // We're undead. The only ways to recover are via
                // an id change or reuse_down_identity(). Nothing else
                // to do
            }
        }
    }

    /// Attempt to join the cluster `dst` belongs to.
    ///
    /// Sends a [`Message::Announce`] to `dst`. If accepted, we'll receive
    /// a [`Message::Feed`] as reply.
    pub fn announce(&mut self, dst: T, runtime: impl Runtime<T>) -> Result<()> {
        self.send_message(dst, Message::Announce, runtime)
    }

    /// Disseminate updates/broadcasts to cluster members.
    ///
    /// This instructs Foca to pick [`Config::num_indirect_probes`]
    /// random active members and send a [`Message::Gossip`] containing
    /// cluster updates.
    ///
    /// Intended for more complex scenarios where an implementation wants
    /// to attempt reducing the time it takes for information to
    /// propagate thoroughly.
    pub fn gossip(&mut self, runtime: impl Runtime<T>) -> Result<()> {
        self.choose_and_send(
            self.config.num_indirect_probes.get(),
            Message::Gossip,
            runtime,
        )
    }

    // Pick `num_members` random active members and send `msg` to them
    fn choose_and_send(
        &mut self,
        num_members: usize,
        msg: Message<T>,
        mut runtime: impl Runtime<T>,
    ) -> Result<()> {
        self.member_buf.clear();
        self.members.choose_active_members(
            num_members,
            &mut self.member_buf,
            &mut self.rng,
            |_| true,
        );

        while let Some(chosen) = self.member_buf.pop() {
            self.send_message(chosen.into_identity(), msg.clone(), &mut runtime)?;
        }

        Ok(())
    }

    /// Only disseminate custom broadcasts to cluster members
    ///
    /// This instructs Foca to pick [`Config::num_indirect_probes`]
    /// random active members that *pass* the
    /// [`BroadcastHandler::should_add_broadcast_data`] check. It
    /// guarantees custom broadcast dissemination if there are
    /// candidate members available.
    ///
    /// No cluster update will be sent with these messages. Intended
    /// to be used in tandem with a non-default
    /// `should_add_broadcast_data`.
    pub fn broadcast(&mut self, mut runtime: impl Runtime<T>) -> Result<()> {
        if self.custom_broadcast_backlog() == 0 {
            // Nothing to broadcast
            return Ok(());
        }

        self.member_buf.clear();
        self.members.choose_active_members(
            self.config.num_indirect_probes.get(),
            &mut self.member_buf,
            &mut self.rng,
            |member| self.broadcast_handler.should_add_broadcast_data(member),
        );

        while let Some(chosen) = self.member_buf.pop() {
            self.send_message(chosen.into_identity(), Message::Broadcast, &mut runtime)?;

            // Crafting the message above left the backlog empty,
            // no need to send more messages since they won't
            // contain anything
            if self.custom_broadcast_backlog() == 0 {
                break;
            }
        }

        Ok(())
    }

    /// Leave the cluster by declaring our own identity as down.
    ///
    /// If there are active members, a few are selected and notified
    /// of our exit so that the cluster learns about it quickly.
    ///
    /// This is the cleanest way to terminate a running Foca.
    pub fn leave_cluster(&mut self, mut runtime: impl Runtime<T>) -> Result<()> {
        let addr = Addr(self.identity().addr());
        let data = self.serialize_member(Member::down(self.identity().clone()))?;
        self.updates
            .add_or_replace(addr, data, self.config.max_transmissions.get().into());

        self.gossip(&mut runtime)?;

        // We could try to be smart here and only go defunct if there
        // are active members, but I'd rather have consistent behaviour.
        self.become_undead(&mut runtime);

        Ok(())
    }

    /// Register some data to be broadcast along with Foca messages.
    ///
    /// Calls into this instance's `BroadcastHandler` and reacts accordingly.
    pub fn add_broadcast(&mut self, data: &[u8]) -> Result<bool> {
        if data.is_empty() {
            return Err(Error::MalformedPacket);
        }

        // Not considering the whole header
        if data.len() > self.config.max_packet_size.get() {
            return Err(Error::DataTooBig);
        }

        if let Some(key) = self
            .broadcast_handler
            .receive_item(data, None)
            .map_err(anyhow::Error::msg)
            .map_err(Error::CustomBroadcast)?
        {
            self.custom_broadcasts.add_or_replace(
                key,
                data.to_vec(),
                self.config.max_transmissions.get().into(),
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// React to a previously scheduled timer event.
    ///
    /// See [`Runtime::submit_after`].
    pub fn handle_timer(&mut self, event: Timer<T>, mut runtime: impl Runtime<T>) -> Result<()> {
        #[cfg(feature = "tracing")]
        let _span =
            tracing::trace_span!("handle_timer", event = tracing::field::debug(&event)).entered();
        match event {
            Timer::SendIndirectProbe { probed_id, token } => {
                // Changing identities in the middle of the probe cycle may
                // naturally lead to this.
                if token != self.timer_token {
                    #[cfg(feature = "tracing")]
                    tracing::trace!("Invalid timer token");
                    return Ok(());
                }

                // Bookkeeping: This is how we verify that the probe code
                // is running correctly. If we reach the end of the
                // probe and this hasn't happened, we know something is
                // wrong.
                self.probe.mark_indirect_probe_stage_reached();

                if !self.probe.is_probing(&probed_id) {
                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        probed_id = tracing::field::debug(&probed_id),
                        "Member not being probed"
                    );
                    return Ok(());
                }

                if self.probe.succeeded() {
                    // We received an Ack already, nothing else to do
                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        probed_id = tracing::field::debug(&probed_id),
                        "Probe succeeded, no need for indirect cycle"
                    );
                    return Ok(());
                }

                self.member_buf.clear();
                self.members.choose_active_members(
                    self.config.num_indirect_probes.get(),
                    &mut self.member_buf,
                    &mut self.rng,
                    |candidate| candidate != &probed_id,
                );

                #[cfg(feature = "tracing")]
                tracing::debug!(
                    probed_id = tracing::field::debug(&probed_id),
                    "Member didn't respond to ping in time, starting indirect probe cycle"
                );

                while let Some(chosen) = self.member_buf.pop() {
                    let indirect = chosen.into_identity();

                    self.probe.expect_indirect_ack(indirect.clone());

                    self.send_message(
                        indirect,
                        Message::PingReq {
                            target: probed_id.clone(),
                            probe_number: self.probe.probe_number(),
                        },
                        &mut runtime,
                    )?;
                }

                Ok(())
            }
            Timer::ChangeSuspectToDown {
                member_id,
                incarnation,
                token,
            } => {
                if self.timer_token == token {
                    let as_down = Member::new(member_id.clone(), incarnation, State::Down);
                    if let Some(summary) = self
                        .members
                        // Down is terminal, so before doing that we ensure the member
                        // is still under suspicion.
                        // Checking only incarnation is sufficient because to refute
                        // suspicion the member must increment its own incarnation
                        .apply_existing_if(as_down.clone(), |member| {
                            member.incarnation() == incarnation
                        })
                    {
                        self.handle_apply_summary(summary, as_down, &mut runtime)?;
                        // Member went down we might need to adjust our internal state
                        self.adjust_connection_state(&mut runtime);

                        if self.config.notify_down_members {
                            // As a courtesy, we send a lightweight message to the member
                            // we're declaring down so that if it manages to receive it,
                            // it can react accordingly
                            self.send_message(member_id, Message::TurnUndead, runtime)?;
                        }
                    }
                }

                Ok(())
            }
            Timer::RemoveDown(down) => {
                #[cfg_attr(
                    not(feature = "tracing"),
                    allow(unused_variables, clippy::if_same_then_else)
                )]
                if let Some(_removed) = self.members.remove_if_down(&down) {
                    #[cfg(feature = "tracing")]
                    tracing::trace!(down = tracing::field::debug(&down), "Member removed");
                }

                Ok(())
            }
            Timer::ProbeRandomMember(token) => {
                if token == self.timer_token {
                    if self.connection_state != ConnectionState::Connected {
                        // Not expected to happen during normal operation, but
                        // may reach here via manually crafted Timer::
                        Err(Error::NotConnected)
                    } else {
                        self.probe_random_member(runtime)
                    }
                } else {
                    // Invalid token, may happen whenever we go offline after
                    // being online
                    Ok(())
                }
            }
            Timer::PeriodicAnnounce(token) => {
                if token == self.timer_token && self.connection_state == ConnectionState::Connected
                {
                    // The configuration may change during runtime, so we can't
                    // assume that this is Some() when the timer fires
                    if let Some(ref params) = self.config.periodic_announce {
                        // Re-schedule the event
                        runtime.submit_after(
                            Timer::PeriodicAnnounce(self.timer_token),
                            params.frequency,
                        );
                        // And send the messages
                        self.choose_and_send(params.num_members.get(), Message::Announce, runtime)?;
                    }
                }
                // else: invalid token and/or not-connected: may happen if the
                // instance gets declared down by the cluster
                Ok(())
            }
            Timer::PeriodicGossip(token) => {
                // Exact same thing as PeriodicAnnounce, just using different settings / messages
                if token == self.timer_token && self.connection_state == ConnectionState::Connected
                {
                    if let Some(ref params) = self.config.periodic_gossip {
                        runtime.submit_after(
                            Timer::PeriodicGossip(self.timer_token),
                            params.frequency,
                        );

                        // Only actually gossip if there are updates to send
                        if !self.updates.is_empty() || !self.custom_broadcasts.is_empty() {
                            self.choose_and_send(
                                params.num_members.get(),
                                Message::Gossip,
                                runtime,
                            )?;
                        }
                    }
                }
                Ok(())
            }
        }
    }

    /// Reports the current length of the cluster updates queue.
    ///
    /// Updates are transmitted [`Config::max_transmissions`] times
    /// at most or until we learn new information about the same
    /// member.
    pub fn updates_backlog(&self) -> usize {
        self.updates.len()
    }

    /// Reports the current length of the custom broadcast queue.
    ///
    /// Custom broadcasts are transmitted [`Config::max_transmissions`]
    /// times at most or until they get invalidated by another custom
    /// broadcast.
    pub fn custom_broadcast_backlog(&self) -> usize {
        self.custom_broadcasts.len()
    }

    /// Replaces the current configuration with a new one.
    ///
    /// Most of the time a static configuration is more than enough, but
    /// for use-cases where the cluster size can drastically change during
    /// normal operations, changing the configuration parameters is a
    /// nicer alternative to recreating the Foca instance.
    ///
    /// Presently, attempting to change [`Config::probe_period`] or
    /// [`Config::probe_rtt`] results in [`Error::InvalidConfig`]; For
    /// such cases it's recommended to recreate your Foca instance. When
    /// an error occurs, every configuration parameter remains
    /// unchanged.
    pub fn set_config(&mut self, config: Config) -> Result<()> {
        if self.config.probe_period != config.probe_period
            || self.config.probe_rtt != config.probe_rtt
            || (self.config.periodic_announce.is_none() && config.periodic_announce.is_some())
            || (self.config.periodic_gossip.is_none() && config.periodic_gossip.is_some())
        {
            Err(Error::InvalidConfig)
        } else {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                config = tracing::field::debug(&config),
                "Configuration changed"
            );

            self.config = config;
            Ok(())
        }
    }

    /// Handle data received from the network.
    ///
    /// Data larger than the configured limit will be rejected. Errors are
    /// expected if you're receiving arbitrary data (which very likely if
    /// you are listening to a socket address).
    pub fn handle_data(&mut self, mut data: &[u8], mut runtime: impl Runtime<T>) -> Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "handle_data",
            len = data.len(),
            header = tracing::field::Empty,
            num_updates = tracing::field::Empty,
        )
        .entered();

        if data.remaining() > self.config.max_packet_size.get() {
            return Err(Error::DataTooBig);
        }

        let header = self
            .codec
            .decode_header(&mut data)
            .map_err(anyhow::Error::msg)
            .map_err(Error::Decode)?;

        #[cfg(feature = "tracing")]
        span.record("header", tracing::field::debug(&header));

        // Since one can implement PartialEq and Identity however
        // they like, there's no guarantee that if addresses are
        // different, so are identities. So we check both
        if header.src == self.identity || header.src.addr() == self.identity.addr() {
            return Err(Error::DataFromOurselves);
        }

        let remaining = data.remaining();
        // A single trailing byte or a Announce payload with _any_
        // data is bad
        if remaining == 1 || (header.message == Message::Announce && remaining > 0) {
            return Err(Error::MalformedPacket);
        }

        if !self.accept_payload(&header) {
            #[cfg(feature = "tracing")]
            tracing::trace!("Payload not accepted");

            return Ok(());
        }

        // We can skip this buffering is we assume that reaching here
        // means the packet is valid. But that doesn't seem like a very
        // good idea...
        self.member_buf.clear();
        if remaining >= 2 && header.message != Message::Broadcast {
            let num_updates = data.get_u16();
            #[cfg(feature = "tracing")]
            span.record("num_updates", num_updates);

            for _i in 0..num_updates {
                self.member_buf.push(
                    self.codec
                        .decode_member(&mut data)
                        .map_err(anyhow::Error::msg)
                        .map_err(Error::Decode)?,
                );
            }
        }

        let Header {
            src,
            src_incarnation,
            dst: _,
            message,
        } = header;

        let sender_is_active = self
            // It's a known member, so we ensure our knowledge about
            // it is up-to-date (it is at _least_ alive, since it can
            // talk)
            .apply_update(
                Member::new(src.clone(), src_incarnation, State::Alive),
                &mut runtime,
            )?;

        // But dead members are ignored. At least until the member
        // list gets reaped.
        if !sender_is_active {
            #[cfg(feature = "tracing")]
            tracing::trace!("Discarded payload: Inactive sender");

            // When the sender is inactive, we opt to not trust anything
            // in their updates payload since the info is likely stale or
            // untrustworthy
            // However, if they consider our identity as down they'll never
            // learn that we think they are down since our payload is
            // untrustworthy from their perspective
            // So we handle TurnUndead here, otherwise the nodes will be
            // spamming each other with this message until enough time passes
            // that foca forgets the down member (`Config::remove_down_after`)
            if message == Message::TurnUndead {
                self.handle_self_update(Incarnation::default(), State::Down, &mut runtime)?;
            }

            if self.config.notify_down_members {
                self.send_message(src, Message::TurnUndead, runtime)?;
            }

            return Ok(());
        }

        // Now that we know the member is active, we'll handle the
        // updates, which may change our referential cluster
        // representation and our own connection state.
        //
        // Here we take the Vec so we can drain it without upsetting
        // the borrow checker. And then put it back in its place, so
        // that we can keep reusing its already-allocated space.
        let mut updates = mem::take(&mut self.member_buf);
        self.apply_many(updates.drain(..), &mut runtime)?;
        debug_assert_eq!(
            0,
            self.member_buf.capacity(),
            "member_buf modified while taken"
        );
        self.member_buf = updates;

        // Right now there might still be some data left to read in the
        // buffer (custom broadcasts). We'll handle those before we
        // react to the message we just received
        let custom_broadcasts_result = self.handle_custom_broadcasts(data, Some(&src));

        // If we're not connected (anymore), we can't react to a message
        // so there's nothing more to do
        if self.connection_state != ConnectionState::Connected {
            return custom_broadcasts_result;
        }

        match message {
            Message::Ping(probe_number) => {
                self.send_message(src, Message::Ack(probe_number), runtime)?;
            }
            Message::Ack(probe_number) => {
                #[cfg_attr(not(feature = "tracing"), allow(clippy::if_same_then_else))]
                if self.probe.receive_ack(&src, probe_number) {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(probed_id = tracing::field::debug(&src), "Probe success");
                } else {
                    // May be triggered by a member that slows down (say, you ^Z
                    // the process and `fg` back after a while).
                    // Might be interesting to keep an eye on.
                    #[cfg(feature = "tracing")]
                    tracing::trace!(
                        current_probe_number = self.probe.probe_number(),
                        "Unexpected Ack"
                    );
                }
            }
            Message::PingReq {
                target,
                probe_number,
            } => {
                if target == self.identity {
                    return Err(Error::IndirectForOurselves);
                }
                self.send_message(
                    target,
                    Message::IndirectPing {
                        origin: src,
                        probe_number,
                    },
                    runtime,
                )?;
            }
            Message::IndirectPing {
                origin,
                probe_number,
            } => {
                if origin == self.identity {
                    return Err(Error::IndirectForOurselves);
                }
                self.send_message(
                    src,
                    Message::IndirectAck {
                        target: origin,
                        probe_number,
                    },
                    runtime,
                )?;
            }
            Message::IndirectAck {
                target,
                probe_number,
            } => {
                if target == self.identity {
                    return Err(Error::IndirectForOurselves);
                }
                self.send_message(
                    target,
                    Message::ForwardedAck {
                        origin: src,
                        probe_number,
                    },
                    runtime,
                )?;
            }
            Message::ForwardedAck {
                origin,
                probe_number,
            } => {
                if origin == self.identity {
                    return Err(Error::IndirectForOurselves);
                }
                if self.probe.receive_indirect_ack(&src, probe_number) {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        probed_id = tracing::field::debug(self.probe.target()),
                        "Indirect probe success"
                    );
                } else {
                    #[cfg(feature = "tracing")]
                    tracing::trace!("Unexpected ForwardedAck sender");
                }
            }
            Message::Announce => self.send_message(src, Message::Feed, runtime)?,
            Message::TurnUndead => {
                #[cfg(feature = "tracing")]
                tracing::debug!("The cluster thinks we're down");

                self.handle_self_update(Incarnation::default(), State::Down, runtime)?;
            }
            // Nothing to do. These messages do not expect any reply
            Message::Gossip | Message::Feed | Message::Broadcast => {}
        };

        custom_broadcasts_result
    }

    fn serialize_member(&mut self, member: Member<T>) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.codec
            .encode_member(&member, &mut buf)
            .map_err(anyhow::Error::msg)
            .map_err(Error::Encode)?;

        Ok(buf)
    }

    fn reset(&mut self) {
        self.connection_state = ConnectionState::Disconnected;
        self.incarnation = Incarnation::default();
        self.timer_token = self.timer_token.wrapping_add(1);
        self.probe.clear();
        // XXX It might make sense to `self.updates.clear()` if we're
        //     down for a very long while, but we don't track instants
        //     internally... Exposing a public method to do so and
        //     letting drivers decide when to do it could be a way
        //     out. But recreating Foca is quite cheap, so revisit
        //     me maybe?
    }

    fn probe_random_member(&mut self, mut runtime: impl Runtime<T>) -> Result<()> {
        // NEEDSWORK: A codec error may leave us in a weird state where
        //            foca talks to the cluster normally but never
        //            probes their peers. It's, however, unlikely to
        //            happen as there are many attempts at
        //            encoding/decoding members before we start probing
        debug_assert_eq!(self.connection_state, ConnectionState::Connected);

        let mut probe_was_incomplete = false;
        if !self.probe.validate() {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                probed_id = tracing::field::debug(self.probe.target()),
                "Recovering: Probe cycle didn't complete correctly"
            );
            // Probe has invalid state. We'll reset and submit another timer
            // so that foca can recover from the issue gracefully
            self.probe.clear();
            probe_was_incomplete = true;
        }

        if let Some(failed) = self.probe.take_failed() {
            // Applying here can fail if:
            //
            //  1. The member increased its incarnation since the probe started
            //     (as a side effect of someone else probing and suspecting it)
            //
            //  2. The member was ALREADY suspect when we picked it for probing
            //
            //  3. The member is now Down, either by leaving voluntarily or by
            //     being declared down by another cluster member
            //
            //  4. The member doesn't exist anymore. i.e. a newer identity with
            //     the same address has appeared in the cluster
            let as_suspect = Member::new(failed.id().clone(), failed.incarnation(), State::Suspect);
            if let Some(summary) = self
                .members
                .apply_existing_if(as_suspect.clone(), |_member| true)
            {
                let is_active_now = summary.is_active_now;
                #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                let apply_successful = summary.apply_successful;
                self.handle_apply_summary(summary, as_suspect, &mut runtime)?;

                // Now we ensure we change the member to Down if it
                // isn't already inactive
                if is_active_now {
                    // We check for summary.apply_successful prior to logging
                    // because we may pick a member multiple times before the
                    // timer runs out.
                    // May lead to not logging at all if our knowledge of this
                    // member was already set as State::Suspect
                    #[cfg(feature = "tracing")]
                    if apply_successful {
                        tracing::debug!(
                            member_id = tracing::field::debug(failed.id()),
                            timeout = tracing::field::debug(self.config.suspect_to_down_after),
                            "Member failed probe, will declare it down if it doesn't react"
                        );
                    }
                    runtime.submit_after(
                        Timer::ChangeSuspectToDown {
                            member_id: failed.id().clone(),
                            incarnation: failed.incarnation(),
                            token: self.timer_token,
                        },
                        self.config.suspect_to_down_after,
                    );
                }
            }
        }

        if let Some(member) = self.members.next(&mut self.rng) {
            let member_id = member.id().clone();
            let probe_number = self.probe.start(member.clone());

            #[cfg(feature = "tracing")]
            tracing::debug!(member_id = tracing::field::debug(&member_id), "Probe start");

            self.send_message(member_id.clone(), Message::Ping(probe_number), &mut runtime)?;

            runtime.submit_after(
                Timer::SendIndirectProbe {
                    probed_id: member_id,
                    token: self.timer_token,
                },
                self.config.probe_rtt,
            );
        } else {
            // Should never happen... Reaching here is gated by being
            // online, which requires having at least one active member
            #[cfg(feature = "tracing")]
            tracing::debug!("Expected to find an active member to probe");
        }

        runtime.submit_after(
            Timer::ProbeRandomMember(self.timer_token),
            self.config.probe_period,
        );

        if probe_was_incomplete {
            Err(Error::IncompleteProbeCycle)
        } else {
            Ok(())
        }
    }

    // shortcut for apply + handle
    fn apply_update(&mut self, update: Member<T>, runtime: impl Runtime<T>) -> Result<bool> {
        debug_assert_ne!(&self.identity, update.id());
        let summary = self.members.apply(update.clone(), &mut self.rng);
        let active = summary.is_active_now;
        self.handle_apply_summary(summary, update, runtime)?;

        Ok(active)
    }

    fn handle_apply_summary(
        &mut self,
        summary: ApplySummary<T>,
        update: Member<T>,
        mut runtime: impl Runtime<T>,
    ) -> Result<()> {
        let id = update.id().clone();

        if summary.apply_successful {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                update = tracing::field::debug(&update),
                summary = tracing::field::debug(&summary),
                "Update applied"
            );

            // Cluster state changed, start broadcasting it
            let addr = Addr(id.addr());
            let data = self.serialize_member(update)?;
            self.updates
                .add_or_replace(addr, data, self.config.max_transmissions.get().into());

            // Down is a terminal state, so set up a handler for removing
            // the member so that it may rejoin later
            if !summary.is_active_now {
                runtime.submit_after(Timer::RemoveDown(id.clone()), self.config.remove_down_after);
            }
        }

        if let Some(old) = summary.replaced_id {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                previous_id = tracing::field::debug(&old),
                member_id = tracing::field::debug(&id),
                "Renamed"
            );
            runtime.notify(Notification::Rename(old, id.clone()));
        }

        if summary.changed_active_set {
            if summary.is_active_now {
                #[cfg(feature = "tracing")]
                tracing::debug!(member_id = tracing::field::debug(&id), "Member up");
                runtime.notify(Notification::MemberUp(id));
            } else {
                #[cfg(feature = "tracing")]
                tracing::debug!(member_id = tracing::field::debug(&id), "Member down");
                runtime.notify(Notification::MemberDown(id));
            }
        }

        Ok(())
    }

    fn handle_custom_broadcasts(&mut self, mut data: &[u8], sender: Option<&T>) -> Result<()> {
        if !data.is_empty() && data.len() < 3 {
            return Err(Error::MalformedPacket);
        }

        while data.remaining() > 2 {
            let pkt_len = data.get_u16() as usize;
            if pkt_len == 0 || data.len() < pkt_len {
                return Err(Error::MalformedPacket);
            }
            let pkt = &data[..pkt_len];
            if let Some(key) = self
                .broadcast_handler
                .receive_item(pkt, sender)
                .map_err(anyhow::Error::msg)
                .map_err(Error::CustomBroadcast)?
            {
                #[cfg(feature = "tracing")]
                tracing::trace!(len = pkt_len, "received broadcast item");

                self.custom_broadcasts.add_or_replace(
                    key,
                    pkt.to_vec(),
                    self.config.max_transmissions.get().into(),
                );
            }
            data.advance(pkt_len);
        }

        if data.has_remaining() {
            Err(Error::MalformedPacket)
        } else {
            Ok(())
        }
    }

    fn become_disconnected(&mut self, mut runtime: impl Runtime<T>) {
        // We reached zero active members, so we're offline
        debug_assert_eq!(0, self.num_members());
        self.connection_state = ConnectionState::Disconnected;

        // Ignore every timer event we sent up until this point.
        // This is to stop the probe cycle and prevent members from
        // being switched the Down state since we have little
        // confidence about our own state at this point.
        self.timer_token = self.timer_token.wrapping_add(1);
        self.probe.clear();

        runtime.notify(Notification::Idle);
    }

    fn become_undead(&mut self, mut runtime: impl Runtime<T>) {
        self.connection_state = ConnectionState::Undead;

        // We're down, whatever we find out by probing is unreliable
        self.probe.clear();

        // Just like `become_disconnected`, we want to avoid
        // handling events that aren't relevant anymore.
        self.timer_token = self.timer_token.wrapping_add(1);

        runtime.notify(Notification::Defunct);
    }

    fn become_connected(&mut self, mut runtime: impl Runtime<T>) {
        debug_assert_ne!(0, self.num_members());
        self.connection_state = ConnectionState::Connected;

        // We have at least one active member, so we can start
        // probing
        runtime.submit_after(
            Timer::ProbeRandomMember(self.timer_token),
            self.config.probe_period,
        );

        if let Some(ref params) = self.config.periodic_announce {
            runtime.submit_after(Timer::PeriodicAnnounce(self.timer_token), params.frequency);
        }

        if let Some(ref params) = self.config.periodic_gossip {
            runtime.submit_after(Timer::PeriodicGossip(self.timer_token), params.frequency);
        }

        runtime.notify(Notification::Active);
    }

    #[inline]
    fn estimate_feed_capacity(&self, remaining: usize) -> usize {
        // We're can't be precise here: not only we don't control how things
        // are being encoded, identities may have variable length too
        // So we'll just do some very rough estimation just to find an upper
        // bound.
        // Header contains 2 identities + message::feed + incarnation(u16)
        // so header_len / 2 is good enough for identity_len
        let identity_len = { self.config.max_packet_size.get().saturating_sub(remaining) / 2 };
        // and we always answer at least 5, in case the estimation is bonkers
        usize::max(remaining / identity_len, 5)
    }

    fn send_message(
        &mut self,
        dst: T,
        message: Message<T>,
        mut runtime: impl Runtime<T>,
    ) -> Result<()> {
        let header = Header {
            src: self.identity.clone(),
            src_incarnation: self.incarnation,
            dst: dst.clone(),
            message,
        };

        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "send_message",
            header = tracing::field::debug(&header),
            num_updates = tracing::field::Empty,
            num_broadcasts = tracing::field::Empty,
            len = tracing::field::Empty,
        )
        .entered();

        // XXX We take() here and by the end we put it back.
        //     This must be done for every return point in send_message
        self.send_buf.clear();
        let mut buf = mem::take(&mut self.send_buf).limit(self.config.max_packet_size.get());
        debug_assert_eq!(
            buf.get_ref().capacity(),
            self.config.max_packet_size.get(),
            "send_buf lost capacity, would trigger unnecessary allocs"
        );

        if let Err(err) = self
            .codec
            .encode_header(&header, &mut buf)
            .map_err(anyhow::Error::msg)
            .map_err(Error::Encode)
        {
            debug_assert_eq!(0, self.send_buf.capacity(), "send_buf modified while taken");
            self.send_buf = buf.into_inner();
            return Err(err);
        }

        let (needs_piggyback, only_active_members) = match header.message {
            // Announce/TurnUndead packets contain nothing but the header
            // Broadcast packets stuffs only custom broadcasts
            Message::Announce | Message::TurnUndead | Message::Broadcast => (false, false),
            // Feed packets stuff active members at the tail
            Message::Feed => (true, true),
            // Every other message stuffs cluster updates
            _ => (true, false),
        };

        // If we're piggybacking data, we need at least 2 extra bytes
        // so that we can also encode the number of items we're stuffing
        // into this buffer
        if needs_piggyback && buf.remaining_mut() > 2 {
            // Where we'll write the total number of items
            let tally_position = buf.get_ref().len();
            // We leave a zero here so that the buffer advances, then
            // we'll come back to `tally_position` and overwrite this
            // with the actual total
            buf.put_u16(0);

            let mut num_items = 0;

            if only_active_members {
                self.member_buf.clear();
                self.members.choose_active_members(
                    // Done in order to prevent copying and sorting a large
                    // set of members just to not use them at all because
                    // they don't fit the remaining buffer
                    self.estimate_feed_capacity(buf.remaining_mut()),
                    &mut self.member_buf,
                    &mut self.rng,
                    |member| member != &dst,
                );

                while let Some(chosen) = self.member_buf.pop() {
                    let pos = buf.get_ref().len();
                    if let Err(_ignored) = self.codec.encode_member(&chosen, &mut buf) {
                        // encoding the member might have advanced the cursor
                        // of the buffer before yielding the error
                        // this resets it to the last known valid position
                        buf.get_mut().truncate(pos);
                        break;
                    }
                    num_items += 1;
                }
            } else {
                // u16::MAX guarantees that num_events can be safely
                // cast from usize to u16
                let num_updates = self.updates.fill(&mut buf, u16::MAX.into());
                num_items = u16::try_from(num_updates).expect("usize bound by u16::MAX");
            }

            // Seek back and write the correct number of items added
            buf.get_mut()[tally_position..].as_mut().put_u16(num_items);
            #[cfg(feature = "tracing")]
            span.record("num_updates", num_items);
        }

        let add_custom_broadcast = buf.has_remaining_mut()
            // Every message but Announce includes custom broadcasts by
            // default
            && !matches!(header.message, Message::Announce)
            // Unless the broadcast handler says no
            && self.broadcast_handler.should_add_broadcast_data(&dst);

        if add_custom_broadcast {
            // Fill the remaining space in the buffer with custom
            // broadcasts, if any
            #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
            let num_broadcasts = self
                .custom_broadcasts
                .fill_with_len_prefix(&mut buf, usize::MAX);
            #[cfg(feature = "tracing")]
            span.record("num_broadcasts", num_broadcasts);
        }

        let data = buf.into_inner();
        #[cfg(feature = "tracing")]
        span.record("len", data.len());

        #[cfg(feature = "tracing")]
        tracing::trace!("Message sent");

        runtime.send_to(dst, &data);

        // absorb the buf into send_buf so we can reuse its capacity
        debug_assert_eq!(0, self.send_buf.capacity(), "send_buf modified while taken");
        self.send_buf = data;
        Ok(())
    }

    fn accept_payload(&self, header: &Header<T>) -> bool {
        // Only accept payloads addressed to us
        header.dst == self.identity
            // Unless it's an Announce message
            || (header.message == Message::Announce
                // Then we accept it if DST is one of our _possible_
                // identities
                && self.identity.addr() == header.dst.addr())
    }

    fn handle_self_update(
        &mut self,
        incarnation: Incarnation,
        state: State,
        mut runtime: impl Runtime<T>,
    ) -> Result<()> {
        match state {
            State::Suspect => {
                let increase_incarnation = match self.incarnation.cmp(&incarnation) {
                    // This can happen when a member received an update about
                    // someone else suspecting us but hasn't received our
                    // refutal yet. There's no need to increase our incarnation
                    Ordering::Greater => {
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            incarnation = self.incarnation,
                            suspected = incarnation,
                            "Received suspicion about old incarnation",
                        );
                        false
                    }

                    // Unexpected: someone suspects our identity but thinks we were
                    // in a higher incarnation. May happen due to members flapping,
                    // but can also be a sign of a bad actor (multiple identical
                    // identities, clients bumping identities from other members,
                    // corrupted data, etc)
                    // We'll emit a warning and then refute the suspicion normally
                    Ordering::Less => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            incarnation = self.incarnation,
                            suspected = incarnation,
                            "Suspicion on incarnation higher than current",
                        );
                        true
                    }

                    // The usual case: our current incarnation is being suspected,
                    // so we need to increase it
                    Ordering::Equal => true,
                };

                let incarnation = Incarnation::max(incarnation, self.incarnation);

                // We need to rejoin the cluster when this situation happens
                // because it will be impossible to refute suspicion
                if incarnation == Incarnation::MAX {
                    if !self.attempt_rejoin(&mut runtime)? {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("Inactive: reached Incarnation::MAX",);
                        self.become_undead(runtime);
                    }
                    return Ok(());
                }

                if increase_incarnation {
                    // XXX Overzealous checking
                    self.incarnation = incarnation.saturating_add(1);
                }

                // We do NOT add ourselves as Alive to the updates buffer
                // because it's unnecessary: by bumping our incarnation *any*
                // message we send will be interpreted as a broadcast update
                // See: `tests::message_from_aware_suspect_refutes_suspicion`
                //
                // But since the cluster is chatting about us possibly being
                // down, we'll send a few updates around in order to help
                // disseminate the refutation
                self.gossip(runtime)?;
            }
            State::Alive => {
                // The cluster is talking about our liveness. Nothing to do.
            }
            State::Down => {
                // It's impossible to refute a Down state so we'll need
                // to rejoin somehow
                if !self.attempt_rejoin(&mut runtime)? {
                    self.become_undead(runtime);
                }
            }
        }
        Ok(())
    }

    fn attempt_rejoin(&mut self, mut runtime: impl Runtime<T>) -> Result<bool> {
        if let Some(new_identity) = self.identity.renew() {
            if self.identity == new_identity {
                #[cfg(feature = "tracing")]
                tracing::debug!("Rejoin failure: Identity::renew() returned same id",);
                Ok(false)
            } else if !new_identity.win_addr_conflict(&self.identity) {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    new = tracing::field::debug(&new_identity),
                    old = tracing::field::debug(&self.identity),
                    "Rejoin failure: New identity doesn't win the conflict with the old one",
                );
                Ok(false)
            } else {
                self.change_identity(new_identity.clone(), &mut runtime)?;

                runtime.notify(Notification::Rejoin(new_identity));

                Ok(true)
            }
        } else {
            Ok(false)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ConnectionState {
    Disconnected,
    Connected,
    Undead,
}

/// A Broadcast Handler that rejects any form of custom broadcast.
///
/// Used by Foca when constructed via [`Foca::new()`].
pub struct NoCustomBroadcast;

/// Error emitted by [`NoCustomBroadcast`] when any trailing byte is
/// found. Will be wrapped by [`Error`]
#[derive(Debug, Clone, Copy)]
pub struct BroadcastsDisabledError;

impl fmt::Display for BroadcastsDisabledError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Broadcasts disabled")
    }
}

#[cfg(feature = "std")]
impl std::error::Error for BroadcastsDisabledError {}

impl<T> BroadcastHandler<T> for NoCustomBroadcast {
    type Key = &'static [u8];
    type Error = BroadcastsDisabledError;

    fn receive_item(
        &mut self,
        _data: &[u8],
        _sender: Option<&T>,
    ) -> core::result::Result<Option<Self::Key>, Self::Error> {
        Err(BroadcastsDisabledError)
    }
}

struct Addr<T>(T);

impl<T: PartialEq> Invalidates for Addr<T> {
    // State is managed externally (via Members), so invalidation
    // is a trivial replace-if-same-key
    fn invalidates(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[cfg(test)]
impl<T, C, RNG, B> Foca<T, C, RNG, B>
where
    T: Identity,
    C: Codec<T>,
    RNG: rand::Rng,
    B: BroadcastHandler<T>,
{
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    pub(crate) fn probe(&self) -> &Probe<T> {
        &self.probe
    }

    pub fn timer_token(&self) -> TimerToken {
        self.timer_token
    }

    pub(crate) fn connection_state(&self) -> ConnectionState {
        self.connection_state
    }

    pub(crate) fn apply(&mut self, member: Member<T>, mut runtime: impl Runtime<T>) -> Result<()> {
        self.apply_many(core::iter::once(member), &mut runtime)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use alloc::vec;
    use core::{
        num::{NonZeroU8, NonZeroUsize},
        time::Duration,
    };

    use bytes::{Buf, BufMut, Bytes};
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::testing::{BadCodec, ID};

    fn rng() -> SmallRng {
        SmallRng::seed_from_u64(0xF0CA)
    }

    fn codec() -> BadCodec {
        BadCodec
    }

    fn config() -> Config {
        Config::simple()
    }

    fn encode(src: (Header<ID>, Vec<Member<ID>>)) -> Bytes {
        let (header, updates) = src;
        let mut codec = codec();
        let mut buf = bytes::BytesMut::new();

        codec
            .encode_header(&header, &mut buf)
            .expect("BadCodec shouldn't fail");

        if !updates.is_empty() {
            buf.put_u16(u16::try_from(updates.len()).unwrap());
            for member in updates.iter() {
                codec
                    .encode_member(member, &mut buf)
                    .expect("BadCodec shouldn't fail");
            }
        }

        buf.freeze()
    }

    fn decode(mut src: impl Buf) -> (Header<ID>, Vec<Member<ID>>) {
        let mut codec = codec();
        let header = codec.decode_header(&mut src).unwrap();

        let mut updates = Vec::new();
        if src.has_remaining() {
            let num_items = src.get_u16();
            updates.reserve(num_items.into());

            for _i in 0..num_items {
                updates.push(codec.decode_member(&mut src).unwrap());
            }
        }

        (header, updates)
    }

    #[test]
    fn invariants() {
        let identity = ID::new(42);
        let mut foca = Foca::new(identity, config(), rng(), codec());

        assert_eq!(ConnectionState::Disconnected, foca.connection_state());

        assert_eq!(0, foca.num_members());

        assert_eq!(None, foca.iter_members().next());

        assert_eq!(Err(Error::NotUndead), foca.reuse_down_identity());

        let mut runtime = AccumulatingRuntime::new();
        assert_eq!(
            Err(Error::SameIdentity),
            foca.change_identity(identity, &mut runtime)
        );
        assert_eq!(Ok(()), foca.change_identity(ID::new(43), &mut runtime));
        assert_eq!(&ID::new(43), foca.identity());
    }

    #[test]
    fn cant_change_config_probe_timers() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());

        let mut bad_config = config();
        bad_config.probe_rtt += Duration::from_millis(1);

        assert_eq!(
            Err(Error::InvalidConfig),
            foca.set_config(bad_config),
            "must not be able to change probe_rtt"
        );

        let mut bad_config = config();
        bad_config.probe_period -= Duration::from_secs(1);

        assert_eq!(
            Err(Error::InvalidConfig),
            foca.set_config(bad_config),
            "must not be able to change probe_period"
        );

        assert_eq!(Ok(()), foca.set_config(config()));
    }

    #[test]
    fn cant_probe_when_not_connected() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());

        let runtime = AccumulatingRuntime::new();
        let res = foca.handle_timer(Timer::ProbeRandomMember(foca.timer_token()), runtime);

        assert_eq!(Err(Error::NotConnected), res);
    }

    #[test]
    fn codec_errors_are_forwarded_correctly() {
        // A codec that only produces errors
        struct UnitErroringCodec;

        #[derive(Debug)]
        struct UnitError;

        impl fmt::Display for UnitError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("no")
            }
        }

        impl Codec<ID> for UnitErroringCodec {
            type Error = UnitError;

            fn encode_header(
                &mut self,
                _header: &Header<ID>,
                _buf: impl BufMut,
            ) -> core::result::Result<(), Self::Error> {
                Err(UnitError)
            }

            fn decode_header(
                &mut self,
                _buf: impl Buf,
            ) -> core::result::Result<Header<ID>, Self::Error> {
                Err(UnitError)
            }

            fn encode_member(
                &mut self,
                _member: &Member<ID>,
                _buf: impl BufMut,
            ) -> core::result::Result<(), Self::Error> {
                Err(UnitError)
            }

            fn decode_member(
                &mut self,
                _buf: impl Buf,
            ) -> core::result::Result<Member<ID>, Self::Error> {
                Err(UnitError)
            }
        }

        // And a runtime that does nothing to pair it with
        struct NoopRuntime;

        impl Runtime<ID> for NoopRuntime {
            fn notify(&mut self, _notification: Notification<ID>) {}
            fn send_to(&mut self, _to: ID, _data: &[u8]) {}
            fn submit_after(&mut self, _event: Timer<ID>, _after: Duration) {}
        }

        let mut foca = Foca::new(ID::new(1), Config::simple(), rng(), UnitErroringCodec);

        assert_eq!(
            Err(Error::Encode(anyhow::Error::msg(UnitError))),
            foca.announce(ID::new(2), NoopRuntime)
        );

        assert_eq!(
            Err(Error::Decode(anyhow::Error::msg(UnitError))),
            foca.handle_data(b"hue", NoopRuntime)
        );
    }

    macro_rules! expect_scheduling {
        ($runtime: expr, $timer: expr, $after: expr) => {
            $runtime
                .take_scheduling($timer)
                .map(|after| assert_eq!(after, $after, "Incorrect scheduling for {:?}", $timer))
                .unwrap_or_else(|| panic!("Timer {:?} not found", $timer));
        };
    }

    macro_rules! expect_notification {
        ($runtime: expr, $notification: expr) => {
            $runtime
                .take_notification($notification)
                .unwrap_or_else(|| panic!("Notification {:?} not found", $notification));
        };
    }

    macro_rules! reject_notification {
        ($runtime: expr, $notification: expr) => {
            assert!(
                $runtime.take_notification($notification).is_none(),
                "Unwanted notification {:?} found",
                $notification
            );
        };
    }

    macro_rules! expect_message {
        ($runtime: expr, $member: expr, $message: expr) => {
            let d = $runtime
                .take_data($member)
                .unwrap_or_else(|| panic!("Message to member {:?} not found", $member));
            let (header, _) = decode(d);
            assert_eq!(
                header.message, $message,
                "Message to member {:?} is {:?}. Expected {:?}",
                $member, header.message, $message
            );
        };
    }

    #[test]
    fn can_join_with_another_client() {
        let mut foca_one = Foca::new(ID::new(1), config(), rng(), codec());
        let mut foca_two = Foca::new(ID::new(2), config(), rng(), codec());

        let mut runtime = AccumulatingRuntime::new();

        // Here foca_one will send an announce packet to foca_two
        foca_one
            .announce(*foca_two.identity(), &mut runtime)
            .expect("no errors");

        assert_eq!(
            0,
            foca_one.num_members(),
            "announcing shouldn't change members"
        );

        // So the runtime should've been instructed to send a
        // message to foca_two

        let data = decode(
            runtime
                .take_data(ID::new(2))
                .expect("No data for ID::new(2) found"),
        );
        assert_eq!(data.0.message, Message::Announce);

        runtime.clear();
        foca_two
            .handle_data(&encode(data), &mut runtime)
            .expect("no errors");

        // Right now, foca_two should be aware of foca_one
        assert_eq!(1, foca_two.num_members());
        // Whilst foca_one is oblivious to the effect of its announce
        assert_eq!(0, foca_one.num_members());

        // So we should have gotten a notification about going online
        expect_notification!(runtime, Notification::<ID>::Active);
        expect_notification!(runtime, Notification::MemberUp(ID::new(1)));

        // And a event to trigger a probe should've been
        // scheduled
        expect_scheduling!(
            runtime,
            Timer::<ID>::ProbeRandomMember(foca_one.timer_token()),
            config().probe_period
        );

        // More importantly, the runtime should've been instructed to
        // send a feed to foca_one, which will finally complete its
        // join cycle
        let data = decode(
            runtime
                .take_data(ID::new(1))
                .expect("Feed for ID::new(1) not found"),
        );
        assert_eq!(data.0.message, Message::Feed);

        runtime.clear();
        assert_eq!(Ok(()), foca_one.handle_data(&encode(data), &mut runtime));

        expect_notification!(runtime, Notification::<ID>::Active);
        expect_notification!(runtime, Notification::MemberUp(ID::new(2)));
        assert_eq!(1, foca_one.num_members());
    }

    #[test]
    fn feed_contains_only_active_members() {
        // We'll make `foca_one` send an Announce to `foca_two` and verify
        // that its reply is a Feed containing its known *active* members
        let one = ID::new(1);
        let two = ID::new(2);
        let mut foca_one = Foca::new(one, config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        assert_eq!(Ok(()), foca_one.announce(two, &mut runtime));
        let data = runtime
            .take_data(two)
            .expect("Should have a message for foca_two");

        runtime.clear();

        // Members 3 and 4 are active, 5 is down and should not be
        // found in the feed
        let members = [
            Member::alive(ID::new(3)),
            Member::suspect(ID::new(4)),
            Member::down(ID::new(5)),
        ];

        let mut foca_two = Foca::new(two, config(), rng(), codec());

        // Let `foca_two` know about the members
        for member in members.iter() {
            assert_eq!(Ok(()), foca_two.apply(member.clone(), &mut runtime));
        }

        // Receive the packet from `foca_one`
        assert_eq!(Ok(()), foca_two.handle_data(&data, &mut runtime));

        let feed_data = runtime
            .take_data(one)
            .expect("Should have a message for foca_one");

        let (header, mut updates) = decode(feed_data);

        assert_eq!(header.message, Message::Feed);
        assert_eq!(2, updates.len());

        // here we check that updates contains the active members
        // from the `members` vec. so we sort both by id then
        // compare
        let mut active = members
            .iter()
            .filter(|&m| m.is_active())
            .cloned()
            .collect::<Vec<_>>();
        active.sort_by_key(|m| *m.id());

        updates.sort_by_key(|m| *m.id());

        assert_eq!(active, updates);
    }

    #[test]
    fn piggyback_behaviour() {
        let max_transmissions = NonZeroU8::new(10).unwrap();
        let num_indirect_probes = NonZeroUsize::new(3).unwrap();

        let config = Config {
            max_transmissions,
            num_indirect_probes,
            ..config()
        };

        let mut foca = Foca::new(ID::new(1), config.clone(), rng(), codec());

        // A manually crafted Gossip packet from ID::new(2) addressed to
        // our foca instance
        let data = {
            let header = Header {
                src: ID::new(2),
                src_incarnation: 0,
                dst: ID::new(1),
                message: Message::Gossip,
            };
            // Containing these cluster updates:
            let updates = vec![
                Member::new(ID::new(3), 3, State::Alive),
                Member::new(ID::new(4), 1, State::Suspect),
                Member::new(ID::new(5), 1, State::Down),
            ];
            (header, updates)
        };

        let mut runtime = AccumulatingRuntime::new();
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));

        expect_notification!(runtime, Notification::<ID>::Active);
        // We didn't know about any  mentioned in the packet
        expect_notification!(runtime, Notification::MemberUp(ID::new(2)));
        expect_notification!(runtime, Notification::MemberUp(ID::new(3)));
        expect_notification!(runtime, Notification::MemberUp(ID::new(4)));
        // But an update about a Down member that we didn't know
        // about is cluster metadata only and shouldn't trigger
        // a notification
        reject_notification!(runtime, Notification::MemberDown(ID::new(5)));
        // It should, however, trigger a scheduling for forgetting
        // the member, so that they may rejoin the cluster
        expect_scheduling!(
            runtime,
            Timer::RemoveDown(ID::new(5)),
            config.remove_down_after
        );

        // 2 active members from the updates + the member the sent
        // the payload
        assert_eq!(3, foca.num_members());
        let mut members = foca
            .iter_members()
            .map(Member::id)
            .cloned()
            .collect::<Vec<_>>();
        members.sort_unstable();
        assert_eq!(vec![ID::new(2), ID::new(3), ID::new(4)], members);

        // Now, whenever we send a message that isn't part of the
        // join subprotocol (i.e.: not Feed nor Announce)
        // we should be emitting updates regardless of who we're
        // sending the message to.
        runtime.clear();
        assert_eq!(Ok(()), foca.gossip(&mut runtime));

        // When we gossip, we pick random `num_indirect_probes`
        // members to send them. And every update is disseminated
        // at most `max_transmissions` times.
        //
        // Since our ids are tiny, we know that every update
        // we have at the moment (the 3 that we received, plus
        // the discovery of the sender) will fit in a single
        // message.
        //
        // And since we just verified we have 3 active members,
        // which is exactly our fan out parameter
        // (`num_indirect_probes`), we expect that every
        // call to `gossip()` will drain 3 from the transmission
        // count of each update.
        //
        // So now we have `max_transmissions - 3` remaining
        // transmissions for each update.
        let mut remaining_tx = usize::from(max_transmissions.get()) - foca.num_members();

        assert_eq!(
            4,
            foca.updates_backlog(),
            "We should still have 4 updates in the backlog"
        );

        // let's gossip some more until we're in a more interesting
        // scenario
        while remaining_tx >= foca.num_members() {
            assert_eq!(Ok(()), foca.gossip(&mut runtime));

            remaining_tx -= foca.num_members();
            // So long as we have remaining_tx, the backlog should
            // remain the same
            assert_eq!(4, foca.updates_backlog());
        }

        assert!(remaining_tx < foca.num_members() && remaining_tx > 0);
        assert_eq!(4, foca.updates_backlog());

        // Now we sent enough broadcasts that we'll finally see the updates
        // backlog tank.
        // Since max_transmissions is set to 10 and every gossip() call
        // dropped 3:
        assert_eq!(1, remaining_tx);

        // Which means that the next gossip round should not only
        // finally drain the backlog: only one of the three Gossip
        // messages sent will contain our 4 updates. The remaining
        // should have no update at all.
        // (The value of an empty gossip message is questionable, but
        // since a valid message counts as a valid update it
        // essentially helps disseminate the knowledge of our
        // existance)
        runtime.clear();
        assert_eq!(Ok(()), foca.gossip(&mut runtime));

        let mut gossip_with_updates = 0;
        let mut empty_gossip = 0;

        for (_dst, data) in runtime.take_all_data() {
            let (header, updates) = decode(data);

            assert_eq!(Message::Gossip, header.message);
            if updates.is_empty() {
                empty_gossip += 1;
            } else {
                gossip_with_updates += 1;
            }
        }

        assert_eq!(1, gossip_with_updates);
        assert_eq!(2, empty_gossip);

        assert_eq!(0, foca.updates_backlog());
    }

    #[test]
    fn new_down_member_triggers_remove_down_scheduling() -> Result<()> {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // ID::new(2) is new and down
        foca.apply(Member::down(ID::new(2)), &mut runtime)?;
        expect_scheduling!(
            runtime,
            Timer::RemoveDown(ID::new(2)),
            config().remove_down_after
        );

        // We already know about ID::new(2) being down. So we don't
        // want to schedule anything
        foca.apply(Member::down(ID::new(2)), &mut runtime)?;
        assert_eq!(
            None,
            runtime.take_scheduling(Timer::RemoveDown(ID::new(2))),
            "Must not duplicate removal scheduling"
        );

        // A new _active_ member must not trigger the scheduling
        foca.apply(Member::alive(ID::new(3)), &mut runtime)?;
        assert_eq!(
            None,
            runtime.take_scheduling(Timer::RemoveDown(ID::new(3))),
            "Must not schedule removal of active member ID=3"
        );

        // But it should trigger if we change it to down via an update
        foca.apply(Member::down(ID::new(3)), &mut runtime)?;
        expect_scheduling!(
            runtime,
            Timer::RemoveDown(ID::new(3)),
            config().remove_down_after
        );

        Ok(())
    }

    #[test]
    fn notification_triggers() -> Result<()> {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // Brand new member. The first in our set, so we should
        // also be notified about going active
        foca.apply(Member::alive(ID::new(2)), &mut runtime)?;
        expect_notification!(runtime, Notification::MemberUp(ID::new(2)));
        expect_notification!(runtime, Notification::<ID>::Active);

        // Updated/stale knowledge about an active member shouldn't
        // trigger a notification so long as it doesn't go down
        runtime.clear();
        foca.apply(Member::alive(ID::new(2)), &mut runtime)?;
        foca.apply(Member::suspect(ID::new(2)), &mut runtime)?;
        foca.apply(Member::new(ID::new(2), 10, State::Alive), &mut runtime)?;
        reject_notification!(runtime, Notification::MemberUp(ID::new(2)));
        reject_notification!(runtime, Notification::MemberDown(ID::new(2)));

        // Another new member
        runtime.clear();
        foca.apply(Member::suspect(ID::new(3)), &mut runtime)?;
        expect_notification!(runtime, Notification::MemberUp(ID::new(3)));
        reject_notification!(runtime, Notification::<ID>::Active);

        // Existing member going down
        runtime.clear();
        foca.apply(Member::down(ID::new(3)), &mut runtime)?;
        expect_notification!(runtime, Notification::MemberDown(ID::new(3)));

        // A stale update should trigger no notification
        runtime.clear();
        foca.apply(Member::down(ID::new(3)), &mut runtime)?;
        reject_notification!(runtime, Notification::MemberDown(ID::new(3)));

        // A new member, but already down, so no notification
        runtime.clear();
        foca.apply(Member::down(ID::new(4)), &mut runtime)?;
        reject_notification!(runtime, Notification::MemberDown(ID::new(4)));

        // Last active member going down, we're going idle
        runtime.clear();
        assert_eq!(1, foca.num_members());
        foca.apply(Member::down(ID::new(2)), &mut runtime)?;
        expect_notification!(runtime, Notification::MemberDown(ID::new(2)));
        expect_notification!(runtime, Notification::<ID>::Idle);

        // New active member, going back to active
        runtime.clear();
        foca.apply(Member::alive(ID::new(5)), &mut runtime)?;
        expect_notification!(runtime, Notification::MemberUp(ID::new(5)));
        expect_notification!(runtime, Notification::<ID>::Active);

        // Now someone declared us (ID=1) down, we should
        // go defunct
        runtime.clear();
        foca.apply(Member::down(ID::new(1)), &mut runtime)?;
        expect_notification!(runtime, Notification::<ID>::Defunct);
        // But since we're not part of the member list, there shouldn't
        // be a notification about our id going down
        reject_notification!(runtime, Notification::MemberDown(ID::new(1)));

        // While defunct, we can still maintain members,
        runtime.clear();
        foca.apply(Member::down(ID::new(5)), &mut runtime)?;
        expect_notification!(runtime, Notification::MemberDown(ID::new(5)));

        foca.apply(Member::alive(ID::new(6)), &mut runtime)?;
        expect_notification!(runtime, Notification::MemberUp(ID::new(6)));

        // But until manual intervention happens, we are not active
        reject_notification!(runtime, Notification::<ID>::Active);

        assert_eq!(Ok(()), foca.reuse_down_identity());
        // Now since we are not defunct anymore, any message
        // received, even if it's a stale update should
        // notify that we're active again
        runtime.clear();
        assert_eq!(1, foca.num_members());
        foca.apply(Member::alive(ID::new(6)), &mut runtime)?;
        expect_notification!(runtime, Notification::<ID>::Active);

        Ok(())
    }

    #[test]
    fn not_submitting_indirect_probe_timer_causes_probe_error() -> Result<()> {
        // The probe cycle requires two timer events:
        //
        //   1. Timer::ProbeRandomMember, which starts the probe
        //   2. Timer::SendIndirectProbe, which sends indirect
        //      probes IFF we haven't received a direct reply
        //
        // This test verifies that not submitting the second
        // timer event causes an error.
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // Add an active member so that the probing can start
        foca.apply(Member::alive(ID::new(2)), &mut runtime)?;
        let probe_random_member = Timer::ProbeRandomMember(foca.timer_token());
        expect_scheduling!(runtime, probe_random_member.clone(), config().probe_period);

        // Start the probe now, instead of after `probe_period`
        runtime.clear();
        assert_eq!(
            Ok(()),
            foca.handle_timer(probe_random_member.clone(), &mut runtime)
        );

        // Which should instruct the runtime to trigger the second stage of
        // the probe after `probe_rtt`
        expect_scheduling!(
            runtime,
            Timer::SendIndirectProbe {
                probed_id: ID::new(2),
                token: foca.timer_token(),
            },
            config().probe_rtt
        );

        // But instead of triggering send_indirect_probe as instructed
        // we'll trigger probe_random_member again, simulating a
        // broken runtime
        assert_eq!(
            Err(Error::IncompleteProbeCycle),
            foca.handle_timer(probe_random_member, &mut runtime)
        );

        Ok(())
    }

    #[test]
    fn receiving_indirect_for_ourselves_causes_error() {
        // To pierce holes/partitions in the cluster the protocol
        // has a mechanism to request a member to talk to another
        // one on our behalf.
        //
        // This test verifies that if someone ask us to talk to
        // ourselves via this mechanism, an error occurrs.
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let probe_number = foca.probe().probe_number();
        let indirect_messages = vec![
            Message::PingReq {
                target: ID::new(1),
                probe_number,
            },
            Message::IndirectPing {
                origin: ID::new(1),
                probe_number,
            },
            Message::IndirectAck {
                target: ID::new(1),
                probe_number,
            },
            Message::ForwardedAck {
                origin: ID::new(1),
                probe_number,
            },
        ];

        for message in indirect_messages {
            let bad_header = Header {
                src: ID::new(2),
                src_incarnation: 0,
                dst: ID::new(1),
                message,
            };

            assert_eq!(
                Err(Error::IndirectForOurselves),
                foca.handle_data(&encode((bad_header, Vec::new())), &mut runtime)
            );
        }
    }

    #[test]
    fn cant_receive_data_from_same_identity() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        assert_eq!(
            Err(Error::DataFromOurselves),
            foca.handle_data(
                &encode((
                    Header {
                        src: ID::new(1),
                        src_incarnation: 0,
                        dst: ID::new(1),
                        message: Message::Announce
                    },
                    Vec::new()
                )),
                &mut runtime
            )
        );
    }

    #[test]
    fn cant_receive_data_from_same_addr() {
        let id = ID::new(1);
        let mut foca = Foca::new(id, config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // Just the address is the same now
        assert_eq!(
            Err(Error::DataFromOurselves),
            foca.handle_data(
                &encode((
                    Header {
                        src: id.bump(),
                        src_incarnation: 0,
                        dst: ID::new(1),
                        message: Message::Announce
                    },
                    Vec::new()
                )),
                &mut runtime
            )
        );
    }

    #[test]
    fn cant_receive_announce_with_extra_data() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        assert_eq!(
            Err(Error::MalformedPacket),
            foca.handle_data(
                &encode((
                    Header {
                        src: ID::new(2),
                        src_incarnation: 0,
                        dst: ID::new(1),
                        message: Message::Announce
                    },
                    Vec::from([Member::alive(ID::new(3))])
                )),
                &mut runtime
            )
        );
    }

    #[test]
    fn announce_to_wrong_id_is_accepted_if_same_prefix() {
        // Joining a cluster involves sending a Announce message to a
        // member we know about, that means that we need to know the
        // exact identity of a member.
        //
        // Re-joining a cluster involves either waiting until the
        // cluster forgets you went down or simply changing your
        // identity.
        //
        // That's when things may get confusing: if we want to be
        // able to rejoin a cluster fast, we need to be able to change
        // identities; But if everyone can change identities, how
        // can we send a valid Announce message?
        //
        // To facilitate this, we provide a mechanism relax the
        // check on Announce messages: if the packet was not addressed
        // directly to us, but to an identity that "has the same prefix"
        // we accept it.
        //
        // This mechanism is disabled by default. To enable it an
        // identity must specialize the default implementation of
        // the `has_same_prefix` method to yield `true` when they
        // want.
        //
        // This test verifies that this mechanism actually works.

        // This is our running Foca instance, with `target_id`. Nobody
        // in the cluster knows that our bump is 255, but everyone
        // knows about the ID::new(1) part.
        let target_id = ID::new_with_bump(1, 255);
        let codec = codec();
        let mut foca = Foca::new(target_id, config(), rng(), codec);
        let mut runtime = AccumulatingRuntime::new();

        // Our goal is getting `src` to join `target_id`'s cluster.
        let src_id = ID::new(2);

        // We'll send a packet destined to the wrong id, not
        // passing the "has same prefix" check to verify the join
        // doesn't happen
        let wrong_dst = ID::new(3);
        assert_ne!(target_id.addr(), wrong_dst.addr());
        let data = (
            Header {
                src: src_id,
                src_incarnation: 0,
                dst: wrong_dst,
                message: Message::Announce,
            },
            Vec::new(),
        );

        // Whislt it won't cause any errors
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));
        // The packet was simply ignored:
        assert_eq!(0, foca.num_members());

        // Now we'll send it to an identity that matches the same
        // prefix check
        let dst = ID::new_with_bump(1, 42);
        assert_ne!(target_id, dst);
        assert_eq!(target_id.addr(), dst.addr());
        let data = (
            Header {
                src: src_id,
                src_incarnation: 0,
                dst,
                message: Message::Announce,
            },
            Vec::new(),
        );
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));
        // So we should've successfully joined
        assert_eq!(1, foca.num_members());
        assert!(foca.iter_members().any(|member| member.id() == &src_id));
    }

    #[test]
    fn suspicion_refutal() -> Result<()> {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let original_incarnation = foca.incarnation();

        // Update declaring we are suspect.
        // We should be able to refute it simply by increasing
        // our incarnation
        foca.apply(Member::suspect(ID::new(1)), &mut runtime)?;
        assert!(original_incarnation < foca.incarnation());

        // Our incarnation may grow until a maximum level
        foca.apply(
            Member::new(ID::new(1), Incarnation::MAX - 1, State::Suspect),
            &mut runtime,
        )?;
        assert_eq!(Incarnation::MAX, foca.incarnation());

        // But if we live long enough, we may reach a point where
        // the incarnation is too high to refute. When this
        // happens, manual intervention is required.
        foca.apply(
            Member::new(ID::new(1), Incarnation::MAX, State::Suspect),
            &mut runtime,
        )?;
        assert_eq!(ConnectionState::Undead, foca.connection_state());

        Ok(())
    }

    #[test]
    fn incarnation_does_not_increase_for_stale_suspicion() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let suspected_incarnation = 10;
        let update = Member::new(ID::new(1), suspected_incarnation, State::Suspect);
        // First time the suspicion is fresh, and foca refures normally
        assert_eq!(Ok(()), foca.apply(update.clone(), &mut runtime));
        let current_incarnation = foca.incarnation();
        assert!(current_incarnation > suspected_incarnation);

        // But receiving the same update shouldn't make it
        // increase again
        assert_eq!(Ok(()), foca.apply(update, &mut runtime));
        assert_eq!(current_incarnation, foca.incarnation());
    }

    #[test]
    fn gossips_when_being_suspected() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();
        // just one peer in the cluster, for simplificy's sake
        assert_eq!(Ok(()), foca.apply(Member::alive(ID::new(2)), &mut runtime));

        // stale or not, receiving an update suspecting our
        // indentity should trigger a gossip round to our
        // peers
        for _round in 0..5 {
            runtime.clear();
            assert_eq!(
                Ok(()),
                foca.apply(Member::suspect(ID::new(1)), &mut runtime)
            );

            let (header, _updates) = decode(
                runtime
                    .take_data(ID::new(2))
                    .expect("Should have sent a message to ID=2"),
            );
            assert_eq!(Message::Gossip, header.message);
        }
    }

    #[test]
    fn change_identity_gossips_immediately() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // Introduce a new member so we have someone to gossip to
        assert_eq!(Ok(()), foca.apply(Member::alive(ID::new(2)), &mut runtime));

        assert_eq!(Ok(()), foca.change_identity(ID::new(99), &mut runtime));

        assert!(foca.updates_backlog() > 0);

        let (header, updates) = decode(
            runtime
                .take_data(ID::new(2))
                .expect("Should have sent a message to ID=2"),
        );

        assert_eq!(Message::Gossip, header.message);
        assert!(!updates.is_empty());
    }

    #[test]
    fn changing_identity_resets_timer_token() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let orig_timer_token = foca.timer_token();

        let mut runtime = AccumulatingRuntime::new();
        assert_eq!(Ok(()), foca.change_identity(ID::new(2), &mut runtime));

        assert_ne!(orig_timer_token, foca.timer_token());
    }

    #[test]
    fn renew_during_probe_shouldnt_cause_errors() {
        let id = ID::new(1).rejoinable();
        let mut foca = Foca::new(id, config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let updates = [
            Member::alive(ID::new(2)),
            Member::alive(ID::new(3)),
            Member::alive(ID::new(4)),
        ];

        // Prepare a foca instance with 3 known peers
        assert_eq!(
            Ok(()),
            foca.apply_many(updates.iter().cloned(), &mut runtime)
        );

        // By now we should've gotten an event to schedule probing
        let expected_timer = Timer::ProbeRandomMember(foca.timer_token());
        expect_scheduling!(runtime, expected_timer.clone(), config().probe_period);

        // When it fires (after Config::probe_period normally- directly now)
        // the probe cycle starts
        assert_eq!(Ok(()), foca.handle_timer(expected_timer, &mut runtime));

        // Which instructs us to probe a random member
        let probe_random_member_timer = runtime
            .find_scheduling(|e| matches!(e, Timer::ProbeRandomMember(_)))
            .expect("Probe cycle should have started")
            .clone();

        // Now we're in the middle of a probe cycle. What happens if
        // we are forced to change identities (either via being declared
        // down or manually changing ids)?
        let new_id = id.renew().unwrap();
        assert_eq!(Ok(()), foca.change_identity(new_id, &mut runtime));

        // In the bug scenario, a member received our new identity
        // and sent us a message, so foca became connected:
        //
        // assert_eq!(Ok(()), foca.apply(Member::alive(new_id), &mut runtime));
        //
        // And THEN the ProbeRandomMember event fired, which accepted
        // the event and made it all the way to `probe_random_member`
        // that correctly detected that there was still a member being
        // probed when it shouldn't

        // But the issue was present even before receiving a message
        // that makes foca go online: Since before the bug fix the
        // timer token wasn't being updated, the ProbeRandomMember
        // event was accepted and another trip-wire would fire:
        // `Error::NotConnected` (right before calling
        // `probe_random_member`, that would lead to the original
        // scenario)
        // So this verifies that Foca now correctly discards the
        // event instead of throwing an error:
        assert_eq!(
            Ok(()),
            foca.handle_timer(probe_random_member_timer, &mut runtime)
        );
    }

    // Simple helper to ease testing of the probe cycle
    // Yields:
    //  .0: a foca instance, ID=1, with `num_members` active members
    //  .1: the member being probed
    //  .2: the event (ProbeRandomMember) to submit in order
    //      to continue the probe cycle
    fn craft_probing_foca(
        num_members: u8,
        config: Config,
    ) -> (
        Foca<ID, BadCodec, SmallRng, NoCustomBroadcast>,
        ID,
        Timer<ID>,
    ) {
        let mut foca = Foca::new(ID::new(1), config.clone(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        assert!(num_members > 0);
        // Assume some members exist
        for smallid in 2..(num_members + 2) {
            foca.apply(Member::alive(ID::new(smallid)), &mut runtime)
                .expect("infallible");
        }

        // The runtime shoud've been instructed to schedule a
        // probe for later on
        let expected_timer = Timer::ProbeRandomMember(foca.timer_token());
        expect_scheduling!(runtime, expected_timer.clone(), config.probe_period);

        // We'll trigger it right now instead
        runtime.clear();
        assert_eq!(Ok(()), foca.handle_timer(expected_timer, &mut runtime));

        let probed = *foca.probe().target().expect("Probe should have started");

        // Now we know which member is being probed. So we can verify
        // that a ping message was sent to it:
        let (header, _updates) = decode(
            runtime
                .take_data(probed)
                .expect("Should have initiated a probe"),
        );
        assert!(matches!(header.message, Message::Ping(_)));

        // We should also have received a scheduling request
        // for when we should trigger the second stage of the
        // probe
        let send_indirect_probe = Timer::SendIndirectProbe {
            probed_id: probed,
            token: foca.timer_token(),
        };
        expect_scheduling!(runtime, send_indirect_probe.clone(), config.probe_rtt);

        (foca, probed, send_indirect_probe)
    }

    #[test]
    fn going_idle_clears_probe_state() {
        // Here we'll craft a scenario where a foca instance is in the middle
        // of a probe cycle when, for whatever reason, it learns that there
        // are no more active members in the cluster (thus going Idle)

        // A foca is probing
        let (mut foca, _probed, _send_indirect_probe) = craft_probing_foca(2, config());
        let mut runtime = AccumulatingRuntime::new();

        // Clippy gets it wrong here: can't use just the plain iterator
        // otherwise foca remains borrowed
        #[allow(clippy::needless_collect)]
        let updates = foca
            .iter_members()
            .map(Member::id)
            .cloned()
            .map(Member::down)
            .collect::<Vec<_>>();
        // But somehow all members "disappear"
        assert_eq!(Ok(()), foca.apply_many(updates.into_iter(), &mut runtime));
        // Making the instance go idle
        expect_notification!(runtime, Notification::<ID>::Idle);

        // The probe state should've been cleared now so that when the instance
        // resumes operation things are actually functional
        assert!(foca.probe().validate(), "invalid probe state");
    }

    #[test]
    fn probe_ping_ack_cycle() {
        let (mut foca, probed, send_indirect_probe) = craft_probing_foca(5, config());
        let mut runtime = AccumulatingRuntime::new();

        // Now if probed replies before the timer fires, the probe
        // should complete and the indirect probe cycle shouldn't
        // start.
        let data = (
            Header {
                src: probed,
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::Ack(foca.probe().probe_number()),
            },
            Vec::new(),
        );
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));

        assert_eq!(Ok(()), foca.handle_timer(send_indirect_probe, &mut runtime));

        assert!(
            foca.probe().succeeded(),
            "probe should have succeeded after Ack"
        );
    }

    #[test]
    fn probe_cycle_requires_correct_probe_number() {
        let (mut foca, probed, send_indirect_probe) = craft_probing_foca(5, config());
        let mut runtime = AccumulatingRuntime::new();

        let incorrect_probe_number = foca.probe().probe_number() + 1;
        assert_ne!(incorrect_probe_number, foca.probe().probe_number());

        // An Ack payload akin to the one in `tests::probe_ping_ack_cycle`,
        // but with an incorrect probe number
        let data = (
            Header {
                src: probed,
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::Ack(incorrect_probe_number),
            },
            Vec::new(),
        );
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));

        assert_eq!(Ok(()), foca.handle_timer(send_indirect_probe, &mut runtime));

        assert!(
            !foca.probe().succeeded(),
            "Ack with incorrect probe number should be discarded"
        );
    }

    #[test]
    fn probe_valid_indirect_ack_completes_succesfully() {
        // Like `probe_ping_ack_cycle` but instead of a successful
        // direct refutal via Ack, we'll stress the indirect mechanism
        // that kicks off after SendIndirectProbe is accepted
        let num_indirect_probes = config().num_indirect_probes.get();
        // We create a cluser with _more_ active members than
        // `num_indirect_probes + 1` so that we can verify that
        // we don't send more requests than the configured value.
        let (mut foca, probed, send_indirect_probe) =
            craft_probing_foca((num_indirect_probes + 2) as u8, config());
        let mut runtime = AccumulatingRuntime::new();

        // `probed` did NOT reply with an Ack before the timer
        assert_eq!(Ok(()), foca.handle_timer(send_indirect_probe, &mut runtime));

        let mut ping_req_dsts = Vec::new();
        let all_data = runtime.take_all_data();
        for (to, data) in all_data {
            let (header, _updates) = decode(data);

            if matches!(
                header.message,
                Message::PingReq {
                    target: _,
                    probe_number: _
                }
            ) {
                assert_ne!(
                    to, probed,
                    "Must not request a ping to the member being probed"
                );

                assert_ne!(
                    to,
                    foca.identity().clone(),
                    "Must not request a ping to ourselves"
                );
                ping_req_dsts.push(to);
            }
        }
        assert_eq!(num_indirect_probes, ping_req_dsts.len());
        runtime.clear();

        // Now the probe can succeed via:
        //
        //  1. A direct ack coming from `probed`
        //  2. An forwarded ack coming from ANY of the members we sent
        //
        // For this indirect scenario we'll verify that:
        //
        //  1. A ForwardedAck from a member we did NOT send a ping
        //     request to gets ignored
        //
        //  2. A well-formed ForwardedAck makes the probe succeed
        let outsider = ID::new(42);
        assert!(ping_req_dsts.iter().all(|id| id != &outsider));
        let forwarded_ack = Message::ForwardedAck {
            origin: probed,
            probe_number: foca.probe().probe_number(),
        };

        assert_eq!(
            Ok(()),
            foca.handle_data(
                &encode((
                    Header {
                        src: outsider,
                        src_incarnation: Incarnation::default(),
                        dst: ID::new(1),
                        message: forwarded_ack.clone(),
                    },
                    Vec::new(),
                )),
                &mut runtime,
            )
        );

        assert!(
            !foca.probe().succeeded(),
            "Must not accept ForwardedAck from outsider"
        );

        for src in ping_req_dsts {
            assert_eq!(
                Ok(()),
                foca.handle_data(
                    &encode((
                        Header {
                            src,
                            src_incarnation: Incarnation::default(),
                            dst: ID::new(1),
                            message: forwarded_ack.clone(),
                        },
                        Vec::new(),
                    )),
                    &mut runtime,
                )
            );

            // Only one ack is necessary for the probe to succeed
            assert!(
                foca.probe().succeeded(),
                "Probe should succeed with any expected ForwardedAck"
            );
        }
    }

    #[test]
    fn probe_receiving_ping_replies_with_ack() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let probe_number = foca.probe().probe_number();
        let data = (
            Header {
                src: ID::new(2),
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::Ping(probe_number),
            },
            Vec::new(),
        );
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));

        let (header, _updates) = decode(runtime.take_data(ID::new(2)).unwrap());
        assert_eq!(header.message, Message::Ack(probe_number));
    }

    #[test]
    fn probe_receiving_ping_req_sends_indirect_ping() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let probe_number = foca.probe().probe_number();
        let data = (
            Header {
                src: ID::new(2),
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::PingReq {
                    target: ID::new(3),
                    probe_number,
                },
            },
            Vec::new(),
        );
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));

        let (header, _updates) = decode(runtime.take_data(ID::new(3)).unwrap());
        assert_eq!(
            header.message,
            Message::IndirectPing {
                origin: ID::new(2),
                probe_number
            }
        );
    }

    #[test]
    fn probe_receiving_indirect_ping_sends_indirect_ack() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let probe_number = foca.probe().probe_number();
        let data = (
            Header {
                src: ID::new(2),
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::IndirectPing {
                    origin: ID::new(3),
                    probe_number,
                },
            },
            Vec::new(),
        );
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));

        let (header, _updates) = decode(runtime.take_data(ID::new(2)).unwrap());
        assert_eq!(
            header.message,
            Message::IndirectAck {
                target: ID::new(3),
                probe_number
            }
        );
    }

    #[test]
    fn probe_receiving_indirect_ack_sends_forwarded_ack() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let probe_number = foca.probe().probe_number();
        let data = (
            Header {
                src: ID::new(2),
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::IndirectAck {
                    target: ID::new(3),
                    probe_number,
                },
            },
            Vec::new(),
        );
        assert_eq!(Ok(()), foca.handle_data(&encode(data), &mut runtime));

        let (header, _updates) = decode(runtime.take_data(ID::new(3)).unwrap());
        assert_eq!(
            header.message,
            Message::ForwardedAck {
                origin: ID::new(2),
                probe_number
            }
        );
    }

    #[test]
    fn message_from_aware_suspect_refutes_suspicion() -> Result<()> {
        // Scenario: 3-member active cluster
        // - One of the members will suspect the other two
        // - Only one of the suspected members will learn about the suspicion
        let mut herd = {
            let mut herd = Vec::new();
            let members = [
                Member::alive(ID::new(1)),
                Member::alive(ID::new(2)),
                Member::alive(ID::new(3)),
            ];

            for member in members.iter().rev() {
                let mut foca = Foca::new(*member.id(), config(), rng(), codec());
                foca.apply_many(members.iter().cloned(), AccumulatingRuntime::new())?;
                herd.push(foca);
            }

            herd
        };

        let mut foca_one = herd.pop().unwrap();
        let mut foca_two = herd.pop().unwrap();
        let mut foca_three = herd.pop().unwrap();

        let one = *foca_one.identity();
        let two = *foca_two.identity();
        let three = *foca_three.identity();

        // foca_one starts suspecting two and three
        let mut runtime = AccumulatingRuntime::new();
        foca_one.apply(Member::suspect(two), &mut runtime)?;
        foca_one.apply(Member::suspect(three), &mut runtime)?;
        assert_eq!(2, foca_one.num_members());

        // But only foca_three learns that its being suspected
        // (Likely learned about ID=2 too, but that's irrelevant)
        foca_three.apply(Member::suspect(three), &mut runtime)?;

        // `foca_two` messages `foca_one`
        runtime.clear();
        assert_eq!(Ok(()), foca_two.announce(one, &mut runtime));
        let data = runtime
            .take_data(one)
            .expect("foca_two sending data to ID::new(1)");

        assert_eq!(Ok(()), foca_one.handle_data(&data, &mut runtime));

        // same for `foca_three`
        runtime.clear();
        assert_eq!(Ok(()), foca_three.announce(one, &mut runtime));
        let data = runtime
            .take_data(one)
            .expect("foca_three sending data to ID::new(1)");
        assert_eq!(Ok(()), foca_one.handle_data(&data, &mut runtime));

        // Now `foca_one` has received messages from both members
        // and our runtime triggered the timer to change suspect
        // member to down

        // timer event related to `foca_two`
        runtime.clear();
        assert_eq!(
            Ok(()),
            foca_one.handle_timer(
                Timer::ChangeSuspectToDown {
                    member_id: two,
                    incarnation: Incarnation::default(),
                    token: foca_one.timer_token()
                },
                &mut runtime
            )
        );
        // foca_two hasn't refuted the suspicion, so `foca_one` should
        // have marked it as down
        expect_notification!(runtime, Notification::MemberDown(two));
        assert_eq!(1, foca_one.num_members());
        assert!(
            foca_one.iter_members().all(|m| m.id() != &two),
            "foca_two shouldn't be in the member list anymore"
        );

        // But `foca_three` knew about it, and its message should've
        // been enough to remain active
        assert_eq!(
            Ok(()),
            foca_one.handle_timer(
                Timer::ChangeSuspectToDown {
                    member_id: three,
                    incarnation: Incarnation::default(),
                    token: foca_one.timer_token()
                },
                &mut runtime
            )
        );
        assert_eq!(1, foca_one.num_members());
        assert!(
            foca_one.iter_members().any(|m| m.id() == &three),
            "foca_three should have recovered"
        );

        Ok(())
    }

    #[test]
    fn leave_cluster_gossips_about_our_death() -> Result<()> {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        foca.apply(Member::alive(ID::new(2)), &mut runtime)?;

        assert_eq!(Ok(()), foca.leave_cluster(&mut runtime));

        // Since we only have ID::new(2) as an active member, we know that
        // `leave_cluster` should have sent a message to it
        let (header, updates) = decode(
            runtime
                .take_data(ID::new(2))
                .expect("No message for ID::new(2) found"),
        );
        assert_eq!(Message::Gossip, header.message);

        assert!(
            updates
                .iter()
                .any(|update| update.id() == &ID::new(1) && update.state() == State::Down),
            "Gossip message should contain an update about our exit"
        );

        Ok(())
    }

    #[test]
    fn leave_cluster_doesnt_gossip_to_duplicates() -> Result<()> {
        // We want to gossip to 5 distict members when leaving
        let config = Config {
            num_indirect_probes: NonZeroUsize::new(5).unwrap(),
            ..Config::simple()
        };

        let mut foca = Foca::new(ID::new(1), config, rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // And only have one
        foca.apply(Member::alive(ID::new(2)), &mut runtime)?;

        assert_eq!(Ok(()), foca.leave_cluster(&mut runtime));

        assert!(
            runtime.take_data(ID::new(2)).is_some(),
            "Should have one message for ID::new(2)"
        );
        assert!(
            runtime.take_data(ID::new(2)).is_none(),
            "But never more than one to the same member"
        );

        Ok(())
    }

    #[test]
    fn auto_rejoin_behaviour() {
        let mut foca = Foca::new(ID::new(1).rejoinable(), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        let updates = [
            // New known members
            Member::alive(ID::new(2)),
            Member::alive(ID::new(3)),
            Member::alive(ID::new(4)),
            // Us, being down
            Member::down(ID::new(1)),
        ];

        assert_eq!(
            Ok(()),
            foca.apply_many(updates.iter().cloned(), &mut runtime)
        );

        // Change our identity
        let expected_new_id = ID::new_with_bump(1, 1);
        assert_eq!(&expected_new_id, foca.identity());
        expect_notification!(runtime, Notification::Rejoin(expected_new_id));
        reject_notification!(runtime, Notification::<ID>::Defunct);

        // And disseminate our new identity to K members
        let to_send = runtime.take_all_data();
        assert!(to_send.into_iter().any(|(_dst, data)| {
            let (header, _updates) = decode(data);
            header.message == Message::Gossip
        }));
    }

    // TODO duplicate renew() identity no error test

    #[test]
    fn more_data_than_allowed_causes_error() {
        let config = config();
        let max_bytes = config.max_packet_size.get();

        let mut foca = Foca::new(ID::new(1), config, rng(), codec());

        let large_data = vec![42u8; max_bytes + 1];

        assert_eq!(
            Err(Error::DataTooBig),
            foca.handle_data(&large_data[..], AccumulatingRuntime::new())
        );

        assert_eq!(Err(Error::DataTooBig), foca.add_broadcast(&large_data[..]));
    }

    #[test]
    fn cant_use_broadcasts_by_default() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        assert!(foca.add_broadcast(b"foo").is_err());
    }

    #[test]
    fn trailing_data_is_error() {
        // We'll prepare some data that's actually valid
        let valid_data = encode((
            Header {
                src: ID::new(2),
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::Ping(0),
            },
            vec![Member::alive(ID::new(3)), Member::down(ID::new(4))],
        ));

        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());

        assert_eq!(
            Ok(()),
            foca.handle_data(valid_data.as_ref(), AccumulatingRuntime::new()),
            "valid_data should be valid :-)"
        );

        // Now we'll append some rubbish to it, so that everything
        // is still valid up the trash.
        let mut bad_data = Vec::from(valid_data.as_ref());

        // A single trailing byte should be enough to trigger an error
        bad_data.push(0);

        assert_eq!(
            Err(Error::MalformedPacket),
            foca.handle_data(bad_data.as_ref(), AccumulatingRuntime::new()),
        );
    }

    #[test]
    fn custom_broadcast() {
        // Here we'll do some basic testing of the custom broadcast
        // functionality.

        // This the item that gets broadcast. It's pretty useless
        // as it is: just an identifier and a version.
        #[derive(Debug)]
        struct VersionedKey {
            // A realistic broadcast would contain actual data
            // but we're not interested in the contents here,
            // just how it all behaves.
            data: [u8; 10],
        }

        impl VersionedKey {
            fn new(key: u64, version: u16) -> Self {
                let mut data = [0u8; 10];
                let mut buf = &mut data[..];
                buf.put_u64(key);
                buf.put_u16(version);
                Self { data }
            }

            fn key(&self) -> u64 {
                let mut buf = &self.data[..];
                buf.get_u64()
            }

            fn version(&self) -> u16 {
                let mut buf = &self.data[8..];
                buf.get_u16()
            }

            fn from_bytes(mut src: impl Buf) -> core::result::Result<Self, &'static str> {
                if src.remaining() < 10 {
                    Err("buffer too small")
                } else {
                    let mut data = [0u8; 10];
                    let mut buf = &mut data[..];
                    buf.put_u64(src.get_u64());
                    buf.put_u16(src.get_u16());
                    Ok(Self { data })
                }
            }
        }

        // Invalidation based on same key => higher version
        impl Invalidates for VersionedKey {
            fn invalidates(&self, other: &Self) -> bool {
                self.key() == other.key() && self.version() > other.version()
            }
        }

        impl AsRef<[u8]> for VersionedKey {
            fn as_ref(&self) -> &[u8] {
                &self.data[..]
            }
        }

        // Notice how if we don't need to cache the full broadcast here
        // if VersionKey was very large it wouldn't matter: all we care
        // about here is whether the broadcast is new information or
        // not.
        use alloc::collections::BTreeMap;
        struct Handler(BTreeMap<u64, u16>);

        impl BroadcastHandler<ID> for Handler {
            type Key = VersionedKey;

            type Error = &'static str;

            fn receive_item(
                &mut self,
                data: &[u8],
                _sender: Option<&ID>,
            ) -> core::result::Result<Option<Self::Key>, Self::Error> {
                let decoded = VersionedKey::from_bytes(data)?;

                let is_new_information = self
                    .0
                    .get(&decoded.key())
                    // If the version we know about is smaller
                    .map_or(true, |&version| version < decoded.version());

                if is_new_information {
                    self.0.insert(decoded.key(), decoded.version());
                    Ok(Some(decoded))
                } else {
                    Ok(None)
                }
            }

            fn should_add_broadcast_data(&self, member: &ID) -> bool {
                // never broadcast to member ID=3
                let blacklisted = ID::new(3);
                !blacklisted.eq(member)
            }
        }

        // Now we can get use our custom broadcasts
        let mut foca = Foca::with_custom_broadcast(
            ID::new(1),
            config(),
            rng(),
            codec(),
            Handler(BTreeMap::new()),
        );

        assert!(
            foca.add_broadcast(b"huehue").is_err(),
            "Adding garbage shouldn't work"
        );

        assert_eq!(
            Ok(true),
            foca.add_broadcast(VersionedKey::new(420, 0).as_ref()),
        );

        assert_eq!(
            1,
            foca.custom_broadcast_backlog(),
            "Adding a new custom broadcast should increase the backlog"
        );

        assert_eq!(
            Ok(true),
            foca.add_broadcast(VersionedKey::new(420, 1).as_ref()),
        );

        assert_eq!(
            1,
            foca.custom_broadcast_backlog(),
            "Receiving a new version should simply replace the existing one"
        );

        assert_eq!(
            Ok(false),
            foca.add_broadcast(VersionedKey::new(420, 1).as_ref()),
            "Adding stale/known broadcast should signal that nothing was added"
        );

        assert_eq!(1, foca.custom_broadcast_backlog(),);

        // Let's add one more custom broadcast because testing with N=1
        // is pretty lousy :-)
        assert_eq!(
            Ok(true),
            foca.add_broadcast(VersionedKey::new(710, 1).as_ref()),
        );

        assert_eq!(2, foca.custom_broadcast_backlog(),);

        // Now let's see if the custom broadcasts actually get
        // disseminated.
        let other_id = ID::new(2);
        let mut other_foca = Foca::with_custom_broadcast(
            other_id,
            config(),
            rng(),
            codec(),
            Handler(BTreeMap::new()),
        );

        // Teach the original foca about this new `other_foca`
        let mut runtime = AccumulatingRuntime::new();
        assert_eq!(Ok(()), foca.apply(Member::alive(other_id), &mut runtime));

        // Now foca will talk to other_foca. The encoded data
        // should contain our custom broadcasts.
        assert_eq!(Ok(()), foca.gossip(&mut runtime));
        let data_for_another_foca = runtime
            .take_data(other_id)
            .expect("foca only knows about other_foca");

        assert_eq!(
            0,
            other_foca.custom_broadcast_backlog(),
            "other_foca custom broadcast backlog should start empty"
        );

        assert_eq!(
            Ok(()),
            other_foca.handle_data(&data_for_another_foca, &mut runtime)
        );

        assert_eq!(
            2,
            other_foca.custom_broadcast_backlog(),
            "Should have received two new custom broadcasts"
        );

        drop(data_for_another_foca);

        // Now we'll talk to member ID=3, but since our handler
        // has a custom implementation for `should_add_broadcast_data`
        // that yields false for this identity, we want
        // no _broadcast_ data to be sent to them, everything else
        // should flow normally.
        let isolated_member = ID::new(3);
        let mut isolated_foca = Foca::with_custom_broadcast(
            isolated_member,
            config(),
            rng(),
            codec(),
            Handler(BTreeMap::new()),
        );

        runtime.clear();
        // Add the isolated member to the cluster
        assert_eq!(
            Ok(()),
            foca.apply(Member::alive(isolated_member), &mut runtime)
        );

        // Since there are just a few members, calling gossip
        // will definitely choose this member
        assert_eq!(Ok(()), foca.gossip(&mut runtime));
        let data_for_isolated_member = runtime
            .take_data(isolated_member)
            .expect("config has num_indirect_probes > 1");

        assert_eq!(
            Ok(()),
            isolated_foca.handle_data(&data_for_isolated_member, &mut runtime)
        );

        assert_eq!(
            0,
            isolated_foca.custom_broadcast_backlog(),
            "Should not have received any custom broadcast"
        );

        assert_eq!(
            2,
            isolated_foca.num_members(),
            "But cluster updates should have arrived normally"
        );

        runtime.clear();
        // `foca` (ID=1) is presently seeing two members:
        //  - ID=2, allowed to receive broadcasts
        //  - ID=3, which never receives broadcasts
        //  So if we call broadcast()
        assert_eq!(Ok(()), foca.broadcast(&mut runtime));

        // We NO message to the isolated member
        assert!(runtime.take_data(isolated_member).is_none());

        // And one Broadcast message to ID=2
        let broadcast_message = runtime
            .take_data(other_id)
            .expect("shoud've sent a message to ID=2");

        let header = codec()
            .decode_header(&broadcast_message[..])
            .expect("valid payload");

        assert_eq!(
            header.message,
            Message::Broadcast,
            "broadcast() should trigger Broadcast messages"
        );

        // And, of course, ID=2 should be able to handle
        // such message
        assert_eq!(
            Ok(()),
            other_foca.handle_data(&broadcast_message, &mut runtime)
        );
    }

    #[test]
    fn can_recover_from_incomplete_probe_cycle() {
        // Here we get a foca in the middle of a probe cycle. The correct
        // sequencing should submit `_send_indirect_probe`
        let (mut foca, _probed, _send_indirect_probe) = craft_probing_foca(2, config());
        let mut runtime = AccumulatingRuntime::new();
        let old_probeno = foca.probe().probe_number();

        // ... but we'll manually craft a ProbeRandomMember event instead
        // to trigger the validation failure
        assert_eq!(
            Err(Error::IncompleteProbeCycle),
            foca.handle_timer(Timer::ProbeRandomMember(foca.timer_token()), &mut runtime)
        );

        // And since there are still active members in the cluster, a *new*
        // probe should've started
        assert_ne!(foca.probe().probe_number(), old_probeno);

        // Which means we should've scheduled a new probe to start
        assert!(
            runtime
                .find_scheduling(|t| matches!(t, Timer::ProbeRandomMember(_)))
                .is_some(),
            "didn't submit a new probe event"
        );

        // And the deadline for starting the indirect probe cycle
        assert!(
            runtime
                .find_scheduling(|t| matches!(
                    t,
                    Timer::SendIndirectProbe {
                        probed_id: _,
                        token: _,
                    }
                ))
                .is_some(),
            "didn't submit a new probe event"
        );
    }

    #[test]
    fn declaring_a_member_as_down_notifies_them() {
        let config = {
            let mut c = Config::simple();
            c.notify_down_members = true;
            c
        };

        let (mut foca, probed, send_indirect_probe) = craft_probing_foca(2, config);
        let mut runtime = AccumulatingRuntime::new();

        // `probed` did NOT reply with an Ack before the timer
        assert_eq!(Ok(()), foca.handle_timer(send_indirect_probe, &mut runtime));
        // ... and nothing happens for the indirect cycle

        runtime.clear();
        // So by the time the ChangeSuspectToDown timer fires
        assert_eq!(
            Ok(()),
            foca.handle_timer(
                Timer::ChangeSuspectToDown {
                    member_id: probed,
                    incarnation: Incarnation::default(),
                    token: foca.timer_token()
                },
                &mut runtime
            )
        );

        // The runtime should be instructed to send a TurnUndead message to `probed`
        expect_message!(runtime, probed, Message::<ID>::TurnUndead);
    }

    #[test]
    fn message_from_down_member_is_replied_with_turn_undead() {
        let config = {
            let mut c = config();
            c.notify_down_members = true;
            c
        };
        let mut runtime = AccumulatingRuntime::new();

        // We have a simple foca instance
        let mut foca = Foca::new(ID::new(1), config, rng(), codec());
        let down_id = ID::new(2);
        // That knows that ID=2 is down
        assert_eq!(Ok(()), foca.apply(Member::down(down_id), &mut runtime));

        // And we have a message from member ID=2 to ID=1
        let header = Header {
            src: down_id,
            src_incarnation: 1,
            dst: ID::new(1),
            message: Message::Announce,
        };

        let mut msg = Vec::new();
        codec()
            .encode_header(&header, &mut msg)
            .expect("codec works fine");

        // When foca receives such message
        assert_eq!(Ok(()), foca.handle_data(&msg[..], &mut runtime));

        // It should send a message to ID=2 notifying it
        expect_message!(runtime, down_id, Message::<ID>::TurnUndead);
    }

    // There are multiple "do this thing periodically" settings. This
    // helps test those. Takes:
    // - something that knows which configuration to set
    // - something that knows which event should be sent
    // - the message that should be sent
    fn check_periodic_behaviour<F, G>(config_setter: F, mut event_maker: G, message: Message<ID>)
    where
        F: Fn(&mut Config, config::PeriodicParams),
        G: FnMut(TimerToken) -> Timer<ID>,
    {
        let frequency = Duration::from_millis(500);
        let num_members = NonZeroUsize::new(2).unwrap();
        let params = config::PeriodicParams {
            frequency,
            num_members,
        };
        let mut config = config();

        // A foca with the given periodic config
        config_setter(&mut config, params);

        let mut foca = Foca::new(ID::new(1), config, rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // When it becomes active (i.e.: has at least one active member)
        assert_eq!(Ok(()), foca.apply(Member::alive(ID::new(2)), &mut runtime));
        assert_eq!(Ok(()), foca.apply(Member::alive(ID::new(3)), &mut runtime));

        // Should schedule the given event
        expect_scheduling!(runtime, event_maker(foca.timer_token()), frequency);

        runtime.clear();
        // After the event fires
        assert_eq!(
            Ok(()),
            foca.handle_timer(event_maker(foca.timer_token()), &mut runtime)
        );

        // It should've scheduled the event again
        expect_scheduling!(runtime, event_maker(foca.timer_token()), frequency);

        // And sent the message to `num_members` random members
        // (since num_members=2 and this instance only knows about two, we know
        // which should've been picked)
        expect_message!(runtime, ID::new(2), message);
        expect_message!(runtime, ID::new(3), message);
    }

    #[test]
    fn periodic_gossip_behaviour() {
        check_periodic_behaviour(
            |c: &mut Config, p: config::PeriodicParams| {
                c.periodic_gossip = Some(p);
            },
            |t: TimerToken| -> Timer<ID> { Timer::PeriodicGossip(t) },
            Message::Gossip,
        );
    }

    #[test]
    fn periodic_announce_behaviour() {
        check_periodic_behaviour(
            |c: &mut Config, p: config::PeriodicParams| {
                c.periodic_announce = Some(p);
            },
            |t: TimerToken| -> Timer<ID> { Timer::PeriodicAnnounce(t) },
            Message::Announce,
        );
    }

    #[test]
    fn periodic_announce_cannot_be_enabled_at_runtime() {
        let mut c = config();
        assert!(c.periodic_announce.is_none());

        // A foca instance that's running without periodic announce
        let mut foca = Foca::new(ID::new(1), c.clone(), rng(), codec());

        c.periodic_announce = Some(config::PeriodicParams {
            frequency: Duration::from_secs(5),
            num_members: NonZeroUsize::new(1).unwrap(),
        });

        // Must not be able to enable it during runtime
        assert_eq!(Err(Error::InvalidConfig), foca.set_config(c.clone()));

        // However, a foca that starts with periodic announce enabled
        let mut foca = Foca::new(ID::new(1), c, rng(), codec());

        // Is able to turn it off
        assert_eq!(Ok(()), foca.set_config(config()));
    }

    #[test]
    fn periodic_gossip_cannot_be_enabled_at_runtime() {
        let mut c = config();
        assert!(c.periodic_gossip.is_none());

        // A foca instance that's running without periodic gossip
        let mut foca = Foca::new(ID::new(1), c.clone(), rng(), codec());

        c.periodic_gossip = Some(config::PeriodicParams {
            frequency: Duration::from_secs(5),
            num_members: NonZeroUsize::new(1).unwrap(),
        });

        // Must not be able to enable it during runtime
        assert_eq!(Err(Error::InvalidConfig), foca.set_config(c.clone()));

        // However, a foca that starts with periodic gossip enabled
        let mut foca = Foca::new(ID::new(1), c, rng(), codec());

        // Is able to turn it off
        assert_eq!(Ok(()), foca.set_config(config()));
    }

    #[test]
    fn cannot_learn_about_own_previous_identity() {
        // We have an identity
        let id = ID::new(1).rejoinable();
        // And it's renewed version
        let renewed = id.renew().unwrap();

        // So that they are not the same
        assert_ne!(id, renewed);
        // But have the same prefix
        assert_eq!(id.addr(), renewed.addr());

        // If we have an instance running with the renewed
        // id as its identity
        let mut foca = Foca::new(renewed, config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // Learning anything about its previous identity
        assert_eq!(
            Ok(()),
            foca.apply_many(core::iter::once(Member::alive(id)), &mut runtime)
        );

        // shouldn't change the cluster state
        assert_eq!(
            0,
            foca.num_members(),
            "shouldn't have considered a previous identity as a new member"
        );
    }

    // assuming fixed-length identity
    fn encoded_feed_header_len() -> usize {
        let header = Header {
            src: ID::new(1),
            src_incarnation: 0,
            dst: ID::new(3),
            message: Message::Feed,
        };

        let mut msg = Vec::new();
        codec()
            .encode_header(&header, &mut msg)
            .expect("codec works fine");
        msg.len()
    }

    #[test]
    fn feed_does_not_contain_trailing_jumk() {
        let mut config = config();
        // we want a max packet size that can definitely fit
        // feed header, a u16 (num_updates) and not enough
        // to fit the rest of the message
        // This way we can exercise what happens when encoding
        // a feed message goes above the max length
        config.max_packet_size = NonZeroUsize::new(
            encoded_feed_header_len()
            // num_updates
            + 2
            // so there's SOME extra space to try and encode a Member
            // but not enough to fit all the metadata
                +2,
        )
        .expect("non-zero");

        // So now we will craft a scenario where one instance
        // announces to another and then verify that we can handle
        // the reply with no errors
        let mut foca_one = Foca::new(ID::new(1), config.clone(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // let's assume that foca_one knows about another member, ID=3'
        // so that the feed reply contains at least one member
        assert_eq!(
            Ok(()),
            foca_one.apply(Member::alive(ID::new(3)), &mut runtime)
        );

        // ID=2 announces to our instance
        let msg = encode((
            Header {
                src: ID::new(2),
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::Announce,
            },
            Vec::default(),
        ));
        assert_eq!(Ok(()), foca_one.handle_data(&msg, &mut runtime));

        // now the runtime should've been instructed to send a feed to
        // ID=2
        let data = runtime
            .take_data(ID::new(2))
            .expect("foca_one reply for foca_two");

        let mut foca_two = Foca::new(ID::new(2), config, rng(), codec());
        // and foca_two should be able to read it just fine
        // (originally would fail with BroadcastsDisabledError)
        assert_eq!(Ok(()), foca_two.handle_data(&data, &mut runtime));
    }

    #[test]
    fn feed_fits_as_many_as_it_can() {
        // We prepare a foca cluster with a bunch of live members
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();
        let cluster = (2u8..=u8::MAX)
            .map(|id| Member::alive(ID::new(id)))
            .collect::<Vec<_>>();

        assert_eq!(Ok(()), foca.apply_many(cluster.into_iter(), &mut runtime));
        assert_eq!(foca.num_members(), usize::from(u8::MAX - 1));

        // So when we send it an announce message
        let msg = encode((
            Header {
                src: ID::new(2),
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::Announce,
            },
            Vec::default(),
        ));
        assert_eq!(Ok(()), foca.handle_data(&msg, &mut runtime));

        // We get a feed back with a large (>10, the min from estimation)
        // number of members
        let (header, feed_data) = decode(
            runtime
                .take_data(ID::new(2))
                .expect("foca_one reply for foca_two"),
        );
        assert_eq!(Message::Feed, header.message);

        assert!(feed_data.len() > 200);
    }

    #[test]
    fn reacts_to_turnundead_even_if_sender_is_down() {
        // Given a foca ID=1
        let id_one = ID::new(1).rejoinable();
        // configured to notify members of their down state
        let config = {
            let mut c = config();
            c.notify_down_members = true;
            c
        };
        let mut foca = Foca::new(id_one, config, rng(), codec());
        let mut runtime = AccumulatingRuntime::new();
        // that thinks ID=2 is down;
        assert_eq!(Ok(()), foca.apply(Member::down(ID::new(2)), &mut runtime));

        // And a TurnUndead message from ID=2
        let msg = encode((
            Header {
                src: ID::new(2),
                src_incarnation: Incarnation::default(),
                dst: ID::new(1),
                message: Message::TurnUndead,
            },
            Vec::default(),
        ));

        // When foca handles the message
        assert_eq!(Ok(()), foca.handle_data(&msg, &mut runtime));

        // It should change its identity
        assert_ne!(foca.identity(), &id_one);

        // And reply to ID=2 with a TurnUndead saying that it's down
        let (header, _feed) = decode(
            runtime
                .take_data(ID::new(2))
                .expect("foca_one reply for foca_two"),
        );
        assert_eq!(Message::TurnUndead, header.message);
        assert_ne!(
            id_one, header.src,
            "message should be crafted with the new/renewed id"
        );
    }

    #[test]
    fn handles_member_addr_conflict() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // Given a known member ID=2,0
        let original = ID::new_with_bump(2, 0);
        assert_eq!(Ok(()), foca.apply(Member::alive(original), &mut runtime));
        assert_eq!(1, foca.num_members());

        // When foca learns about a new member with same address ID=2,1
        // that wins its conflict resolution round
        let conflicted = ID::new_with_bump(2, 1);
        assert_eq!(original.addr(), conflicted.addr());
        assert!(conflicted.win_addr_conflict(&original));
        assert_eq!(Ok(()), foca.apply(Member::alive(conflicted), &mut runtime));

        // It should replace the original state
        assert_eq!(1, foca.num_members());
        assert_eq!(
            foca.iter_members().next().unwrap(),
            &Member::alive(conflicted)
        );

        // Conversely, if it learns about a member with same address
        // that loses the conflict
        assert!(!original.win_addr_conflict(&conflicted));
        // nothing changes
        for m in [
            Member::alive(original),
            Member::suspect(original),
            Member::down(original),
        ] {
            assert_eq!(Ok(()), foca.apply(m, &mut runtime));
            assert_eq!(1, foca.num_members());
            assert_eq!(
                foca.iter_members().next().unwrap(),
                &Member::alive(conflicted)
            );
        }
    }

    #[test]
    fn does_not_mark_renewed_identity_as_down() {
        let (mut foca, probed, send_indirect_probe) = craft_probing_foca(1, config());
        let mut runtime = AccumulatingRuntime::new();
        assert_eq!(1, foca.num_members());

        // `probed` did NOT reply with an Ack before the timer
        assert_eq!(Ok(()), foca.handle_timer(send_indirect_probe, &mut runtime));

        // meanwhile, the member rejoined (same addr, but not the same id)
        let bumped = probed.bump();
        assert_ne!(probed, bumped);
        assert_eq!(probed.addr(), bumped.addr());
        assert_eq!(Ok(()), foca.apply(Member::alive(bumped), &mut runtime));
        assert_eq!(1, foca.num_members());

        runtime.clear();
        // So by the time the ChangeSuspectToDown timer fires
        assert_eq!(
            Ok(()),
            foca.handle_timer(
                Timer::ChangeSuspectToDown {
                    member_id: probed,
                    incarnation: Incarnation::default(),
                    token: foca.timer_token()
                },
                &mut runtime
            )
        );

        // the member is NOT marked as down
        assert_eq!(1, foca.num_members());
        assert_eq!(foca.iter_members().next().unwrap(), &Member::alive(bumped));
    }

    #[test]
    fn notifies_on_conflict_resolution() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = AccumulatingRuntime::new();

        // Given a known member
        let member = ID::new(2).rejoinable();
        assert_eq!(Ok(()), foca.apply(Member::alive(member), &mut runtime));
        assert_eq!(1, foca.num_members());

        runtime.clear();

        // Learning about its renewd id
        let renewed = member.renew().expect("bumped");
        assert_eq!(Ok(()), foca.apply(Member::alive(renewed), &mut runtime));
        assert_eq!(1, foca.num_members());
        // Should notify the runtime about the change
        expect_notification!(runtime, Notification::Rename(member, renewed));
        // But no MemberUp notification should be fired, since
        // previous addr was already active
        reject_notification!(runtime, Notification::MemberUp(member));
        reject_notification!(runtime, Notification::MemberUp(renewed));

        runtime.clear();

        // But if the renewed id is not active
        let inactive = renewed.renew().expect("bumped");
        assert_eq!(Ok(()), foca.apply(Member::down(inactive), &mut runtime));
        assert_eq!(0, foca.num_members());
        // We get notified of the rename
        expect_notification!(runtime, Notification::Rename(renewed, inactive));
        // AND about the member going down with its new identity
        expect_notification!(runtime, Notification::MemberDown(inactive));
        // but nothing about the (now overriden, forgotten) previous one
        reject_notification!(runtime, Notification::MemberDown(renewed));

        runtime.clear();

        // The inverse behaves similarly:
        // Learning about a renewed active
        let active = inactive.renew().expect("bumped");
        assert_eq!(Ok(()), foca.apply(Member::suspect(active), &mut runtime));
        assert_eq!(1, foca.num_members());
        // Should notify about the rename
        expect_notification!(runtime, Notification::Rename(inactive, active));
        // And about the member being active
        expect_notification!(runtime, Notification::MemberUp(active));

        runtime.clear();
        // And if it learns about the previous ids again, regardless
        // of their state, nothing happens
        for m in [member, renewed, inactive] {
            assert_eq!(Ok(()), foca.apply(Member::alive(m), &mut runtime));
            assert_eq!(1, foca.num_members());
            assert!(runtime.is_empty());

            assert_eq!(Ok(()), foca.apply(Member::down(m), &mut runtime));
            assert_eq!(1, foca.num_members());
            assert!(runtime.is_empty());
        }
    }
}
