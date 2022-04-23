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
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

extern crate alloc;
use alloc::vec::Vec;

#[cfg(feature = "std")]
extern crate std;

use core::{cmp::Ordering, convert::TryFrom, fmt, mem};

use bytes::{Buf, BufMut, Bytes, BytesMut};
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
    config::Config,
    error::Error,
    identity::Identity,
    member::{Incarnation, Member, State},
    payload::{Header, Message, ProbeNumber},
    runtime::{Notification, Runtime, Timer, TimerToken},
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
pub struct Foca<T, C, RNG, B: BroadcastHandler<T>> {
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

    // Since we emit data via `Runtime::send_to`, this could
    // easily be a Vec, but `BytesMut::limit` is quite handy
    send_buf: BytesMut,

    // Holds (serialized) cluster updates, which may live for a
    // while until they get disseminated `Config::max_transmissions`
    // times or replaced by fresher updates.
    updates_buf: BytesMut,
    updates: Broadcasts<ClusterUpdate<T>>,

    broadcast_handler: B,
    custom_broadcasts: Broadcasts<B::Broadcast>,
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
impl<T: Identity, C, RNG, B: BroadcastHandler<T>> fmt::Debug for Foca<T, C, RNG, B> {
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
            send_buf: BytesMut::with_capacity(max_bytes),
            custom_broadcasts: Broadcasts::new(),
            updates_buf: BytesMut::new(),
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
    #[cfg_attr(feature = "tracing", tracing::instrument)]
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
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(runtime)))]
    pub fn change_identity(&mut self, new_id: T, runtime: impl Runtime<T>) -> Result<()> {
        if self.identity == new_id {
            Err(Error::SameIdentity)
        } else {
            let previous_is_down = self.connection_state == ConnectionState::Undead;
            let previous_id = mem::replace(&mut self.identity, new_id);

            self.reset();

            // If our previous identity wasn't known as Down already,
            // we'll declare it ourselves
            if !previous_is_down {
                let data = self.serialize_member(Member::down(previous_id.clone()))?;
                self.updates.add_or_replace(
                    ClusterUpdate {
                        member_id: previous_id,
                        data,
                    },
                    self.config.max_transmissions.get().into(),
                );
            }

            self.gossip(runtime)?;

            Ok(())
        }
    }

    /// Iterate over the currently active cluster members.
    pub fn iter_members(&self) -> impl Iterator<Item = &T> {
        self.members.iter_active().map(|member| member.id())
    }

    /// Returns the number of active members in the cluster.
    ///
    /// May only be used as a bound for [`Foca::iter_members`] if no
    /// Foca method that takes `&mut self` is called in-between.
    pub fn num_members(&self) -> usize {
        self.members.num_active()
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
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(runtime)))]
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
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(runtime)))]
    pub fn gossip(&mut self, mut runtime: impl Runtime<T>) -> Result<()> {
        self.member_buf.clear();
        self.members.choose_active_members(
            self.config.num_indirect_probes.get(),
            &mut self.member_buf,
            &mut self.rng,
            |_| true,
        );

        while let Some(chosen) = self.member_buf.pop() {
            self.send_message(chosen.into_identity(), Message::Gossip, &mut runtime)?;
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
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(runtime)))]
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
    /// If there are active members, we select a few are selected
    /// and notify them of our exit so that the cluster learns
    /// about it quickly.
    ///
    /// This is the cleanest way to terminate a running Foca.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(runtime)))]
    pub fn leave_cluster(mut self, mut runtime: impl Runtime<T>) -> Result<()> {
        let data = self.serialize_member(Member::down(self.identity().clone()))?;
        self.updates.add_or_replace(
            ClusterUpdate {
                member_id: self.identity().clone(),
                data,
            },
            self.config.max_transmissions.get().into(),
        );

        self.gossip(&mut runtime)?;

        // We could try to be smart here and only go defunct if there
        // are active members, but I'd rather have consistent behaviour.
        self.become_undead(&mut runtime);

        Ok(())
    }

    /// Register some data to be broadcast along with Foca messages.
    ///
    /// Calls into this instance's BroadcastHandler and reacts accordingly.
    pub fn add_broadcast(&mut self, data: &[u8]) -> Result<()> {
        // NOTE: Receiving B::Broadcast instead of a byte slice would make it
        //       look more convenient, however it gets in the way when
        //       implementing more ergonomic interfaces (say: an async driver)
        //       it forces everything to know the exact concrete type of
        //       the broadcast. So... maybe revisit this decision later?
        #[cfg(feature = "tracing")]
        let span = tracing::span!(tracing::Level::DEBUG, "add_broadcast", len = data.len(),);
        #[cfg(feature = "tracing")]
        let _guard = span.enter();

        // Not considering the whole header
        if data.len() > self.config.max_packet_size.get() {
            return Err(Error::DataTooBig);
        }

        if let Some(broadcast) = self
            .broadcast_handler
            .receive_item(data)
            .map_err(anyhow::Error::msg)
            .map_err(Error::CustomBroadcast)?
        {
            #[cfg(feature = "tracing")]
            tracing::debug!("new item received");
            self.custom_broadcasts
                .add_or_replace(broadcast, self.config.max_transmissions.get().into());
        }

        Ok(())
    }

    /// React to a previously scheduled timer event.
    ///
    /// See [`Runtime::submit_after`].
    pub fn handle_timer(&mut self, event: Timer<T>, mut runtime: impl Runtime<T>) -> Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::span!(
            tracing::Level::DEBUG,
            "handle_timer",
            self.timer_token,
            ?event,
        );
        #[cfg(feature = "tracing")]
        let _guard = span.enter();

        match event {
            Timer::SendIndirectProbe { probed_id, token } => {
                // Changing identities in the middle of the probe cycle may
                // naturally lead to this.
                if token != self.timer_token {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("Invalid timer token");
                    return Ok(());
                }

                // Bookkeeping: This is how we verify that the probe code
                // is running correctly. If we reach the end of the
                // probe and this hasn't happened, we know something is
                // wrong.
                self.probe.mark_indirect_probe_stage_reached();

                if !self.probe.is_probing(&probed_id) {
                    #[cfg(feature = "tracing")]
                    tracing::warn!("Member not being probed");
                    return Ok(());
                }

                if self.probe.succeeded() {
                    // We received an Ack already, nothing else to do
                    return Ok(());
                }

                self.member_buf.clear();
                self.members.choose_active_members(
                    self.config.num_indirect_probes.get(),
                    &mut self.member_buf,
                    &mut self.rng,
                    |candidate| Some(candidate) != self.probe.target(),
                );

                #[cfg(feature = "tracing")]
                tracing::debug!("Indirect Probe Cycle started");

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
                    let as_down = Member::new(member_id, incarnation, State::Down);
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
                        self.handle_apply_summary(&summary, as_down, &mut runtime)?;
                        // Member went down we might need to adjust our internal state
                        self.adjust_connection_state(runtime);
                    } else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("Member not found");
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
                    tracing::trace!("Member removed");
                } else {
                    #[cfg(feature = "tracing")]
                    tracing::trace!("Member not found / not down");
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

    /// Repports the current length of the custom broadcast queue.
    ///
    /// Custom broadcasts are transmitted [`Config::max_transmissions`]
    /// times at most or until they get invalidated by another custom
    /// broadcast.
    pub fn custom_broadcast_backlog(&self) -> usize {
        self.custom_broadcasts.len()
    }

    /// Handle data received from the network.
    ///
    /// Data larger than the configured limit will be rejected. Errors are
    /// expected if you're receiving arbitrary data (which very likely if
    /// you are listening to a socket address).
    pub fn handle_data(&mut self, mut data: &[u8], mut runtime: impl Runtime<T>) -> Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::span!(
            tracing::Level::DEBUG,
            "handle_data",
            data_len = data.len(),
            src = tracing::field::Empty,
            message = tracing::field::Empty,
        );
        #[cfg(feature = "tracing")]
        let _guard = span.enter();

        if data.remaining() > self.config.max_packet_size.get() {
            return Err(Error::DataTooBig);
        }

        let header = self
            .codec
            .decode_header(&mut data)
            .map_err(anyhow::Error::msg)
            .map_err(Error::Decode)?;

        #[cfg(feature = "tracing")]
        span.record("src", &tracing::field::debug(&header.src));
        #[cfg(feature = "tracing")]
        span.record("message", &tracing::field::debug(&header.message));

        let remaining = data.remaining();
        // A single trailing byte or a Announce payload with _any_
        // data is bad
        if remaining == 1 || (header.message == Message::Announce && remaining > 0) {
            return Err(Error::MalformedPacket);
        }

        if !self.accept_payload(&header) {
            #[cfg(feature = "tracing")]
            tracing::debug!("payload rejected");
            return Ok(());
        }

        // We can skip this buffering is we assume that reaching here
        // means the packet is valid. But that doesn't seem like a very
        // good idea...
        self.member_buf.clear();
        if remaining >= 2 && header.message != Message::Broadcast {
            let num_updates = data.get_u16();
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

        if src == self.identity {
            return Err(Error::DataFromOurselves);
        }

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
            tracing::debug!("Discarded: Inactive sender");

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
        debug_assert!(
            self.member_buf.is_empty(),
            "member_buf modified while taken"
        );
        self.member_buf = updates;

        // Right now there might still be some data left to read in the
        // buffer (custom broadcasts).
        // We choose to defer handling them until after we're done
        // with the core of the protocol.

        // If we're not connected (anymore), we can't react to a message
        // So we just finish consuming the data
        if self.connection_state != ConnectionState::Connected {
            return self.handle_custom_broadcasts(data);
        }

        match message {
            Message::Ping(probe_number) => {
                self.send_message(src, Message::Ack(probe_number), runtime)?;
            }
            Message::Ack(probe_number) => {
                #[cfg_attr(not(feature = "tracing"), allow(clippy::if_same_then_else))]
                if self.probe.receive_ack(&src, probe_number) {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("Probe success");
                } else {
                    // May be triggered by a member that slows down (say, you ^Z
                    // the proccess and `fg` back after a while).
                    // Might be interesting to keep an eye on.
                    #[cfg(feature = "tracing")]
                    tracing::warn!("Ack from unexpected member");
                }
            }
            Message::PingReq {
                target,
                probe_number,
            } => {
                if target == self.identity {
                    return Err(Error::IndirectForOurselves);
                } else {
                    self.send_message(
                        target,
                        Message::IndirectPing {
                            origin: src,
                            probe_number,
                        },
                        runtime,
                    )?;
                }
            }
            Message::IndirectPing {
                origin,
                probe_number,
            } => {
                if origin == self.identity {
                    return Err(Error::IndirectForOurselves);
                } else {
                    self.send_message(
                        src,
                        Message::IndirectAck {
                            target: origin,
                            probe_number,
                        },
                        runtime,
                    )?;
                }
            }
            Message::IndirectAck {
                target,
                probe_number,
            } => {
                if target == self.identity {
                    return Err(Error::IndirectForOurselves);
                } else {
                    self.send_message(
                        target,
                        Message::ForwardedAck {
                            origin: src,
                            probe_number,
                        },
                        runtime,
                    )?;
                }
            }
            Message::ForwardedAck {
                origin,
                probe_number,
            } =>
            {
                #[cfg_attr(not(feature = "tracing"), allow(clippy::if_same_then_else))]
                if origin == self.identity {
                    return Err(Error::IndirectForOurselves);
                } else if self.probe.receive_indirect_ack(&src, probe_number) {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("Indirect probe success");
                } else {
                    #[cfg(feature = "tracing")]
                    tracing::warn!("Unexpected ForwardedAck sender");
                }
            }
            Message::Announce => self.send_message(src, Message::Feed, runtime)?,
            // Nothing to do. These messages do not expect any reply
            Message::Gossip | Message::Feed | Message::Broadcast => {}
        };

        self.handle_custom_broadcasts(data)
    }

    fn serialize_member(&mut self, member: Member<T>) -> Result<Bytes> {
        let mut buf = self.updates_buf.split();
        self.codec
            .encode_member(&member, &mut buf)
            .map_err(anyhow::Error::msg)
            .map_err(Error::Encode)?;

        Ok(buf.freeze())
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
        if !self.probe.validate() {
            return Err(Error::IncompleteProbeCycle);
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
            //  4. The member doesn't exist anymore, which shouldn't actually
            //     happen...?
            let as_suspect = Member::new(failed.id().clone(), failed.incarnation(), State::Suspect);
            if let Some(summary) = self
                .members
                .apply_existing_if(as_suspect.clone(), |_member| true)
            {
                self.handle_apply_summary(&summary, as_suspect, &mut runtime)?;

                // Now we ensure we change the member to Down if it
                // isn't already inactive
                if summary.is_active_now {
                    runtime.submit_after(
                        Timer::ChangeSuspectToDown {
                            member_id: failed.id().clone(),
                            incarnation: failed.incarnation(),
                            token: self.timer_token,
                        },
                        self.config.suspect_to_down_after,
                    );
                }
            } else {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    failed = ?failed.id(),
                    "Member failed probe but doesn't exist"
                );
            }
        }

        if let Some(member) = self.members.next(&mut self.rng) {
            let member_id = member.id().clone();
            let probe_number = self.probe.start(member.clone());

            #[cfg(feature = "tracing")]
            tracing::debug!("Probe start");

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
            tracing::error!("Expected to find an active member to probe");
        }

        runtime.submit_after(
            Timer::ProbeRandomMember(self.timer_token),
            self.config.probe_period,
        );

        Ok(())
    }

    // shortcut for apply + handle
    fn apply_update(&mut self, update: Member<T>, runtime: impl Runtime<T>) -> Result<bool> {
        debug_assert_ne!(&self.identity, update.id());
        let summary = self.members.apply(update.clone(), &mut self.rng);
        self.handle_apply_summary(&summary, update, runtime)?;

        Ok(summary.is_active_now)
    }

    fn handle_apply_summary(
        &mut self,
        summary: &ApplySummary,
        update: Member<T>,
        mut runtime: impl Runtime<T>,
    ) -> Result<()> {
        let id = update.id().clone();

        if summary.apply_successful {
            // Cluster state changed, start broadcasting it
            let data = self.serialize_member(update)?;
            self.updates.add_or_replace(
                ClusterUpdate {
                    member_id: id.clone(),
                    data,
                },
                self.config.max_transmissions.get().into(),
            );

            // Down is a terminal state, so set up a handler for removing
            // the member so that it may rejoin later
            if !summary.is_active_now {
                runtime.submit_after(Timer::RemoveDown(id.clone()), self.config.remove_down_after);
            }
        }

        if summary.changed_active_set {
            if summary.is_active_now {
                runtime.notify(Notification::MemberUp(id));
            } else {
                runtime.notify(Notification::MemberDown(id));
            }
        }

        Ok(())
    }

    fn handle_custom_broadcasts(&mut self, mut data: impl Buf) -> Result<()> {
        while data.has_remaining() {
            if let Some(broadcast) = self
                .broadcast_handler
                .receive_item(&mut data)
                .map_err(anyhow::Error::msg)
                .map_err(Error::CustomBroadcast)?
            {
                self.custom_broadcasts
                    .add_or_replace(broadcast, self.config.max_transmissions.get().into());
            }
        }

        Ok(())
    }

    fn become_disconnected(&mut self, mut runtime: impl Runtime<T>) {
        // We reached zero active members, so we're offline
        debug_assert_eq!(0, self.num_members());
        self.connection_state = ConnectionState::Disconnected;

        #[cfg(feature = "tracing")]
        tracing::debug!("now idle");

        // Ignore every timer event we sent up until this point.
        // This is to stop the probe cycle and prevent members from
        // being switched the Down state since we have little
        // confidence about our own state at this point.
        self.timer_token = self.timer_token.wrapping_add(1);

        runtime.notify(Notification::Idle);
    }

    fn become_undead(&mut self, mut runtime: impl Runtime<T>) {
        self.connection_state = ConnectionState::Undead;

        #[cfg(feature = "tracing")]
        tracing::debug!("now defunct");

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

        #[cfg(feature = "tracing")]
        tracing::debug!("now active");

        // We have at least one active member, so we can start
        // probing
        runtime.submit_after(
            Timer::ProbeRandomMember(self.timer_token),
            self.config.probe_period,
        );

        runtime.notify(Notification::Active);
    }

    fn send_message(
        &mut self,
        dst: T,
        message: Message<T>,
        mut runtime: impl Runtime<T>,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::span!(
            tracing::Level::DEBUG,
            "send_message",
            ?dst,
            ?message,
            num_updates = tracing::field::Empty,
            num_broadcasts = tracing::field::Empty,
            len = tracing::field::Empty,
        );
        #[cfg(feature = "tracing")]
        let _guard = span.enter();

        let header = Header {
            src: self.identity.clone(),
            src_incarnation: self.incarnation,
            dst: dst.clone(),
            message,
        };

        let mut buf = self
            .send_buf
            .split()
            .limit(self.config.max_packet_size.get());

        self.codec
            .encode_header(&header, &mut buf)
            .map_err(anyhow::Error::msg)
            .map_err(Error::Encode)?;

        let (needs_piggyback, only_active_members) = match header.message {
            // Announce packets contain nothing but the header
            Message::Announce => (false, false),
            // Feed packets stuff active members at the tail
            Message::Feed => (true, true),
            // Broadcast packets stuffs only custom broadcasts
            Message::Broadcast => (false, false),
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
                for member in self
                    .members
                    .iter_active()
                    .filter(|member| member.id() != &dst)
                {
                    // XXX It's not very difficult to lift this restriction:
                    // This means that codecs MUST NOT leave the buffer
                    // dirty on failure
                    if let Err(_ignored) = self.codec.encode_member(member, &mut buf) {
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
            span.record("num_updates", &num_items);
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
            let num_broadcasts = self.custom_broadcasts.fill(&mut buf, usize::MAX);
            #[cfg(feature = "tracing")]
            span.record("num_broadcasts", &num_broadcasts);
        }

        let data = buf.into_inner();
        #[cfg(feature = "tracing")]
        span.record("len", &data.len());

        #[cfg(feature = "tracing")]
        tracing::debug!("Message sent");

        runtime.send_to(dst, &data);
        Ok(())
    }

    fn accept_payload(&self, header: &Header<T>) -> bool {
        // Only accept payloads addessed to us
        header.dst == self.identity
            // Unless it's an Announce message
            || (header.message == Message::Announce
                // Then we accept it if DST is one of our _possible_
                // identities
                && self.identity.has_same_prefix(&header.dst))
    }

    fn handle_self_update(
        &mut self,
        incarnation: Incarnation,
        state: State,
        mut runtime: impl Runtime<T>,
    ) -> Result<()> {
        match state {
            State::Suspect => {
                match self.incarnation.cmp(&incarnation) {
                    // This can happen when a member received an update about
                    // someone else suspecting us but hasn't received our
                    // refutal yet. We can ignore it.
                    // There is a chance that it may lead to us being declared
                    // down due to this if our new incarnation doesn't reach
                    // them, but we shouldn't try to bump our incarnation again
                    // else we risk entering a game of counting
                    Ordering::Greater => {
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            ?self.incarnation,
                            suspected = incarnation,
                            "Ignored suspicion about old incarnation",
                        );
                        return Ok(());
                    }

                    // Unexpected: someone suspects our identity but thinks we were
                    // in a higher incarnation. May happen due to members flapping,
                    // but can also be a sign of a bad actor (multiple identical
                    // identities, clients bumping identities from other members,
                    // corrupted data, etc)
                    // We'll emit a warning and then refute the suspicion normally
                    Ordering::Less => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            ?self.incarnation,
                            suspected = incarnation,
                            "Suspicion on incarnation higher than current",
                        );
                    }

                    // The usual case: our current incarnation is being suspected,
                    // so we need to bump ours.
                    Ordering::Equal => {}
                };

                let incarnation = Incarnation::max(incarnation, self.incarnation);

                // We need to rejoin the cluster when this situation happens
                // because it will be impossible to refute suspicion
                if incarnation == Incarnation::MAX {
                    if !self.attempt_rejoin(&mut runtime)? {
                        #[cfg(feature = "tracing")]
                        tracing::warn!("Inactive: reached Incarnation::MAX",);
                        self.become_undead(runtime);
                    }
                    return Ok(());
                }

                // XXX Overzealous checking
                self.incarnation = incarnation.saturating_add(1);

                // We do NOT add ourselves as Alive to the updates buffer
                // because it's unnecessary: by bumping our incarnation *any*
                // message we send will be interpreted as a broadcast update
                // See: `tests::message_from_aware_suspect_refutes_suspicion`
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
                tracing::warn!("Rejoin failure: Identity::renew() returned same id",);
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
    type Broadcast = &'static [u8];
    type Error = BroadcastsDisabledError;

    fn receive_item(
        &mut self,
        _data: impl Buf,
    ) -> core::result::Result<Option<Self::Broadcast>, Self::Error> {
        Err(BroadcastsDisabledError)
    }
}

struct ClusterUpdate<T> {
    member_id: T,
    data: Bytes,
}

impl<T: PartialEq> Invalidates for ClusterUpdate<T> {
    // State is managed externally (via Members), so invalidation
    // is a trivial replace-if-same-key
    fn invalidates(&self, other: &Self) -> bool {
        self.member_id == other.member_id
    }
}

impl<T> AsRef<[u8]> for ClusterUpdate<T> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
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

    pub fn probe(&self) -> &Probe<T> {
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

    use bytes::{Buf, BufMut};
    use rand::{rngs::SmallRng, SeedableRng};

    use crate::testing::{BadCodec, InMemoryRuntime, ID};

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
        let mut codec = BadCodec;
        let mut buf = BytesMut::new();

        codec
            .encode_header(&header, &mut buf)
            .expect("MAYBE FIXME?");

        if !updates.is_empty() {
            buf.put_u16(u16::try_from(updates.len()).unwrap());
            for member in updates.iter() {
                codec.encode_member(member, &mut buf).expect("MAYBE FIXME?");
            }
        }

        buf.freeze()
    }

    fn decode(mut src: impl Buf) -> (Header<ID>, Vec<Member<ID>>) {
        let mut codec = BadCodec;
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

        let mut runtime = InMemoryRuntime::new();
        assert_eq!(
            Err(Error::SameIdentity),
            foca.change_identity(identity, &mut runtime)
        );
        assert_eq!(Ok(()), foca.change_identity(ID::new(43), &mut runtime));
        assert_eq!(&ID::new(43), foca.identity());
    }

    #[test]
    fn cant_probe_when_not_connected() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());

        let runtime = InMemoryRuntime::new();
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

    #[test]
    fn can_join_with_another_client() {
        let mut foca_one = Foca::new(ID::new(1), config(), rng(), codec());
        let mut foca_two = Foca::new(ID::new(2), config(), rng(), codec());

        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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

        let (header, updates) = decode(feed_data);

        assert_eq!(header.message, Message::Feed);
        assert_eq!(2, updates.len());
        assert_eq!(
            members
                .iter()
                .cloned()
                .filter(|m| m.is_active())
                .collect::<Vec<_>>(),
            updates
        );
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

        let mut runtime = InMemoryRuntime::new();
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
        let mut members = foca.iter_members().cloned().collect::<Vec<_>>();
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
        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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

        for message in indirect_messages.into_iter() {
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
        let mut runtime = InMemoryRuntime::new();

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
    fn cant_receive_announce_with_extra_data() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = InMemoryRuntime::new();

        assert_eq!(
            Err(Error::MalformedPacket),
            foca.handle_data(
                &encode((
                    Header {
                        src: ID::new(1),
                        src_incarnation: 0,
                        dst: ID::new(2),
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
        let codec = BadCodec;
        let mut foca = Foca::new(target_id, config(), rng(), codec);
        let mut runtime = InMemoryRuntime::new();

        // Our goal is getting `src` to join `target_id`'s cluster.
        let src_id = ID::new(2);

        // We'll send a packet destined to the wrong id, not
        // passing the "has same prefix" check to verify the join
        // doesn't happen
        let wrong_dst = ID::new(3);
        assert!(!target_id.has_same_prefix(&wrong_dst));
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
        assert!(target_id.has_same_prefix(&dst));
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
        assert!(foca.iter_members().any(|member| member == &src_id));
    }

    #[test]
    fn suspicion_refutal() -> Result<()> {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = InMemoryRuntime::new();

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
    fn change_identity_gossips_immediately() {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = InMemoryRuntime::new();

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

        let mut runtime = InMemoryRuntime::new();
        assert_eq!(Ok(()), foca.change_identity(ID::new(2), &mut runtime));

        assert_ne!(orig_timer_token, foca.timer_token());
    }

    #[test]
    fn renew_during_probe_shouldnt_cause_errors() {
        let id = ID::new(1).rejoinable();
        let mut foca = Foca::new(id, config(), rng(), codec());
        let mut runtime = InMemoryRuntime::new();

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
    ) -> (
        Foca<ID, BadCodec, SmallRng, NoCustomBroadcast>,
        ID,
        Timer<ID>,
    ) {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = InMemoryRuntime::new();

        assert!(num_members > 0);
        // Assume some members exist
        for smallid in 2..(num_members + 2) {
            foca.apply(Member::alive(ID::new(smallid)), &mut runtime)
                .expect("infallible");
        }

        // The runtime shoud've been instructed to schedule a
        // probe for later on
        let expected_timer = Timer::ProbeRandomMember(foca.timer_token());
        expect_scheduling!(runtime, expected_timer.clone(), config().probe_period);

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
        expect_scheduling!(runtime, send_indirect_probe.clone(), config().probe_rtt);

        (foca, probed, send_indirect_probe)
    }

    #[test]
    fn probe_ping_ack_cycle() {
        let (mut foca, probed, send_indirect_probe) = craft_probing_foca(5);
        let mut runtime = InMemoryRuntime::new();

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
        let (mut foca, probed, send_indirect_probe) = craft_probing_foca(5);
        let mut runtime = InMemoryRuntime::new();

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
            craft_probing_foca((num_indirect_probes + 2) as u8);
        let mut runtime = InMemoryRuntime::new();

        // `probed` did NOT reply with an Ack before the timer
        assert_eq!(Ok(()), foca.handle_timer(send_indirect_probe, &mut runtime));

        let mut ping_req_dsts = Vec::new();
        let all_data = runtime.take_all_data();
        for (to, data) in all_data.into_iter() {
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

        for src in ping_req_dsts.into_iter() {
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
        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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
                foca.apply_many(members.iter().cloned(), InMemoryRuntime::new())?;
                herd.push(foca)
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
        let mut runtime = InMemoryRuntime::new();
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
            foca_one.iter_members().all(|id| id != &two),
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
            foca_one.iter_members().any(|id| id == &three),
            "foca_three should have recovered"
        );

        Ok(())
    }

    #[test]
    fn leave_cluster_gossips_about_our_death() -> Result<()> {
        let mut foca = Foca::new(ID::new(1), config(), rng(), codec());
        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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
        let mut runtime = InMemoryRuntime::new();

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
            foca.handle_data(&large_data[..], InMemoryRuntime::new())
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
            foca.handle_data(valid_data.as_ref(), InMemoryRuntime::new()),
            "valid_data should be valid :-)"
        );

        // Now we'll append some rubbish to it, so that everything
        // is still valid up the trash.
        let mut bad_data = Vec::from(valid_data.as_ref());

        // A single trailing byte should be enough to trigger an error
        bad_data.push(0);

        assert_eq!(
            Err(Error::CustomBroadcast(anyhow::Error::msg(
                BroadcastsDisabledError
            ))),
            foca.handle_data(bad_data.as_ref(), InMemoryRuntime::new()),
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
            type Broadcast = VersionedKey;

            type Error = &'static str;

            fn receive_item(
                &mut self,
                data: impl Buf,
            ) -> core::result::Result<Option<Self::Broadcast>, Self::Error> {
                let decoded = VersionedKey::from_bytes(data)?;

                let is_new_information = self
                    .0
                    .get(&decoded.key())
                    // If the version we know about is smaller
                    .map(|&version| version < decoded.version())
                    // Or we never seen the key before
                    .unwrap_or(true);

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
            foca.add_broadcast(b"hue").is_err(),
            "Adding garbage shouldn't work"
        );

        assert_eq!(
            Ok(()),
            foca.add_broadcast(VersionedKey::new(420, 0).as_ref()),
        );

        assert_eq!(
            1,
            foca.custom_broadcast_backlog(),
            "Adding a new custom broadcast should increase the backlog"
        );

        assert_eq!(
            Ok(()),
            foca.add_broadcast(VersionedKey::new(420, 1).as_ref()),
        );

        assert_eq!(
            1,
            foca.custom_broadcast_backlog(),
            "But receiving a new version should simply replace the existing one"
        );

        // Let's add one more custom broadcast because testing with N=1
        // is pretty lousy :-)
        assert_eq!(
            Ok(()),
            foca.add_broadcast(VersionedKey::new(710, 1).as_ref()),
        );

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
        let mut runtime = InMemoryRuntime::new();
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
}
