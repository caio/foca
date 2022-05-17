/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use core::fmt;

/// Identity is a cluster-global identifier. It qualifies a cluster
/// member and there must not be multiple members sharing the
/// same identity.
///
/// When talking about network protocols we're often talking about
/// an IP-address paired with a port number (`std::net::SocketAddr`,
/// for example), but Foca doesn't actually care about what's inside
/// an identity so long as its unique.
///
/// This allows implementations to make their identities as lean or
/// large as they need. For example: if every Foca instance will bind
/// to the same port, there's no need to make the port number part of
/// the identity.
///
/// That said, most of the time it's useful to have *more* information
/// in a identity than just a way to figure out a "network" address.
/// And that's because of how SWIM works: when an identity is declared
/// down or deliberately leaves the cluster, it cannot rejoin for a
/// relatively long while, so a little extra metadata allows us to
/// come back as fast as possible.
///
/// See `examples/identity_golf.rs` for ideas
///
pub trait Identity: Clone + Eq + fmt::Debug {
    /// Opt-in on auto-rejoining by providing a new identity.
    ///
    /// When Foca detects it's been declared Down by another member
    /// of the cluster, it will call [`Self::renew()`] on its current
    /// identity and if it yields a new one will immediately
    /// switch to it and notify the cluster so that downtime is
    /// minimized.
    fn renew(&self) -> Option<Self>;

    /// Optionally accept Announce messages addressed to an identity
    /// that isn't exactly the same as ours.
    ///
    /// Foca discards messages that aren't addressed to its exact
    /// identity. This means that if your identity has an unpredictable
    /// field (a UUID or a random number, for example), nobody will
    /// be able to join with us directly.
    ///
    /// The [`Self::has_same_prefix`] method is how we teach Foca to
    /// relax this restriction: Upon receiving an Announce message it
    /// will call `current_id.has_same_prefix(dst)` and if it yields
    /// `true` the message will be accepted and the new member will
    /// be allowed to join the cluster.
    fn has_same_prefix(&self, other: &Self) -> bool;
}

#[cfg(feature = "std")]
macro_rules! impl_basic_identity {
    ($type: ty) => {
        impl Identity for $type {
            fn renew(&self) -> Option<Self> {
                None
            }

            fn has_same_prefix(&self, _other: &Self) -> bool {
                false
            }
        }
    };
}

#[cfg(feature = "std")]
impl_basic_identity!(std::net::SocketAddr);

#[cfg(feature = "std")]
impl_basic_identity!(std::net::SocketAddrV6);

#[cfg(feature = "std")]
impl_basic_identity!(std::net::SocketAddrV4);
