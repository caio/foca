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
    /// The type of the unique (cluster-wide) address of this identity
    ///
    /// A plain identity that cannot auto-rejoin (see `Identity::renew`)
    /// could have `Addr` the same as `Self` (`std::net::SocketAddr` is
    /// an example of one)
    ///
    /// It's a good idea to have this type as lean as possible
    type Addr: PartialEq;

    /// Opt-in on auto-rejoining by providing a new identity.
    ///
    /// When Foca detects it's been declared Down by another member
    /// of the cluster, it will call [`Self::renew()`] on its current
    /// identity and if it yields a new one will immediately
    /// switch to it and notify the cluster so that downtime is
    /// minimized.
    ///
    /// **NOTE** The new identity must win the conflict
    fn renew(&self) -> Option<Self>;

    /// Return this identity's unique address
    ///
    /// Typically a socket address, a hostname or similar
    ///
    /// On previous versions of this crate, there was a `has_same_prefix()`
    /// method. This serves the same purpose. Having a concrete type
    /// instead of just a yes/no allows Foca to fully manage the
    /// cluster members and keep its memory bound by the number of nodes
    /// instead of the number of identities
    fn addr(&self) -> Self::Addr;

    /// Decides which to keep when Foca encounters multiple identities
    /// sharing the same address
    ///
    /// Returning `true` means that self will be kept
    fn win_addr_conflict(&self, _adversary: &Self) -> bool;
}

#[cfg(feature = "std")]
macro_rules! impl_basic_identity {
    ($type: ty) => {
        impl Identity for $type {
            type Addr = $type;

            fn renew(&self) -> Option<Self> {
                None
            }

            fn addr(&self) -> $type {
                *self
            }

            fn win_addr_conflict(&self, _adversary: &Self) -> bool {
                panic!("addr is self, there'll never be a conflict");
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
