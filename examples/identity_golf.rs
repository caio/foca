/* Any copyright is dedicated to the Public Domain.
 * https://creativecommons.org/publicdomain/zero/1.0/ */
// NOTE: This is intended to be read from the top to the bottom,
//       literate style, as you would normally read text.
#![allow(dead_code)]
use foca::Identity;

fn main() {
    // Foca (with the `std` feature) already gives us a very simple Identity
    // implementation for the socket addreess types. So we could use just
    // that:
    use std::net::{Ipv4Addr, SocketAddrV4};

    let basic_identity = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080);

    // But it's so basic that we won't even be able to rejoin a cluster
    // quickly:
    assert_eq!(None, basic_identity.renew());

    // It's very desirable to rejoin fast: we want to recover from false
    // positives fast and, most importantly, if we need to restart the
    // application, we don't want to wait a long while to be able to
    // join the cluster again. So let's add some metadata that we can
    // control:

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct FatIdentity {
        addr: SocketAddrV4,
        extra: u16,
    }

    impl From<SocketAddrV4> for FatIdentity {
        fn from(addr: SocketAddrV4) -> Self {
            Self {
                addr,
                // We initialize with random() instead of zero to add
                // some unpredictability to it
                extra: rand::random(),
            }
        }
    }

    impl Identity for FatIdentity {
        type Addr = SocketAddrV4;

        // We want fast rejoins, so we simply bump the extra field
        // maintaining the actual network address intact
        fn renew(&self) -> Option<Self> {
            Some(Self {
                addr: self.addr,
                extra: self.extra.wrapping_add(1),
            })
        }

        // And we ensure that members can Announce to us without
        // knowing our (randomized) extra field
        fn has_same_prefix(&self, other: &Self) -> bool {
            self.addr.eq(&other.addr)
        }

        fn addr(&self) -> SocketAddrV4 {
            self.addr
        }
    }

    // So now Foca will happily attempt to rejoin a cluster
    // for us as soon as it figures we're Down
    assert!(FatIdentity::from(basic_identity).renew().is_some());

    // But now our identities have increased in size and we haven't even
    // started adding interesting data to it. Let's shrink it a bit:
    //
    // It's very likely that you won't be binding to a random port on
    // startup- Often you'll have to use a specific port, dictated by
    // whoever operates the network, so why send this 16-bit number
    // when we know exactly what it is?
    //
    // We may also be in a situation that the IP addresses are repeated
    // all the time: say, we're operating in LAN context and they
    // are always something like 192.168.X.Y

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct SubnetFixedPortId {
        addr: (u8, u8),
        extra: u16,
    }

    impl From<Ipv4Addr> for SubnetFixedPortId {
        fn from(src: Ipv4Addr) -> Self {
            let octets = src.octets();
            Self {
                addr: (octets[2], octets[3]),
                extra: rand::random(),
            }
        }
    }

    // We can trivially transform this back into a socket address:
    impl SubnetFixedPortId {
        const PORTNR: u16 = 8080;

        pub fn as_socket_addr_v4(&self) -> SocketAddrV4 {
            SocketAddrV4::new(
                Ipv4Addr::new(192, 168, self.addr.0, self.addr.1),
                Self::PORTNR,
            )
        }
    }

    // And implementing identity is as trivial as it always is:
    impl Identity for SubnetFixedPortId {
        type Addr = (u8, u8);
        fn renew(&self) -> Option<Self> {
            Some(Self {
                addr: self.addr,
                extra: self.extra.wrapping_add(1),
            })
        }

        // And we ensure that members can Announce to us without
        // knowing our (randomized) extra field
        fn has_same_prefix(&self, other: &Self) -> bool {
            self.addr.eq(&other.addr)
        }

        fn addr(&self) -> (u8, u8) {
            self.addr
        }
    }

    // We'll stop golfing here, but it can be taken very far:
    //
    // * It's very common to have a "primary key" for every
    //   computer in the data center, meaning that you can
    //   have a `HashMap<u8, SocketAddress>` lying in memory
    //   somewhere, use `u8` as the main identifier as bask
    //   at your glorious tiny id.
    //
    // * Nowadays even tiny shops are going all-in on micro-services
    //   so maybe `u8` is not large enough... Then use `u16`, pack
    //   lookup data on 10bits, use the remaining as an extra random
    //   field for auto-rejoining!
    //
    // The best part of being able to minimize the byte size of
    // your identity is that you get more freedom to enrich it with
    // host-based metadata: shard_id, data_snapshot_version,
    // deployment_version - Stuff relevant information there and
    // you can avoid going all-in on real service discovery for
    // as long as your architecture is sane.
}
