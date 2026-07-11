// This shows how one could teach foca about well known (or all)
// members of a cluster
//
// The important steps are:
//
// - Teaching foca about these members via `Foca::apply_many`
// - Ensuring the config option `periodic_announce_to_down_members`
//   is enabled (It's enabled by default for new_lan and new_lan constructors)
//
// This requires identities that have opted-in on automatic rejoining
//
// Notice that `apply_many` can also be used for a state sync protocal
// (anti-entropy) and for restoring state after a restart
use rand::rngs::StdRng;
use std::{net::SocketAddr, num::NonZeroUsize, str::FromStr, time::Duration};

use foca::{BincodeCodec, Config, Foca, Identity, Member, State};

#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
struct ID {
    addr: SocketAddr,
    bump: u64,
}

fn main() {
    let wellknown = [
        SocketAddr::from_str("192.168.0.100:8080").unwrap(),
        SocketAddr::from_str("192.168.0.101:8080").unwrap(),
        SocketAddr::from_str("192.168.0.102:8080").unwrap(),
    ];

    let config = Config {
        periodic_announce_to_down_members: Some(foca::PeriodicParams {
            frequency: Duration::from_secs(60),
            num_members: NonZeroUsize::new(3).unwrap(),
        }),
        // Never forget a member
        remove_down_after: None,
        ..Config::simple()
    };

    let ourselves = ID::new(SocketAddr::from_str("192.168.0.200:8080").unwrap(), 1);
    let mut runtime = foca::AccumulatingRuntime::new();
    let rng: StdRng = rand::make_rng();
    let mut foca = Foca::new(
        ourselves,
        config,
        rng,
        BincodeCodec(bincode::config::standard()),
    );

    foca.apply_many(
        wellknown
            .iter()
            .copied()
            .map(ID::wellknown)
            .map(|id| Member::new(id, 0, State::Down)),
        false,
        &mut runtime,
    )
    .unwrap();

    // Go swimming!
}

impl ID {
    fn wellknown(addr: SocketAddr) -> Self {
        Self { addr, bump: 0 }
    }

    fn new(addr: SocketAddr, bump: u64) -> Self {
        assert_ne!(0, bump);
        Self { addr, bump }
    }

    fn bump(&self) -> Self {
        let bump = if self.bump == u64::MAX {
            1
        } else {
            self.bump + 1
        };

        Self {
            addr: self.addr,
            bump,
        }
    }
}

impl Identity for ID {
    type Addr = SocketAddr;

    fn renew(&self) -> Option<Self> {
        Some(self.bump())
    }

    fn addr(&self) -> Self::Addr {
        self.addr
    }

    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.bump > adversary.bump
    }
}
