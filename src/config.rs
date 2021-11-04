/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use core::{
    num::{NonZeroU8, NonZeroUsize},
    time::Duration,
};

/// A Config specifies the parameters Foca will use for the SWIM
/// protocol.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Config {
    /// Specifies how often a random member will be probed for activity.
    ///
    /// At the end of this period, if the member didn't reply (directly
    /// or indirectly, see [`crate::Message`]) it's declared
    /// [`crate::State::Suspect`].
    ///
    /// Should be strictly larger than [`Self::probe_rtt`]. Preferably more
    /// than twice its value since we need to wait for the indirect ping cycle.
    /// If unsure, err on the safe side with `probe_rtt * 3` and tune
    /// later.
    ///
    /// Must not be zero.
    pub probe_period: Duration,

    /// How long to wait for a direct reply to a probe before starting
    /// the indirect probing cycle.
    ///
    /// It should be set to a value that describes well your transport
    /// round-trip time. A reasonable value would be a high quantile
    /// (p99, for example) of your cluster-wide `ICMP PING` RTT.
    ///
    /// Must be strictly smaller than [`Self::probe_period`].
    ///
    /// Must not be zero.
    pub probe_rtt: Duration,

    /// How many members will be asked to perform an indirect ping
    /// in case the probed member takes too long to reply.
    ///
    /// This doesn't need to be a large number: we're essentially
    /// fanning out to ensure a message actually reaches the original
    /// ping target in case of poor transmission quality or weird
    /// partitions.
    ///
    /// Setting this to 3-5 should be more than enough for a "modern"
    /// network.
    pub num_indirect_probes: NonZeroUsize,

    /// Specifies how many times a single update/broadcast will be sent
    /// along with a normal message.
    ///
    /// A high value trades off bandwidth for higher chances of fully
    /// disseminating broadcasts throughout the cluster.
    ///
    /// Reasonable values range from 5, for small clusters to 15 for
    /// *very* large clusters.
    pub max_transmissions: NonZeroU8,

    /// How long a Suspect member is considered active before being
    /// declared Down.
    ///
    /// Here you want to give time for the member to realize it has
    /// been declared Suspect and notify the cluster that its actually
    /// active.
    ///
    /// Higher values give more time for a member to recover from a
    /// false suspicion, but slows down detection of a failed state.
    ///
    /// Very application-dependent. Smaller clusters likely want
    /// this value to be a small multiplier of [`Self::probe_period`]
    /// whereas large clusters can easily tolerate several seconds of
    /// wait.
    ///
    /// Must not be zero.
    pub suspect_to_down_after: Duration,

    /// Governs how long Foca will remember an identity as being
    /// Down.
    ///
    /// A high value is recommended to avoid confusing cluster
    /// members with partial joins. If in doubt use a high multiplier
    /// over the probe period, like `10 * probe_period`.
    pub remove_down_after: Duration,

    /// The maximum packet size Foca will produce AND consume.
    ///
    /// This is transport-dependent. The main goal is reducing
    /// fragmentation and congestion.
    ///
    /// If using UDP as a transport, use `rfc8085` guidelines and stick
    /// to a value smaller than your network's MTU.  1400 is a good
    /// value for a in a non-ancient network.
    pub max_packet_size: NonZeroUsize,
}

impl Config {
    /// A simple configuration that should work well in a LAN scenario.
    pub fn simple() -> Self {
        Self {
            probe_period: Duration::from_millis(1500),
            probe_rtt: Duration::from_millis(500),
            num_indirect_probes: NonZeroUsize::new(3).unwrap(),

            max_transmissions: NonZeroU8::new(10).unwrap(),

            suspect_to_down_after: Duration::from_secs(3),
            remove_down_after: Duration::from_secs(15),

            max_packet_size: NonZeroUsize::new(1400).unwrap(),
        }
    }
}
