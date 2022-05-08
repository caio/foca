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

#[cfg(feature = "std")]
use core::num::NonZeroU32;

#[cfg(feature = "std")]
impl Config {
    /// Generate a configuration for a LAN cluster given an expected
    /// total number of active members.
    ///
    /// The `cluster_size` parameter is used to define how many times updates
    /// are disseminated ([`Config::max_transmissions`]) and how long Foca
    /// will wait before declaring a suspected member as down
    /// ([`Config::suspect_to_down_after`]).
    ///
    /// Settings derived from [memberlist's DefaultLanConfig][dlc].
    ///
    /// [dlc]: https://pkg.go.dev/github.com/hashicorp/memberlist#DefaultLANConfig
    pub fn new_lan(cluster_size: NonZeroU32) -> Self {
        let period = Duration::from_secs(1);

        Self {
            probe_period: period,
            probe_rtt: Duration::from_millis(500),
            num_indirect_probes: NonZeroUsize::new(3).unwrap(),

            max_transmissions: Self::compute_max_tx(cluster_size),

            suspect_to_down_after: Self::suspicion_duration(cluster_size, period, 4.0),
            remove_down_after: Duration::from_secs(15),

            max_packet_size: NonZeroUsize::new(1400).unwrap(),
        }
    }

    /// Generate a configuration for a WAN cluster given an expected
    /// total number of active members.
    ///
    /// Settings derived from [memberlist's DefaultWanConfig][dwc].
    ///
    /// [dwc]: https://pkg.go.dev/github.com/hashicorp/memberlist#DefaultWANConfig
    ///
    /// See [`Config::new_lan`].
    pub fn new_wan(cluster_size: NonZeroU32) -> Self {
        let period = Duration::from_secs(5);

        Self {
            probe_period: period,
            probe_rtt: Duration::from_secs(3),
            num_indirect_probes: NonZeroUsize::new(3).unwrap(),

            max_transmissions: Self::compute_max_tx(cluster_size),

            suspect_to_down_after: Self::suspicion_duration(cluster_size, period, 6.0),
            remove_down_after: Duration::from_secs(15),

            max_packet_size: NonZeroUsize::new(1400).unwrap(),
        }
    }

    fn compute_max_tx(cluster_size: NonZeroU32) -> NonZeroU8 {
        let multiplier = 4.0f64;
        let max_tx = f64::from(cluster_size.get().saturating_add(1)).log10() * multiplier;
        // XXX over-zealous: `multiplier` is not exposed; 4.0 guarantees it doesn't end up here
        if max_tx <= 1.0 {
            NonZeroU8::new(1).unwrap()
        } else if max_tx >= 255.0 {
            NonZeroU8::new(core::u8::MAX).unwrap()
        } else {
            NonZeroU8::new(max_tx as u8).expect("f64 ]1,255[ as u8 is non-zero")
        }
    }

    fn suspicion_duration(
        cluster_size: NonZeroU32,
        probe_period: Duration,
        multiplier: f64,
    ) -> Duration {
        let secs = f64::max(1.0, f64::from(cluster_size.get()).log10())
            * multiplier
            * probe_period.as_secs_f64();

        // XXX `Duration::from_secs_f64` is panicky, but:
        //  - multiplier is either 4 or 6
        //  - probe_period is either 1 or 5 secs
        // So we know `secs` is finite, greater than zero and won't overflow
        Duration::from_secs_f64(secs)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    #[cfg(feature = "std")]
    fn suspicion_scales_slowly() {
        use super::*;

        let probe_period = Duration::from_secs(1);
        let mult = 4.0;

        assert_eq!(
            Duration::from_secs(4),
            Config::suspicion_duration(NonZeroU32::new(5).unwrap(), probe_period, mult)
        );

        assert_eq!(
            Duration::from_secs(4),
            Config::suspicion_duration(NonZeroU32::new(10).unwrap(), probe_period, mult)
        );

        assert_eq!(
            Duration::from_secs(8),
            Config::suspicion_duration(NonZeroU32::new(100).unwrap(), probe_period, mult)
        );
    }
}
