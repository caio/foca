# Changelog

## v0.17.2 - 2024-06-04

- Bugfix: foca could incorrectly attach custom broadcasts to
  messages supposed to be lightweight leading to strange decode
  errors in the logs and slowing down cluster self-healing, but
  no further impact on functionality
  See: https://github.com/caio/foca/issues/35

## v0.17.1 - 2024-04-25

- Bugfix: when restarting members, there was a chance foca would
  ask a member to ping its own previous identity.
  No harm done to the cluster state, but would lead to noise in
  the form of `Error::DataFromOurselves` in the member logs
  See: https://github.com/caio/foca/issues/34
- Bugfix: messages from known stale identities were being accepted
  instead being discarded. Unlikely to have impacted anyone
  that isn't replaying data

## v0.17.0 - 2024-03-20

This release contains significant changes aimed at freeing users
from having to manage their own version of a unique list of members.

Whilst upgrading won't be just trivially bumping the version, it's
expected to be very easy and users will probably find themselves
deleting more code than writing new one when doing it.

Memory and CPU usage should go down since members and cluster
updates are now bound by the number of distinct addresses whereas
previously it would grow with the number of distinct identities.

As this is a large change, users should be more cautious when
upgrading and are welcome to open an issue in case of questions
or problems.

- **BREAKING**: `foca::Identity` has been revamped and now requires
  a unique cluster-wide identifier (typically a socket address or a
  hostname). When multiple identities appear with the same `Addr`
  the conflict is handled by `Identity::win_addr_conflict`
- **BREAKING**: _Custom_ broadcasts wire format has changed and
  handler implementations now only need to emit an identifier (Key)
  for each value being broadcast instead of managing the allocation
  See the `BroadcastHandler` documentation and examples for details
- `Config::periodic_announce_to_down_members`: Foca periodically
  tries to join with members it considers down, as an attempt to
  recover from a network partition. This setting is **enabled** by
  default for `Config::new_wan` and `Config::new_lan`
- There's now `Notification::Rename` that signals whenever an
  identity with a conflicting `Addr` in the cluster gets replaced
  by a newer one
- There's no need to manage the list of members externally anymore:
  Foca does it all for you and `Foca::iter_members` only lists
  the unique (by `Identity::Addr`), freshest identities
- `examples/foca_insecure_udp_agent.rs` now comes with a fully working
  custom broadcast example
- There's now a simple `runtime::AccumulatingRuntime` that's good
  enough for basic usage if you don't want to implement your own

## v0.16.0 - 2023-10-01

- Introduce `Foca::iter_membership_state` that provides a view into the
  whole state facilitating state replication scenarios. Previously one
  would need to juggle `Notification::MemberDown` and
  `Foca::iter_members` to achieve the same

## v0.15.0 - 2023-09-04

- `Foca::leave_cluster` doesn't consume self anymore, so users can keep
  using the instance; Even rejoin the cluster if they so wish.
  See: https://github.com/caio/foca/issues/30

## v0.14.0 - 2023-09-03

- **BREAKING** Custom broadcast handlers now know which member sent the
  data they're handling to facilitate anti-entropy use-cases.
  See: https://github.com/caio/foca/issues/28
- Foca now only emits DEBUG and TRACE level traces when using the
  `tracing` feature
  See: https://github.com/caio/foca/issues/30
- The `foca_insecure_udp_agent` example now contains a more robust timer
  handler able to tolerate arbitrary runtime lags

## v0.13.0 - 2023-07-09

- Foca will now gossip upon receiving messages that flag their identity
  as suspect
- Foca now resumes probing more quickly when recovering from an incorrect
  sequence of Timer events
- The Timer enum now has a very simple Ord implementation to facilitate
  dealing with out-of-order delivery. Sorting a slice of Timer events
  should leave them in the order they should be handled, starting from
  the first index.
  See: https://github.com/caio/foca/issues/26

## v0.12.0 - 2023-06-18

- **CRITICAL BUGFIX**: uses of the functionality to notify down members
  introduced on v0.6.0 could end up in a situation with a seemingly
  endless flood of Message::TurnUndead payloads.
  Upgrade is highly recommended.
  See: https://github.com/caio/foca/issues/25

## v0.11.0 - 2023-03-22

- config::PeriodicParams is now exposed correctly so users can setup
  periodic tasks without relying on and mutating the result from
  `Config::new_{wan,lan}`
  See: https://github.com/caio/foca/issues/24

## v0.10.1 - 2023-02-26

- Foca now always randomizes the list of members it sends when replying
  to announce messages. Previously this shuffling would happen
  periodically, so a fast announce rate would end up seeing the same set
  of members for a while.
  See: https://github.com/caio/foca/issues/22

## v0.10.0 - 2023-01-13

- Traces now include useful information even when running with less
  verbose level
- Bugfix: albeit rare, foca could end up choosing an identity with
  the same prefix for the indirect probe cycle, leading to a confusing
  error in the logs of the target about receiving a message from
  itself

## v0.9.0 - 2023-01-07

- **BREAKING**: `iter_members` now yields an iterator of `Member`
  structs, previously if would yield `Member::id`. This allows
  users to bootstrap a foca instance with existing cluster state
  by feeding its output directly to `Foca::apply_many`
- **BREAKING**: `Config::remove_down_after` has been increased to
  24h. The previous default value of 2 minutes was still too small
  and would lead to unreasonably large updates backlog on large
  clusters. See: https://github.com/caio/foca/issues/19
- Performance/Bugfix: now foca doesn't allocate when preparing a
  message to be sent

## v0.8.0 - 2022-12-31

- Bugfix: foca would send garbage at the end of the payload when
  replying to Announce messages under certain scenarios.
  See: https://github.com/caio/foca/issues/18

## v0.7.0 - 2022-11-27

- **BREAKING**: `Config::remove_down_after` now defaults to 2 minutes
  instead of 15 seconds: clients _not_ using the auto-rejoin
  functionality will take longer to rejoin the cluster after being
  declared down. Clients whose identity implement `Identity::renew`
  have nothing to worry about.
- Bugfix: A foca instance could end up learning that its previous
  identity was as active member on the cluster, adding more false
  positives to the cluster chatter

## v0.6.0 - 2022-11-13

- New features:
  - `Config::notify_down_members`: foca can now inform down members that
     keep talking to the cluster that their messages are being ignored
  - `Config::periodic_announce`: you can instruct foca to periodically
    ask its peers for more peers, so that it learns about every member
    in the cluster faster
  - `Config::periodic_gossip`: to help speed up the propagation of
    cluster updates
- **BREAKING**: `Config::new_wan` and `Config::new_lan` now _enable_
  every new feature in this release

## v0.5.0 - 2022-10-24

- Foca now gracefully resumes probing when it detects an incorrect
  probe cycle. https://github.com/caio/foca/pull/13
- Fixed one more instance of missing cleanup when the cluster state
  changes mid probe cycle. https://github.com/caio/foca/issues/2
- Public entities now implement `Eq` along with `PartialEq`

## v0.4.1 - 2022-07-23

- Bugfix: "Member not found" trace lowered from warn to debug, only
  affects users of the `tracing` feature

## v0.4.0 - 2022-07-02

- Optional feature `postcard-codec` now requires postcard v1.0

## v0.3.1 - 2022-05-29

- Configuration can now be changed at runtime via `Foca::set_config`
- Added `Config::new_wan` and `Config::new_lan` helpers to generate a
  good enough configuration based on a given cluster size (requires the
  `std` feature)
- Traces (from using the `tracing` feature) are a lot less noisy

## v0.3.0 - 2022-05-29

- **Yanked**, released accidentally

## v0.2.0 - 2022-04-18

- **BREAKING** `BroadcastHandler` now takes a generic parameter:
  the identity type used by Foca
- `BroadcastHandler` can now decide which members should receive
  custom broadcasts
- You can now ask Foca to disseminate broadcasts only, via
  `Foca::broadcast`
- Simple broadcasting example added at `./examples/broadcasting.rs`
- Fixed a bug that could lead to incorrect behaviour when a Foca
  instance changed identities in the middle of a probe cycle
  https://github.com/caio/foca/issues/2

## v0.1.0 - 2021-11-04

- First release
