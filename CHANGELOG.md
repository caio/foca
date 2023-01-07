# Changelog

## Unreleased

- **BREAKING**: `iter_members` now yields an iterator of `Member`
  structs, previously if would yield `Member::id`. This allows
  users to bootstrap a foca instance with existing cluster state
  by feeding its output directly to `Foca::apply_many`
- **BREAKING**: `Config::remove_down_after` has been increated to
  24h. The previous default value of 2 minutes was still too small
  and would lead to unreasonably large updates backlog on large
  clusters.
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
