# Changelog

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
