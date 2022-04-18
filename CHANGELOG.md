# Changelog

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
