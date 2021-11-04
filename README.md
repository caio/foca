# Foca: Cluster membership discovery on your terms

Foca is a building block for your gossip-based cluster discovery. It's
a small `no_std` + `alloc` crate that implements the SWIM protocol along
with its useful extensions (`SWIM+Inf.+Susp.`).

Project:

* Git Repository: https://github.com/caio/foca
* Issue tracker: https://github.com/caio/foca/issues
* CI: https://github.com/caio/foca/actions/workflows/ci.yml
* Packages: https://crates.io/crates/foca
* Documentation: https://docs.rs/foca


# Introduction

The most notable thing about Foca is the fact that it does almost
nothing. Out of the box, all it gives is a reliable and efficient
implementation of the [SWIM protocol][1] that's transport and
identity agnostic.

Knowledge of how SWIM works is helpful but not necessary to make use
of this library. Reading the documentation for the `Message` enum
should give you an idea of how the protocol works, but the paper is
a very accessible read.

Foca is designed to fit into any sort of transport: If your network
allows peers to talk to each other you can deploy Foca on it.
Not only the general bandwidth requirements are low, but you also
have full control of how members identify each other (see
`./examples/identity_golf.rs`) and how messages are encoded.


# Usage

Please take a look at `./examples/foca_insecure_udp_agent.rs`. It
showcases how a simple tokio-based agent could look like and lets
you actually run and see Foca swimming.

~~~
$ cargo run --features agent --example foca_insecure_udp_agent -- --help
foca_insecure_udp_agent 

USAGE:
    foca_insecure_udp_agent [OPTIONS] <BIND_ADDR>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -a, --announce <announce>    Address to another Foca instance to join with
    -f, --filename <filename>    Name of the file that will contain all active members
    -i, --identity <identity>    The address cluster members will use to talk to you.
                                 Defaults to BIND_ADDR

ARGS:
    <BIND_ADDR>    Socket address to bind to. Example: 127.0.0.1:8080
~~~

So you can start the agent in one terminal with
`./foca_insecure_udp_agent 127.0.0.1:8000` and join it with as many others
as you want with using a different `BIND_ADDR` and `--announce` to a
running instance. Example:
`./foca_insecure_udp_agent 127.0.0.1:8001 -a 127.0.0.1:8000`.

The agent outputs some information to the console via [tracing][]'s
subscriber. It defaults to the `INFO` log level and can be customized
via the `RUST_LOG` environment variable using [tracing_subscriber's
EnvFilter directives][dir].


## Cargo Features

Every feature is optional. The `default` set will always be empty.

* `std`: Adds `std::error::Error` support and implements `foca::Identity`
  for `std::net::SocketAddr*`.
* `tracing`: Instruments Foca using the [tracing][] crate.
* `serde`: Derives `Serialize` and `Deserialize` for Foca's public
  types.
* `bincode-codec`: Provides `BincodeCodec`, a serde-based codec type
  that uses [bincode][] under the hood.
* `postcard-codec`: Provides `PostcardCodec` a serde-based, `no_std`
  friendly codec that uses [postcard][] under the hood.

Only for examples:

* `identity-golf`: For `./examples/identity_golf.rs`
* `agent`: For `./examples/foca_insecure_udp_agent.rs`


# Notes

When writing this library, the main goal was having a simple and small
core that's easy to test, simulate and reason about; It was mostly
about getting a better understanding of the protocol after reading
the paper.

Sticking to these goals naturally led to an implementation that doesn't
rely on many operating system features like a hardware clock, atomics
and threads, so becoming a `no_std` crate (albeit still requiring heap
allocations) was kind of a nice accidental feature that I decided to
commit to.


## Comparison to memberlist

I avoided looking at [memberlist][2] until I was satisfied with my
own implementation. Since then I did take a non-thorough look at it:

* memberlist supports custom broadcasts, which is a very cool feature
  for complex service discovery scenarios, so now Foca has support
  for disseminating user data too (see `BroadcastHandler`
  documentation) :-)

* It has a stream-based synchronization mechanism (push-pull) that's
  used for joining and periodic merging state between members: It's
  way beyond Foca's responsibilities, but it's a very interesting idea,
  so I've exposed the `Foca::apply_many` method which enables code
  using Foca to do a similar thing if desired.

* Its configuration parameters change based on (current) cluster
  size. It's super useful for a more plug-and-play experience, so
  I want introduce something along those lines in the future, likely
  by pulling `Config` into Foca as a trait implementation.


## Future

Foca is very focused on doing almost nothing, so it's likely that
some things will end up on separate crates. But, in no particular
order, I want to:

* Provide a more plug-and-play experience, closer to what memberlist
  gives out of the box.

* Make Foca run as a library for a higher level language. I'm not
  even sure I can take it that far, so sounds like fun!

* Deliver a (re)usable simulator. Right now I've been yolo-coding
  one just to give me more confidence on what's implemented right
  now, but what I want is something that you can: Slap your own
  identity, codec and configuration; Set network parameters like
  TTL, loss rate, bandwidth; And then simulate production behavior
  (rolling restarts, partitions, etc) while watching convergence
  stats. This is a ridiculous amount of work.

* Actually demonstrate a running Foca with `no_std` constraints; I
  don't have access to devices to play with at the moment, so
  it's been difficult to find motivation to pursue this.


# References

* The paper [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership
Protocol][1]
* HashiCorp's [memberlist][2]

[1]: https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
[2]: https://github.com/hashicorp/memberlist
[bincode]: https://github.com/bincode-org/bincode
[postcard]: https://github.com/jamesmunns/postcard
[tracing]: https://tracing.rs/
[dir]: https://tracing.rs/tracing_subscriber/struct.envfilter#directives

# License

Unless explicitly stated otherwise, all work is subject to the terms
of the Mozilla Public License, version 2.0.

Files inside the `ensure_no_std_alloc/` directory are under the MIT
license, as their original.

Files inside the `examples/` directory are dedicated to the Public
Domain.
