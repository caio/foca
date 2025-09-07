
<div align="right">
  <details>
    <summary >🌐 Language</summary>
    <div>
      <div align="center">
        <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=en">English</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=zh-CN">简体中文</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=zh-TW">繁體中文</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=ja">日本語</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=ko">한국어</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=hi">हिन्दी</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=th">ไทย</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=fr">Français</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=de">Deutsch</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=es">Español</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=it">Italiano</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=ru">Русский</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=pt">Português</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=nl">Nederlands</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=pl">Polski</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=ar">العربية</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=fa">فارسی</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=tr">Türkçe</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=vi">Tiếng Việt</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=id">Bahasa Indonesia</a>
        | <a href="https://openaitx.github.io/view.html?user=caio&project=foca&lang=as">অসমীয়া</
      </div>
    </div>
  </details>
</div>

# Foca: Cluster membership discovery on your terms

Foca is a building block for your gossip-based cluster discovery. It's
a small `no_std` + `alloc` crate that implements the SWIM protocol along
with its useful extensions (`SWIM+Inf.+Susp.`).

Project:

* Git Repository: https://caio.co/de/foca/
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
$ cargo run --features std,tracing,bincode-codec --example foca_insecure_udp_agent -- --help
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

* `std`: Implements `foca::Identity` for `std::net::SocketAddr*` and
  exposes `Config::new_lan` and `Config::new_wan`
* `tracing`: Instruments Foca using the [tracing][] crate. High-level
  protocol interactions are emited as `DEBUG` traces, more details can
  be exposed with the `TRACE` level. No other levels are emitted.
* `serde`: Derives `Serialize` and `Deserialize` for Foca's public
  types.
* `bincode-codec`: Provides `BincodeCodec`, a serde-based codec type
  that uses [bincode][] under the hood.
* `postcard-codec`: Provides `PostcardCodec` a serde-based, `no_std`
  friendly codec that uses [postcard][] under the hood.
* `unstable-notifications`: Provides new notifications that allow
  inspecting messages being sent and received


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
  size. Users can make use of `Config::new_{lan,wan}` along with
  `Foca::set_config` to achieve the same

# References

* The paper [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership
Protocol][1]
* HashiCorp's [memberlist][2]

[1]: https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
[2]: https://github.com/hashicorp/memberlist
[bincode]: https://github.com/bincode-org/bincode
[postcard]: https://github.com/jamesmunns/postcard
[tracing]: https://docs.rs/tracing/latest/tracing/
[dir]: https://docs.rs/tracing-subscriber/0.3.17/tracing_subscriber/filter/struct.EnvFilter.html#directives

# License

Unless explicitly stated otherwise, all work is subject to the terms
of the Mozilla Public License, version 2.0.

Files inside the `examples/` directory are dedicated to the Public
Domain.
