/* Any copyright is dedicated to the Public Domain.
 * https://creativecommons.org/publicdomain/zero/1.0/ */
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    fs::File,
    io::Write,
    net::SocketAddr,
    path::Path,
    str::FromStr,
    sync::Arc,
};

use bincode::Options;
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    fmt,
    prelude::*,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::{App, Arg};
use rand::{rngs::StdRng, SeedableRng};
use tokio::{
    net::UdpSocket,
    sync::mpsc,
    time::{sleep_until, Instant},
};

use foca::{BincodeCodec, Config, Foca, Notification, Timer};

#[derive(Debug)]
struct CliParams {
    bind_addr: SocketAddr,
    identity: ID,
    announce_to: Option<ID>,
    filename: String,
}

impl CliParams {
    fn new() -> Self {
        let matches = App::new("foca_insecure_udp_agent")
        .arg(
            Arg::with_name("BIND_ADDR")
                .help("Socket address to bind to. Example: 127.0.0.1:8080")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("identity")
                .help("The address cluster members will use to talk to you. Defaults to BIND_ADDR")
                .takes_value(true)
                .short("i")
                .long("identity"),
        )
        .arg(
            Arg::with_name("announce")
                .help("Address to another Foca instance to join with")
                .takes_value(true)
                .short("a")
                .long("announce"),
        )
        .arg(
            Arg::with_name("filename")
                .help("Name of the file that will contain all active members")
                .takes_value(true)
                .short("f")
                .long("filename"),
        )
        .get_matches();

        let bind_addr = SocketAddr::from_str(matches.value_of("BIND_ADDR").unwrap())
            .expect("Invalid BIND_ADDR");

        let identity = if let Some(identity) = matches.value_of("identity") {
            let addr = SocketAddr::from_str(identity)
                .expect("Failed to parse identity parameter as a socket address");
            ID::new(addr)
        } else {
            ID::new(bind_addr)
        };

        let announce_to = matches.value_of("announce").map(|param| {
            let addr = SocketAddr::from_str(param)
                .expect("Failed to parse announce parameter as a socket address");
            ID::new(addr)
        });

        let filename = matches
            .value_of("filename")
            .map(String::from)
            .unwrap_or_else(|| {
                // default to $TEMP_DIR/foca_cluster_members.{$RAND}.txt
                String::from(
                    std::env::temp_dir()
                        .join(format!(
                            "foca_cluster_members.{}.txt",
                            rand::random::<u64>()
                        ))
                        .to_str()
                        .unwrap(),
                )
            });

        Self {
            bind_addr,
            identity,
            announce_to,
            filename,
        }
    }
}

#[derive(Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct ID {
    addr: SocketAddr,
    // An extra field to allow fast rejoin
    bump: u64,
}

// We implement a custom, simpler Debug format just to make the tracing
// output cuter
impl std::fmt::Debug for ID {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ID")
            .field(&self.addr)
            .field(&self.bump)
            .finish()
    }
}

impl ID {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            bump: secs_since_epoch(),
        }
    }
}

impl foca::Identity for ID {
    type Addr = SocketAddr;

    // And by implementing `renew` we enable automatic rejoining:
    // when another member declares us as down, Foca immediatelly
    // switches to this new identity and rejoins the cluster for us
    fn renew(&self) -> Option<Self> {
        Some(Self {
            addr: self.addr,
            bump: self.bump.wrapping_add(1),
        })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    // This teaches every member how to compare two identities
    // with the same Addr value
    // In our case, the one with the larger bump always wins
    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.bump > adversary.bump
    }
}

fn do_the_file_replace_dance<'a>(
    fname: &str,
    addrs: impl Iterator<Item = &'a SocketAddr>,
) -> std::io::Result<()> {
    // Shirley, there's a more hygienic way of doing all this

    let tmp_fname = format!("{}.new", fname);

    let mut tmp = File::create(&tmp_fname)?;
    for addr in addrs {
        writeln!(&mut tmp, "{}", addr)?;
    }

    let dst = Path::new(fname);
    if dst.exists() {
        let old_fname = format!("{}.old", fname);
        std::fs::rename(dst, Path::new(&old_fname))?;
    }

    std::fs::rename(Path::new(&tmp_fname), Path::new(fname))?;

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    let params = CliParams::new();

    // Configured via RUST_LOG environment variable
    // See https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html
    tracing_subscriber::registry()
        .with(fmt::Layer::default().compact())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    tracing::info!(params = tracing::field::debug(&params), "Started");

    let CliParams {
        bind_addr,
        identity,
        announce_to,
        filename,
    } = params;

    let rng = StdRng::from_entropy();
    let config = Config::new_lan(std::num::NonZeroU32::new(10).unwrap());

    let buf_len = config.max_packet_size.get();
    let mut recv_buf = vec![0u8; buf_len];

    let mut foca = Foca::with_custom_broadcast(
        identity,
        config,
        rng,
        BincodeCodec(bincode::DefaultOptions::new()),
        Handler::new(),
    );
    let socket = Arc::new(UdpSocket::bind(bind_addr).await?);

    // We'll create a task responsible to sending data through the
    // socket.
    // These are what we use to communicate with it
    let (tx_send_data, mut rx_send_data) = mpsc::channel::<(SocketAddr, Bytes)>(100);
    // The socket writing task
    let write_socket = Arc::clone(&socket);
    tokio::spawn(async move {
        while let Some((dst, data)) = rx_send_data.recv().await {
            // A more reasonable implementation would do some more stuff
            // here before sending, like:
            //  * zlib or something else to compress the data
            //  * encryption (shared key, AES most likely)
            //  * an envelope with tag+version+checksum to allow
            //    protocol evolution
            let _ignored_send_result = write_socket.send_to(&data, &dst).await;
        }
    });

    // We'll launch a task to manage foca, responsible for handling
    // received data, events and user input
    let (tx_foca, mut rx_foca) = mpsc::channel(100);
    // One specialized task to deal with timer events where it sleeps
    // until the necessary time to submit the events it receives
    let scheduler = launch_scheduler(tx_foca.clone()).await;

    // Periodically instruct foca to send a custom broadcast
    let broadcast_tx_foca = tx_foca.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            if broadcast_tx_foca.send(Input::SendBroadcast).await.is_err() {
                break;
            }
        }
    });

    let mut runtime = foca::AccumulatingRuntime::new();
    tokio::spawn(async move {
        let mut ser_buf = Vec::new();
        let mut last_change_at = 0;

        while let Some(input) = rx_foca.recv().await {
            debug_assert_eq!(0, runtime.backlog());

            let result = match input {
                Input::Event(timer) => foca.handle_timer(timer, &mut runtime),
                Input::Data(data) => foca.handle_data(&data, &mut runtime),
                Input::Announce(dst) => foca.announce(dst, &mut runtime),
                Input::SendBroadcast => {
                    let msg = format!(
                        "Hello from {:?}! I have {} peers",
                        foca.identity(),
                        foca.num_members()
                    );

                    let key = BroadcastKey {
                        addr: foca.identity().addr,
                        version: last_change_at,
                    };
                    ser_buf.clear();
                    bincode::DefaultOptions::new()
                        .serialize_into(&mut ser_buf, &Broadcast { key, msg })
                        .expect("ser error handling");

                    // Notice that we're unconditionally adding a custom
                    // broadcast to the backlog, so there will always be some
                    // data being passed around (i.e. it's akin to a heartbeat)
                    // A complex system would have multiple kinds of broadcasts
                    // some heartbeat-like (service advertisement, node status)
                    // and some more message-like (leadership election, anti-
                    // entropy)
                    foca.add_broadcast(&ser_buf).map(|_| ())
                }
            };

            // Every public foca result yields `()` on success, so there's
            // nothing to do with Ok
            if let Err(error) = result {
                // And we'd decide what to do with each error, but Foca
                // is pretty tolerant so we just log them and pretend
                // all is fine
                tracing::error!(error = tracing::field::debug(error), "Ignored error");
            }

            // Now we react to what happened.
            // This is how we enable async: buffer one single interaction
            // and then drain the runtime.

            // First we submit everything that needs to go to the network
            while let Some((dst, data)) = runtime.to_send() {
                // ToSocketAddrs would be the fancy thing to use here
                let _ignored_send_result = tx_send_data.send((dst.addr, data)).await;
            }

            // Then schedule what needs to be scheduled
            let now = Instant::now();
            while let Some((delay, event)) = runtime.to_schedule() {
                scheduler
                    .send((now + delay, event))
                    .expect("error handling");
            }

            // And finally react to notifications.
            //
            // Here we could do smarter things to keep other actors in
            // the system up-to-date with the cluster state.
            // We could, for example:
            //
            //  * Have a broadcast channel where we submit the MemberUp
            //    and MemberDown notifications to everyone and each one
            //    keeps a lock-free version of the list
            //
            //  * Update a shared/locked Vec that every consumer has
            //    read access
            //
            // But since this is an agent, we simply write to a file
            // so other proccesses periodically open()/read()/close()
            // to figure out the cluster members.
            let mut active_list_has_changed = false;
            while let Some(notification) = runtime.to_notify() {
                match notification {
                    Notification::MemberUp(_) | Notification::MemberDown(_) => {
                        active_list_has_changed = true;
                        last_change_at = secs_since_epoch();
                    }
                    Notification::Idle => {
                        tracing::info!("cluster empty");
                    }
                    Notification::Rename(old, new) => {
                        tracing::info!("member {old:?} is now known as {new:?}");
                    }

                    other => {
                        tracing::debug!(notification = tracing::field::debug(other), "Unhandled")
                    }
                }
            }

            if active_list_has_changed {
                do_the_file_replace_dance(&filename, foca.iter_members().map(|m| &m.id().addr))
                    .expect("Can write the file alright");
            }
        }
    });

    // Foca is running, we can tell it to announce to our target
    if let Some(dst) = announce_to {
        let _ignored_send_error = tx_foca.send(Input::Announce(dst)).await;
    }

    // And finally, we receive forever
    let mut databuf = BytesMut::new();
    loop {
        let (len, _from_addr) = socket.recv_from(&mut recv_buf).await?;
        // Accordingly, we would undo everything that's done prior to
        // sending: decompress, decrypt, remove the envelope
        databuf.put_slice(&recv_buf[..len]);
        // And simply forward it to foca
        let _ignored_send_error = tx_foca.send(Input::Data(databuf.split().freeze())).await;
    }
}

enum Input<T> {
    Data(Bytes),
    Announce(T),
    Event(Timer<T>),
    SendBroadcast,
}

async fn launch_scheduler(
    timer_tx: mpsc::Sender<Input<ID>>,
) -> mpsc::UnboundedSender<(Instant, Timer<ID>)> {
    // Unbounded so we don't worry about deadlocks: this is intended to
    // be used alongside the receiving end of `timer_tx`, so we don't
    // want to end up in a situation where we're handling `timer_rx` and
    // need to submit data to the scheduler but its buffer is full
    // Since timer events are unlikely to ever go wild, this is a better
    // approach than having a large mostly unused buffer whislt still not
    // being sure we're deadlock safe.
    // Since the `timer_tx` handler is the only thing that submits events
    // the buffer growth is effectivelly bound
    let (tx, mut rx) = mpsc::unbounded_channel::<(Instant, Timer<ID>)>();

    let mut queue = TimerQueue::new();
    tokio::spawn(async move {
        'handler: loop {
            let now = Instant::now();

            macro_rules! submit_event {
                ($event:expr) => {
                    if let Err(err) = timer_tx.send(Input::Event($event)).await {
                        tracing::error!(
                            err = tracing::field::debug(err),
                            "Error submitting timer event. Shutting down timer task"
                        );
                        rx.close();
                        break 'handler;
                    }
                };
                ($when:expr, $event:expr) => {
                    if $when < now {
                        submit_event!($event);
                    } else {
                        queue.enqueue($when, $event);
                    }
                };
            }

            // XXX Maybe watch for large `now - _ins` deltas to detect runtime lag
            while let Some((_ins, event)) = queue.pop_next(&now) {
                submit_event!(event);
            }

            // If the queue is not empty, we have a deadline: can only
            // wait until we reach `wake_at`
            if let Some(wake_at) = queue.next_deadline() {
                // wait for input OR sleep
                let sleep_fut = sleep_until(*wake_at);
                let recv_fut = rx.recv();

                tokio::select! {
                    _ = sleep_fut => {
                        // woke up after deadline, time to handle events
                        continue 'handler;
                    },
                    maybe = recv_fut => {
                        if maybe.is_none() {
                            // channel closed
                            break 'handler;
                        }
                        let (when, event) = maybe.expect("checked for None already");
                        submit_event!(when, event);
                    }
                };
            } else {
                // Otherwise we'll wait until someone submits a new deadline
                if let Some((when, event)) = rx.recv().await {
                    submit_event!(when, event);
                } else {
                    // channel closed
                    break 'handler;
                }
            }
        }
    });

    tx
}

// Just a (Instant, Timer) min-heap
struct TimerQueue(BinaryHeap<Reverse<(Instant, Timer<ID>)>>);

impl TimerQueue {
    fn new() -> Self {
        Self(Default::default())
    }

    fn next_deadline(&self) -> Option<&Instant> {
        self.0.peek().map(|Reverse((deadline, _))| deadline)
    }

    fn enqueue(&mut self, deadline: Instant, event: Timer<ID>) {
        self.0.push(Reverse((deadline, event)));
    }

    fn pop_next(&mut self, deadline: &Instant) -> Option<(Instant, Timer<ID>)> {
        if self
            .0
            .peek()
            .map(|Reverse((when, _))| when < deadline)
            .unwrap_or(false)
        {
            self.0.pop().map(|Reverse(inner)| inner)
        } else {
            None
        }
    }
}

fn secs_since_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BroadcastKey {
    addr: SocketAddr,
    version: u64,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Broadcast {
    key: BroadcastKey,
    msg: String,
}

impl foca::Invalidates for BroadcastKey {
    fn invalidates(&self, other: &Self) -> bool {
        self.addr == other.addr && self.version > other.version
    }
}

struct Handler {
    messages: HashMap<SocketAddr, (u64, String)>,
    opts: bincode::DefaultOptions,
}

impl Handler {
    fn new() -> Self {
        Self {
            messages: Default::default(),
            opts: bincode::DefaultOptions::new(),
        }
    }
}

impl foca::BroadcastHandler<ID> for Handler {
    type Key = BroadcastKey;

    type Error = String;

    fn receive_item(
        &mut self,
        data: &[u8],
        _sender: Option<&ID>,
    ) -> Result<Option<Self::Key>, Self::Error> {
        let mut reader = data.reader();

        // In this contrived example, we decode the whole broadcast
        // directly. Ideally, one would first decode just the key
        // so that you can quickly verify if there's a need to
        // decode the rest of the payload.
        let Broadcast { key, msg }: Broadcast = self
            .opts
            .deserialize_from(&mut reader)
            .map_err(|err| format!("bad broadcast: {err}"))?;

        let is_new_message = self
            .messages
            .get(&key.addr)
            // If we already have info about the node, check if the version
            // is newer
            .map(|(cur_version, _)| cur_version < &key.version)
            .unwrap_or(true);

        if is_new_message {
            tracing::info!(
                payload = tracing::field::debug(&msg),
                "new custom broadcast",
            );

            if let Some(previous) = self.messages.insert(key.addr, (key.version, msg)) {
                tracing::debug!(previous = tracing::field::debug(&previous), "old node data");
            }

            Ok(Some(key))
        } else {
            tracing::trace!(
                node = tracing::field::debug(key.addr),
                version = tracing::field::debug(key.version),
                payload = tracing::field::debug(&msg),
                "discarded previously seen message"
            );
            Ok(None)
        }
    }
}
