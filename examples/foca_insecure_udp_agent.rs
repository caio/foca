/* Any copyright is dedicated to the Public Domain.
 * https://creativecommons.org/publicdomain/zero/1.0/ */
use std::{
    collections::HashMap, fs::File, io::Write, net::SocketAddr, path::Path, str::FromStr,
    sync::Arc, time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use clap::{App, Arg};
use rand::{rngs::StdRng, SeedableRng};
use tokio::{net::UdpSocket, sync::mpsc};

use foca::{Config, Foca, Identity, Notification, PostcardCodec, Runtime, Timer};

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
            .unwrap_or_else(|| format!("foca_cluster_members.{}.txt", rand::random::<u64>()));

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
    bump: u16,
}

// We implement a custom, simpler Debug format just to make the tracing
// output cuter
impl std::fmt::Debug for ID {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("ID").field(&self.addr).finish()
    }
}

impl ID {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            bump: rand::random(),
        }
    }
}

impl Identity for ID {
    // Since a client outside the cluster will not be aware of our
    // `bump` field, we implement the optional trait method
    // `has_same_prefix` to allow anyone that knows our `addr`
    // to join our cluster.
    fn has_same_prefix(&self, other: &Self) -> bool {
        self.addr.eq(&other.addr)
    }

    // And by implementing `renew` we enable automatic rejoining:
    // when another member declares us as down, Foca immediatelly
    // switches to this new identity and rejoins the cluster for us
    fn renew(&self) -> Option<Self> {
        Some(Self {
            addr: self.addr,
            bump: self.bump.wrapping_add(1),
        })
    }
}

struct AccumulatingRuntime<T> {
    pub to_send: Vec<(T, Bytes)>,
    pub to_schedule: Vec<(Duration, Timer<T>)>,
    pub notifications: Vec<Notification<T>>,
    buf: BytesMut,
}

impl<T: Identity> Runtime<T> for AccumulatingRuntime<T> {
    // Notice that we'll interact to these via pop(), so we're taking
    // them in reverse order of when it happened.
    // That's perfectly fine, the order of items from a single interaction
    // is irrelevant. A "nicer" implementation could use VecDeque or
    // react directly here instead of accumulating.

    fn notify(&mut self, notification: Notification<T>) {
        self.notifications.push(notification);
    }

    fn send_to(&mut self, to: T, data: &[u8]) {
        let mut packet = self.buf.split();
        packet.put_slice(data);
        self.to_send.push((to, packet.freeze()));
    }

    fn submit_after(&mut self, event: Timer<T>, after: Duration) {
        // We could spawn+sleep here
        self.to_schedule.push((after, event));
    }
}

impl<T> AccumulatingRuntime<T> {
    pub fn new() -> Self {
        Self {
            to_send: Vec::new(),
            to_schedule: Vec::new(),
            notifications: Vec::new(),
            buf: BytesMut::new(),
        }
    }

    pub fn backlog(&self) -> usize {
        self.to_send.len() + self.to_schedule.len() + self.notifications.len()
    }
}

// Our identity is a composite of a socket address and extra
// stuff, but downstream consumers likely only care about
// the address part.
//
// It's perfectly valid to temprarily have more than one member
// pointing at the same address (with a different `bump`): one
// could, for example: join the cluster, ^C the program and
// immediatelly join again. Before Foca detects that the previous
// identity is down we'll receive a notification about this new
// identity going up.
//
// So what we maintain here is a HashMap of addresses to an
// occurence count:
//
//  * The count will most of the time be 1;
//  * But in scenarios like above it may reach 2. Meaning:
//    something made the address change identities, but
//    it's still active
//  * And when the count reaches 0 the address is actually
//    down, so we remove it
//
struct Members(HashMap<SocketAddr, u8>);

impl Members {
    fn new() -> Self {
        Self(HashMap::new())
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    fn add_member(&mut self, member: ID) -> bool {
        // Notice how we don't care at all about the `bump` part.
        // It's only useful for Foca.
        let counter = self.0.entry(member.addr).or_insert(0);

        *counter += 1;

        counter == &1
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    fn remove_member(&mut self, member: ID) -> bool {
        let effectivelly_down = if let Some(counter) = self.0.get_mut(&member.addr) {
            *counter -= 1;

            counter == &0
        } else {
            // Shouldn't happen
            false
        };

        if effectivelly_down {
            self.0.remove(&member.addr);
        }

        effectivelly_down
    }

    fn addrs(&self) -> impl Iterator<Item = &SocketAddr> {
        self.0.keys()
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

    // It's really confusing how fmt().init() and fmt::init()
    // behave differently, but hey...
    // When RUST_LOG is unset, default to Level::INFO
    if std::env::var("RUST_LOG").is_err() {
        tracing_subscriber::fmt().init();
    } else {
        // Else use whatever it is set to, which defaults to Level::ERROR
        tracing_subscriber::fmt::init();
    }

    tracing::info!(?params, "Started");

    let CliParams {
        bind_addr,
        identity,
        announce_to,
        filename,
    } = params;

    let rng = StdRng::from_entropy();
    let config = Config::simple();

    let buf_len = config.max_packet_size.get();
    let mut recv_buf = vec![0u8; buf_len];

    let mut foca = Foca::new(identity, config, rng, PostcardCodec);
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

    // We'll also launch a task to manage Foca. Since there are timers
    // involved, one simple way to do it is unifying the input:
    enum Input<T> {
        Event(Timer<T>),
        Data(Bytes),
        Announce(T),
    }
    // And communicating via channels
    let (tx_foca, mut rx_foca) = mpsc::channel(100);
    // Another alternative would be putting a Lock around Foca, but
    // yours truly likes to hide behind (the lock inside) channels
    // instead.
    let mut runtime = AccumulatingRuntime::new();
    let mut members = Members::new();
    let tx_foca_copy = tx_foca.clone();
    tokio::spawn(async move {
        while let Some(input) = rx_foca.recv().await {
            debug_assert_eq!(0, runtime.backlog());

            let result = match input {
                Input::Event(timer) => foca.handle_timer(timer, &mut runtime),
                Input::Data(data) => foca.handle_data(&data, &mut runtime),
                Input::Announce(dst) => foca.announce(dst, &mut runtime),
            };

            // Every public foca result yields `()` on success, so there's
            // nothing to do with Ok
            if let Err(error) = result {
                // And we'd decide what to do with each error, but Foca
                // is pretty tolerant so we just log them and pretend
                // all is fine
                eprintln!("Ignored Error: {}", error);
            }

            // Now we react to what happened.
            // This is how we enable async: buffer one single interaction
            // and then drain the runtime.

            // First we submit everything that needs to go to the network
            while let Some((dst, data)) = runtime.to_send.pop() {
                // ToSocketAddrs would be the fancy thing to use here
                let _ignored_send_result = tx_send_data.send((dst.addr, data)).await;
            }

            // Then schedule what needs to be scheduled
            while let Some((delay, event)) = runtime.to_schedule.pop() {
                let own_input_handle = tx_foca_copy.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ignored_send_error = own_input_handle.send(Input::Event(event)).await;
                });
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
            while let Some(notification) = runtime.notifications.pop() {
                match notification {
                    Notification::MemberUp(id) => {
                        tracing::info!(?id, "Member Up");
                        active_list_has_changed |= members.add_member(id)
                    }
                    Notification::MemberDown(id) => {
                        tracing::info!(?id, "Member Down");
                        active_list_has_changed |= members.remove_member(id)
                    }

                    other => {
                        tracing::debug!(notification = ?other, "Unhandled")
                    }
                }
            }

            if active_list_has_changed {
                do_the_file_replace_dance(&filename, members.addrs())
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
        // Accordinly, we would undo everything that's done prior to
        // sending: decompress, decrypt, remove the envelope
        databuf.put_slice(&recv_buf[..len]);
        // And simply forward it to foca
        let _ignored_send_error = tx_foca.send(Input::Data(databuf.split().freeze())).await;
    }
}
