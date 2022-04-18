/* Any copyright is dedicated to the Public Domain.
 * https://creativecommons.org/publicdomain/zero/1.0/ */
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::SystemTime,
};

use bincode::Options;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use foca::{BroadcastHandler, Invalidates};

// Broadcasts here will always have the following shape:
//
// 0. u16 length prefix
// 1. Tag describing the payload
// 2. Payload
//

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum Tag {
    // We can propagate general-purpose operations. Foca shouldn't
    // care about what's inside the payload, just wether this
    // has been acted on already or not.
    // Side-effects, conflict resolution and whatnot are not its
    // responsibilities, so messages like these aren't invalidated
    // at all: everything NEW it receives will be broadcast.
    Operation {
        operation_id: Uuid,
        // Depending on the nature of the opeartions, this could
        // use more metadata.
        // E.g.: members may receive operations out of order;
        // If the storage doesn't handle that correctly you'll
        // need to do it yourself
    },

    // For scenarios where the interactions are very clear, we can
    // be smarter with what we decide to broadcast.
    // E.g.: In our cluster we expect nodes to broadcast their
    // configuration when they join and when it changes, so for
    // any key `node`, there's only one writer (assuming no byzantine
    // behaviour): we can simply use last-write wins
    NodeConfig {
        node: SocketAddr,
        // XXX SystemTime does NOT guarantee monotonic growth.
        //     It's good enough for an example, but it's an outage
        //     waiting to happen. Use something you have better
        //     control of.
        version: SystemTime,
    },
}

struct Broadcast {
    tag: Tag,
    data: Bytes,
}

impl Invalidates for Broadcast {
    // I think this is where confusion happens: It's about invalidating
    // items ALREADY in the broadcast buffer, i.e.: foca uses this
    // to manage its broadcast buffer so it can stop talking about unecessary
    // (invalidated) data.
    fn invalidates(&self, other: &Self) -> bool {
        match (self.tag, other.tag) {
            // The only time we care about invalidation is when we have
            // a new nodeconfig for a node and are already broadcasting
            // a config about this same node. We need to decide which
            // to keep.
            (
                Tag::NodeConfig {
                    node: self_node,
                    version: self_version,
                },
                Tag::NodeConfig {
                    node: other_node,
                    version: other_version,
                },
            ) if self_node == other_node => self_version > other_version,
            // Any other case we'll keep broadcasting until it gets sent
            // `Config::max_transmissions` times (or gets invalidated)
            _ => false,
        }
    }
}

impl AsRef<[u8]> for Broadcast {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

// XXX Use actually useful types
type Operation = String;
type NodeConfig = String;

struct Handler {
    buffer: BytesMut,
    seen_op_ids: HashSet<Uuid>,
    node_config: HashMap<SocketAddr, (SystemTime, NodeConfig)>,
}

impl Handler {
    fn craft_broadcast<T: Serialize>(&mut self, tag: Tag, item: T) -> Broadcast {
        self.buffer.reserve(1400);
        let mut crafted = self.buffer.split();

        // The payload length. We'll circle back and update it to
        // a real value at the end
        crafted.put_u16(0);

        let mut writer = crafted.writer();

        let opts = bincode::DefaultOptions::new();
        opts.serialize_into(&mut writer, &tag)
            .expect("error handling");

        opts.serialize_into(&mut writer, &item)
            .expect("error handling");

        let mut crafted = writer.into_inner();
        let final_len = crafted.len() as u16;
        (&mut crafted[0..1]).put_u16(final_len);

        // Notice that `tag` here is already inside `data`,
        // we keep a copy outside to make it easier when implementing
        // `Invalidates`
        Broadcast {
            tag,
            data: crafted.freeze(),
        }
    }
}

impl<T> BroadcastHandler<T> for Handler {
    type Broadcast = Broadcast;
    type Error = String;

    fn receive_item(
        &mut self,
        data: impl bytes::Buf,
    ) -> Result<Option<Self::Broadcast>, Self::Error> {
        // Broadcast payload is u16-length prefixed
        if data.remaining() < 2 {
            return Err(String::from("Not enough bytes"));
        }

        let mut cursor = data;
        let len = cursor.get_u16();
        if cursor.remaining() < usize::from(len) {
            return Err(String::from("Malformed packet"));
        }

        // And a tag/header that tells us what the remaining
        // bytes actually are. We leave the blob untouched until
        // we decide wether we care about it.
        let opts = bincode::DefaultOptions::new();
        let mut reader = cursor.reader();
        let tag: Tag = opts.deserialize_from(&mut reader).unwrap();

        // Now `reader` points at the actual useful data in
        // the buffer, immediatelly after the tag. We can finally
        // make a decision
        match tag {
            Tag::Operation { operation_id } => {
                if self.seen_op_ids.contains(&operation_id) {
                    // We've seen this data before, nothing to do
                    return Ok(None);
                }

                self.seen_op_ids.insert(operation_id);

                let op: Operation = opts.deserialize_from(&mut reader).expect("error handling");
                {
                    // This is where foca stops caring
                    // If it were me, I'd stuff the bytes as-is into a channel
                    // and have a separate task/thread consuming it.
                    do_something_with_the_data()
                }

                // This WAS new information, so we signal it to foca
                let broadcast = self.craft_broadcast(tag, op);
                Ok(Some(broadcast))
            }
            Tag::NodeConfig { node, version } => {
                if let Some((current_version, _)) = self.node_config.get(&node) {
                    if &version > current_version {
                        let conf: NodeConfig =
                            opts.deserialize_from(&mut reader).expect("error handling");
                        Ok(Some(self.craft_broadcast(tag, conf)))
                    } else {
                        Ok(None)
                    }
                } else {
                    let conf: NodeConfig =
                        opts.deserialize_from(&mut reader).expect("error handling");
                    Ok(Some(self.craft_broadcast(tag, conf)))
                }
            }
        }
    }
}

fn do_something_with_the_data() {
    unimplemented!()
}

fn main() {}
