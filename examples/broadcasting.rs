/* Any copyright is dedicated to the Public Domain.
 * https://creativecommons.org/publicdomain/zero/1.0/ */
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::SystemTime,
};

use bincode::Options;
use bytes::Buf;
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

impl Invalidates for Tag {
    // I think this is where confusion happens: It's about invalidating
    // items ALREADY in the broadcast buffer, i.e.: foca uses this
    // to manage its broadcast buffer so it can stop talking about unecessary
    // (invalidated) data.
    fn invalidates(&self, other: &Self) -> bool {
        match (self, other) {
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

// XXX Use actually useful types
type Operation = String;
type NodeConfig = String;

struct Handler {
    seen_op_ids: HashSet<Uuid>,
    node_config: HashMap<SocketAddr, (SystemTime, NodeConfig)>,
}

impl<T> BroadcastHandler<T> for Handler {
    type Key = Tag;
    type Error = String;

    fn receive_item(
        &mut self,
        data: &[u8],
        _sender: Option<&T>,
    ) -> Result<Option<Self::Key>, Self::Error> {
        // There is exactly one broadcast within `data`
        // from craft_broadcast(), first comes the tag
        let opts = bincode::DefaultOptions::new();
        let mut reader = data.reader();
        let tag: Tag = opts
            .deserialize_from(&mut reader)
            .map_err(|err| format!("bad data: {err}"))?;

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

                #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                let payload: Operation = opts
                    .deserialize_from(&mut reader)
                    .map_err(|err| format!("bad operation payload: {err}"))?;

                // Do something real with the data
                #[cfg(feature = "tracing")]
                tracing::info!("Operation {operation_id} {payload}");

                // This WAS new information, so we signal it to foca
                Ok(Some(tag))
            }
            Tag::NodeConfig { node, version } => {
                let new_data = self
                    .node_config
                    .get(&node)
                    // If we already have info about the node, check if the version
                    // is newer
                    .map(|(cur_version, _)| cur_version < &version)
                    .unwrap_or(true);

                if new_data {
                    let payload: NodeConfig = opts
                        .deserialize_from(&mut reader)
                        .map_err(|err| format!("bad nodeconfig payload: {err}"))?;

                    #[cfg(feature = "tracing")]
                    tracing::info!(
                        node = tracing::field::debug(node),
                        version = tracing::field::debug(version),
                        payload = tracing::field::debug(&payload),
                        "new data"
                    );

                    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
                    if let Some(previous) = self.node_config.insert(node, (version, payload)) {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            previous = tracing::field::debug(&previous),
                            "old node data"
                        );
                    }

                    Ok(Some(tag))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

fn main() {}
