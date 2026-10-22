# Clustering

Several nodes as one broker: identity, joining, failure detection, and what a partition does.

## What this node is called, and what identifies it


```toml
[node]
name = "edge-1"        # the default is "local"
```

**The name is not the identity.** Section 8.1 gives a node a UUID
minted the first time it starts and stored in its store directory, and
that UUID is what singleton ownership and every conflict tie-break
order by. The name is what an operator reads beside it, and nothing
compares one — two copied configurations share a name, and an ordering
that is not antisymmetric leaves both sides believing they won.

So changing `node.name` renames the node and does not re-found it: the
stored UUID does not move. `SDMQTT_NODE_NAME` sets the same value. An
empty name is refused rather than read as absent, for the reason an
empty `admin.token` is.

**A node is its store directory.** Copying one, image and all, copies
the UUID with it, and two nodes with one identity is the thing
conflict resolution cannot survive. Start a second node with a store
directory of its own; a node that receives a greeting carrying its own
identity refuses the link and says so.

## Joining a cluster


```toml
[cluster]
secret = "the-same-on-every-node"   # or SDMQTT_CLUSTER_SECRET
address = "0.0.0.0:7700"            # where peers connect; unset means no cluster
advertise = "10.0.0.4:7700"         # what peers should dial
seeds = ["10.0.0.5:7700", "10.0.0.6:7700"]
```

**`cluster.address` is the switch.** Without it a node does not
cluster, which is the default and a perfectly good way to run this
broker. With it, `cluster.secret` is compulsory: there is no
unauthenticated mode and no loopback exemption, because a link grants
session takeover and unauthenticated writes to every replicated
register. An empty secret is refused for being empty rather than read
as absent, as an empty `admin.token` is.

**`advertise` is what peers dial, and it is not the bind address.** A
node bound to every interface, behind a port mapping, or behind a NAT
has a bind address no peer can use. Left out, the bind address is
advertised, which is right when you bind one reachable address;
binding `0.0.0.0` with nothing advertised is refused, because nothing
here can guess which interface you meant. The address a node
advertises is also how its peers find it again after a link is lost,
including peers that were never given it as a seed.

**Clustering needs a store.** A node's identity is minted once and
written down, so a node with no `storage.directory` would be a new
node to its peers after every restart. That configuration is refused
rather than started.

Seeds are where to look, not who is there. **One seed is enough**:
nodes tell each other which members they can hear and the address each
advertised, so a node that reaches one member is soon linked to every
live one. That tells a node only where to dial — every link still has
to prove it holds the secret — and a node you have forgotten through
`DELETE /api/v1/nodes/{id}` is not dialled again because another node
still knows it; it is met again only if it connects by itself. A seed
list naming this node is harmless — the handshake recognises it and the
node stops dialling itself — so the same file can go on every node. Two nodes
that dial each other at the same moment settle on **one** link: the
one dialled by the lower identity, which both ends work out from the
two identities alone.

Failure detection uses the link. A closed socket is immediate. A
connection that is open and silent is covered by a heartbeat every
`heartbeat_ms` (1000): a peer is suspect after `suspect_after` (3)
missed beats and dead after `dead_after_ms` (10000). A peer declared
dead over a connection that is still there comes back alive on that
same connection the moment it speaks again; nothing reconnects.
`reconnect_min_ms`, `reconnect_max_ms` and `refused_backoff_ms` are
the backoff, and the last of those is much longer on purpose: a peer
that refuses the secret is a configuration error that will not fix
itself.

A write a node made reaches every node, even one that missed it and
can no longer ask the writer: every couple of seconds each node asks a
random peer what it is missing, and a peer answers for a node it cannot
hear from either — a dead one, or one that died before the asker joined
— by sending its copy of every replicated record. A node a peer can still
hear is not answered for; the asker reads that node itself once it can.

`SDMQTT_CLUSTER_NAME`, `SDMQTT_CLUSTER_SECRET`,
`SDMQTT_CLUSTER_ADDRESS`, `SDMQTT_CLUSTER_ADVERTISE` and
`SDMQTT_CLUSTER_SEEDS` (comma separated) set the same values.
`cluster.name` is bound into every handshake, so a staging node
configured with a production secret still cannot join production.

`GET /api/v1/nodes` lists what this node can see, and
`GET /health/cluster` says whether that view is complete. On a node
that replicates, the latter also carries `replication`: the mode, the
sessions this node streams and the copies it holds, the lag, the
repair numbers, the unconfirmed count, and a row per peer saying how
many of this node's sessions that peer holds a confirmed copy of, how
far behind it is, and how many of its sessions this node holds. The
Cluster page shows all of it beside the membership table; every
number is this node's own, for the reason every state on that page is.

## A partition: keep serving, or refuse on purpose


A node cut off from the rest of the cluster keeps serving the clients
that can still reach it. That is the default and there is nothing to
set for it: a broker that refused them would turn one node's network
fault into every client's outage.

If you would rather lose availability than accept a write the other
side cannot see, say which operations to refuse:

```toml
[cluster]
expected_size = 3                 # how many nodes this cluster has

[cluster.strict]
connects = true                   # refuse new clients while short
publishes = false
subscribes = false
```

Each flag refuses the one operation it names. A refused connect is
answered with reason code 0x88 — "server unavailable", and return code
3 to a 3.1.1 client — so the client knows to try another node. A
refused subscribe is answered with 0x83 per filter, and the connection
stays open. A refused publish is answered with 0x83 in its PUBACK or
PUBREC where the protocol has one to put a code in; at at-most-once,
and for a 3.1.1 publisher at any level, there is no acknowledgement
that can carry a reason — a 3.1.1 PUBACK means *accepted* — so the
connection is closed instead, with a DISCONNECT 0x83 for an MQTT 5
client. Those closes are counted as
`sdmqtt_sockets_closed_total{outcome="not_whole"}`.

**`expected_size` is the only thing that can say what "whole" means,
so a flag set without it is refused at startup.** Membership is
learnt from seeds, so the nodes a node has met cannot answer the
question: three nodes that never heard of a fourth look exactly like
four with one down. A flag on a node with no `cluster.address` is
refused for the same reason — it is a cluster of one for ever, so the
flag could never fire.

More nodes answering than expected is not short of anything, so
growing a cluster before you edit the key is not an outage.
`GET /health/cluster` reports `expected_size` as its `expected` when
it is set, and the count of nodes met when it is not, which is what it
has always reported.

`SDMQTT_CLUSTER_EXPECTED_SIZE`, `SDMQTT_CLUSTER_STRICT_CONNECTS`,
`SDMQTT_CLUSTER_STRICT_PUBLISHES` and
`SDMQTT_CLUSTER_STRICT_SUBSCRIBES` set the same values; the three
flags take `true` or `false` and nothing else.
