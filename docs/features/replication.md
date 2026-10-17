# Queue replication and failover

Each session's queue on more than one node, synchronous or asynchronous, repaired when a node is gone and pulled from a copy when the owner dies.

## How many nodes hold a session's queue


```toml
[cluster]
replication_factor = 2            # the owner, and one follower
```

Two by default. Which nodes they are is worked out from the session's
identity and the live members, with no coordination and no table: when
a member leaves, only the sessions that were on it move.

One keeps no follower and replicates nothing. Three is the most this
broker takes, because every copy costs a full copy of the durable
queue data and proportionate traffic between nodes. A cluster smaller
than the factor is not an error — the set is as large as membership
allows, and it is repaired when the other nodes arrive.

`SDMQTT_CLUSTER_REPLICATION_FACTOR` sets the same value. A number
outside one to three is refused at startup rather than clamped.

## When a durable publish is acknowledged


```toml
[cluster]
durability = "async"              # or "sync"
sync_timeout_ms = 2000            # how long sync waits for a copy
```

`async` is the default: a publish is acknowledged once *this* node has
it on disk, and the copy follows behind. What a sudden loss of the node
would cost is therefore whatever has not been copied yet, which is
`sdmqtt_replication_lag` — watch that series, and alert on it.

`sync` acknowledges once **one** follower has confirmed it, so a single
node failing loses nothing. The cost is one local round trip per
durable publish. One follower and not all of them: waiting for every
copy would make the slowest one every publisher's latency.

**With no follower to ask, a synchronous publish is acknowledged
anyway** — a cluster of one, `replication_factor = 1`, or every
follower down — and counted as
`sdmqtt_publishes_unconfirmed_total`. Refusing instead would turn the
loss of one node into an outage for every durable publish. If that is
the trade you want:

```toml
[cluster.strict]
unconfirmed = true                # refuse rather than acknowledge
```

The refusal closes the connection with reason code 0x83, as the other
"this broker will not answer for your message" refusals do. The flag
is refused at startup when `durability` is `"async"` — nothing would
ever be unconfirmed — and when the node does not cluster, where there
can never be a follower.

The same mode applies to a publish made through `POST /api/v1/publish`
and through an ingest rule: in `sync` the answer waits for a copy as
a client's acknowledgement does, an unconfirmed one is counted on the
same series, and with the strict flag it is refused with `503` and
the code `unavailable`. The message is kept on this node either way;
what is refused is the promise, and a retry is a duplicate, which is
what at-least-once has always meant.

**Both modes describe the node that accepted the publish.** A publish
accepted by one node for a session owned by another is forwarded over
the mesh, and the accepting node answers when *it* is done — the
owner has not appended yet, and no copy of the owner's has either. So
a `sync` acknowledgement from a node that is not the owner is not
evidence of a copy, and the lag on the owner does not count what is
still in flight towards it. Publish to the owner for the guarantee as
written: a client connected to the node its session lives on, which is
the ordinary case, gets exactly it.

`SDMQTT_CLUSTER_DURABILITY`, `SDMQTT_CLUSTER_SYNC_TIMEOUT_MS` and
`SDMQTT_CLUSTER_STRICT_UNCONFIRMED` set the same three.

Per listener: `listener.durability = "sync"` (or `"async"`) overrides
the node's mode for publishes arriving on that listener, so a listener
carrying commands can wait for a copy while one carrying telemetry does
not. The timeout is the node's in both cases. A listener set to `sync`
on a node that does not cluster is refused at startup, for the reason
the strict flag is. `SDMQTT_LISTENER_DURABILITY` sets it.

## When a node is gone for good


```toml
[cluster]
gone_after_ms = 3600000           # an hour, by default
repair_sessions_per_second = 50
```

A node that is down keeps its place in every replica set until
`gone_after_ms` runs out — short by one copy while it is away, which
`sdmqtt_replication_lag` and `sdmqtt_publishes_unconfirmed_total`
both show — and is then given up on: the sets that included it are
repaired onto the nodes that remain, each session's queue sent to its
new follower, at most `repair_sessions_per_second` of them a second.
An hour, because a node that is down is usually a node restarting, and
copying the whole queue estate every time somebody applies a patch is
the thing the grace prevents.

If you know a node is never coming back, do not wait:

```bash
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:1884/api/v1/nodes/<id>
```

That forgets it on **this node only** — membership is not replicated —
and repair starts here at once. Tell each surviving node, or let the
grace handle the ones you did not. A forgotten node that comes back is
re-admitted. `sdmqtt_replica_repairs_pending` is how many sessions
still need a copy sent somewhere, and `sdmqtt_replica_repairs_done_total`
how many have been.

`SDMQTT_CLUSTER_GONE_AFTER_MS` and
`SDMQTT_CLUSTER_REPAIR_SESSIONS_PER_SECOND` set the same two.

## What happens when a node dies


A client whose node has gone reconnects to any other node, and gets
its queue back from whichever copy is furthest ahead. Nothing to
configure: the node it connects to first asks the old owner, and when
that cannot be reached asks the copies. `sdmqtt_sessions_pulled_total`
counts how often a copy was what answered;
`sdmqtt_migrated_messages_missing_total` is what was lost after it did,
which with a factor of two and one node down is nothing.

A copy holds the queue and only the queue. The client's subscriptions
are not in it, so a client that has failed over re-subscribes — as it
would after any session loss — and then receives what was queued.

A client whose node is alive can also reconnect elsewhere, and the
session moves with it: the new node claims it, the old one closes the
old connection and hands over the queue, and the subscriptions follow.
A publication forwarded to the old node while that was happening finds
the session gone and is sent on to the node that took it, counted on
`sdmqtt_forwards_redirected_total`; one that reaches the new node before
the session is installed there is held for up to two seconds and then
delivered, counted on `sdmqtt_forwards_held_total`. A forwarded
publication nothing on a node wants and nothing can be done with is
counted on `sdmqtt_forwards_unroutable_total`; a steady stream of those means a
node's view of who subscribes where has stopped converging.

Messages published on either side of a partition are not exchanged
afterwards, whatever the flags say, and a node that acknowledged a
message it never managed to replicate loses it when it rejoins.
Closing that window needs synchronous replication of the queue, which
this build does not do yet: a publish would then not be acknowledged
until a replica held it. Eliminating it entirely would need consensus,
which this design does not take on.
