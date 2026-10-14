# sd-mqtt

A distributed MQTT broker with a built-in admin UI and UI-configurable
integrations. One binary, no external dependencies.

Status: in development. See `docs/superpowers/specs/` for the design and
`docs/superpowers/plans/` for the implementation plans.
`CHANGELOG.md` is what to read before upgrading: while nothing is
released the stored format is allowed to move, and a store written by an
earlier build is refused rather than migrated.

## Build

```bash
cd services/broker
cargo build --release
```

## Configuration

`sdmqtt serve` with no arguments and no configuration file starts a
working broker on the registered MQTT port. Everything below is
optional, and the keys are documented in full in
`services/broker/crates/sdmqtt/src/config.rs`. Three sources are layered
in this order: `sdmqtt.toml`, then `SDMQTT_*` in the environment, then a
flag.

### Persistence is off unless you ask for it

```toml
[storage]
directory = "/var/lib/sdmqtt"   # no default: without it, nothing is stored
sync = "immediate"              # or "relaxed", which is for measuring only
flush_interval_ms = 2
flush_records = 1000
```

**With no `storage.directory` the broker writes nothing down.** Sessions,
their subscriptions and their queued messages last as long as the
process and no longer, and starting this way logs a warning saying so.
That is deliberate: refusing to start without a directory would break
the promise that no configuration is a working configuration, and
defaulting to one would drop a database file into whatever directory the
service happened to start in.

Give it a directory and a restart preserves durable sessions, their
subscriptions and their queued messages, and no acknowledgement is sent
to a publisher before its message is on the disk.

The directory is created if it is not there, and the store is the file
`sdmqtt.redb` inside it. One broker per directory: a second is refused
with the path it could not take.

**A store written by an earlier build is refused rather than migrated.**
The stored format carries a version and the check on it is exact, so a
broker that meets a store it does not know refuses to start and names
both versions:

```
store format version 3 is not readable by this build, which reads version 4.
The store is not damaged; it was written by a different build. Nothing is
released yet, so there is no migration: see CHANGELOG.md for what moved and
what starting with a fresh store directory throws away.
```

The file is not damaged; it is being read by the wrong build, and
guessing at an unknown layout would restore nonsense from healthy bytes.
Nothing is released yet, so there is no migration: move or delete the
store directory and start again. `CHANGELOG.md` says, per milestone,
whether the version moved and what starting again throws away.

### Rules, and the four things one can do

A rule is one trigger and **one** action. Two actions in a table is an
operator who means two rules and is refused; none is a rule that does
nothing and is refused as well.

| table | what it does | what fires it |
|---|---|---|
| `[rules.http_out]` | posts a JSON envelope to a URL | a published message matching `filter` |
| `[rules.http_in]` | publishes the body of a post it is sent | a request to `/ingest/<id>` |
| `[rules.http_poll]` | publishes the body of a URL it fetches | its own interval |
| `[rules.mqtt_bridge]` | carries topics to and from a remote broker | its own topic entries |

`filter`, `min_qos` and `client_id` belong to a rule that a published
message fires, which is `http_out` alone. On the other three they are
refused rather than ignored, because an operator who wrote one believes
something about when their rule runs that is not true.

**Rules need `storage.directory`.** Section 9.1 makes a rule durable
metadata and its outbox hangs off it: an outbox that is not written
down is not an outbox, so a node with no store is refused a rule
rather than given one that forgets.

**A rule can also be written over the API**, which is what the Rules
page in the interface does — list, create, update, enable, disable and
delete, at `/api/v1/rules`. A file and the API go through one
validator, so the refusals below are the same refusals either way and
name the same key.

A rule read back over the API carries **no** `http_in.token`, **no**
`mqtt_bridge.password`, its `headers` with their names and empty
values, and a `url` reduced to its scheme and host with the rest
elided: those are secrets and the read resources need no token to
read. A URL is one as often as not — a chat platform's webhook is an
unguessable path and nothing else, and a key in a query string is the
same thing in another shape — and the host is what a form needs to
show you where your rule posts. An update that leaves a secret out, or
that sends a URL back exactly as it was read, keeps the stored one,
which is what makes "read a rule, change one field, write it back"
safe.

#### Post a message out: `[rules.http_out]`

A rule watches a topic filter and posts a JSON envelope to a URL.

```toml
[[rules]]
id = "telemetry"                  # one topic level: no /, + or #
name = "Telemetry out"
enabled = true                    # the default
filter = "sensor/+/temp"
min_qos = 1                       # optional: only at this level or above
client_id = "sensor-*"            # optional: only from these clients

[rules.http_out]
url = "https://example.com/hook"  # https, or http to a loopback address
method = "POST"
timeout_ms = 10000                # how long one attempt may take
retries = 3                       # attempts after the first
backoff_ms = 1000                 # doubling, and capped at five minutes
headers = { "x-token" = "secret" }
```

**A message that fires a rule is written to that rule's durable outbox
before the publisher is answered**, and the posting happens afterwards
from a task of its own. So an endpoint that is down does not slow the
broker down, and a broker that is killed with messages in flight still
delivers them when it comes back. Rules need `storage.directory`: an
outbox that is not written down is not an outbox.

**Delivery is at least once. Your endpoint will eventually see the same
message twice.** A request that times out may have arrived, been
understood and been acted on — this end cannot tell — and the choice is
between retrying and risking a duplicate or not retrying and losing a
message. sd-mqtt retries.

Every envelope therefore carries an `id` that is minted once, stored
with it, and **does not change between attempts**:

```json
{"id":"0123456789abcdeffedcba9876543210","topic":"sensor/1/temp",
 "payload":"21.5","qos":1,"retain":false,"client_id":"sensor-1",
 "timestamp":1700000000,"user_properties":[["unit","celsius"]]}
```

Remember recent identifiers and drop the repeats, or make the endpoint
idempotent. A payload that is not valid UTF-8 arrives as
`payload_base64` instead of `payload`, never both.

An envelope that fails every attempt is published to
`$sd/rules/<id>/dlq` with the reason and the attempt count, and leaves
the outbox.

**A failing endpoint is not hammered.** The backoff belongs to the rule
rather than to each envelope, so a rule with a thousand messages waiting
makes one attempt per backoff window and not a thousand at once. A
`Retry-After` in seconds is honoured over the rule's own figure, up to
five minutes.

#### Accept a post and publish it: `[rules.http_in]`

A rule that is fired by an HTTP request rather than by a message:
whatever is posted to `/ingest/<id>` is published to a topic.

```toml
[ingest]
address = "127.0.0.1:1885"   # the default
listener = "own"             # or "admin", which must be chosen explicitly

[[rules]]
id = "gateway"
name = "Gateway in"

[rules.http_in]
token = "a-secret"                 # required: this endpoint publishes
topic = "ingest/gateway/default"   # where a body goes when the request does not choose
topic_header = "x-topic"           # optional: the request may choose, from a header
# topic_query = "topic"            # or from a query parameter — never both
topic_prefix = "ingest/gateway"    # required with either, and written without a trailing /
qos = 1
retain = true
```

```bash
curl -X POST http://127.0.0.1:1885/ingest/gateway \
     -H 'Authorization: Bearer a-secret' \
     -H 'x-topic: ingest/gateway/kitchen' \
     --data 'whatever the sender sends'
```

The body is published as it arrives: this is a gateway, not a
transformer. An `id`, a `name`, a `token` and a `topic` are what a
rule of this kind must say; everything else takes the defaults above.

**The token is not optional**, because the endpoint publishes. An
unguarded one is a hole rather than a degraded mode, so a rule without
one is refused when it is written and a request without one is
answered `401`.

**`topic_prefix` is the whole safety of letting a request choose its
topic.** Without it, whoever holds this rule's token can publish
anywhere on the broker. With it, a chosen topic must lie at or under
the prefix. A prefix is a topic name with no wildcard in it, outside
the `$` namespace, and written without a trailing separator: with one
it is a prefix of three levels ending in an empty one, which no caller
could satisfy.

**The ingest listener is bound while this node has an ingest rule to
serve**, and not otherwise: it is bound the moment the first one
exists — including one created over the API or on the Rules page,
which is reachable at once and needs no restart — and closed again
when the last one is deleted or switched off. A node that does no
ingest holds no ingest port, which matters because the port is a fixed
number: bound unconditionally it would be one every broker on a host
contended for, whether or not any of them accepted a post.

An address something else is already on is therefore refused where the
rule is written, at startup or over the API, rather than accepted and
served nowhere. `ingest.listener = "admin"` serves the routes on the
admin listener instead, which must be chosen explicitly and takes that
listener's host check with it.

#### Fetch a URL on an interval: `[rules.http_poll]`

A rule fired by its own clock: fetch a URL, publish the body.

```toml
[[rules]]
id = "weather"
name = "Weather poll"

[rules.http_poll]
url = "https://example.invalid/now"   # https, or http to a loopback address
headers = { "accept" = "text/plain" }
interval_ms = 30000
timeout_ms = 2500                     # may be longer than the interval
max_body_bytes = 65536
topic = "weather/now"
qos = 1
retain = true                         # usually what a poller wants
```

`url` and `topic` are all that is required; the rest are the defaults
in `crates/rules/src/rule.rs`, which are a minute between fetches, ten
seconds for one of them and 256 KiB of body.

**There is no retry count and no backoff here**, unlike `http_out`. A
poller has no outbox: a fetch that failed is tried again on the next
interval, and a body republished late is worse than one not
republished at all.

**One fetch is ever in flight.** A fetch slower than the interval
loses the intervals it covered rather than queueing them, so an
endpoint that has become slow is not answered with a run of
back-to-back requests. A body past `max_body_bytes` is a **failed
fetch and not a truncated publication**: half a document on a topic is
worse than nothing on it, and a subscriber cannot tell the two apart.

`max_body_bytes` must be one this broker could publish: a body larger
than `broker.max_packet_size` leaves room for is refused at startup
and by the API, naming both numbers. The ingest endpoint derives its
own limit from that setting for the same reason, and a poller that
could fetch a document and never publish it is a rule that fails once
an interval for ever.

`retain = true` is usually what an operator wants from a poller — the
current value, there for a subscriber that has just connected — and is
off by default all the same, because a retained message outlives the
rule that wrote it.

A poller is a **singleton**: in a cluster the live member with the
lowest node identifier runs it, so two nodes do not both fetch.

#### Bridge to another broker: `[rules.mqtt_bridge]`

This broker connects to a remote one as an ordinary MQTT client and
carries the topics you name.

```toml
[[rules]]
id = "to-b"
name = "Bridge to the far broker"

[rules.mqtt_bridge]
address = "mqtts://remote.invalid:8883"  # mqtts, or mqtt to a loopback address
client_id = "bridge-a"                   # required on protocol_level 4
protocol_level = 5                       # 4 for MQTT 3.1.1, 5 for MQTT 5
username = "user"
password = "a-bridge-secret"
keep_alive_secs = 5
state_topic = "bridges/b/state"          # "online" or "offline", here and on the remote
reconnect_min_ms = 100
reconnect_max_ms = 1000
max_hops = 8

[[rules.mqtt_bridge.topics]]
filter = "sensors/#"                     # carrying neither prefix
direction = "out"
qos = 1
remote_prefix = "from-a/"

[[rules.mqtt_bridge.topics]]
filter = "#"
direction = "in"
qos = 1
remote_prefix = "from-b/"
local_prefix = "remote/"
```

That bridge publishes `from-a/sensors/#` to the remote and subscribes
to `from-b/#` there, putting what arrives under `remote/`. The two
sets do not meet, which is why this configuration starts at all.

**A bridge whose outbound and inbound topics intersect is refused at
startup**, with the two entries and a topic that would go round named.
That is the first of section 9.2's loop defences and it is checked
before anything connects, because MQTT 3.1.1 has no marker a
republished message could carry to be recognised later. `max_hops` is
the second: a publication above the limit is dropped, which stops a
chain of bridges nobody drew on a whiteboard.

`direction = "both"` on **one** entry publishes what it subscribes to
and is therefore refused unless the prefixes keep the two sets apart.
Two entries, as above, is the ordinary way to carry a topic each way.

**Two bridge _rules_ to one remote are checked against each other
too.** The example above is one rule with two entries, and the obvious
way to generalise it is two rules — one out, one back. If what one
carries out to a remote is what the other reads back off it, and what
the second publishes here is what the first carries out, that is the
same cycle with a rule boundary through the middle, and neither rule
is wrong on its own. It is refused at startup and by the API, naming
both rules, both entries and a topic on each side. Two rules to two
remotes are not a cycle and are accepted, and neither is a rule that
mirrors another's traffic into a local namespace nothing carries back
out.

An outbound entry may rewrite by position:
`filter = "sensors/+/temp"` with `template = "site-a/temp/{1}"`
publishes `sensors/kitchen/temp` remotely as `site-a/temp/kitchen`. A
template on an inbound entry is refused.

**Plain text to anywhere but this machine is refused**, on the same
reasoning `http_out` refuses a plain endpoint and with more at stake:
a bridge sends its password in the connect and then everything it
carries. `password` is held in a wrapper that will not print itself,
so it does not reach the log line this binary writes at startup.

**The first connection of a process asks for a clean start whatever
`clean_start` says.** The in-flight window is held in memory, so a
resumed session would hold packet identifiers the restarted process
no longer knows about.

#### Where a rule lives, and what removing one from the file does

Rules named in the configuration are written into the store at
startup, so they survive a restart. **A rule the store holds that the
configuration no longer names is installed too**, and it runs: a rule
created over the API is exactly that, and it has to fire again after a
restart. Before the API could write one, a stored rule the file had
dropped could only be an edit somebody had made and the safe thing was
to leave it alone; now it is usually a rule somebody meant, and it is
logged when it is installed.

**So deleting a `[[rules]]` table no longer stops a rule.** Delete the
rule — `DELETE /api/v1/rules/<id>`, or the button on the Rules page —
which is refused while its outbox still holds anything, because
deleting a rule deletes its outbox and what is in one is messages this
broker told somebody it had accepted. Turning a connector off while an
endpoint is down is `enabled = false`, or the disable button, which
keeps the outbox.

### Who may connect

```toml
[auth]
anonymous = true                  # false refuses a client with no username
scram_iterations = 4096           # PBKDF2 rounds for a new SCRAM-SHA-256 verifier
max_failures_per_minute = 10      # failed logins from one address before it waits; 0 is no limit
```

Users are not in the file. They are durable metadata, written through
the API and replicated to every node like a rule:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"alice","password":"hunter2"}' http://127.0.0.1:1884/api/v1/users
```

The password is hashed with argon2id for a password login and derived
into a SCRAM-SHA-256 verifier for the AUTH exchange, both at that
moment, and neither is ever returned: `GET /api/v1/users` lists names
and which credential forms exist. Posting the same name again changes
the password. `DELETE /api/v1/users/{name}` removes the user, and the
removal replicates as a tombstone. A client presenting a wrong password,
or a name nobody has, is refused with 0x87; the two are one answer, so
nobody can learn which names exist by trying. `anonymous` is a switch
and nothing else: creating the first user does not flip it. A node with
no store holds no users and admits by the default alone.
`SDMQTT_AUTH_ANONYMOUS` and `SDMQTT_AUTH_SCRAM_ITERATIONS` set the two.

**Repeated failures from one address are slowed.** After
`max_failures_per_minute` failed logins — a wrong password, an unknown
name, a failed SCRAM proof or re-authentication — a connect from that
address is refused before its credentials are looked at, with 0x9F
(`3`, server unavailable, on 3.1.1), until enough of the minute has
passed for one more try. An IPv6 address is counted by its /64, which
is what one host is usually given. The broker tracks at most 65,536
addresses and logs a warning when that is full.
`sdmqtt_authentications_total{outcome}` counts every decision,
`sdmqtt_rate_limited_total{limiter="authentication"}` the connects
turned away, and `sdmqtt_auth_failure_addresses` how many addresses are
held. `SDMQTT_AUTH_MAX_FAILURES_PER_MINUTE` sets it.

A version 5 client can authenticate without sending its password at all,
with **SCRAM-SHA-256** over the AUTH exchange (RFC 7677): it names the
method in CONNECT, the broker challenges, the client proves, and the
connect acknowledgement carries the server's own proof back. The same
exchange renews the credentials mid-session (`AUTH` with
re-authenticate), as the same user only. A wrong password and an unknown
name are refused the same way at the same step, so nobody can learn
which names exist by trying. The `-PLUS` channel-binding variant is not
offered and a client asking for it is told `0x8C`, as is any other
method. Passwords are taken as written: SASLprep is not applied, so a
password that is plain ASCII behaves exactly as RFC 7677 describes and
one that is not should be set through a client that normalises it.

### Who may use the admin API

Mutating requests to the admin listener need an **API key** with a scope
that covers them: `read` opens taps, `write` also publishes and writes
rules, `admin` also manages users, access control rules, keys and nodes.
The configured `admin.token` is a key with `admin` scope, which is how
the first key is minted:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"ci","scope":"write"}' http://127.0.0.1:1884/api/v1/keys
```

The answer carries the key, `sdmqtt_<id>_<secret>`, **once**: the broker
keeps a digest of it and nothing that would give it back. `GET
/api/v1/keys` lists identifiers, names and scopes; `DELETE
/api/v1/keys/{id}` revokes, and the revocation replicates to every node
like a user's removal. A real key that is too narrow is answered `403`
with both scopes named; a wrong one is `401`. A node with no store
holds no keys, and the token is then the only credential.

The **Settings** page does all of this in a browser: paste a key or the
token at the top — it is kept for that tab and forgotten when it
closes — and manage users, access control rules and keys below it. It
also lists this node's listeners, which stay in the file.

### Who may publish and subscribe where

```toml
[[acl]]
id = "no-secrets"
order = 10                        # lower is consulted first; 100 by default
subject = "anyone"                # or "user:alice", or "client:gateway-1"
filter = "secret/#"
subscribe = false                 # an opinion on subscribing; publish is left unsaid

[[acl]]
id = "everything-else"
subject = "anyone"
filter = "#"
publish = true
subscribe = true
```

With no rule at all, every publish and subscribe is allowed, as it was
through milestone 6. **The first rule flips the default to deny**: from
then on an operation is allowed only by a rule that says so, which is
why the second rule above exists. Rules are consulted in `order`, then
by identifier, and the first whose subject and filter match and that
has an opinion on the operation decides; a rule with no opinion is
skipped. A rule's filter matches a published topic the way a
subscription does, wildcards and all, and a requested subscription
filter the way a topic name would; a shared subscription is judged by
the filter it shares, so a rule about `fleet/#` covers
`$share/workers/fleet/#` too. A refused subscribe is answered
`0x87` on version 5 and `0x80` on 3.1.1; a refused publish is `0x87` on
its acknowledgement, and an at-most-once publish, which has none, is
dropped and counted on `sdmqtt_messages_dropped_total{reason="refused"}`.

Rules in the file are this node's configuration. Rules written through
the API — `POST /api/v1/acl` with the same fields, `GET /api/v1/acl` in
consultation order, `DELETE /api/v1/acl/{id}` — are durable metadata
and replicate to every node like a user; both sets are consulted
together by `order`.

A client never publishes into the dollar-prefixed namespace, rules or
no rules: `$SYS/...` and the like are the broker's, and a client's
publish there is refused with `0x87` (or dropped and counted at
at-most-once). Only the first level is reserved; `a/$b` is an ordinary
topic.

### What a listener accepts

```toml
[listener]
address = "0.0.0.0:1883"
max_connections = 100000
mountpoint = ""                   # a namespace; two mountpoints are two sets of sessions
max_qos = 2                       # 0, 1 or 2; advertised to version 5 clients
retain_available = true           # false refuses retained messages
max_publish_rate = 0              # PUBLISH packets a second per session; 0 is no cap
publish_burst = 0                 # how many at once; never less than the rate
```

A session over `max_publish_rate` is disconnected, with 0x96 (message
rate too high) on version 5 and a bare close on 3.1.1, because neither
version can refuse one publication for rate and keep the connection.
Each is counted on `sdmqtt_rate_limited_total{limiter="publish"}`. The
rate is measured on the broker's one-second clock, which is why the
burst is never less than one second's worth.
`SDMQTT_MAX_PUBLISH_RATE` and `SDMQTT_PUBLISH_BURST` set the two on
`[listener]`.

`[listener]` is the default listener and is named `mqtt`. More
listeners share the same broker — the same sessions, subscriptions,
retained messages and store — and each carries its own policy:

```toml
[[listeners]]
name = "tenant-b"                 # required; shown on the Cluster and Settings pages
address = "0.0.0.0:1884"
mountpoint = "tenant-b"
max_qos = 1
```

Two listeners on one address, a nameless entry, and a name the admin
API already uses for its own rows (`mqtt`, `admin`, `cluster`,
`ingest`) are refused at startup. `[[listeners]]` is file-only; the
environment variables below reach `[listener]` alone.

A listener can speak TLS:

```toml
[[listeners]]
name = "secure"
kind = "tls"
address = "0.0.0.0:8883"

[listeners.tls]
certificate = "/etc/sdmqtt/fullchain.pem"   # PEM, leaf first
key = "/etc/sdmqtt/privkey.pem"             # PEM, unencrypted
reload_ms = 60000                           # how often a renewal is looked for
```

Both files are read at startup — a listener whose certificate is
missing, not PEM, or issued for a different key refuses to start,
naming the file — and re-read every `reload_ms`, so a renewal tool
that rewrites them needs no restart: the next handshake serves the new
pair. A renewal half-written, with one file new and the other old or
empty, keeps the previous pair and logs once. `kind = "tls"` without a
`tls` table, or a `tls` table on a plain listener, is refused. An
encrypted key is not supported; decrypt it first. Client certificates
are not asked for. `sdmqtt_tls_handshakes_failed_total` and
`sdmqtt_tls_certificate_reloads_total{outcome}` count what happened,
and exist only on a node with a TLS listener.

And WebSocket, plain or over TLS:

```toml
[[listeners]]
name = "web"
kind = "ws"                                 # or "wss", with a [listeners.tls] table
address = "0.0.0.0:8080"
```

A client upgrades on any path — libraries default to `/mqtt`, `/ws`
and `/`, and refusing all but one would be wrong in every tutorial —
and must offer the `mqtt` subprotocol, which the broker confirms. MQTT
travels in binary frames; a text frame closes the connection with
1003, and pings are answered underneath. One frame may carry part of a
packet or several packets, as the standard allows. A refused upgrade is
counted on `sdmqtt_websocket_upgrades_refused_total{reason}`, which
exists only on a node with a WebSocket listener.

`max_qos` caps what the listener serves. A version 5 client is told the
cap in the connect acknowledgement; a publish above it is a protocol
error either way — version 5 is disconnected with reason 0x9B, 3.1.1 is
closed, because it has no code for it — and a subscription above it is
granted at the cap, which the acknowledgement says. `retain_available =
false` refuses a retained publish the same way (0x9A) and refuses the
connect of a client whose will is retained, rather than accepting the
connect and quietly dropping the flag. `SDMQTT_MAX_QOS` and
`SDMQTT_RETAIN_AVAILABLE` set the same two.

### What this node is called, and what identifies it

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

### Joining a cluster

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

Seeds are where to look, not who is there. A seed list naming this
node is harmless — the handshake recognises it and the node stops
dialling itself — so the same file can go on every node. Two nodes
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

### A partition: keep serving, or refuse on purpose

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

### How many nodes hold a session's queue

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

### When a durable publish is acknowledged

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

### When a node is gone for good

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

### What happens when a node dies

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

Messages published on either side of a partition are not exchanged
afterwards, whatever the flags say, and a node that acknowledged a
message it never managed to replicate loses it when it rejoins.
Closing that window needs synchronous replication of the queue, which
this build does not do yet: a publish would then not be acknowledged
until a replica held it. Eliminating it entirely would need consensus,
which this design does not take on.

### The admin listener answers to an address, not to a name

```toml
[admin]
address = "127.0.0.1:1884"        # the default
allowed_hosts = ["broker.internal"]   # only if you reach it through a name
```

The admin listener is bound to loopback by default, and binding it
further with no `admin.token` set is refused at startup rather than
warned about: the API publishes. On loopback with no token, a mutating
request that presents nothing is still answered; one that presents a
key is judged by it.

It also checks the name a request was made under. An **address** is
always answered — by a browser, a probe, a scraper, `curl` — and so is
`localhost`. A **hostname** is answered only if it is in
`allowed_hosts` (or `SDMQTT_ADMIN_ALLOWED_HOSTS`, comma separated).
That is not fussiness: an ordinary cross-origin request from a web page
is already stopped by a preflight nothing here answers, and DNS
rebinding gets around that by making the page's request same-origin.
The name is the one thing the attack cannot forge. If you reach the
listener through a name of your own, put it in the list; the refusal
says so.

## Conformance

`services/broker/conformance/run.sh` runs Eclipse Paho's interoperability
suite against a release build, over plain TCP, over WebSocket and — when
`openssl` is on the path to mint a certificate for the run — over TLS,
for both protocol versions, and fails on any difference from
`conformance/known-failures.txt` in either direction. `CONFORMANCE_MODES="tls"`
runs one layer while working on it.

## Development containers

Everything builds and tests in a **single container**. The per-service
containers exist for convenience, not necessity, and share one image.

Open the repository root in the container described by
`.devcontainer/root/devcontainer.json` and the whole workspace is available
at `/workspaces/repo`. Or without an editor:

```bash
docker build -f .devcontainer/base.Dockerfile -t sd-mqtt-dev .
```

```bash
docker run --rm -it -v "$PWD:/workspaces/repo" -w /workspaces/repo/services/broker sd-mqtt-dev bash
```

The `broker` and `web` containers in `.devcontainer/compose.yaml` are the
same image with a different working directory, for anyone who prefers one
editor window per service. Nothing requires them.

## Licensing

sd-mqtt is MIT licensed; see `LICENSE`. It includes third-party
components under other permissive licenses, and does not claim they are
all MIT. Two of them are Apache-2.0 with no alternative branch this
project's allow list accepts, which asks for the license text to travel
with the binary.

It does. The attribution is compiled into `sdmqtt`, in two halves:

```bash
sdmqtt licenses            # everything in this binary, on standard output
```

and the same document at `/third-party-notices.txt` on the admin
listener, for anyone who has a browser pointed at it rather than a
shell.

The two halves are also in the repository, each checked in CI against
the thing it is generated from:

| File | Covers | Generated by |
|---|---|---|
| `services/broker/THIRD-PARTY-NOTICES.txt` | the crates compiled in | `scripts/rust-notices.py` |
| `services/web/THIRD-PARTY-NOTICES.txt` | the libraries in the bundle | `npm run build` |

**They are not generated the same way.** The browser half is read out
of the artifact: a plugin walks the bundler's post-tree-shaking module
graph, so a dependency that is installed and never imported is
correctly absent. The Rust half is generated from the dependency
graph, which is a manifest, and then `scripts/rust-notices.py --check`
reads the release binary and fails on any crate the binary names that
the file does not — rustc records the source path of every panic site,
so a crate that ships is usually named in the bytes. That is a
completeness assertion in one direction: a crate can be linked in and
leave no path string at all, so the check catches an omission and
never confirms the list is exactly right.

Neither of those checks runs in `cargo test`. Both are CI jobs, and a
green local suite says nothing about either.

The policy behind all of this — which code is distributed, which is
build tooling, and what each license asks for — is in
`docs/research/component-selection.md`.
