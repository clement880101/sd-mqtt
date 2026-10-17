# Rules and connectors

Carry messages between MQTT and HTTP, poll an API, or bridge to another broker, with a durable outbox and dead letters.

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
