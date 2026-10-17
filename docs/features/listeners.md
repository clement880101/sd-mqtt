# Listeners

TCP, TLS, WebSocket and secure WebSocket listeners, each with its own mountpoint, maximum QoS, retain policy and publish cap.

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

**When the process runs out of file descriptors**, a listener pauses
accepting for a moment and tries again, rather than spinning, and counts
each pause on `sdmqtt_accept_pauses_total`. No connection is refused: it
waits in the kernel's backlog until the pause ends. A counter that keeps
moving means the descriptor limit is below what the node is asked to
hold; raise it (`ulimit -n`, or `LimitNOFILE=` under systemd), since each
connection is one descriptor. `sdmqtt_build_info{version, allocator}`
names the build a node runs.

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
