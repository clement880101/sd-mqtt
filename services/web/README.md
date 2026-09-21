# The sd-mqtt browser interface

Preact 10 with signals, routed by preact-iso, bundled by Vite 8, styled
with hand-written CSS over Open Props tokens. Section 11.2 of the
design says what it is for; there are no pages yet, only the shell and
the pipeline around it.

## Running it

Two processes. The broker first, from `services/broker`:

```bash
cargo run -p sdmqtt -- serve --store-directory /tmp/sdmqtt-dev
```

Then the development server, from here:

```bash
npm ci
npm run dev
```

The interface is on the address Vite prints. The broker's admin
listener is on `127.0.0.1:1884` by default; point at another one with
`SDMQTT_ADMIN=http://127.0.0.1:11884 npm run dev`.

## Building it

```bash
npm run build
```

That writes `dist/`, regenerates `THIRD-PARTY-NOTICES.txt`, and fails
on a dependency whose licence is outside the allow list in
`vite.config.ts`. `crates/admin/build.rs` precompresses whatever is in
`dist/` and embeds it, so a `cargo build` after this serves the
interface from the binary. A `cargo build` without it serves a page
saying the interface was not built: **a build of the binary must not
require Node.**

`scripts/budgets.sh` weighs the bundle against the 150 KB of section
15.

## What the proxy does, and why this direction

`vite.config.ts` forwards `/api`, `/health` and `/metrics` to the
broker. The hot-reload socket is Vite's own and is not forwarded
anywhere.

**Vite proxies to the broker, not the reverse.** An earlier draft of
the design had the broker proxying to the development server, which
means a streaming reverse proxy with WebSocket upgrade forwarding
living in the shipped binary for the benefit of development. This is
the same thing in a dozen lines of somebody else's configuration, and
none of it ships.

## The events stream, which is the part that breaks

`/api/v1/events` has to arrive event by event. A proxy or a
compression layer will buffer a stream unless told not to, and the
symptom is an interface that updates in clumps — easy to live with and
miserable to diagnose months later.

It was measured rather than assumed. With a client connecting to the
MQTT port once a second, the stream was read directly from the broker
and again through the development server, recording the arrival time
of every read:

```
[direct]                 7 frames, +0.53s +1.53s +2.53s +3.53s +4.53s +5.53s +6.53s
[through the dev proxy]  7 frames, +0.31s +1.31s +2.31s +3.31s +4.31s +5.31s +6.31s
```

One frame per second through the proxy, the same cadence as the
broker's own. Not one lump at the end. Nothing here compresses the
body, the broker sends `X-Accel-Buffering: no` for the intermediaries
that would, and the admin router deliberately has no compression layer
for the events route to be exempted from.

## What development does not exercise

The plan this was built from expected the development server to
swallow a client disconnect, leaving the broker's per-stream cleanup
path unexercised here. Measured on Vite 8, it does not: three streams
opened through the proxy and then closed took all three upstream
connections to the broker with them within three seconds.

The caveat that is real is on the broker's side, and it is not the
proxy's fault. A closed socket is a half close, so the task feeding a
stream does not discover that its browser has gone until it writes and
the peer resets. A tab closed on a broker where nothing is happening
leaves its task until the next event — a wait bounded by the next
event rather than a leak.

Either way, that path is guarded against the broker directly rather
than by anyone noticing it here:
`crates/admin/src/events.rs::a_client_that_goes_away_ends_its_stream`.

## The subscribe tool, and the mechanism it does not have

The Topics page has a publish form and a watcher. The watcher is the
"subscribe" half, and it is **not** a subscription. This is the
decision and what it costs.

**The events stream carries no publications.** Section 11.1: it
carries counter deltas, connect and disconnect events, and later rule
outcomes. Nothing on the admin API lets a browser see a message pass.
So the tool either needed a new broker mechanism or had to be built
from what is there.

**What is built.** The watcher takes an MQTT topic filter, pages the
retained resource, matches each topic with the protocol's own rules
(4.7, including that a leading wildcard does not match a `$` topic)
and re-reads every two seconds, marking what changed.

**What it costs.** It sees retained messages only. A publication
without the retain flag never appears, so a filter over a topic
published to a thousand times a second but never retained shows
nothing at all. The page says this on screen, because a watcher that
draws nothing is otherwise indistinguishable from a quiet broker. It
also samples rather than streams: a value that changes twice between
two passes is one change here.

**Its limit.** A filter of `#` on a busy broker is the obvious abuse
of anything that forwards matching messages. Here it is bounded by
construction: one pass reads at most five pages of two hundred
topics, so `#` costs exactly what one topic costs, and a store
holding more than that is reported as partially watched rather than
silently truncated. The cost to the broker is one request per
interval per open tab, which is bounded and visible.

**What was not built, and what it would take.** A *tap*: a real
subscriber inside the broker whose deliveries a browser can read. It
needs no new matching rules, because it can be a genuine session —
`Broker::connect`, `Broker::handle` with a subscribe, and
`Shared::register_outbox` are all public and are what a connection
itself uses. What it needs beyond that is the part that makes it
safe: a bounded ring per tap with a counted drop, a cap on how many
taps exist, a payload cap, and an expiry for a tap nobody is polling
— because a forgotten tap is a session that never leaves, and a
session that never leaves is a client on the Clients page that
nothing can explain. Polling rather than a second stream, because
section 11.1 allows the interface exactly one.

That is a change to the broker and to the admin API, with its own
abuse surface and a session-creating code path, and it was not put
inside a commit that writes three pages.

## Attribution

`THIRD-PARTY-NOTICES.txt` is generated from the bundle, not from the
package list: a dependency that is installed and never imported does
not ship and is not named. It is checked in, and CI fails on any
difference between the regenerated file and the committed one. Build
tooling is absent on purpose — running a tool to produce an artifact
does not distribute the tool.
