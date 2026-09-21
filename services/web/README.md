# The sd-mqtt browser interface

Preact 10 with signals, routed by preact-iso, bundled by Vite 8, styled
with hand-written CSS over Open Props tokens. Section 11.2 of the
design says what it is for. Three of its six pages are built —
Overview, Clients and Topics — over the shell and the pipeline that
carries them.

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

## The subscribe tool, which is a tap

The Topics page has a publish form, a retained browser and a
watcher. The watcher is the "subscribe" half, and it **is** a
subscription now. This is what changed and what it costs.

**What it was.** Milestone three had no mechanism to build it out
of. The events stream carries counter deltas, connect and disconnect
events and no publications (section 11.1), so the watcher took a
filter, paged the retained resource, matched each topic with the
protocol's rules *here in the browser*, and re-read every two
seconds. It saw retained messages only — a publication without the
retain flag never appeared — and it sampled rather than streamed. It
said so on screen, because a watcher that draws nothing is otherwise
indistinguishable from a quiet broker.

**What it is.** A *tap*: a real subscriber inside the broker, opened
on request, collecting matching publications into a bounded ring
this page polls. `POST /api/v1/taps` with a filter,
`GET /api/v1/taps/{id}?after=<cursor>` to read, `DELETE` to give it
back. It is a genuine session — `Broker::connect`, `Broker::handle`
with a SUBSCRIBE packet, and the outbox `Shared::register_outbox`
hands it, which are the same three calls a TCP connection makes.

**There is no matcher in this application any more.** There was one,
and deleting it was half the point. A second implementation of
MQTT 4.7 in TypeScript is a second set of rules that drifts from the
broker's the first time either is corrected, and the browser's copy
is the one nothing tests against a conformance suite. Shared
subscriptions, the rule that a leading wildcard does not match a `$`
topic, no-local and retain-as-published are all the broker's, and
the page gets them by not having an opinion.

**Polling rather than a second stream**, because section 11.1 allows
one events stream per document and it is spent. One request a second
per open watcher, with a cursor, which is bounded and visible.

**The limits, which are the hard half and are the broker's.** A tap
is a way for somebody holding this interface to make the broker do
work, and a filter of `#` on a busy broker is the obvious abuse.
`services/broker/crates/admin/src/taps.rs` records four numbers and
the reason each is the number it is; in summary, eight taps at once,
256 messages each with the oldest dropped, 1 KiB of payload carried
and anything larger described, and 120 seconds unread before a tap
closes itself. **This application hard-codes none of them.** Every
answer carries a `limits` object and the page renders what the
broker said, so an interface older than the broker serving it cannot
quote the wrong bounds — and the bounds are the whole point, because
an operator watching a filter and seeing nothing needs to know
whether that means no traffic or a ring being overrun. The counts
are on the page beside the rows: caught, dropped by the ring, lost
by the broker, and missed by this particular reader.

**What a tap does not do.** It does not replay the retained store: a
tap subscribes with retain handling "do not send", because the
retained browser is on the same screen with its own bounds, and
sending a whole store into a 256-message ring would fill the ring
with history that is already visible. So a topic retained an hour
ago and not published to since is in the list above the watcher, not
in it.

**What it costs.** A tap is a client. It appears on the Clients page
as `$admin-tap-<n>`, which is deliberate — a session the Clients
page cannot explain is worse than one with an odd name — and its
session and subscription are journalled like anybody else's. Not its
messages: a fire-and-forget delivery to an attached session writes
nothing. A tab that closes without deleting its tap leaves it until
the idle timeout, which is what the timeout is for; the page tries
on both unmount and `pagehide` anyway.

## Attribution

`THIRD-PARTY-NOTICES.txt` is generated from the bundle, not from the
package list: a dependency that is installed and never imported does
not ship and is not named. It is checked in, and CI fails on any
difference between the regenerated file and the committed one. Build
tooling is absent on purpose — running a tool to produce an artifact
does not distribute the tool.
