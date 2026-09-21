# Milestone 3, demonstrated

Milestone 3's exit criterion is that **the interface shows live
clients and retained messages against a running broker**. This is
what was run, what was seen, and what did not work. The last part is
the point: a demonstration that lists only what worked is an
advertisement.

Run on 2026-09-21, macOS on arm64, against `feat/api`. The interface
was served **from the binary**, not from the development server, so
what is described here is the production path: embedded, precompressed
and content-negotiated.

## What was run

```bash
# The interface, built and embedded.
cd services/web && npm ci && npm run build
cd ../broker && cargo build -p sdmqtt

# A broker with a store, on a directory nothing else was using.
rm -rf /tmp/sdmqtt-demo && mkdir -p /tmp/sdmqtt-demo
./target/debug/sdmqtt serve --store-directory /tmp/sdmqtt-demo
```

The browser was pointed at `http://127.0.0.1:1884/`, which is the
admin listener serving the embedded bundle. A stopped broker
therefore serves no interface at all — an already-open tab keeps
working and says it has lost the connection, but the page cannot be
loaded or reloaded. That is not a defect, it is what embedding
means, and it is worth knowing before an incident.

The clients and the traffic came from `scripts/mqtt-load.py`, a
small MQTT 3.1.1 client written for this and checked in so that the
numbers below can be reproduced. The repository had no client of its
own: the conformance suite is a conformance suite, and it cannot be
asked to hold four hundred connections open. It is a development
tool, it ships with nothing, and no test depends on it.

```bash
ulimit -n 4096   # four hundred sockets in one process

# Six retained messages.
scripts/mqtt-load.py retain \
    sensors/kitchen/temp=21.5 sensors/hall/temp=19.0 \
    sensors/kitchen/humidity=48 status/broker=online \
    status/store=open devices/lamp/state=on

# Four hundred clients, held open.
scripts/mqtt-load.py flood 400 demo &

# Twenty seconds of three hundred publications a second of 350 bytes.
scripts/mqtt-load.py traffic 20 300 350

# Three subscribers, one per shell: sensors/#, #, devices/+/state.
scripts/mqtt-load.py subscribe 'sensors/#' 600 &
```

## Overview

- The gauges read **400 connections, 400 sessions, 6 retained, 0
  queued**, matching `/api/v1/clients?limit=1` and
  `/api/v1/retained?limit=1`.
- The bytes chart drew a plateau the cursor read as **110.3 kB/s**
  against 300 publications a second of about 370 bytes on the wire,
  which is 111 kB/s. The rate is computed in the browser from
  successive totals, not from the server's `deltas`; see below.
- The node line read `Node local, version 0.1.0, up 0m 21s`.
- Stopping the broker turned the indicator red within a second:
  "Lost the connection to the broker. Retrying." Starting it again
  recovered to "Live" with no reload, after the fifteen seconds the
  **server** asks for in its `retry` field.
- The restart was answered by the server with a resynchronise, the
  interface refetched, and the Overview showed the notice: "The
  broker's counters went backwards once while this page was open,
  which is a restart."

**What did not work as expected.** That notice did not appear as soon
as the broker came back. It appears when the first counters event of
the new run arrives, and the sampler publishes a counters event only
when a number moves, so on an idle broker the restart is invisible
until something happens to it. It appeared immediately once traffic
resumed. The line in the chart breaks correctly either way.

## Clients

- The table showed **400 rows** of `demo-0000`…`demo-0399` with their
  protocol version and counts, and the footer read "All 400 are
  loaded."
- Three subscribing clients connected while the page was open and
  appeared **without a reload**; the count went to 403 and
  `dashboard` showed one subscription.
- Filtering on `dash` narrowed it to one row and the header read
  "1 shown · 403 fetched · 403 connected to the broker".
- Killing the fleet took the table to "3 shown · 3 fetched · 3
  connected", again with no reload.

Tested harder than the demonstration needs, during the build:

- With **1,000 rows loaded**, the document held 24 of them and a
  scroll of the whole list cost 1.2 to 2.1 ms of work per frame and
  5 ms at worst, measured over forty steps. With the window removed
  as a deliberate mutation, 2,300 rows in the document cost 27 ms
  median and 36 ms at worst — the difference between a table and a
  slideshow.
- The table was driven up to **3,600 connected clients** and stayed
  at 24 to 32 rows in the document throughout.
- The window walks the pages as it reaches them: 1,000 → 1,500 →
  2,000 → 2,300, and then to 3,600, and the footer says which of
  those it is on the whole time.

**What did not work.** Once, the table stopped fetching further pages
and sat on 1,000 of 2,300 with the footer offering a button that did
nothing on scroll. The cause was not established. A race that could
produce it was found and closed — a request abandoned by a
resynchronise either cleared a flag that now belongs to the request
that replaced it, or left it set forever — but the stall could not be
reproduced on demand afterwards, with or without that guard. It is a
fix by reasoning, not by bisection, and it is recorded here as such.

## Topics

- The retained browser listed all six topics with their payloads,
  levels and sizes.
- Publishing `sensors/kitchen/temp=24.75` retained, at QoS 0, with
  three subscribers connected, answered **"Published. 2 subscribers
  received it."** — `sensors/#` and `#` matched and
  `devices/+/state` did not — and the row updated to 24.75.
- Clearing `sensors/porch/temp` with the row's button, which
  publishes an empty retained payload, removed it from the list and
  from `/api/v1/retained`, checked independently.
- The watcher on `sensors/#` listed the three matching topics and
  counted each change; a value published three times showed four
  changes, counting the one it started with.
- The six retained messages **survived the broker restart** and were
  listed again after it, which is the store doing its job.

**What the watcher is not.** It is not a subscription, and the page
says so where an operator will read it. Demonstrated rather than
asserted: publishing `sensors/attic/temp=3.0` **without** the retain
flag answered "Published. 2 subscribers received it" and the watcher
showed nothing, because it reads the retained store and a
publication that is not retained never goes there.

The reason is that the events stream carries counter deltas and
connection events and no publications, and the mechanism that would
carry them — a tap that is a real subscriber inside the broker — is
a change to the broker and to the admin API rather than to a page.
The design and its limits are written down in
`services/web/README.md`.

With 1,207 retained topics the watcher said it was watching the
first 1,000 in topic order rather than pretending to the whole
store. That bound is what stops a filter of `#` from costing more
than a filter of one topic.

## The things that are unfinished

1. **The subscribe tool cannot see live traffic.** The largest gap,
   described above and in the readme.
2. **There is no automated test for any of this.** Every behaviour
   above was checked by hand in a browser against a real broker, and
   several were checked by breaking the code and watching the check
   fail. That is better than nothing and worse than a suite: nothing
   will catch a regression in these pages except somebody doing it
   again. The repository has no browser test runner, and adding one
   is a decision with its own weight.
3. **The restart notice is late on an idle broker**, as described.
4. **`queued` has no resource behind it.** Every other gauge can be
   fetched; queue depth arrives only on the events stream, so a
   freshly loaded tab shows `—` for it until the first counters
   event. It is left as an absence rather than filled with a zero.
5. **A colour-scheme change does not restyle an open chart.** uPlot
   takes concrete colours, they are read once when the plot is
   created, and a reload fixes it.
6. **Rows invented from a connect event are replaced wholesale.**
   A client that connects appears immediately with its identity and
   `…` for everything else; the debounced refetch that fills those in
   re-reads only the pages that were loaded, so during a burst of
   hundreds of connects the visible set can shrink back to the
   fetched pages. The footer always says how many of how many are
   loaded, so the number is never wrong, but a row can appear and
   then go.
7. **Sessions, subscriptions, rules, cluster and settings have no
   pages.** Section 11.2 names six; this milestone builds three, and
   the resources for sessions and subscriptions are served and
   unused by any page.
8. **The retained browser is not windowed.** Only the Clients table
   is. The retained list renders every row it has fetched, two
   hundred at a time, so walking a store of tens of thousands by
   pressing "Load the next page" will eventually put tens of
   thousands of rows in the document and slow the page down in
   exactly the way the Clients table was measured not to. The
   watcher's bound does not help here, because it is a different
   list.
9. **A second browser tab is a second stream.** The one-stream rule
   is per document, which is all a page can enforce. Six tabs on the
   Overview page will exhaust the connection budget exactly as
   section 11.1 describes.

## Budget

The bundle is **51.5 KB gzipped against the 150 KB of section 15**,
which is 34% of it, summed per file the way the broker embeds and
serves them. The shell alone was 13.3 KB, so the three pages, uPlot,
the virtual core and valibot cost about 38 KB between them.
