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
  changes, counting the one it started with. **That watcher no
  longer exists**: see the second run below, which replaced it.
- The six retained messages **survived the broker restart** and were
  listed again after it, which is the store doing its job.

**What the watcher was not.** It was not a subscription, and the
page said so where an operator would read it. Demonstrated rather
than asserted: publishing `sensors/attic/temp=3.0` **without** the
retain flag answered "Published. 2 subscribers received it" and the
watcher showed nothing, because it read the retained store and a
publication that is not retained never goes there.

With 1,207 retained topics the watcher said it was watching the
first 1,000 in topic order rather than pretending to the whole
store. That bound is what stopped a filter of `#` from costing more
than a filter of one topic.

Both of those are now history. The next section is what replaced
them.

## Second run: the watcher is a tap

Run later on 2026-09-21, same machine, same branch, same production
path: the interface served from the binary on `http://127.0.0.1:1884/`,
a broker with a store on `/tmp/sdmqtt-demo`, and traffic from
`scripts/mqtt-load.py`.

The gap the first run recorded — "the subscribe tool cannot see live
traffic", the largest of the nine unfinished things — is closed. The
watcher now asks the broker to open a **tap**: a real subscriber
inside the broker, made with `Broker::connect`, `Broker::handle` with
a SUBSCRIBE packet and the outbox `Shared::register_outbox` hands it,
collecting matching publications into a bounded ring the page polls
once a second. `services/broker/crates/admin/src/taps.rs` has the
mechanism and the reason behind each of its four limits.

### The thing that could not be done before

A tap was opened on `sensors/#` from the page. It showed nothing,
and said so: *"Nothing has been published to this filter since the
watch started."* Then a client connected over the MQTT port and
published three messages, none of them retained:

```
sensors/attic/temp   = 3.0
sensors/kitchen/temp = 24.75
devices/lamp/state   = off
```

Within one poll the page showed two rows, freshly highlighted:
`sensors/kitchen/temp = 24.75` and `sensors/attic/temp = 3.0`, both
with retain `no`. `devices/lamp/state` was not among them, because it
does not match the filter. `/api/v1/retained` still said `"total": 6`
throughout, and the retained browser on the same screen still showed
`sensors/kitchen/temp = 21.5` — the value retained at the start of
this run, sitting beside the live 24.75 the tap had just caught. Those two numbers
on one screen are the whole difference between the two mechanisms.

### Under real traffic

`scripts/mqtt-load.py traffic 20 300 350` — twenty seconds of three
hundred publications a second of 350 bytes, across eight topics —
against a tap on `#`. The page read, in order, as it went:

```
 256 shown across  8 topics ·   300 caught ·    44 dropped by the ring ·   7 polls
 256 shown across  8 topics · 1,800 caught · 1,544 dropped by the ring ·  12 polls
 256 shown across  8 topics · 3,900 caught · 3,644 dropped by the ring ·  19 polls
 256 shown across 10 topics · 6,002 caught · 5,746 dropped by the ring ·  27 polls
```

and drew the notice it owes an operator: *"881 messages arrived and
left the tap's ring before this page read them, so they are not
below. The filter is producing more than 256 messages a second;
narrow it to see everything."* The document held exactly 256 rows
throughout, counted in the DOM. **Nothing was lost by the broker**:
the `lost` counter, which is what the tap's own session queue
overflowing would move, stayed at zero for the whole run, and the
broker's log holds no line at warning or error level for any of it.

That is the ring doing its job rather than failing: a tap on `#`
against a broker doing three hundred a second is the abuse case, and
what it costs is one subscriber's fan-out and one request a second.

### The limits, each shown binding

- **Eight taps.** Opening them one at a time by hand: the first six
  (beside the two already open) answered `200`, and the seventh
  answered `429` with `"code": "resource_exhausted"` and a message
  naming the cap and saying how to get a slot back. `/api/v1/taps`
  then read `"live": 8`.
- **256 messages, oldest dropped.** Above, and the rows at the top of
  the list were the newest.
- **1 KiB of payload.** A 2,000-byte publication showed as
  `(2000 bytes, over the tap's payload cap)` with its real size, not
  as a truncated string pretending to be the payload.
- **120 seconds unread.** A tap was opened on `forgotten/#` and left.
  It disappeared from `/api/v1/taps` 2 minutes and 5 seconds later,
  measured by polling the list every two seconds: the 120-second
  lease plus a sweep that runs every five, which is within a sweep
  of the earliest it could go. Reading it then answered `404` in the one
  error shape, `/api/v1/subscriptions` was empty, and
  `/api/v1/sessions` was empty: the broker had the whole session
  back.

**What did not work the first time.** The expiry was measured wrong
to begin with. The check polled `GET /api/v1/taps/2` every five
seconds waiting for a `404`, and the tap never expired, because
**reading a tap is what renews its lease** — the measurement was
keeping alive the thing it was measuring. It is a real property,
tested and deliberate, and it took being caught by it to believe it.
The number above was taken by polling the *list*, which does not
touch a lease.

### That a tap is only an observer

- With a real subscriber on `sensors/#` over the MQTT port and the
  tap on `#` both attached, a publish answered `"delivered": 2`, and
  the real subscriber's socket received the message — 108 bytes
  carrying its topic and payload. The tap took nothing from it.
- Publishing to `$SYS/fake/probe` answered `"delivered": 0` with a
  tap on `#` open. The rule that a leading wildcard does not match a
  `$` topic is the broker's, and the tap is subject to it for free
  because it goes through the broker's own matching rather than a
  second copy of it.
- `POST /api/v1/taps` with `$share/g/sensors/#` answered `400`,
  `"field": "filter"`, and a sentence saying why: a tap in a share
  group would take messages away from the clients meant to have them.
  That is the one filter through which a subscriber changes what
  other subscribers receive, which is the one thing a tap may not be.
- The taps were visible on the Clients page as `$admin-tap-2` …
  `$admin-tap-9`, each with one subscription, and
  `/api/v1/subscriptions` showed all eight at `qos 0`,
  `retain_as_published: true`, `retain_handling: 2`. A tap is a
  client and looks like one; that is the point of naming it
  legibly.

### What the page does when its tap goes away

Deleting the page's tap from outside, with `curl -X DELETE`: within
one poll the page stopped polling, put the button back to "Watch",
kept the rows it already had on screen, and showed the broker's own
sentence.

**And a defect that found**, small and now fixed. The sentence read
"either it was never opened, or nobody read it for 120 seconds and
it expired" — true of the state and wrong about the cause, because
it had just been deleted. The registry holds no tombstones and
cannot tell the three apart, so the sentence now names all three.

### What is rough

- **The list scrolls under you.** New rows are prepended, so a reader
  who has scrolled down into the history is carried backwards through
  it as messages arrive. Someone left at the top is unaffected.
  Recorded rather than half-fixed: holding the scroll position
  against a prepend is a real piece of work and this is a stream you
  are meant to watch from the top.
- **`lost` has no demonstration.** It counts a discard the broker
  reports against a tap, and the broker did not produce one in any of
  this, because a connected at-most-once subscriber is dispatched to
  directly and its queue stays empty. The counter is there so that
  the branch is not the one branch of four that drops a message
  silently; that it is wired up is tested by handing the action to
  `Shared::deliver`, and that the broker would ever raise one is not
  something this run can show.
- **A tap writes to the store.** Its session and its subscription are
  journalled like anybody else's, so opening and closing one costs a
  handful of records. Not its messages.

## The things that are unfinished

1. ~~**The subscribe tool cannot see live traffic.**~~ **Closed.**
   It was the largest gap; the second run above is what closed it.
   Left in the list rather than deleted, because a list of
   unfinished things that quietly loses entries cannot be read as a
   record of anything.
2. **There is no automated test for any of this.** Every behaviour
   above was checked by hand in a browser against a real broker, and
   several were checked by breaking the code and watching the check
   fail. That is better than nothing and worse than a suite: nothing
   will catch a regression in these pages except somebody doing it
   again. The repository has no browser test runner, and adding one
   is a decision with its own weight. **Still true of the pages, and
   no longer true of what they talk to**: the tap has twenty-five
   tests in `crates/admin/src/taps.rs`, nearly all of them against a
   real broker over a real socket, and thirty deliberate mutations
   were run against them. Five survived their first run — the epoch
   check in a tap's teardown, the counter for a discard the broker
   reports, and three about the cap — and each one is a test or a
   design change in the commits above. The browser half of the
   watcher is still guarded by nothing but somebody doing it again.
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
   watcher is not windowed either, and does not need to be: it is
   bounded at the tap's 256 messages, which was counted in the DOM
   under three hundred publications a second.
9. **A second browser tab is a second stream.** The one-stream rule
   is per document, which is all a page can enforce. Six tabs on the
   Overview page will exhaust the connection budget exactly as
   section 11.1 describes.
10. **A tap's list scrolls under a reader who has scrolled into it**,
   and **`lost` has no demonstration**, and **a tap writes a handful
   of records to the store**. All three are described at the end of
   the second run.

## Budget

The bundle is **52.1 KB gzipped against the 150 KB of section 15**,
which is 35% of it, summed per file the way the broker embeds and
serves them. The shell alone was 13.3 KB, so the three pages, uPlot,
the virtual core and valibot cost about 39 KB between them.

It was 51.5 KB before the tap. The watcher gained a message list, a
cursor and a lifecycle and lost the MQTT 4.7 matcher it used to
carry, which nets to **+0.6 KB**. The matcher going is worth more
than the six hundred bytes: a second implementation of the matching
rules, in a language no conformance suite is pointed at, is the kind
of thing convention 8 is about.
