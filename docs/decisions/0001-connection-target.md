# 0001: The connection target is met

**Status:** decided, milestone 7, 2026-10-15.

## The question

Section 15 budgets "concurrent connections per node, target hardware:
100,000" and says the figure is a load test, not a build gate, and
milestone 7's to prove. "Target hardware" is defined nowhere. The honest
reading is that the measurement names what it ran on.

## What was measured

`scripts/connection-target.sh`, which runs in the development image on
Linux:

| | |
|---|---|
| Machine | Apple M2 Pro, 12 cores; Docker Desktop's Linux VM with 12 cores and 11.7 GB, kernel 7.0.14-linuxkit, aarch64 |
| Broker | release build, `max_connections` 200,000, descriptor limit 1,048,576, a store, no other load |
| Generator | `sdmqtt-load` in the same container, 100,000 MQTT 3.1.1 connections over four loopback source addresses, keep-alive 60 s |
| Opened | **100,000 in 2.5 s**, about 40,000 a second, none refused |
| Held | **100,000 for 60 s**, every one answering a PINGREQ at the end |
| Broker memory | 20 MB idle, 3,063 MB at the peak: **about 32 KB a connection** |

Running out of descriptors was measured in the same run: the broker
under `ulimit -n 4096`, offered 6,000 connections, took 4,084, left the
rest waiting in its backlog until the generator gave up, stayed ready,
used no measurable CPU while it waited, and counted 368 pauses on
`sdmqtt_accept_pauses_total`. A counter was added for this, because
accepting paused and nothing said so.

## Decision

The budget is met as written and does not move. The script is the
evidence and is rerun before a release; it is not a CI job, because a
hosted runner's descriptor limits and memory are not under this
project's control and a failure there would say more about the runner.

## What it costs, and what is not answered

- **Memory is the limit that will be met first, not descriptors or
  CPU.** At 32 KB a connection, 100,000 idle clients take about 3 GB.
  Where those bytes go — the read buffer, the outbox's channel, the
  session — was not measured here; it is the first thing to look at if
  a deployment needs more connections per gigabyte.
- The generator shares the machine with the broker, so the connect rate
  is a floor rather than the broker's ceiling.
- Clients that publish are a different measurement: this one is
  connections held, which is what the budget names.

## What would reopen it

A deployment that needs more than about 300,000 connections on one
node, where memory per connection decides it; or a change to the
connection path that moves the per-connection figure by more than a
quarter, which the script will show.
