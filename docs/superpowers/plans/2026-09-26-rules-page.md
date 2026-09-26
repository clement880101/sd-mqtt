# Rules Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The Rules page, with a per-action form editor, per-rule statistics
and dead letters. And the table-row sparklines section 11.2 promised, which
milestone three deferred to here because per-rule statistics are the first
per-row series this project has to draw.

**Architecture:** Rules are read and written over the admin API, which means
the API has to serve them first. The sparkline is hand-drawn as a path
element per row rather than a chart per row, which is the technique the
component research chose and the reason it chose it.

**Tech Stack:** The admin crate, and the Preact application from milestone
three.

---

## Read first

- `docs/conventions.md`, all eight.
- Sections 9.1, 9.2 and 11.2 of
  `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`.
- `crates/admin/src/resources.rs`, which shows how a resource is served
  without holding the broker lock while serialising.
- `services/web/src/pages/clients.tsx`, which is the windowed table pattern,
  and `services/web/src/live.ts`, which owns the one events stream.

## What is already true

Rules exist, are stored durably, and are loaded from configuration at
startup. Four connector types work and are tested against the real binary,
including against a deliberately strict third-party broker.

The interface has three pages and a 52.2 KB bundle against a 150 KB budget.
One events stream serves the whole application; a fourth page must not open
a second.

## Two things to be careful about

**A rule written over the API is durable metadata**, so creating one must go
through the same path that loads one from configuration, or the two will
disagree about what a valid rule is. The refusals already exist: a rule with
no token on an ingest action, a bridge whose filters intersect, a trigger on
an action that does not fire on messages. A rule created over the API must
meet all of them, and the test that proves it is one that tries.

**A dead letter is published to a topic**, `$sd/rules/<id>/dlq`. The page can
therefore read them by subscribing rather than by a new resource, which is
what the message tap already does. Decide which, and say why.

## Conventions

Branch is `feat/rules`. From `services/broker`, before every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check && ./scripts/budgets.sh
```

For the interface, the discipline is the same in spirit: for every behaviour
you assert, break the thing and confirm the assertion fails, and say which
you did.

---

### Task 1: Rules over the API

**Files:**
- Create: `crates/admin/src/rules.rs`
- Modify: `crates/admin/src/lib.rs`
- Test: those files

List, read, create, update, enable, disable and delete. Mutating requests
need the token, as every mutating request on that listener does.

**Validate through the path that exists.** The rules crate already refuses
the invalid shapes and the reasons are good ones. A second validator in the
admin crate is a second set of rules that will drift.

- [ ] **Step 1: Write the failing tests**

A rule created over the API is loaded and fires, which is the test that
proves it went all the way rather than into a list. A rule created and then
the broker restarted still fires, which proves it was stored.

Then the refusals, one test each: an ingest action with no token; a bridge
whose inbound and outbound filters intersect; a trigger on an action that
does not fire on messages. Each must be refused with the error shape the API
already uses, naming which field is at fault.

- [ ] **Step 2: Implement, run, confirm teeth**

Mutations: the token not required on a create; validation skipped; a created
rule not persisted, which the restart test is there to catch.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(admin): rules, read and written over the API"
```

---

### Task 2: Per-rule statistics

**Files:**
- Modify: `crates/admin/src/rules.rs`, `crates/server/src/metrics.rs`, `crates/rules/`
- Test: those files

Fired, delivered, failed, dead-lettered, and what is waiting in the outbox.
Per rule, because a single total across all rules tells an operator nothing
about which one is broken.

**Do not walk anything under the broker lock.** The clients page and the
retained page both had to be fixed for that and the measurements are
recorded. An outbox depth is a stored counter already.

**A series, not a number**, because task 4 draws it. Decide how much history
is kept and where. A ring per rule in memory is the obvious answer and the
obvious question is what it costs at the rule counts an operator might have.

- [ ] **Step 1: Write the failing tests**

Each counter moves when the thing it counts happens, and does not move when
a neighbouring thing happens. A counter that rises on everything is worse
than no counter.

- [ ] **Step 2: Implement, run, confirm teeth**

Mutations: two counters fed from one event; the series not advancing when
nothing happens, which would make a quiet rule look like a busy one whose
chart stopped.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(admin): what each rule has been doing"
```

---

### Task 3: Dead letters

**Files:**
- Modify: `crates/admin/src/rules.rs` or the web application, depending on your decision
- Test: accordingly

Surfaced in the interface, per rule.

- [ ] **Step 1: Decide and write down how they are read**

By subscription through the existing tap, or by a resource. The tap costs
nothing new and bounds itself; a resource is a second mechanism but survives
a page that was not open when the letter was published.

The second half of that sentence is probably decisive. Say so either way.

- [ ] **Step 2: Implement and test**

- [ ] **Step 3: Commit**

```bash
git commit -m "feat: dead letters, where somebody will see them"
```

---

### Task 4: The page

**Files:**
- Create: `services/web/src/pages/rules.tsx`, `services/web/src/sparkline.tsx`
- Modify: `services/web/src/app.tsx`, `services/web/src/api.ts`
- Test: by hand against a real broker, as the other pages were

A table of rules with a per-row sparkline, a per-action form editor, and each
rule's dead letters.

**The sparkline is a hand-drawn path element**, one per row. The component
research chose this over a chart per row and gave the reason: a chart library
instance per table row does not scale. A table of two hundred rules is two
hundred of whatever you choose.

**The form editor is per action**, because the four actions have almost
nothing in common. Validate in the browser what the API will refuse, so a
mistake is a message beside the field rather than a round trip.

- [ ] **Step 1: The sparkline, on its own**

Draw one from a known series and check it by eye and by reading the path. A
sparkline that draws a flat line convincingly is the failure mode.

Decide what an empty series draws, and what a single point draws. Both happen
constantly and neither is a line.

- [ ] **Step 2: The table**

- [ ] **Step 3: The editor**

Create a rule of each of the four kinds through the form, and confirm each
one works afterwards: a message reaches an endpoint, a post arrives, a poll
publishes, a bridge connects. That is the test that the editor writes what
the broker reads.

- [ ] **Step 4: Dead letters in the page**

- [ ] **Step 5: Check the bundle and commit**

Report the size against the budget. Two hundred sparklines is a lot of path
data; if the page is heavy, say so.

```bash
git commit -m "feat(web): the rules page"
```

---

### Task 5: The configuration reference is missing three connectors

**Files:**
- Modify: `README.md`

The reference documents the outbound connector and neither ingest, nor the
poller, nor the bridge. Flagged when the poller landed and not closed.

An undocumented connector is one nobody outside this conversation can use.

- [ ] **Step 1: Document all four, with a worked example each**

Take the examples from the tests, which are known to work.

- [ ] **Step 2: Commit**

```bash
git commit -m "docs: the configuration reference covers every connector"
```
