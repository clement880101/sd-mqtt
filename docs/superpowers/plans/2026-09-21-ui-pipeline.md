# Interface Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A browser interface that builds, carries correct attribution for
what it ships, is embedded in the binary, is served compressed with content
negotiation, and reaches the broker in development without the broker
knowing about the development server.

**Architecture:** A Vite application under `services/web`, built to a
directory the broker's build script precompresses and embeds. The whole
pipeline is proved before a single page is written, because every part of it
fails in a way that is hard to see from inside a page.

**Tech Stack:** Preact 10, `@preact/signals`, `preact-iso`, Vite 8, plain CSS
over Open Props tokens, `rollup-plugin-license` for attribution.

---

## Read first

- `docs/conventions.md`, all eight.
- Section 11.2 of `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`,
  which was corrected before this plan, and section 15 for the bundle budget.
- The "Verified again before milestone 3" notes in
  `docs/research/component-selection.md`.

## Four things that are settled, so do not re-decide them

**Vite proxies to the broker, not the reverse.** An earlier draft had the
broker proxying to the development server, which means a streaming reverse
proxy with WebSocket upgrade forwarding living in the shipped binary for the
benefit of development. Vite does it in a few lines of its own configuration.

**A build of the binary must not require Node.** Gate the embedding so a
clone with no interface built compiles and serves a page saying so. Otherwise
every Rust-only contributor and the whole test matrix depends on an npm
install.

**Attribution is generated from the bundle, not from the package list.** What
ships is what survived tree-shaking. `rollup-plugin-license` at exactly
`3.7.1` reads the built module graph. Its declared peer range names Rollup
and not Vite, so the compatibility is empirical: pin it, and assert in the
build that the notices file is non-empty and names a package you know is in
the bundle.

**Preact 10, not the 11 release candidate.** And pin uPlot deliberately when
its page arrives: its published artifact is much older than its repository,
which is a thing to know rather than a reason to depend on a commit.

## Conventions

Branch is `feat/api`. From `services/broker`, before every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check && ./scripts/budgets.sh
```

Front-end work has no mutation testing tradition here, so the equivalent
discipline is: for every behaviour you assert, break the thing and confirm
the assertion fails. Say which you did.

---

### Task 1: The application, and a build that produces something

**Files:**
- Create: `services/web/package.json`, `vite.config.ts`, `index.html`, `src/main.tsx`, `src/app.tsx`, `.gitignore`
- Modify: the repository's root ignore file if needed

- [ ] **Step 1: Create the application**

Preact 10, `@preact/signals`, `preact-iso`, Vite 8, TypeScript. Open Props
imported per module rather than wholesale: the tokens are custom properties
on the root element and do not tree-shake, so the single import costs about
7.7 KB gzipped where four or five modules cost 1.5 to 3.

A shell with the navigation for Overview, Clients and Topics, and a route for
each showing nothing yet. No data.

- [ ] **Step 2: Build it and record the size**

```bash
npm ci && npm run build
```

Record the gzipped total against the 150 KB budget. The library floor is
roughly 39 KB, so a shell far above that means something is wrong and is
worth finding now rather than after three pages are written.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(web): the application shell"
```

---

### Task 2: Attribution that cannot drift from what ships

**Files:**
- Modify: `services/web/vite.config.ts`, `package.json`
- Create: `services/web/THIRD-PARTY-NOTICES.txt`
- Modify: `.github/workflows/ci.yml`

The bundled libraries are distributed, so they need attribution. The build
tooling is not, and does not. The distinction was got wrong once on this
project and corrected by the user; the research records it.

- [ ] **Step 1: Add the plugin, pinned exactly**

`rollup-plugin-license@3.7.1`, no caret. It must run only on build and after
tree-shaking, or it reports the wrong set. Configure the allow list to fail
the build on a licence outside it and on a package with no licence at all.

- [ ] **Step 2: Prove it reports the bundle and not the package list**

Add a dependency to the manifest, do not import it, build, and confirm it is
**absent** from the notices. Then import it, rebuild, and confirm it appears.
Remove it again.

That is the test that distinguishes this plugin from the tools that walk the
installed tree, and it is worth doing by hand once.

- [ ] **Step 3: Handle the bundler's own injected helper**

The module preload helper ships and has no package to attribute, because a
virtual module has no manifest. Emit the bundler's own notice when that
helper is present in the bundle, and only then.

Attribute the bundler's own copyright only. Its full licence file is
thousands of lines covering tooling that is not distributed, and copying it
wholesale would misrepresent what ships.

- [ ] **Step 4: Commit the generated file and make drift fail**

Check the notices into the repository. Add a CI step that rebuilds and then
fails on any difference between the regenerated file and the committed one.
An unapproved licence fails at build; a new dependency fails at the diff.

- [ ] **Step 5: Prove the gate works**

Make each failure happen on purpose: a package with a licence outside the
list, and a committed file that does not match. Confirm each fails the build,
then undo.

- [ ] **Step 6: Commit**

```bash
git commit -m "feat(web): attribution generated from the bundle"
```

---

### Task 3: Embedding, compression and content negotiation

**Files:**
- Create: `crates/admin/build.rs`, `crates/admin/src/assets.rs`
- Modify: `crates/admin/Cargo.toml`, `crates/admin/src/lib.rs`

The build script precompresses the built bundle and embeds it. Doing the
compression in the build script rather than as a bundler plugin is one fewer
npm dependency and one fewer compatibility risk with a bundler that recently
changed.

**The easy version of content negotiation is wrong in three specific ways**,
and all three are worth writing tests for before the code:

- A quality value of zero means *not acceptable*, so `identity;q=0` forbids
  the uncompressed form, and `*` is a wildcard that has to be handled.
- Without a `Vary` header naming the encoding, a shared cache serves one
  client's brotli bytes to a client that asked for gzip.
- The same document in two encodings is two representations and needs two
  entity tags, or a conditional request gets the wrong answer.

- [ ] **Step 1: Write the failing tests**

A client accepting brotli gets brotli. One accepting only gzip gets gzip. One
accepting nothing gets the identity bytes. One sending `identity;q=0` with no
other acceptable encoding is refused rather than served uncompressed. Every
response names the encoding in `Vary`. Two encodings of one document have
different entity tags.

- [ ] **Step 2: Make the absence of a built interface a working build**

With no built bundle present, the crate compiles and serves a page saying the
interface was not built and how to build it. Test it, because this is the
path every Rust-only contributor takes and nobody will notice it breaking.

Tell the build system to rerun when the built directory changes.

- [ ] **Step 3: Implement, run, and confirm the tests have teeth**

Break each of the three: ignore quality values, drop the `Vary`, give both
encodings one entity tag. Each must fail its test.

- [ ] **Step 4: Add the bundle to the budget script**

The budget script currently skips the bundle and says so. Now it can measure.
Make it measure, and report the headroom.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(admin): serve the interface, negotiated and precompressed"
```

---

### Task 4: Development reaches the broker

**Files:**
- Modify: `services/web/vite.config.ts`, `services/web/package.json`
- Create or modify: `services/web/README.md`

The development server proxies the API and the events stream to the broker,
with the hot-reload socket left alone.

- [ ] **Step 1: Configure the proxy and prove it by hand**

Start the broker, start the development server, and confirm: an API call
reaches the broker, and the events stream arrives **incrementally** rather
than in one lump at the end.

The second is the one that breaks. A proxy or a compression layer will buffer
a stream unless told not to, and the symptom is an interface that updates in
clumps, which is miserable to diagnose later.

- [ ] **Step 2: Write down the known gap**

The development server does not propagate a client disconnect to the backend,
so the per-stream cleanup path is never exercised in development. Say so in
the readme next to the proxy configuration, and make sure that path has a
test against the broker directly rather than relying on anyone noticing.

- [ ] **Step 3: Write the readme**

How to run both, what the proxy does, why it is this direction, and the gap
above. Short.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(web): the development server reaches the broker"
```

---

### Task 5: The pipeline works end to end

**Files:**
- Create: `crates/sdmqtt/tests/interface.rs`

One test, against the real binary: build the interface, start the broker,
fetch the root of the admin listener, and assert the served document is the
built application rather than the placeholder.

Then fetch it again claiming to accept brotli and assert the bytes are
brotli and decompress to the same document.

- [ ] **Step 1: Decide how this behaves without a built interface**

It cannot require Node, because that is the rule this plan set in task 3. So
it must skip, loudly, when the interface has not been built, and say what to
run. A test that silently passes when it could not check anything is the
thing convention 5 exists about.

- [ ] **Step 2: Write it, run it both ways, commit**

```bash
git commit -m "test: the interface reaches a browser from the binary"
```
