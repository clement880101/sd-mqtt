# 0002: The SCRAM iteration count stays at 4,096 by default

**Status:** decided, milestone 7, 2026-10-15. Section 17's "SCRAM
iteration count default".

## The question

A SCRAM-SHA-256 verifier is derived with PBKDF2 at some iteration count,
and the client runs the same derivation on every connect it does not
cache. More rounds make a stolen verifier slower to attack offline and a
constrained client slower to connect. Section 17 asked for the number to
be picked against measurements rather than guessed.

## What was measured

`salted_password` in `crates/server/src/scram.rs`, the same aws-lc-rs
PBKDF2-HMAC-SHA-256 the server uses, timed by the ignored test
`scram_cost` (release build, 20 derivations each):

| Iterations | Apple M2 Pro, one core | Linux container, a quarter of one core |
|---:|---:|---:|
| 4,096 | 0.30 ms | 4.2 ms |
| 10,000 | 0.74 ms | 5.9 ms |
| 50,000 | 3.8 ms | 34 ms |

The quarter-core figures are noisy, because a CPU quota is enforced in
scheduler periods, and they stand in for a slow device rather than
measuring one. **No microcontroller was measured**: none was available.
By operation count, each iteration is two SHA-256 compressions, so a
device a hundred times slower than the M2 Pro core spends about 30 ms at
4,096 and about 380 ms at 50,000, per connect it does not cache.

## Decision

**4,096 stays the default**, and `auth.scram_iterations` stays
configurable with a floor of 1,000.

- It is RFC 7677's floor and the common default: PostgreSQL, RabbitMQ
  and EMQX all ship it. A client written against any of them already
  copes with it.
- The server never pays the cost at connect: the verifier is stored, so
  the iteration count is the client's cost and the attacker's, not the
  broker's.
- The verifier this protects is narrower here than in those systems:
  since milestone 7 it exists only for users explicitly enrolled in
  SCRAM, and it is held as a secret the API never returns. A user with
  no enrolment has only the argon2 hash at rest.
- A fleet of capable clients loses nothing by raising it: 50,000 costs
  a laptop 4 ms. The README says so beside the setting.

## What would reopen it

A measurement on a real constrained device showing 4,096 is too slow to
connect with, or the verifier store becoming reachable by something other
than the node's own disk.
