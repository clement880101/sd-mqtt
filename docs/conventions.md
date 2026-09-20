# Conventions

Rules this project works by, each earned by a specific failure. Plans
reference this rather than restating it, so the list can grow.

## 1. The writer refuses what the reader rejects

If a decoder rejects something, the encoder must refuse to produce it.
Otherwise the system emits bytes it will not read back, and the bug
surfaces at a peer rather than at its source.

Found when the codec would happily write a publish carrying a packet
identifier at quality of service zero, a zero identifier, a wildcard in a
topic, and a subscription with no-local on a shared filter. Each round
tripped to something different or was refused by the decoder that had
just written it.

## 2. Assert the exact error, not its category

A test asserting only `matches!(err, MalformedPacket(_))` passes for any
other cause of the same category, so it does not guard the check it is
named after.

Found twice. Six codec tests asserted a category, and a truncation bug was
hiding behind two of them: they were passing without testing anything,
because an unrelated failure produced the same category.

## 3. Derive rules from the specification, not from the plan

The plan has been wrong. Read the standard.

Two practical notes. The fetch tool has mis-transcribed specification
tables more than once, including a bit layout it got wrong in two
different ways on two attempts. And the published HTML is a Word export
that splits words across tags, so a plain search finds nothing: download
with `curl` and strip tags before reading.

The worst instance: the design spec said a resumed message is "requeued
with the duplicate flag" and omitted that it must keep its original packet
identifier. The plan inherited the omission and so did the code. The
result delivers a message twice to a peer still waiting on the old
identifier, and later discards a genuinely new message when the counter
wraps onto the stranded one. Duplication and loss from one sentence.

## 4. Prove a test can fail

Break the implementation deliberately, confirm that test and only that
test fails, then restore. Back the file up with `cp`, not
`git checkout --`, because the change being proven is usually itself
uncommitted and `git checkout` discards it.

## 5. Ask what a test cannot reach

This is the one that keeps recurring, in a different disguise each time.
Four instances so far, and in every one the code was right and the guard
was not, which is the failure mode that survives review by looking green.

- **A round trip cannot see a consistent error.** A property identifier
  read from and written to the same wrong byte round trips perfectly and
  is wrong against every other implementation. So does a flag bit read
  from and written to the same wrong position. Fixed by golden vectors
  derived from the specification rather than captured from the encoder.
- **A generator that cannot produce a case.** The differential tests could
  not generate a bare multi-level wildcard, so deleting the guard that
  keeps a subscribe-to-everything client away from broker status topics
  survived twenty thousand cases.
- **A path no test walks.** The packet allocator's loop that skips
  identifiers still in use was unexercised, because reaching it needs the
  counter to wrap onto something held, and no test wrapped.
- **A test measuring the wrong dimension.** The keep-alive test asserted
  which connections expire in a given second. That depends only on each
  connection's own timestamp, not on where it sits in the ring, so
  degrading the sweep from bounded work to scanning the whole population
  every second changed no outcome and passed.

- **A test asserting an absence.** The broker's keep-alive was guarded by
  one test asserting that no timeout occurred, and every property fixed
  the interval at zero, which disables the mechanism. Five mutations
  survived the whole suite, including one that deleted the keep-alive
  entirely. A test that asserts nothing happened cannot tell a working
  feature from a missing one.
- **A test measuring the cheap half.** The keep-alive ring proved its
  sweep was bounded while an operation beside it scanned the whole
  population on every departure, measured at forty microseconds with
  fifty thousand armed. The accessor added to assert bounded work
  measured only the half that already was.

The question to ask of any new test: what would have to be true for this
to fail, and can the inputs actually make it true? If the answer is "the
feature would have to be absent", the test is not guarding it.

## 6. Beware a harness that mirrors the implementation

A test that accepts whatever the code accepts cannot catch a wrong
acceptance. The conservation harness treats any acknowledgement the state
machine accepts as a legitimate ending, so a bug in that acceptance is
reflected rather than caught. Recorded as a known limitation rather than
pretended away.

## 7. Report what happened, not what was expected

A mutation that survives is a finding. Twice an implementer reported "no
test failed" and traced why, and both times the test was guarding the
wrong thing. An implementer also corrected a test when the assertion was
wrong rather than the code, and another corrected a figure in a commit
message that was mine and wrong.

The reverse also holds: when a property fails, check whether it asserts
something the protocol actually promises before assuming a defect. Once it
did not, and the harness was wrong.
