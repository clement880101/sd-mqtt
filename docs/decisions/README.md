# Decisions

Architecture decision records: one file per question the design left
open or a measurement had to settle, numbered in the order they were
taken. Section 17 of the design says each open question is resolved by
the milestone that needs it and recorded here; section 15 says a budget
moves only by a recorded decision, and that is here too.

Each record says what was decided, what it was decided from — a
measurement, with the machine it ran on, wherever there is one — what
it costs, and what would reopen it. A record is not edited to change
its decision; a later record supersedes it and says so.

| Number | Decision | Milestone |
|---|---|---|
| [0001](0001-connection-target.md) | The connection target is met: 100,000 held on one node | 7 |
| [0002](0002-scram-iterations.md) | The SCRAM iteration count stays at 4,096 by default | 7 |
| [0003](0003-channel-binding.md) | SCRAM-SHA-256-PLUS is not offered in this release | 7 |
| [0004](0004-admin-http2.md) | The admin listener stays HTTP/1.1 | 7 |
| [0005](0005-platforms.md) | Release artifacts for Linux and macOS, not Windows | 7 |
