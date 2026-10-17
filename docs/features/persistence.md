# Persistence

Sessions, subscriptions and queued messages survive a restart once the broker has a directory to keep them in.

```toml
[storage]
directory = "/var/lib/sdmqtt"   # no default: without it, nothing is stored
sync = "immediate"              # or "relaxed", which is for measuring only
flush_interval_ms = 2
flush_records = 1000
```

**With no `storage.directory` the broker writes nothing down.** Sessions,
their subscriptions and their queued messages last as long as the
process and no longer, and starting this way logs a warning saying so.
That is deliberate: refusing to start without a directory would break
the promise that no configuration is a working configuration, and
defaulting to one would drop a database file into whatever directory the
service happened to start in.

Give it a directory and a restart preserves durable sessions, their
subscriptions and their queued messages, and no acknowledgement is sent
to a publisher before its message is on the disk.

The directory is created if it is not there, and the store is the file
`sdmqtt.redb` inside it. One broker per directory: a second is refused
with the path it could not take.

**A store written by an earlier build is refused rather than migrated.**
The stored format carries a version and the check on it is exact, so a
broker that meets a store it does not know refuses to start and names
both versions:

```
store format version 10 is not readable by this build, which reads version 11.
The store is not damaged; it was written by a different build. Releases before
1.0 do not migrate a store between formats: see CHANGELOG.md for what moved and
what starting with a fresh store directory throws away.
```

The file is not damaged; it is being read by the wrong build, and
guessing at an unknown layout would restore nonsense from healthy bytes.
Releases before 1.0 do not migrate a store: move or delete the store
directory and start again. `CHANGELOG.md` says, per milestone,
whether the version moved and what starting again throws away.
