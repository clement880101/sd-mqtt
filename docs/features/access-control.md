# Access control

Rules that say who may publish and subscribe where, deny by default once one exists.

```toml
[[acl]]
id = "no-secrets"
order = 10                        # lower is consulted first; 100 by default
subject = "anyone"                # or "user:alice", "client:gateway-1", "client:sensor-*", "user:*"
filter = "secret/#"
subscribe = false                 # an opinion on subscribing; publish is left unsaid

[[acl]]
id = "everything-else"
subject = "anyone"
filter = "#"
publish = true
subscribe = true
```

With no rule at all, every publish and subscribe is allowed, as it was
through milestone 6. **The first rule flips the default to deny**: from
then on an operation is allowed only by a rule that says so, which is
why the second rule above exists. **Through the API, write the broad
allow first**: each rule is its own write and replicates on its own, so
a narrow deny written alone denies everything else, on every node,
until the allow arrives. Rules are consulted in `order`, then
by identifier, and the first whose subject and filter match and that
has an opinion on the operation decides; a rule with no opinion is
skipped. A subject's name may end in `*` to mean every name with that
prefix: `client:sensor-*` is every client whose identifier starts
`sensor-`, and `user:*` is every authenticated connection, which is how a
rule tells a logged-in client from an anonymous one. A `*` anywhere else
is refused. A rule's filter matches a published topic the way a
subscription does, wildcards and all, and a requested subscription
filter the way a topic name would; a shared subscription is judged by
the filter it shares, so a rule about `fleet/#` covers
`$share/workers/fleet/#` too. A refused subscribe is answered
`0x87` on version 5 and `0x80` on 3.1.1; a refused publish is `0x87` on
its acknowledgement, and an at-most-once publish, which has none, is
dropped and counted on `sdmqtt_messages_dropped_total{reason="refused"}`.

Rules in the file are this node's configuration. Rules written through
the API — `POST /api/v1/acl` with the same fields, `GET /api/v1/acl` in
consultation order, `DELETE /api/v1/acl/{id}` — are durable metadata
and replicate to every node like a user; both sets are consulted
together by `order`.

A client never publishes into the dollar-prefixed namespace, rules or
no rules: `$SYS/...` and the like are the broker's, and a client's
publish there is refused with `0x87` (or dropped and counted at
at-most-once). Only the first level is reserved; `a/$b` is an ordinary
topic.
