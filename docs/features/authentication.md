# Users and authentication

Users with argon2 passwords, SCRAM-SHA-256 over MQTT 5's AUTH exchange, an anonymous switch, and a limit on failed logins.

```toml
[auth]
anonymous = true                  # false refuses a client with no username
scram_iterations = 4096           # PBKDF2 rounds for a new SCRAM-SHA-256 verifier
max_failures_per_minute = 10      # failed logins from one address before it waits; 0 is no limit
```

Users are not in the file. They are durable metadata, written through
the API and replicated to every node like a rule:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"alice","password":"hunter2"}' http://127.0.0.1:1884/api/v1/users
```

The password is hashed with argon2id for a password login. With
`"scram": true` in the same request it is also derived into a
SCRAM-SHA-256 verifier for the AUTH exchange, at that moment, since a
verifier cannot be made later from the hash. Enrolment is asked for
rather than automatic because a verifier is a secret equivalent to a
private key. Neither form is ever returned: `GET /api/v1/users` lists
names and which credential forms exist. Posting the same name again
changes the password, and without `"scram": true` withdraws an
enrolment. `DELETE /api/v1/users/{name}` removes the user, and the
removal replicates as a tombstone. A client presenting a wrong password,
or a name nobody has, is refused with 0x87; the two are one answer, so
nobody can learn which names exist by trying. `anonymous` is a switch
and nothing else: creating the first user does not flip it. A node with
no store holds no users and admits by the default alone.
`SDMQTT_AUTH_ANONYMOUS` and `SDMQTT_AUTH_SCRAM_ITERATIONS` set the two.
The iteration count is what a SCRAM client pays on every connect and what
an attacker with a stolen verifier pays per guess; the broker pays it only
when a password is set. 4,096 costs a laptop 0.3 ms, so a fleet of capable
clients loses nothing by raising it, and a microcontroller may need it
where it is (`docs/decisions/0002-scram-iterations.md`).

**Repeated failures from one address are slowed.** After
`max_failures_per_minute` failed logins — a wrong password, an unknown
name, a failed SCRAM proof or re-authentication — a connect from that
address is refused before its credentials are looked at, with 0x9F
(`3`, server unavailable, on 3.1.1), until enough of the minute has
passed for one more try. An IPv6 address is counted by its /64, which
is what one host is usually given. The broker tracks at most 65,536
addresses and logs a warning when that is full.
`sdmqtt_authentications_total{outcome}` counts every decision,
`sdmqtt_rate_limited_total{limiter="authentication"}` the connects
turned away, and `sdmqtt_auth_failure_addresses` how many addresses are
held. `SDMQTT_AUTH_MAX_FAILURES_PER_MINUTE` sets it.

A version 5 client of a user enrolled in SCRAM can authenticate without
sending its password at all, with **SCRAM-SHA-256** over the AUTH
exchange (RFC 7677): it names the
method in CONNECT, the broker challenges, the client proves, and the
connect acknowledgement carries the server's own proof back. The same
exchange renews the credentials mid-session (`AUTH` with
re-authenticate), as the same user only. A wrong password and an unknown
name are refused the same way at the same step, so nobody can learn
which names exist by trying. The `-PLUS` channel-binding variant is not
offered and a client asking for it is told `0x8C`, as is any other
method. Passwords are taken as written: SASLprep is not applied, so a
password that is plain ASCII behaves exactly as RFC 7677 describes and
one that is not should be set through a client that normalises it.
