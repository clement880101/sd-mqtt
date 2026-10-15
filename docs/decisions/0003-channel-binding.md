# 0003: SCRAM-SHA-256-PLUS is not offered in this release

**Status:** decided, milestone 7, 2026-10-15. Section 17's "Channel
binding".

## The question

`SCRAM-SHA-256-PLUS` binds the exchange to the TLS channel it runs in,
so an attacker who terminates TLS in the middle cannot relay the proof.
Section 17 said whether that threat applies depends on whether
deployments terminate TLS at a proxy, and left it to milestone 7.

## Decision

**Not offered.** The broker answers only `SCRAM-SHA-256`; a client
asking for `-PLUS` is told 0x8C, bad authentication method, and a client
that sets the `y` flag — it supports binding and believes the server
does not — is served, as RFC 5802 requires.

- Where TLS ends at a proxy, which is how brokers are commonly run behind
  load balancers, the broker never sees the client's channel and cannot
  bind to it. `-PLUS` would be refused in exactly the deployments the
  threat model is about.
- Where TLS ends at the broker, an attacker in the middle must already
  hold a certificate the client trusts. That is a compromise of the
  client's trust store, which binding mitigates but which is not this
  release's to defend.
- MQTT clients that implement `-PLUS` are rare, so the cost of offering
  it, `tls-exporter` support through rustls and a second code path in
  the exchange, buys little.

## What would reopen it

A deployment that terminates TLS at the broker and needs protection
against a trusted-certificate compromise, or common MQTT client libraries
shipping `-PLUS`.
