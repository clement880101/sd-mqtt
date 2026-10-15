# 0004: The admin listener stays HTTP/1.1

**Status:** decided, milestone 7, 2026-10-15. Section 11.1's "the limit
is revisited when TLS lands".

## The question

Browsers allow six HTTP/1.1 connections per origin and the events stream
holds one, so the interface opens exactly one stream for the whole
application. HTTP/2 would remove the limit, and browsers negotiate it
only over TLS. Section 11.1 deferred it until TLS existed.

## Decision

**HTTP/1.1 stays, and the one-stream rule with it.** TLS landed in
milestone 7 on the MQTT listeners, not on the admin listener, which
binds loopback by default and is reached through an operator's own
proxy when it is reached from elsewhere. With no TLS of its own there is
nothing for a browser to negotiate HTTP/2 over, and the proxy in front
of it can speak HTTP/2 to the browser while speaking HTTP/1.1 here.

## What would reopen it

TLS on the admin listener itself; or a page that needs a second
long-lived stream.
