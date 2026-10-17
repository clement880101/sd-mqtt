# The admin API and interface

A REST API, an events stream, Prometheus metrics, health endpoints and the embedded interface, on a listener guarded by where it listens and by API keys.

```toml
[admin]
address = "127.0.0.1:1884"        # the default
allowed_hosts = ["broker.internal"]   # only if you reach it through a name
```

The admin listener is bound to loopback by default, and binding it
further with no `admin.token` set is refused at startup rather than
warned about: the API publishes. On loopback with no token, a mutating
request that presents nothing is still answered; one that presents a
key is judged by it.

It also checks the name a request was made under. An **address** is
always answered — by a browser, a probe, a scraper, `curl` — and so is
`localhost`. A **hostname** is answered only if it is in
`allowed_hosts` (or `SDMQTT_ADMIN_ALLOWED_HOSTS`, comma separated).
That is not fussiness: an ordinary cross-origin request from a web page
is already stopped by a preflight nothing here answers, and DNS
rebinding gets around that by making the page's request same-origin.
The name is the one thing the attack cannot forge. If you reach the
listener through a name of your own, put it in the list; the refusal
says so.
