# API keys

Keys with read, write and admin scopes for the admin API, shown once and stored as digests.

Mutating requests to a guarded admin listener need an **API key** with a
scope that covers them: `read` opens taps, `write` also publishes and
writes rules, `admin` also manages users, access control rules, keys and
nodes. A listener is guarded when `admin.token` is set, which binding it
anywhere but loopback requires. On loopback with no token, a mutating
request that presents nothing is still answered — the listener is
reachable only from the machine itself — and one that presents a key is
judged by it, so a wrong or too-narrow key is refused there too. The
configured `admin.token` is a key with `admin` scope, which is how the
first key is minted:

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"ci","scope":"write"}' http://127.0.0.1:1884/api/v1/keys
```

The answer carries the key, `sdmqtt_<id>_<secret>`, **once**: the broker
keeps a digest of it and nothing that would give it back. `GET
/api/v1/keys` lists identifiers, names and scopes; `DELETE
/api/v1/keys/{id}` revokes, and the revocation replicates to every node
like a user's removal: at once on the node that revoked it, and on each
other node as the register arrives, so a node partitioned from the rest
keeps accepting the key until it hears. A real key that is too narrow is answered `403`
with both scopes named; a wrong one is `401`. A node with no store
holds no keys, and the token is then the only credential.

The **Settings** page does all of this in a browser: paste a key or the
token at the top — it is kept for that tab and forgotten when it
closes — and manage users, access control rules and keys below it. It
also lists this node's listeners, which stay in the file.
