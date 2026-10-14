# Listeners Implementation Plan (milestone 7, part 1 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A broker can listen on more than one socket, over plain TCP, TLS,
WebSocket and WebSocket over TLS, with a certificate that renews without a
restart, and each listener carries its own policy: a maximum quality of
service, whether retained messages are accepted, and which durability mode a
publish on it gets.

**Architecture:** One `Shared` broker, several accept loops. The accept loop
wraps the accepted socket in whatever the listener's kind needs — a TLS
handshake, a WebSocket upgrade, or both — and hands `connection::serve` the
same `AsyncRead + AsyncWrite` it has always taken. Policy travels in
`ConnectionConfig`, which is already per listener. The TLS certificate is
served through a resolver that re-reads its files on an interval and swaps
them atomically, so a renewed certificate is used by the next handshake.

**Tech Stack:** `tokio-rustls` 0.26 server side (already linked for the
rules crate's client side, same `aws-lc-rs` provider), `rustls-pki-types`'s
PEM reader, `tokio-tungstenite` for WebSocket framing (one new direct
dependency), `rcgen` as a dev-dependency to mint test certificates, the
Paho interoperability suite through a stdlib-only Python forwarder.

**Spec:** `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`, sections
12.1 (the sentence "TLS and WebSocket listeners are supported, with
certificate hot reload through a certificate resolver so renewal needs no
restart"), 5.4 ("Maximum QoS and Retain Availability move to milestone 7"),
8.6 (the per-listener durability override deferred from milestone 6), 11.1
(the listeners resource), 16 (milestone 7's exit criteria: "the full
conformance suite green including TLS and WebSocket").

**Milestone 7 is three plans.** This is the first. The second is
authentication and access control (argon2 users, SCRAM-SHA-256 with
re-authentication, access control rules, the dollar-topic refusal, the two
rate limiters, the secret wrapper, and the Settings page). The third is
release (the container image, cross-compiled artifacts, mimalloc, the
release job, documentation). Each produces working software on its own, and
this one goes first because the second's Settings page lists listeners and
the third ships whatever the first two built.

## Global Constraints

- `docs/conventions.md`, all eight plus 7b and 7c. Prove every test can
  fail (4); restore mutations byte-identical with a newer timestamp (7b);
  check free space before calling a test flaky (7c); walk the spec sections
  at the close (8).
- Direct dependencies across broker crates: **under 35** (section 15; 25
  today, this plan adds `tokio-tungstenite` and no other).
- Release binary, stripped, link-time optimised: **under 15 MB** (8.47 MB
  today).
- Every discard path has a metric and, where a hook can see it, a hook
  (section 11.3). A refused handshake and a refused upgrade are discards.
- A node that does not cluster leaves cluster series absent, not zero.
- Reason strings are not sent (section 5.4). The reason code is the
  contract.
- Version 3.1.1 has no reason codes on most acknowledgements and none on
  disconnect: a refusal that v5 answers with a code, 3.1.1 answers by
  closing the socket (section 13).
- The conformance baseline, `conformance/known-failures.txt`, changes only
  with a classified line. Defaults must leave the existing passes passing.
- Golden vectors come from the standard, not from this implementation
  (convention 5). WebSocket framing has RFC 6455's examples; use them.
- Configuration keys are `snake_case`, every one has an environment
  variable where the existing ones do, and every refusal at startup names
  the key and what to do.

## Review Focus

Inputs the spec implies and no task's tests would otherwise exercise, most
likely to bite first. Each has its test added to the owning task below.

1. **A certificate file replaced by an empty or half-written one** during a
   renewal. The resolver must keep serving the previous certificate and say
   so once, not serve nothing and not crash. (Task 4.)
2. **A WebSocket client that sends MQTT in text frames**, or splits one
   MQTT packet across several binary frames, or interleaves a ping. The
   first is refused, the second and third work. (Task 5.)
3. **A client whose CONNECT carries a retained will on a listener where
   retain is unavailable.** The connect is refused with 0x9A, not accepted
   with the will quietly stripped. (Task 1.)
4. **Two listeners configured on the same address.** Refused at startup
   with both names, rather than the second failing to bind after the first
   is already serving. (Task 3.)
5. **A TLS listener whose key does not match its certificate**, or whose
   key is encrypted. Refused at startup, naming the file, rather than
   accepted and then failing every handshake. (Task 4.)

---

## Read first

- `crates/server/src/listener.rs` in full. The accept loop is where every
  task here lands, and it is short.
- `crates/server/src/connection.rs`: `serve`, `ConnectionConfig`, the
  `acknowledgement` function near line 785, the inbound publish at line
  908 and the two gate sites near lines 1225 and 1244, which are the
  shape a per-listener refusal takes.
- `crates/sdmqtt/src/config.rs`: `Listener`, `Config::server`, `validate`,
  and the environment block near line 1486. Every key this plan adds goes
  through all four.
- `crates/sdmqtt/src/main.rs` near lines 1018 and 1283: where the one
  listener is bound and reported.
- `conformance/run.sh` and `conformance/known-failures.txt`.
- Section 13 of the spec, for how a 3.1.1 refusal is expressed.

## Four things settled before you start

**One `Shared`, several listeners.** A listener is an accept loop and a
policy, nothing more. Sessions, subscriptions, retained messages and the
store are the broker's and are shared by every listener on the node; a
client that connects on TLS and reconnects on TCP resumes the same session,
mountpoint permitting. Do not give a listener its own broker.

**Policy is enforced where the packet arrives, and advertised at connect.**
The maximum QoS and retain availability are properties of the listener the
connection came in on. They are advertised in CONNACK (v5) so a conformant
client never sends what will be refused, and they are enforced on the
packet anyway, because a client may not look. A v5 violation is a
DISCONNECT with the code the standard names; a 3.1.1 violation closes the
socket. A subscription's granted QoS is clamped to the maximum and the
SUBACK says so, which is not a refusal.

**TLS is below WebSocket, and both are below MQTT.** `wss` is a TLS
handshake, then an HTTP upgrade, then MQTT. Each layer is a function from
one `AsyncRead + AsyncWrite` to another, and `serve` is the same function
for all four kinds. If a task finds itself teaching `serve` about frames or
handshakes, it has gone wrong.

**Hot reload is polling, not notification.** The resolver re-reads the
certificate and key files every `reload_ms` and swaps them when they parse;
a file that does not parse leaves the previous pair in place and is logged
once per change, not once per interval. File-change notification APIs
differ per platform and miss a replace-by-rename on some; a poll of two
files a minute costs nothing measurable.

---

### Task 1: A listener's maximum QoS and retain availability

**Files:**
- Modify: `crates/sdmqtt/src/config.rs` (`Listener`, `Config::server`,
  `validate`, the environment block, tests)
- Modify: `crates/server/src/connection.rs` (`ConnectionConfig`,
  `acknowledgement`, the connect path near line 594, the inbound publish
  near line 908, the subscribe handling)
- Modify: `README.md`, `CHANGELOG.md`
- Test: `crates/server/src/connection.rs` tests, `crates/sdmqtt/src/config.rs`
  tests

**Interfaces:**
- Produces: `ConnectionConfig { max_qos: QoS, retain_available: bool, .. }`
  with defaults `QoS::ExactlyOnce` and `true`; config keys
  `listener.max_qos` (0, 1 or 2) and `listener.retain_available` (bool);
  environment `SDMQTT_MAX_QOS` and `SDMQTT_RETAIN_AVAILABLE`.

- [x] **Step 1: Write the failing connection tests**

In `crates/server/src/connection.rs`'s test module, beside
`serve_one_with_config`:

```rust
    /// A listener capped at one advertises it, and the cap is enforced
    /// on the packet as well, because a client may not look.
    #[tokio::test]
    async fn a_listener_capped_at_qos_one_says_so_and_disconnects_a_qos_two_publish() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let config = ConnectionConfig {
            max_qos: QoS::AtLeastOnce,
            ..ConnectionConfig::default()
        };
        let mut client = serve_one_with_config(Arc::clone(&shared), config.clone());
        let ack = connected(&mut client, Version::V5).await;
        assert_eq!(ack.reason, ReasonCode::Success);
        assert_eq!(
            ack.properties.maximum_qos,
            Some(1),
            "3.2.2.3.4: a server that does not support QoS 2 says so"
        );

        write(
            &mut client,
            &Packet::Publish(mqtt_codec::packet::Publish {
                dup: false,
                qos: QoS::ExactlyOnce,
                retain: false,
                topic: "site/1/temp".into(),
                packet_id: Some(9),
                payload: bytes::Bytes::from_static(b"21.5"),
                properties: Properties::default(),
            }),
            Version::V5,
        )
        .await;
        match read(&mut client, Version::V5).await {
            Packet::Disconnect(gone) => assert_eq!(
                gone.reason,
                ReasonCode::QosNotSupported,
                "[MQTT-3.2.2-11]: a publish above the maximum is a protocol error"
            ),
            other => panic!("expected a disconnect, got {other:?}"),
        }
        assert_eq!(
            shared.metrics().socket_closed(SocketOutcome::ProtocolError).get(),
            1
        );
    }

    /// The default advertises nothing and refuses nothing, which is what
    /// every existing test and the conformance suite rely on.
    #[tokio::test]
    async fn the_default_listener_advertises_no_maximum() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let mut client = serve_one(Arc::clone(&shared));
        let ack = connected(&mut client, Version::V5).await;
        assert_eq!(ack.properties.maximum_qos, None);
        assert_eq!(ack.properties.retain_available, None);
    }

    /// A subscription above the cap is granted at the cap, not refused:
    /// 3.8.4 says the granted level may be lower than requested.
    #[tokio::test]
    async fn a_subscription_above_the_cap_is_granted_at_the_cap() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let config = ConnectionConfig {
            max_qos: QoS::AtMostOnce,
            ..ConnectionConfig::default()
        };
        let mut client = serve_one_with_config(Arc::clone(&shared), config);
        connected(&mut client, Version::V5).await;
        write(
            &mut client,
            &Packet::Subscribe(mqtt_codec::packet::Subscribe {
                packet_id: 1,
                filters: vec![mqtt_codec::packet::SubscriptionOptions {
                    filter: "site/+/temp".into(),
                    qos: QoS::ExactlyOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: mqtt_codec::packet::RetainHandling::DoNotSend,
                }],
                properties: Properties::default(),
            }),
            Version::V5,
        )
        .await;
        match read(&mut client, Version::V5).await {
            Packet::SubAck(ack) => assert_eq!(ack.reasons, vec![ReasonCode::GrantedQos0]),
            other => panic!("expected a subscription acknowledgement, got {other:?}"),
        }
    }

    /// Retain unavailable: advertised as zero, a retained publish is a
    /// disconnect with 0x9A, and a retained will is refused at connect
    /// rather than accepted with the flag quietly dropped.
    #[tokio::test]
    async fn a_listener_without_retain_refuses_a_retained_publish_and_a_retained_will() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let config = ConnectionConfig {
            retain_available: false,
            ..ConnectionConfig::default()
        };
        let mut client = serve_one_with_config(Arc::clone(&shared), config.clone());
        let ack = connected(&mut client, Version::V5).await;
        assert_eq!(ack.properties.retain_available, Some(0));
        write(
            &mut client,
            &Packet::Publish(mqtt_codec::packet::Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic: "site/1/temp".into(),
                packet_id: None,
                payload: bytes::Bytes::from_static(b"21.5"),
                properties: Properties::default(),
            }),
            Version::V5,
        )
        .await;
        match read(&mut client, Version::V5).await {
            Packet::Disconnect(gone) => assert_eq!(gone.reason, ReasonCode::RetainNotSupported),
            other => panic!("expected a disconnect, got {other:?}"),
        }

        // Review focus 3: the will.
        let mut client = serve_one_with_config(Arc::clone(&shared), config);
        let mut connect = connect("with-a-will", 0);
        connect.will = Some(mqtt_codec::packet::Will {
            topic: "site/1/gone".into(),
            payload: bytes::Bytes::from_static(b"bye"),
            qos: QoS::AtMostOnce,
            retain: true,
            properties: Properties::default(),
        });
        write(&mut client, &Packet::Connect(Box::new(connect)), Version::V5).await;
        match read(&mut client, Version::V5).await {
            Packet::ConnAck(ack) => assert_eq!(
                ack.reason,
                ReasonCode::RetainNotSupported,
                "3.1.2.7: a retained will on a server without retain is refused"
            ),
            other => panic!("expected a connack, got {other:?}"),
        }
    }

    /// Over 3.1.1 there is no code to send: the socket closes.
    #[tokio::test]
    async fn a_v311_publish_above_the_cap_closes_the_socket() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let config = ConnectionConfig {
            max_qos: QoS::AtMostOnce,
            ..ConnectionConfig::default()
        };
        let mut client = serve_one_with_config(Arc::clone(&shared), config);
        connected(&mut client, Version::V311).await;
        write(
            &mut client,
            &Packet::Publish(mqtt_codec::packet::Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: "site/1/temp".into(),
                packet_id: Some(3),
                payload: bytes::Bytes::from_static(b"21.5"),
                properties: Properties::default(),
            }),
            Version::V311,
        )
        .await;
        let mut buf = [0u8; 1];
        assert_eq!(
            client.read(&mut buf).await.expect("a read"),
            0,
            "the socket is closed without an acknowledgement"
        );
    }
```

Check the `Will` struct's field names in `crates/mqtt-codec/src/packet.rs`
before relying on the ones above; the plan has been wrong before
(convention 3). If `SocketOutcome::ProtocolError` does not exist by that
name, use the outcome the existing protocol-error path counts under — find
it by searching `connection.rs` for `MalformedPacket` handling — and do not
add a new one.

- [x] **Step 2: Run them to see them fail**

Run: `cargo test -p sdmqtt-server --lib a_listener_capped the_default_listener a_subscription_above a_listener_without a_v311_publish_above`
Expected: compile errors on `max_qos` and `retain_available`.

- [x] **Step 3: Add the two fields and advertise them**

In `ConnectionConfig`:

```rust
    /// The highest quality of service this listener serves. Section 5.4.
    ///
    /// Advertised in the acknowledgement when below two, and enforced on
    /// every publish regardless, because advertising is advice and a
    /// client may not take it. A subscription above it is granted at it.
    pub max_qos: QoS,
    /// Whether this listener accepts retained messages. Section 5.4.
    ///
    /// Advertised as zero when false; a retained publish is then a
    /// protocol error and a retained will refuses the connect.
    pub retain_available: bool,
```

with `max_qos: QoS::ExactlyOnce` and `retain_available: true` in
`Default`. In `acknowledgement`:

```rust
            maximum_qos: match config.max_qos {
                QoS::ExactlyOnce => None,
                QoS::AtLeastOnce => Some(1),
                QoS::AtMostOnce => Some(0),
            },
            retain_available: if config.retain_available { None } else { Some(0) },
```

- [x] **Step 4: Enforce on the publish, the subscribe and the will**

On the inbound publish path (near line 908), before the packet reaches the
broker:

```rust
        if publish.qos > self.config.max_qos {
            return self.protocol_error(ReasonCode::QosNotSupported);
        }
        if publish.retain && !self.config.retain_available {
            return self.protocol_error(ReasonCode::RetainNotSupported);
        }
```

where `protocol_error` is whatever the existing malformed-packet path does
to send a v5 DISCONNECT with a code and close a 3.1.1 socket — reuse it;
do not write a second. If it takes no code today, give it one.

For the subscribe: clamp each filter's `qos` to `self.config.max_qos`
before handing the packet to the broker, so the broker grants what it was
asked and the SUBACK reflects the cap. Clamp a copy; the packet is
borrowed.

For the will: in the connect path, after decoding and before
`broker.connect`:

```rust
        if let Some(will) = &connect.will {
            if will.retain && !self.config.retain_available {
                return self.refuse_connect(ReasonCode::RetainNotSupported);
            }
        }
```

using the existing refuse-at-connect helper (the one that answers
`NotAuthorized`); for 3.1.1 that helper already closes the socket, which is
what section 13 wants.

- [x] **Step 5: Run the tests to see them pass**

Run: `cargo test -p sdmqtt-server --lib connection::`
Expected: all pass, the five new ones included.

- [x] **Step 6: Prove each can fail**

Back up `connection.rs` with `cp`. One at a time: delete the publish QoS
check (the first and fifth tests fail); delete the will check (the fourth
fails); replace the clamp with nothing (the third fails); replace the
`maximum_qos` match with `None` (the first fails on the advertise). Restore
with `cp` and `touch`. Report which test failed under each, by name.

- [x] **Step 7: Configuration**

In `crates/sdmqtt/src/config.rs`, `Listener` gains:

```rust
    /// The highest quality of service this listener serves: 0, 1 or 2.
    pub max_qos: u8,
    /// Whether this listener accepts retained messages.
    pub retain_available: bool,
```

defaults `2` and `true`. `validate` refuses `max_qos > 2` with:

```rust
            return Err(ConfigError::Invalid {
                key: "listener.max_qos",
                why: "a quality of service is 0, 1 or 2",
            });
```

(match the existing `ConfigError` variant's shape; the name above is a
guess at it). `Config::server` maps `0 => QoS::AtMostOnce, 1 =>
QoS::AtLeastOnce, _ => QoS::ExactlyOnce` into `ConnectionConfig`. The
environment block gains `SDMQTT_MAX_QOS` and `SDMQTT_RETAIN_AVAILABLE`,
parsed the way `SDMQTT_MAX_CONNECTIONS` is. Add to the config tests, beside
the existing listener assertions near line 2918:

```rust
    #[test]
    fn a_listener_policy_is_read_and_a_bad_qos_is_refused() {
        let config = parse_toml(
            r#"
[listener]
max_qos = 1
retain_available = false
"#,
        );
        assert_eq!(config.listener.max_qos, 1);
        assert!(!config.listener.retain_available);
        let server = config.server();
        assert_eq!(server.connection.max_qos, QoS::AtLeastOnce);
        assert!(!server.connection.retain_available);

        let bad = parse_toml("[listener]\nmax_qos = 3\n");
        let why = bad.validate().expect_err("three is not a quality of service");
        assert!(why.to_string().contains("listener.max_qos"), "{why}");
    }
```

using whatever helper the config tests already use to parse a string (look
for how the test at line 2918 builds its `Config`).

- [x] **Step 8: Docs and commit**

README, under the listener section (search for `max_connections` in the
`[listener]` block; if the block has no prose, add it beside the other
listener keys):

```toml
[listener]
max_qos = 2                       # 0, 1 or 2; advertised to v5 clients
retain_available = true           # false refuses retained messages
```

with two sentences: a publish above the cap is a protocol error (v5 gets
0x9B, 3.1.1 is closed), a subscription above it is granted at it, and a
retained will on a listener without retain refuses the connect with 0x9A.
CHANGELOG under a new `## Unreleased — milestone 7: hardening` heading, an
`### Added` entry for both.

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add -A && git commit -m "feat(listener): a maximum QoS and retain availability per listener"
```

---

### Task 2: A listener's durability mode

**Files:**
- Modify: `crates/server/src/shared.rs` (`confirming`, `with_broker_committed`
  and a sibling that takes the override)
- Modify: `crates/server/src/connection.rs` (`ConnectionConfig`, the
  publish commit near line 1091)
- Modify: `crates/sdmqtt/src/config.rs`, `README.md`, `CHANGELOG.md`, and
  the spec's 8.6 paragraph that says the override is deferred
- Test: `crates/server/src/connection.rs` tests

**Interfaces:**
- Consumes: `Shared::confirming(promised, streamed)`, `Committed::settled`,
  `cluster::Durability`.
- Produces: `ConnectionConfig { durability: Option<cluster::Durability>, .. }`
  (`None` means the global mode);
  `Shared::with_broker_committed_as<T>(&self, mode: Option<Durability>, f,
  promised) -> (T, Committed)`; config key `listener.durability` with
  values `"async"`, `"sync"` or absent; environment `SDMQTT_LISTENER_DURABILITY`.

- [x] **Step 1: Write the failing test**

Beside milestone 6's `synchronous_mode_waits_for_a_follower_and_then_acknowledges`
in `connection.rs`:

```rust
    /// Two listeners on one broker, one synchronous and one not: the
    /// publisher on the synchronous one waits for the follower, and the
    /// publisher on the other does not, with the global mode `async`.
    #[tokio::test]
    async fn a_listener_s_own_durability_overrides_the_node_s() {
        let (shared, copies) = replicating(cluster::Durability::Async, Duration::from_secs(30));
        a_queued_session(&shared, "sensor", "site/+/temp").await;
        let session = SessionKey::new("", "sensor");
        copies.install_followers(|_| {
            vec![cluster::Placed {
                node: [0xaa; 16],
                reachable: true,
            }]
        });
        copies.marked(session.clone(), [0xaa; 16], 0);

        // The asynchronous listener: answered at once, no follower
        // consulted.
        let mut relaxed = serve_one(Arc::clone(&shared));
        connected_as(&mut relaxed, Version::V5, "relaxed").await;
        let ack = tokio::time::timeout(Duration::from_secs(2), publish_once(&mut relaxed, "site/1/temp"))
            .await
            .expect("asynchronous mode does not wait for the follower");
        assert_eq!(ack.reason, ReasonCode::Success);

        // The synchronous listener: waits until the follower confirms.
        let strict = serve_one_with_config(
            Arc::clone(&shared),
            ConnectionConfig {
                durability: Some(cluster::Durability::Sync),
                ..ConnectionConfig::default()
            },
        );
        let mut strict = strict;
        connected_as(&mut strict, Version::V5, "strict").await;
        let releasing = Arc::clone(&copies);
        let told = session.clone();
        let confirmed_at = Arc::new(std::sync::Mutex::new(None));
        let noted = Arc::clone(&confirmed_at);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            *noted.lock().expect("a lock") = Some(std::time::Instant::now());
            releasing.marked(told, [0xaa; 16], u64::MAX);
        });
        let ack = publish_once(&mut strict, "site/1/temp").await;
        let acknowledged_at = std::time::Instant::now();
        assert_eq!(ack.reason, ReasonCode::Success);
        let confirmed = confirmed_at
            .lock()
            .expect("a lock")
            .expect("the acknowledgement arrived before the follower confirmed, so it did not wait");
        assert!(acknowledged_at >= confirmed);
    }
```

- [x] **Step 2: Run it to see it fail**

Run: `cargo test -p sdmqtt-server --lib a_listener_s_own_durability`
Expected: compile error on `durability`.

- [x] **Step 3: Thread the override**

`ConnectionConfig` gains:

```rust
    /// Section 8.6's durability mode for publishes on this listener, or
    /// `None` for the node's. A listener carrying commands can be
    /// synchronous while one carrying telemetry is not.
    pub durability: Option<cluster::Durability>,
```

default `None`. In `shared.rs`, `confirming` takes the mode to use:

```rust
    fn confirming(
        &self,
        mode: Option<Durability>,
        promised: bool,
        streamed: &[Streamed],
    ) -> Option<(Vec<Confirming>, Duration)> {
        if !promised {
            return None;
        }
        let (installed, patience) = self.durability.get().copied()?;
        if !mode.unwrap_or(installed).waits() {
            return None;
        }
```

and the existing callers pass `None`. Add beside `with_broker_committed`:

```rust
    /// [`Self::with_broker_committed`] with the listener's durability
    /// mode, section 8.6's per-listener override. `None` is the node's.
    pub fn with_broker_committed_as<T>(
        &self,
        mode: Option<Durability>,
        f: impl FnOnce(&mut Broker) -> T,
        promised: impl FnOnce(&T) -> bool,
    ) -> (T, Committed) {
```

whose body is `with_broker_committed`'s with `mode` passed down to where
`confirming` is called (follow `hand_over`; the mode travels one level).
`with_broker_committed` becomes `self.with_broker_committed_as(None, f,
promised)`. The connection's publish commit near line 1091 calls the `_as`
form with `self.config.durability`.

The patience is the node's `cluster.sync_timeout_ms` in both cases: the
override picks the mode, not the timeout, because a second timeout would
be a second answer to "how long is too long" on one node.

- [x] **Step 4: Run the tests**

Run: `cargo test -p sdmqtt-server --lib`
Expected: all pass.

- [x] **Step 5: Prove it can fail**

Back up `shared.rs`; replace `mode.unwrap_or(installed)` with `installed`;
the new test fails on "did not wait". Restore with `cp` and `touch`.

- [x] **Step 6: Configuration, docs, spec, commit**

`Listener` gains `pub durability: Option<cluster::Durability>` (serde
already reads `"async"`/`"sync"` for the cluster key; reuse the type).
`Config::server` passes it through. `validate` refuses a listener set to
`"sync"` on a node that does not cluster, with the same wording the strict
flag uses for the same case, because a listener that waits for a follower
on a node that can never have one is a listener that counts every publish
unconfirmed. Environment `SDMQTT_LISTENER_DURABILITY`. A config test that
reads it and one that sees the refusal.

README under durability: "Per listener: `listener.durability = "sync"`
overrides the node's mode for publishes arriving on that listener; the
timeout is the node's." CHANGELOG. In the spec's 8.6 paragraph that begins
"**Durability modes.** Configurable globally. **The per-listener override
is deferred to milestone 7**", replace from "**The per-listener override"
to the end of that paragraph with: "and overridable per listener, since
milestone 7: `listener.durability` names the mode for publishes on that
listener and the node's `sync_timeout_ms` bounds the wait in both." Tick
the deferral in the milestone 6 plan's closing walk.

```bash
git add -A && git commit -m "feat(listener): a durability mode per listener"
```

---

### Task 3: Several listeners

**Files:**
- Modify: `crates/sdmqtt/src/config.rs` (`Listener` gains `name` and
  `kind`; `Config` gains `listeners: Vec<Listener>`; `server` becomes
  `servers`; `validate`)
- Modify: `crates/server/src/listener.rs` (`ServerConfig.kind`, `Running`
  unchanged)
- Modify: `crates/sdmqtt/src/main.rs` (bind each, report each, stop each)
- Modify: `crates/admin/src/resources.rs` (`ListenerProtocol` variants,
  `ListenerView` policy fields)
- Modify: `services/web/src/api.ts` if `ListenerView` is typed there
- Test: `crates/sdmqtt/src/config.rs` tests, `crates/sdmqtt/tests/listeners.rs`
  (new, three-process style is not needed: one process, two listeners)

**Interfaces:**
- Produces: `config::ListenerKind { Tcp, Tls, Ws, Wss }` (serde
  `lowercase`); `Listener { name: String, kind: ListenerKind, address,
  max_connections, mountpoint, max_qos, retain_available, durability, tls:
  Option<TlsSettings> }`; `Config::servers(&self) -> Vec<(String,
  ServerConfig)>` in configuration order, `[listener]` first under the name
  `"mqtt"`; `listener::ServerConfig { kind: Kind, .. }` where
  `listener::Kind` is `Tcp` for this task and gains variants in tasks 4
  and 5; admin `ListenerProtocol::{Mqtt, MqttTls, MqttWs, MqttWss, Admin,
  Ingest, Cluster}` and `ListenerView { max_qos: Option<u8>,
  retain_available: Option<bool>, durability: Option<&'static str>, .. }`.

- [x] **Step 1: Write the failing config tests**

```rust
    #[test]
    fn extra_listeners_are_read_in_order_after_the_default_one() {
        let config = parse_toml(
            r#"
[listener]
address = "127.0.0.1:1883"

[[listeners]]
name = "tenant-b"
address = "127.0.0.1:1884"
mountpoint = "tenant-b"
max_qos = 1
"#,
        );
        let servers = config.servers();
        let names: Vec<&str> = servers.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, ["mqtt", "tenant-b"]);
        assert_eq!(servers[1].1.address.port(), 1884);
        assert_eq!(&*servers[1].1.connection.mountpoint, "tenant-b");
        assert_eq!(servers[1].1.connection.max_qos, QoS::AtLeastOnce);
    }

    /// Review focus 4.
    #[test]
    fn two_listeners_on_one_address_are_refused_by_name() {
        let config = parse_toml(
            r#"
[listener]
address = "127.0.0.1:1883"

[[listeners]]
name = "again"
address = "127.0.0.1:1883"
"#,
        );
        let why = config.validate().expect_err("one address, two listeners");
        let text = why.to_string();
        assert!(text.contains("mqtt") && text.contains("again"), "{text}");
    }

    #[test]
    fn a_listener_without_a_name_or_with_a_taken_one_is_refused() {
        let unnamed = parse_toml("[[listeners]]\naddress = \"127.0.0.1:1884\"\n");
        assert!(unnamed.validate().is_err());
        let taken = parse_toml("[[listeners]]\nname = \"mqtt\"\naddress = \"127.0.0.1:1884\"\n");
        let why = taken.validate().expect_err("mqtt is the default listener's name");
        assert!(why.to_string().contains("mqtt"), "{why}");
    }

    #[test]
    fn an_unknown_listener_kind_is_refused() {
        let text = parse_toml_err("[[listeners]]\nname = \"x\"\nkind = \"udp\"\naddress = \"127.0.0.1:1884\"\n");
        assert!(text.contains("udp") || text.contains("kind"), "{text}");
    }
```

where `parse_toml_err` returns the parse error's text; add it beside
`parse_toml` if there is nothing like it.

- [x] **Step 2: Run them to see them fail**

Run: `cargo test -p sdmqtt --lib config::tests::extra_listeners config::tests::two_listeners config::tests::a_listener_without config::tests::an_unknown_listener_kind`
Expected: compile errors on `listeners`, `servers`, `kind`.

- [x] **Step 3: The configuration**

```rust
/// What a listener speaks under MQTT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListenerKind {
    /// Plain TCP. The default.
    #[default]
    Tcp,
    /// TLS over TCP. `tls` is required.
    Tls,
    /// MQTT over WebSocket, plain.
    Ws,
    /// MQTT over WebSocket over TLS. `tls` is required.
    Wss,
}
```

`Listener` gains `name: String` (empty for `[listener]`, which is named
`"mqtt"` by `servers`), `kind: ListenerKind`, and `tls: Option<TlsSettings>`
where `TlsSettings` is defined in task 4 — for this task declare it as

```rust
/// Section 12.1's certificate and key. Task 4 reads them.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSettings {
    pub certificate: PathBuf,
    pub key: PathBuf,
    #[serde(default = "default_reload_ms")]
    pub reload_ms: u64,
}
fn default_reload_ms() -> u64 { 60_000 }
```

`Config` gains `#[serde(default)] pub listeners: Vec<Listener>`.
`servers()` returns the default listener as `("mqtt", ..)` followed by each
extra in order. `validate`: every extra listener has a non-empty name,
names are distinct and none is `"mqtt"`, `"admin"`, `"cluster"` or
`"ingest"` (the names the admin resource already uses); no two listeners
share an address (compare the `SocketAddr`s; `0.0.0.0:1883` and
`127.0.0.1:1883` are different strings and are allowed, because the OS
allows them); a `tls` or `wss` listener has `tls`; a `tcp` or `ws` listener
with `tls` set is refused ("tls is set on a listener that does not use
it"). `ListenerKind` maps to `listener::Kind` in `servers`. Only
`[listener]` has environment variables; `[[listeners]]` is file-only, and
the README says so.

- [x] **Step 4: Bind each**

In `listener.rs`, `ServerConfig` gains `pub kind: Kind` with

```rust
/// What wraps an accepted socket before MQTT. Tasks 4 and 5 add variants.
#[derive(Debug, Clone, Default)]
pub enum Kind {
    #[default]
    Tcp,
}
```

In `main.rs`, replace the single `Server::bind_shared(config.server(),
shared)` with a loop over `config.servers()` binding each with the same
`shared`, collecting `Vec<(String, Running)>`; log each with its name and
kind; report each in `bound` with `ListenerView { name, protocol: match
kind { Tcp => Mqtt, .. }, max_qos: Some(..), retain_available: Some(..),
durability: Some("async"|"sync"|"node") }`, where the admin, ingest and
cluster rows carry `None` for the three; shut each down where `running` is
shut down today. `spawn_ticker` runs once per `bind_shared` today — it
must run **once per node**, not once per listener, or the broker ticks
twice a second with two listeners. Move it: `bind_shared` takes the ticker
out, and `main` (and `Server::bind`, for the tests that use it) spawn it
once. Add a unit test in `listener.rs` asserting that two `bind_shared`
calls on one `Shared` leave one ticker running — count via a metric the
tick already moves, or by adding `Shared::ticks() -> u64` if there is none.

- [x] **Step 5: The integration test**

`crates/sdmqtt/tests/listeners.rs`, modelled on `tests/cluster.rs`'s
`spawn` and `wait_until_listening` but one process:

```rust
//! Two listeners, one broker: a session subscribed on one is reached
//! from the other, and each listener's policy is its own.

#[test]
fn a_second_listener_shares_the_broker_and_keeps_its_own_policy() {
    // Configure [listener] on an ephemeral port and [[listeners]] "capped"
    // on another with max_qos = 0 and mountpoint "". Start the binary
    // with --config. Subscribe "rover" on the default listener; publish
    // on "capped" at QoS 0; assert the delivery arrives on the default
    // listener's socket. Then publish at QoS 1 on "capped" and assert the
    // DISCONNECT 0x9B. Then GET /api/v1/listeners and assert two rows
    // with protocol "mqtt", and "capped" reporting max_qos 0.
}
```

Write it in full with the `Client` helpers copied from `tests/cluster.rs`
(`resuming`, `subscribe`, `publish`, `delivered_within`); a copy is fine
here because the two test crates cannot share a module without a crate,
and a `tests/common` module is the right fix only when a third test wants
them. Ephemeral ports: bind `TcpListener` to port 0 and release, as
`cluster.rs` does.

- [x] **Step 6: Run everything, prove, commit**

Run: `cargo test -p sdmqtt --test listeners` then the whole workspace.
Prove: comment out the `servers()` loop's second iteration (bind only the
first); the integration test fails to connect to "capped". Restore.

README: a `[[listeners]]` example with two entries and one sentence per
key; "the default listener is `[listener]` and is named `mqtt`;
`[[listeners]]` adds more, file-only". CHANGELOG. `services/web/src/api.ts`:
if `ListenerView` is typed, add the three optional fields.

```bash
git add -A && git commit -m "feat(listener): several listeners on one broker"
```

---

### Task 4: TLS, with a certificate that renews without a restart

**Files:**
- Create: `crates/server/src/tls.rs`
- Modify: `crates/server/src/lib.rs` (`pub mod tls`), `crates/server/src/listener.rs`
  (`Kind::Tls(Arc<tls::Acceptor>)`, the accept loop), `crates/server/Cargo.toml`
  (`tokio-rustls` with `aws-lc-rs` and `tls12`, `rustls-pki-types` with
  `pem`, dev-dependency `rcgen`), `crates/server/src/metrics.rs`
- Modify: `crates/sdmqtt/src/config.rs` (`validate` loads the files),
  `crates/sdmqtt/src/main.rs` (build acceptors, spawn reloaders)
- Test: `crates/server/src/tls.rs` tests, `crates/server/tests/over_tls.rs`

**Interfaces:**
- Consumes: `listener::Kind`, `ServerConfig`.
- Produces: `tls::Settings { certificate: PathBuf, key: PathBuf, reload:
  Duration }`; `tls::Acceptor::load(&Settings) -> Result<Acceptor,
  tls::Error>` which reads and validates both files once;
  `Acceptor::accept<S>(&self, socket: S) -> impl Future<Output =
  Result<TlsStream<S>, io::Error>>`; `Acceptor::reloading(self: &Arc<Self>)
  -> impl Future<Output = ()>` (never returns; the poll loop);
  `Acceptor::reloaded() -> u64` and `Acceptor::reload_failed() -> u64`
  counters; `tls::Error::{Unreadable { path, why }, NotPem { path },
  NoCertificate { path }, NoKey { path }, Mismatch, Encrypted { path }}`.

- [x] **Step 1: Write the failing unit tests**

`crates/server/src/tls.rs` test module. Mint certificates with `rcgen`:

```rust
    /// A certificate and key on disk, minted for the test.
    fn minted(directory: &Path, name: &str) -> (PathBuf, PathBuf) {
        let cert = rcgen::generate_simple_self_signed(vec![name.to_owned()])
            .expect("rcgen mints a certificate");
        let certificate = directory.join(format!("{name}.pem"));
        let key = directory.join(format!("{name}.key"));
        std::fs::write(&certificate, cert.cert.pem()).expect("written");
        std::fs::write(&key, cert.signing_key.serialize_pem()).expect("written");
        (certificate, key)
    }

    #[test]
    fn a_certificate_and_key_load_and_a_mismatched_pair_is_refused() {
        let directory = tempfile::tempdir().expect("a directory");
        let (certificate, key) = minted(directory.path(), "one");
        let (_, other_key) = minted(directory.path(), "two");
        Acceptor::load(&Settings {
            certificate: certificate.clone(),
            key,
            reload: Duration::from_secs(60),
        })
        .expect("a matching pair loads");
        // Review focus 5.
        let refused = Acceptor::load(&Settings {
            certificate,
            key: other_key,
            reload: Duration::from_secs(60),
        })
        .expect_err("the key does not match the certificate");
        assert!(matches!(refused, Error::Mismatch), "{refused:?}");
    }

    #[test]
    fn an_empty_key_file_names_itself() {
        let directory = tempfile::tempdir().expect("a directory");
        let (certificate, key) = minted(directory.path(), "one");
        std::fs::write(&key, b"").expect("emptied");
        let refused = Acceptor::load(&Settings { certificate, key: key.clone(), reload: Duration::from_secs(60) })
            .expect_err("no key in an empty file");
        assert!(matches!(&refused, Error::NoKey { path } if *path == key), "{refused:?}");
    }

    /// Review focus 1: a renewal half-written keeps the old pair.
    #[tokio::test]
    async fn a_reload_swaps_a_good_pair_and_keeps_the_old_one_through_a_bad_write() {
        let directory = tempfile::tempdir().expect("a directory");
        let (certificate, key) = minted(directory.path(), "first");
        let acceptor = Arc::new(
            Acceptor::load(&Settings { certificate: certificate.clone(), key: key.clone(), reload: Duration::from_millis(50) })
                .expect("loads"),
        );
        let before = acceptor.certificate_der();
        let reloader = tokio::spawn({
            let acceptor = Arc::clone(&acceptor);
            async move { acceptor.reloading().await }
        });

        // A half-written renewal: the certificate is empty for a moment.
        std::fs::write(&certificate, b"").expect("emptied");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(acceptor.certificate_der(), before, "the old pair is kept");
        assert!(acceptor.reload_failed() >= 1);

        // Then the renewal lands.
        let (renewed_cert, renewed_key) = minted(directory.path(), "second");
        std::fs::copy(&renewed_key, &key).expect("key replaced");
        std::fs::copy(&renewed_cert, &certificate).expect("certificate replaced");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while acceptor.certificate_der() == before {
            assert!(std::time::Instant::now() < deadline, "the renewal was never picked up");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(acceptor.reloaded() >= 1);
        reloader.abort();
    }
```

`certificate_der()` returns the leaf's DER bytes, for tests and for the
admin resource later; add it to the interface.

- [x] **Step 2: Run them to see them fail**

Run: `cargo test -p sdmqtt-server --lib tls::`
Expected: compile error, no module.

- [x] **Step 3: Write `tls.rs`**

```rust
//! Section 12.1's TLS listener, and the certificate that renews without
//! a restart.
//!
//! # The resolver is the hot reload
//!
//! rustls asks a `ResolvesServerCert` for a certificate on every
//! handshake. Ours holds an `ArcSwap`-shaped `RwLock<Arc<CertifiedKey>>`
//! and a task re-reads the two files every `reload` and swaps the pair in
//! when both parse and match. A handshake in flight keeps the `Arc` it
//! was given; the next one gets the new pair. Nothing is reloaded on a
//! signal, because a renewal tool writes files and does not know what
//! signals this process takes.
//!
//! # A bad write keeps the old pair
//!
//! Renewal writes two files, usually by rename and sometimes not, and
//! there is a moment when one is new and the other old, or one is empty.
//! A pair that does not load, or does not match, is logged once per
//! change and the previous pair stays. Counted both ways:
//! `sdmqtt_tls_certificate_reloads_total{outcome="reloaded"|"failed"}`.

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

/// Where the certificate and key are, and how often to look again.
#[derive(Debug, Clone)]
pub struct Settings {
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub reload: Duration,
}

/// Why a pair of files is not a certificate and key.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{path} could not be read: {why}")]
    Unreadable { path: PathBuf, why: io::Error },
    #[error("{path} is not PEM")]
    NotPem { path: PathBuf },
    #[error("{path} holds no certificate")]
    NoCertificate { path: PathBuf },
    #[error("{path} holds no private key; an encrypted key is not supported, decrypt it first")]
    NoKey { path: PathBuf },
    #[error("the private key does not match the certificate's public key")]
    Mismatch,
    #[error("the key type is not one this build signs with: {why}")]
    Unsupported { why: String },
}

/// Reads the pair once. The `Mismatch` check: rustls's `CertifiedKey::keys_match`.
fn read_pair(settings: &Settings) -> Result<CertifiedKey, Error> {
    let certificates: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(&settings.certificate)
            .map_err(|why| pem_error(&settings.certificate, why))?
            .collect::<Result<_, _>>()
            .map_err(|why| pem_error(&settings.certificate, why))?;
    if certificates.is_empty() {
        return Err(Error::NoCertificate { path: settings.certificate.clone() });
    }
    let key = PrivateKeyDer::from_pem_file(&settings.key).map_err(|why| match why {
        rustls_pki_types::pem::Error::NoItemsFound => Error::NoKey { path: settings.key.clone() },
        other => pem_error(&settings.key, other),
    })?;
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let signing = provider
        .key_provider
        .load_private_key(key)
        .map_err(|why| Error::Unsupported { why: why.to_string() })?;
    let pair = CertifiedKey::new(certificates, signing);
    pair.keys_match().map_err(|_| Error::Mismatch)?;
    Ok(pair)
}

fn pem_error(path: &std::path::Path, why: rustls_pki_types::pem::Error) -> Error {
    match why {
        rustls_pki_types::pem::Error::Io(why) => Error::Unreadable { path: path.to_owned(), why },
        rustls_pki_types::pem::Error::NoItemsFound => Error::NoCertificate { path: path.to_owned() },
        _ => Error::NotPem { path: path.to_owned() },
    }
}

/// The pair as rustls sees it, swappable.
#[derive(Debug)]
struct Resolver {
    current: RwLock<Arc<CertifiedKey>>,
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.current.read().unwrap_or_else(std::sync::PoisonError::into_inner)))
    }
}

/// One listener's TLS: accepts handshakes, re-reads its files.
pub struct Acceptor {
    settings: Settings,
    resolver: Arc<Resolver>,
    acceptor: TlsAcceptor,
    reloaded: AtomicU64,
    reload_failed: AtomicU64,
    /// Whether the last attempt failed, so a failing file is logged on
    /// the change and not on every poll.
    failing: std::sync::atomic::AtomicBool,
}

impl Acceptor {
    pub fn load(settings: &Settings) -> Result<Self, Error> {
        let pair = read_pair(settings)?;
        let resolver = Arc::new(Resolver { current: RwLock::new(Arc::new(pair)) });
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("the provider supports the default versions")
        .with_no_client_auth()
        .with_cert_resolver(Arc::clone(&resolver) as Arc<dyn ResolvesServerCert>);
        Ok(Self {
            settings: settings.clone(),
            resolver,
            acceptor: TlsAcceptor::from(Arc::new(config)),
            reloaded: AtomicU64::new(0),
            reload_failed: AtomicU64::new(0),
            failing: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub async fn accept<S>(&self, socket: S) -> io::Result<TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.acceptor.accept(socket).await
    }

    /// Re-read the files every `reload`, for ever.
    pub async fn reloading(&self) {
        let mut last_seen: Option<(std::time::SystemTime, std::time::SystemTime)> = None;
        loop {
            tokio::time::sleep(self.settings.reload).await;
            let stamps = (
                std::fs::metadata(&self.settings.certificate).and_then(|m| m.modified()).ok(),
                std::fs::metadata(&self.settings.key).and_then(|m| m.modified()).ok(),
            );
            let stamps = match stamps {
                (Some(c), Some(k)) => (c, k),
                _ => {
                    self.note_failure("a certificate or key file is missing");
                    continue;
                }
            };
            if last_seen == Some(stamps) {
                continue;
            }
            last_seen = Some(stamps);
            match read_pair(&self.settings) {
                Ok(pair) => {
                    *self.resolver.current.write().unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Arc::new(pair);
                    self.reloaded.fetch_add(1, Ordering::Relaxed);
                    self.failing.store(false, Ordering::Relaxed);
                    tracing::info!(certificate = %self.settings.certificate.display(), "the TLS certificate was reloaded");
                }
                Err(why) => self.note_failure(&why.to_string()),
            }
        }
    }

    fn note_failure(&self, why: &str) {
        self.reload_failed.fetch_add(1, Ordering::Relaxed);
        if !self.failing.swap(true, Ordering::Relaxed) {
            tracing::warn!(certificate = %self.settings.certificate.display(), why, "the TLS certificate could not be reloaded; the previous one is still served");
        }
    }

    pub fn reloaded(&self) -> u64 { self.reloaded.load(Ordering::Relaxed) }
    pub fn reload_failed(&self) -> u64 { self.reload_failed.load(Ordering::Relaxed) }

    /// The leaf certificate being served, DER. For tests and the admin API.
    pub fn certificate_der(&self) -> Vec<u8> {
        self.resolver.current.read().unwrap_or_else(std::sync::PoisonError::into_inner).cert[0].to_vec()
    }
}
```

The reloader is driven by modification times, with the first poll
comparing against `None` so a file replaced between `load` and the first
poll is picked up. Check the exact `rustls` 0.23 API names
(`builder_with_provider`, `key_provider.load_private_key`, `keys_match`)
against `~/.cargo/registry` before trusting the above; the plan has been
wrong before (convention 3).

Add to `Cargo.toml` of the server crate: `tokio-rustls = { version =
"0.26", default-features = false, features = ["tls12", "aws-lc-rs"] }`,
`rustls = { version = "0.23", default-features = false, features = ["std",
"tls12", "aws-lc-rs"] }`, `rustls-pki-types = { version = "1", features =
["pem", "std"] }`, and `[dev-dependencies] rcgen = "0.13"`, `tempfile`. If
`rustls` was not previously a *direct* dependency anywhere, the budget
count rises by one for it as well; `scripts/budgets.sh` says what it
counts.

- [x] **Step 4: Wire the kind into the accept loop**

`listener::Kind::Tls(Arc<tls::Acceptor>)`. In `accept_loop`, after the
permit is taken, before `serve`:

```rust
            match &kind {
                Kind::Tcp => spawn_serving(socket, ..),
                Kind::Tls(acceptor) => {
                    let acceptor = Arc::clone(acceptor);
                    let metrics = Arc::clone(shared.metrics());
                    // spawn: handshake, then serve
                    tokio::spawn(async move {
                        let handshake = tokio::time::timeout(config.connect_timeout, acceptor.accept(socket)).await;
                        match handshake {
                            Ok(Ok(stream)) => { /* serve(stream, ..) as before */ }
                            Ok(Err(error)) => {
                                metrics.tls_handshake_failed().inc();
                                tracing::debug!(%peer, %error, "a TLS handshake failed");
                            }
                            Err(_) => {
                                metrics.tls_handshake_failed().inc();
                                tracing::debug!(%peer, "a TLS handshake did not finish within the connect timeout");
                            }
                        }
                        drop(permit);
                    });
                }
            }
```

Factor the "spawn, serve, drop the permit" into one function so the two
arms share it. The handshake is bounded by the same `connect_timeout` the
CONNECT packet is, because a peer that opens a socket and never completes
a handshake is the same peer that never sends CONNECT.

Metrics: `sdmqtt_tls_handshakes_failed_total` (counter) and
`sdmqtt_tls_certificate_reloads_total{outcome}` with `reloaded` and
`failed`, registered only when a TLS listener exists (absent, not zero,
otherwise — the cluster series' rule). The reload counters live on the
`Acceptor`; the metrics render reads them through a trait the way
`ClusterMetrics` does, or the acceptor increments metric counters it is
handed at `load` — choose the second; it is less machinery.

- [x] **Step 5: The over-TLS integration test**

`crates/server/tests/over_tls.rs`, modelled on `over_tcp.rs`:

```rust
//! A client speaks MQTT through a TLS handshake, and a renewed
//! certificate is what the next client sees.

#[tokio::test]
async fn a_client_connects_over_tls_and_sees_the_renewed_certificate_after_a_reload() {
    // minted() as in tls.rs tests; Acceptor::load; ServerConfig { kind: Kind::Tls(acceptor.clone()), ..ephemeral() };
    // Server::bind; spawn acceptor.reloading().
    // Client: tokio_rustls::TlsConnector with a RootCertStore holding the minted cert,
    // ServerName "localhost"; CONNECT/CONNACK over it; assert Success.
    // Read the peer certificate from the client's connection (`get_ref().1.peer_certificates()`),
    // assert it equals the minted DER.
    // Replace the files with a second minted pair; wait until acceptor.certificate_der() changes;
    // connect a second client trusting the second cert; assert its peer certificate is the second.
    // A third client still trusting only the first must fail the handshake: the old pair is gone.
}
```

Write it out in full. `rcgen` certificates carry `localhost` as a SAN
when minted with that name; the client verifies against it.

- [x] **Step 6: Run, prove, configure, commit**

Run: `cargo test -p sdmqtt-server`. Prove: in `reloading`, replace the swap
with a no-op; the unit reload test and the integration test fail. Restore.

Configuration: `validate` calls `tls::Acceptor::load` for every `tls` and
`wss` listener and turns the error into a startup refusal naming the
listener and the file ("listener \"secure\": {error}"); `main` builds the
acceptor once (not twice — `validate` loading and `main` loading again
would read the files twice, which is fine, but keep one code path: have
`validate` only call `read_pair` through a `tls::check(&Settings) ->
Result<(), Error>` and `main` call `load`). `main` spawns
`acceptor.reloading()` per TLS listener and aborts it at shutdown.
`ListenerProtocol::MqttTls` for the row.

README: a `[[listeners]]` example with `kind = "tls"`, `[listeners.tls]
certificate = ..., key = ..., reload_ms = 60000`, and the paragraph: the
files are re-read every `reload_ms` and swapped in when both parse and
match; a renewal that is half-written keeps the previous certificate and
logs once; an encrypted key is not supported; client certificates are not
asked for in this milestone. CHANGELOG. Regenerate
`THIRD-PARTY-NOTICES.txt` if `scripts/rust-notices.py` says the dependency
set changed (`rustls` and `rustls-pki-types` were already in the tree
through `hyper-rustls`, so it may not).

```bash
git add -A && git commit -m "feat(listener): TLS, with a certificate that renews without a restart"
```

---

### Task 5: WebSocket, plain and over TLS

**Files:**
- Create: `crates/server/src/websocket.rs`
- Modify: `crates/server/src/lib.rs`, `crates/server/src/listener.rs`
  (`Kind::Ws`, `Kind::Wss(Arc<tls::Acceptor>)`), `crates/server/Cargo.toml`
  (`tokio-tungstenite` without default features, with `handshake`),
  `crates/server/src/metrics.rs`, `crates/sdmqtt/src/main.rs`,
  `crates/admin/src/resources.rs`
- Test: `crates/server/src/websocket.rs` tests, `crates/server/tests/over_websocket.rs`

**Interfaces:**
- Produces: `websocket::upgrade<S>(socket: S, timeout: Duration) ->
  Result<Bytes<S>, Refused>` performing the server handshake and requiring
  the `mqtt` subprotocol; `websocket::Bytes<S>: AsyncRead + AsyncWrite`,
  which reads binary frames as a byte stream and writes each `poll_write`
  as one binary frame; `Refused::{NotAnUpgrade, NoMqttSubprotocol,
  Handshake(..)}`.

- [x] **Step 1: Write the failing unit tests**

In `websocket.rs`'s test module, over `tokio::io::duplex`, using
`tokio_tungstenite::client_async` with a request carrying
`Sec-WebSocket-Protocol: mqtt`:

```rust
    /// One MQTT packet in one binary frame, and the reply in one frame.
    #[tokio::test]
    async fn a_connect_in_a_binary_frame_is_served_and_answered_in_one() { .. }

    /// Review focus 2a: a text frame is not MQTT. RFC 6455 lets the
    /// server close with 1003 "unsupported data"; the MQTT-over-WebSocket
    /// clause (6.0) says MQTT data MUST be binary.
    #[tokio::test]
    async fn a_text_frame_closes_the_connection_with_unsupported_data() { .. }

    /// Review focus 2b: a packet split across two frames is one packet.
    #[tokio::test]
    async fn a_packet_split_across_frames_is_reassembled() { .. }

    /// Review focus 2c: a ping between frames is answered and does not
    /// reach MQTT.
    #[tokio::test]
    async fn a_ping_is_answered_and_not_seen_by_mqtt() { .. }

    /// Without the subprotocol the upgrade is refused with 400 and
    /// nothing is served.
    #[tokio::test]
    async fn an_upgrade_without_the_mqtt_subprotocol_is_refused() { .. }
```

Write each in full: drive the server side with
`serve(upgrade(server_half, ..).await?, shared, config)` in a spawned
task, the client side with tungstenite's async client over the other
half, and assert on decoded MQTT packets read back from binary messages.
For the split test send the CONNECT's bytes as two `Message::Binary`
frames of unequal length. For the text test send `Message::Text` and
assert the next message is `Message::Close(Some(CloseFrame { code:
CloseCode::Unsupported, .. }))`.

- [x] **Step 2: Run them to see them fail**

Run: `cargo test -p sdmqtt-server --lib websocket::`
Expected: compile error, no module.

- [x] **Step 3: Write `websocket.rs`**

```rust
//! MQTT over WebSocket, section 12.1.
//!
//! # A WebSocket is a byte stream with a frame around it
//!
//! The MQTT specification's WebSocket clause says MQTT control packets
//! are sent in binary frames, a frame may carry part of a packet or
//! several packets, and the subprotocol is `mqtt`. So the adapter here
//! turns a stream of binary messages into `AsyncRead` and each write into
//! one binary message, and `connection::serve` never learns there was a
//! frame. Text frames close the connection with 1003; pings are answered
//! by tungstenite underneath.

use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use futures_util::{Sink, Stream};  // check: futures-util may need adding; tungstenite re-exports what it uses
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response, ErrorResponse};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub const SUBPROTOCOL: &str = "mqtt";

#[derive(Debug, thiserror::Error)]
pub enum Refused {
    #[error("the request did not offer the mqtt subprotocol")]
    NoMqttSubprotocol,
    #[error("the upgrade did not complete within {0:?}")]
    TimedOut(Duration),
    #[error("the WebSocket handshake failed: {0}")]
    Handshake(#[from] tokio_tungstenite::tungstenite::Error),
}

/// Perform the server handshake, insisting on the `mqtt` subprotocol.
pub async fn upgrade<S>(socket: S, timeout: Duration) -> Result<Bytes<S>, Refused>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let offered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = std::sync::Arc::clone(&offered);
    let callback = move |request: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
        let offers_mqtt = request
            .headers()
            .get_all("sec-websocket-protocol")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|name| name.trim().eq_ignore_ascii_case(SUBPROTOCOL));
        if !offers_mqtt {
            let mut refused = ErrorResponse::new(Some("the mqtt subprotocol is required".to_owned()));
            *refused.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::BAD_REQUEST;
            return Err(refused);
        }
        seen.store(true, std::sync::atomic::Ordering::Relaxed);
        response.headers_mut().insert("sec-websocket-protocol", SUBPROTOCOL.parse().expect("ascii"));
        Ok(response)
    };
    let stream = tokio::time::timeout(timeout, tokio_tungstenite::accept_hdr_async(socket, callback))
        .await
        .map_err(|_| Refused::TimedOut(timeout))?
        .map_err(|error| {
            if offered.load(std::sync::atomic::Ordering::Relaxed) {
                Refused::Handshake(error)
            } else {
                Refused::NoMqttSubprotocol
            }
        })?;
    Ok(Bytes { inner: stream, unread: bytes::Bytes::new(), closing: false })
}

/// The frames as a byte stream.
pub struct Bytes<S> {
    inner: WebSocketStream<S>,
    /// What the last binary frame held that the reader has not taken.
    unread: bytes::Bytes,
    closing: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Bytes<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            if !self.unread.is_empty() {
                let take = self.unread.len().min(buf.remaining());
                buf.put_slice(&self.unread.split_to(take));
                return Poll::Ready(Ok(()));
            }
            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                None => return Poll::Ready(Ok(())),
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
                Some(Ok(Message::Binary(data))) => self.unread = data.into(),
                // Pings and pongs are tungstenite's; a close is the end.
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) => return Poll::Ready(Ok(())),
                Some(Ok(Message::Text(_))) => {
                    // The MQTT WebSocket clause: binary only. Close with 1003.
                    self.closing = true;
                    let _ = Pin::new(&mut self.inner).start_send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Unsupported,
                        reason: "MQTT over WebSocket is binary".into(),
                    })));
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "a text frame on an MQTT WebSocket")));
                }
                Some(Ok(Message::Frame(_))) => continue,
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Bytes<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        ready!(Pin::new(&mut self.inner).poll_ready(cx)).map_err(io::Error::other)?;
        Pin::new(&mut self.inner)
            .start_send(Message::Binary(bytes::Bytes::copy_from_slice(data)))
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(data.len()))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx).map_err(io::Error::other)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx).map_err(io::Error::other)
    }
}
```

Check `tokio-tungstenite`'s current API (`accept_hdr_async`, the `Message`
variants, whether `Binary` carries `Bytes` or `Vec<u8>`) against the
version cargo resolves; the names above are from 0.26 and may have moved.
`futures-util` is a dependency of tungstenite already; if `Stream`/`Sink`
need it as a direct dependency, that is one more against the budget — use
`tokio_tungstenite::tungstenite`'s re-exports first, and only then add
`futures-util` with default features off.

The connection's own write batching means one `poll_write` is often
several packets; that is allowed by the clause ("a frame may contain
multiple MQTT control packets") and is cheaper than one frame per packet.

- [x] **Step 4: Wire the two kinds**

`Kind::Ws` and `Kind::Wss(Arc<tls::Acceptor>)`. The accept loop: `Ws`
upgrades then serves; `Wss` handshakes, upgrades, serves — both bounded
by `connect_timeout` together, not each. A refused upgrade is counted on
`sdmqtt_websocket_upgrades_refused_total{reason}` with `no_subprotocol`,
`handshake` and `timed_out`, registered only when a WebSocket listener
exists. `ListenerProtocol::MqttWs` and `MqttWss`.

- [x] **Step 5: The integration test and the proof**

`crates/server/tests/over_websocket.rs`: bind a `Kind::Ws` server on an
ephemeral port, connect with `tokio_tungstenite::connect_async` to
`ws://127.0.0.1:{port}/` with the subprotocol header, CONNECT/CONNACK,
SUBSCRIBE/SUBACK, a PUBLISH that comes back as a binary frame; then a
`Kind::Wss` server the same way through a `tokio_rustls` connector
underneath the WebSocket client (`client_async` over a `TlsStream`).

Prove: delete the subprotocol check; the "without the subprotocol" tests
fail. Replace `self.unread = data.into()` with dropping the data; the
reassembly test and the integration test fail. Restore.

- [x] **Step 6: Configuration, docs, commit**

`kind = "ws"` and `"wss"` are already parsed by task 3; `main` builds the
kinds. README: a `[[listeners]]` example for each, the sentence that the
`mqtt` subprotocol is required and text frames are refused, and that the
path is ignored (every path upgrades, because a client library's default
path varies and a broker that refused `/mqtt` would be the broker that is
wrong). CHANGELOG. Third-party notices regenerated for `tokio-tungstenite`
and `tungstenite` (MIT/Apache; `deny.toml` already allows both).

```bash
git add -A && git commit -m "feat(listener): MQTT over WebSocket, plain and over TLS"
```

---

### Task 6: The conformance suite over TLS and WebSocket

**Files:**
- Create: `conformance/via.py`
- Modify: `conformance/run.sh`, `conformance/known-failures.txt`,
  `.github/workflows/ci.yml`

**Interfaces:**
- Produces: `via.py tls <listen-port> <broker-host:port> <ca.pem>` and
  `via.py ws <listen-port> <broker-host:port>`, a stdlib-only forwarder the
  suite's plain-TCP client connects through; runner labels `v5-tls`,
  `v311-tls`, `v5-ws`, `v311-ws` beside `v5` and `v311`.

- [x] **Step 1: The forwarder**

`conformance/via.py`, Python 3 standard library only (`socket`, `ssl`,
`threading`, `base64`, `os`, `struct`): listens on a plain TCP port, and
for each accepted client opens a connection to the broker — for `tls`,
wrapped in `ssl.create_default_context(cafile=ca)`, server hostname
`localhost`; for `ws`, performing an RFC 6455 client handshake with
`Sec-WebSocket-Protocol: mqtt` and then carrying bytes client→broker as
masked binary frames and broker→client by unwrapping binary frames,
answering pings with pongs and treating a close frame as end of stream.
Two threads per connection, one per direction. Fragmented frames are
reassembled (the broker sends none, but the forwarder must not assume
it). Write it in full, about 150 lines; test it by hand against the
broker from task 5 before wiring the runner.

- [x] **Step 2: The runner**

`run.sh` mints a certificate for the run if `openssl` is on the path
(`openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes
-keyout "$work/key.pem" -out "$work/cert.pem" -subj /CN=localhost -days 2
-addext subjectAltName=DNS:localhost`) and skips the TLS pass with a
printed reason if not — a skipped pass is printed, never silent. The
broker is started once per test as today, with a config file written into
`$work` declaring `[listener]` on `$PORT+1` (plain, unused by the via
passes), a `tls` listener on `$PORT+2` and a `ws` listener on `$PORT+3`;
`via.py` listens on `$PORT` for the pass in progress. The six passes run
in a loop over `"v5 client_test5.py plain" "v311 client_test.py plain"
"v5-tls client_test5.py tls" ...`. The baseline comparison keys on the
label, so `known-failures.txt` gains the two deferred lines per new label
(`v5-tls:test_subscribe_failure deferred ...` and so on), and nothing
else — the point of the task is that nothing else is needed.

- [x] **Step 3: Run it, and CI**

Run: `services/broker/conformance/run.sh` locally; expected: six passes,
the same failures per label as the plain pass has, and the runner green.
If a via pass fails a test the plain pass does not, that is a defect in
task 4 or 5 or in the forwarder, and it is found here by design: fix it
there, do not classify it. The CI job needs no change beyond `openssl`
being present on `ubuntu-latest` (it is).

```bash
git add -A && git commit -m "test: the conformance suite over TLS and WebSocket"
```

---

### Task 7: Closing walk, budgets, docs

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, the spec (12.1, 5.4, 11.1), this plan

- [x] **Step 1: Budgets and the full gate**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo deny --config ../../deny.toml check
(cd ../.. && scripts/budgets.sh)
```

All green; note the binary size and dependency count in the CHANGELOG
entry if either moved by more than a megabyte or a dependency.

- [x] **Step 2: The walk (convention 8)**

Section 12.1's listener sentence, 5.4's maximum-QoS paragraph, 8.6's
per-listener paragraph and 11.1's listeners resource, each sentence named
to the task that implements it, written under a "Closing walk" heading at
the end of this plan. Say out loud what is deferred: client certificates
(not in the spec; say so), the certificate's not-after as a metric (would
need an X.509 parser; a dependency this plan declined), per-listener
`max_connections` was already there.

- [x] **Step 3: Docs and commit**

Spec 5.4: replace "**Maximum QoS and Retain Availability move to milestone
7.**" paragraph's content with the shipped behaviour in two sentences.
Spec 12.1: one sentence after the listener sentence saying the four kinds,
the subprotocol, and that reload is a poll of `reload_ms`. README's table
of contents if it has one. CHANGELOG's milestone 7 section reads as a whole.

```bash
git add -A && git commit -m "docs: milestone 7's listeners, walked against the spec"
```

---

## Closing walk

Section 12.1's listener sentence, 5.4's maximum-QoS paragraph, 8.6's
per-listener paragraph and 11.1's listeners resource, read against the
code at the close (convention 8):

- **"TLS and WebSocket listeners are supported"**: tasks 4 and 5,
  `kind = "tls" | "ws" | "wss"`, each a layer in front of the same
  `serve`. Checked by the Paho suite through `via.py` in task 6, where
  the failures under each layer are exactly the plain pass's.
- **"certificate hot reload through a certificate resolver so renewal
  needs no restart"**: task 4, `tls::Acceptor::reloading`, a poll of two
  files' modification times; a half-written renewal keeps the old pair.
- **5.4, "Maximum QoS and Retain Availability move to milestone 7"**:
  task 1, `listener.max_qos` and `listener.retain_available`, advertised
  and enforced, with the 3.1.1 refusal a closed socket as section 13
  wants. The spec paragraph now says what shipped.
- **8.6, the per-listener durability override**: task 2,
  `listener.durability`, the node's timeout in both modes; the milestone
  6 walk's deferral is closed.
- **11.1's listeners resource**: task 3, one row per listener with its
  protocol and policy.
- **16's exit criterion, "the full conformance suite green including TLS
  and WebSocket"**: task 6, six passes, one baseline keyed by label.

Deferred, said out loud:

- **Client certificates.** Not in the spec's sentence; a listener that
  asked for one needs a trust store and a meaning for the name in it,
  which is authorization work, and milestone 7's second plan is where
  authorization lives.
- **The certificate's not-after as a metric.** Would need an X.509
  parser this build does not otherwise carry; the budget was spent on
  `tokio-tungstenite` and `futures-util` instead (29 of 35).
- **One frame per packet on WebSocket.** The clause allows several per
  frame and this adapter sends a burst as one; a client that wanted
  otherwise was promised nothing by the standard.

Found on the way, each by a test refusing to pass:

- A WebSocket frame sat queued until the next write, because
  tungstenite writes on flush and the connection shell flushes only at
  the end of a burst (task 5). The adapter now flushes inside the write.
- The forwarder read and wrote an SSL socket from two threads, which
  OpenSSL does not allow, and dropped the broker's DISCONNECT on close
  (task 6). Both found by the suite and fixed in the forwarder.
- Two listeners would have ticked the broker twice a second (task 3).
  The first listener bound drives the clock.
