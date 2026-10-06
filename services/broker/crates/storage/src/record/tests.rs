//! Tests for the on-disk encoding.
//!
//! Two layers, and only the first is a guard against the failure this
//! module exists to prevent.

use super::*;

/// Golden byte vectors, hand-derived from postcard's format rules.
///
/// Every array below was written by hand from the rules listed in the
/// parent module's documentation, which were themselves read out of
/// postcard 1.1.3's serializer rather than remembered. None was captured
/// from this crate's output. A captured vector agrees with the encoder by
/// construction and cannot see the encoder change, which is the whole
/// hazard: an encoding that shifts between releases produces a broker that
/// starts cleanly and misreads everything it stored before.
///
/// **Independence property for this vector set.** A golden vector only
/// catches a transposition of two fields if those fields encode to
/// different bytes in that vector. So within each record, no two fields of
/// the same type carry the same value:
///
/// - `SessionKey`: `"tenant-a"` against `"sensor"`, different lengths.
/// - `SessionRecord`: `expires_at` is `Some(1_700_000_000)` and `will_at`
///   is `Some(1_700_000_030)`, both present and different, so swapping
///   them is visible. Making one `None` would have hidden a swap behind a
///   presence byte.
/// - `WillRecord`: `qos` is `ExactlyOnce` (`0x02`) rather than
///   `AtLeastOnce`, so it differs from the adjacent `retain: true`
///   (`0x01`). They encode to one byte each and sit next to each other.
/// - `SubscriptionRecord`: `no_local` is true and `retain_as_published` is
///   false, the only two booleans.
/// - `DeliveryRecord`: `retain` and `dup` are opposites in both vectors
///   that carry one.
/// - `BodyRecord` and `RetainedRecord`: the two `Bytes` fields hold
///   different lengths, and the two `Option`s differ in presence.
/// - `RuleRecord`: `id` and `name` are different lengths, and `name` and
///   the trigger's `filter` are the same length and different text, so a
///   swap of either pair is visible. `enabled` is false, which is the
///   one byte in the record that is `0x00`, so it cannot be confused
///   with the two `Option` presence bytes beside it, both `0x01`.
/// - `HttpOutRecord`: `timeout_ms` and `backoff_ms` are different
///   two-byte varints; `max_concurrency` and `retries` are different
///   one-byte ones and adjacent, which is where a transposition would
///   otherwise hide. `method` is `PUT` rather than the `POST` a default
///   would produce, so the vector pins what was stored and not what a
///   default would have supplied.
///
/// - `OutboxRecord`: `id` and `body` are both sixteen raw bytes and are
///   filled with different ones, so a swap is visible; `attempts` is a
///   one-byte varint and `enqueued_at` and `next_attempt_at` are
///   five-byte ones that differ in their first byte; and the rule
///   identifier outside the record is a different length from the
///   client identifier inside it.
///
/// When adding a record type, check the same property before adding its
/// vector.
mod golden {
    use super::*;

    /// Assert both directions against bytes that neither direction wrote.
    ///
    /// Encoding alone would not notice a decoder that reads a field it
    /// does not write; decoding alone would not notice the reverse.
    #[track_caller]
    fn pins(record: &Record, expected: &[u8]) {
        assert_eq!(
            record.encode(),
            expected,
            "encoded bytes differ from the hand-derived vector"
        );
        assert_eq!(
            &Record::decode(expected).expect("the vector must decode"),
            record,
            "the hand-derived vector decoded to a different record"
        );
    }

    /// The session key that appears in every per-session vector.
    ///
    /// postcard writes a `String` as a `usize` varint length then the UTF-8
    /// bytes, and a struct as its fields in order with no framing.
    const KEY_BYTES: &[u8] = &[
        0x08, b't', b'e', b'n', b'a', b'n', b't', b'-', b'a', // mountpoint "tenant-a"
        0x06, b's', b'e', b'n', b's', b'o', b'r', // client_id "sensor"
    ];

    fn key() -> SessionKey {
        SessionKey::new("tenant-a", "sensor")
    }

    /// Build a vector from the discriminant, the key, and the rest.
    ///
    /// The key's bytes are shared because they are the same sixteen bytes
    /// in every vector and repeating them thirteen times would invite a
    /// copy-paste error rather than prevent one. The discriminant and every
    /// record-specific byte are written out in full at each use.
    fn keyed(discriminant: u8, tail: &[u8]) -> Vec<u8> {
        let mut out = vec![discriminant];
        out.extend_from_slice(KEY_BYTES);
        out.extend_from_slice(tail);
        out
    }

    #[test]
    fn the_format_marker() {
        // A fixed-size array is its elements with no length prefix, and a
        // u16 is a varint, so version 8 is one byte. The version moved
        // from 1 when the reference count of a body left the body's own
        // row, from 2 when rules were added, from 3 when each rule
        // gained an outbox — a version 3 store has no outbox table at
        // all and would come up looking healthy with every undelivered
        // envelope missing — from 4 when the node's own identity
        // was stored, because a store from before it holds none and the
        // node reading it would mint a second UUID for itself, and from
        // 5 when the clock's high-water mark was stored, because a
        // store from before it holds none and the node reading it would
        // resume its clock from zero and re-issue stamps it has already
        // used, and from 6 when the replicated registers themselves were
        // stored, because a store from before it holds none and the node
        // reading it would come up routing to nobody and then resurrect
        // everything it had ever deleted under fresh stamps, and from 7
        // when the session epoch joined the owner in a session-owner
        // register — the first version that changed the shape of
        // something already stored rather than adding a variant at the
        // end, so a version 7 register is sixteen bytes where this
        // build expects sixteen and a varint and a reader would run off
        // the end of one record into the next.
        let expected: &[u8] = &[
            b's', b'd', b'm', b'q', b't', b't', b's', b't', // magic "sdmqttst"
            0x08, // version 8
        ];
        assert_eq!(FormatMarker::current().encode(), expected);
        assert_eq!(
            FormatMarker::read(expected).expect("our own marker must read"),
            FormatMarker::current()
        );
    }

    #[test]
    fn session_put() {
        let record = Record::SessionPut(SessionRecord {
            key: key(),
            version: ProtocolVersion::V5,
            expiry_interval: 3600,
            expires_at: Some(1_700_000_000),
            will: Some(WillRecord {
                topic: "gone".to_owned(),
                payload: Bytes::from_static(b"bye"),
                qos: Qos::ExactlyOnce,
                retain: true,
                delay: 30,
                properties: Bytes::from_static(&[0x01, 0x00]),
            }),
            will_at: Some(1_700_000_030),
        });
        // 3600 = 28 * 128 + 16, so the varint is 16|0x80 then 28.
        // 1_700_000_000 in base 128, least significant group first, is
        // 0, 98, 79, 42, 6: 98*128 + 79*128^2 + 42*128^3 + 6*128^4.
        // 1_700_000_030 differs only in the first group, 30 for 0.
        pins(
            &record,
            &keyed(
                0x00, // Record::SessionPut is variant 0
                &[
                    0x01, // ProtocolVersion::V5 is variant 1
                    0x90, 0x1c, // expiry_interval 3600
                    0x01, 0x80, 0xe2, 0xcf, 0xaa, 0x06, // expires_at Some(1_700_000_000)
                    0x01, // will is Some
                    0x04, b'g', b'o', b'n', b'e', // will topic "gone"
                    0x03, b'b', b'y', b'e', // will payload b"bye"
                    0x02, // Qos::ExactlyOnce is variant 2
                    0x01, // retain true
                    0x1e, // delay 30
                    0x02, 0x01, 0x00, // properties, two opaque bytes
                    0x01, 0x9e, 0xe2, 0xcf, 0xaa, 0x06, // will_at Some(1_700_000_030)
                ],
            ),
        );
    }

    #[test]
    fn session_removed() {
        pins(
            &Record::SessionRemoved { session: key() },
            &keyed(0x01, &[]), // variant 1, then the key and nothing else
        );
    }

    #[test]
    fn subscription_added() {
        let record = Record::SubscriptionAdded {
            session: key(),
            subscription: SubscriptionRecord {
                filter: "sensors/+/temp".to_owned(),
                qos: Qos::ExactlyOnce,
                no_local: true,
                retain_as_published: false,
                retain_handling: RetainHandling::SendIfNew,
                subscription_id: Some(7),
            },
        };
        pins(
            &record,
            &keyed(
                0x02, // Record::SubscriptionAdded is variant 2
                &[
                    0x0e, b's', b'e', b'n', b's', b'o', b'r', b's', b'/', b'+', b'/', b't', b'e',
                    b'm', b'p', // filter, fourteen bytes
                    0x02, // Qos::ExactlyOnce
                    0x01, // no_local true
                    0x00, // retain_as_published false
                    0x01, // RetainHandling::SendIfNew is variant 1
                    0x01, 0x07, // subscription_id Some(7)
                ],
            ),
        );
    }

    #[test]
    fn subscription_removed() {
        pins(
            &Record::SubscriptionRemoved {
                session: key(),
                filter: "sensors/+/temp".to_owned(),
            },
            &keyed(
                0x03, // Record::SubscriptionRemoved is variant 3
                &[
                    0x0e, b's', b'e', b'n', b's', b'o', b'r', b's', b'/', b'+', b'/', b't', b'e',
                    b'm', b'p',
                ],
            ),
        );
    }

    #[test]
    fn inbound_reserved() {
        // 300 = 2 * 128 + 44.
        pins(
            &Record::InboundReserved {
                session: key(),
                packet_id: 300,
            },
            &keyed(0x04, &[0xac, 0x02]),
        );
    }

    #[test]
    fn inbound_released() {
        // 4095 = 31 * 128 + 127.
        pins(
            &Record::InboundReleased {
                session: key(),
                packet_id: 4095,
            },
            &keyed(0x05, &[0xff, 0x1f]),
        );
    }

    /// The sixteen opaque bytes of a body identifier, ascending so that a
    /// reversal or a length prefix would show.
    const BODY_ID: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];

    #[test]
    fn inflight_put() {
        let record = Record::InflightPut {
            session: key(),
            entry: InflightRecord {
                packet_id: 42,
                sequence: 9,
                state: InflightState::AwaitingComplete,
                sent_at: 1000,
                delivery: DeliveryRecord {
                    body: MessageId(BODY_ID),
                    qos: Qos::AtLeastOnce,
                    retain: false,
                    subscription_ids: vec![1, 200],
                    dup: true,
                },
            },
        };
        // 1000 = 7 * 128 + 104; 200 = 1 * 128 + 72.
        pins(
            &record,
            &keyed(
                0x06, // Record::InflightPut is variant 6
                &[
                    0x2a, // packet_id 42
                    0x09, // sequence 9
                    0x02, // InflightState::AwaitingComplete is variant 2
                    0xe8, 0x07, // sent_at 1000
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                    0x0d, 0x0e, 0x0f, // body identifier, no length prefix
                    0x01, // Qos::AtLeastOnce
                    0x00, // retain false
                    0x02, 0x01, 0xc8, 0x01, // subscription_ids [1, 200]
                    0x01, // dup true
                ],
            ),
        );
    }

    #[test]
    fn inflight_removed() {
        pins(
            &Record::InflightRemoved {
                session: key(),
                packet_id: 42,
            },
            &keyed(0x07, &[0x2a]),
        );
    }

    #[test]
    fn queue_pushed() {
        let record = Record::QueuePushed {
            session: key(),
            entry: QueueEntry {
                sequence: 130,
                packet_id: None,
                delivery: DeliveryRecord {
                    body: MessageId(BODY_ID),
                    qos: Qos::AtMostOnce,
                    retain: true,
                    subscription_ids: Vec::new(),
                    dup: false,
                },
            },
        };
        // 130 = 1 * 128 + 2, the smallest value needing two varint bytes
        // past a byte boundary.
        pins(
            &record,
            &keyed(
                0x08, // Record::QueuePushed is variant 8
                &[
                    0x82, 0x01, // sequence 130
                    0x00, // packet_id None
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                    0x0d, 0x0e, 0x0f, // body identifier
                    0x00, // Qos::AtMostOnce
                    0x01, // retain true
                    0x00, // subscription_ids, empty
                    0x00, // dup false
                ],
            ),
        );
    }

    #[test]
    fn queue_popped() {
        pins(
            &Record::QueuePopped {
                session: key(),
                sequence: 130,
            },
            &keyed(0x09, &[0x82, 0x01]),
        );
    }

    #[test]
    fn body_put() {
        let record = Record::BodyPut {
            id: MessageId(BODY_ID),
            body: BodyRecord {
                topic: "a/b".to_owned(),
                payload: Bytes::from_static(b"21.5"),
                qos: Qos::AtLeastOnce,
                retain: false,
                expires_at: Some(60),
                properties: Bytes::new(),
            },
        };
        // No session key on this one: a body belongs to no session.
        let expected: &[u8] = &[
            0x0a, // Record::BodyPut is variant 10
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, // identifier
            0x03, b'a', b'/', b'b', // topic "a/b"
            0x04, b'2', b'1', b'.', b'5', // payload b"21.5"
            0x01, // Qos::AtLeastOnce
            0x00, // retain false
            0x01, 0x3c, // expires_at Some(60)
            0x00, // properties, empty
        ];
        pins(&record, expected);
    }

    #[test]
    fn body_released() {
        let expected: &[u8] = &[
            0x0b, // Record::BodyReleased is variant 11
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        pins(
            &Record::BodyReleased {
                id: MessageId(BODY_ID),
            },
            expected,
        );
    }

    #[test]
    fn retained_put() {
        let record = Record::RetainedPut {
            topic: "a/b".to_owned(),
            message: RetainedRecord {
                payload: Bytes::from_static(b"on"),
                qos: Qos::ExactlyOnce,
                expires_at: None,
                properties: Bytes::from_static(&[0x01, 0x01]),
            },
        };
        let expected: &[u8] = &[
            0x0c, // Record::RetainedPut is variant 12
            0x03, b'a', b'/', b'b', // topic "a/b"
            0x02, b'o', b'n', // payload b"on"
            0x02, // Qos::ExactlyOnce
            0x00, // expires_at None
            0x02, 0x01, 0x01, // properties, two opaque bytes
        ];
        pins(&record, expected);
    }

    /// The rule the vectors below are of.
    ///
    /// Written out here once because both of them name it, and because
    /// what the bytes say it is has to be legible beside the bytes.
    fn rule() -> RuleRecord {
        RuleRecord {
            id: "telemetry".to_owned(),
            name: "Telemetry out".to_owned(),
            enabled: false,
            trigger: TriggerRecord {
                filter: "sensor/+/temp".to_owned(),
                min_qos: Some(Qos::ExactlyOnce),
                client_id: Some("sensor-*".to_owned()),
            },
            action: ActionRecord::HttpOut(HttpOutRecord {
                url: "https://example.invalid/hook".to_owned(),
                method: "PUT".to_owned(),
                headers: vec![("x-token".to_owned(), "secret".to_owned())],
                timeout_ms: 2_500,
                max_concurrency: 8,
                retries: 5,
                backoff_ms: 750,
            }),
        }
    }

    #[test]
    fn rule_put() {
        // A tuple is its elements in order with no framing, so a header
        // is two lengthed strings and a `Vec` of them is a varint count
        // then that. 2500 = 19 * 128 + 68, so the varint is 68|0x80 then
        // 19; 750 = 5 * 128 + 110, so it is 110|0x80 then 5.
        let expected: &[u8] = &[
            0x0e, // Record::RulePut is variant 14
            0x09, b't', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y', // id
            0x0d, b'T', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y', b' ', b'o', b'u',
            b't', // name "Telemetry out"
            0x00, // enabled false
            0x0d, b's', b'e', b'n', b's', b'o', b'r', b'/', b'+', b'/', b't', b'e', b'm',
            b'p', // trigger filter "sensor/+/temp"
            0x01, 0x02, // min_qos Some(Qos::ExactlyOnce)
            0x01, 0x08, b's', b'e', b'n', b's', b'o', b'r', b'-',
            b'*', // client_id Some("sensor-*")
            0x00, // ActionRecord::HttpOut is variant 0
            0x1c, b'h', b't', b't', b'p', b's', b':', b'/', b'/', b'e', b'x', b'a', b'm', b'p',
            b'l', b'e', b'.', b'i', b'n', b'v', b'a', b'l', b'i', b'd', b'/', b'h', b'o', b'o',
            b'k', // url, twenty-eight bytes
            0x03, b'P', b'U', b'T', // method
            0x01, // one header
            0x07, b'x', b'-', b't', b'o', b'k', b'e', b'n', // its name
            0x06, b's', b'e', b'c', b'r', b'e', b't', // its value
            0xc4, 0x13, // timeout_ms 2500
            0x08, // max_concurrency 8
            0x05, // retries 5
            0xee, 0x05, // backoff_ms 750
        ];
        pins(&Record::RulePut { rule: rule() }, expected);
    }

    /// The ingest rule the vector below is of.
    ///
    /// Section 9.2's `http_in`. Its trigger is the empty one an ingest
    /// rule stores — see [`RuleRecord::trigger`] — so three zero bytes
    /// sit together in the middle of it. A transposition among them is
    /// invisible here and is pinned by [`rule_put`] instead, where the
    /// three fields carry values that differ; what this vector is for
    /// is the action.
    fn ingest_rule() -> RuleRecord {
        RuleRecord {
            id: "gateway".to_owned(),
            name: "Gateway in".to_owned(),
            enabled: true,
            trigger: TriggerRecord {
                filter: String::new(),
                min_qos: None,
                client_id: None,
            },
            action: ActionRecord::HttpIn(HttpInRecord {
                token: Secret::new("a-secret"),
                topic: "from/the/web".to_owned(),
                topic_from: Some(TopicFromRequestRecord {
                    source: TopicFromRecord::Query("t".to_owned()),
                    prefix: "ingest/gateway".to_owned(),
                }),
                qos: Qos::AtLeastOnce,
                retain: false,
            }),
        }
    }

    /// The independence property, for this vector: every string in it
    /// is a different length — 7, 10, 8, 12, 1, 14 — so a transposition
    /// of any pair changes the bytes; the query parameter's name is one
    /// character and the variant index beside it is `0x01`, which are
    /// the same byte, so the name is `t` rather than a character that
    /// would make the pair look interchangeable; and `qos` and `retain`
    /// are the two adjacent one-byte fields and differ.
    #[test]
    fn rule_put_http_in() {
        let expected: &[u8] = &[
            0x0e, // Record::RulePut is variant 14
            0x07, b'g', b'a', b't', b'e', b'w', b'a', b'y', // id
            0x0a, b'G', b'a', b't', b'e', b'w', b'a', b'y', b' ', b'i',
            b'n', // name "Gateway in"
            0x01, // enabled true
            0x00, // trigger filter "", the empty trigger of an ingest rule
            0x00, // min_qos None
            0x00, // client_id None
            0x01, // ActionRecord::HttpIn is variant 1
            // `Secret` is transparent, so the token is the string and
            // nothing around it.
            0x08, b'a', b'-', b's', b'e', b'c', b'r', b'e', b't', // token
            0x0c, b'f', b'r', b'o', b'm', b'/', b't', b'h', b'e', b'/', b'w', b'e',
            b'b', // topic "from/the/web"
            0x01, // topic_from Some
            0x01, // TopicFromRecord::Query is variant 1
            0x01, b't', // the query parameter's name
            0x0e, b'i', b'n', b'g', b'e', b's', b't', b'/', b'g', b'a', b't', b'e', b'w', b'a',
            b'y', // prefix "ingest/gateway"
            0x01, // qos AtLeastOnce
            0x00, // retain false
        ];
        pins(
            &Record::RulePut {
                rule: ingest_rule(),
            },
            expected,
        );
    }

    /// The polling rule the vector below is of.
    ///
    /// Section 9.2's `http_poll`. Like an ingest rule it stores the
    /// empty trigger — a poller is fired by its own interval, not by a
    /// published message — so the three zero bytes sit together again
    /// and are pinned by [`rule_put`] rather than here. What this
    /// vector is for is the action, and in particular its **three
    /// adjacent varint fields**: an interval, a timeout and a body
    /// limit, which is the transposition a round trip cannot see.
    fn polling_rule() -> RuleRecord {
        RuleRecord {
            id: "weather".to_owned(),
            name: "Weather poll".to_owned(),
            enabled: true,
            trigger: TriggerRecord {
                filter: String::new(),
                min_qos: None,
                client_id: None,
            },
            action: ActionRecord::HttpPoll(HttpPollRecord {
                url: "https://example.invalid/now".to_owned(),
                headers: vec![("accept".to_owned(), "text/plain".to_owned())],
                interval_ms: 60_000,
                timeout_ms: 5_000,
                max_body_bytes: 65_536,
                topic: "weather/now".to_owned(),
                qos: Qos::ExactlyOnce,
                retain: true,
            }),
        }
    }

    /// The independence property, for this vector: every string in it
    /// is a different length — 7, 12, 27, 6, 10, 11 — so a
    /// transposition of any pair changes the bytes; the three varints
    /// encode to three, two and three bytes and share no byte value in
    /// the same position, so swapping any two of them changes the
    /// vector; and `qos` and `retain` are the two adjacent one-byte
    /// fields and differ.
    #[test]
    fn rule_put_http_poll() {
        // 60000 = 3 * 16384 + 84 * 128 + 96, so the varint is 96|0x80,
        // 84|0x80, 3. 5000 = 39 * 128 + 8, so it is 8|0x80 then 39.
        // 65536 = 4 * 16384, so it is 0|0x80, 0|0x80, 4 — the one that
        // shows a varint's trailing groups are not dropped.
        let expected: &[u8] = &[
            0x0e, // Record::RulePut is variant 14
            0x07, b'w', b'e', b'a', b't', b'h', b'e', b'r', // id
            0x0c, b'W', b'e', b'a', b't', b'h', b'e', b'r', b' ', b'p', b'o', b'l',
            b'l', // name "Weather poll"
            0x01, // enabled true
            0x00, // trigger filter "", the empty trigger of a polling rule
            0x00, // min_qos None
            0x00, // client_id None
            0x02, // ActionRecord::HttpPoll is variant 2
            0x1b, b'h', b't', b't', b'p', b's', b':', b'/', b'/', b'e', b'x', b'a', b'm', b'p',
            b'l', b'e', b'.', b'i', b'n', b'v', b'a', b'l', b'i', b'd', b'/', b'n', b'o',
            b'w', // url, twenty-seven bytes
            0x01, // one header
            0x06, b'a', b'c', b'c', b'e', b'p', b't', // its name
            0x0a, b't', b'e', b'x', b't', b'/', b'p', b'l', b'a', b'i', b'n', // its value
            0xe0, 0xd4, 0x03, // interval_ms 60000
            0x88, 0x27, // timeout_ms 5000
            0x80, 0x80, 0x04, // max_body_bytes 65536
            0x0b, b'w', b'e', b'a', b't', b'h', b'e', b'r', b'/', b'n', b'o',
            b'w', // topic "weather/now"
            0x02, // qos ExactlyOnce
            0x01, // retain true
        ];
        pins(
            &Record::RulePut {
                rule: polling_rule(),
            },
            expected,
        );
    }

    #[test]
    fn rule_removed() {
        let expected: &[u8] = &[
            0x0f, // Record::RuleRemoved is variant 15
            0x09, b't', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y',
        ];
        pins(
            &Record::RuleRemoved {
                id: "telemetry".to_owned(),
            },
            expected,
        );
    }

    /// The envelope the three vectors below are of.
    ///
    /// `id` and `body` are sixteen bytes each of `0x11` and `0x22`, so
    /// that reading one where the other belongs is legible rather than
    /// hidden behind shared zeroes.
    fn envelope() -> OutboxRecord {
        OutboxRecord {
            sequence: 300,
            id: MessageId([0x11; 16]),
            body: MessageId([0x22; 16]),
            client_id: "sensor".to_owned(),
            enqueued_at: 1_700_000_000,
            attempts: 2,
            next_attempt_at: 1_700_000_017,
        }
    }

    #[test]
    fn outbox_appended() {
        // A newtype struct is its inner value, and a fixed-size array is
        // its elements with no length prefix, so a `MessageId` is
        // sixteen bare bytes. 300 = 2 * 128 + 44, so its varint is
        // 44|0x80 then 2. 1_700_000_000 and 1_700_000_017 differ only in
        // their lowest seven bits, which is the first byte of each.
        let expected: &[u8] = &[
            0x10, // Record::OutboxAppended is variant 16
            0x09, b't', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y', // rule
            0xac, 0x02, // sequence 300
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, // id, sixteen bytes
            0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
            0x22, 0x22, // body, sixteen bytes
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x06, b's', b'e', b'n', b's', b'o',
            b'r', // client_id "sensor"
            0x80, 0xe2, 0xcf, 0xaa, 0x06, // enqueued_at 1_700_000_000
            0x02, // attempts 2
            0x91, 0xe2, 0xcf, 0xaa, 0x06, // next_attempt_at 1_700_000_017
        ];
        pins(
            &Record::OutboxAppended {
                rule: "telemetry".to_owned(),
                entry: envelope(),
            },
            expected,
        );
    }

    #[test]
    fn outbox_attempted() {
        let expected: &[u8] = &[
            0x11, // Record::OutboxAttempted is variant 17
            0x09, b't', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y', // rule
            0xac, 0x02, // sequence 300
            0x04, // attempts 4
            0x91, 0xe2, 0xcf, 0xaa, 0x06, // next_attempt_at 1_700_000_017
        ];
        pins(
            &Record::OutboxAttempted {
                rule: "telemetry".to_owned(),
                sequence: 300,
                attempts: 4,
                next_attempt_at: 1_700_000_017,
            },
            expected,
        );
    }

    #[test]
    fn outbox_removed() {
        let expected: &[u8] = &[
            0x12, // Record::OutboxRemoved is variant 18
            0x09, b't', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y', // rule
            0xac, 0x02, // sequence 300
        ];
        pins(
            &Record::OutboxRemoved {
                rule: "telemetry".to_owned(),
                sequence: 300,
            },
            expected,
        );
    }

    /// Section 8.1's identity.
    ///
    /// The UUID is a fixed-size array, so postcard writes its sixteen
    /// bytes with no length prefix; the name is a varint length and
    /// then the bytes. The two are distinguishable in this vector —
    /// sixteen bytes that are not ASCII, then a length that is — so a
    /// transposition of the fields would not decode at all, let alone
    /// decode to something else.
    #[test]
    fn node_identity_put() {
        let expected: &[u8] = &[
            0x13, // Record::NodeIdentityPut is variant 19
            0x01, 0x93, 0x4e, 0x3a, 0x7c, 0x10, 0x70, 0x00, // uuid, sixteen bytes
            0x80, 0x00, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, //
            0x06, b'e', b'd', b'g', b'e', b'-', b'1', // name "edge-1"
        ];
        pins(
            &Record::NodeIdentityPut {
                identity: NodeIdentityRecord {
                    uuid: [
                        0x01, 0x93, 0x4e, 0x3a, 0x7c, 0x10, 0x70, 0x00, 0x80, 0x00, 0x0a, 0x0b,
                        0x0c, 0x0d, 0x0e, 0x0f,
                    ],
                    name: "edge-1".to_owned(),
                },
            },
            expected,
        );
    }

    /// Section 8.2's clock mark.
    ///
    /// Two varints, and the vector is chosen so that a transposition of
    /// the fields would not decode: a realistic millisecond reading is
    /// six bytes of varint and the counter is two, so reading them the
    /// other way round runs off the end of the record.
    #[test]
    fn clock_high_water_put() {
        let expected: &[u8] = &[
            0x14, // Record::ClockHighWaterPut is variant 20
            0xfb, 0xd0, 0x95, 0xff, 0xbc, 0x31, // millis 1_700_000_000_123
            0xac, 0x02, // counter 300
        ];
        pins(
            &Record::ClockHighWaterPut {
                reading: ClockRecord {
                    millis: 1_700_000_000_123,
                    counter: 300,
                },
            },
            expected,
        );
    }

    /// Section 8.2's replicated register, with a value.
    ///
    /// Two sixteen-byte arrays in one record — the node in the key and
    /// the node in the stamp — so they carry **different** bytes here: a
    /// transposition of the two would otherwise round trip perfectly and
    /// attribute every write to the wrong node. The filter is three
    /// bytes between them, which is what makes the transposition
    /// visible at all.
    ///
    /// The value is a unit variant, which is one byte after the
    /// `Option`'s presence byte. That is the whole of a node
    /// subscription: section 8.2 replicates that a node has a
    /// subscriber, and nothing about which of its clients it is.
    #[test]
    fn metadata_put_with_a_value() {
        let expected: &[u8] = &[
            0x15, // Record::MetadataPut is variant 21
            0x00, // MetadataKeyRecord::NodeSubscription is variant 0
            0x01, 0x93, 0x4e, 0x3a, 0x7c, 0x10, 0x70, 0x00, // the key's node
            0x80, 0x00, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, //
            0x03, b'a', b'/', b'b', // filter "a/b"
            0xfb, 0xd0, 0x95, 0xff, 0xbc, 0x31, // stamp millis 1_700_000_000_123
            0xac, 0x02, // stamp counter 300
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // the stamp's node
            0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, //
            0x01, // the value is present
            0x00, // MetadataValueRecord::NodeSubscription is variant 0
        ];
        pins(
            &Record::MetadataPut {
                entry: MetadataRecord {
                    key: MetadataKeyRecord::NodeSubscription {
                        node: [
                            0x01, 0x93, 0x4e, 0x3a, 0x7c, 0x10, 0x70, 0x00, 0x80, 0x00, 0x0a, 0x0b,
                            0x0c, 0x0d, 0x0e, 0x0f,
                        ],
                        filter: "a/b".to_owned(),
                    },
                    stamp: StampRecord {
                        millis: 1_700_000_000_123,
                        counter: 300,
                        node: [
                            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
                            0x1d, 0x1e, 0x1f, 0x20,
                        ],
                    },
                    value: Some(MetadataValueRecord::NodeSubscription),
                },
            },
            expected,
        );
    }

    /// A group membership register, which is the last variant.
    ///
    /// **Pinned because the variant index is the format.** A node
    /// subscription and a group membership differ by their leading
    /// byte and by one extra string, and section 8.3 depends on the two
    /// being different registers: a group whose membership were filed
    /// as a node subscription would put every member node on the
    /// ordinary route and deliver one publication once per node.
    ///
    /// Four, and at the end of the enum, which is what lets this be
    /// added without moving `FORMAT_VERSION`: every register a previous
    /// build wrote still decodes to the variant it was written as.
    ///
    /// The group and the filter are two separate strings rather than
    /// one joined by a slash, and a vector is where that is visible: a
    /// share name containing a slash is refused by the codec, but a
    /// format that relied on it would be a format whose meaning changed
    /// if that ever stopped being true.
    #[test]
    fn metadata_put_for_a_group_membership() {
        let expected: &[u8] = &[
            0x15, // Record::MetadataPut is variant 21
            0x04, // MetadataKeyRecord::NodeGroup is variant 4
            0x01, 0x93, 0x4e, 0x3a, 0x7c, 0x10, 0x70, 0x00, // the key's node
            0x80, 0x00, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, //
            0x07, b'w', b'o', b'r', b'k', b'e', b'r', b's', // group "workers"
            0x03, b'a', b'/', b'b', // filter "a/b"
            0xfb, 0xd0, 0x95, 0xff, 0xbc, 0x31, // stamp millis 1_700_000_000_123
            0xac, 0x02, // stamp counter 300
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // the stamp's node
            0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, //
            0x01, // the value is present
            0x04, // MetadataValueRecord::NodeGroup is variant 4
        ];
        pins(
            &Record::MetadataPut {
                entry: MetadataRecord {
                    key: MetadataKeyRecord::NodeGroup {
                        node: [
                            0x01, 0x93, 0x4e, 0x3a, 0x7c, 0x10, 0x70, 0x00, 0x80, 0x00, 0x0a, 0x0b,
                            0x0c, 0x0d, 0x0e, 0x0f,
                        ],
                        group: "workers".to_owned(),
                        filter: "a/b".to_owned(),
                    },
                    stamp: StampRecord {
                        millis: 1_700_000_000_123,
                        counter: 300,
                        node: [
                            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
                            0x1d, 0x1e, 0x1f, 0x20,
                        ],
                    },
                    value: Some(MetadataValueRecord::NodeGroup),
                },
            },
            expected,
        );
    }

    /// The same record carrying a tombstone.
    ///
    /// **The only thing that distinguishes a deletion is one byte**, the
    /// `Option`'s absence byte, and that is the design rather than an
    /// economy: section 8.2 makes a tombstone a value competing by the
    /// same comparison, so there is no removal record and no flag. A
    /// vector for it because a single byte is exactly the kind of
    /// difference an encoding change loses silently, and losing it turns
    /// every delete in the cluster back into a write.
    #[test]
    fn metadata_put_carrying_a_tombstone() {
        let expected: &[u8] = &[
            0x15, // Record::MetadataPut is variant 21
            0x02, // MetadataKeyRecord::Rule is variant 2
            0x09, b't', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y', // id
            0x05, // stamp millis 5
            0x00, // stamp counter 0
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, // the stamp's node
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, //
            0x00, // the value is absent, which is what a tombstone is
        ];
        pins(
            &Record::MetadataPut {
                entry: MetadataRecord {
                    key: MetadataKeyRecord::Rule {
                        id: "telemetry".to_owned(),
                    },
                    stamp: StampRecord {
                        millis: 5,
                        counter: 0,
                        node: [0x22; 16],
                    },
                    value: None,
                },
            },
            expected,
        );
    }

    /// A session owner register, whose value carries the epoch.
    ///
    /// **Pinned because the epoch is the one field in a stored register
    /// that another node's behaviour depends on arithmetically.**
    /// Section 8.4 has the claiming node write the epoch it read plus
    /// one, so a build that wrote the epoch where another read the
    /// node's bytes would have every node claiming at an epoch derived
    /// from sixteen bytes of UUID. A round trip cannot see that, which
    /// is convention 5's first entry: read from and written to the same
    /// wrong place, it round trips perfectly.
    ///
    /// Two sixteen-byte arrays again — the owner in the value and the
    /// writer in the stamp — carrying different bytes, because they are
    /// different nodes whenever a claim was relayed and a transposition
    /// would otherwise attribute the ownership to whoever gossiped it.
    ///
    /// The epoch is 300, which is two varint bytes: a one-byte epoch
    /// would be indistinguishable from a format that wrote it as a
    /// single octet.
    #[test]
    fn metadata_put_for_a_session_owner() {
        let expected: &[u8] = &[
            0x15, // Record::MetadataPut is variant 21
            0x03, // MetadataKeyRecord::SessionOwner is variant 3
            0x01, b't', // the key's mountpoint "t"
            0x05, b'g', b'a', b'u', b'g', b'e', // the key's client "gauge"
            0x05, // stamp millis 5
            0x00, // stamp counter 0
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, // the stamp's node
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, //
            0x01, // the value is present
            0x03, // MetadataValueRecord::SessionOwner is variant 3
            0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, // the owning node
            0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, //
            0xac, 0x02, // epoch 300
        ];
        pins(
            &Record::MetadataPut {
                entry: MetadataRecord {
                    key: MetadataKeyRecord::SessionOwner {
                        session: SessionKey::new("t", "gauge"),
                    },
                    stamp: StampRecord {
                        millis: 5,
                        counter: 0,
                        node: [0x22; 16],
                    },
                    value: Some(MetadataValueRecord::SessionOwner {
                        node: [0x33; 16],
                        epoch: 300,
                    }),
                },
            },
            expected,
        );
    }

    /// Boxing the rule inside a register costs no bytes.
    ///
    /// [`MetadataValueRecord::Rule`] holds a `Box<RuleRecord>` so that
    /// the variant is not three hundred bytes while the smallest is
    /// sixteen, and the whole of that decision rests on serde writing a
    /// `Box<T>` exactly as it writes a `T`. **That is a claim about
    /// somebody else's derive**, and the two vectors above do not pin it,
    /// because neither of them carries a rule: a release that
    /// length-prefixed a box would move every stored register and the
    /// round trip would not notice, because it would read back whatever
    /// it had written.
    ///
    /// So the bytes are compared against the variant index followed by
    /// the rule's own encoding, which is what the unboxed variant would
    /// have produced.
    #[test]
    fn boxing_the_rule_in_a_register_costs_no_bytes() {
        let held = rule();
        let mut expected = vec![0x02]; // MetadataValueRecord::Rule is variant 2
        expected.extend(postcard::to_allocvec(&held).expect("a rule encodes"));
        assert_eq!(
            postcard::to_allocvec(&MetadataValueRecord::Rule(Box::new(held)))
                .expect("a register's value encodes"),
            expected,
            "a boxed rule is not written as the rule it boxes"
        );
    }

    #[test]
    fn retained_removed() {
        let expected: &[u8] = &[
            0x0d, // Record::RetainedRemoved is variant 13
            0x03, b'a', b'/', b'b',
        ];
        pins(
            &Record::RetainedRemoved {
                topic: "a/b".to_owned(),
            },
            expected,
        );
    }

    /// A collection takes a key and carries nothing else.
    ///
    /// The one record that removes a register. Its key is `Retained`
    /// rather than `NodeSubscription` so that the variant index of the key
    /// (`0x01`) differs from the record's own discriminant and from the
    /// topic's length, which is what makes a transposition visible.
    #[test]
    fn metadata_removed() {
        let expected: &[u8] = &[
            0x16, // Record::MetadataRemoved is variant 22
            0x01, // MetadataKeyRecord::Retained is variant 1
            0x03, b'a', b'/', b'b', // topic "a/b"
        ];
        pins(
            &Record::MetadataRemoved {
                key: MetadataKeyRecord::Retained {
                    topic: "a/b".to_owned(),
                },
            },
            expected,
        );
    }

    /// How far a peer's log has been read: sixteen bytes and a number.
    ///
    /// The node's bytes are not all the same and none of them is the
    /// two-byte varint that follows, so neither a transposition nor a
    /// width mistake can hide.
    #[test]
    fn metadata_seen_put() {
        let expected: &[u8] = &[
            0x17, // Record::MetadataSeenPut is variant 23
            0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, // the node
            0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, //
            0xac, 0x02, // through 300
        ];
        pins(
            &Record::MetadataSeenPut {
                seen: SeenRecord {
                    node: [
                        0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c,
                        0x3d, 0x3e, 0x3f, 0x40,
                    ],
                    through: 300,
                },
            },
            expected,
        );
    }

    /// One entry of this node's own log: the sequence, then the register.
    ///
    /// **The sequence comes first**, which is what pins the field order of
    /// `LoggedRecord`: swapped, the register's key variant would be read
    /// as the sequence and the whole entry would land under a number
    /// nobody asked for. It is a two-byte varint and nothing else here is,
    /// so the swap is visible.
    #[test]
    fn oplog_appended() {
        let expected: &[u8] = &[
            0x18, // Record::OpLogAppended is variant 24
            0x81, 0x01, // sequence 129
            0x02, // MetadataKeyRecord::Rule is variant 2
            0x09, b't', b'e', b'l', b'e', b'm', b'e', b't', b'r', b'y', // id
            0x05, // stamp millis 5
            0x00, // stamp counter 0
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, // the stamp's node
            0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, //
            0x00, // the value is absent, which is what a tombstone is
        ];
        pins(
            &Record::OpLogAppended {
                entry: LoggedRecord {
                    sequence: 129,
                    entry: MetadataRecord {
                        key: MetadataKeyRecord::Rule {
                            id: "telemetry".to_owned(),
                        },
                        stamp: StampRecord {
                            millis: 5,
                            counter: 0,
                            node: [0x22; 16],
                        },
                        value: None,
                    },
                },
            },
            expected,
        );
    }

    #[test]
    fn oplog_truncated() {
        let expected: &[u8] = &[
            0x19, // Record::OpLogTruncated is variant 25
            0xac, 0x02, // through 300
        ];
        pins(&Record::OpLogTruncated { through: 300 }, expected);
    }
}

/// The format version, and what a reader does when it does not know one.
///
/// This is the test that matters most in this module. Every other failure
/// here is noisy; this one is silent, and what it corrupts is a session
/// store that looked fine at startup.
mod version {
    use super::*;

    /// A marker for a version that is not ours, assembled by hand so that
    /// the test does not depend on this build being able to produce one.
    fn marker_for(version: u16) -> Vec<u8> {
        let mut bytes = FORMAT_MAGIC.to_vec();
        assert!(version < 128, "the hand-written varint is one byte only");
        bytes.push(version as u8);
        bytes
    }

    #[test]
    fn a_later_version_is_refused_by_name() {
        let later = FORMAT_VERSION + 1;
        let err = FormatMarker::read(&marker_for(later))
            .expect_err("a store from a later build must not be read");
        assert_eq!(
            err,
            FormatError::UnknownVersion {
                found: later,
                expected: FORMAT_VERSION,
            }
        );
        // An operator who cannot see which build wrote the file cannot act
        // on the refusal, so the message carries both numbers.
        let message = err.to_string();
        assert!(
            message.contains(&later.to_string()) && message.contains(&FORMAT_VERSION.to_string()),
            "the error must name both versions, said: {message}"
        );
    }

    #[test]
    fn an_earlier_version_is_refused_too() {
        // Nothing has been released, so there is no older format to be
        // compatible with, and accepting one would claim a compatibility
        // never tested. When migrations exist, this becomes a range and
        // this test changes with it.
        let err = FormatMarker::read(&marker_for(0)).expect_err("version 0 is not ours");
        assert_eq!(
            err,
            FormatError::UnknownVersion {
                found: 0,
                expected: FORMAT_VERSION,
            }
        );
    }

    #[test]
    fn something_that_is_not_a_store_is_refused_as_such() {
        let mut bytes = b"notamqtt".to_vec();
        bytes.push(FORMAT_VERSION as u8);
        assert_eq!(FormatMarker::read(&bytes), Err(FormatError::NotAStore));
    }

    #[test]
    fn a_truncated_marker_does_not_decode() {
        let short = &FORMAT_MAGIC[..4];
        assert!(matches!(
            FormatMarker::read(short),
            Err(FormatError::Undecodable(_))
        ));
    }

    #[test]
    fn trailing_bytes_after_a_marker_are_refused() {
        // postcard stops when the value is complete and says nothing about
        // what follows. A record written by a longer layout would decode
        // as a shorter one and look healthy, so the remainder is checked.
        let mut bytes = FormatMarker::current().encode();
        bytes.push(0xff);
        assert_eq!(
            FormatMarker::read(&bytes),
            Err(FormatError::Trailing { extra: 1 })
        );
    }

    #[test]
    fn trailing_bytes_after_a_record_are_refused() {
        let mut bytes = Record::RetainedRemoved {
            topic: "a/b".to_owned(),
        }
        .encode();
        bytes.extend_from_slice(&[0x00, 0x00]);
        assert_eq!(
            Record::decode(&bytes),
            Err(FormatError::Trailing { extra: 2 })
        );
    }

    #[test]
    fn a_truncated_record_does_not_decode() {
        let bytes = Record::SessionRemoved { session: key() }.encode();
        let err = Record::decode(&bytes[..bytes.len() - 3])
            .expect_err("a record cut short must not decode");
        assert!(matches!(err, FormatError::Undecodable(_)), "said: {err:?}");
    }

    #[test]
    fn an_unknown_discriminant_does_not_decode() {
        // The highest variant is 18. A record written by a build that has
        // more of them must be refused rather than read as something else.
        let err = Record::decode(&[0x63]).expect_err("variant 99 is not ours");
        assert!(matches!(err, FormatError::Undecodable(_)), "said: {err:?}");
    }

    fn key() -> SessionKey {
        SessionKey::new("tenant-a", "sensor")
    }
}

/// What [`Record::weight`] must count.
///
/// The writer's high-water mark in bytes is only a bound on memory if
/// every part of a record that a peer can grow is in the figure. A term
/// left out is a hole exactly the size of whatever a client puts there,
/// which for a payload is the configured maximum packet size per record.
///
/// So this is not "the weight is about right": each case grows one field
/// by a known amount and requires the weight to grow by exactly that.
/// Deleting any term from the estimate fails the case that grows it, and
/// nothing else.
mod weight {
    use super::*;

    /// How much heavier the record is when one of its fields grows by
    /// `grown`.
    fn grew_by(small: &Record, large: &Record) -> usize {
        large
            .weight()
            .checked_sub(small.weight())
            .expect("growing a field cannot make a record lighter")
    }

    /// A body carrying `payload` bytes and `properties` bytes.
    fn a_body(topic: &str, payload: usize, properties: usize) -> BodyRecord {
        BodyRecord {
            topic: topic.to_owned(),
            payload: Bytes::from(vec![0xa5u8; payload]),
            qos: Qos::AtLeastOnce,
            retain: false,
            expires_at: None,
            properties: Bytes::from(vec![0x26u8; properties]),
        }
    }

    #[test]
    fn a_payload_is_counted_in_full() {
        let small = Record::BodyPut {
            id: MessageId([1; 16]),
            body: a_body("sensor/a", 0, 0),
        };
        let large = Record::BodyPut {
            id: MessageId([1; 16]),
            body: a_body("sensor/a", 4_096, 0),
        };
        assert_eq!(grew_by(&small, &large), 4_096);
    }

    #[test]
    fn a_property_block_is_counted() {
        // Opaque bytes the publisher chose, and as unbounded as the
        // payload beside it.
        let small = Record::BodyPut {
            id: MessageId([1; 16]),
            body: a_body("sensor/a", 0, 0),
        };
        let large = Record::BodyPut {
            id: MessageId([1; 16]),
            body: a_body("sensor/a", 0, 300),
        };
        assert_eq!(grew_by(&small, &large), 300);
    }

    #[test]
    fn a_topic_is_counted() {
        let small = Record::BodyPut {
            id: MessageId([1; 16]),
            body: a_body("a", 0, 0),
        };
        let large = Record::BodyPut {
            id: MessageId([1; 16]),
            body: a_body(&"a".repeat(65), 0, 0),
        };
        assert_eq!(grew_by(&small, &large), 64);
    }

    #[test]
    fn a_retained_payload_is_counted_in_full() {
        // The one a fire-and-forget publisher can send in a loop
        // without touching any quota, which is what the mark exists
        // for.
        let retained = |payload: usize| Record::RetainedPut {
            topic: "sensor/a".to_owned(),
            message: RetainedRecord {
                payload: Bytes::from(vec![0xa5u8; payload]),
                qos: Qos::AtMostOnce,
                expires_at: None,
                properties: Bytes::new(),
            },
        };
        assert_eq!(grew_by(&retained(0), &retained(1_024)), 1_024);
    }

    #[test]
    fn a_will_payload_is_counted() {
        // A connect can carry one, and it is as large as any other
        // payload.
        let with_will = |payload: usize| {
            Record::SessionPut(SessionRecord {
                key: SessionKey::new("", "c"),
                version: ProtocolVersion::V5,
                expiry_interval: 0,
                expires_at: None,
                will: Some(WillRecord {
                    topic: "bye".to_owned(),
                    payload: Bytes::from(vec![0u8; payload]),
                    qos: Qos::AtMostOnce,
                    retain: false,
                    delay: 0,
                    properties: Bytes::new(),
                }),
                will_at: None,
            })
        };
        assert_eq!(grew_by(&with_will(0), &with_will(512)), 512);
    }

    #[test]
    fn a_session_key_is_counted() {
        // Every per-session record carries one, and both halves of it
        // are the client's own strings.
        let removed = |client: &str| Record::SessionRemoved {
            session: SessionKey::new("tenant-a", client),
        };
        assert_eq!(grew_by(&removed("c"), &removed(&"c".repeat(129))), 128);
    }

    #[test]
    fn a_filter_is_counted() {
        let added = |filter: &str| Record::SubscriptionAdded {
            session: SessionKey::new("", "c"),
            subscription: SubscriptionRecord {
                filter: filter.to_owned(),
                qos: Qos::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAlways,
                subscription_id: None,
            },
        };
        assert_eq!(grew_by(&added("a"), &added(&"a".repeat(41))), 40);
    }

    #[test]
    fn the_subscription_identifiers_of_a_delivery_are_counted() {
        // A client with many overlapping subscriptions puts one of
        // these on every delivery, so the vector is the client's to
        // grow.
        let queued = |ids: usize| Record::QueuePushed {
            session: SessionKey::new("", "c"),
            entry: QueueEntry {
                sequence: 1,
                packet_id: None,
                delivery: DeliveryRecord {
                    body: MessageId([0; 16]),
                    qos: Qos::AtLeastOnce,
                    retain: false,
                    subscription_ids: (0..ids as u32).collect(),
                    dup: false,
                },
            },
        };
        assert_eq!(
            grew_by(&queued(0), &queued(16)),
            16 * std::mem::size_of::<u32>()
        );
    }

    #[test]
    fn the_smallest_record_still_weighs_what_it_occupies() {
        // Zero would be wrong: the mark in records is not the only one
        // a flood of tiny records has to cross, and a record that
        // weighed nothing would cross neither.
        assert!(
            Record::BodyReleased {
                id: MessageId([0; 16])
            }
            .weight()
                >= std::mem::size_of::<Record>()
        );
    }
}

/// Round trips over generated values.
///
/// Not the guard: a round trip cannot see a layout that is consistently
/// wrong in both directions, which is exactly what the golden vectors are
/// for. This covers the shapes the vectors do not enumerate, in particular
/// empty and multi-byte strings, long sequences and absent options.
mod roundtrip {
    use super::*;
    use proptest::prelude::*;

    fn bytes() -> impl Strategy<Value = Bytes> {
        proptest::collection::vec(any::<u8>(), 0..24).prop_map(Bytes::from)
    }

    fn text() -> impl Strategy<Value = String> {
        ".{0,24}"
    }

    fn session_key() -> impl Strategy<Value = SessionKey> {
        (text(), text()).prop_map(|(mountpoint, client_id)| SessionKey {
            mountpoint,
            client_id,
        })
    }

    fn qos() -> impl Strategy<Value = Qos> {
        prop_oneof![
            Just(Qos::AtMostOnce),
            Just(Qos::AtLeastOnce),
            Just(Qos::ExactlyOnce)
        ]
    }

    fn retain_handling() -> impl Strategy<Value = RetainHandling> {
        prop_oneof![
            Just(RetainHandling::SendAlways),
            Just(RetainHandling::SendIfNew),
            Just(RetainHandling::DoNotSend)
        ]
    }

    fn inflight_state() -> impl Strategy<Value = InflightState> {
        prop_oneof![
            Just(InflightState::AwaitingAck),
            Just(InflightState::AwaitingReceived),
            Just(InflightState::AwaitingComplete)
        ]
    }

    fn message_id() -> impl Strategy<Value = MessageId> {
        any::<[u8; 16]>().prop_map(MessageId)
    }

    fn will() -> impl Strategy<Value = WillRecord> {
        (text(), bytes(), qos(), any::<bool>(), any::<u32>(), bytes()).prop_map(
            |(topic, payload, qos, retain, delay, properties)| WillRecord {
                topic,
                payload,
                qos,
                retain,
                delay,
                properties,
            },
        )
    }

    fn session_record() -> impl Strategy<Value = SessionRecord> {
        (
            session_key(),
            prop_oneof![Just(ProtocolVersion::V311), Just(ProtocolVersion::V5)],
            any::<u32>(),
            any::<Option<u32>>(),
            proptest::option::of(will()),
            any::<Option<u32>>(),
        )
            .prop_map(
                |(key, version, expiry_interval, expires_at, will, will_at)| SessionRecord {
                    key,
                    version,
                    expiry_interval,
                    expires_at,
                    will,
                    will_at,
                },
            )
    }

    fn subscription() -> impl Strategy<Value = SubscriptionRecord> {
        (
            text(),
            qos(),
            any::<bool>(),
            any::<bool>(),
            retain_handling(),
            any::<Option<u32>>(),
        )
            .prop_map(
                |(filter, qos, no_local, retain_as_published, retain_handling, subscription_id)| {
                    SubscriptionRecord {
                        filter,
                        qos,
                        no_local,
                        retain_as_published,
                        retain_handling,
                        subscription_id,
                    }
                },
            )
    }

    fn delivery() -> impl Strategy<Value = DeliveryRecord> {
        (
            message_id(),
            qos(),
            any::<bool>(),
            proptest::collection::vec(any::<u32>(), 0..6),
            any::<bool>(),
        )
            .prop_map(
                |(body, qos, retain, subscription_ids, dup)| DeliveryRecord {
                    body,
                    qos,
                    retain,
                    subscription_ids,
                    dup,
                },
            )
    }

    fn queue_entry() -> impl Strategy<Value = QueueEntry> {
        (any::<u64>(), any::<Option<u16>>(), delivery()).prop_map(
            |(sequence, packet_id, delivery)| QueueEntry {
                sequence,
                packet_id,
                delivery,
            },
        )
    }

    fn inflight() -> impl Strategy<Value = InflightRecord> {
        (
            any::<u16>(),
            any::<u64>(),
            inflight_state(),
            any::<u32>(),
            delivery(),
        )
            .prop_map(
                |(packet_id, sequence, state, sent_at, delivery)| InflightRecord {
                    packet_id,
                    sequence,
                    state,
                    sent_at,
                    delivery,
                },
            )
    }

    fn body() -> impl Strategy<Value = BodyRecord> {
        (
            text(),
            bytes(),
            qos(),
            any::<bool>(),
            any::<Option<u32>>(),
            bytes(),
        )
            .prop_map(
                |(topic, payload, qos, retain, expires_at, properties)| BodyRecord {
                    topic,
                    payload,
                    qos,
                    retain,
                    expires_at,
                    properties,
                },
            )
    }

    fn trigger() -> impl Strategy<Value = TriggerRecord> {
        (
            text(),
            proptest::option::of(qos()),
            proptest::option::of(text()),
        )
            .prop_map(|(filter, min_qos, client_id)| TriggerRecord {
                filter,
                min_qos,
                client_id,
            })
    }

    /// **Every variant, not the first one.** Convention 5: a generator
    /// that cannot produce a case leaves that case untested however
    /// many thousand cases are run, and this strategy produced nothing
    /// but an `HttpOut` while two other variants existed. The round
    /// trip below is what says a record survives the encoding, so a
    /// variant it never builds is a variant nothing says that about.
    fn action() -> impl Strategy<Value = ActionRecord> {
        prop_oneof![
            (
                text(),
                text(),
                proptest::collection::vec((text(), text()), 0..4),
                any::<u32>(),
                any::<u16>(),
                any::<u8>(),
                any::<u32>(),
            )
                .prop_map(
                    |(url, method, headers, timeout_ms, max_concurrency, retries, backoff_ms)| {
                        ActionRecord::HttpOut(HttpOutRecord {
                            url,
                            method,
                            headers,
                            timeout_ms,
                            max_concurrency,
                            retries,
                            backoff_ms,
                        })
                    },
                ),
            (
                text(),
                text(),
                proptest::option::of((topic_from(), text())),
                qos(),
                any::<bool>(),
            )
                .prop_map(|(token, topic, topic_from, qos, retain)| {
                    ActionRecord::HttpIn(HttpInRecord {
                        token: Secret::new(token),
                        topic,
                        topic_from: topic_from
                            .map(|(source, prefix)| TopicFromRequestRecord { source, prefix }),
                        qos,
                        retain,
                    })
                }),
            (
                text(),
                proptest::collection::vec((text(), text()), 0..4),
                any::<u32>(),
                any::<u32>(),
                any::<u32>(),
                text(),
                qos(),
                any::<bool>(),
            )
                .prop_map(
                    |(
                        url,
                        headers,
                        interval_ms,
                        timeout_ms,
                        max_body_bytes,
                        topic,
                        qos,
                        retain,
                    )| {
                        ActionRecord::HttpPoll(HttpPollRecord {
                            url,
                            headers,
                            interval_ms,
                            timeout_ms,
                            max_body_bytes,
                            topic,
                            qos,
                            retain,
                        })
                    },
                ),
        ]
    }

    /// Where an ingest rule reads a topic from, either spelling.
    fn topic_from() -> impl Strategy<Value = TopicFromRecord> {
        prop_oneof![
            text().prop_map(TopicFromRecord::Header),
            text().prop_map(TopicFromRecord::Query),
        ]
    }

    fn outbox_entry() -> impl Strategy<Value = OutboxRecord> {
        (
            any::<u64>(),
            message_id(),
            message_id(),
            text(),
            any::<u32>(),
            any::<u32>(),
            any::<u32>(),
        )
            .prop_map(
                |(sequence, id, body, client_id, enqueued_at, attempts, next_attempt_at)| {
                    OutboxRecord {
                        sequence,
                        id,
                        body,
                        client_id,
                        enqueued_at,
                        attempts,
                        next_attempt_at,
                    }
                },
            )
    }

    fn rule() -> impl Strategy<Value = RuleRecord> {
        (text(), text(), any::<bool>(), trigger(), action()).prop_map(
            |(id, name, enabled, trigger, action)| RuleRecord {
                id,
                name,
                enabled,
                trigger,
                action,
            },
        )
    }

    /// Every [`MetadataKeyRecord`] variant.
    fn metadata_key() -> impl Strategy<Value = MetadataKeyRecord> {
        prop_oneof![
            (any::<[u8; 16]>(), text())
                .prop_map(|(node, filter)| MetadataKeyRecord::NodeSubscription { node, filter }),
            text().prop_map(|topic| MetadataKeyRecord::Retained { topic }),
            text().prop_map(|id| MetadataKeyRecord::Rule { id }),
            session_key().prop_map(|session| MetadataKeyRecord::SessionOwner { session }),
        ]
    }

    /// Every [`MetadataValueRecord`] variant.
    fn metadata_value() -> impl Strategy<Value = MetadataValueRecord> {
        prop_oneof![
            Just(MetadataValueRecord::NodeSubscription),
            retained().prop_map(MetadataValueRecord::Retained),
            rule().prop_map(|held| MetadataValueRecord::Rule(Box::new(held))),
            (any::<[u8; 16]>(), any::<u64>())
                .prop_map(|(node, epoch)| MetadataValueRecord::SessionOwner { node, epoch }),
            // The last variant, which this list did not produce: a
            // generator that cannot make a case is convention 5's
            // second entry, and the case it could not make here is the
            // one section 8.3 separates from a node subscription.
            Just(MetadataValueRecord::NodeGroup),
        ]
    }

    /// A register, with a tombstone as often as a value.
    ///
    /// `proptest::option::of` produces `None` rarely by default, and a
    /// tombstone is not a rare case here — it is half of what section
    /// 8.2's deletion vocabulary is. Weighted so that the absence byte
    /// is exercised rather than stumbled on.
    fn metadata_entry() -> impl Strategy<Value = MetadataRecord> {
        (
            metadata_key(),
            (any::<u64>(), any::<u32>(), any::<[u8; 16]>()),
            prop_oneof![
                1 => Just(None),
                1 => metadata_value().prop_map(Some),
            ],
        )
            .prop_map(|(key, (millis, counter, node), value)| MetadataRecord {
                key,
                stamp: StampRecord {
                    millis,
                    counter,
                    node,
                },
                value,
            })
    }

    fn retained() -> impl Strategy<Value = RetainedRecord> {
        (bytes(), qos(), any::<Option<u32>>(), bytes()).prop_map(
            |(payload, qos, expires_at, properties)| RetainedRecord {
                payload,
                qos,
                expires_at,
                properties,
            },
        )
    }

    /// Every variant, so that adding one and forgetting it here is a
    /// non-exhaustive match and a compile error rather than a silent gap.
    fn record() -> impl Strategy<Value = Record> {
        prop_oneof![
            session_record().prop_map(Record::SessionPut),
            session_key().prop_map(|session| Record::SessionRemoved { session }),
            (session_key(), subscription()).prop_map(|(session, subscription)| {
                Record::SubscriptionAdded {
                    session,
                    subscription,
                }
            }),
            (session_key(), text())
                .prop_map(|(session, filter)| Record::SubscriptionRemoved { session, filter }),
            (session_key(), any::<u16>())
                .prop_map(|(session, packet_id)| Record::InboundReserved { session, packet_id }),
            (session_key(), any::<u16>())
                .prop_map(|(session, packet_id)| Record::InboundReleased { session, packet_id }),
            (session_key(), inflight())
                .prop_map(|(session, entry)| Record::InflightPut { session, entry }),
            (session_key(), any::<u16>())
                .prop_map(|(session, packet_id)| Record::InflightRemoved { session, packet_id }),
            (session_key(), queue_entry())
                .prop_map(|(session, entry)| Record::QueuePushed { session, entry }),
            (session_key(), any::<u64>())
                .prop_map(|(session, sequence)| Record::QueuePopped { session, sequence }),
            (message_id(), body()).prop_map(|(id, body)| Record::BodyPut { id, body }),
            message_id().prop_map(|id| Record::BodyReleased { id }),
            (text(), retained())
                .prop_map(|(topic, message)| Record::RetainedPut { topic, message }),
            text().prop_map(|topic| Record::RetainedRemoved { topic }),
            rule().prop_map(|rule| Record::RulePut { rule }),
            text().prop_map(|id| Record::RuleRemoved { id }),
            (text(), outbox_entry())
                .prop_map(|(rule, entry)| Record::OutboxAppended { rule, entry }),
            (text(), any::<u64>(), any::<u32>(), any::<u32>()).prop_map(
                |(rule, sequence, attempts, next_attempt_at)| Record::OutboxAttempted {
                    rule,
                    sequence,
                    attempts,
                    next_attempt_at,
                }
            ),
            (text(), any::<u64>())
                .prop_map(|(rule, sequence)| Record::OutboxRemoved { rule, sequence }),
            // Section 8.1's identity and section 8.2's clock mark. Both
            // were missing from this list, which the comment above says
            // cannot happen and `prop_oneof!` does not in fact enforce:
            // it is a list and not a match, so a variant left out is
            // silently never generated. Convention 5's "a generator that
            // cannot produce a case", in the generator that exists to
            // catch everything nobody thought of.
            (any::<[u8; 16]>(), text()).prop_map(|(uuid, name)| Record::NodeIdentityPut {
                identity: NodeIdentityRecord { uuid, name },
            }),
            (any::<u64>(), any::<u32>()).prop_map(|(millis, counter)| {
                Record::ClockHighWaterPut {
                    reading: ClockRecord { millis, counter },
                }
            }),
            metadata_entry().prop_map(|entry| Record::MetadataPut { entry }),
            metadata_key().prop_map(|key| Record::MetadataRemoved { key }),
            (any::<[u8; 16]>(), any::<u64>()).prop_map(|(node, through)| {
                Record::MetadataSeenPut {
                    seen: SeenRecord { node, through },
                }
            }),
            (any::<u64>(), metadata_entry()).prop_map(|(sequence, entry)| {
                Record::OpLogAppended {
                    entry: LoggedRecord { sequence, entry },
                }
            }),
            any::<u64>().prop_map(|through| Record::OpLogTruncated { through }),
        ]
    }

    proptest! {
        #[test]
        fn a_record_survives_its_encoding(record in record()) {
            let bytes = record.encode();
            prop_assert_eq!(Record::decode(&bytes).expect("our own bytes must decode"), record);
        }

        /// Cutting any byte off the end must be refused, never read as a
        /// shorter record. Without this, a torn write could restore a
        /// session with fields it never had.
        #[test]
        fn a_truncated_record_is_never_accepted(record in record(), cut in 1usize..8) {
            let bytes = record.encode();
            prop_assume!(cut < bytes.len());
            let short = &bytes[..bytes.len() - cut];
            if let Ok(decoded) = Record::decode(short) {
                prop_assert!(
                    false,
                    "{} bytes cut off {:?} still decoded, to {:?}",
                    cut, record, decoded
                );
            }
        }
    }
}
