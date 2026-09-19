//! Golden wire-format vectors.
//!
//! Every round trip in the unit tests encodes a packet and immediately
//! decodes it again, so a flag bit or a field order that is consistently
//! wrong in both directions passes the whole suite undetected. The
//! `properties` module hit exactly this hazard during development, and a
//! golden vector caught it.
//!
//! The byte arrays below were hand-derived from the MQTT specification by a
//! reviewer and verified against it, not captured from this codec's own
//! output. They lock in a checked-correct encoding, not merely whatever this
//! crate currently produces. Each vector names the packet it pins and cites
//! the specification section it was derived from.
//!
//! Every later packet type gets its own module here, alongside `connect`.

mod connect {
    // Independence property for this vector set: every settable flag bit
    // must appear set in at least one vector where none of its neighbouring
    // flags are also set, so that swapping any pair of flag constants
    // changes at least one expected byte. A vector where two flags are
    // always true together is invariant under swapping exactly those two,
    // and cannot catch that swap. `encodes_a_v5_connect_with_credentials_
    // and_a_retained_will` sets both will retain and password, so on its
    // own it cannot distinguish `FLAG_WILL_RETAIN` from `FLAG_PASSWORD`;
    // `encodes_a_v5_connect_with_a_retained_will_and_no_password` and
    // `encodes_a_v5_connect_with_a_password_and_no_user_name` below isolate
    // those two bits from each other. When adding vectors for the next
    // packet type, check the same property: no two flags may be "always
    // both set" across the whole set without a vector that separates them.

    use bytes::Bytes;
    use mqtt_codec::packet::{ConnAck, Connect, Will};
    use mqtt_codec::reader::Reader;
    use mqtt_codec::writer::{put_binary, put_string, put_u16};
    use mqtt_codec::{Limits, Properties, QoS, ReasonCode, Version};

    /// A minimal MQTT 3.1.1 CONNECT.
    ///
    /// MQTT-v3.1.1 section 3.1.2: variable header is protocol name, protocol
    /// level, connect flags, keep alive; payload is the client identifier,
    /// each length-prefixed per section 1.5.3.
    #[test]
    fn encodes_a_minimal_v311_connect() {
        let packet = Connect {
            version: Version::V311,
            client_id: "a".into(),
            clean_start: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, // "MQTT", length-prefixed
                0x04, // protocol level 4
                0x00, // connect flags: nothing set
                0x00, 0x00, // keep alive = 0
                0x00, 0x01, 0x61, // client id "a", length-prefixed
            ]
        );
    }

    /// An MQTT 5 CONNECT carrying a user name, a password, and a retained
    /// QoS 1 will, with every property block left empty.
    ///
    /// MQTT-v5.0 section 3.1.2: the property block (section 2.2.2) follows
    /// the keep alive and precedes the payload; section 3.1.3.2 places the
    /// will properties before the will topic, ahead of the will payload.
    #[test]
    fn encodes_a_v5_connect_with_credentials_and_a_retained_will() {
        let packet = Connect {
            version: Version::V5,
            client_id: "cid".into(),
            clean_start: true,
            keep_alive: 60,
            username: Some("u".into()),
            password: Some(Bytes::from_static(b"pw")),
            will: Some(Will {
                topic: "w".into(),
                payload: Bytes::from_static(b"p"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: Properties::default(),
            }),
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        // Connect flags: clean start (0x02) | will (0x04) | will QoS 1 << 3
        // (0x08) | will retain (0x20) | password (0x40) | user name (0x80).
        assert_eq!(out[7], 0xEE);

        assert_eq!(
            out,
            vec![
                0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, // "MQTT"
                0x05, // protocol level 5
                0xEE, // connect flags, see above
                0x00, 0x3C, // keep alive = 60
                0x00, // top-level property block: empty
                0x00, 0x03, 0x63, 0x69, 0x64, // client id "cid"
                0x00, // will property block: empty
                0x00, 0x01, 0x77, // will topic "w"
                0x00, 0x01, 0x70, // will payload "p"
                0x00, 0x01, 0x75, // user name "u"
                0x00, 0x02, 0x70, 0x77, // password "pw"
            ]
        );
    }

    /// An MQTT 5 CONNECT with a retained will and no password.
    ///
    /// Isolates the will-retain bit from the password bit, which the vector
    /// above never sets independently: there, retain and password are both
    /// true, so swapping `FLAG_WILL_RETAIN` and `FLAG_PASSWORD` leaves that
    /// vector's flags byte unchanged. Here only retain is set.
    ///
    /// MQTT-v5.0 section 3.1.2.6 / 3.1.2.7: will retain is bit 5, password
    /// is bit 6, of the connect flags.
    #[test]
    fn encodes_a_v5_connect_with_a_retained_will_and_no_password() {
        let packet = Connect {
            version: Version::V5,
            client_id: "c1".into(),
            clean_start: true,
            keep_alive: 10,
            username: Some("u1".into()),
            password: None,
            will: Some(Will {
                topic: "t1".into(),
                payload: Bytes::from_static(b"p1"),
                qos: QoS::AtMostOnce,
                retain: true,
                properties: Properties::default(),
            }),
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        // Connect flags: clean start (0x02) | will (0x04) | will retain
        // (0x20) | user name (0x80). Password is not set.
        assert_eq!(out[7], 0xA6);

        assert_eq!(
            out,
            vec![
                0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, // "MQTT"
                0x05, // protocol level 5
                0xA6, // connect flags, see above
                0x00, 0x0A, // keep alive = 10
                0x00, // top-level property block: empty
                0x00, 0x02, 0x63, 0x31, // client id "c1"
                0x00, // will property block: empty
                0x00, 0x02, 0x74, 0x31, // will topic "t1"
                0x00, 0x02, 0x70, 0x31, // will payload "p1"
                0x00, 0x02, 0x75, 0x31, // user name "u1"
            ]
        );
    }

    /// An MQTT 5 CONNECT with a password and no user name, no will.
    ///
    /// MQTT 5 permits a password without a user name (3.1.1 does not; see
    /// `rejects_a_311_password_without_a_user_name` in `packet::connect`).
    /// With no will and no user name, this vector sets only the password
    /// bit, isolating it from every other flag, including will retain.
    ///
    /// MQTT-v5.0 section 3.1.2.9: password is bit 6 of the connect flags.
    #[test]
    fn encodes_a_v5_connect_with_a_password_and_no_user_name() {
        let packet = Connect {
            version: Version::V5,
            client_id: "c2".into(),
            clean_start: true,
            keep_alive: 20,
            username: None,
            password: Some(Bytes::from_static(b"pw2")),
            will: None,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        // Connect flags: clean start (0x02) | password (0x40). Nothing else
        // is set.
        assert_eq!(out[7], 0x42);

        assert_eq!(
            out,
            vec![
                0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, // "MQTT"
                0x05, // protocol level 5
                0x42, // connect flags, see above
                0x00, 0x14, // keep alive = 20
                0x00, // top-level property block: empty
                0x00, 0x02, 0x63, 0x32, // client id "c2"
                0x00, 0x03, 0x70, 0x77, 0x32, // password "pw2"
            ]
        );
    }

    /// A CONNECT whose will is QoS 2 and nothing else is set.
    ///
    /// MQTT-v3.1.1 section 3.1.2.6 / MQTT-v5.0 section 3.1.2.6: the will QoS
    /// occupies bits 3-4 of the connect flags. QoS 2 there, with the will
    /// flag (bit 2) and nothing else, is `0b0001_0100`.
    #[test]
    fn encodes_will_qos_two_flags_byte() {
        let packet = Connect {
            version: Version::V311,
            client_id: "c".into(),
            clean_start: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: Some(Will {
                topic: "t".into(),
                payload: Bytes::from_static(b"x"),
                qos: QoS::ExactlyOnce,
                retain: false,
                properties: Properties::default(),
            }),
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(out[7], 0x14);
    }

    /// MQTT 5 CONNACK, success, session present.
    ///
    /// MQTT-v5.0 section 3.2.2: session present is bit 0 of the acknowledge
    /// flags, followed by the one-byte reason code, followed by the
    /// property block.
    #[test]
    fn encodes_connack_v5_success_with_session_present() {
        let packet = ConnAck {
            session_present: true,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(out, vec![0x01, 0x00, 0x00]);
    }

    /// MQTT 5 CONNACK, refused as not authorized.
    ///
    /// MQTT-v5.0 section 3.2.2.2: reason code 0x87 is "Not authorized".
    #[test]
    fn encodes_connack_v5_refused_not_authorized() {
        let packet = ConnAck {
            session_present: false,
            reason: ReasonCode::NotAuthorized,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(out, vec![0x00, 0x87, 0x00]);
    }

    /// MQTT 3.1.1 CONNACK, refused as not authorized.
    ///
    /// MQTT-v3.1.1 section 3.2.2.3: return code 5 is "Connection Refused,
    /// not authorized". No property block exists in 3.1.1.
    #[test]
    fn encodes_connack_v311_refused_not_authorized() {
        let packet = ConnAck {
            session_present: false,
            reason: ReasonCode::NotAuthorized,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(out, vec![0x00, 0x05]);
    }

    /// Decodes a hand-built MQTT 3.1.1 CONNECT, pinning the decoder to the
    /// specification's field layout independently of this crate's encoder.
    ///
    /// MQTT-v3.1.1 section 3.1.2.3: connect flags `0xC6` is
    /// `0b1100_0110`: clean start (bit 1) and will flag (bit 2) set, will
    /// QoS (bits 3-4) zero, will retain (bit 5) clear, password (bit 6) and
    /// user name (bit 7) set.
    #[test]
    fn decodes_a_hand_built_v311_connect() {
        let mut out = Vec::new();
        put_string(&mut out, "MQTT").unwrap();
        out.push(4); // protocol level
        out.push(0xC6); // connect flags, see above
        put_u16(&mut out, 42); // keep alive
        put_string(&mut out, "id1").unwrap(); // client id
        put_string(&mut out, "status").unwrap(); // will topic
        put_binary(&mut out, b"bye").unwrap(); // will payload
        put_string(&mut out, "bob").unwrap(); // user name
        put_binary(&mut out, b"pw1").unwrap(); // password

        let mut reader = Reader::new(&out);
        let decoded = Connect::decode_body(&mut reader, &Limits::default()).unwrap();

        assert_eq!(decoded.version, Version::V311);
        assert!(decoded.clean_start);
        assert_eq!(decoded.client_id, "id1");
        assert_eq!(decoded.keep_alive, 42);
        assert_eq!(
            decoded.will,
            Some(Will {
                topic: "status".into(),
                payload: Bytes::from_static(b"bye"),
                qos: QoS::AtMostOnce,
                retain: false,
                properties: Properties::default(),
            })
        );
        assert_eq!(decoded.username, Some("bob".into()));
        assert_eq!(decoded.password, Some(Bytes::from_static(b"pw1")));
        assert_eq!(decoded.properties, Properties::default());
    }
}

mod publish {
    // Independence property for this vector set: the PUBLISH flags nibble
    // holds duplicate (bit 3), QoS (bits 2-1) and retain (bit 0), so a
    // vector in which two of them are always set together is invariant
    // under swapping exactly those two. The vectors below separate every
    // pair. `encodes_a_retained_qos_zero_v311_publish` sets retain alone
    // (0x01) and `encodes_a_qos_one_v311_publish` sets QoS alone (0x02), so
    // each distinguishes itself from the duplicate bit; the duplicate bit
    // cannot legally stand alone, because it is only meaningful above QoS 0,
    // so `encodes_a_duplicate_qos_one_publish` pairs it with QoS 1 only
    // (0x0A) and `encodes_a_duplicate_qos_two_retained_publish` sets all
    // three (0x3D as a whole first byte). Swapping duplicate and retain
    // turns 0x01 into 0x08 and 0x0A into 0x03; swapping duplicate with
    // either QoS bit turns 0x02 into 0x08 or 0x04 into 0x08. No pair
    // survives.

    use bytes::Bytes;
    use mqtt_codec::packet::{Ack, Publish};
    use mqtt_codec::reader::Reader;
    use mqtt_codec::{Limits, PacketKind, Properties, QoS, ReasonCode, Version};

    /// The first byte of a PUBLISH: type 3 in the high nibble, flags low.
    ///
    /// MQTT-v3.1.1 section 2.2.1 / MQTT-v5.0 section 2.1.2: PUBLISH is
    /// control packet type 3.
    fn first_byte(packet: &Publish) -> u8 {
        (3 << 4) | packet.flags()
    }

    /// A QoS 0 MQTT 3.1.1 PUBLISH, retain and duplicate clear.
    ///
    /// MQTT-v3.1.1 section 3.3.1: byte 1 is the type nibble, then DUP
    /// (bit 3), QoS (bits 2-1) and RETAIN (bit 0); with all three clear the
    /// byte is 0x30. Section 3.3.2.1: the topic name is the first field of
    /// the variable header, length-prefixed per section 1.5.3. Section
    /// 3.3.2.2: no packet identifier is present at QoS 0. Section 3.3.3:
    /// the payload is the remaining bytes, with no length of its own.
    #[test]
    fn encodes_a_qos_zero_v311_publish() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "a/b".into(),
            packet_id: None,
            payload: Bytes::from_static(b"hi"),
            properties: Properties::default(),
        };

        assert_eq!(first_byte(&packet), 0x30);

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x03, 0x61, 0x2F, 0x62, // topic "a/b", length-prefixed
                0x68, 0x69, // payload "hi", to the end of the packet
            ]
        );
    }

    /// A QoS 1 MQTT 3.1.1 PUBLISH, which carries a packet identifier.
    ///
    /// MQTT-v3.1.1 section 3.3.1.2: QoS 1 is bits 2-1 set to 0b01, so the
    /// first byte is 0x32. Section 3.3.2.2: the two-byte packet identifier
    /// follows the topic name and precedes the payload, and is present only
    /// at QoS 1 and 2.
    #[test]
    fn encodes_a_qos_one_v311_publish() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "a/b".into(),
            packet_id: Some(10),
            payload: Bytes::from_static(b"hi"),
            properties: Properties::default(),
        };

        assert_eq!(first_byte(&packet), 0x32);

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x03, 0x61, 0x2F, 0x62, // topic "a/b"
                0x00, 0x0A, // packet identifier 10, after the topic
                0x68, 0x69, // payload "hi"
            ]
        );
    }

    /// A retained QoS 0 PUBLISH, isolating the retain bit.
    ///
    /// MQTT-v3.1.1 section 3.3.1.3: RETAIN is bit 0 of the first byte, so a
    /// retained QoS 0 PUBLISH with no duplicate flag is 0x31.
    #[test]
    fn encodes_a_retained_qos_zero_v311_publish() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: "r".into(),
            packet_id: None,
            payload: Bytes::from_static(b"x"),
            properties: Properties::default(),
        };

        assert_eq!(first_byte(&packet), 0x31);

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x01, 0x72, // topic "r"
                0x78, // payload "x"
            ]
        );
    }

    /// A duplicate QoS 1 PUBLISH with retain clear, isolating the duplicate
    /// bit from the retain bit.
    ///
    /// MQTT-v3.1.1 section 3.3.1.1: DUP is bit 3, and must be 0 for QoS 0
    /// messages, so the narrowest legal vector for it is QoS 1 with retain
    /// clear: 0x30 | 0x08 | 0x02 = 0x3A.
    #[test]
    fn encodes_a_duplicate_qos_one_publish() {
        let packet = Publish {
            dup: true,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "d".into(),
            packet_id: Some(1),
            payload: Bytes::new(),
            properties: Properties::default(),
        };

        assert_eq!(packet.flags(), 0x0A);
        assert_eq!(first_byte(&packet), 0x3A);

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x01, 0x64, // topic "d"
                0x00, 0x01, // packet identifier 1
                      // empty payload
            ]
        );
    }

    /// A duplicate, retained, QoS 2 PUBLISH: every flag bit set at once.
    ///
    /// MQTT-v3.1.1 section 3.3.1 / MQTT-v5.0 section 3.3.1: type 3 with DUP
    /// (0x08), QoS 2 (0b10 in bits 2-1, so 0x04) and RETAIN (0x01) is
    /// 0b0011_1101, or 0x3D.
    #[test]
    fn encodes_a_duplicate_qos_two_retained_publish() {
        let packet = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: "a".into(),
            packet_id: Some(1),
            payload: Bytes::from_static(b"z"),
            properties: Properties::default(),
        };

        assert_eq!(first_byte(&packet), 0x3D);

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x01, 0x61, // topic "a"
                0x00, 0x01, // packet identifier 1
                0x7A, // payload "z"
            ]
        );
    }

    /// An MQTT 5 PUBLISH carrying one property.
    ///
    /// MQTT-v5.0 section 3.3.2: the variable header is topic name, packet
    /// identifier, then the PUBLISH properties, and section 3.3.3 puts the
    /// payload after them. Section 3.3.2.3.2: the payload format indicator
    /// is property identifier 1 (0x01) followed by one byte, so the block
    /// is a length of 2 and then `01 01`.
    #[test]
    fn encodes_a_v5_publish_with_a_property() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "a".into(),
            packet_id: Some(2),
            payload: Bytes::from_static(b"hi"),
            properties: Properties {
                payload_format_indicator: Some(1),
                ..Default::default()
            },
        };

        assert_eq!(first_byte(&packet), 0x32);

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x01, 0x61, // topic "a"
                0x00, 0x02, // packet identifier 2
                0x02, // property block length = 2 bytes
                0x01, 0x01, // payload format indicator = 1
                0x68, 0x69, // payload "hi", after the property block
            ]
        );
    }

    /// An MQTT 3.1.1 PUBACK body: the packet identifier and nothing else.
    ///
    /// MQTT-v3.1.1 section 3.4.2: the variable header contains the packet
    /// identifier from the PUBLISH being acknowledged, with no reason code
    /// or properties in this version.
    ///
    /// Section 3.4.1 fixes the first byte at 0x40, but nothing here asserts
    /// it: `Ack` has no `flags()` and does not build a fixed header, unlike
    /// `Publish`, whose flags nibble is part of the packet. The first byte
    /// of each acknowledgement is pinned at the frame level in a later
    /// task, when the frame encoder exists. The same applies to the PUBREC
    /// and PUBACK vectors below, whose names say which packet the body
    /// belongs to even though these bytes are the body alone.
    #[test]
    fn encodes_a_v311_puback() {
        let ack = Ack {
            packet_id: 7,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        ack.encode_body(Version::V311, PacketKind::PubAck, &mut out)
            .unwrap();

        assert_eq!(out, vec![0x00, 0x07]); // packet identifier 7
    }

    /// An MQTT 5 PUBACK for a success with no properties: the short form.
    ///
    /// MQTT-v5.0 section 3.4.2.1: "If the Remaining Length is 2, the Reason
    /// Code has the value 0x00 (Success)", and section 3.4.2.2.1 allows the
    /// property length to be omitted with it. The body is therefore the two
    /// identifier bytes alone.
    #[test]
    fn encodes_a_v5_puback_in_its_short_form() {
        let ack = Ack {
            packet_id: 7,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        ack.encode_body(Version::V5, PacketKind::PubAck, &mut out)
            .unwrap();

        assert_eq!(out, vec![0x00, 0x07]); // packet identifier 7, nothing else
    }

    /// An MQTT 5 PUBREC with a failure reason and a property: the long form.
    ///
    /// MQTT-v5.0 section 3.5.2: PUBREC is packet identifier, reason code,
    /// then properties. Section 3.5.2.1: reason code 0x10 is "No matching
    /// subscribers". Section 3.5.2.2.2: the reason string is property
    /// identifier 31 (0x1F) followed by a UTF-8 string, so the block is 5
    /// bytes: `1F 00 02 6E 6F`.
    ///
    /// What makes these bytes a PUBREC rather than a PUBACK is the packet
    /// kind, which selects the permitted reason codes here and becomes the
    /// fixed header's type nibble in a later task; the body shape is the
    /// same for both.
    #[test]
    fn encodes_a_v5_pubrec_with_a_reason_and_a_property() {
        let ack = Ack {
            packet_id: 9,
            reason: ReasonCode::NoMatchingSubscribers,
            properties: Properties {
                reason_string: Some("no".into()),
                ..Default::default()
            },
        };

        let mut out = Vec::new();
        ack.encode_body(Version::V5, PacketKind::PubRec, &mut out)
            .unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x09, // packet identifier 9
                0x10, // reason code: no matching subscribers
                0x05, // property block length = 5 bytes
                0x1F, 0x00, 0x02, 0x6E, 0x6F, // reason string "no"
            ]
        );
    }

    /// Decodes a hand-built MQTT 5 PUBLISH, pinning the decoder to the
    /// specification's field layout independently of this crate's encoder.
    ///
    /// MQTT-v5.0 section 3.3.1: flags nibble 0x0B is `0b1011`: DUP (bit 3)
    /// set, QoS (bits 2-1) 0b01, RETAIN (bit 0) set. Section 3.3.2: topic
    /// name, then packet identifier, then properties; section 3.3.2.3.3
    /// makes the message expiry interval property identifier 2 (0x02)
    /// followed by a four-byte integer. Section 3.3.3: the payload is
    /// whatever remains.
    #[test]
    fn decodes_a_hand_built_v5_publish() {
        let wire = [
            0x00, 0x03, 0x64, 0x2F, 0x65, // topic "d/e"
            0x01, 0x02, // packet identifier 258
            0x05, // property block length = 5 bytes
            0x02, 0x00, 0x00, 0x00, 0x3C, // message expiry interval = 60
            0x70, 0x61, 0x79, // payload "pay"
        ];

        let mut reader = Reader::new(&wire);
        let decoded = Publish::decode_body(&mut reader, 0x0B, Version::V5, &Limits::default())
            .expect("decodes");

        assert!(decoded.dup);
        assert_eq!(decoded.qos, QoS::AtLeastOnce);
        assert!(decoded.retain);
        assert_eq!(decoded.topic, "d/e");
        assert_eq!(decoded.packet_id, Some(258));
        assert_eq!(
            decoded.properties,
            Properties {
                message_expiry_interval: Some(60),
                ..Default::default()
            }
        );
        assert_eq!(decoded.payload, Bytes::from_static(b"pay"));
    }

    /// Decodes a hand-built MQTT 5 PUBACK in its short form.
    ///
    /// MQTT-v5.0 section 3.4.2.1: a two-byte body means success with no
    /// properties. The decoder must accept it rather than demanding a
    /// reason code.
    #[test]
    fn decodes_a_hand_built_v5_puback_short_form() {
        let wire = [0x00, 0x2A]; // packet identifier 42, nothing else

        let mut reader = Reader::new(&wire);
        let decoded =
            Ack::decode_body(&mut reader, Version::V5, PacketKind::PubAck).expect("decodes");

        assert_eq!(decoded.packet_id, 42);
        assert_eq!(decoded.reason, ReasonCode::Success);
        assert_eq!(decoded.properties, Properties::default());
    }

    /// An MQTT 5 PUBLISH whose topic name is empty because a topic alias
    /// stands in for it.
    ///
    /// MQTT-v5.0 section 3.3.2.3.4: "If a Topic Alias mapping has been set
    /// at the receiver, a sender can send a PUBLISH packet that contains
    /// that Topic Alias and a zero length Topic Name." The topic is still
    /// a length-prefixed string, so it is two zero bytes; the alias is
    /// property identifier 35 (0x23) followed by a two-byte integer.
    #[test]
    fn encodes_a_v5_publish_with_an_empty_topic_and_an_alias() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: String::new(),
            packet_id: None,
            payload: Bytes::from_static(b"hi"),
            properties: Properties {
                topic_alias: Some(7),
                ..Default::default()
            },
        };

        assert_eq!(first_byte(&packet), 0x30);

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x00, // topic name: zero length
                0x03, // property block length = 3 bytes
                0x23, 0x00, 0x07, // topic alias = 7
                0x68, 0x69, // payload "hi"
            ]
        );

        // The same bytes decode back to the same shape, so this legal wire
        // form is pinned in both directions.
        let mut reader = Reader::new(&out);
        let decoded =
            Publish::decode_body(&mut reader, 0x00, Version::V5, &Limits::default()).unwrap();
        assert_eq!(decoded, packet);
    }
}

mod subscribe {
    // Independence property for this vector set: the subscription options
    // byte holds six settable bit positions -- maximum QoS (bits 1-0), no
    // local (bit 2), retain as published (bit 3) and retain handling (bits
    // 5-4), per MQTT-v5.0 section 3.8.3.1. A vector in which two of them
    // are always set together is invariant under swapping exactly those
    // two, and so cannot catch that swap; the connect vectors had that gap
    // for will-retain and password, and it had to be closed after the fact.
    //
    // `encodes_a_v5_subscribe_isolating_each_option_bit` closes it here in
    // one step: it carries six filters whose options bytes are 0x01, 0x04,
    // 0x08, 0x10, 0x20 and 0x02, so every one of the six bit positions
    // appears set in a byte where the other five are clear. For any pair of
    // positions there is therefore a byte holding exactly one of the two,
    // and swapping the pair changes that byte. That is strictly stronger
    // than the pairwise requirement.
    //
    // `encodes_a_v5_subscribe_with_a_subscription_identifier` then sets no
    // local, retain as published and retain handling 2 together (0x2D), to
    // pin the combination and the property block's position; on its own it
    // could not distinguish no local from retain as published, which is
    // precisely why the isolating vector above exists.

    use mqtt_codec::packet::{
        RetainHandling, SubAck, Subscribe, SubscriptionOptions, UnsubAck, Unsubscribe,
    };
    use mqtt_codec::reader::Reader;
    use mqtt_codec::{Limits, Properties, QoS, ReasonCode, Version};

    /// A filter with no option set beyond its quality of service.
    fn plain(filter: &str, qos: QoS) -> SubscriptionOptions {
        SubscriptionOptions {
            filter: filter.into(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAlways,
        }
    }

    /// An MQTT 3.1.1 SUBSCRIBE with two filters at different qualities of
    /// service, in the specification's own non-normative example.
    ///
    /// MQTT-v3.1.1 section 3.8.2: the variable header is the two-byte
    /// packet identifier, most significant byte first. Section 3.8.3: the
    /// payload is topic filter / requested QoS pairs "packed contiguously",
    /// each filter a UTF-8 encoded string per section 1.5.3. Figure 3.22
    /// puts QoS in bits 1-0 of the requested QoS byte and reserves the
    /// upper six bits, which MUST be zero [MQTT-3-8.3-4]. Table 3.5 and
    /// figure 3.23 give exactly this payload: "a/b" at 0x01 then "c/d" at
    /// 0x02.
    ///
    /// Filter order is part of the wire format, because SUBACK's reason
    /// codes MUST match it (section 3.9.3), so this vector pins it.
    #[test]
    fn encodes_a_v311_subscribe_with_two_filters() {
        let packet = Subscribe {
            packet_id: 10,
            filters: vec![
                plain("a/b", QoS::AtLeastOnce),
                plain("c/d", QoS::ExactlyOnce),
            ],
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x0A, // packet identifier 10
                0x00, 0x03, 0x61, 0x2F, 0x62, // topic filter "a/b"
                0x01, // requested QoS 1, upper six bits reserved and zero
                0x00, 0x03, 0x63, 0x2F, 0x64, // topic filter "c/d"
                0x02, // requested QoS 2, second in the payload
            ]
        );
    }

    /// An MQTT 5 SUBSCRIBE carrying a subscription identifier, with no
    /// local, retain as published and retain handling all set on its one
    /// filter.
    ///
    /// MQTT-v5.0 section 3.8.2: the variable header is the packet
    /// identifier then the properties, so the property block sits between
    /// the identifier and the payload. Section 3.8.2.1.2: the subscription
    /// identifier is property identifier 11 (0x0B) followed by a Variable
    /// Byte Integer, so the block is a length of 2 and then `0B 01`.
    ///
    /// Section 3.8.3.1 fixes the options byte: bits 1-0 are maximum QoS,
    /// bit 2 is no local, bit 3 is retain as published, bits 5-4 are retain
    /// handling and bits 7-6 are reserved. QoS 1, no local, retain as
    /// published and retain handling 2 is therefore
    /// `0x01 | 0x04 | 0x08 | (2 << 4)`, or 0b0010_1101 = 0x2D.
    #[test]
    fn encodes_a_v5_subscribe_with_a_subscription_identifier() {
        let packet = Subscribe {
            packet_id: 10,
            filters: vec![SubscriptionOptions {
                filter: "a/b".into(),
                qos: QoS::AtLeastOnce,
                no_local: true,
                retain_as_published: true,
                retain_handling: RetainHandling::DoNotSend,
            }],
            properties: Properties {
                subscription_identifiers: vec![1],
                ..Default::default()
            },
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x0A, // packet identifier 10
                0x02, // property block length = 2 bytes, before the payload
                0x0B, 0x01, // subscription identifier = 1
                0x00, 0x03, 0x61, 0x2F, 0x62, // topic filter "a/b"
                0x2D, // options, see above
            ]
        );
    }

    /// One MQTT 5 SUBSCRIBE whose six filters each set a single bit of the
    /// subscription options byte, with the other five clear.
    ///
    /// MQTT-v5.0 section 3.8.3.1, bit by bit: maximum QoS is bits 1-0, so
    /// QoS 1 is 0x01 and QoS 2 is 0x02; no local is bit 2, so 0x04; retain
    /// as published is bit 3, so 0x08; retain handling is bits 5-4, so the
    /// value 1 ("send retained messages at subscribe only if the
    /// subscription does not currently exist") is 0x10 and the value 2
    /// ("do not send retained messages at the time of the subscribe") is
    /// 0x20. Bits 7-6 are reserved and zero throughout.
    ///
    /// This is the vector that gives the set its independence property; see
    /// the note at the top of this module.
    #[test]
    fn encodes_a_v5_subscribe_isolating_each_option_bit() {
        let only = |filter: &str, qos, no_local, retain_as_published, retain_handling| {
            SubscriptionOptions {
                filter: filter.into(),
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            }
        };

        let packet = Subscribe {
            packet_id: 1,
            filters: vec![
                // Maximum QoS bit 0 alone.
                only(
                    "q/1",
                    QoS::AtLeastOnce,
                    false,
                    false,
                    RetainHandling::SendAlways,
                ),
                // No local alone.
                only(
                    "n/l",
                    QoS::AtMostOnce,
                    true,
                    false,
                    RetainHandling::SendAlways,
                ),
                // Retain as published alone.
                only(
                    "r/p",
                    QoS::AtMostOnce,
                    false,
                    true,
                    RetainHandling::SendAlways,
                ),
                // Retain handling bit 0 alone.
                only(
                    "h/1",
                    QoS::AtMostOnce,
                    false,
                    false,
                    RetainHandling::SendIfNew,
                ),
                // Retain handling bit 1 alone.
                only(
                    "h/2",
                    QoS::AtMostOnce,
                    false,
                    false,
                    RetainHandling::DoNotSend,
                ),
                // Maximum QoS bit 1 alone.
                only(
                    "q/2",
                    QoS::ExactlyOnce,
                    false,
                    false,
                    RetainHandling::SendAlways,
                ),
            ],
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x01, // packet identifier 1
                0x00, // property block: empty
                0x00, 0x03, 0x71, 0x2F, 0x31, 0x01, // "q/1", QoS bit 0 only
                0x00, 0x03, 0x6E, 0x2F, 0x6C, 0x04, // "n/l", no local only
                0x00, 0x03, 0x72, 0x2F, 0x70, 0x08, // "r/p", retain as published only
                0x00, 0x03, 0x68, 0x2F, 0x31, 0x10, // "h/1", retain handling 1 only
                0x00, 0x03, 0x68, 0x2F, 0x32, 0x20, // "h/2", retain handling 2 only
                0x00, 0x03, 0x71, 0x2F, 0x32, 0x02, // "q/2", QoS bit 1 only
            ]
        );
    }

    /// The same fully-optioned v5 subscription encoded as MQTT 3.1.1, which
    /// keeps only the quality of service.
    ///
    /// MQTT-v3.1.1 section 3.8.3 has no no-local, retain-as-published or
    /// retain-handling option, and reserves the upper six bits of the
    /// requested QoS byte; a server MUST treat the packet as malformed if
    /// any reserved bit is non-zero [MQTT-3-8.3-4]. The degradation is
    /// therefore lossy by necessity, and this vector pins it: the options
    /// byte is the bare QoS, not 0x2D.
    #[test]
    fn degrading_a_v5_subscription_to_v311_drops_the_v5_only_options() {
        let packet = Subscribe {
            packet_id: 10,
            filters: vec![SubscriptionOptions {
                filter: "a/b".into(),
                qos: QoS::AtLeastOnce,
                no_local: true,
                retain_as_published: true,
                retain_handling: RetainHandling::DoNotSend,
            }],
            properties: Properties {
                subscription_identifiers: vec![1],
                ..Default::default()
            },
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x0A, // packet identifier 10, and no property block
                0x00, 0x03, 0x61, 0x2F, 0x62, // topic filter "a/b"
                0x01, // requested QoS 1 alone: the v5-only options are gone
            ]
        );
    }

    /// An MQTT 3.1.1 SUBACK granting one subscription and failing another.
    ///
    /// MQTT-v3.1.1 section 3.9.2: the variable header is the packet
    /// identifier from the SUBSCRIBE being acknowledged. Section 3.9.3: the
    /// payload is one return code per topic filter, in the same order, and
    /// the only permitted values are 0x00, 0x01 and 0x02 for success at
    /// maximum QoS 0, 1 and 2, and 0x80 for failure. This version has one
    /// failure value, so every v5 error code maps onto 0x80.
    #[test]
    fn encodes_a_v311_suback_with_a_grant_and_a_failure() {
        let packet = SubAck {
            packet_id: 10,
            reasons: vec![ReasonCode::GrantedQos2, ReasonCode::NotAuthorized],
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x0A, // packet identifier 10
                0x02, // success, maximum QoS 2
                0x80, // failure: the only failure value this version has
            ]
        );
    }

    /// An MQTT 5 SUBACK with a property, pinning where the property block
    /// sits relative to the reason codes.
    ///
    /// MQTT-v5.0 section 3.9.2: "The Variable Header of the SUBACK Packet
    /// contains the following fields in the order: the Packet Identifier
    /// from the SUBSCRIBE Packet that is being acknowledged, and
    /// Properties." Section 3.9.3 then puts the reason codes in the
    /// payload, after the variable header, one per topic filter and in the
    /// same order [MQTT-3.9.3-1]. So the block is in the middle, not at the
    /// end. Section 3.9.2.1.2: the reason string is property identifier 31
    /// (0x1F) followed by a UTF-8 string, so the block is 5 bytes.
    ///
    /// Table 3-8 gives 0x01 for "Granted QoS 1" and 0x80 for "Unspecified
    /// error"; unlike 3.1.1, this version keeps the distinct error codes.
    #[test]
    fn encodes_a_v5_suback_with_a_property_before_the_reason_codes() {
        let packet = SubAck {
            packet_id: 10,
            reasons: vec![ReasonCode::GrantedQos1, ReasonCode::UnspecifiedError],
            properties: Properties {
                reason_string: Some("no".into()),
                ..Default::default()
            },
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x0A, // packet identifier 10
                0x05, // property block length = 5 bytes, before the payload
                0x1F, 0x00, 0x02, 0x6E, 0x6F, // reason string "no"
                0x01, // granted QoS 1
                0x80, // unspecified error
            ]
        );
    }

    /// An MQTT 3.1.1 UNSUBSCRIBE with two filters.
    ///
    /// MQTT-v3.1.1 section 3.10.2: the variable header is the packet
    /// identifier. Section 3.10.3: the payload is the list of topic
    /// filters, UTF-8 encoded strings packed contiguously, with no options
    /// byte between them -- that is the whole difference from SUBSCRIBE's
    /// payload. Figure 3.30 uses "a/b" and "c/d".
    #[test]
    fn encodes_a_v311_unsubscribe() {
        let packet = Unsubscribe {
            packet_id: 10,
            filters: vec!["a/b".into(), "c/d".into()],
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x0A, // packet identifier 10
                0x00, 0x03, 0x61, 0x2F, 0x62, // topic filter "a/b"
                0x00, 0x03, 0x63, 0x2F, 0x64, // topic filter "c/d", no options byte
            ]
        );
    }

    /// An MQTT 5 UNSUBSCRIBE, which inserts a property block.
    ///
    /// MQTT-v5.0 section 3.10.2: the variable header is the packet
    /// identifier then the properties. Section 3.10.3: the payload is the
    /// topic filters, packed contiguously, and MUST contain at least one
    /// [MQTT-3.10.3-2]. With no properties the block is a single zero
    /// length byte, which is the only difference from the 3.1.1 bytes
    /// above.
    #[test]
    fn encodes_a_v5_unsubscribe() {
        let packet = Unsubscribe {
            packet_id: 10,
            filters: vec!["a/b".into(), "c/d".into()],
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x0A, // packet identifier 10
                0x00, // property block: empty
                0x00, 0x03, 0x61, 0x2F, 0x62, // topic filter "a/b"
                0x00, 0x03, 0x63, 0x2F, 0x64, // topic filter "c/d"
            ]
        );
    }

    /// An MQTT 3.1.1 UNSUBACK: the packet identifier and nothing else.
    ///
    /// MQTT-v3.1.1 section 3.11.2: "The variable header contains the Packet
    /// Identifier of the UNSUBSCRIBE Packet that is being acknowledged."
    /// Section 3.11.3: "The UNSUBACK Packet has no payload." Section 3.11.1
    /// fixes the remaining length at 2, so the body is exactly two bytes.
    ///
    /// The reason codes below are therefore dropped, not encoded. Anything
    /// longer than two bytes would be unreadable to a 3.1.1 client.
    #[test]
    fn encodes_a_v311_unsuback_as_exactly_the_identifier() {
        let packet = UnsubAck {
            packet_id: 10,
            reasons: vec![ReasonCode::Success, ReasonCode::NoSubscriptionExisted],
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(out, vec![0x00, 0x0A]); // packet identifier 10, and nothing else
    }

    /// An MQTT 5 UNSUBACK carrying one reason code per filter.
    ///
    /// MQTT-v5.0 section 3.11.2: the variable header is the packet
    /// identifier then the properties. Section 3.11.3: "The Payload of the
    /// UNSUBACK packet contains a list of Reason Codes", one per topic
    /// filter and in the same order [MQTT-3.11.3-1]. Table 3-9 gives 0x00
    /// for "Success" and 0x11 for "No subscription existed" -- the latter
    /// has no 3.1.1 equivalent, which is why that version sends none.
    #[test]
    fn encodes_a_v5_unsuback_with_reason_codes() {
        let packet = UnsubAck {
            packet_id: 10,
            reasons: vec![ReasonCode::Success, ReasonCode::NoSubscriptionExisted],
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x00, 0x0A, // packet identifier 10
                0x00, // property block: empty
                0x00, // success
                0x11, // no subscription existed
            ]
        );
    }

    /// Decodes a hand-built MQTT 5 SUBSCRIBE, pinning the decoder to the
    /// specification's layout independently of this crate's encoder.
    ///
    /// MQTT-v5.0 section 3.8.2: packet identifier, then the property block;
    /// section 3.8.2.1.2 makes the subscription identifier property 11
    /// (0x0B) followed by a Variable Byte Integer. Section 3.8.3: the
    /// payload is filter / options pairs packed contiguously.
    ///
    /// The two options bytes are read with section 3.8.3.1 in hand. 0x05 is
    /// 0b0000_0101: QoS bits 1-0 are 0b01, no local (bit 2) is set, retain
    /// as published (bit 3) is clear, retain handling (bits 5-4) is 0. 0x2A
    /// is 0b0010_1010: QoS bits 1-0 are 0b10, no local is clear, retain as
    /// published is set, retain handling (bits 5-4) is 0b10, or 2.
    ///
    /// The independence property applies on this side too, and these bytes
    /// were chosen for it: no local is set here only where retain as
    /// published is clear, and vice versa, so swapping the two masks in the
    /// decoder alone changes what this vector decodes to. An earlier draft
    /// used 0x0D and 0x22, which set both bits together and then neither,
    /// and so was invariant under exactly that swap.
    #[test]
    fn decodes_a_hand_built_v5_subscribe() {
        let wire = [
            0x00, 0x2A, // packet identifier 42
            0x02, // property block length = 2 bytes
            0x0B, 0x05, // subscription identifier = 5
            0x00, 0x03, 0x61, 0x2F, 0x62, // topic filter "a/b"
            0x05, // options: QoS 1, no local, retain as published clear
            0x00, 0x03, 0x63, 0x2F, 0x64, // topic filter "c/d"
            0x2A, // options: QoS 2, retain as published, retain handling 2
        ];

        let mut reader = Reader::new(&wire);
        let decoded =
            Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).expect("decodes");

        assert_eq!(decoded.packet_id, 42);
        assert_eq!(decoded.properties.subscription_identifiers, vec![5]);

        assert_eq!(decoded.filters.len(), 2);
        assert_eq!(
            decoded.filters[0],
            SubscriptionOptions {
                filter: "a/b".into(),
                qos: QoS::AtLeastOnce,
                no_local: true,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAlways,
            }
        );
        assert_eq!(
            decoded.filters[1],
            SubscriptionOptions {
                filter: "c/d".into(),
                qos: QoS::ExactlyOnce,
                no_local: false,
                retain_as_published: true,
                retain_handling: RetainHandling::DoNotSend,
            }
        );
    }
}

mod control {
    // DISCONNECT and AUTH both have a legal zero-length body, and that is
    // the property this vector set exists to pin. A round trip cannot see
    // the difference between "the short form was used" and "the long form
    // was used and read back", because both decode to the same value; only
    // literal bytes can.
    //
    // PINGREQ and PINGRESP have no body at all, so there is nothing for an
    // encoder vector to pin here. Their whole wire form is the fixed
    // header, so they are pinned below through `PacketKind::nibble` and
    // `header::peek`, and in the encode direction by
    // `frame::encodes_pingreq_and_pingresp_as_exactly_two_bytes`, which
    // asserts `0xC0 0x00` and `0xD0 0x00` against the fixed-header encoder
    // that arrived with the packet enum.

    use bytes::Bytes;
    use mqtt_codec::header::{self, PacketKind};
    use mqtt_codec::packet::{Auth, Disconnect};
    use mqtt_codec::reader::Reader;
    use mqtt_codec::{Limits, Properties, ReasonCode, Version};

    /// An MQTT 3.1.1 DISCONNECT has an entirely empty body.
    ///
    /// MQTT-v3.1.1 section 3.14.2: "The DISCONNECT Packet has no variable
    /// header." Section 3.14.3: "The DISCONNECT Packet has no payload."
    /// Figure 3.35 fixes the whole packet at two bytes, the second being a
    /// remaining length of 0. So there is no place on the wire for a reason
    /// code or a property block, and both are dropped rather than encoded:
    /// this vector sets both and still expects nothing.
    #[test]
    fn encodes_a_v311_disconnect_as_an_empty_body() {
        let packet = Disconnect {
            reason: ReasonCode::SessionTakenOver,
            properties: Properties {
                reason_string: Some("dropped".into()),
                ..Default::default()
            },
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();

        assert_eq!(out, Vec::<u8>::new());
    }

    /// An MQTT 5 normal disconnection is also an empty body.
    ///
    /// MQTT-v5.0 section 3.14.2.1: "Byte 1 in the Variable Header is the
    /// Disconnect Reason Code. If the Remaining Length is less than 1 the
    /// value of 0x00 (Normal disconnection) is used." Section 3.14.2.2.1
    /// adds: "If the Remaining Length is less than 2, a value of 0 is
    /// used" for the property length. Together those make a zero-length
    /// body the canonical spelling of "success, no properties", so the
    /// encoder must not emit the two bytes `0x00 0x00` that would say the
    /// same thing the long way.
    #[test]
    fn encodes_a_v5_normal_disconnection_as_an_empty_body() {
        let packet = Disconnect {
            reason: ReasonCode::Success,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(out, Vec::<u8>::new());
    }

    /// An MQTT 5 DISCONNECT with a reason code and nothing else.
    ///
    /// MQTT-v5.0 section 3.14.2: "The Variable Header of the DISCONNECT
    /// Packet contains the following fields in the order: Disconnect
    /// Reason Code, and Properties." Table 3-10 gives 0x8D for "Keep Alive
    /// timeout", sent by the server. Section 2.2.2.1 makes the property
    /// length a Variable Byte Integer that is present whenever the
    /// properties are, and zero when the set is empty, so the long form of
    /// this packet is two bytes: the reason code, then a property length
    /// of 0.
    ///
    /// The short form of section 3.14.2.2.1 -- the reason byte alone, with
    /// the property length omitted -- is also legal, and is pinned in the
    /// decode direction by `decodes_a_v5_disconnect_that_omits_the_
    /// property_length` below. This codec reads both and writes this one.
    #[test]
    fn encodes_a_v5_disconnect_with_a_reason_and_no_properties() {
        let packet = Disconnect {
            reason: ReasonCode::KeepAliveTimeout,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x8D, // keep alive timeout
                0x00, // property block: empty
            ]
        );
    }

    /// An MQTT 5 DISCONNECT carrying a reason code and one property.
    ///
    /// MQTT-v5.0 table 3-10 gives 0x9D for "Server moved". Section
    /// 3.14.2.2.5: "28 (0x1C) Byte, Identifier of the Server Reference.
    /// Followed by a UTF-8 Encoded String which can be used by the Client
    /// to identify another Server to use", and "The Server sends
    /// DISCONNECT including a Server Reference and Reason Code 0x9C (Use
    /// another server) or 0x9D (Server moved)". So this is the pairing the
    /// specification names, not an arbitrary one.
    ///
    /// The block is the identifier byte plus a UTF-8 Encoded String, which
    /// section 1.5.4 length-prefixes with two bytes: 1 + 2 + 13 = 16, or
    /// 0x10.
    ///
    /// A caution for whoever extends this set. The specification's own
    /// figure 3-24, the DISCONNECT variable header non-normative example,
    /// labels its property length "Length (5)" but draws the bit row as
    /// 0000 0111, or 7, for a block that is structurally 5 bytes: one
    /// identifier byte plus a Four Byte Integer. The label and the
    /// structure agree; the bit row is wrong. Derive property lengths by
    /// counting the encoded block, not by reading that figure.
    #[test]
    fn encodes_a_v5_disconnect_with_a_reason_and_a_server_reference() {
        let packet = Disconnect {
            reason: ReasonCode::ServerMoved,
            properties: Properties {
                server_reference: Some("b.example.com".into()),
                ..Default::default()
            },
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x9D, // server moved
                0x10, // property block length = 16 bytes
                0x1C, // server reference identifier (28)
                0x00, 0x0D, // string length = 13
                0x62, 0x2E, 0x65, 0x78, 0x61, 0x6D, 0x70, 0x6C, 0x65, 0x2E, 0x63, 0x6F,
                0x6D, // "b.example.com"
            ]
        );
    }

    /// An MQTT 5 AUTH continuing an authentication exchange.
    ///
    /// MQTT-v5.0 section 3.15.2: "The Variable Header of the AUTH Packet
    /// contains the following fields in the order: Authenticate Reason
    /// Code, and Properties." Table 3-11 gives 0x18 for "Continue
    /// authentication", sent by client or server. Section 3.15.2.2.2: "21
    /// (0x15) Byte, Identifier of the Authentication Method. Followed by a
    /// UTF-8 Encoded String". Section 3.15.2.2.3: "22 (0x16) Byte,
    /// Identifier of the Authentication Data. Followed by Binary Data",
    /// which section 1.5.6 also length-prefixes with two bytes. Section
    /// 3.15.3: "The AUTH packet has no Payload", so the body ends with the
    /// property block.
    ///
    /// Block length: method is 1 + 2 + 13 = 16, data is 1 + 2 + 7 = 10,
    /// total 26, or 0x1A. The specification does not fix the order of
    /// properties within a block (section 2.2.2); this codec emits them in
    /// ascending identifier order, which puts the method before the data,
    /// and that is what the bytes below pin.
    #[test]
    fn encodes_a_v5_auth_continuing_authentication() {
        let packet = Auth {
            reason: ReasonCode::ContinueAuthentication,
            properties: Properties {
                authentication_method: Some("SCRAM-SHA-256".into()),
                authentication_data: Some(Bytes::from_static(b"r=nonce")),
                ..Default::default()
            },
        };

        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(
            out,
            vec![
                0x18, // continue authentication
                0x1A, // property block length = 26 bytes
                0x15, // authentication method identifier (21)
                0x00, 0x0D, // string length = 13
                0x53, 0x43, 0x52, 0x41, 0x4D, 0x2D, 0x53, 0x48, 0x41, 0x2D, 0x32, 0x35,
                0x36, // "SCRAM-SHA-256"
                0x16, // authentication data identifier (22)
                0x00, 0x07, // binary data length = 7
                0x72, 0x3D, 0x6E, 0x6F, 0x6E, 0x63, 0x65, // "r=nonce"
            ]
        );
    }

    /// An empty MQTT 5 DISCONNECT body decodes as a normal disconnection.
    ///
    /// MQTT-v5.0 section 3.14.2.1: "If the Remaining Length is less than 1
    /// the value of 0x00 (Normal disconnection) is used." Pinned from
    /// literal bytes -- an empty slice -- rather than from this crate's
    /// encoder, so that the decoder keeps reading the short form even if
    /// the encoder stopped writing it.
    #[test]
    fn decodes_an_empty_v5_disconnect_body_as_a_normal_disconnection() {
        let wire: [u8; 0] = [];

        let mut reader = Reader::new(&wire);
        let decoded = Disconnect::decode_body(&mut reader, Version::V5).expect("decodes");

        assert_eq!(decoded.reason, ReasonCode::Success);
        assert_eq!(decoded.properties, Properties::default());
    }

    /// A one-byte MQTT 5 DISCONNECT body: a reason code with the property
    /// length omitted.
    ///
    /// MQTT-v5.0 section 3.14.2.2.1: "The length of Properties in the
    /// DISCONNECT packet Variable Header encoded as a Variable Byte
    /// Integer. If the Remaining Length is less than 2, a value of 0 is
    /// used." So a body of exactly one byte is the reason code and an
    /// implied empty property block, and must not be read as a reason code
    /// followed by a truncated property length. Table 3-10 gives 0x8D for
    /// "Keep Alive timeout".
    #[test]
    fn decodes_a_v5_disconnect_that_omits_the_property_length() {
        let wire = [0x8D]; // keep alive timeout, property length omitted

        let mut reader = Reader::new(&wire);
        let decoded = Disconnect::decode_body(&mut reader, Version::V5).expect("decodes");

        assert_eq!(decoded.reason, ReasonCode::KeepAliveTimeout);
        assert_eq!(decoded.properties, Properties::default());
    }

    /// Decodes a hand-built MQTT 5 DISCONNECT, pinning the decoder's
    /// layout independently of this crate's encoder.
    ///
    /// MQTT-v5.0 section 3.14.2: reason code, then the property block.
    /// Table 3-10 gives 0x98 for "Administrative action". Section
    /// 3.14.2.2.3: "31 (0x1F) Byte, Identifier of the Reason String.
    /// Followed by the UTF-8 Encoded String". Block length: 1 + 2 + 3 = 6.
    #[test]
    fn decodes_a_hand_built_v5_disconnect() {
        let wire = [
            0x98, // administrative action
            0x06, // property block length = 6 bytes
            0x1F, // reason string identifier (31)
            0x00, 0x03, 0x62, 0x79, 0x65, // "bye"
        ];

        let mut reader = Reader::new(&wire);
        let decoded = Disconnect::decode_body(&mut reader, Version::V5).expect("decodes");

        assert_eq!(decoded.reason, ReasonCode::AdministrativeAction);
        assert_eq!(decoded.properties.reason_string.as_deref(), Some("bye"));
    }

    /// An empty AUTH body decodes as success.
    ///
    /// MQTT-v5.0 section 3.15.2.1: "The Reason Code and Property Length
    /// can be omitted if the Reason Code is 0x00 (Success) and there are
    /// no Properties. In this case the AUTH has a Remaining Length of 0."
    /// Pinned from literal bytes so that the meaning of a zero-length AUTH
    /// body is fixed here and not inferred from the encoder.
    #[test]
    fn decodes_an_empty_auth_body_as_success() {
        let wire: [u8; 0] = [];

        let mut reader = Reader::new(&wire);
        let decoded = Auth::decode_body(&mut reader).expect("decodes");

        assert_eq!(decoded.reason, ReasonCode::Success);
        assert_eq!(decoded.properties, Properties::default());
    }

    /// Decodes a hand-built MQTT 5 AUTH, pinning the decoder's layout
    /// independently of this crate's encoder.
    ///
    /// MQTT-v5.0 section 3.15.2: reason code, then the property block.
    /// Table 3-11 gives 0x19 for "Re-authenticate", sent by the client.
    /// Section 3.15.2.2.2 makes the authentication method identifier 21
    /// (0x15) followed by a UTF-8 Encoded String; section 3.15.2.2.3 makes
    /// the authentication data identifier 22 (0x16) followed by Binary
    /// Data. Block length: method is 1 + 2 + 6 = 9, data is 1 + 2 + 4 = 7,
    /// total 16, or 0x10.
    ///
    /// The data below is non-UTF-8 on purpose: section 1.5.6 Binary Data
    /// has no character encoding, so a decoder that reads it as a string
    /// must fail here rather than pass.
    #[test]
    fn decodes_a_hand_built_v5_auth() {
        let wire = [
            0x19, // re-authenticate
            0x10, // property block length = 16 bytes
            0x15, // authentication method identifier (21)
            0x00, 0x06, 0x6D, 0x65, 0x74, 0x68, 0x6F, 0x64, // "method"
            0x16, // authentication data identifier (22)
            0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF, // four bytes, not UTF-8
        ];

        let mut reader = Reader::new(&wire);
        let decoded = Auth::decode_body(&mut reader).expect("decodes");

        assert_eq!(decoded.reason, ReasonCode::ReAuthenticate);
        assert_eq!(
            decoded.properties.authentication_method.as_deref(),
            Some("method")
        );
        assert_eq!(
            decoded.properties.authentication_data,
            Some(Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF]))
        );
    }

    /// The fixed-header first byte of each packet this module covers.
    ///
    /// MQTT-v5.0 figure 3.33 for PINGREQ draws byte 1 as 1100 0000, with
    /// the control packet type (12) in bits 7-4 and bits 3-0 reserved and
    /// zero, and byte 2 as a remaining length of 0. Figure 3.34 draws
    /// PINGRESP as 1101 0000 the same way, type 13. Section 3.14.1 draws
    /// DISCONNECT as 1110 0000, type 14, and section 3.15.1 draws AUTH as
    /// 1111 0000, type 15, both with bits 3-0 reserved and zero
    /// [MQTT-3.14.1-1], [MQTT-3.15.1-1]. MQTT-v3.1.1 figure 3.35 draws its
    /// DISCONNECT identically, with a remaining length fixed at 0.
    ///
    /// Only the decode direction is pinned here. The encode direction now
    /// lives in the `frame` module, which asserts `0xC0 0x00` and
    /// `0xD0 0x00` against the fixed-header encoder directly.
    #[test]
    fn pins_the_fixed_header_first_byte_of_each_control_packet() {
        assert_eq!(PacketKind::PingReq.nibble() << 4, 0xC0);
        assert_eq!(PacketKind::PingResp.nibble() << 4, 0xD0);
        assert_eq!(PacketKind::Disconnect.nibble() << 4, 0xE0);
        assert_eq!(PacketKind::Auth.nibble() << 4, 0xF0);

        // A whole PINGREQ and a whole PINGRESP, read back from the bytes
        // the next task's encoder must produce.
        for (wire, kind) in [
            ([0xC0, 0x00], PacketKind::PingReq),
            ([0xD0, 0x00], PacketKind::PingResp),
        ] {
            let parsed = header::peek(&wire, &Limits::default())
                .expect("well formed")
                .expect("complete");
            assert_eq!(parsed.kind, kind);
            assert_eq!(parsed.flags, 0);
            assert_eq!(parsed.body_len, 0);
            assert_eq!(parsed.total_len(), 2);
        }
    }
}

mod frame {
    // What this module pins that the per-packet modules cannot: those
    // vectors all assert a *body*, because until the packet enum landed
    // there was no encoder for the fixed header. The two bytes in front of
    // every body -- the type nibble, the reserved flags nibble, and the
    // variable byte integer holding the remaining length -- were therefore
    // pinned in the decode direction only, through `header::peek`. A type
    // nibble read from and written to the same wrong value would have
    // round tripped perfectly and been wrong against every other MQTT
    // implementation, which is the hazard golden vectors exist to catch.
    //
    // The first byte of all fifteen packet types is pinned below, along
    // with the remaining-length encoding either side of the one-byte
    // boundary, all derived from the specification's own tables.

    use bytes::{Bytes, BytesMut};
    use mqtt_codec::packet::{
        Ack, Auth, ConnAck, Connect, Disconnect, Publish, RetainHandling, SubAck, Subscribe,
        SubscriptionOptions, UnsubAck, Unsubscribe,
    };
    use mqtt_codec::{decode, encode, Limits, Packet, QoS, ReasonCode, Version};

    /// One legal packet of every type, each the smallest form that decodes
    /// back to exactly what was encoded, paired with the whole first byte
    /// the specification requires for it.
    ///
    /// MQTT-v5.0 Table 2-1 fixes the type value in bits 7-4: CONNECT 1,
    /// CONNACK 2, PUBLISH 3, PUBACK 4, PUBREC 5, PUBREL 6, PUBCOMP 7,
    /// SUBSCRIBE 8, SUBACK 9, UNSUBSCRIBE 10, UNSUBACK 11, PINGREQ 12,
    /// PINGRESP 13, DISCONNECT 14, AUTH 15. Table 2-2 fixes bits 3-0: they
    /// are reserved and zero for every type except PUBLISH, which carries
    /// DUP, QoS and RETAIN there, and PUBREL, SUBSCRIBE and UNSUBSCRIBE,
    /// whose reserved nibble is 0,0,1,0 rather than all zero.
    ///
    /// The PUBLISH here is QoS 1 with neither DUP nor RETAIN, so its low
    /// nibble is 0b0010 by section 3.3.1.2 and its first byte is 0x32. The
    /// flag bits themselves are pinned in the `publish` module; what is
    /// pinned here is that the enum puts the packet's own flags into the
    /// nibble rather than zero.
    fn one_of_each() -> Vec<(Packet, u8)> {
        vec![
            (
                Packet::Connect(Connect {
                    version: Version::V5,
                    client_id: "c".into(),
                    clean_start: true,
                    keep_alive: 10,
                    username: None,
                    password: None,
                    will: None,
                    properties: Default::default(),
                }),
                0x10,
            ),
            (
                Packet::ConnAck(ConnAck {
                    session_present: false,
                    reason: ReasonCode::Success,
                    properties: Default::default(),
                }),
                0x20,
            ),
            (
                Packet::Publish(Publish {
                    dup: false,
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    topic: "a/b".into(),
                    packet_id: Some(1),
                    payload: Bytes::from_static(b"hello"),
                    properties: Default::default(),
                }),
                0x32,
            ),
            (Packet::PubAck(ack()), 0x40),
            (Packet::PubRec(ack()), 0x50),
            (Packet::PubRel(ack()), 0x62),
            (Packet::PubComp(ack()), 0x70),
            (
                Packet::Subscribe(Subscribe {
                    packet_id: 1,
                    filters: vec![SubscriptionOptions {
                        filter: "a/b".into(),
                        qos: QoS::AtMostOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: RetainHandling::SendAlways,
                    }],
                    properties: Default::default(),
                }),
                0x82,
            ),
            (
                Packet::SubAck(SubAck {
                    packet_id: 1,
                    reasons: vec![ReasonCode::Success],
                    properties: Default::default(),
                }),
                0x90,
            ),
            (
                Packet::Unsubscribe(Unsubscribe {
                    packet_id: 1,
                    filters: vec!["a/b".into()],
                    properties: Default::default(),
                }),
                0xA2,
            ),
            (
                Packet::UnsubAck(UnsubAck {
                    packet_id: 1,
                    reasons: vec![ReasonCode::Success],
                    properties: Default::default(),
                }),
                0xB0,
            ),
            (Packet::PingReq, 0xC0),
            (Packet::PingResp, 0xD0),
            (
                Packet::Disconnect(Disconnect {
                    reason: ReasonCode::Success,
                    properties: Default::default(),
                }),
                0xE0,
            ),
            (
                Packet::Auth(Auth {
                    reason: ReasonCode::Success,
                    properties: Default::default(),
                }),
                0xF0,
            ),
        ]
    }

    /// A plain successful acknowledgement, legal on all four ack types.
    fn ack() -> Ack {
        Ack {
            packet_id: 1,
            reason: ReasonCode::Success,
            properties: Default::default(),
        }
    }

    /// The first byte of the fixed header, for every packet type.
    ///
    /// Derived from MQTT-v5.0 Table 2-1 and Table 2-2, quoted in the doc
    /// comment on `one_of_each`. MQTT-v3.1.1 Table 2.1, "Control packet
    /// types", and Table 2.2, "Flag Bits", give the same values for the
    /// fourteen types that version has.
    #[test]
    fn pins_the_fixed_header_first_byte_of_every_packet_type() {
        for (packet, first) in one_of_each() {
            let mut buf = BytesMut::new();
            encode(&packet, Version::V5, &mut buf).unwrap();
            assert_eq!(
                buf[0],
                first,
                "wrong first byte for {:?}: got {:#04X}, want {first:#04X}",
                packet.kind(),
                buf[0]
            );
        }
    }

    /// PUBLISH puts its own flags in the low nibble rather than zero.
    ///
    /// MQTT-v5.0 section 3.3.1: bit 3 is DUP, bits 2-1 are QoS and bit 0 is
    /// RETAIN. A QoS 0 publish with neither DUP nor RETAIN therefore has an
    /// all-zero low nibble and a first byte of 0x30, while the QoS 1
    /// publish in `one_of_each` has 0x32. Both are asserted so that an
    /// encoder which hard-coded either value is caught.
    #[test]
    fn pins_the_publish_flags_nibble_in_the_first_byte() {
        let mut buf = BytesMut::new();
        encode(
            &Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: false,
                topic: "a/b".into(),
                packet_id: None,
                payload: Bytes::from_static(b"hi"),
                properties: Default::default(),
            }),
            Version::V311,
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], 0x30);
    }

    /// PINGREQ is exactly `C0 00` and PINGRESP exactly `D0 00`.
    ///
    /// MQTT-v3.1.1 figure 3.33 draws the PINGREQ fixed header as 1100 0000
    /// followed by a remaining length of 0, and figure 3.34 draws PINGRESP
    /// as 1101 0000 followed by a remaining length of 0. MQTT-v5.0 figures
    /// 3.33 and 3.34, in sections 3.12.1 and 3.13.1, are identical. Neither
    /// packet has a variable header or a payload, so those two bytes are
    /// the whole packet, and an encoder that appended anything at all would
    /// be wrong. The `control` module pinned these in the decode direction
    /// and left the encode direction for the fixed-header encoder; this is
    /// it.
    #[test]
    fn encodes_pingreq_and_pingresp_as_exactly_two_bytes() {
        let mut buf = BytesMut::new();
        encode(&Packet::PingReq, Version::V311, &mut buf).unwrap();
        assert_eq!(&buf[..], &[0xC0, 0x00]);

        let mut buf = BytesMut::new();
        encode(&Packet::PingResp, Version::V311, &mut buf).unwrap();
        assert_eq!(&buf[..], &[0xD0, 0x00]);
    }

    /// The remaining length either side of the one-byte boundary.
    ///
    /// MQTT-v5.0 section 2.1.4 makes the Remaining Length a Variable Byte
    /// Integer, and Table 1-1, "Size of Variable Byte Integer", in section
    /// 1.5.5 gives the ranges each digit count covers: one byte encodes 0
    /// (0x00) through 127 (0x7F), and two bytes encode 128 (0x80, 0x01)
    /// through 16,383 (0xFF, 0x7F). So a 127-byte body is preceded by the
    /// single byte 0x7F and a 128-byte body by the pair 0x80 0x01 --
    /// continuation bit set on the first, clear on the second, least
    /// significant group first. MQTT-v3.1.1 section 2.2.3, Table 2.4,
    /// "Size of Remaining Length field", is the same table.
    ///
    /// A QoS 0 PUBLISH to "a/b" as MQTT 3.1.1 has a body of two length
    /// bytes, three topic bytes and the payload, so payloads of 122 and 123
    /// bytes land exactly on either side of the boundary.
    #[test]
    fn pins_the_remaining_length_across_the_one_byte_boundary() {
        for (payload_len, length_bytes) in [(122usize, vec![0x7Fu8]), (123, vec![0x80, 0x01])] {
            let payload = vec![0xAB; payload_len];
            let mut buf = BytesMut::new();
            encode(
                &Packet::Publish(Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: "a/b".into(),
                    packet_id: None,
                    payload: Bytes::from(payload.clone()),
                    properties: Default::default(),
                }),
                Version::V311,
                &mut buf,
            )
            .unwrap();

            let mut expected = vec![0x30];
            expected.extend_from_slice(&length_bytes);
            expected.extend_from_slice(&[0x00, 0x03, b'a', b'/', b'b']);
            expected.extend_from_slice(&payload);
            assert_eq!(&buf[..], &expected[..]);
            assert_eq!(buf.len(), 1 + length_bytes.len() + 5 + payload_len);
        }
    }

    /// Every packet type survives the two public entry points unchanged.
    ///
    /// This is the round trip the per-packet modules could not run, because
    /// they call `encode_body` and `decode_body` directly and so never see
    /// a fixed header. Here the whole frame goes out and comes back, and
    /// the buffer must be empty afterwards: a length that was written one
    /// byte short or one byte long leaves the buffer non-empty or fails to
    /// decode, and neither the body vectors nor the header vectors above
    /// would notice on their own.
    #[test]
    fn round_trips_every_packet_type_through_the_entry_points() {
        for (packet, _) in one_of_each() {
            let mut buf = BytesMut::new();
            encode(&packet, Version::V5, &mut buf).unwrap();

            let decoded = decode(&mut buf, Version::V5, &Limits::default())
                .unwrap()
                .unwrap_or_else(|| panic!("a whole frame for {:?}", packet.kind()));
            assert_eq!(decoded, packet);
            assert!(
                buf.is_empty(),
                "{:?} left {} bytes behind",
                packet.kind(),
                buf.len()
            );
        }
    }
}
