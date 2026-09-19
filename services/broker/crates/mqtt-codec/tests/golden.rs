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
    use mqtt_codec::{Limits, Properties, QoS, ReasonCode, Version};

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
    /// identifier from the PUBLISH being acknowledged, and 3.4.1 fixes the
    /// first byte at 0x40 with no reason code or properties in this version.
    #[test]
    fn encodes_a_v311_puback() {
        let ack = Ack {
            packet_id: 7,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        };

        let mut out = Vec::new();
        ack.encode_body(Version::V311, &mut out).unwrap();

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
        ack.encode_body(Version::V5, &mut out).unwrap();

        assert_eq!(out, vec![0x00, 0x07]); // packet identifier 7, nothing else
    }

    /// An MQTT 5 PUBREC with a failure reason and a property: the long form.
    ///
    /// MQTT-v5.0 section 3.5.2: PUBREC is packet identifier, reason code,
    /// then properties. Section 3.5.2.1: reason code 0x10 is "No matching
    /// subscribers". Section 3.5.2.2.2: the reason string is property
    /// identifier 31 (0x1F) followed by a UTF-8 string, so the block is 5
    /// bytes: `1F 00 02 6E 6F`.
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
        ack.encode_body(Version::V5, &mut out).unwrap();

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
        let decoded = Ack::decode_body(&mut reader, Version::V5).expect("decodes");

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
