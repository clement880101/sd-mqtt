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
