//! Malformed inputs and the exact error each must produce.
//!
//! Every case is well formed apart from the one defect it names, so the
//! error it asserts can only come from the check it is named after. Each
//! hand-built MQTT 5 body therefore carries its mandatory property-length
//! byte, without which the decoder would fail with a truncation instead.

use bytes::BytesMut;
use mqtt_codec::packet::Connect;
use mqtt_codec::{decode, encode, CodecError, Limits, Packet, Properties, Version};

/// Each case is a name, the bytes, and the error the decoder must return.
fn cases() -> Vec<(&'static str, Vec<u8>, CodecError)> {
    vec![
        (
            "packet type zero is not assigned",
            vec![0x00, 0x00],
            CodecError::MalformedPacket("unknown packet type"),
        ),
        (
            "a fifth continuation byte in the length",
            vec![0x30, 0x80, 0x80, 0x80, 0x80],
            CodecError::MalformedPacket("variable byte integer longer than four bytes"),
        ),
        (
            // Valid but for the quality of service: a topic, the packet
            // identifier that quality of service two requires, and the
            // property block. Flipping the flags to 0x34 decodes cleanly.
            "publish quality of service three",
            vec![0x36, 0x06, 0x00, 0x01, b'a', 0x00, 0x01, 0x00],
            CodecError::MalformedPacket("quality of service must be 0, 1 or 2"),
        ),
        (
            // Valid but for the duplicate flag; 0x30 decodes cleanly.
            "duplicate flag at quality of service zero",
            vec![0x38, 0x04, 0x00, 0x01, b'a', 0x00],
            CodecError::MalformedPacket("duplicate flag set on a quality of service zero publish"),
        ),
        (
            // Valid but for the wildcard; any ordinary level decodes.
            "wildcard in a publish topic",
            vec![0x30, 0x04, 0x00, 0x01, b'#', 0x00],
            CodecError::InvalidTopicName,
        ),
        (
            // An empty topic is legal in MQTT 5 when a topic alias is
            // present, so the check runs after the property block. The
            // block must therefore be supplied, or the failure is a
            // truncation rather than the rejection this case is about.
            "empty publish topic with no alias",
            vec![0x30, 0x03, 0x00, 0x00, 0x00],
            CodecError::InvalidTopicName,
        ),
        (
            // A whole subscribe to "a" at quality of service zero, carrying
            // the wrong reserved flags; 0x82 decodes cleanly.
            "subscribe without the required flag bits",
            vec![0x80, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, b'a', 0x00],
            CodecError::MalformedPacket("reserved flags in the fixed header are wrong"),
        ),
        (
            "packet identifier zero",
            vec![0x62, 0x02, 0x00, 0x00],
            CodecError::ProtocolError("packet identifier must not be zero"),
        ),
        (
            "trailing byte after a ping request",
            vec![0xC0, 0x01, 0xFF],
            CodecError::MalformedPacket("trailing bytes after the packet body"),
        ),
        (
            // A whole MQTT 5 CONNECT: protocol name, level 5, clean start,
            // keep alive zero, a property block holding a topic alias, and
            // an empty client identifier. A topic alias is legal only on
            // PUBLISH (specification 2.2.2.2), so the property block is the
            // one defect: replacing it with an empty block (0x00, and a
            // remaining length of 0x0D) decodes cleanly.
            "topic alias in a connect",
            vec![
                0x10, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x00, 0x03, 0x23,
                0x00, 0x0A, 0x00, 0x00,
            ],
            CodecError::ProtocolError("property is not permitted on this packet type"),
        ),
        (
            "a string that is not valid UTF-8",
            vec![0x30, 0x05, 0x00, 0x02, 0xFF, 0xFE, 0x00],
            CodecError::InvalidUtf8,
        ),
        (
            "a string containing a null character",
            vec![0x30, 0x05, 0x00, 0x02, b'a', 0x00, 0x00],
            CodecError::InvalidUtf8,
        ),
    ]
}

#[test]
fn every_malformed_input_produces_its_documented_error() {
    for (name, raw, expected) in cases() {
        let mut buf = BytesMut::from(&raw[..]);
        let actual = decode(&mut buf, Version::V5, &Limits::default());
        assert_eq!(
            actual,
            Err(expected),
            "case {name:?} produced the wrong outcome"
        );
    }
}

#[test]
fn an_oversized_declaration_is_rejected_before_the_body_arrives() {
    let limits = Limits {
        max_packet_size: 64,
        ..Limits::default()
    };
    // Declares a body of 300 bytes; supplies none of it.
    let mut buf = BytesMut::from(&[0x30u8, 0xAC, 0x02][..]);
    assert_eq!(
        decode(&mut buf, Version::V5, &limits),
        Err(CodecError::PacketTooLarge)
    );
    assert_eq!(buf.len(), 3, "nothing should have been consumed");
}

#[test]
fn a_topic_deeper_than_the_limit_is_rejected() {
    let limits = Limits {
        max_topic_depth: 2,
        ..Limits::default()
    };
    // PUBLISH to "a/b/c".
    let mut buf =
        BytesMut::from(&[0x30u8, 0x08, 0x00, 0x05, b'a', b'/', b'b', b'/', b'c', 0x00][..]);
    assert_eq!(
        decode(&mut buf, Version::V5, &limits),
        Err(CodecError::TopicTooDeep)
    );
}

/// The motivating example, checked from both ends.
///
/// A topic alias in a CONNECT is the clearest case of a property outside its
/// packet's subset, and before this check the decoder accepted it silently.
/// The encoder must refuse it too, or the crate would write packets it would
/// not read back.
#[test]
fn a_topic_alias_is_refused_on_a_connect_and_accepted_on_a_publish() {
    let mut buf = BytesMut::from(
        &[
            0x10u8, 0x10, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x00, 0x03, 0x23,
            0x00, 0x0A, 0x00, 0x00,
        ][..],
    );
    assert_eq!(
        decode(&mut buf, Version::V5, &Limits::default()),
        Err(CodecError::ProtocolError(
            "property is not permitted on this packet type"
        ))
    );

    // The identical property block on a PUBLISH is legal, so the rejection
    // above is about the packet type and not about the bytes.
    let mut buf = BytesMut::from(&[0x30u8, 0x07, 0x00, 0x01, b'a', 0x03, 0x23, 0x00, 0x0A][..]);
    let publish = decode(&mut buf, Version::V5, &Limits::default())
        .expect("decodes")
        .expect("a whole frame");
    match publish {
        Packet::Publish(publish) => assert_eq!(publish.properties.topic_alias, Some(10)),
        other => panic!("expected a publish, got {other:?}"),
    }

    // And the encoder refuses what the decoder rejects.
    let connect = Connect {
        version: Version::V5,
        client_id: String::new(),
        clean_start: true,
        keep_alive: 0,
        username: None,
        password: None,
        will: None,
        properties: Properties {
            topic_alias: Some(10),
            ..Properties::default()
        },
    };
    let mut out = BytesMut::new();
    assert_eq!(
        encode(&Packet::Connect(Box::new(connect)), Version::V5, &mut out),
        Err(CodecError::ProtocolError(
            "property is not permitted on this packet type"
        ))
    );
}
