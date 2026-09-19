//! Round-trip properties over generated packets.

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{
    Ack, ConnAck, Connect, Disconnect, Publish, RetainHandling, SubAck, Subscribe,
    SubscriptionOptions, UnsubAck, Unsubscribe, Will,
};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use proptest::prelude::*;

fn any_qos() -> impl Strategy<Value = QoS> {
    prop_oneof![
        Just(QoS::AtMostOnce),
        Just(QoS::AtLeastOnce),
        Just(QoS::ExactlyOnce),
    ]
}

/// Topic levels that are always valid in a name and a filter.
fn any_level() -> impl Strategy<Value = String> {
    "[a-z0-9]{1,8}"
}

fn any_topic() -> impl Strategy<Value = String> {
    prop::collection::vec(any_level(), 1..4).prop_map(|parts| parts.join("/"))
}

fn any_filter() -> impl Strategy<Value = String> {
    prop::collection::vec(prop_oneof![any_level(), Just("+".to_string())], 1..4)
        .prop_map(|parts| parts.join("/"))
}

fn any_payload() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..64).prop_map(Bytes::from)
}

/// Properties that every packet type accepts, so one generator serves all.
fn any_properties() -> impl Strategy<Value = Properties> {
    (
        prop::option::of(any::<u32>()),
        prop::option::of("[a-z ]{0,16}"),
        prop::collection::vec(("[a-z]{1,4}", "[a-z]{0,8}"), 0..3),
    )
        .prop_map(|(expiry, reason, user)| Properties {
            message_expiry_interval: expiry,
            reason_string: reason,
            user_properties: user,
            ..Properties::default()
        })
}

/// Properties are only encoded for MQTT 5, so 3.1.1 packets carry none.
fn properties_for(version: Version) -> BoxedStrategy<Properties> {
    match version {
        Version::V5 => any_properties().boxed(),
        Version::V311 => Just(Properties::default()).boxed(),
    }
}

fn any_packet_id() -> impl Strategy<Value = u16> {
    1u16..=u16::MAX
}

fn any_publish(version: Version) -> impl Strategy<Value = Packet> {
    (
        any_qos(),
        any::<bool>(),
        any_topic(),
        any_packet_id(),
        any_payload(),
        properties_for(version),
    )
        .prop_map(|(qos, retain, topic, id, payload, properties)| {
            Packet::Publish(Publish {
                // The duplicate flag is illegal at quality of service zero.
                dup: false,
                qos,
                retain,
                topic,
                packet_id: if qos == QoS::AtMostOnce {
                    None
                } else {
                    Some(id)
                },
                payload,
                properties,
            })
        })
}

fn any_subscribe(version: Version) -> impl Strategy<Value = Packet> {
    let option = (
        any_filter(),
        any_qos(),
        any::<bool>(),
        any::<bool>(),
        prop_oneof![
            Just(RetainHandling::SendAlways),
            Just(RetainHandling::SendIfNew),
            Just(RetainHandling::DoNotSend),
        ],
    )
        .prop_map(
            move |(filter, qos, no_local, rap, handling)| SubscriptionOptions {
                filter,
                qos,
                // These three are only encoded for MQTT 5.
                no_local: version == Version::V5 && no_local,
                retain_as_published: version == Version::V5 && rap,
                retain_handling: if version == Version::V5 {
                    handling
                } else {
                    RetainHandling::SendAlways
                },
            },
        );

    (
        any_packet_id(),
        prop::collection::vec(option, 1..4),
        properties_for(version),
    )
        .prop_map(|(packet_id, filters, properties)| {
            Packet::Subscribe(Subscribe {
                packet_id,
                filters,
                properties,
            })
        })
}

fn any_connect(version: Version) -> impl Strategy<Value = Packet> {
    (
        "[a-z0-9-]{0,24}",
        any::<bool>(),
        any::<u16>(),
        prop::option::of("[a-z]{1,8}"),
        prop::option::of(any_payload()),
        prop::option::of((any_topic(), any_payload(), any_qos(), any::<bool>())),
        properties_for(version),
    )
        .prop_map(
            move |(client_id, clean_start, keep_alive, username, password, will, properties)| {
                // MQTT 3.1.1 forbids a password without a user name.
                let password = if username.is_none() && version == Version::V311 {
                    None
                } else {
                    password
                };
                // The version field must agree with the version the packet
                // is encoded for; `Connect::encode_body` refuses otherwise.
                Packet::Connect(Box::new(Connect {
                    version,
                    client_id,
                    clean_start,
                    keep_alive,
                    username,
                    password,
                    will: will.map(|(topic, payload, qos, retain)| Will {
                        topic,
                        payload,
                        qos,
                        retain,
                        properties: Properties::default(),
                    }),
                    properties,
                }))
            },
        )
}

fn any_ack(version: Version) -> impl Strategy<Value = Packet> {
    (any_packet_id(), properties_for(version)).prop_map(move |(packet_id, properties)| {
        // Only success survives the 3.1.1 short form, which carries no code.
        let reason = ReasonCode::Success;
        Packet::PubAck(Ack {
            packet_id,
            reason,
            properties: if version == Version::V5 {
                properties
            } else {
                Properties::default()
            },
        })
    })
}

fn any_unsubscribe(version: Version) -> impl Strategy<Value = Packet> {
    (
        any_packet_id(),
        prop::collection::vec(any_filter(), 1..4),
        properties_for(version),
    )
        .prop_map(|(packet_id, filters, properties)| {
            Packet::Unsubscribe(Unsubscribe {
                packet_id,
                filters,
                properties,
            })
        })
}

fn any_packet(version: Version) -> BoxedStrategy<Packet> {
    prop_oneof![
        any_connect(version),
        any_publish(version),
        any_subscribe(version),
        any_unsubscribe(version),
        any_ack(version),
        Just(Packet::PingReq),
        Just(Packet::PingResp),
        Just(Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: Properties::default(),
        })),
    ]
    .boxed()
}

fn round_trip(packet: &Packet, version: Version) -> Packet {
    let mut buf = BytesMut::new();
    encode(packet, version, &mut buf).expect("encodes");
    let decoded = decode(&mut buf, version, &Limits::default())
        .expect("decodes")
        .expect("a whole frame");
    assert!(buf.is_empty(), "the frame should have been consumed");
    decoded
}

/// The version a decoded packet must be re-encoded as.
///
/// CONNECT carries its own version and refuses to be written as any other,
/// which is deliberate: the packet that announces the version cannot be
/// re-encoded under a different one without lying about it.
fn encoding_version(packet: &Packet, default: Version) -> Version {
    match packet {
        Packet::Connect(connect) => connect.version,
        _ => default,
    }
}

proptest! {
    #[test]
    fn v5_packets_survive_a_round_trip(packet in any_packet(Version::V5)) {
        prop_assert_eq!(round_trip(&packet, Version::V5), packet);
    }

    #[test]
    fn v311_packets_survive_a_round_trip(packet in any_packet(Version::V311)) {
        prop_assert_eq!(round_trip(&packet, Version::V311), packet);
    }

    /// A frame delivered one byte at a time must decode exactly once, at the
    /// moment its final byte arrives, and never before.
    #[test]
    fn a_frame_split_across_reads_decodes_once_complete(
        packet in any_packet(Version::V5)
    ) {
        let mut whole = BytesMut::new();
        encode(&packet, Version::V5, &mut whole).expect("encodes");

        let mut fed = BytesMut::new();
        for (index, byte) in whole.iter().enumerate() {
            fed.extend_from_slice(&[*byte]);
            let outcome = decode(&mut fed, Version::V5, &Limits::default()).expect("no error");
            if index + 1 < whole.len() {
                prop_assert!(outcome.is_none(), "decoded early at byte {}", index);
                prop_assert_eq!(fed.len(), index + 1, "a partial read must consume nothing");
            } else {
                prop_assert_eq!(outcome, Some(packet.clone()));
                prop_assert!(fed.is_empty(), "the whole frame should have been consumed");
            }
        }
    }

    /// Arbitrary bytes must produce an error or a request for more, never a
    /// panic and never an infinite loop.
    ///
    /// The three outcomes are pinned, not merely the absence of a panic: a
    /// decode consumes, a partial read consumes nothing, and an error
    /// consumes nothing either, so the caller can always account for every
    /// byte it handed over.
    #[test]
    fn arbitrary_bytes_never_panic(raw in prop::collection::vec(any::<u8>(), 0..512)) {
        let mut buf = BytesMut::from(&raw[..]);
        let before = buf.len();
        match decode(&mut buf, Version::V5, &Limits::default()) {
            Ok(Some(_)) => prop_assert!(buf.len() < before, "a decode must consume bytes"),
            Ok(None) => prop_assert_eq!(buf.len(), before, "a partial read must consume nothing"),
            Err(_) => prop_assert_eq!(buf.len(), before, "a failed read must consume nothing"),
        }
    }

    /// The same three outcomes, over inputs shaped like real packets.
    ///
    /// Purely random bytes almost never get past the fixed header, so this
    /// runs the same assertions over a valid encoding with one byte
    /// corrupted, which reaches the body decoders.
    #[test]
    fn corrupted_frames_never_panic(
        packet in any_packet(Version::V5),
        index in any::<prop::sample::Index>(),
        replacement in any::<u8>(),
    ) {
        let mut whole = BytesMut::new();
        encode(&packet, Version::V5, &mut whole).expect("encodes");
        let at = index.index(whole.len());
        whole[at] = replacement;

        let before = whole.len();
        match decode(&mut whole, Version::V5, &Limits::default()) {
            Ok(Some(_)) => prop_assert!(whole.len() < before, "a decode must consume bytes"),
            Ok(None) => prop_assert_eq!(whole.len(), before, "a partial read must consume nothing"),
            Err(_) => prop_assert_eq!(whole.len(), before, "a failed read must consume nothing"),
        }
    }

    /// Whatever the decoder accepts, the encoder must be able to write again.
    #[test]
    fn anything_decoded_can_be_encoded(raw in prop::collection::vec(any::<u8>(), 0..512)) {
        let mut buf = BytesMut::from(&raw[..]);
        if let Ok(Some(packet)) = decode(&mut buf, Version::V5, &Limits::default()) {
            let mut out = BytesMut::new();
            let version = encoding_version(&packet, Version::V5);
            prop_assert!(encode(&packet, version, &mut out).is_ok());
        }
    }

    /// The same asymmetry check, over inputs that actually reach the body
    /// decoders. Random bytes rarely produce a decodable packet, so a
    /// corrupted valid frame does the work here.
    #[test]
    fn anything_decoded_from_a_corrupted_frame_can_be_encoded(
        packet in any_packet(Version::V5),
        index in any::<prop::sample::Index>(),
        replacement in any::<u8>(),
    ) {
        let mut whole = BytesMut::new();
        encode(&packet, Version::V5, &mut whole).expect("encodes");
        let at = index.index(whole.len());
        whole[at] = replacement;

        if let Ok(Some(decoded)) = decode(&mut whole, Version::V5, &Limits::default()) {
            let mut out = BytesMut::new();
            let version = encoding_version(&decoded, Version::V5);
            prop_assert!(
                encode(&decoded, version, &mut out).is_ok(),
                "decoded {decoded:?} but could not encode it again"
            );
        }
    }
}

/// These types are re-exported for downstream crates; touch them so an
/// accidental removal fails the build rather than a distant crate.
#[test]
fn public_surface_is_reachable() {
    let _ = ConnAck {
        session_present: false,
        reason: ReasonCode::Success,
        properties: Properties::default(),
    };
    let _ = SubAck {
        packet_id: 1,
        reasons: vec![ReasonCode::GrantedQos2],
        properties: Properties::default(),
    };
    let _ = UnsubAck {
        packet_id: 1,
        reasons: vec![ReasonCode::Success],
        properties: Properties::default(),
    };
}
