//! Quality of service levels.

use crate::error::CodecError;

/// How hard the broker works to deliver a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum QoS {
    /// Fire and forget.
    #[default]
    AtMostOnce,
    /// Acknowledged, may be delivered more than once.
    AtLeastOnce,
    /// Two-phase, delivered exactly once.
    ExactlyOnce,
}

impl QoS {
    /// Interpret the two-bit wire encoding. Three is not a level.
    ///
    /// The value three is a *malformed packet* here. That is the
    /// classification MQTT-v5.0 gives it in the two places this is called
    /// from: section 3.3.1.2, "If a Server or Client receives a PUBLISH
    /// packet which has both QoS bits set to 1 it is a Malformed Packet",
    /// and section 3.1.2.6, where a Will QoS of 3 "is a Malformed Packet".
    ///
    /// A SUBSCRIBE subscription options byte classifies the same value
    /// differently, so it has its own constructor; see
    /// [`QoS::from_subscription_options`].
    pub fn from_u8(value: u8) -> Result<Self, CodecError> {
        match value {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(CodecError::MalformedPacket(
                "quality of service must be 0, 1 or 2",
            )),
        }
    }

    /// Interpret bits 0 and 1 of a SUBSCRIBE subscription options byte.
    ///
    /// MQTT-v5.0 section 3.8.3.1: "Bits 0 and 1 of the Subscription Options
    /// represent Maximum QoS field ... It is a Protocol Error if the
    /// Maximum QoS field has the value 3." A protocol error, not a
    /// malformed packet: the byte parses, the value it names is not one the
    /// protocol allows. The same clause makes only the reserved bits 6 and
    /// 7 malformed, [MQTT-3.8.3-5], which is why those stay where they are.
    pub fn from_subscription_options(value: u8) -> Result<Self, CodecError> {
        match value {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(CodecError::ProtocolError(
                "maximum quality of service in the subscription options must not be 3",
            )),
        }
    }

    /// The two-bit wire encoding.
    pub fn as_u8(self) -> u8 {
        match self {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MQTT-v5.0 classifies the same value two ways depending on where it
    /// appears: malformed in a PUBLISH fixed header (section 3.3.1.2) and a
    /// protocol error in a SUBSCRIBE subscription options byte (section
    /// 3.8.3.1).
    #[test]
    fn three_is_malformed_in_a_fixed_header_and_a_protocol_error_in_subscription_options() {
        assert_eq!(
            QoS::from_u8(3).unwrap_err(),
            CodecError::MalformedPacket("quality of service must be 0, 1 or 2")
        );
        assert_eq!(
            QoS::from_subscription_options(3).unwrap_err(),
            CodecError::ProtocolError(
                "maximum quality of service in the subscription options must not be 3"
            )
        );
        for value in 0..=2 {
            assert_eq!(
                QoS::from_u8(value).unwrap(),
                QoS::from_subscription_options(value).unwrap()
            );
        }
    }
}
