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

    /// The two-bit wire encoding.
    pub fn as_u8(self) -> u8 {
        match self {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
}
