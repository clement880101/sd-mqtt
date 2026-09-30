//! What a message looks like on a cluster link.
//!
//! A 32-bit little-endian length **covering the version byte and the
//! body**, then the version byte, then a postcard-encoded message.
//! Bounded by a configured maximum. A version byte this build does not
//! know closes the link with a reason rather than guessing at the
//! bytes behind it.
//!
//! # The length covers the version byte, and that was ambiguous
//!
//! "A length, the version byte, then the message" can be read two
//! ways, and the two differ by one. Either reading works on its own;
//! what does not work is one end reading each way, and a round trip
//! through this module cannot see that at all — convention 5, a round
//! trip cannot see a consistent error. So the answer is pinned by a
//! golden vector rather than by a round trip; see
//! [`tests::the_length_covers_the_version_byte`], which is written from
//! the layout above rather than captured from [`encode`].
//!
//! The length is over the version byte because the version byte is
//! *inside* the frame. A reader that has the length has the whole
//! frame, including the one byte that says whether it can read the
//! rest, and can skip a frame it does not understand without parsing
//! anything — which is the only thing a version byte is for.
//!
//! # Oversized is refused from the declared length alone
//!
//! `crates/server/src/framing.rs` is the precedent and the reason it
//! matters is the same here: a peer that declares four gigabytes must
//! be refused before four gigabytes are reserved for it, and therefore
//! before the body arrives — it never will. [`decode`] checks the
//! maximum against the declared length and nothing else, with nothing
//! allocated and no body awaited.
//!
//! Convention 1 the other way about: [`encode`] refuses to produce a
//! frame [`decode`] would reject, so an oversized message fails at the
//! sender that built it rather than at the peer that has to hang up on
//! it.

use bytes::BytesMut;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// The frame layout this build writes and reads.
///
/// Its own version, separate from the greeting's: a node that cannot be
/// greeted must say so before either side has spent anything, and this
/// is about messages that only flow once a handshake has succeeded.
pub const FRAME_VERSION: u8 = 1;

/// How wide the length is: 32 bits, little-endian.
pub const LENGTH_BYTES: usize = 4;

/// What the length counts, beyond the body: the version byte.
pub const VERSION_BYTES: usize = 1;

/// The most a frame may declare, when nothing says otherwise.
///
/// A megabyte. Large enough for any metadata record section 8.2
/// replicates and for a forwarded publication at the broker's own
/// default maximum packet size, small enough that fifty peers each
/// mid-frame is fifty megabytes and not fifty gigabytes.
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 1024 * 1024;

/// How large a frame this link will read or write.
///
/// One number in one place, so that the refusal [`encode`] makes and
/// the refusal [`decode`] makes cannot drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameLimits {
    /// The largest length a frame may declare, counting the version
    /// byte and the body.
    pub max_frame_bytes: u32,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

/// One message, as it goes on the wire.
///
/// # Errors
///
/// [`FrameError::TooLarge`] when the frame this would produce is larger
/// than the maximum — refused here rather than at the peer, because the
/// writer must refuse what the reader rejects. [`FrameError::Unencodable`]
/// when the message cannot be serialised, which is a bug in the message
/// type rather than anything a peer did.
pub fn encode<M: Serialize>(message: &M, limits: &FrameLimits) -> Result<Vec<u8>, FrameError> {
    let body = postcard::to_allocvec(message).map_err(FrameError::Unencodable)?;
    // The version byte is inside the frame, so it is inside the
    // length. See the module documentation for why this is written down
    // in two places and pinned by a golden vector.
    let declared = body.len() + VERSION_BYTES;
    let declared = u32::try_from(declared).unwrap_or(u32::MAX);
    if declared > limits.max_frame_bytes {
        return Err(FrameError::TooLarge {
            declared,
            maximum: limits.max_frame_bytes,
        });
    }
    let mut out = Vec::with_capacity(LENGTH_BYTES + declared as usize);
    out.extend_from_slice(&declared.to_le_bytes());
    out.push(FRAME_VERSION);
    out.extend_from_slice(&body);
    Ok(out)
}

/// The next whole frame in `buffer`, if all of it is there.
///
/// Consumes nothing when the frame is incomplete, so a caller may read
/// more bytes in and ask again. Consumes the whole frame when it is
/// there, whether or not the message inside it decodes.
///
/// # Errors
///
/// [`FrameError::TooLarge`] from the declared length alone, before any
/// body has arrived and with nothing allocated for it.
/// [`FrameError::Empty`] for a frame that does not even hold its own
/// version byte. [`FrameError::UnknownVersion`] for a layout this build
/// does not speak. [`FrameError::Undecodable`] and
/// [`FrameError::Trailing`] for a body that is not the message it
/// claimed to be.
pub fn decode<M: DeserializeOwned>(
    buffer: &mut BytesMut,
    limits: &FrameLimits,
) -> Result<Option<M>, FrameError> {
    if buffer.len() < LENGTH_BYTES {
        return Ok(None);
    }
    // Read, not consumed: an incomplete frame must leave the buffer
    // exactly as it was.
    let mut length = [0_u8; LENGTH_BYTES];
    length.copy_from_slice(&buffer[..LENGTH_BYTES]);
    let declared = u32::from_le_bytes(length);

    // **Before the body, and before anything is reserved for it.** A
    // peer declaring four gigabytes is refused on the strength of four
    // bytes. Checking this after waiting for the body would make the
    // maximum a limit on what has already been accepted, which is no
    // limit at all.
    if declared > limits.max_frame_bytes {
        return Err(FrameError::TooLarge {
            declared,
            maximum: limits.max_frame_bytes,
        });
    }
    // A frame has to hold its own version byte. Nought is not a small
    // frame, it is a length that cannot be one.
    if (declared as usize) < VERSION_BYTES {
        return Err(FrameError::Empty);
    }

    let whole = LENGTH_BYTES + declared as usize;
    if buffer.len() < whole {
        return Ok(None);
    }
    let frame = buffer.split_to(whole);
    let version = frame[LENGTH_BYTES];
    if version != FRAME_VERSION {
        return Err(FrameError::UnknownVersion {
            found: version,
            expected: FRAME_VERSION,
        });
    }
    let body = &frame[LENGTH_BYTES + VERSION_BYTES..];
    let (message, rest) = postcard::take_from_bytes::<M>(body).map_err(FrameError::Undecodable)?;
    if !rest.is_empty() {
        // Rejected rather than ignored, for the reason
        // `storage::record`'s reader gives: bytes nobody read are bytes
        // one end wrote and the other did not, which is the two ends
        // disagreeing about the layout.
        return Err(FrameError::Trailing { extra: rest.len() });
    }
    Ok(Some(message))
}

/// How many bytes a body of this size weighs on the wire.
///
/// The length, the version byte and the body. Used by the link to
/// decide when a batch is large enough to flush, so that the threshold
/// is about what the socket will carry rather than about what the
/// messages would have weighed.
#[must_use]
pub fn framed_size(body: usize) -> usize {
    LENGTH_BYTES + VERSION_BYTES + body
}

/// Why a frame was not read or not written.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// The frame declares more than this link will carry.
    ///
    /// Refused from the declared length alone. Names both numbers,
    /// because an operator's next move is to raise the maximum or to
    /// find out what is sending something that size, and neither is
    /// possible from one of them.
    #[error("a frame declaring {declared} bytes is larger than this link's maximum of {maximum}")]
    TooLarge {
        /// What the frame said it was.
        declared: u32,
        /// What this link will carry.
        maximum: u32,
    },
    /// The frame does not even hold its own version byte.
    #[error("a frame must carry at least its version byte, and this one declares nothing")]
    Empty,
    /// A frame layout this build does not speak.
    ///
    /// The link closes and says so. Not skipped: a peer speaking a
    /// layout this build does not know is a peer whose next frame
    /// cannot be trusted to be where the length says either, and a
    /// mixed-version mesh silently dropping half of what it is told is
    /// worse than one that will not form.
    #[error("the peer sent frame version {found}, and this build speaks {expected}")]
    UnknownVersion {
        /// What the peer sent.
        found: u8,
        /// What this build speaks.
        expected: u8,
    },
    /// The body is not the message it claimed to be.
    #[error("a frame's body did not decode: {0}")]
    Undecodable(postcard::Error),
    /// The body decoded, and there were bytes left over.
    #[error("a frame's body had {extra} bytes left after the message")]
    Trailing {
        /// How many.
        extra: usize,
    },
    /// The message could not be encoded.
    #[error("a message could not be encoded: {0}")]
    Unencodable(postcard::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::Deserialize;

    /// A message, standing in for whatever the mesh will carry.
    ///
    /// This module is deliberately generic over the payload: nothing
    /// routes or replicates yet, and a frame is a frame whatever is in
    /// it.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Message {
        /// A number, so the vectors below are readable.
        beat: u32,
        /// A string, so a length prefix is in the body too.
        note: String,
    }

    fn message() -> Message {
        Message {
            beat: 1,
            note: "hello".to_owned(),
        }
    }

    fn limits(max: u32) -> FrameLimits {
        FrameLimits {
            max_frame_bytes: max,
        }
    }

    // -----------------------------------------------------------
    // The layout
    // -----------------------------------------------------------

    /// The bytes, written from the layout rather than from [`encode`].
    ///
    /// **The length is five and not four**, because it covers the
    /// version byte as well as the body. That is the one ambiguity the
    /// specification had, and it is the mutation this test exists for:
    /// a build whose length excluded the version byte would round trip
    /// with itself perfectly and disagree with every other build, which
    /// is convention 5's first bullet exactly.
    #[test]
    fn the_length_covers_the_version_byte() {
        // postcard writes `beat` as a varint and `note` as a varint
        // length then its bytes: 0x01, then 0x05 and "hello". Seven
        // bytes of body, so eight with the version byte.
        let expected: &[u8] = &[
            0x08, 0x00, 0x00, 0x00, // length: version byte and body
            0x01, // frame version 1
            0x01, // beat = 1
            0x05, b'h', b'e', b'l', b'l', b'o', // note = "hello"
        ];
        let encoded = encode(&message(), &FrameLimits::default()).expect("it encodes");
        assert_eq!(encoded, expected);

        // And said again as a rule rather than as a vector, so that a
        // different message is covered too.
        let body = postcard::to_allocvec(&message()).expect("it serialises");
        let declared = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(
            declared as usize,
            body.len() + VERSION_BYTES,
            "the declared length must count the version byte"
        );
        assert_eq!(encoded.len(), framed_size(body.len()));
    }

    /// A frame round trips.
    #[test]
    fn a_frame_round_trips() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&encode(&message(), &FrameLimits::default()).expect("encodes"));
        let read: Message = decode(&mut buffer, &FrameLimits::default())
            .expect("it decodes")
            .expect("a whole frame is there");
        assert_eq!(read, message());
        assert!(buffer.is_empty(), "a whole frame is consumed whole");
    }

    /// Several frames in one buffer are all read, in order.
    #[test]
    fn several_frames_in_one_buffer_are_all_read() {
        let mut buffer = BytesMut::new();
        for beat in 0..3_u32 {
            let one = Message {
                beat,
                note: "beat".to_owned(),
            };
            buffer.extend_from_slice(&encode(&one, &FrameLimits::default()).expect("encodes"));
        }
        for beat in 0..3_u32 {
            let read: Message = decode(&mut buffer, &FrameLimits::default())
                .expect("it decodes")
                .expect("a frame");
            assert_eq!(read.beat, beat);
        }
        assert_eq!(
            decode::<Message>(&mut buffer, &FrameLimits::default()).expect("no error"),
            None
        );
    }

    // -----------------------------------------------------------
    // Partial frames
    // -----------------------------------------------------------

    /// A partial frame yields nothing and resumes when the rest
    /// arrives.
    ///
    /// Every prefix of a frame, one byte at a time, because "it works
    /// when split" and "it works when split *here*" are different
    /// claims and the second is the one a socket makes.
    #[test]
    fn a_partial_frame_yields_nothing_and_resumes_when_the_rest_arrives() {
        let whole = encode(&message(), &FrameLimits::default()).expect("encodes");
        let mut buffer = BytesMut::new();
        for (at, byte) in whole.iter().copied().enumerate() {
            // Every prefix shorter than the whole frame is nothing yet,
            // and must leave the buffer untouched.
            assert_eq!(
                decode::<Message>(&mut buffer, &FrameLimits::default()).expect("no error"),
                None,
                "{at} bytes in is not a frame yet"
            );
            assert_eq!(buffer.len(), at, "an incomplete frame was consumed");
            buffer.extend_from_slice(&[byte]);
        }
        let read: Message = decode(&mut buffer, &FrameLimits::default())
            .expect("it decodes")
            .expect("the last byte completes it");
        assert_eq!(read, message());
    }

    // -----------------------------------------------------------
    // The maximum
    // -----------------------------------------------------------

    /// A frame declaring more than the maximum is refused from its
    /// length alone.
    ///
    /// **Four bytes and nothing else.** The body never arrives, and
    /// never will: the whole point is that a peer declaring far more
    /// than this link carries is refused before anything is reserved
    /// for it. A build that checked the maximum after reading the body
    /// would return `Ok(None)` here and wait for ever.
    #[test]
    fn a_frame_larger_than_the_maximum_is_refused_from_its_length_alone() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&4_000_000_000_u32.to_le_bytes());

        let error = decode::<Message>(&mut buffer, &limits(64))
            .expect_err("a frame that size is not one this link carries");
        match error {
            FrameError::TooLarge { declared, maximum } => {
                assert_eq!(declared, 4_000_000_000);
                assert_eq!(maximum, 64);
            }
            other => panic!("refused as {other:?}"),
        }
        assert_eq!(
            buffer.len(),
            LENGTH_BYTES,
            "nothing beyond the length was read, because nothing beyond it arrived"
        );
    }

    /// The maximum is exact in both directions.
    ///
    /// A frame of exactly the maximum is carried and one byte more is
    /// not, at both ends, because the writer must refuse what the
    /// reader rejects. An off-by-one in one of the two is a message a
    /// node sends and no node will read.
    #[test]
    fn the_maximum_is_the_same_number_at_both_ends() {
        let body = postcard::to_allocvec(&message()).expect("it serialises");
        let exact = u32::try_from(body.len() + VERSION_BYTES).expect("small");

        let at_the_limit = encode(&message(), &limits(exact)).expect("exactly the maximum fits");
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&at_the_limit);
        assert_eq!(
            decode::<Message>(&mut buffer, &limits(exact))
                .expect("it decodes")
                .expect("a frame"),
            message(),
            "a frame of exactly the maximum is read"
        );

        let error = encode(&message(), &limits(exact - 1))
            .expect_err("one byte over the maximum is refused by the writer");
        assert!(matches!(error, FrameError::TooLarge { .. }), "{error:?}");

        // And the reader refuses the same frame, which is what makes
        // the two one rule.
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&at_the_limit);
        assert!(matches!(
            decode::<Message>(&mut buffer, &limits(exact - 1)).expect_err("the reader agrees"),
            FrameError::TooLarge { .. }
        ));
    }

    /// A frame declaring nothing at all is refused as such.
    #[test]
    fn a_frame_that_does_not_hold_its_version_byte_is_refused() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&0_u32.to_le_bytes());
        assert!(matches!(
            decode::<Message>(&mut buffer, &FrameLimits::default()).expect_err("not a frame"),
            FrameError::Empty
        ));
    }

    // -----------------------------------------------------------
    // The version byte
    // -----------------------------------------------------------

    /// An unknown version is refused, saying both numbers.
    ///
    /// The reason is the reason the link closes on it: a peer speaking
    /// a layout this build does not know cannot be trusted about where
    /// its next frame begins either.
    #[test]
    fn an_unknown_version_is_refused_with_a_reason() {
        let mut frame = encode(&message(), &FrameLimits::default()).expect("encodes");
        frame[LENGTH_BYTES] = FRAME_VERSION + 9;
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&frame);

        match decode::<Message>(&mut buffer, &FrameLimits::default())
            .expect_err("a version this build does not speak")
        {
            FrameError::UnknownVersion { found, expected } => {
                assert_eq!(found, FRAME_VERSION + 9);
                assert_eq!(expected, FRAME_VERSION);
            }
            other => panic!("refused as {other:?}"),
        }
        let said = FrameError::UnknownVersion {
            found: FRAME_VERSION + 9,
            expected: FRAME_VERSION,
        }
        .to_string();
        assert!(said.contains("version"), "{said}");
    }

    /// A body that is not the message it claimed to be is refused, and
    /// so is one with bytes left over.
    #[test]
    fn a_body_that_is_not_the_message_is_refused() {
        // A body of one byte, which is a truncated `beat` and no
        // `note` at all.
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&2_u32.to_le_bytes());
        buffer.extend_from_slice(&[FRAME_VERSION, 0x01]);
        assert!(matches!(
            decode::<Message>(&mut buffer, &FrameLimits::default()).expect_err("not a message"),
            FrameError::Undecodable(_)
        ));

        // And a whole message with a byte after it.
        let body = postcard::to_allocvec(&message()).expect("it serialises");
        let mut buffer = BytesMut::new();
        let declared = u32::try_from(body.len() + VERSION_BYTES + 1).expect("small");
        buffer.extend_from_slice(&declared.to_le_bytes());
        buffer.extend_from_slice(&[FRAME_VERSION]);
        buffer.extend_from_slice(&body);
        buffer.extend_from_slice(&[0xFF]);
        match decode::<Message>(&mut buffer, &FrameLimits::default())
            .expect_err("bytes nobody read are two ends disagreeing")
        {
            FrameError::Trailing { extra } => assert_eq!(extra, 1),
            other => panic!("refused as {other:?}"),
        }
    }
}
