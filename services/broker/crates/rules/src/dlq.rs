//! The dead letters a rule has produced, kept where somebody will see
//! them.
//!
//! # A resource, not a subscription, and why
//!
//! Section 9.2 publishes an exhausted envelope to `$sd/rules/<id>/dlq`,
//! so the interface could read dead letters by subscribing through the
//! message tap it already has. That costs nothing new and bounds
//! itself, which is a real argument. It was not taken, and the reason
//! is one sentence:
//!
//! **A subscription only shows what is published while somebody is
//! watching, and nobody watches a rules page waiting for a dead
//! letter.** An operator learns that a connector has stopped working
//! from an alert, from a colleague, or from whoever was expecting the
//! data — minutes or hours after the letter was published — and *then*
//! opens the page. A tap would show them an empty list in exactly the
//! situation it exists for, and an empty list is indistinguishable
//! from a rule that has not dead-lettered anything.
//!
//! Two smaller reasons point the same way. A tap is a real session
//! inside the broker and there is a fixed number of them; a page that
//! opened one per rule, or one on `$sd/rules/+/dlq`, would spend that
//! cap on a view that is usually empty. And a tap cannot answer "what
//! has *this rule* dead-lettered", only "what arrives from now on",
//! which is not the question the page is asking.
//!
//! **The cost is that this is a second mechanism**, and it is paid
//! honestly: a bounded ring in memory, filled where the letter is
//! produced. Nothing about section 9.2 changes — the letter is still
//! published to the topic, so a durable subscriber is still how an
//! operator keeps them for ever, and this is the recent history a page
//! can show without one.
//!
//! # What is kept, and what that costs
//!
//! The last [`KEPT`] letters per rule, each truncated to [`LARGEST`]
//! bytes of the document that was published. Ten and two kibibytes, so
//! at most about 20 KB per rule that has actually dead-lettered
//! something; a rule that never has keeps an empty ring. Two hundred
//! rules all failing at once is 4 MB, which is the worst case of a
//! broker in which every connector is broken.
//!
//! **A letter is truncated rather than dropped**, because what an
//! operator needs from one is the reason and the topic, and both are
//! at the front of the document. The whole letter is on the topic for
//! anyone who wants it.
//!
//! **In memory, and lost when the process stops.** A dead letter that
//! must outlive a restart is one somebody subscribed to durably, which
//! is what the topic is for. Saying so here stops a reader taking this
//! ring for the record.

use std::collections::VecDeque;

use parking_lot::Mutex;
use serde::Serialize;

/// How many letters a rule keeps.
pub const KEPT: usize = 10;

/// How much of one letter is kept, in bytes.
pub const LARGEST: usize = 2_048;

/// One dead letter, as the interface reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeadLetter {
    /// Which one this is, counting from one for this rule in this
    /// process.
    ///
    /// Not an identifier anything else knows: the envelope carries its
    /// own, inside `document`. This is what gives the ring an order a
    /// page can read and a cursor can name.
    pub sequence: u64,
    /// When it was published, on the wall clock, in whole seconds.
    pub at: u32,
    /// The topic it was published to, which is `$sd/rules/<id>/dlq`.
    pub topic: String,
    /// The document that was published, as text.
    ///
    /// Cut to [`LARGEST`] bytes when it is longer; `truncated` says
    /// so. It is not parsed here: what was published is what is kept,
    /// so a reader of this resource and a subscriber to the topic are
    /// looking at the same thing.
    pub document: String,
    /// Whether `document` is the whole of what was published.
    pub truncated: bool,
}

/// The dead letters one rule has produced, most recent last.
#[derive(Debug, Default)]
pub struct DeadLetterLog {
    kept: Mutex<Ring>,
}

/// The ring and the count it numbers letters from.
#[derive(Debug, Default)]
struct Ring {
    /// How many letters this rule has produced, which is more than
    /// the ring holds once it has wrapped.
    produced: u64,
    /// The letters, oldest first.
    letters: VecDeque<DeadLetter>,
}

impl DeadLetterLog {
    /// Nothing dead-lettered yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep one letter, dropping the oldest when the ring is full.
    ///
    /// Called from the delivery pump's task, which may block on a
    /// short lock and does not hold the broker's.
    pub fn record(&self, topic: &str, at: u32, published: &[u8]) {
        let truncated = published.len() > LARGEST;
        // On a character boundary, because the document is JSON and
        // JSON is UTF-8: cutting a multi-byte character in half would
        // make the kept text something no reader can display.
        let cut = if truncated {
            boundary(published, LARGEST)
        } else {
            published.len()
        };
        let mut kept = self.kept.lock();
        kept.produced += 1;
        let letter = DeadLetter {
            sequence: kept.produced,
            at,
            topic: topic.to_owned(),
            document: String::from_utf8_lossy(&published[..cut]).into_owned(),
            truncated,
        };
        if kept.letters.len() == KEPT {
            kept.letters.pop_front();
        }
        kept.letters.push_back(letter);
    }

    /// The letters this rule has kept, oldest first.
    pub fn recent(&self) -> Vec<DeadLetter> {
        self.kept.lock().letters.iter().cloned().collect()
    }

    /// How many letters this rule has produced, including those the
    /// ring has dropped.
    ///
    /// Reported beside the letters, so a page saying "the last ten"
    /// can say how many there were.
    pub fn produced(&self) -> u64 {
        self.kept.lock().produced
    }
}

/// The largest index at or below `at` that ends a UTF-8 character.
///
/// A continuation byte is `10xxxxxx`; anything else starts a
/// character. Walking back at most three bytes is enough, because no
/// encoded character is longer than four.
fn boundary(bytes: &[u8], at: usize) -> usize {
    let mut cut = at.min(bytes.len());
    while cut > 0 && bytes[cut] & 0b1100_0000 == 0b1000_0000 {
        cut -= 1;
    }
    cut
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ring_keeps_the_last_letters_and_counts_them_all() {
        let log = DeadLetterLog::new();
        for n in 0..KEPT + 3 {
            log.record(
                "$sd/rules/one/dlq",
                1_700_000_000,
                format!("{n}").as_bytes(),
            );
        }
        let kept = log.recent();
        assert_eq!(kept.len(), KEPT);
        assert_eq!(kept[0].document, "3", "the oldest letters did not fall out");
        assert_eq!(kept[KEPT - 1].document, format!("{}", KEPT + 2));
        assert_eq!(
            log.produced(),
            u64::try_from(KEPT + 3).expect("a small number"),
            "the ring forgot how many there had been"
        );
        assert_eq!(kept[0].sequence, 4, "the numbering restarted");
    }

    #[test]
    fn a_letter_over_the_limit_is_cut_and_says_so() {
        let log = DeadLetterLog::new();
        log.record("$sd/rules/one/dlq", 0, &vec![b'x'; LARGEST + 100]);
        let kept = log.recent();
        assert!(kept[0].truncated);
        assert_eq!(kept[0].document.len(), LARGEST);

        log.record("$sd/rules/one/dlq", 0, b"short");
        let kept = log.recent();
        assert!(
            !kept[1].truncated,
            "a letter that fits was called truncated"
        );
        assert_eq!(kept[1].document, "short");
    }

    /// The cut is on a character boundary, because the document is
    /// JSON and half a character is text nothing can display.
    #[test]
    fn a_letter_is_cut_where_a_character_ends() {
        let log = DeadLetterLog::new();
        // A three-byte character straddling the limit: the padding
        // ends one byte before it, so the cut lands inside the é.
        let mut published = vec![b'x'; LARGEST - 1];
        published.extend_from_slice("é".as_bytes());
        published.extend(std::iter::repeat_n(b'x', 100));
        log.record("$sd/rules/one/dlq", 0, &published);

        let kept = log.recent();
        assert!(kept[0].truncated);
        assert_eq!(
            kept[0].document.len(),
            LARGEST - 1,
            "the cut split a character rather than stopping before it"
        );
        assert!(
            !kept[0].document.contains('\u{fffd}'),
            "the kept text has a replacement character in it, so a character was split"
        );
    }
}
