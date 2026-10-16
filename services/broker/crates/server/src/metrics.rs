//! What the broker counts, and what a scraper reads.
//!
//! # Every label combination is resolved once, at startup
//!
//! A [`Family`] is a read-write-locked map from a label set to a
//! metric, and the obvious way to use one — `get_or_create(&labels).inc()`
//! — holds the read guard across the increment. Doing that on the
//! packet path would put a shared lock acquisition on every packet on
//! every worker thread, which is the pathology this project has
//! already measured once: a shared lock cost it three quarters of its
//! throughput, and it declined a timer design over the same thing.
//!
//! The metric behind a family is an atomic behind a reference count,
//! so an owned handle taken out of the family once makes every later
//! increment a bare atomic add with no lock anywhere near it. The
//! label cross-product here is bounded and static — packet types by
//! version, drop reasons, socket outcomes — so every combination is
//! resolved into a flat array in [`Metrics::new`] and the family is
//! never touched again.
//!
//! That is why the handles are indexed by discriminant rather than
//! looked up by label: an index into an array cannot accidentally
//! become a hash of a label set.
//!
//! # The registry is behind a lock and that is not a contradiction
//!
//! [`Registry`] is touched exactly twice: once here while the handles
//! are being resolved, and once per scrape by [`Metrics::render`].
//! Nothing on the packet path goes near it. A scrape every fifteen
//! seconds taking a mutex is not a cost worth designing around; an
//! increment taking one is.
//!
//! # Label values are written out by hand
//!
//! `prometheus-client`'s derive takes an enum's variant names
//! verbatim, which would give `Publish` where the Prometheus
//! convention is `publish`. The label values here are therefore
//! written out rather than derived, and the exposition test asserts
//! the text so that a capital letter is a failure rather than a
//! surprise in somebody's dashboard.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::{ConstGauge, Gauge};
use prometheus_client::registry::Registry;

use broker_core::hooks::Refusal;
use broker_core::session::DropReason;

use crate::shared::Shared;
use mqtt_codec::{PacketKind, ReasonCode, Version};

/// The label set of every family in this module: a fixed number of
/// borrowed name-and-value pairs.
///
/// Deliberately not a derived struct. Nothing looks a metric up by its
/// labels after startup, so the label set is a startup-only value and
/// the simplest thing that can carry one is right.
type Labels2 = [(&'static str, &'static str); 2];

/// Every packet type, in the order the handles are indexed in.
///
/// An array rather than a `match` arm per type, because the array is
/// what guarantees a handle exists for each: adding a variant to
/// [`PacketKind`] makes [`kind_label`] fail to compile, and the length
/// here fails to compile with it.
const PACKET_KINDS: [PacketKind; 15] = [
    PacketKind::Connect,
    PacketKind::ConnAck,
    PacketKind::Publish,
    PacketKind::PubAck,
    PacketKind::PubRec,
    PacketKind::PubRel,
    PacketKind::PubComp,
    PacketKind::Subscribe,
    PacketKind::SubAck,
    PacketKind::Unsubscribe,
    PacketKind::UnsubAck,
    PacketKind::PingReq,
    PacketKind::PingResp,
    PacketKind::Disconnect,
    PacketKind::Auth,
];

/// Every protocol version this broker serves.
const VERSIONS: [Version; 2] = [Version::V311, Version::V5];

/// The label value for a packet type.
///
/// Exhaustive on purpose: a new packet type is a compile error here
/// rather than a metric that silently stops distinguishing it.
fn kind_label(kind: PacketKind) -> &'static str {
    match kind {
        PacketKind::Connect => "connect",
        PacketKind::ConnAck => "connack",
        PacketKind::Publish => "publish",
        PacketKind::PubAck => "puback",
        PacketKind::PubRec => "pubrec",
        PacketKind::PubRel => "pubrel",
        PacketKind::PubComp => "pubcomp",
        PacketKind::Subscribe => "subscribe",
        PacketKind::SubAck => "suback",
        PacketKind::Unsubscribe => "unsubscribe",
        PacketKind::UnsubAck => "unsuback",
        PacketKind::PingReq => "pingreq",
        PacketKind::PingResp => "pingresp",
        PacketKind::Disconnect => "disconnect",
        PacketKind::Auth => "auth",
    }
}

/// Where a packet type's handle sits in the flat array.
fn kind_index(kind: PacketKind) -> usize {
    match kind {
        PacketKind::Connect => 0,
        PacketKind::ConnAck => 1,
        PacketKind::Publish => 2,
        PacketKind::PubAck => 3,
        PacketKind::PubRec => 4,
        PacketKind::PubRel => 5,
        PacketKind::PubComp => 6,
        PacketKind::Subscribe => 7,
        PacketKind::SubAck => 8,
        PacketKind::Unsubscribe => 9,
        PacketKind::UnsubAck => 10,
        PacketKind::PingReq => 11,
        PacketKind::PingResp => 12,
        PacketKind::Disconnect => 13,
        PacketKind::Auth => 14,
    }
}

/// The label value for a protocol version.
///
/// The number as the standard writes it, not the identifier this code
/// spells it with: a dashboard reads `3.1.1`, not `V311`.
fn version_label(version: Version) -> &'static str {
    match version {
        Version::V311 => "3.1.1",
        Version::V5 => "5",
    }
}

/// Where a version's handles sit in the flat array.
fn version_index(version: Version) -> usize {
    match version {
        Version::V311 => 0,
        Version::V5 => 1,
    }
}

/// Every reason code the standard assigns, in the order the handles
/// are indexed in.
const ALL_REASONS: [ReasonCode; 43] = [
    ReasonCode::Success,
    ReasonCode::GrantedQos1,
    ReasonCode::GrantedQos2,
    ReasonCode::DisconnectWithWillMessage,
    ReasonCode::NoMatchingSubscribers,
    ReasonCode::NoSubscriptionExisted,
    ReasonCode::ContinueAuthentication,
    ReasonCode::ReAuthenticate,
    ReasonCode::UnspecifiedError,
    ReasonCode::MalformedPacket,
    ReasonCode::ProtocolError,
    ReasonCode::ImplementationSpecificError,
    ReasonCode::UnsupportedProtocolVersion,
    ReasonCode::ClientIdentifierNotValid,
    ReasonCode::BadUserNameOrPassword,
    ReasonCode::NotAuthorized,
    ReasonCode::ServerUnavailable,
    ReasonCode::ServerBusy,
    ReasonCode::Banned,
    ReasonCode::ServerShuttingDown,
    ReasonCode::BadAuthenticationMethod,
    ReasonCode::KeepAliveTimeout,
    ReasonCode::SessionTakenOver,
    ReasonCode::TopicFilterInvalid,
    ReasonCode::TopicNameInvalid,
    ReasonCode::PacketIdentifierInUse,
    ReasonCode::PacketIdentifierNotFound,
    ReasonCode::ReceiveMaximumExceeded,
    ReasonCode::TopicAliasInvalid,
    ReasonCode::PacketTooLarge,
    ReasonCode::MessageRateTooHigh,
    ReasonCode::QuotaExceeded,
    ReasonCode::AdministrativeAction,
    ReasonCode::PayloadFormatInvalid,
    ReasonCode::RetainNotSupported,
    ReasonCode::QosNotSupported,
    ReasonCode::UseAnotherServer,
    ReasonCode::ServerMoved,
    ReasonCode::SharedSubscriptionsNotSupported,
    ReasonCode::ConnectionRateExceeded,
    ReasonCode::MaximumConnectTime,
    ReasonCode::SubscriptionIdentifiersNotSupported,
    ReasonCode::WildcardSubscriptionsNotSupported,
];

/// Where a reason code's handle sits in the flat array, by its wire
/// byte.
///
/// Built at compile time from [`ALL_REASONS`] so that the two cannot
/// disagree. A byte no code uses is [`u8::MAX`], which
/// [`reason_index`] can never be handed: its argument is the enum,
/// and every value of the enum is in the table.
const REASON_SLOT: [u8; 256] = {
    let mut table = [u8::MAX; 256];
    let mut at = 0;
    while at < ALL_REASONS.len() {
        table[ALL_REASONS[at] as usize] = at as u8;
        at += 1;
    }
    table
};

/// The label value for a reason code.
///
/// Exhaustive, so a code added to the standard and to the codec is a
/// compile error here rather than a series that quietly never appears.
fn reason_label(reason: ReasonCode) -> &'static str {
    match reason {
        ReasonCode::Success => "success",
        ReasonCode::GrantedQos1 => "granted_qos1",
        ReasonCode::GrantedQos2 => "granted_qos2",
        ReasonCode::DisconnectWithWillMessage => "disconnect_with_will_message",
        ReasonCode::NoMatchingSubscribers => "no_matching_subscribers",
        ReasonCode::NoSubscriptionExisted => "no_subscription_existed",
        ReasonCode::ContinueAuthentication => "continue_authentication",
        ReasonCode::ReAuthenticate => "re_authenticate",
        ReasonCode::UnspecifiedError => "unspecified_error",
        ReasonCode::MalformedPacket => "malformed_packet",
        ReasonCode::ProtocolError => "protocol_error",
        ReasonCode::ImplementationSpecificError => "implementation_specific_error",
        ReasonCode::UnsupportedProtocolVersion => "unsupported_protocol_version",
        ReasonCode::ClientIdentifierNotValid => "client_identifier_not_valid",
        ReasonCode::BadUserNameOrPassword => "bad_user_name_or_password",
        ReasonCode::NotAuthorized => "not_authorized",
        ReasonCode::ServerUnavailable => "server_unavailable",
        ReasonCode::ServerBusy => "server_busy",
        ReasonCode::Banned => "banned",
        ReasonCode::ServerShuttingDown => "server_shutting_down",
        ReasonCode::BadAuthenticationMethod => "bad_authentication_method",
        ReasonCode::KeepAliveTimeout => "keep_alive_timeout",
        ReasonCode::SessionTakenOver => "session_taken_over",
        ReasonCode::TopicFilterInvalid => "topic_filter_invalid",
        ReasonCode::TopicNameInvalid => "topic_name_invalid",
        ReasonCode::PacketIdentifierInUse => "packet_identifier_in_use",
        ReasonCode::PacketIdentifierNotFound => "packet_identifier_not_found",
        ReasonCode::ReceiveMaximumExceeded => "receive_maximum_exceeded",
        ReasonCode::TopicAliasInvalid => "topic_alias_invalid",
        ReasonCode::PacketTooLarge => "packet_too_large",
        ReasonCode::MessageRateTooHigh => "message_rate_too_high",
        ReasonCode::QuotaExceeded => "quota_exceeded",
        ReasonCode::AdministrativeAction => "administrative_action",
        ReasonCode::PayloadFormatInvalid => "payload_format_invalid",
        ReasonCode::RetainNotSupported => "retain_not_supported",
        ReasonCode::QosNotSupported => "qos_not_supported",
        ReasonCode::UseAnotherServer => "use_another_server",
        ReasonCode::ServerMoved => "server_moved",
        ReasonCode::SharedSubscriptionsNotSupported => "shared_subscriptions_not_supported",
        ReasonCode::ConnectionRateExceeded => "connection_rate_exceeded",
        ReasonCode::MaximumConnectTime => "maximum_connect_time",
        ReasonCode::SubscriptionIdentifiersNotSupported => "subscription_identifiers_not_supported",
        ReasonCode::WildcardSubscriptionsNotSupported => "wildcard_subscriptions_not_supported",
    }
}

/// Where a reason code's handle sits in the flat array.
fn reason_index(reason: ReasonCode) -> usize {
    usize::from(REASON_SLOT[reason as usize])
}

/// The two packets this broker puts a connection-level reason code on.
///
/// A CONNACK says why a connection was refused or accepted; a
/// DISCONNECT says why one is ending. The per-subscription and
/// per-publication codes a SUBACK or a PUBACK carries are not here:
/// they belong to one request rather than to the connection, and
/// section 11.3's reason-code dimension is about why a client's time
/// with this broker went the way it did.
const REASON_KINDS: [PacketKind; 2] = [PacketKind::ConnAck, PacketKind::Disconnect];

/// How a socket ended.
///
/// The cross-product this labels is [`ConnectionError`]'s variants
/// plus the two endings that are not errors, which is why it is an
/// enum here rather than a string built at the call site: a new way
/// for a connection to end has to be given a label before it
/// compiles.
///
/// [`ConnectionError`]: crate::connection::ConnectionError
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketOutcome {
    /// The client closed, or said goodbye, and nothing went wrong.
    Clean,
    /// No connect packet arrived in time.
    ConnectTimeout,
    /// The first packet was not a connect.
    NotAConnect,
    /// The peer sent something that is not a valid packet.
    Malformed,
    /// The peer broke the protocol and was told which rule.
    Protocol,
    /// The socket failed, or ended partway through a packet.
    Io,
    /// Something this broker produced could not be encoded.
    Codec,
    /// A publication could not be made durable, so the client was
    /// closed rather than acknowledged.
    NotDurable,
    /// The cluster was short of its expected size and this node is
    /// configured to refuse, section 8.5.
    ///
    /// **Its own outcome and not `Protocol`**, because it is the one
    /// ending in the list that an operator asked for: the flag that
    /// produces it is one they set. A refusal that an acknowledgement
    /// could carry does not end a socket at all and is counted by its
    /// reason code instead, so what this counts is the subset the
    /// protocol left no room to answer — 3.1.1 publishes, and
    /// at-most-once ones.
    NotWhole,
    /// A durable publish no copy confirmed, on a node configured to
    /// refuse those. Section 8.6.
    NotConfirmed,
    /// The connection was dropped before it finished: a shutdown, or
    /// a panic. Counted from [`Drop`] rather than from a return, so
    /// that a cancelled connection is an ending like any other rather
    /// than a socket that is open forever in the exposition.
    Abandoned,
}

/// Every socket outcome, in the order the handles are indexed in.
const SOCKET_OUTCOMES: [SocketOutcome; 11] = [
    SocketOutcome::Clean,
    SocketOutcome::ConnectTimeout,
    SocketOutcome::NotAConnect,
    SocketOutcome::Malformed,
    SocketOutcome::Protocol,
    SocketOutcome::Io,
    SocketOutcome::Codec,
    SocketOutcome::NotDurable,
    SocketOutcome::Abandoned,
    // Appended rather than slotted in beside `NotDurable`, which it
    // reads next to: this array's positions **are** the indices
    // `outcome_index` returns, so inserting one in the middle
    // renumbers every outcome after it and relabels their counters.
    SocketOutcome::NotWhole,
    SocketOutcome::NotConfirmed,
];

/// The label value for a socket outcome.
fn outcome_label(outcome: SocketOutcome) -> &'static str {
    match outcome {
        SocketOutcome::Clean => "clean",
        SocketOutcome::ConnectTimeout => "connect_timeout",
        SocketOutcome::NotAConnect => "not_a_connect",
        SocketOutcome::Malformed => "malformed",
        SocketOutcome::Protocol => "protocol",
        SocketOutcome::Io => "io",
        SocketOutcome::Codec => "codec",
        SocketOutcome::NotDurable => "not_durable",
        SocketOutcome::NotWhole => "not_whole",
        SocketOutcome::NotConfirmed => "not_confirmed",
        SocketOutcome::Abandoned => "abandoned",
    }
}

/// Where a socket outcome's handle sits in the flat array.
fn outcome_index(outcome: SocketOutcome) -> usize {
    match outcome {
        SocketOutcome::Clean => 0,
        SocketOutcome::ConnectTimeout => 1,
        SocketOutcome::NotAConnect => 2,
        SocketOutcome::Malformed => 3,
        SocketOutcome::Protocol => 4,
        SocketOutcome::Io => 5,
        SocketOutcome::Codec => 6,
        SocketOutcome::NotDurable => 7,
        SocketOutcome::Abandoned => 8,
        SocketOutcome::NotWhole => 9,
        SocketOutcome::NotConfirmed => 10,
    }
}

/// How an authentication ended.
///
/// Registered before authentication existed, at zero rather than
/// missing, so a dashboard written against an earlier build kept
/// working; since milestone 7 both move. A family with no series is
/// skipped by the encoder entirely, which is why the handles are
/// resolved here like every other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthOutcome {
    /// The client proved who it said it was.
    Success,
    /// It did not, and was refused.
    Failure,
}

/// Which of section 10.3's limiters turned something away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limiter {
    /// A session's publish cap: the connection was closed with 0x96.
    Publish,
    /// An address's authentication failures: a CONNECT was refused
    /// before its credentials were looked at.
    Authentication,
}

/// Every limiter, in the order the handles are indexed in.
const LIMITERS: [Limiter; 2] = [Limiter::Publish, Limiter::Authentication];

/// The label value for a limiter.
fn limiter_label(limiter: Limiter) -> &'static str {
    match limiter {
        Limiter::Publish => "publish",
        Limiter::Authentication => "authentication",
    }
}

/// Every authentication outcome, in the order the handles are indexed
/// in.
const AUTH_OUTCOMES: [AuthOutcome; 2] = [AuthOutcome::Success, AuthOutcome::Failure];

/// The label value for an authentication outcome.
fn auth_label(outcome: AuthOutcome) -> &'static str {
    match outcome {
        AuthOutcome::Success => "success",
        AuthOutcome::Failure => "failure",
    }
}

/// Where an authentication outcome's handle sits in the flat array.
fn auth_index(outcome: AuthOutcome) -> usize {
    match outcome {
        AuthOutcome::Success => 0,
        AuthOutcome::Failure => 1,
    }
}

/// Declare a reason table and its label function from one list.
///
/// One list, because two would drift, and a reason that drifts out of
/// the table is a message discarded against a counter that does not
/// exist — which is precisely what section 11.3 forbids.
///
/// The match this expands to has no wildcard, so **a variant added to
/// the enum does not compile** until it is added here, and adding it
/// here adds it to the table, gives it a label and gives it a slot in
/// one edit. That is the exhaustiveness mechanism: compilation, not a
/// list somebody has to remember to extend.
macro_rules! reason_table {
    ($enum:ident, $all:ident, $label:ident, $index:ident, { $($variant:ident => $text:literal,)+ }) => {
        /// Every variant, in the order the handles are indexed in.
        pub(crate) const $all: &[$enum] = &[$($enum::$variant,)+];

        /// The label value, lower case and written out by hand.
        pub(crate) fn $label(reason: $enum) -> &'static str {
            match reason {
                $($enum::$variant => $text,)+
            }
        }

        /// Where this variant's handle sits in the flat array.
        ///
        /// A scan of a table of single digits' length, and it is on
        /// the discard path rather than the packet path: by the time
        /// this is called a message has already been lost, which is
        /// not a moment to be counting comparisons. Nothing here
        /// hashes and nothing here locks, which is what the packet
        /// path could not afford.
        fn $index(reason: $enum) -> usize {
            $all
                .iter()
                .position(|candidate| *candidate == reason)
                .expect("the table and the match come from one list")
        }
    };
}

reason_table!(DropReason, ALL_DROP_REASONS, drop_label, drop_index, {
    QueueFull => "queue_full",
    ByteBudget => "byte_budget",
    QosZeroWhileOffline => "qos0_while_offline",
    Expired => "expired",
    SessionEnded => "session_ended",
    Migrated => "migrated",
    RejectedByPeer => "rejected_by_peer",
    TooLargeForPeer => "too_large_for_peer",
    Refused => "refused",
});

/// Why the shell discarded something, where `broker-core` cannot see
/// it.
///
/// The broker reports its own discards as an action carrying a
/// [`DropReason`], and every one of those is counted. These are the
/// ones that happen afterwards, in this crate, between the broker
/// saying what to do and the socket taking the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellDrop {
    /// The session's outbox was full. Its depth is derived from the
    /// configured bounds and a configuration that breaks the relation
    /// is refused at startup, so this should be unreachable; it is
    /// counted rather than assumed away.
    OutboxFull,
    /// The session had no outbox, or its reader had already gone. An
    /// ordinary teardown most of the time — and the path a drop
    /// report for an *offline* session takes, which is why the report
    /// itself is counted before this is.
    NoReader,
    /// The encoder refused a packet the broker produced. A fault in
    /// this process rather than in the peer, and a message that goes
    /// nowhere either way.
    NotEncodable,
    /// A packet was larger than the peer allows and carried nothing
    /// the broker could be told to settle: an at-most-once
    /// publication, which owes no acknowledgement and therefore gets
    /// no `Action::Dropped` of its own.
    TooLargeToReport,
    /// This connection's own actions, discarded because the batch
    /// that produced them could not be made durable and the client is
    /// being closed instead.
    NotDurable,
}

reason_table!(ShellDrop, ALL_SHELL_DROPS, shell_label, shell_index, {
    OutboxFull => "outbox_full",
    NoReader => "no_reader",
    NotEncodable => "not_encodable",
    TooLargeToReport => "too_large_to_report",
    NotDurable => "not_durable",
});

/// Why a journal record went nowhere.
///
/// Records rather than messages, and a separate family for that
/// reason: a message a subscriber did not get comes back when its
/// session resumes, and a record the store never learnt does not come
/// back at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordDrop {
    /// The storage writer's thread has stopped. Nothing sent from
    /// then on is written and nothing says so again.
    WriterStopped,
}

reason_table!(RecordDrop, ALL_RECORD_DROPS, record_drop_label, record_drop_index, {
    WriterStopped => "writer_stopped",
});

// Why a publication a connector made went nowhere.
//
// Section 9's connectors, through the same mechanism as everything
// above: `Refusal` is declared in `broker_core::hooks` and is
// matched here without a wildcard, so **a refusal a connector learns
// to make does not compile until it has a counter**.
//
// Milestone four is why this exists. It introduced three refusal
// vocabularies the `DropReason` table could not see — a bridge's
// loop marker, the shell's `PublishRefused`, and a per-connection
// tally inside a bridge's session that reset on every reconnection —
// and counted none of them. A message refused by a bridge's loop
// marker was logged at debug and counted nowhere, which is section
// 11.3's sentence broken by a whole milestone rather than by one
// path.
reason_table!(Refusal, ALL_REFUSALS, refusal_label, refusal_index, {
    Looped => "looped",
    TooManyHops => "too_many_hops",
    NotATopic => "not_a_topic",
    Reserved => "reserved",
    NotDurable => "not_durable",
    NotConfirmed => "not_confirmed",
});

/// What a rule did, as a label value.
///
/// Section 11.3 does not list per-rule counters, and these are a
/// deliberate addition rather than a line of it being filled in. The
/// argument is section 9's: a rule is a connector to somebody else's
/// system, the way an operator finds out one has stopped working is an
/// alert, and a total across every rule cannot say which one stopped.
///
/// **The cardinality is a number an operator wrote.** A rule
/// identifier is a label where a client identifier deliberately is not
/// one, because rules are created by the person running the broker and
/// counted in tens, while clients arrive from outside and are counted
/// in hundreds of thousands.
///
/// A deleted rule keeps its series, at the value it stopped at. That
/// is the right way round: a counter that disappears is one a
/// dashboard reads as a reset to zero.
/// # Four of these are a series and five are not
///
/// The first four are also `rules::stats`' ring, which the Rules page
/// draws per row. The five below them are exported and not sampled,
/// and the line between the two is what a 96-pixel cell is for: a
/// shape over the last minute. "This bridge connected" is not a shape;
/// it is an event an alert fires on, and a per-second difference of it
/// is a row of zeros with an occasional one. They are counted where an
/// alert can read them and left out of where they would say nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleEvent {
    /// A message matched it, a request arrived for it, or its interval
    /// came round.
    Fired,
    /// Something reached where the rule was sending it.
    Delivered,
    /// An attempt failed.
    Failed,
    /// An envelope ran out of attempts.
    DeadLettered,
    /// A request went out to the rule's endpoint.
    ///
    /// Attempts rather than envelopes, so `attempted` over `delivered`
    /// is what it costs this rule to deliver one thing — which is the
    /// number that says an endpoint is flaky rather than dead.
    Attempted,
    /// One of those attempts was not the first for its envelope.
    ///
    /// A subset of [`RuleEvent::Attempted`] and deliberately so: an
    /// operator sizing a retry policy wants to know how much of the
    /// traffic to an endpoint is this broker asking again.
    Retried,
    /// A bridge established a connection to its remote.
    ///
    /// Was a log line and nothing else, which cannot be alerted on. A
    /// bridge that reconnects every thirty seconds is working
    /// perfectly by every other number this broker publishes.
    Connected,
    /// A bridge's connection to its remote ended.
    ///
    /// Counted only for a connection that was established, so that
    /// this and [`RuleEvent::Connected`] pair: their difference is
    /// whether the bridge is up now.
    Disconnected,
    /// A bridge took a message from its remote and published it here.
    ///
    /// This was a field in a per-connection struct whose only consumer
    /// was one log line and which reset on every reconnection, so the
    /// question "how much has this bridge brought in" had no answer at
    /// all.
    Received,
}

/// Every rule outcome, in the order the handles are indexed in.
const RULE_EVENTS: [RuleEvent; 9] = [
    RuleEvent::Fired,
    RuleEvent::Delivered,
    RuleEvent::Failed,
    RuleEvent::DeadLettered,
    RuleEvent::Attempted,
    RuleEvent::Retried,
    RuleEvent::Connected,
    RuleEvent::Disconnected,
    RuleEvent::Received,
];

/// The label value for a rule outcome.
fn rule_event_label(event: RuleEvent) -> &'static str {
    match event {
        RuleEvent::Fired => "fired",
        RuleEvent::Delivered => "delivered",
        RuleEvent::Failed => "failed",
        RuleEvent::DeadLettered => "dead_lettered",
        RuleEvent::Attempted => "attempted",
        RuleEvent::Retried => "retried",
        RuleEvent::Connected => "connected",
        RuleEvent::Disconnected => "disconnected",
        RuleEvent::Received => "received",
    }
}

/// Where a rule outcome's handle sits in the flat array.
fn rule_event_index(event: RuleEvent) -> usize {
    match event {
        RuleEvent::Fired => 0,
        RuleEvent::Delivered => 1,
        RuleEvent::Failed => 2,
        RuleEvent::DeadLettered => 3,
        RuleEvent::Attempted => 4,
        RuleEvent::Retried => 5,
        RuleEvent::Connected => 6,
        RuleEvent::Disconnected => 7,
        RuleEvent::Received => 8,
    }
}

/// One rule's owned handles.
///
/// Resolved when the rule is installed and never looked up again,
/// which is the shape every other family in this module has: nothing
/// searches a label set while a request or a packet is being served.
#[derive(Debug)]
pub struct RuleCounters {
    events: [Counter; RULE_EVENTS.len()],
    waiting: Gauge,
}

impl RuleCounters {
    /// The counter for one outcome. An index, not a lookup.
    pub fn event(&self, event: RuleEvent) -> &Counter {
        &self.events[rule_event_index(event)]
    }

    /// How many envelopes are waiting in this rule's outbox.
    ///
    /// A gauge rather than a counter, and **set rather than added
    /// to**: it is a depth. The sampler that advances the rule's own
    /// series sets it, so a scrape reads a number that was already
    /// there rather than one it went looking for.
    pub fn waiting(&self) -> &Gauge {
        &self.waiting
    }
}

/// One counter per packet type per version, resolved at startup.
#[derive(Debug)]
struct PacketCounters([[Counter; PACKET_KINDS.len()]; VERSIONS.len()]);

impl PacketCounters {
    /// Register the family and take an owned handle for every
    /// combination of type and version.
    fn resolve(registry: &mut Registry, name: &'static str, help: &'static str) -> Self {
        let family = Family::<Labels2, Counter>::default();
        registry.register(name, help, family.clone());
        Self(std::array::from_fn(|version| {
            std::array::from_fn(|kind| {
                family.get_or_create_owned(&[
                    ("kind", kind_label(PACKET_KINDS[kind])),
                    ("version", version_label(VERSIONS[version])),
                ])
            })
        }))
    }

    /// The handle for one type and version. An index, not a lookup.
    fn get(&self, version: Version, kind: PacketKind) -> &Counter {
        &self.0[version_index(version)][kind_index(kind)]
    }
}

/// Section 12.1's TLS counters. See [`Metrics::tls`].
#[derive(Debug)]
pub struct TlsCounters {
    /// Handshakes that did not complete, by error or by the connect
    /// timeout.
    pub handshakes_failed: Counter,
    reloads: Family<[(&'static str, &'static str); 1], Counter>,
}

impl TlsCounters {
    /// Counters nothing renders: for a test, and for [`Metrics::tls`]
    /// to register.
    #[must_use]
    pub fn detached() -> Arc<Self> {
        Arc::new(Self {
            handshakes_failed: Counter::default(),
            reloads: Family::default(),
        })
    }

    /// A certificate pair re-read and taken.
    pub fn reloaded(&self) -> Counter {
        self.reloads.get_or_create_owned(&[("outcome", "reloaded")])
    }

    /// A certificate pair that could not be taken, the old one kept.
    pub fn reload_failed(&self) -> Counter {
        self.reloads.get_or_create_owned(&[("outcome", "failed")])
    }
}

/// Every counter and gauge this broker publishes.
#[derive(Debug)]
pub struct Metrics {
    registry: Mutex<Registry>,
    packets_received: PacketCounters,
    packets_sent: PacketCounters,
    /// Bytes counted where they cross the socket, not by adding up
    /// what the packets should have weighed. The whole point of a byte
    /// counter is to be able to see the difference.
    bytes_received: Counter,
    bytes_sent: Counter,
    /// Section 8.6's synchronous publishes that no copy confirmed.
    publishes_unconfirmed: Counter,
    sockets_opened: Counter,
    sockets_refused: Counter,
    /// Section 11.3, for the admin events stream: a reader that has
    /// fallen behind loses events, and none of them may disappear
    /// uncounted. See [`Metrics::admin_events_dropped`].
    admin_events_dropped: Counter,
    /// Times a listener stopped accepting for a moment because accepting
    /// failed for the whole process — descriptor exhaustion, in practice.
    accept_pauses: Counter,
    /// Section 8.3 forwards that found no subscriber here because the
    /// session had just moved, and were sent on to where it went.
    forwards_redirected: Counter,
    /// Section 8.3 forwards that found no subscriber here and nowhere
    /// to send them on: dropped, and counted rather than only logged.
    forwards_unroutable: Counter,
    /// Section 9's connectors, by why a publication one of them made
    /// went nowhere. One family rather than a counter per refusal,
    /// because an operator wants one number for "a connector lost a
    /// message" broken down by why. See
    /// [`Metrics::publication_refused`] and [`Refusal`].
    publications_refused: Box<[Counter]>,
    sockets_closed: [Counter; SOCKET_OUTCOMES.len()],
    /// Indexed by [`REASON_KINDS`] then by [`ALL_REASONS`].
    reasons_sent: [[Counter; ALL_REASONS.len()]; REASON_KINDS.len()],
    /// Indexed by [`AUTH_OUTCOMES`].
    authentications: [Counter; AUTH_OUTCOMES.len()],
    /// Section 10.3: what each limiter turned away. Indexed by
    /// [`LIMITERS`].
    rate_limited: [Counter; LIMITERS.len()],
    /// How many addresses the authentication limiter is holding, as of
    /// the last sweep.
    auth_failure_addresses: Gauge,
    /// Section 11.3: a message never disappears without being
    /// counted. One family, two tables — the broker's own reasons and
    /// the shell's — because an operator wants one number for
    /// "messages lost" broken down by why.
    dropped_by_queue: Box<[Counter]>,
    dropped_by_shell: Box<[Counter]>,
    records_dropped: Box<[Counter]>,
    /// Section 9's rules, by identifier and outcome. See
    /// [`RuleEvent`]. Handles are taken by [`Metrics::rule`] when a
    /// rule is installed.
    rule_events: Family<Vec<(String, String)>, Counter>,
    /// How much each rule has waiting. See [`RuleCounters::waiting`].
    rule_waiting: Family<Vec<(String, String)>, Gauge>,
    /// Whether [`Metrics::watch`] has already run. A registry with
    /// two collectors over the same numbers would render every one of
    /// them twice, which a scraper reads as a duplicate series.
    watched: AtomicBool,
    /// The same, for [`Metrics::watch_cluster`].
    watching_cluster: AtomicBool,
    /// Section 12.1's TLS series, registered on the first TLS listener
    /// and absent, not zero, on a node with none. See [`Metrics::tls`].
    tls: OnceLock<Arc<TlsCounters>>,
    /// Refused WebSocket upgrades, by reason; the same rule.
    websocket_refused: OnceLock<Family<[(&'static str, &'static str); 1], Counter>>,
    /// Whether [`Metrics::record_build`] has run.
    build_recorded: OnceLock<()>,
}

/// What a running cluster can be asked for, once there is one.
///
/// Section 11.3's cluster numbers: **the members gauge, and bytes
/// sent, received and dropped on the links.** All four were deferred
/// to milestone 5 and recorded as deferred — a counter that can only
/// ever read zero says something false rather than something empty,
/// and a membership of zero is wrong rather than empty because this
/// node is a member of its own cluster of one.
///
/// A trait rather than the mesh itself, for the reason
/// `sdmqtt_admin::Cluster` is one: what this needs of section 8 is
/// four numbers, and taking the mesh's type would make this crate
/// unbuildable without a transport, a handshake and a failure
/// detector.
pub trait ClusterMetrics: std::fmt::Debug + Send + Sync + 'static {
    /// How many nodes this one can see, **itself included**.
    fn members(&self) -> u64;
    /// Bytes written to peers.
    fn bytes_sent(&self) -> u64;
    /// Bytes read from peers.
    fn bytes_received(&self) -> u64;
    /// Bytes never written, because a peer's queue was full.
    fn bytes_dropped(&self) -> u64;
    /// Section 8.6's replication numbers, in the order
    /// [`REPLICATION_NUMBERS`] names them.
    fn replication(&self) -> [u64; REPLICATION_NUMBERS.len()];
    /// Changes this node has applied as a follower, ever. See
    /// [`REPLICA_APPLIED`].
    fn replica_applied(&self) -> u64;
    /// Sessions whose replica set was short at the last sweep, and
    /// sessions re-streamed to a new follower ever: a gauge and a
    /// counter. See [`REPAIRS_PENDING`] and [`REPAIRS_DONE`].
    fn repairs(&self) -> (u64, u64);
    /// Section 8.4's migration numbers, in the order
    /// [`MIGRATION_NUMBERS`] names them.
    ///
    /// An array rather than seven methods, because every one of them
    /// is the same question asked of the same table and a trait with
    /// seven one-line methods is seven places for an implementation to
    /// answer one of them from the wrong field.
    fn migrations(&self) -> [u64; MIGRATION_NUMBERS.len()];
}

/// Section 8.6's replication numbers: the metric name, its help text,
/// and the order [`ClusterMetrics::replication`] returns them in.
///
/// **`replication_lag` is the one section 8.6 names.** In asynchronous
/// mode the loss window *is* the lag — what the owner has acknowledged
/// and no copy holds is what losing the owner would lose — so the
/// design says it is exposed and alerted on, and milestone 6's exit
/// criterion reads this series to bound what a kill may lose.
///
/// It is the **furthest** any follower is behind, across every session
/// this node owns, because the window is about what a failure would
/// cost: measured from the copy that happens to be closest it would
/// describe a failure that did not happen. One number rather than one
/// per session, because a gauge per session at the design's ceilings is
/// a hundred thousand series.
pub const REPLICATION_NUMBERS: [(&str, &str); 3] = [
    (
        "replication_lag",
        "Changes the furthest-behind copy of any session this node owns has not applied",
    ),
    (
        "replicated_sessions",
        "Sessions this node owns and streams to at least one other node",
    ),
    (
        "replica_sessions",
        "Sessions this node holds a copy of for another node",
    ),
];

/// Changes this node has applied as a follower, ever. Section 8.6.
///
/// A counter, beside the three gauges above, because it is the one
/// number that says a follower is still *doing* anything: the gauges
/// describe what this node holds, and a node that holds copies and
/// has quietly stopped applying to them looks identical on all three.
/// A flat line here on a node that is meant to be following is that
/// node. It is also the only way a test can know a follower has caught
/// up before it does something irreversible to the owner.
pub const REPLICA_APPLIED: (&str, &str) = (
    "replica_changes_applied",
    "Queue changes this node has applied to copies it holds for other nodes",
);

/// Sessions this node owns whose replica set is short. Section 8.6.
///
/// What section 8.6 means by repair progress being visible: the number
/// goes up when a node is gone and comes down as the sweep sends each
/// session's queue to the follower that replaced it, at the configured
/// rate. A gauge, because it goes both ways.
pub const REPAIRS_PENDING: (&str, &str) = (
    "replica_repairs_pending",
    "Sessions this node owns whose replica set is short of a copy",
);

/// Sessions re-streamed to a new follower, ever. A counter.
pub const REPAIRS_DONE: (&str, &str) = (
    "replica_repairs_done",
    "Sessions whose queue this node has re-streamed to a follower that held no copy",
);

/// Durable publishes acknowledged in synchronous mode that no follower
/// confirmed. Section 8.6.
///
/// **A counter rather than a gauge**, unlike the three above: this one
/// only ever goes up, and what an operator alerts on is the rate.
///
/// It exists because section 8.6 acknowledges such a publish anyway —
/// refusing would turn the loss of one node into an outage for every
/// durable publish — and an operator who asked for synchronous mode and
/// is getting this is not getting what they asked for. Silent, it would
/// be a guarantee quietly not kept.
pub const UNCONFIRMED: (&str, &str) = (
    "publishes_unconfirmed",
    "Durable publishes acknowledged in synchronous mode with no follower confirming",
);

/// Section 8.4's migration numbers: the metric name, its help text,
/// and the order [`ClusterMetrics::migrations`] returns them in.
///
/// **`sessions_migrated_incomplete` is the one section 8.4 promises.**
/// "On timeout, or when the previous owner cannot be reached, the
/// connection is accepted with what arrived, the session is reported
/// as present, and the shortfall is **counted and surfaced**" — it was
/// counted from milestone 5's second task and surfaced nowhere, which
/// is half of a sentence that reads as a whole one. The other six are
/// here because a shortfall with nothing to compare it against cannot
/// be read: a hundred incomplete drains is an outage or a quiet
/// afternoon depending on how many there were altogether.
pub const MIGRATION_NUMBERS: [(&str, &str); 9] = [
    (
        "sessions_claimed",
        "Sessions this node claimed from another node",
    ),
    (
        "sessions_migrated",
        "Claims whose drain finished before the wait ran out",
    ),
    (
        "sessions_migrated_incomplete",
        "Claims accepted without a finished drain, which is the shortfall of section 8.4",
    ),
    (
        "sessions_migrated_unreachable",
        "Of those, the ones with no link to the previous owner to ask over",
    ),
    (
        "migrated_messages",
        "Messages that arrived in a drain and were kept",
    ),
    (
        "migrated_messages_missing",
        "Messages a previous owner said it held and that never arrived",
    ),
    (
        "migrated_messages_late",
        "Chunks that arrived for a drain nothing was waiting for any more",
    ),
    (
        "sessions_pulled",
        "Claims whose queue came from a copy, because the owner could not be reached or did \
         not finish",
    ),
    (
        "pulled_messages",
        "Messages that arrived in a pull from a copy",
    ),
];

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Build the registry and resolve every label combination into an
    /// owned handle.
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("sdmqtt");
        let packets_received = PacketCounters::resolve(
            &mut registry,
            "packets_received",
            // The registry appends the full stop, so the help text
            // here ends without one.
            "Packets read from a client, by type and protocol version",
        );
        let packets_sent = PacketCounters::resolve(
            &mut registry,
            "packets_sent",
            "Packets written to a client, by type and protocol version",
        );

        let bytes_received = Counter::default();
        registry.register(
            "bytes_received",
            "Bytes read from client sockets",
            bytes_received.clone(),
        );
        let bytes_sent = Counter::default();
        registry.register(
            "bytes_sent",
            "Bytes written to client sockets",
            bytes_sent.clone(),
        );

        let publishes_unconfirmed = Counter::default();
        registry.register(UNCONFIRMED.0, UNCONFIRMED.1, publishes_unconfirmed.clone());

        let sockets_opened = Counter::default();
        registry.register(
            "sockets_opened",
            "Client sockets this broker began serving",
            sockets_opened.clone(),
        );
        let sockets_refused = Counter::default();
        registry.register(
            "sockets_refused",
            "Client sockets closed without being served, at the connection limit",
            sockets_refused.clone(),
        );
        let forwards_redirected = Counter::default();
        registry.register(
            "forwards_redirected",
            "Forwarded publications that arrived after their session had moved and were sent on to its new node",
            forwards_redirected.clone(),
        );
        let forwards_unroutable = Counter::default();
        registry.register(
            "forwards_unroutable",
            "Forwarded publications that found no subscriber here and no node to send them on to",
            forwards_unroutable.clone(),
        );
        let accept_pauses = Counter::default();
        registry.register(
            "accept_pauses",
            // No socket is refused when this moves: the connection
            // waits in the kernel's backlog until the pause ends. What
            // happened is that the process could not take it, which an
            // operator fixes by raising the descriptor limit.
            "Times accepting paused because the process could not take a connection, usually out of file descriptors",
            accept_pauses.clone(),
        );
        let admin_events_dropped = Counter::default();
        registry.register(
            "admin_events_dropped",
            // Section 11.3 forbids anything disappearing uncounted,
            // and an event a browser never saw is one of those. The
            // reader is told to resynchronise rather than handed a
            // gap, so this is a health signal rather than a loss;
            // that is what the help text has to say, because a
            // non-zero counter otherwise reads as data lost.
            "Admin events a stream did not receive because its reader fell behind; \
             each one is answered with a resynchronise rather than a gap",
            admin_events_dropped.clone(),
        );
        let refused = Family::<[(&'static str, &'static str); 1], Counter>::default();
        registry.register(
            "publications_refused",
            // Unlike the counter above these *are* losses, and two of
            // them are deliberate ones: a chain of connectors that
            // feeds itself is stopped by dropping the message that
            // would extend it, and a bridge refuses a message it has
            // already carried. A non-zero `looped` or `too_many_hops`
            // means there is a cycle to find; the other three mean a
            // connector is handing this broker something it will not
            // take.
            "Publications a connector made that went nowhere, by why; looped and \
             too_many_hops mean a rule is feeding a cycle",
            refused.clone(),
        );
        let publications_refused = ALL_REFUSALS
            .iter()
            .map(|reason| refused.get_or_create_owned(&[("reason", refusal_label(*reason))]))
            .collect();
        let closed = Family::<[(&'static str, &'static str); 1], Counter>::default();
        registry.register(
            "sockets_closed",
            "Client sockets that have finished, by how they ended",
            closed.clone(),
        );
        let sockets_closed = std::array::from_fn(|at| {
            closed.get_or_create_owned(&[("outcome", outcome_label(SOCKET_OUTCOMES[at]))])
        });

        let reasons = Family::<Labels2, Counter>::default();
        registry.register(
            "reason_codes_sent",
            "Connection-level reason codes this broker gave a client, by packet type",
            reasons.clone(),
        );
        let reasons_sent = std::array::from_fn(|kind| {
            std::array::from_fn(|reason| {
                reasons.get_or_create_owned(&[
                    ("kind", kind_label(REASON_KINDS[kind])),
                    ("reason", reason_label(ALL_REASONS[reason])),
                ])
            })
        });

        let auth = Family::<[(&'static str, &'static str); 1], Counter>::default();
        registry.register(
            "authentications",
            "Authentication attempts, by outcome",
            auth.clone(),
        );
        let authentications = std::array::from_fn(|at| {
            auth.get_or_create_owned(&[("outcome", auth_label(AUTH_OUTCOMES[at]))])
        });

        let limited = Family::<[(&'static str, &'static str); 1], Counter>::default();
        registry.register(
            "rate_limited",
            "What section 10.3's limiters turned away, by limiter",
            limited.clone(),
        );
        let rate_limited = std::array::from_fn(|at| {
            limited.get_or_create_owned(&[("limiter", limiter_label(LIMITERS[at]))])
        });
        let auth_failure_addresses = Gauge::default();
        registry.register(
            "auth_failure_addresses",
            "Addresses the authentication failure limiter is holding",
            auth_failure_addresses.clone(),
        );

        let dropped = Family::<[(&'static str, &'static str); 1], Counter>::default();
        registry.register(
            "messages_dropped",
            "Messages discarded rather than delivered, by why",
            dropped.clone(),
        );
        let dropped_by_queue = ALL_DROP_REASONS
            .iter()
            .map(|reason| dropped.get_or_create_owned(&[("reason", drop_label(*reason))]))
            .collect();
        let dropped_by_shell = ALL_SHELL_DROPS
            .iter()
            .map(|reason| dropped.get_or_create_owned(&[("reason", shell_label(*reason))]))
            .collect();

        let rule_events = Family::<Vec<(String, String)>, Counter>::default();
        registry.register(
            "rule_events",
            "What each of section 9's rules has done, by rule and outcome",
            rule_events.clone(),
        );
        let rule_waiting = Family::<Vec<(String, String)>, Gauge>::default();
        registry.register(
            "rule_outbox_waiting",
            "Envelopes waiting in each rule's durable outbox",
            rule_waiting.clone(),
        );

        let records = Family::<[(&'static str, &'static str); 1], Counter>::default();
        registry.register(
            "records_dropped",
            "Journal records that were never written down, by why",
            records.clone(),
        );
        let records_dropped = ALL_RECORD_DROPS
            .iter()
            .map(|reason| records.get_or_create_owned(&[("reason", record_drop_label(*reason))]))
            .collect();

        Self {
            registry: Mutex::new(registry),
            packets_received,
            packets_sent,
            bytes_received,
            bytes_sent,
            publishes_unconfirmed,
            sockets_opened,
            sockets_refused,
            sockets_closed,
            reasons_sent,
            authentications,
            rate_limited,
            auth_failure_addresses,
            admin_events_dropped,
            accept_pauses,
            forwards_redirected,
            forwards_unroutable,
            publications_refused,
            dropped_by_queue,
            dropped_by_shell,
            records_dropped,
            rule_events,
            rule_waiting,
            watched: AtomicBool::new(false),
            watching_cluster: AtomicBool::new(false),
            tls: OnceLock::new(),
            websocket_refused: OnceLock::new(),
            build_recorded: OnceLock::new(),
        }
    }

    /// Register the numbers that are read from the structures that
    /// already hold them, rather than copied into a metric.
    ///
    /// A [`Weak`] reference, because `shared` owns this. Nothing is
    /// rendered by the collector once the broker it reads has gone,
    /// which is correct: a scrape during shutdown should report that
    /// it cannot see the numbers, not the last values somebody
    /// remembered.
    ///
    /// Idempotent, so a second listener over the same broker does not
    /// register a second collector.
    pub fn watch(&self, shared: &Arc<Shared>) {
        if self.watched.swap(true, Ordering::Relaxed) {
            return;
        }
        self.registry
            .lock()
            .register_collector(Box::new(Live(Arc::downgrade(shared))));
    }

    /// Section 12.1's TLS counters, registered on first use.
    ///
    /// A node with no TLS listener never asks, and its exposition has
    /// no TLS series — absent rather than zero, the cluster series'
    /// rule: a zero would read as "handshakes are fine" on a node that
    /// has none.
    pub fn tls(&self) -> Arc<TlsCounters> {
        Arc::clone(self.tls.get_or_init(|| {
            let counters = TlsCounters::detached();
            let mut registry = self.registry.lock();
            registry.register(
                "tls_handshakes_failed",
                "TLS handshakes that did not complete, by error or by the connect timeout",
                counters.handshakes_failed.clone(),
            );
            registry.register(
                "tls_certificate_reloads",
                "Certificate reloads, by whether the new pair was taken or the old one kept",
                counters.reloads.clone(),
            );
            counters
        }))
    }

    /// Name the build: `sdmqtt_build_info{version, allocator} 1`.
    ///
    /// The binary knows both and this crate knows neither, so the binary
    /// says, once, at startup. A second call is ignored rather than
    /// adding a second row: a node runs one build.
    pub fn record_build(&self, version: &str, allocator: &str) {
        self.build_recorded.get_or_init(|| {
            let info = Family::<[(&'static str, String); 2], Gauge>::default();
            info.get_or_create(&[
                ("version", version.to_owned()),
                ("allocator", allocator.to_owned()),
            ])
            .set(1);
            self.registry
                .lock()
                .register("build_info", "Which build this node runs", info);
        });
    }

    /// A WebSocket upgrade refused for `reason`, registered on first
    /// use for the reason [`Metrics::tls`] is.
    pub fn websocket_refused(&self, reason: &'static str) -> Counter {
        self.websocket_refused
            .get_or_init(|| {
                let family = Family::default();
                self.registry.lock().register(
                    "websocket_upgrades_refused",
                    "WebSocket upgrades not served, by why",
                    family.clone(),
                );
                family
            })
            .get_or_create_owned(&[("reason", reason)])
    }

    /// Register section 11.3's cluster numbers, read from the mesh
    /// that holds them.
    ///
    /// Called only on a node that clusters, which is what keeps the
    /// four series absent rather than zero on a node that does not:
    /// "the links are quiet" and "there are no links" are different
    /// statements, and a scraper cannot tell them apart from a zero.
    ///
    /// A strong reference, unlike [`Metrics::watch`]: the mesh does
    /// not own this registry, so there is no cycle, and a mesh that
    /// has been shut down still holds the totals its links reached.
    ///
    /// Idempotent, for the reason [`Metrics::watch`] is.
    pub fn watch_cluster(&self, cluster: Arc<dyn ClusterMetrics>) {
        if self.watching_cluster.swap(true, Ordering::Relaxed) {
            return;
        }
        self.registry
            .lock()
            .register_collector(Box::new(ClusterLive(cluster)));
    }

    /// One rule's owned handles.
    ///
    /// Called once, when the rule is installed. Calling it twice for
    /// one identifier gives two handles to the same series, which is
    /// what makes reinstalling a rule keep its numbers rather than
    /// start them again.
    pub fn rule(&self, rule: &str) -> RuleCounters {
        let events = std::array::from_fn(|at| {
            self.rule_events.get_or_create_owned(&vec![
                ("rule".to_owned(), rule.to_owned()),
                (
                    "event".to_owned(),
                    rule_event_label(RULE_EVENTS[at]).to_owned(),
                ),
            ])
        });
        RuleCounters {
            events,
            waiting: self
                .rule_waiting
                .get_or_create_owned(&vec![("rule".to_owned(), rule.to_owned())]),
        }
    }

    /// The counter for a packet read from a client.
    pub fn packet_received(&self, version: Version, kind: PacketKind) -> &Counter {
        self.packets_received.get(version, kind)
    }

    /// The counter for a packet written to a client.
    pub fn packet_sent(&self, version: Version, kind: PacketKind) -> &Counter {
        self.packets_sent.get(version, kind)
    }

    /// Bytes read from a client socket.
    pub fn bytes_received(&self) -> &Counter {
        &self.bytes_received
    }

    /// Bytes written to a client socket.
    pub fn bytes_sent(&self) -> &Counter {
        &self.bytes_sent
    }

    /// A socket this broker has begun serving.
    pub fn socket_opened(&self) -> &Counter {
        &self.sockets_opened
    }

    /// A socket closed unserved, because the node is at its
    /// connection limit.
    pub fn socket_refused(&self) -> &Counter {
        &self.sockets_refused
    }

    /// Admin events a stream's reader was too slow to receive.
    ///
    /// Section 11.3: a message never disappears without being
    /// counted, and section 11.1 applies that to the events stream in
    /// as many words — "every dropped event is counted". The reader
    /// is sent a resynchronise rather than a gap, so this counter is
    /// how an operator sees that a browser is falling behind.
    pub fn admin_events_dropped(&self) -> &Counter {
        &self.admin_events_dropped
    }

    /// A listener paused accepting because accepting failed for the
    /// whole process. See `listener::affects_one_connection`.
    pub fn accept_paused(&self) -> &Counter {
        &self.accept_pauses
    }

    /// A forwarded publication that found its session gone and was sent
    /// on to the node that took it. Section 8.4's move, met by 8.3's
    /// forward in flight.
    pub fn forward_redirected(&self) -> &Counter {
        &self.forwards_redirected
    }

    /// A forwarded publication nothing here wanted and nowhere was known
    /// to send on. Section 11.3: counted, where before it was a debug line.
    pub fn forward_unroutable(&self) -> &Counter {
        &self.forwards_unroutable
    }

    /// A publication a connector made that went nowhere, by why.
    ///
    /// Section 9.2: "Every publication a connector originates carries
    /// a hop count, and one over the configured maximum is dropped and
    /// counted." That is [`Refusal::TooManyHops`], and the other four
    /// are section 11.3's rule applied to the rest of milestone four:
    /// a connector chain that quietly swallowed a message would be a
    /// support case nobody could answer, and until this every one of
    /// these was a log line at best.
    pub fn publication_refused(&self, reason: Refusal) -> &Counter {
        &self.publications_refused[refusal_index(reason)]
    }

    /// A durable publish acknowledged with no copy confirming it.
    ///
    /// Section 8.6. **Registered on every node**, not only a
    /// clustering one, unlike the cluster gauges: a node running in
    /// synchronous mode with nothing to replicate to is exactly the
    /// case this counts, and an absent series there would hide it.
    pub fn publish_unconfirmed(&self) -> &Counter {
        &self.publishes_unconfirmed
    }

    /// A socket that has finished, by how it ended.
    pub fn socket_closed(&self, outcome: SocketOutcome) -> &Counter {
        &self.sockets_closed[outcome_index(outcome)]
    }

    /// A connection-level reason code given to a client.
    ///
    /// `kind` must be one of [`REASON_KINDS`]; anything else is a
    /// programming error and panics rather than landing on the wrong
    /// series.
    pub fn reason_sent(&self, kind: PacketKind, reason: ReasonCode) -> &Counter {
        let at = REASON_KINDS
            .iter()
            .position(|candidate| *candidate == kind)
            .expect("a reason code is only counted for a CONNACK or a DISCONNECT");
        &self.reasons_sent[at][reason_index(reason)]
    }

    /// An authentication decision, by outcome: a CONNECT admitted or
    /// refused by the authenticate chain or the enhanced exchange, and
    /// a re-authentication. A connect turned away by the limiter is not
    /// one; it is [`Metrics::rate_limited`].
    pub fn authentication(&self, outcome: AuthOutcome) -> &Counter {
        &self.authentications[auth_index(outcome)]
    }

    /// Something a section 10.3 limiter turned away.
    pub fn rate_limited(&self, limiter: Limiter) -> &Counter {
        &self.rate_limited[match limiter {
            Limiter::Publish => 0,
            Limiter::Authentication => 1,
        }]
    }

    /// How many addresses the authentication limiter holds.
    pub fn auth_failure_addresses(&self) -> &Gauge {
        &self.auth_failure_addresses
    }

    /// A message the broker discarded, by the reason it reported.
    pub fn message_dropped(&self, reason: DropReason) -> &Counter {
        &self.dropped_by_queue[drop_index(reason)]
    }

    /// A message the shell discarded, where the broker cannot see it.
    pub fn shell_dropped(&self, reason: ShellDrop) -> &Counter {
        &self.dropped_by_shell[shell_index(reason)]
    }

    /// Journal records that went nowhere.
    pub fn records_dropped(&self, reason: RecordDrop) -> &Counter {
        &self.records_dropped[record_drop_index(reason)]
    }

    /// Render the whole registry in the Prometheus text format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &self.registry.lock())
            .expect("writing to a String cannot fail");
        out
    }
}

/// The numbers the broker already maintains, read when a scraper
/// asks.
///
/// **Not copied into a gauge.** A gauge holding a second copy of a
/// count that has an authoritative home is two sources of truth and
/// a drift bug waiting for the one path that forgets to update it.
/// Everything here is read from the structure that owns it, at the
/// moment of the scrape.
#[derive(Debug)]
struct Live(Weak<Shared>);

impl Collector for Live {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let Some(shared) = self.0.upgrade() else {
            // The broker has gone. Rendering the last values anybody
            // remembered would be worse than rendering nothing.
            return Ok(());
        };

        // The store's own count. `Writer` maintains it because it is
        // the only thing that can see a batch refused with nobody
        // waiting to be told, so this reads it rather than keeping a
        // second tally beside it.
        let rejected = ConstCounter::new(shared.rejected_batches());
        let descriptor = encoder.encode_descriptor(
            "store_batches_rejected",
            "Batches of records the store refused, which the broker and the store now disagree about",
            None,
            rejected.metric_type(),
        )?;
        rejected.encode(descriptor)?;

        // Section 11.3's gauges. Every one of them is read from the
        // structure that owns it, at the moment of the scrape.
        //
        // One acquisition of the broker's lock for the four that live
        // behind it, and every one of the four is a read of a number
        // the broker already maintains. Three of them used to mean a
        // walk: the queue depth summed every session, the
        // subscription count added up every shard of the trie, and
        // the retained count walked the retained tree — 405
        // microseconds, 2.64 milliseconds and 2.66 milliseconds
        // respectively with a hundred thousand of each, on this
        // lock, every time a scraper asked. See
        // [`broker_core::session::QueueTotal`] for the measurement
        // that decided the first, and the note on
        // [`broker_core::subscriptions::SubscriptionTrie`] and
        // [`broker_core::retained::RetainedStore`] for the other two.
        // Each is guarded against drifting from what it counts by a
        // property that walks.
        //
        // Section 11.3's cluster members gauge and its three byte
        // counters used to be listed here as deferred to milestone 5,
        // with the reason: a membership of zero would be wrong rather
        // than empty, and three counters that could only read zero
        // would say "the links are quiet" rather than "there are no
        // links". They are not deferred any more. They live in
        // [`ClusterLive`], registered by [`Metrics::watch_cluster`]
        // **only on a node that clusters**, which is what keeps them
        // absent rather than zero on a node that does not.
        let connections = shared.outbox_count();
        let (sessions, subscriptions, retained, queued, matches) = shared.with_broker(|broker| {
            (
                broker.session_count(),
                broker.subscription_count(),
                broker.retained_count(),
                broker.queue_depth(),
                broker.router_matches(),
            )
        });

        // Section 11.3's "router matches split local and remote",
        // **deferred from milestone 3 and not deferred any more.** The
        // note that stood here said the remote half needed a cluster and
        // that counting the local half needed the matching layer to
        // report a number it did not return, which would have put
        // counting inside the crate that deliberately counts nothing.
        // Neither is true now: section 8.3's forwarding is what wanted
        // the remote half, and the local half is a running total the
        // broker keeps and this reads, which is the same division as
        // every gauge above.
        //
        // One family split by label rather than two names, because the
        // question an operator asks is what proportion of this node's
        // matching is costing a network copy. Both series exist on
        // every node: a node that does not cluster really did forward to
        // nobody, which is not the same as `cluster_members`, where zero
        // would be a lie because a node is a member of its own cluster.
        let mut router = encoder.encode_descriptor(
            "router_matches",
            "Publications routed, by whether they were delivered here or forwarded to \
             another node",
            None,
            prometheus_client::metrics::MetricType::Counter,
        )?;
        for (scope, value) in [("local", matches.local), ("remote", matches.remote)] {
            let counter = ConstCounter::new(value);
            counter.encode(router.encode_family(&[("scope", scope)])?)?;
        }

        for (name, help, value) in [
            (
                "connections",
                "Sessions with a connection attached",
                connections,
            ),
            (
                "sessions",
                "Sessions the broker holds, connected or not",
                sessions,
            ),
            (
                "subscriptions",
                "Subscriptions held across every session",
                subscriptions,
            ),
            (
                "retained_messages",
                "Messages the retained store holds",
                retained,
            ),
            (
                "queue_depth",
                "Messages waiting in every session's queue",
                queued,
            ),
        ] {
            let value = i64::try_from(value).unwrap_or(i64::MAX);
            let reading = ConstGauge::new(value);
            let descriptor = encoder.encode_descriptor(name, help, None, reading.metric_type())?;
            reading.encode(descriptor)?;
        }

        Ok(())
    }
}

/// Section 11.3's cluster numbers, read from the mesh at the moment
/// of the scrape.
///
/// Registered only on a node that clusters; see
/// [`Metrics::watch_cluster`].
#[derive(Debug)]
struct ClusterLive(Arc<dyn ClusterMetrics>);

impl Collector for ClusterLive {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        // Itself included, which is what the trait promises: a node
        // that has lost every peer is a cluster of one, not of none.
        let members = ConstGauge::new(i64::try_from(self.0.members()).unwrap_or(i64::MAX));
        let descriptor = encoder.encode_descriptor(
            "cluster_members",
            "Nodes in this cluster that this node can see, itself included",
            None,
            members.metric_type(),
        )?;
        members.encode(descriptor)?;

        for (name, help, value) in [
            (
                "cluster_bytes_sent",
                "Bytes written to cluster peers",
                self.0.bytes_sent(),
            ),
            (
                "cluster_bytes_received",
                "Bytes read from cluster peers",
                self.0.bytes_received(),
            ),
            (
                "cluster_bytes_dropped",
                "Bytes never written to a cluster peer, because its queue was full",
                self.0.bytes_dropped(),
            ),
        ] {
            let counter = ConstCounter::new(value);
            let descriptor = encoder.encode_descriptor(name, help, None, counter.metric_type())?;
            counter.encode(descriptor)?;
        }

        // Section 8.4's, zipped against their names rather than
        // written out again: two lists of seven in one file is how one
        // of them ends up describing the other one's fifth entry.
        let migrations = self.0.migrations();
        for ((name, help), value) in MIGRATION_NUMBERS.iter().zip(migrations) {
            let counter = ConstCounter::new(value);
            let descriptor = encoder.encode_descriptor(name, help, None, counter.metric_type())?;
            counter.encode(descriptor)?;
        }

        // Section 8.6's, which are **gauges and not counters**: a lag
        // and a count of sessions are levels that go down as well as
        // up, and rendering a level as a counter tells every scraper
        // it may take differences of it.
        let replication = self.0.replication();
        for ((name, help), value) in REPLICATION_NUMBERS.iter().zip(replication) {
            let gauge = ConstGauge::new(i64::try_from(value).unwrap_or(i64::MAX));
            let descriptor = encoder.encode_descriptor(name, help, None, gauge.metric_type())?;
            gauge.encode(descriptor)?;
        }
        // And the one that only goes up, as a counter.
        let applied = ConstCounter::new(self.0.replica_applied());
        let descriptor = encoder.encode_descriptor(
            REPLICA_APPLIED.0,
            REPLICA_APPLIED.1,
            None,
            applied.metric_type(),
        )?;
        applied.encode(descriptor)?;
        // Section 8.6's repair: a level and a total.
        let (pending, done) = self.0.repairs();
        let pending = ConstGauge::new(i64::try_from(pending).unwrap_or(i64::MAX));
        let descriptor = encoder.encode_descriptor(
            REPAIRS_PENDING.0,
            REPAIRS_PENDING.1,
            None,
            pending.metric_type(),
        )?;
        pending.encode(descriptor)?;
        let done = ConstCounter::new(done);
        let descriptor =
            encoder.encode_descriptor(REPAIRS_DONE.0, REPAIRS_DONE.1, None, done.metric_type())?;
        done.encode(descriptor)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Section 11.3's build series: one constant row naming the version
    /// and the allocator, registered once, so a dashboard can tell which
    /// build a node runs and a second call cannot add a second row.
    #[test]
    fn the_build_is_one_constant_row() {
        let metrics = Metrics::new();
        metrics.record_build("0.7.0", "mimalloc");
        metrics.record_build("9.9.9", "system");
        let rendered = metrics.render();
        assert!(
            rendered.contains("sdmqtt_build_info{version=\"0.7.0\",allocator=\"mimalloc\"} 1"),
            "{rendered}"
        );
        assert!(!rendered.contains("9.9.9"), "{rendered}");
    }

    /// One metric family's whole exposition block, with its samples in
    /// a stable order.
    ///
    /// The whole block rather than a line of it: the exposition is what
    /// a scraper parses, and a test that looks for a name inside a
    /// larger text passes against a family that is missing series, has
    /// the wrong label names, or renders its values in upper case.
    ///
    /// Sorted because the family behind the name is a `HashMap` and its
    /// iteration order is not stable between runs. A scraper does not
    /// care in which order the series of one family arrive, so sorting
    /// removes a difference that is not a fact about the exposition,
    /// and removes nothing else: a missing, extra or misspelt series
    /// still changes the text.
    pub(super) fn block(rendered: &str, name: &str) -> String {
        let help = format!("# HELP {name} ");
        let start = rendered
            .find(&help)
            .unwrap_or_else(|| panic!("no block for {name} in:\n{rendered}"));
        let rest = &rendered[start..];
        let end = rest[1..].find("\n# HELP ").map_or(rest.len(), |at| at + 2);
        let mut lines: Vec<&str> = rest[..end].lines().collect();
        let mut samples = lines.split_off(2);
        samples.sort_unstable();
        lines.extend(samples);
        let mut out = lines.join("\n");
        out.push('\n');
        out
    }

    #[test]
    fn every_packet_type_and_version_is_a_series_before_a_single_packet_arrives() {
        // The exact text, because it is what a scraper parses. Three
        // things are asserted at once and each has a way of going
        // wrong that a substring search would not see: that the
        // cross-product is resolved at startup, so a type nobody has
        // sent yet is a zero rather than a hole in a dashboard; that
        // the label values are lower case, which the derive would not
        // have made them; and that an increment lands on the one
        // series it names and on no other.
        let metrics = Metrics::new();
        metrics
            .packet_received(Version::V5, PacketKind::Publish)
            .inc();
        metrics
            .packet_received(Version::V311, PacketKind::Connect)
            .inc();
        metrics
            .packet_received(Version::V311, PacketKind::Connect)
            .inc();

        assert_eq!(
            block(&metrics.render(), "sdmqtt_packets_received"),
            "\
# HELP sdmqtt_packets_received Packets read from a client, by type and protocol version.
# TYPE sdmqtt_packets_received counter
sdmqtt_packets_received_total{kind=\"auth\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"auth\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"connack\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"connack\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"connect\",version=\"3.1.1\"} 2
sdmqtt_packets_received_total{kind=\"connect\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"disconnect\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"disconnect\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"pingreq\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"pingreq\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"pingresp\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"pingresp\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"puback\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"puback\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"pubcomp\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"pubcomp\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"publish\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"publish\",version=\"5\"} 1
sdmqtt_packets_received_total{kind=\"pubrec\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"pubrec\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"pubrel\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"pubrel\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"suback\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"suback\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"subscribe\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"subscribe\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"unsuback\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"unsuback\",version=\"5\"} 0
sdmqtt_packets_received_total{kind=\"unsubscribe\",version=\"3.1.1\"} 0
sdmqtt_packets_received_total{kind=\"unsubscribe\",version=\"5\"} 0
"
        );
    }

    /// Section 11.3's router matches, split local and remote.
    ///
    /// **Both series exist before a single publication**, which is the
    /// half a substring search would not catch: a family that only grew
    /// a series when it was first incremented leaves a hole in a
    /// dashboard, and a remote half that was registered only on a node
    /// that clusters would make the split mean different things on
    /// different nodes.
    ///
    /// The exact text, because it is what a scraper parses: the label is
    /// `scope`, the values are lower case, and a local delivery does not
    /// land on the remote series.
    #[tokio::test]
    async fn the_router_matches_are_split_local_and_remote() {
        use broker_core::broker::BrokerConfig;
        use mqtt_codec::packet::{Connect, RetainHandling, Subscribe, SubscriptionOptions};
        use mqtt_codec::{Packet, Properties, QoS};

        use crate::shared::Shared;

        let shared = std::sync::Arc::new(Shared::new(BrokerConfig::default()));
        shared.metrics().watch(&shared);

        assert_eq!(
            block(&shared.metrics().render(), "sdmqtt_router_matches"),
            "\
# HELP sdmqtt_router_matches Publications routed, by whether they were delivered here or forwarded to another node
# TYPE sdmqtt_router_matches counter
sdmqtt_router_matches_total{scope=\"local\"} 0
sdmqtt_router_matches_total{scope=\"remote\"} 0
",
            "both series must exist before anything is published"
        );

        let registration = shared.with_broker(|broker| {
            broker
                .connect(
                    "",
                    &Connect {
                        version: Version::V5,
                        client_id: "listener".into(),
                        clean_start: false,
                        keep_alive: 0,
                        username: None,
                        password: None,
                        will: None,
                        properties: Properties {
                            session_expiry_interval: Some(3600),
                            ..Properties::default()
                        },
                    },
                    &mut Vec::new(),
                )
                .expect("the default decision authenticates")
        });
        shared.with_broker(|broker| {
            broker
                .handle(
                    registration.subscriber,
                    &Packet::Subscribe(Subscribe {
                        packet_id: 1,
                        filters: vec![SubscriptionOptions {
                            filter: "a/+".into(),
                            qos: QoS::AtMostOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: RetainHandling::SendAlways,
                        }],
                        properties: Properties::default(),
                    }),
                    &mut Vec::new(),
                )
                .expect("the subscription is accepted");
        });
        shared.with_broker(|broker| {
            let now = broker.clock().now();
            broker.publish(
                broker_core::session::Message::new(
                    "a/b",
                    bytes::Bytes::from_static(b"here"),
                    QoS::AtMostOnce,
                    false,
                ),
                None,
                now,
                &mut Vec::new(),
            );
        });

        assert_eq!(
            block(&shared.metrics().render(), "sdmqtt_router_matches"),
            "\
# HELP sdmqtt_router_matches Publications routed, by whether they were delivered here or forwarded to another node
# TYPE sdmqtt_router_matches counter
sdmqtt_router_matches_total{scope=\"local\"} 1
sdmqtt_router_matches_total{scope=\"remote\"} 0
",
            "one local delivery, and nothing forwarded by a node with no cluster"
        );
    }

    /// The value of one gauge in a rendered exposition.
    fn gauge(rendered: &str, name: &str) -> i64 {
        let line = rendered
            .lines()
            .find(|line| line.starts_with(&format!("{name} ")))
            .unwrap_or_else(|| panic!("no {name} in:\n{rendered}"));
        line.rsplit(' ')
            .next()
            .expect("a value")
            .parse()
            .expect("a number")
    }

    #[tokio::test]
    async fn the_gauges_read_the_broker_when_the_scraper_asks() {
        use broker_core::broker::BrokerConfig;
        use mqtt_codec::packet::{Connect, RetainHandling, Subscribe, SubscriptionOptions};
        use mqtt_codec::{Packet, Properties, QoS};

        use crate::shared::Shared;

        fn connect(id: &str) -> Connect {
            Connect {
                version: Version::V5,
                client_id: id.into(),
                clean_start: true,
                keep_alive: 0,
                username: None,
                password: None,
                will: None,
                properties: Properties::default(),
            }
        }

        let shared = std::sync::Arc::new(Shared::new(BrokerConfig::default()));
        shared.metrics().watch(&shared);

        // Nothing yet, and registered rather than absent: a gauge
        // that only appears once it is non-zero is a hole in a
        // dashboard at exactly the moment it is meant to reassure.
        let rendered = shared.metrics().render();
        for name in [
            "sdmqtt_connections",
            "sdmqtt_sessions",
            "sdmqtt_subscriptions",
            "sdmqtt_retained_messages",
            "sdmqtt_queue_depth",
        ] {
            assert_eq!(gauge(&rendered, name), 0, "{name} before anything exists");
        }

        let mut handles = Vec::new();
        for (at, name) in ["one", "two", "three"].iter().enumerate() {
            let registration = shared.with_broker(|broker| {
                broker
                    .connect("", &connect(name), &mut Vec::new())
                    .expect("the default decision authenticates")
            });
            // One subscription for the first, two for the second,
            // three for the third: six in total, which no count of
            // sessions can be mistaken for.
            for filter in 0..=at {
                shared.with_broker(|broker| {
                    broker
                        .handle(
                            registration.subscriber,
                            &Packet::Subscribe(Subscribe {
                                packet_id: 1,
                                filters: vec![SubscriptionOptions {
                                    filter: format!("{name}/{filter}"),
                                    qos: QoS::AtMostOnce,
                                    no_local: false,
                                    retain_as_published: false,
                                    retain_handling: RetainHandling::DoNotSend,
                                }],
                                properties: Properties::default(),
                            }),
                            &mut Vec::new(),
                        )
                        .expect("the subscription is accepted");
                });
            }
            let _outbox = shared.register_outbox(registration.subscriber, 8);
            handles.push((registration.subscriber, _outbox));
        }

        // A retained message, so that its gauge is not zero either.
        shared.with_broker(|broker| {
            let now = broker.clock().now();
            broker.publish(
                broker_core::session::Message::new(
                    "one/0",
                    bytes::Bytes::from_static(b"kept"),
                    QoS::AtMostOnce,
                    true,
                ),
                None,
                now,
                &mut Vec::new(),
            );
        });

        let rendered = shared.metrics().render();
        assert_eq!(gauge(&rendered, "sdmqtt_connections"), 3);
        assert_eq!(gauge(&rendered, "sdmqtt_sessions"), 3);
        assert_eq!(gauge(&rendered, "sdmqtt_subscriptions"), 6);
        assert_eq!(gauge(&rendered, "sdmqtt_retained_messages"), 1);

        // **The half that catches a gauge which only ever rises.** A
        // number reported once at registration, or copied into a
        // gauge by the path that creates a session and by no path
        // that destroys one, passes everything above.
        let (subscriber, outbox) = handles.remove(0);
        drop(outbox);
        shared.remove_outbox(subscriber);
        shared.with_broker(|broker| {
            broker.disconnect(subscriber, &mut Vec::new());
            // An expiry interval of zero, so the next tick takes the
            // session and its one subscription with it.
            broker.tick(&mut Vec::new());
        });

        let rendered = shared.metrics().render();
        assert_eq!(gauge(&rendered, "sdmqtt_connections"), 2, "one went");
        assert_eq!(gauge(&rendered, "sdmqtt_sessions"), 2, "and its session");
        assert_eq!(
            gauge(&rendered, "sdmqtt_subscriptions"),
            5,
            "and the subscription it held"
        );
    }

    #[tokio::test]
    async fn the_queue_depth_gauge_falls_as_well_as_rises() {
        use broker_core::broker::BrokerConfig;
        use mqtt_codec::packet::{Connect, RetainHandling, Subscribe, SubscriptionOptions};
        use mqtt_codec::{Packet, Properties, QoS};

        use crate::shared::Shared;

        let shared = std::sync::Arc::new(Shared::new(BrokerConfig::default()));
        shared.metrics().watch(&shared);

        let registration = shared.with_broker(|broker| {
            broker
                .connect(
                    "",
                    &Connect {
                        version: Version::V5,
                        client_id: "waiting".into(),
                        clean_start: false,
                        keep_alive: 0,
                        username: None,
                        password: None,
                        will: None,
                        properties: Properties {
                            session_expiry_interval: Some(3600),
                            ..Properties::default()
                        },
                    },
                    &mut Vec::new(),
                )
                .expect("the default decision authenticates")
        });
        shared.with_broker(|broker| {
            broker
                .handle(
                    registration.subscriber,
                    &Packet::Subscribe(Subscribe {
                        packet_id: 1,
                        filters: vec![SubscriptionOptions {
                            filter: "a/b".into(),
                            qos: QoS::AtLeastOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: RetainHandling::DoNotSend,
                        }],
                        properties: Properties::default(),
                    }),
                    &mut Vec::new(),
                )
                .expect("the subscription is accepted");
            // Offline, so what is published waits in the queue rather
            // than going out.
            broker.disconnect(registration.subscriber, &mut Vec::new());
            for _ in 0..4 {
                let now = broker.clock().now();
                broker.publish(
                    broker_core::session::Message::new(
                        "a/b",
                        bytes::Bytes::from_static(b"waiting"),
                        QoS::AtLeastOnce,
                        false,
                    ),
                    None,
                    now,
                    &mut Vec::new(),
                );
            }
        });

        assert_eq!(gauge(&shared.metrics().render(), "sdmqtt_queue_depth"), 4);

        // The session ends, and its queue with it.
        shared.with_broker(|broker| {
            broker
                .connect(
                    "",
                    &Connect {
                        version: Version::V5,
                        client_id: "waiting".into(),
                        clean_start: true,
                        keep_alive: 0,
                        username: None,
                        password: None,
                        will: None,
                        properties: Properties::default(),
                    },
                    &mut Vec::new(),
                )
                .expect("the default decision authenticates")
        });
        assert_eq!(
            gauge(&shared.metrics().render(), "sdmqtt_queue_depth"),
            0,
            "a clean start took the queue with it"
        );
    }

    #[test]
    fn a_handle_is_the_counter_the_family_holds_and_not_a_copy_of_it() {
        // The whole reason the handles are owned. If
        // `get_or_create_owned` returned a detached metric rather than
        // a clone of the reference-counted one, every increment would
        // land somewhere the registry cannot see and the exposition
        // would stay at zero while the broker was busy. The test above
        // would catch that too; this one says which of the two things
        // is wrong when it does.
        let metrics = Metrics::new();
        let handle = metrics.packet_received(Version::V5, PacketKind::PingReq);
        handle.inc_by(7);
        assert!(
            metrics
                .render()
                .contains("sdmqtt_packets_received_total{kind=\"pingreq\",version=\"5\"} 7"),
            "an increment through the handle must reach the registry"
        );
    }

    #[test]
    fn each_connector_refusal_has_its_own_handle_and_its_own_label() {
        // The same shape as the test below, for the family milestone
        // four added. Two refusals mapped onto one slot would be a
        // counter that says a message was lost and cannot say which
        // defence lost it -- and the loop marker and the hop limit are
        // exactly the pair an operator has to tell apart, because one
        // is a bridge working and the other is a cycle to go and find.
        let metrics = Metrics::new();
        for reason in ALL_REFUSALS.iter().copied() {
            metrics.publication_refused(reason).inc();
        }
        for reason in ALL_REFUSALS.iter().copied() {
            assert_eq!(
                metrics.publication_refused(reason).get(),
                1,
                "{reason:?} shares a handle with another refusal"
            );
        }
        // And the label values are the ones a dashboard is written
        // against, lower case and written out by hand. See the module
        // note: a derive would give `TooManyHops`.
        let exposition = metrics.render();
        for reason in ALL_REFUSALS.iter().copied() {
            let line = format!(
                "sdmqtt_publications_refused_total{{reason=\"{}\"}} 1",
                refusal_label(reason)
            );
            assert!(
                exposition.contains(&line),
                "{reason:?} is not on the exposition as {line}:\n{exposition}"
            );
        }
    }

    #[test]
    fn each_packet_type_and_version_has_its_own_handle() {
        // Convention 5: the exposition test above would pass against a
        // `kind_index` that mapped two types onto one slot only if the
        // text changed, and it would — but this says it directly, over
        // every combination rather than the three the text exercises.
        let metrics = Metrics::new();
        for version in VERSIONS {
            for kind in PACKET_KINDS {
                metrics.packet_received(version, kind).inc();
            }
        }
        for version in VERSIONS {
            for kind in PACKET_KINDS {
                assert_eq!(
                    metrics.packet_received(version, kind).get(),
                    1,
                    "{kind:?} at {version:?} shares a handle with something else"
                );
            }
        }
    }

    /// A cluster whose every number differs from every other.
    ///
    /// **Distinct values, and that is the point.** Seven counters
    /// rendered from one array against one list of names is a zip, and
    /// a zip that has slipped by one renders seven plausible series
    /// with the wrong numbers under them. Equal values, or zeroes,
    /// would make that invisible.
    #[derive(Debug)]
    struct Counted;

    impl ClusterMetrics for Counted {
        fn members(&self) -> u64 {
            3
        }
        fn bytes_sent(&self) -> u64 {
            11
        }
        fn bytes_received(&self) -> u64 {
            22
        }
        fn bytes_dropped(&self) -> u64 {
            33
        }
        fn migrations(&self) -> [u64; MIGRATION_NUMBERS.len()] {
            [41, 42, 43, 44, 45, 46, 47, 48, 49]
        }
        fn replication(&self) -> [u64; REPLICATION_NUMBERS.len()] {
            [51, 52, 53]
        }
        fn replica_applied(&self) -> u64 {
            54
        }
        fn repairs(&self) -> (u64, u64) {
            (55, 56)
        }
    }

    #[test]
    fn a_clustering_node_renders_its_cluster_and_migration_numbers() {
        let metrics = Metrics::new();
        metrics.watch_cluster(Arc::new(Counted));
        let rendered = metrics.render();

        assert_eq!(
            block(&rendered, "sdmqtt_cluster_members"),
            // No trailing full stop: the registry appends one to the
            // families it owns, and a collector's descriptors are
            // written as given.
            "# HELP sdmqtt_cluster_members Nodes in this cluster that this node can see, \
             itself included\n\
             # TYPE sdmqtt_cluster_members gauge\n\
             sdmqtt_cluster_members 3\n"
        );
        for (name, value) in [
            ("sdmqtt_cluster_bytes_sent", 11),
            ("sdmqtt_cluster_bytes_received", 22),
            ("sdmqtt_cluster_bytes_dropped", 33),
        ] {
            assert!(
                block(&rendered, name).contains(&format!("{name}_total {value}\n")),
                "{name} should be {value} in:\n{}",
                block(&rendered, name)
            );
        }

        // Section 8.4's seven, each against the name it belongs to and
        // not merely present somewhere.
        for ((name, help), value) in MIGRATION_NUMBERS
            .iter()
            .zip([41, 42, 43, 44, 45, 46, 47, 48, 49])
        {
            let family = format!("sdmqtt_{name}");
            let shown = block(&rendered, &family);
            assert!(
                shown.contains(&format!("{family}_total {value}\n")),
                "{family} should be {value} in:\n{shown}"
            );
            assert!(
                shown.contains(help),
                "{family} should carry its own help text, not another's:\n{shown}"
            );
        }

        // And the one the spec names, spelled out, because an operator
        // alerting on a partition is alerting on this series and a
        // rename would silently stop their rule firing.
        assert!(
            rendered.contains("sdmqtt_sessions_migrated_incomplete_total 43\n"),
            "section 8.4's shortfall is this series:\n{rendered}"
        );

        // Section 8.6's three, which are **gauges**: a lag and a count
        // of sessions go down as well as up, and a counter tells a
        // scraper it may take differences.
        for ((name, help), value) in REPLICATION_NUMBERS.iter().zip([51, 52, 53]) {
            let family = format!("sdmqtt_{name}");
            let shown = block(&rendered, &family);
            assert!(
                shown.contains(&format!("# TYPE {family} gauge\n")),
                "{family} is a level, not a counter:\n{shown}"
            );
            assert!(
                shown.contains(&format!("{family} {value}\n")),
                "{family} should be {value} in:\n{shown}"
            );
            assert!(
                shown.contains(help),
                "{family} carries its own help:\n{shown}"
            );
        }
        assert!(
            rendered.contains("sdmqtt_replication_lag 51\n"),
            "section 8.6's loss window is this series:\n{rendered}"
        );
        let applied = block(&rendered, "sdmqtt_replica_changes_applied");
        assert!(
            applied.contains("# TYPE sdmqtt_replica_changes_applied counter\n"),
            "applied changes only go up, so this is a counter:\n{applied}"
        );
        assert!(
            applied.contains("sdmqtt_replica_changes_applied_total 54\n"),
            "{applied}"
        );
        let pending = block(&rendered, "sdmqtt_replica_repairs_pending");
        assert!(
            pending.contains("# TYPE sdmqtt_replica_repairs_pending gauge\n")
                && pending.contains("sdmqtt_replica_repairs_pending 55\n"),
            "repairs pending is a level:\n{pending}"
        );
        let done = block(&rendered, "sdmqtt_replica_repairs_done");
        assert!(
            done.contains("# TYPE sdmqtt_replica_repairs_done counter\n")
                && done.contains("sdmqtt_replica_repairs_done_total 56\n"),
            "repairs done is a total:\n{done}"
        );
    }

    #[test]
    fn a_node_that_does_not_cluster_renders_none_of_them() {
        // Absent rather than zero, which is section 11.3's rule and the
        // reason `watch_cluster` is called only on a node that
        // clusters: "no links" and "quiet links" are different
        // statements, and a scraper cannot tell them apart from a zero.
        let rendered = Metrics::new().render();
        assert!(!rendered.contains("sdmqtt_cluster_members"), "{rendered}");
        for (name, _) in MIGRATION_NUMBERS
            .iter()
            .chain(REPLICATION_NUMBERS.iter())
            .chain([&REPLICA_APPLIED, &REPAIRS_PENDING, &REPAIRS_DONE])
        {
            assert!(
                !rendered.contains(&format!("sdmqtt_{name}")),
                "{name} should be absent on a node that does not cluster:\n{rendered}"
            );
        }
    }
}
