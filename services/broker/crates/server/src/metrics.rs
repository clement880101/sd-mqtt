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
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::registry::Registry;

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
    /// The connection was dropped before it finished: a shutdown, or
    /// a panic. Counted from [`Drop`] rather than from a return, so
    /// that a cancelled connection is an ending like any other rather
    /// than a socket that is open forever in the exposition.
    Abandoned,
}

/// Every socket outcome, in the order the handles are indexed in.
const SOCKET_OUTCOMES: [SocketOutcome; 9] = [
    SocketOutcome::Clean,
    SocketOutcome::ConnectTimeout,
    SocketOutcome::NotAConnect,
    SocketOutcome::Malformed,
    SocketOutcome::Protocol,
    SocketOutcome::Io,
    SocketOutcome::Codec,
    SocketOutcome::NotDurable,
    SocketOutcome::Abandoned,
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
    }
}

/// How an authentication ended.
///
/// Authentication itself is milestone 7. The outcomes are registered
/// now and are **deliberately zero rather than missing**, so that the
/// dashboard and the alerting rule an operator writes against this
/// broker today keep working unchanged when it arrives. A family with
/// no series is skipped by the encoder entirely, which is why the
/// handles are resolved here like every other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthOutcome {
    /// The client proved who it said it was.
    Success,
    /// It did not, and was refused.
    Failure,
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
    RejectedByPeer => "rejected_by_peer",
    TooLargeForPeer => "too_large_for_peer",
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
    sockets_opened: Counter,
    sockets_refused: Counter,
    /// Section 11.3, for the admin events stream: a reader that has
    /// fallen behind loses events, and none of them may disappear
    /// uncounted. See [`Metrics::admin_events_dropped`].
    admin_events_dropped: Counter,
    /// Section 9.2's third loop, counted where it is stopped: "one
    /// over the configured maximum is dropped and counted". See
    /// [`Metrics::publications_over_hop_limit`].
    publications_over_hop_limit: Counter,
    sockets_closed: [Counter; SOCKET_OUTCOMES.len()],
    /// Indexed by [`REASON_KINDS`] then by [`ALL_REASONS`].
    reasons_sent: [[Counter; ALL_REASONS.len()]; REASON_KINDS.len()],
    /// Registered and never incremented until milestone 7. See
    /// [`AUTH_OUTCOMES`].
    authentications: [Counter; AUTH_OUTCOMES.len()],
    /// Section 11.3: a message never disappears without being
    /// counted. One family, two tables — the broker's own reasons and
    /// the shell's — because an operator wants one number for
    /// "messages lost" broken down by why.
    dropped_by_queue: Box<[Counter]>,
    dropped_by_shell: Box<[Counter]>,
    records_dropped: Box<[Counter]>,
    /// Whether [`Metrics::watch`] has already run. A registry with
    /// two collectors over the same numbers would render every one of
    /// them twice, which a scraper reads as a duplicate series.
    watched: AtomicBool,
}

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
        let publications_over_hop_limit = Counter::default();
        registry.register(
            "publications_over_hop_limit",
            // Unlike the counter above this one *is* a loss, and a
            // deliberate one: a chain of connectors that feeds itself
            // is stopped by dropping the message that would extend it.
            // A non-zero value means there is a cycle to find.
            "Publications dropped for having been through more connectors than              broker.max_hops allows; a non-zero value means a rule is feeding a cycle",
            publications_over_hop_limit.clone(),
        );
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
            sockets_opened,
            sockets_refused,
            sockets_closed,
            reasons_sent,
            authentications,
            admin_events_dropped,
            publications_over_hop_limit,
            dropped_by_queue,
            dropped_by_shell,
            records_dropped,
            watched: AtomicBool::new(false),
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

    /// Publications dropped for having been through too many
    /// connectors.
    ///
    /// Section 9.2: "Every publication a connector originates carries
    /// a hop count, and one over the configured maximum is dropped and
    /// counted." This is the counting. Section 11.3's rule that
    /// nothing disappears uncounted applies here as much as to a
    /// queue: a connector chain that quietly swallowed a message would
    /// be a support case nobody could answer.
    pub fn publications_over_hop_limit(&self) -> &Counter {
        &self.publications_over_hop_limit
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

    /// An authentication attempt, by outcome.
    ///
    /// Nothing calls this yet and that is the point: see
    /// [`AuthOutcome`]. Milestone 7 is where it starts moving.
    pub fn authentication(&self, outcome: AuthOutcome) -> &Counter {
        &self.authentications[auth_index(outcome)]
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
        // **Cluster members are milestone 5 and are deliberately not
        // registered at all.** An authentication outcome that will
        // always be zero until milestone 7 is still a true statement
        // about this broker; a membership of zero is not, because
        // this node is a member of its own cluster of one and
        // reporting none would be wrong rather than empty.
        //
        // The same goes for section 11.3's cluster bytes sent,
        // received and dropped, which are also milestone 5 and are
        // also absent. There is no inter-node link to count bytes on
        // until section 8 exists, so the three counters could only
        // read zero, and zero here would read as "the links are
        // quiet" rather than "there are no links". Said out loud
        // because the other two cluster deferrals were recorded and
        // these three were not, which is the difference between a
        // decision and an omission.
        let connections = shared.outbox_count();
        let (sessions, subscriptions, retained, queued) = shared.with_broker(|broker| {
            (
                broker.session_count(),
                broker.subscription_count(),
                broker.retained_count(),
                broker.queue_depth(),
            )
        });

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
