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

use parking_lot::Mutex;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;

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
    sockets_closed: [Counter; SOCKET_OUTCOMES.len()],
    /// Indexed by [`REASON_KINDS`] then by [`ALL_REASONS`].
    reasons_sent: [[Counter; ALL_REASONS.len()]; REASON_KINDS.len()],
    /// Registered and never incremented until milestone 7. See
    /// [`AUTH_OUTCOMES`].
    authentications: [Counter; AUTH_OUTCOMES.len()],
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

    /// Render the whole registry in the Prometheus text format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &self.registry.lock())
            .expect("writing to a String cannot fail");
        out
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
