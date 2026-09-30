/**
 * The admin API, in the shapes it actually answers with.
 *
 * Written against `services/broker/crates/admin/src/resources.rs` and
 * `error.rs` rather than guessed at. Two rules from that crate are
 * load bearing here:
 *
 * - **Every failure has one shape**, nested under `error`, with a
 *   `code` from a closed set, a `message` no client may match on, and
 *   sometimes a `field`. So `"error" in body` is a complete test for
 *   a failure, and this module branches on `code` and never on prose.
 * - **A cursor is opaque.** `next` is passed back exactly as given
 *   and nothing is read into it.
 */

/** The one envelope all six read resources answer with. */
export interface Page<T> {
  /** This page's rows, in the resource's order. */
  items: T[];
  /** How many rows the whole resource holds, not how many are here. */
  total: number;
  /** The cursor for the next page, or `null` on the last one. */
  next: string | null;
}

/** The closed set of codes a client may branch on. */
export type ErrorCode =
  | 'invalid_request'
  | 'not_found'
  | 'method_not_allowed'
  | 'not_acceptable'
  | 'unauthorized'
  | 'conflict'
  | 'resource_exhausted'
  | 'loop_detected'
  | 'internal';

/** A failure the API described in its own shape. */
export class ApiError extends Error {
  /** What went wrong, for a machine. */
  readonly code: ErrorCode | 'unreachable';
  /** The request parameter at fault, when the API named one. */
  readonly field: string | null;

  constructor(code: ErrorCode | 'unreachable', message: string, field: string | null = null) {
    super(message);
    this.name = 'ApiError';
    this.code = code;
    this.field = field;
  }
}

/** A session, connected or not. `/clients` is this filtered to the connected. */
export interface SessionView {
  mountpoint: string;
  client_id: string;
  connected: boolean;
  protocol_version: string;
  subscriptions: number;
  queued: number;
  inflight: number;
  expiry_interval: number;
  expires_at: number | null;
}

/** A message the broker is holding for future subscribers of a topic. */
export interface RetainedView {
  topic: string;
  size: number;
  qos: number;
  expires_at: number | null;
  /** The payload, when it is text and at most 1 KiB. */
  text: string | null;
  /** Why `text` is null, when it is: `binary` or `too_large`. */
  text_omitted?: 'binary' | 'too_large';
}

/** One filter one session holds. */
export interface SubscriptionView {
  mountpoint: string;
  client_id: string;
  filter: string;
  qos: number;
  no_local: boolean;
  retain_as_published: boolean;
  retain_handling: number;
  subscription_id: number | null;
}

/** A node in this cluster. */
export interface NodeView {
  id: string;
  name: string;
  /** What peers dial to reach it; empty on a node that does not cluster. */
  address: string;
  this_node: boolean;
  /** `up`, `suspect` or `down`. */
  state: string;
  /** Null for a peer: membership carries no build. */
  version: string | null;
  /** Null for a peer, for the same reason. */
  uptime_seconds: number | null;
}

/** What the publish endpoint answers when it accepted a message. */
export interface PublishAccepted {
  /** How many subscribers it reached. Zero is an ordinary answer. */
  delivered: number;
  /** Whether that answer waited for the store. */
  durable: boolean;
}

/** Whether a body is the API's failure shape. See `error.rs`. */
function isFailure(body: unknown): body is { error: { code: ErrorCode; message: string; field?: string } } {
  return typeof body === 'object' && body !== null && 'error' in body;
}

/**
 * One request, with the API's own errors turned into `ApiError`.
 *
 * A transport failure becomes `unreachable`, which is deliberately
 * **not** one of the API's codes: a broker that did not answer and a
 * broker that answered "no" are different situations and a page that
 * conflates them tells an operator the wrong thing.
 */
async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, init);
  } catch (cause) {
    throw new ApiError('unreachable', `the broker did not answer: ${String(cause)}`);
  }
  const body: unknown = await response.json().catch(() => null);
  if (isFailure(body)) {
    throw new ApiError(body.error.code, body.error.message, body.error.field ?? null);
  }
  if (!response.ok) {
    // A failure with no body in the one shape. Nothing here can say
    // more than the status, and saying more than it knows is how an
    // interface invents a cause.
    throw new ApiError('internal', `the broker answered ${response.status} with no error body`);
  }
  return body as T;
}

/** Ask for one page of a resource. */
export function fetchPage<T>(
  resource: string,
  options: { limit?: number; after?: string | null } = {},
): Promise<Page<T>> {
  const query = new URLSearchParams();
  if (options.limit !== undefined) {
    query.set('limit', String(options.limit));
  }
  if (options.after) {
    // Opaque: passed back exactly as `next` gave it.
    query.set('after', options.after);
  }
  const suffix = query.toString();
  return request<Page<T>>(`/api/v1/${resource}${suffix ? `?${suffix}` : ''}`);
}

/** What the publish endpoint accepts. */
export interface PublishRequest {
  topic: string;
  payload: string;
  qos: number;
  retain: boolean;
  message_expiry_interval?: number;
}

/** Publish through the broker's own publish path. */
export function publish(message: PublishRequest): Promise<PublishAccepted> {
  return request<PublishAccepted>('/api/v1/publish', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(message),
  });
}

// ---------------------------------------------------------------
// Taps
// ---------------------------------------------------------------

/**
 * A tap: a real subscriber inside the broker whose deliveries this
 * interface reads.
 *
 * Written against `services/broker/crates/admin/src/taps.rs`, where
 * the four limits and the reason for each are recorded. The one
 * thing this module must not do is hard-code them: every answer
 * carries `limits`, and the page renders what the broker said rather
 * than what this file believes. A build of the interface older than
 * the broker it is served by would otherwise tell an operator the
 * wrong numbers, and the numbers are the whole point.
 */
export interface TapLimits {
  /** How many taps the broker allows at once. */
  max_taps: number;
  /** How many messages one tap holds before the oldest goes. */
  capacity: number;
  /** The largest payload it carries rather than describes. */
  max_payload_bytes: number;
  /** How long it survives with nobody reading it. */
  idle_timeout_seconds: number;
}

/** One publication a tap caught. */
export interface TappedMessage {
  /** Where this sits in the tap's own sequence. */
  seq: number;
  topic: string;
  /** The level it was delivered at, which is the tap's granted zero. */
  qos: number;
  /** Whether the publisher set the retain flag. */
  retain: boolean;
  /** The payload's size in bytes, whether or not it is carried. */
  size: number;
  /** The payload, when it is text and within the payload cap. */
  text: string | null;
  /** Why `text` is null, when it is: `binary` or `too_large`. */
  text_omitted?: 'binary' | 'too_large';
  /** When the tap caught it, in milliseconds since the epoch. */
  at: number;
}

/** A tap, without its contents. */
export interface TapView {
  id: number;
  filter: string;
  /** The session it is, as the Clients page shows it. */
  client_id: string;
  /** How many publications it has caught since it opened. */
  delivered: number;
  /** How many it dropped from its ring to make room. */
  dropped: number;
  /** How many the broker discarded before they reached the ring. */
  lost: number;
  /** Why the broker closed it, when it has. */
  closed: string | null;
}

/** What opening a tap answers. */
export interface TapOpened extends TapView {
  limits: TapLimits;
}

/** What reading a tap answers. */
export interface TapRead extends TapView {
  limits: TapLimits;
  /** Everything after the cursor that was passed, oldest first. */
  messages: TappedMessage[];
  /** What to pass as `after` next time. */
  next: number;
  /** How many this reader missed between its cursor and the ring. */
  missed: number;
}

/** Open a tap on `filter`. */
export function openTap(filter: string): Promise<TapOpened> {
  return request<TapOpened>('/api/v1/taps', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ filter }),
  });
}

/** Read everything a tap has caught since `after`. */
export function readTap(id: number, after: number | null): Promise<TapRead> {
  const suffix = after === null ? '' : `?after=${after}`;
  return request<TapRead>(`/api/v1/taps/${id}${suffix}`);
}

/**
 * Close a tap.
 *
 * `keepalive` so that a tab being closed still gets the request out:
 * an unclosed tap is not a leak — the broker expires it after
 * `idle_timeout_seconds` — but a tap that leaves when its tab does is
 * a slot somebody else can have now rather than in two minutes.
 * Nothing waits on the answer, and a failure is deliberately
 * swallowed: there is no page left to show it to.
 */
export function closeTap(id: number): void {
  void fetch(`/api/v1/taps/${id}`, { method: 'DELETE', keepalive: true }).catch(() => {});
}

// ---------------------------------------------------------------
// Rules
// ---------------------------------------------------------------

/**
 * A rule, in exactly the shape the API reads and writes.
 *
 * Written against `services/broker/crates/rules/src/spec.rs`, which
 * is the one type a rule is read into whether it came from a
 * `[[rules]]` table or from this page: one serde derive over TOML and
 * JSON, so a key here is the same key an operator writes in a file.
 * That is why this interface is the whole rule and not a subset — a
 * create and an update both send all of it, because the API takes the
 * whole rule and not a patch.
 *
 * **A rule has exactly one action.** Two is refused and none is
 * refused, so exactly one of the four optional tables below is
 * present.
 */
export interface RuleView {
  /** One topic level: no `/`, `+` or `#`. It names the outbox. */
  id: string;
  /** What an operator calls it. */
  name: string;
  /** Whether it fires at all. Not the same as deleting it. */
  enabled: boolean;
  /** The topic filter, for an action that fires on messages. */
  filter?: string | null;
  /** The lowest level that fires it, if it is narrowed. */
  min_qos?: number | null;
  /** The client identifier glob that fires it, if it is narrowed. */
  client_id?: string | null;
  http_out?: HttpOutSpec | null;
  http_in?: HttpInSpec | null;
  http_poll?: HttpPollSpec | null;
  mqtt_bridge?: MqttBridgeSpec | null;
}

/** Which of the four a rule is. */
export type ActionKind = 'http_out' | 'http_in' | 'http_poll' | 'mqtt_bridge';

/** The four, in the order the editor offers them. */
export const ACTIONS: ActionKind[] = ['http_out', 'http_in', 'http_poll', 'mqtt_bridge'];

/** Which action this rule has, or null for one this build cannot name. */
export function actionOf(rule: RuleView): ActionKind | null {
  return ACTIONS.find((kind) => rule[kind] != null) ?? null;
}

/** Post a JSON envelope out. */
export interface HttpOutSpec {
  url: string;
  method: string;
  /**
   * Headers, by name.
   *
   * **Read back with every value empty**, deliberately: a rule's
   * headers are where its bearer token goes and the read resources
   * are not behind the token. An update that sends an empty value
   * keeps the stored one, which is what lets the obvious edit — read
   * a rule, change one field, write it back — not blank a
   * credential.
   */
  headers: Record<string, string>;
  timeout_ms: number;
  max_concurrency: number;
  retries: number;
  backoff_ms: number;
}

/** Accept a post and publish its body. */
export interface HttpInSpec {
  /**
   * The bearer token a request must present.
   *
   * Never read back — it is a secret and this resource needs no
   * token to read. Sent empty on an update, which keeps the stored
   * one; required on a create, where there is nothing to keep.
   */
  token?: string;
  topic: string;
  topic_header?: string | null;
  topic_query?: string | null;
  topic_prefix?: string | null;
  qos: number;
  retain: boolean;
}

/** Fetch a URL on an interval and publish the body. */
export interface HttpPollSpec {
  url: string;
  /** As `HttpOutSpec.headers`: read back empty, kept when sent empty. */
  headers: Record<string, string>;
  interval_ms: number;
  timeout_ms: number;
  max_body_bytes: number;
  topic: string;
  qos: number;
  retain: boolean;
}

/** One topic a bridge carries. */
export interface BridgeTopicSpec {
  filter: string;
  /** `in`, `out` or `both`. `both` on one entry is a loop and is refused. */
  direction: string;
  qos: number;
  local_prefix: string;
  remote_prefix: string;
  template?: string | null;
}

/** Connect to a remote broker as a client. */
export interface MqttBridgeSpec {
  address: string;
  client_id: string;
  /** 4 for MQTT 3.1.1, 5 for MQTT 5. The byte, as every other tool writes it. */
  protocol_level: number;
  username?: string | null;
  /** A secret: never read back, and kept when an update leaves it out. */
  password?: string | null;
  keep_alive_secs: number;
  clean_start: boolean;
  session_expiry_secs: number;
  receive_maximum: number;
  maximum_packet_size: number;
  topic_alias_maximum: number;
  state_topic: string;
  reconnect_min_ms: number;
  reconnect_max_ms: number;
  max_hops: number;
  topics: BridgeTopicSpec[];
}

/** What a rule has done since this process started. */
export interface RuleTotals {
  fired: number;
  delivered: number;
  failed: number;
  dead_lettered: number;
  waiting: number;
}

/** Which series a sparkline can draw. */
export type SeriesName = keyof RuleTotals;

/**
 * One rule's numbers and the series behind them.
 *
 * **Five arrays rather than an array of five-field objects**, which
 * is the broker's shape and is what a sparkline reads: one of them at
 * a time. Shorter than the full minute for a rule installed less than
 * a minute ago, and empty for one installed less than a second ago.
 */
export interface RuleStatsView {
  id: string;
  totals: RuleTotals;
  /** How long one point covers. Sent, so nothing here assumes it. */
  interval_seconds: number;
  series: Record<SeriesName, number[]>;
}

/** One envelope a rule gave up on. */
export interface DeadLetterView {
  /** Which letter this is for this rule, counting from one. */
  sequence: number;
  /** When it was published, in whole seconds since the epoch. */
  at: number;
  /** The topic it went to: `$sd/rules/<id>/dlq`. */
  topic: string;
  /** What was published, as text, cut to what the broker keeps. */
  document: string;
  /** Whether `document` is the whole of it. */
  truncated: boolean;
}

/** A rule identifier in a path. One topic level, but not a safe one. */
function rulePath(id: string): string {
  return `rules/${encodeURIComponent(id)}`;
}

/** Create a rule. Refused if one of that name already exists. */
export function createRule(rule: RuleView): Promise<RuleView> {
  return request<RuleView>('/api/v1/rules', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(rule),
  });
}

/**
 * Replace a rule.
 *
 * The whole rule, not a patch — with the one exception the API makes
 * for a secret it never gave out. See `HttpInSpec.token`.
 */
export function updateRule(rule: RuleView): Promise<RuleView> {
  return request<RuleView>(`/api/v1/${rulePath(rule.id)}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(rule),
  });
}

/** Turn a rule on or off without deleting it. */
export function setRuleEnabled(id: string, enabled: boolean): Promise<RuleView> {
  return request<RuleView>(`/api/v1/${rulePath(id)}/${enabled ? 'enable' : 'disable'}`, {
    method: 'POST',
  });
}

/**
 * Delete a rule and everything running for it.
 *
 * Refused while its outbox holds anything, because deleting a rule
 * deletes its outbox and what is in one is messages this broker told
 * somebody it had accepted.
 */
export function deleteRule(id: string): Promise<{ id: string }> {
  return request<{ id: string }>(`/api/v1/${rulePath(id)}`, { method: 'DELETE' });
}
