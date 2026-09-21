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
  | 'resource_exhausted'
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

/** A node in this cluster; today there is one. */
export interface NodeView {
  id: string;
  this_node: boolean;
  state: string;
  version: string;
  uptime_seconds: number;
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
