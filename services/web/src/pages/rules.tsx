/**
 * Rules: what each one is, what it has been doing, and what it gave
 * up on.
 *
 * # The statistics are polled, and that is a decision
 *
 * Section 11.1 allows this application exactly one events stream, and
 * the Overview page spends it. The stream carries counter deltas,
 * connect and disconnect events and nothing about a rule, so there is
 * no third option here: the sparklines are polled, once a second,
 * with one request covering the whole table — `/api/v1/rule-stats` is
 * a page of every rule's numbers, not a request per row. The Topics
 * page's tap reader made the same choice for the same reason and this
 * is the same cadence, so the two live pages move together.
 *
 * The poll stops while the tab is hidden. A background tab drawing a
 * sparkline nobody is looking at is a request a second the broker
 * answers by copying sixty samples per rule out of a ring, and
 * resuming is one request.
 *
 * **The first read happens whether the tab is hidden or not**, and
 * that is not a detail. A page loaded in a tab that is not in front —
 * opened in the background, restored by a browser on start-up, or
 * driven by anything that is not a person looking at it — would
 * otherwise never make its first request, and the table would say
 * `no samples` beside every rule for as long as it stayed there. That
 * sentence is a claim about the broker, and it would be this page's
 * own silence. Found by loading this page in a hidden tab, where it
 * said exactly that against a broker with a full minute of samples.
 * So the interval skips while hidden, becoming visible reads
 * immediately rather than waiting for the next tick, and the read on
 * mount is unconditional.
 *
 * **The rules themselves are not polled.** A rule changes when an
 * operator edits one, so the list is fetched on load, on a
 * resynchronise, and after this page changes something. A resource
 * that changes at the rate of an operator typing does not belong on a
 * timer.
 *
 * # The table is not windowed, and the sparkline is why it can be
 *
 * The Clients page windows because a broker holds a hundred thousand
 * sessions. Rules are configuration: section 9 imagines tens, the
 * statistics module sizes its memory against two hundred, and the
 * sparkline is one `<path>` element rather than a chart instance, so
 * two hundred rows is two hundred paths and not two hundred canvases
 * with their own event handlers. It is paged all the same, and the
 * footer says how much is held against how much exists, because a
 * table showing the first page as though it were the whole collection
 * is the lie the Clients page is written against.
 *
 * # The editor is per action
 *
 * The four actions have almost nothing in common — one posts, one is
 * posted to, one fetches and one connects — so a form with a mode
 * switch and a union of every field would be four forms wearing one
 * coat. Which action a rule has is chosen once, on a create, and
 * cannot be changed afterwards: an action is what a rule *is*, the
 * identifier is what names its outbox, and a rule that changed from a
 * poller into a bridge would keep an outbox full of envelopes the new
 * action has no idea what to do with. Delete it and write the other
 * one.
 *
 * **A trigger belongs to an action that fires on messages**, which is
 * `http_out` alone. So the filter, the level and the client pattern
 * are on that form and on no other, rather than shown everywhere and
 * refused on three of them. The API refuses a filter on the other
 * three with a sentence naming which mistake was made; this form
 * cannot make it.
 *
 * **What is validated here is what the API would refuse anyway.** The
 * broker is the authority — one `RuleSpec::to_rule`, which a
 * configuration file and this page both go through — and this is a
 * courtesy so that a missing token is a message beside the field
 * rather than a round trip. The rule it must never break is accepting
 * something the broker refuses; where the two disagree the broker
 * wins, and its refusal names the field, so the message lands beside
 * the same input either way. That is why the problems below are keyed
 * by the API's own dotted field names.
 *
 * # Secrets go in and do not come back out
 *
 * An ingest rule's token, a bridge's password and every outbound
 * header value are redacted on the way out — the read resources need
 * no token, and a listing that handed out bearer tokens would be the
 * secret wrapper working exactly as far as the first serde derive. So
 * a rule read back has an empty token, no password, and its header
 * names with empty values.
 *
 * **An update that sends an empty secret keeps the stored one**, which
 * is the admin crate's rule and not this page's. This form therefore
 * sends nothing for a secret left blank, and says on screen that
 * blank means unchanged. A form that sent the empty string it was
 * showing would blank an endpoint's credentials on every save, and
 * the save would look like it worked.
 */
import { useSignal, useSignalEffect, type Signal } from '@preact/signals';
import type { ComponentChildren } from 'preact';
import { useEffect, useRef } from 'preact/hooks';
import * as v from 'valibot';

import {
  ACTIONS,
  ApiError,
  actionOf,
  createRule,
  deleteRule,
  fetchPage,
  setRuleEnabled,
  updateRule,
  type ActionKind,
  type BridgeTopicSpec,
  type DeadLetterView,
  type Page,
  type RuleStatsView,
  type RuleView,
  type SeriesName,
} from '../api.js';
import { generation } from '../live.js';
import { Sparkline } from '../sparkline.js';

/** How many rules one request asks for. */
const PAGE = 200;

/**
 * How many pages of rules this table will fetch.
 *
 * The resource pages and this table draws what it fetched, so the
 * question is what it does at the end of a page. The Clients page
 * fetches more as the window scrolls, which it must: a broker holds a
 * hundred thousand sessions. Rules are configuration and section 9
 * imagines tens, so this reads the collection whole — up to a bound,
 * because "read everything" with no bound is a resource whose size
 * this page does not control. Two thousand rules is eight times what
 * the statistics module sizes its ring against, and a node past it
 * gets a sentence saying what is not shown rather than a table that
 * quietly stops.
 */
const PAGES = 10;

/** How often the statistics are read, in milliseconds. */
const STATS_POLL = 1_000;

/** How many dead letters are asked for at once. */
const LETTERS = 50;

/** What each action is called on screen. */
const CALLED: Record<ActionKind, string> = {
  http_out: 'Post out',
  http_in: 'Accept posts',
  http_poll: 'Poll a URL',
  mqtt_bridge: 'Bridge to a broker',
};

/** What each action does, in one line, on the form that creates one. */
const DOES: Record<ActionKind, string> = {
  http_out: 'A message matching a filter is posted to a URL as a JSON envelope, through a durable outbox, at least once.',
  http_in: 'A POST to /ingest/<id> carrying this rule’s bearer token is published to a topic.',
  http_poll: 'A URL is fetched on an interval and its body is published to a topic.',
  mqtt_bridge: 'This broker connects to a remote one as a client and carries the topics you name.',
};

/** The five series, and what each is called beside the sparkline. */
const SERIES: { name: SeriesName; label: string }[] = [
  { name: 'fired', label: 'fired' },
  { name: 'delivered', label: 'delivered' },
  { name: 'failed', label: 'failed' },
  { name: 'dead_lettered', label: 'dead-lettered' },
  { name: 'waiting', label: 'waiting in the outbox' },
];

// ---------------------------------------------------------------
// What a mistake is called, in the API's own words
// ---------------------------------------------------------------

/**
 * A problem, by the field it is about.
 *
 * Keyed by the API's dotted field names — `id`, `http_in.token`,
 * `mqtt_bridge.topics` — because both halves speak them: the schemas
 * below are written with those keys, and every refusal the broker
 * makes carries the same name. One key space, so a message lands
 * beside the right input whoever produced it.
 */
type Problems = Record<string, string>;

/** Turn valibot's issues into problems, keeping the first per field. */
function problemsOf(issues: readonly v.BaseIssue<unknown>[]): Problems {
  const found: Problems = {};
  for (const issue of issues) {
    const key = issue.path?.[0]?.key;
    if (typeof key === 'string' && !(key in found)) {
      found[key] = issue.message;
    }
  }
  return found;
}

/** A whole number within what the broker's own type can hold. */
function whole(what: string, max: number) {
  return v.pipe(
    v.string(),
    v.regex(/^\d+$/u, `${what} is a whole number`),
    v.check((text) => Number(text) <= max, `${what} is at most ${max.toLocaleString()}`),
  );
}

/** A whole number that may not be zero. */
function positive(what: string, max: number, why: string) {
  return v.pipe(whole(what, max), v.check((text) => Number(text) > 0, why));
}

/** The levels MQTT has, as the form holds them: text. */
const LEVEL = v.picklist(['0', '1', '2'], 'a level is 0, 1 or 2');

/**
 * Whether this URL is one the broker will post to or fetch from.
 *
 * `https`, or `http` to somewhere unambiguously this machine — which
 * is `crates/rules/src/http_out.rs::check_scheme` and its `is_loopback`,
 * restated rather than shared because there is no way to share it. If
 * the two ever disagree the broker's answer is the one that counts and
 * its refusal names `http_out.url`, which is where this message would
 * have gone anyway.
 */
function reachable(url: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  if (parsed.protocol === 'https:') {
    return true;
  }
  if (parsed.protocol !== 'http:') {
    return false;
  }
  return isLoopback(parsed.hostname);
}

/** Whether this host is unambiguously this machine. */
function isLoopback(host: string): boolean {
  const bare = host.replace(/^\[|\]$/gu, '');
  if (bare.toLowerCase() === 'localhost') {
    return true;
  }
  if (/^127(\.\d{1,3}){3}$/u.test(bare)) {
    return true;
  }
  // The only loopback address IPv6 has, in the spellings a URL can
  // carry it in.
  return bare === '::1' || bare === '0:0:0:0:0:0:0:1';
}

/** A topic a rule publishes to: a name, with no wildcard and not reserved. */
function publishable(what: string) {
  return v.pipe(
    v.string(),
    v.minLength(1, `${what} is required: a rule that publishes needs somewhere to publish`),
    v.check((topic) => !topic.includes('+') && !topic.includes('#'), 'a topic published to carries no + or #, which are filter wildcards'),
    v.check((topic) => !topic.includes('\u0000'), 'a topic cannot carry a null character'),
    v.check((topic) => !topic.startsWith('$'), 'the $ namespace is the broker’s own; a rule may not publish into it'),
  );
}

/** What every rule is checked for, whatever it does. */
const Identity = v.object({
  id: v.pipe(
    v.string(),
    v.minLength(1, 'a rule needs an identifier'),
    v.check(
      (id) => !/[/+#\u0000]/u.test(id),
      'an identifier is one topic level: no /, + or #, because it names this rule’s dead-letter topic',
    ),
  ),
  name: v.pipe(v.string(), v.minLength(1, 'a rule needs a name')),
});

/** What an action that fires on messages is checked for. */
const OnMessages = v.object({
  filter: v.pipe(
    v.string(),
    v.minLength(1, 'this rule fires on published messages and has no filter; set one'),
    v.check((filter) => !filter.includes('\u0000'), 'a filter cannot carry a null character'),
  ),
  min_qos: v.union([v.literal(''), LEVEL], 'min_qos is 0, 1 or 2, or nothing'),
});

/** `http_out`, by its API field names. */
const Posting = v.object({
  'http_out.url': v.pipe(
    v.string(),
    v.minLength(1, 'a rule that posts needs somewhere to post'),
    v.check(reachable, 'https, or http to a loopback address: a plain-text endpoint anywhere else is refused'),
  ),
  'http_out.method': v.pipe(v.string(), v.minLength(1, 'a method is required')),
  'http_out.timeout_ms': whole('a timeout', 4_294_967_295),
  'http_out.max_concurrency': positive('a concurrency', 65_535, 'a maximum concurrency of zero is a rule that never delivers anything'),
  'http_out.retries': whole('a retry count', 255),
  'http_out.backoff_ms': whole('a backoff', 4_294_967_295),
});

/** `http_in`. The token is checked by the caller, which knows whether this is a create. */
const Ingesting = v.object({
  'http_in.topic': publishable('a topic'),
  'http_in.topic_header': v.string(),
  'http_in.topic_query': v.string(),
  'http_in.qos': LEVEL,
});

/** `http_poll`. */
const Polling = v.object({
  'http_poll.url': v.pipe(
    v.string(),
    v.minLength(1, 'a poller needs somewhere to fetch from'),
    v.check(reachable, 'https, or http to a loopback address: a plain-text endpoint anywhere else is refused'),
  ),
  'http_poll.interval_ms': positive('an interval', 4_294_967_295, 'an interval of zero is a poller that never fetches'),
  'http_poll.timeout_ms': whole('a timeout', 4_294_967_295),
  'http_poll.max_body_bytes': positive('a body limit', 4_294_967_295, 'a body limit of zero is a poller that can read nothing'),
  'http_poll.topic': publishable('a topic'),
  'http_poll.qos': LEVEL,
});

/** `mqtt_bridge`, less its topics, which are checked one at a time. */
const Bridging = v.object({
  'mqtt_bridge.address': v.pipe(
    v.string(),
    v.minLength(1, 'a bridge needs a remote broker to connect to'),
    v.check(
      (address) => /^mqtts:\/\/\S+$/u.test(address) || (/^mqtt:\/\/\S+$/u.test(address) && isLoopback(hostOf(address))),
      'mqtts://host:port, or mqtt://host:port to a loopback address: a bridge sends its password in the connect',
    ),
  ),
  'mqtt_bridge.protocol_level': v.picklist(['4', '5'], 'write 4 for MQTT 3.1.1 or 5 for MQTT 5'),
  'mqtt_bridge.keep_alive_secs': whole('a keep-alive', 65_535),
  'mqtt_bridge.session_expiry_secs': whole('a session expiry', 4_294_967_295),
  'mqtt_bridge.receive_maximum': positive('a receive maximum', 65_535, 'a receive maximum of zero is a protocol error, and a bridge that can receive nothing'),
  'mqtt_bridge.maximum_packet_size': positive('a maximum packet size', 4_294_967_295, 'a maximum packet size of zero would refuse every packet'),
  'mqtt_bridge.topic_alias_maximum': whole('a topic alias maximum', 65_535),
  'mqtt_bridge.reconnect_min_ms': whole('a reconnect wait', 4_294_967_295),
  'mqtt_bridge.reconnect_max_ms': whole('a reconnect wait', 4_294_967_295),
  'mqtt_bridge.max_hops': positive('a hop limit', 4_294_967_295, 'a hop limit of zero is a bridge that carries nothing'),
});

/** The host of an `mqtt://` address, for the loopback check. */
function hostOf(address: string): string {
  // `new URL` does not parse an mqtt scheme's authority, so this
  // takes the authority itself and drops a port and any credentials.
  const authority = address.replace(/^mqtts?:\/\//u, '').split('/')[0];
  const withoutUser = authority.split('@').pop() ?? '';
  if (withoutUser.startsWith('[')) {
    return withoutUser.slice(0, withoutUser.indexOf(']') + 1).replace(/^\[|\]$/gu, '');
  }
  return withoutUser.split(':')[0];
}

// ---------------------------------------------------------------
// The draft a form holds
// ---------------------------------------------------------------

/** One header, as two inputs. */
interface HeaderDraft {
  name: string;
  value: string;
}

/** One bridge topic, as its row of inputs. */
interface TopicDraft {
  filter: string;
  direction: string;
  qos: string;
  local_prefix: string;
  remote_prefix: string;
  template: string;
}

/**
 * Everything a form holds, as text.
 *
 * Text and not numbers, because an `<input>` holds text and a number
 * field that cannot be empty while it is being retyped is a field
 * that fights its user. The conversion happens once, at the end,
 * where the refusal can be attached to the field.
 */
interface Draft {
  id: string;
  name: string;
  enabled: boolean;
  kind: ActionKind;
  filter: string;
  min_qos: string;
  client_id: string;
  url: string;
  method: string;
  headers: HeaderDraft[];
  timeout_ms: string;
  max_concurrency: string;
  retries: string;
  backoff_ms: string;
  token: string;
  topic: string;
  source: 'none' | 'header' | 'query';
  source_name: string;
  topic_prefix: string;
  qos: string;
  retain: boolean;
  interval_ms: string;
  max_body_bytes: string;
  address: string;
  client_identifier: string;
  protocol_level: string;
  username: string;
  password: string;
  keep_alive_secs: string;
  clean_start: boolean;
  session_expiry_secs: string;
  receive_maximum: string;
  maximum_packet_size: string;
  topic_alias_maximum: string;
  state_topic: string;
  reconnect_min_ms: string;
  reconnect_max_ms: string;
  max_hops: string;
  topics: TopicDraft[];
}

/**
 * A new rule of this kind, at the broker's own defaults.
 *
 * The defaults are the rules crate's, restated: `HttpOut::default`,
 * `HttpPoll::default` and `MqttBridge::default`. A URL, a topic and an
 * address are empty in all three because a default that reaches
 * somewhere is worse than one that reaches nowhere, and this form
 * keeps that rather than helpfully filling in an example.
 */
function blank(kind: ActionKind): Draft {
  return {
    id: '',
    name: '',
    enabled: true,
    kind,
    filter: '',
    min_qos: '',
    client_id: '',
    url: '',
    method: 'POST',
    headers: [],
    timeout_ms: '10000',
    max_concurrency: '4',
    retries: '3',
    backoff_ms: '1000',
    token: '',
    topic: '',
    source: 'none',
    source_name: '',
    topic_prefix: '',
    qos: '0',
    retain: false,
    interval_ms: '60000',
    max_body_bytes: '262144',
    address: '',
    client_identifier: '',
    protocol_level: '5',
    username: '',
    password: '',
    keep_alive_secs: '60',
    clean_start: false,
    session_expiry_secs: '3600',
    receive_maximum: '32',
    maximum_packet_size: '1048576',
    topic_alias_maximum: '16',
    state_topic: '',
    reconnect_min_ms: '1000',
    reconnect_max_ms: '60000',
    max_hops: '8',
    topics: [],
  };
}

/** A header map as rows, sorted as the API sorts it. */
function headerRows(headers: Record<string, string>): HeaderDraft[] {
  return Object.keys(headers)
    .sort()
    .map((name) => ({ name, value: headers[name] }));
}

/** A rule as a draft, for editing one that exists. */
function draftOf(rule: RuleView): Draft {
  const kind = actionOf(rule) ?? 'http_out';
  const draft = blank(kind);
  draft.id = rule.id;
  draft.name = rule.name;
  draft.enabled = rule.enabled;
  draft.filter = rule.filter ?? '';
  draft.min_qos = rule.min_qos == null ? '' : String(rule.min_qos);
  draft.client_id = rule.client_id ?? '';
  if (rule.http_out) {
    draft.url = rule.http_out.url;
    draft.method = rule.http_out.method;
    draft.headers = headerRows(rule.http_out.headers);
    draft.timeout_ms = String(rule.http_out.timeout_ms);
    draft.max_concurrency = String(rule.http_out.max_concurrency);
    draft.retries = String(rule.http_out.retries);
    draft.backoff_ms = String(rule.http_out.backoff_ms);
  }
  if (rule.http_in) {
    draft.topic = rule.http_in.topic;
    draft.source = rule.http_in.topic_header ? 'header' : rule.http_in.topic_query ? 'query' : 'none';
    draft.source_name = rule.http_in.topic_header ?? rule.http_in.topic_query ?? '';
    draft.topic_prefix = rule.http_in.topic_prefix ?? '';
    draft.qos = String(rule.http_in.qos);
    draft.retain = rule.http_in.retain;
  }
  if (rule.http_poll) {
    draft.url = rule.http_poll.url;
    draft.headers = headerRows(rule.http_poll.headers);
    draft.interval_ms = String(rule.http_poll.interval_ms);
    draft.timeout_ms = String(rule.http_poll.timeout_ms);
    draft.max_body_bytes = String(rule.http_poll.max_body_bytes);
    draft.topic = rule.http_poll.topic;
    draft.qos = String(rule.http_poll.qos);
    draft.retain = rule.http_poll.retain;
  }
  if (rule.mqtt_bridge) {
    const bridge = rule.mqtt_bridge;
    draft.address = bridge.address;
    draft.client_identifier = bridge.client_id;
    draft.protocol_level = String(bridge.protocol_level);
    draft.username = bridge.username ?? '';
    draft.keep_alive_secs = String(bridge.keep_alive_secs);
    draft.clean_start = bridge.clean_start;
    draft.session_expiry_secs = String(bridge.session_expiry_secs);
    draft.receive_maximum = String(bridge.receive_maximum);
    draft.maximum_packet_size = String(bridge.maximum_packet_size);
    draft.topic_alias_maximum = String(bridge.topic_alias_maximum);
    draft.state_topic = bridge.state_topic;
    draft.reconnect_min_ms = String(bridge.reconnect_min_ms);
    draft.reconnect_max_ms = String(bridge.reconnect_max_ms);
    draft.max_hops = String(bridge.max_hops);
    draft.topics = bridge.topics.map((topic) => ({
      filter: topic.filter,
      direction: topic.direction,
      qos: String(topic.qos),
      local_prefix: topic.local_prefix,
      remote_prefix: topic.remote_prefix,
      template: topic.template ?? '',
    }));
  }
  return draft;
}

/** The headers a form is sending, less the ones with no name. */
function headerMap(rows: HeaderDraft[]): Record<string, string> {
  const headers: Record<string, string> = {};
  for (const row of rows) {
    const name = row.name.trim();
    if (name) {
      // An empty value is deliberate and is not dropped: on an update
      // it means "keep the stored one", which is how a bearer token
      // survives an edit it was never shown in.
      headers[name] = row.value;
    }
  }
  return headers;
}

/** What checking a draft produced. */
type Checked = { rule: RuleView; problems?: undefined } | { rule?: undefined; problems: Problems };

/**
 * A draft as a rule, or the problems that stop it being one.
 *
 * `creating` is why the token is checked here rather than in a
 * schema: on a create there is nothing stored to fall back on and a
 * missing token is a rule the broker refuses, and on an update an
 * empty one is the ordinary case and means "unchanged".
 */
function check(draft: Draft, creating: boolean): Checked {
  const problems: Problems = {};
  const identity = v.safeParse(Identity, { id: draft.id.trim(), name: draft.name.trim() });
  if (!identity.success) {
    Object.assign(problems, problemsOf(identity.issues));
  }

  const rule: RuleView = { id: draft.id.trim(), name: draft.name.trim(), enabled: draft.enabled };

  if (draft.kind === 'http_out') {
    const trigger = v.safeParse(OnMessages, { filter: draft.filter.trim(), min_qos: draft.min_qos });
    if (!trigger.success) {
      Object.assign(problems, problemsOf(trigger.issues));
    }
    const action = v.safeParse(Posting, {
      'http_out.url': draft.url.trim(),
      'http_out.method': draft.method.trim(),
      'http_out.timeout_ms': draft.timeout_ms.trim(),
      'http_out.max_concurrency': draft.max_concurrency.trim(),
      'http_out.retries': draft.retries.trim(),
      'http_out.backoff_ms': draft.backoff_ms.trim(),
    });
    if (!action.success) {
      Object.assign(problems, problemsOf(action.issues));
    }
    rule.filter = draft.filter.trim();
    if (draft.min_qos !== '') {
      rule.min_qos = Number(draft.min_qos);
    }
    if (draft.client_id.trim()) {
      rule.client_id = draft.client_id.trim();
    }
    rule.http_out = {
      url: draft.url.trim(),
      method: draft.method.trim(),
      headers: headerMap(draft.headers),
      timeout_ms: Number(draft.timeout_ms),
      max_concurrency: Number(draft.max_concurrency),
      retries: Number(draft.retries),
      backoff_ms: Number(draft.backoff_ms),
    };
  }

  if (draft.kind === 'http_in') {
    const action = v.safeParse(Ingesting, {
      'http_in.topic': draft.topic.trim(),
      'http_in.topic_header': draft.source === 'header' ? draft.source_name.trim() : '',
      'http_in.topic_query': draft.source === 'query' ? draft.source_name.trim() : '',
      'http_in.qos': draft.qos,
    });
    if (!action.success) {
      Object.assign(problems, problemsOf(action.issues));
    }
    if (creating && !draft.token) {
      problems['http_in.token'] =
        'this endpoint publishes, so it is not allowed to be unguarded: set a token';
    }
    const named = draft.source === 'header' ? 'http_in.topic_header' : 'http_in.topic_query';
    if (draft.source !== 'none') {
      if (!draft.source_name.trim()) {
        problems[named] = 'name the header or the query parameter a request chooses its topic in';
      }
      const prefix = draft.topic_prefix.trim();
      if (!prefix) {
        problems['http_in.topic_prefix'] =
          'a prefix is the whole safety of letting a request choose a topic: without one, whoever holds this token can publish anywhere';
      } else if (prefix.endsWith('/')) {
        problems['http_in.topic_prefix'] =
          'a prefix is written without a trailing separator; with one it ends in an empty level no caller can satisfy';
      } else if (prefix.startsWith('$')) {
        problems['http_in.topic_prefix'] = 'the $ namespace is the broker’s own';
      } else if (/[+#]/u.test(prefix)) {
        problems['http_in.topic_prefix'] = 'a prefix carries no + or #, which are filter wildcards';
      }
    }
    rule.http_in = {
      topic: draft.topic.trim(),
      qos: Number(draft.qos),
      retain: draft.retain,
      ...(draft.token ? { token: draft.token } : {}),
      ...(draft.source === 'header' ? { topic_header: draft.source_name.trim() } : {}),
      ...(draft.source === 'query' ? { topic_query: draft.source_name.trim() } : {}),
      ...(draft.source === 'none' ? {} : { topic_prefix: draft.topic_prefix.trim() }),
    };
  }

  if (draft.kind === 'http_poll') {
    const action = v.safeParse(Polling, {
      'http_poll.url': draft.url.trim(),
      'http_poll.interval_ms': draft.interval_ms.trim(),
      'http_poll.timeout_ms': draft.timeout_ms.trim(),
      'http_poll.max_body_bytes': draft.max_body_bytes.trim(),
      'http_poll.topic': draft.topic.trim(),
      'http_poll.qos': draft.qos,
    });
    if (!action.success) {
      Object.assign(problems, problemsOf(action.issues));
    }
    rule.http_poll = {
      url: draft.url.trim(),
      headers: headerMap(draft.headers),
      interval_ms: Number(draft.interval_ms),
      timeout_ms: Number(draft.timeout_ms),
      max_body_bytes: Number(draft.max_body_bytes),
      topic: draft.topic.trim(),
      qos: Number(draft.qos),
      retain: draft.retain,
    };
  }

  if (draft.kind === 'mqtt_bridge') {
    const action = v.safeParse(Bridging, {
      'mqtt_bridge.address': draft.address.trim(),
      'mqtt_bridge.protocol_level': draft.protocol_level,
      'mqtt_bridge.keep_alive_secs': draft.keep_alive_secs.trim(),
      'mqtt_bridge.session_expiry_secs': draft.session_expiry_secs.trim(),
      'mqtt_bridge.receive_maximum': draft.receive_maximum.trim(),
      'mqtt_bridge.maximum_packet_size': draft.maximum_packet_size.trim(),
      'mqtt_bridge.topic_alias_maximum': draft.topic_alias_maximum.trim(),
      'mqtt_bridge.reconnect_min_ms': draft.reconnect_min_ms.trim(),
      'mqtt_bridge.reconnect_max_ms': draft.reconnect_max_ms.trim(),
      'mqtt_bridge.max_hops': draft.max_hops.trim(),
    });
    if (!action.success) {
      Object.assign(problems, problemsOf(action.issues));
    }
    if (draft.protocol_level === '4' && !draft.client_identifier.trim()) {
      problems['mqtt_bridge.client_id'] =
        'MQTT 3.1.1 has no way for the remote to say what identifier it assigned, so a session could never be resumed: set one, or speak MQTT 5';
    }
    if (draft.topics.length === 0) {
      problems['mqtt_bridge.topics'] = 'a bridge that carries nothing is a bridge nobody meant: add a topic';
    }
    draft.topics.forEach((topic, index) => {
      if (!topic.filter.trim() && !problems['mqtt_bridge.topics']) {
        problems['mqtt_bridge.topics'] = `topic ${index + 1} has no filter`;
      }
    });
    const bridge = {
      address: draft.address.trim(),
      client_id: draft.client_identifier.trim(),
      protocol_level: Number(draft.protocol_level),
      keep_alive_secs: Number(draft.keep_alive_secs),
      clean_start: draft.clean_start,
      session_expiry_secs: Number(draft.session_expiry_secs),
      receive_maximum: Number(draft.receive_maximum),
      maximum_packet_size: Number(draft.maximum_packet_size),
      topic_alias_maximum: Number(draft.topic_alias_maximum),
      state_topic: draft.state_topic.trim(),
      reconnect_min_ms: Number(draft.reconnect_min_ms),
      reconnect_max_ms: Number(draft.reconnect_max_ms),
      max_hops: Number(draft.max_hops),
      topics: draft.topics.map((topic): BridgeTopicSpec => ({
        filter: topic.filter.trim(),
        direction: topic.direction,
        qos: Number(topic.qos),
        local_prefix: topic.local_prefix.trim(),
        remote_prefix: topic.remote_prefix.trim(),
        ...(topic.template.trim() ? { template: topic.template.trim() } : {}),
      })),
      ...(draft.username.trim() ? { username: draft.username.trim() } : {}),
      // Left out when blank, which keeps the stored one. See the
      // module note on secrets.
      ...(draft.password ? { password: draft.password } : {}),
    };
    rule.mqtt_bridge = bridge;
  }

  return Object.keys(problems).length > 0 ? { problems } : { rule };
}

// ---------------------------------------------------------------
// The form
// ---------------------------------------------------------------

/** One labelled input, with the message about it underneath. */
function Field({
  label,
  field,
  problems,
  hint,
  children,
}: {
  label: string;
  field?: string;
  problems: Problems;
  hint?: string;
  children: ComponentChildren;
}) {
  const problem = field ? problems[field] : undefined;
  return (
    <label>
      {label}
      {children}
      {hint ? <span class="hint">{hint}</span> : null}
      {problem ? <span class="problem">{problem}</span> : null}
    </label>
  );
}

/** A text input bound to one key of the draft. */
function Text({
  draft,
  field,
  name,
  problems,
  placeholder,
  password,
}: {
  draft: Signal<Draft>;
  field: keyof Draft;
  name?: string;
  problems: Problems;
  placeholder?: string;
  password?: boolean;
}) {
  const value = draft.value[field];
  return (
    <input
      type={password ? 'password' : 'text'}
      value={typeof value === 'string' ? value : ''}
      placeholder={placeholder}
      aria-invalid={name && problems[name] ? 'true' : undefined}
      onInput={(event) => {
        draft.value = { ...draft.value, [field]: (event.target as HTMLInputElement).value };
      }}
    />
  );
}

/** A checkbox bound to one key of the draft. */
function Check({
  draft,
  field,
  label,
}: {
  draft: Signal<Draft>;
  field: keyof Draft;
  label: string;
}) {
  return (
    <label class="inline">
      <input
        type="checkbox"
        checked={Boolean(draft.value[field])}
        onChange={(event) => {
          draft.value = { ...draft.value, [field]: (event.target as HTMLInputElement).checked };
        }}
      />
      {label}
    </label>
  );
}

/** The three levels, as a select bound to one key. */
function Level({ draft, field }: { draft: Signal<Draft>; field: keyof Draft }) {
  return (
    <select
      value={String(draft.value[field])}
      onChange={(event) => {
        draft.value = { ...draft.value, [field]: (event.target as HTMLSelectElement).value };
      }}
    >
      <option value="0">0 — at most once</option>
      <option value="1">1 — at least once</option>
      <option value="2">2 — exactly once</option>
    </select>
  );
}

/** The header table, shared by the two actions that send headers. */
function Headers({ draft, problems, field }: { draft: Signal<Draft>; problems: Problems; field: string }) {
  const rows = draft.value.headers;
  const set = (next: HeaderDraft[]) => {
    draft.value = { ...draft.value, headers: next };
  };
  return (
    <div class="headers">
      <span class="label">Headers</span>
      {rows.map((row, index) => (
        <div class="header-row" key={index}>
          <input
            type="text"
            value={row.name}
            placeholder="name"
            onInput={(event) =>
              set(rows.map((held, at) => (at === index ? { ...held, name: (event.target as HTMLInputElement).value } : held)))
            }
          />
          <input
            type="text"
            value={row.value}
            placeholder="value"
            onInput={(event) =>
              set(rows.map((held, at) => (at === index ? { ...held, value: (event.target as HTMLInputElement).value } : held)))
            }
          />
          <button type="button" onClick={() => set(rows.filter((_, at) => at !== index))}>
            Remove
          </button>
        </div>
      ))}
      <button type="button" onClick={() => set([...rows, { name: '', value: '' }])}>
        Add a header
      </button>
      {problems[field] ? <span class="problem">{problems[field]}</span> : null}
      <span class="hint">
        A header value is a secret as often as not, so this API never reads one back: the names come
        back with empty values. <strong>A value left empty on a save keeps the stored one.</strong>
      </span>
    </div>
  );
}

/** The bridge's topic entries. */
function Topics({ draft, problems }: { draft: Signal<Draft>; problems: Problems }) {
  const rows = draft.value.topics;
  const set = (next: TopicDraft[]) => {
    draft.value = { ...draft.value, topics: next };
  };
  const change = (index: number, key: keyof TopicDraft, value: string) =>
    set(rows.map((held, at) => (at === index ? { ...held, [key]: value } : held)));
  return (
    <div class="bridge-topics">
      <span class="label">Topics</span>
      {rows.map((row, index) => (
        <div class="topic-row" key={index}>
          <input
            type="text"
            value={row.filter}
            placeholder="filter, carrying neither prefix"
            onInput={(event) => change(index, 'filter', (event.target as HTMLInputElement).value)}
          />
          <select
            value={row.direction}
            onChange={(event) => change(index, 'direction', (event.target as HTMLSelectElement).value)}
          >
            <option value="out">out — to the remote</option>
            <option value="in">in — from the remote</option>
            <option value="both">both</option>
          </select>
          <select value={row.qos} onChange={(event) => change(index, 'qos', (event.target as HTMLSelectElement).value)}>
            <option value="0">QoS 0</option>
            <option value="1">QoS 1</option>
            <option value="2">QoS 2</option>
          </select>
          <input
            type="text"
            value={row.local_prefix}
            placeholder="local prefix"
            onInput={(event) => change(index, 'local_prefix', (event.target as HTMLInputElement).value)}
          />
          <input
            type="text"
            value={row.remote_prefix}
            placeholder="remote prefix"
            onInput={(event) => change(index, 'remote_prefix', (event.target as HTMLInputElement).value)}
          />
          <input
            type="text"
            value={row.template}
            placeholder="template, outbound only"
            onInput={(event) => change(index, 'template', (event.target as HTMLInputElement).value)}
          />
          <button type="button" onClick={() => set(rows.filter((_, at) => at !== index))}>
            Remove
          </button>
        </div>
      ))}
      <button
        type="button"
        onClick={() =>
          set([...rows, { filter: '', direction: 'out', qos: '0', local_prefix: '', remote_prefix: '', template: '' }])
        }
      >
        Add a topic
      </button>
      {problems['mqtt_bridge.topics'] ? (
        <span class="problem">{problems['mqtt_bridge.topics']}</span>
      ) : null}
      <span class="hint">
        A filter carries neither prefix. <strong>both</strong> on one entry publishes what it
        subscribes to and is refused unless the two prefixes keep the sets apart; two entries is
        the ordinary way to carry a topic in each direction.
      </span>
    </div>
  );
}

/**
 * The editor: one form, four shapes.
 *
 * `existing` is null for a create. The action cannot be changed on an
 * edit, and the identifier cannot either: it names the outbox and the
 * dead-letter topic, which is why the API refuses a body whose `id`
 * disagrees with its path.
 */
function Editor({
  existing,
  onDone,
  onCancel,
}: {
  existing: RuleView | null;
  onDone: (rule: RuleView) => void;
  onCancel: () => void;
}) {
  const draft = useSignal<Draft>(existing ? draftOf(existing) : blank('http_out'));
  const problems = useSignal<Problems>({});
  const said = useSignal<string | null>(null);
  const failed = useSignal(false);
  const saving = useSignal(false);
  const creating = existing === null;

  const save = async (event: Event) => {
    event.preventDefault();
    said.value = null;
    const checked = check(draft.value, creating);
    if (checked.problems) {
      problems.value = checked.problems;
      failed.value = true;
      said.value = 'The rule was not sent: see the fields marked below.';
      return;
    }
    problems.value = {};
    saving.value = true;
    try {
      const written = creating ? await createRule(checked.rule) : await updateRule(checked.rule);
      failed.value = false;
      onDone(written);
    } catch (error) {
      failed.value = true;
      if (error instanceof ApiError) {
        said.value = `The broker refused it (${error.code}): ${error.message}`;
        if (error.field) {
          // The refusal names the field in the same spelling this
          // form keys its problems by, so it lands beside the input.
          problems.value = { [error.field]: error.message };
        }
      } else {
        said.value = String(error);
      }
    } finally {
      saving.value = false;
    }
  };

  const held = problems.value;
  const kind = draft.value.kind;
  return (
    <form class="editor" onSubmit={(event) => void save(event)}>
      <h2>{creating ? 'A new rule' : `Editing ${existing.id}`}</h2>
      <div class="fields">
        {creating ? (
          <Field label="What it does" problems={held} hint={DOES[kind]}>
            <select
              value={kind}
              onChange={(event) =>
                (draft.value = {
                  ...blank((event.target as HTMLSelectElement).value as ActionKind),
                  id: draft.value.id,
                  name: draft.value.name,
                })
              }
            >
              {ACTIONS.map((action) => (
                <option value={action} key={action}>
                  {CALLED[action]} — {action}
                </option>
              ))}
            </select>
          </Field>
        ) : (
          <Field label="What it does" problems={held} hint="An action is what a rule is. To change it, delete this rule and write the other one: the identifier names an outbox, and envelopes waiting in it belong to this action.">
            <input type="text" value={`${CALLED[kind]} — ${kind}`} readonly />
          </Field>
        )}

        <Field
          label="Identifier"
          field="id"
          problems={held}
          hint="One topic level. It names this rule’s outbox and its dead-letter topic, so it cannot be changed later."
        >
          {creating ? (
            <Text draft={draft} field="id" name="id" problems={held} placeholder="telemetry" />
          ) : (
            <input type="text" value={draft.value.id} readonly />
          )}
        </Field>

        <Field label="Name" field="name" problems={held}>
          <Text draft={draft} field="name" name="name" problems={held} placeholder="Telemetry out" />
        </Field>

        <Check draft={draft} field="enabled" label="Enabled" />
      </div>

      {kind === 'http_out' ? (
        <div class="fields">
          <Field label="Filter" field="filter" problems={held} hint="The topic filter a message must match. This is the only action fired by a published message, so it is the only one with a filter.">
            <Text draft={draft} field="filter" name="filter" problems={held} placeholder="sensor/+/temp" />
          </Field>
          <Field label="Minimum QoS" field="min_qos" problems={held} hint="Optional: only messages at this level or above.">
            <select
              value={draft.value.min_qos}
              onChange={(event) =>
                (draft.value = { ...draft.value, min_qos: (event.target as HTMLSelectElement).value })
              }
            >
              <option value="">any</option>
              <option value="0">0 and above</option>
              <option value="1">1 and above</option>
              <option value="2">2 only</option>
            </select>
          </Field>
          <Field label="Client pattern" problems={held} hint="Optional. * stands for any run of characters and is the whole of the syntax.">
            <Text draft={draft} field="client_id" problems={held} placeholder="sensor-*" />
          </Field>
          <Field label="URL" field="http_out.url" problems={held} hint="https, or http to a loopback address.">
            <Text draft={draft} field="url" name="http_out.url" problems={held} placeholder="https://example.com/hook" />
          </Field>
          <Field label="Method" field="http_out.method" problems={held}>
            <Text draft={draft} field="method" name="http_out.method" problems={held} />
          </Field>
          <Field label="Timeout (ms)" field="http_out.timeout_ms" problems={held}>
            <Text draft={draft} field="timeout_ms" name="http_out.timeout_ms" problems={held} />
          </Field>
          <Field label="Concurrency" field="http_out.max_concurrency" problems={held} hint="How many attempts may be in flight at once.">
            <Text draft={draft} field="max_concurrency" name="http_out.max_concurrency" problems={held} />
          </Field>
          <Field label="Retries" field="http_out.retries" problems={held} hint="Attempts after the first. Delivery is at least once either way.">
            <Text draft={draft} field="retries" name="http_out.retries" problems={held} />
          </Field>
          <Field label="Backoff (ms)" field="http_out.backoff_ms" problems={held} hint="Doubling, capped at five minutes, and belonging to the rule rather than to each envelope.">
            <Text draft={draft} field="backoff_ms" name="http_out.backoff_ms" problems={held} />
          </Field>
          <Headers draft={draft} problems={held} field="http_out.headers" />
        </div>
      ) : null}

      {kind === 'http_in' ? (
        <div class="fields">
          <Field
            label="Token"
            field="http_in.token"
            problems={held}
            hint={
              creating
                ? 'Required: this endpoint publishes, so an unguarded one is a hole rather than a degraded mode. Sent as Authorization: Bearer <token>.'
                : 'Left empty, the stored token is kept. This API never reads a token back, so an empty box here is not an empty token.'
            }
          >
            <Text draft={draft} field="token" name="http_in.token" problems={held} password placeholder={creating ? '' : 'unchanged'} />
          </Field>
          <Field label="Topic" field="http_in.topic" problems={held} hint="Where a body is published when the request does not choose.">
            <Text draft={draft} field="topic" name="http_in.topic" problems={held} placeholder="ingest/webhook" />
          </Field>
          <Field label="Let the request choose a topic" problems={held} hint="From a header or a query parameter, bounded by a prefix. Without a prefix, whoever holds the token can publish anywhere.">
            <select
              value={draft.value.source}
              onChange={(event) =>
                (draft.value = {
                  ...draft.value,
                  source: (event.target as HTMLSelectElement).value as Draft['source'],
                })
              }
            >
              <option value="none">no, always the topic above</option>
              <option value="header">from a header</option>
              <option value="query">from a query parameter</option>
            </select>
          </Field>
          {draft.value.source !== 'none' ? (
            <>
              <Field
                label={draft.value.source === 'header' ? 'Header name' : 'Query parameter'}
                field={draft.value.source === 'header' ? 'http_in.topic_header' : 'http_in.topic_query'}
                problems={held}
              >
                <Text
                  draft={draft}
                  field="source_name"
                  name={draft.value.source === 'header' ? 'http_in.topic_header' : 'http_in.topic_query'}
                  problems={held}
                  placeholder={draft.value.source === 'header' ? 'x-topic' : 'topic'}
                />
              </Field>
              <Field label="Topic prefix" field="http_in.topic_prefix" problems={held} hint="Written without a trailing separator.">
                <Text draft={draft} field="topic_prefix" name="http_in.topic_prefix" problems={held} placeholder="ingest" />
              </Field>
            </>
          ) : null}
          <Field label="QoS" field="http_in.qos" problems={held}>
            <Level draft={draft} field="qos" />
          </Field>
          <Check draft={draft} field="retain" label="Retain" />
        </div>
      ) : null}

      {kind === 'http_poll' ? (
        <div class="fields">
          <Field label="URL" field="http_poll.url" problems={held} hint="https, or http to a loopback address.">
            <Text draft={draft} field="url" name="http_poll.url" problems={held} placeholder="https://example.com/state.json" />
          </Field>
          <Field label="Interval (ms)" field="http_poll.interval_ms" problems={held} hint="One fetch is ever in flight; a fetch slower than this loses the intervals it covered.">
            <Text draft={draft} field="interval_ms" name="http_poll.interval_ms" problems={held} />
          </Field>
          <Field label="Timeout (ms)" field="http_poll.timeout_ms" problems={held}>
            <Text draft={draft} field="timeout_ms" name="http_poll.timeout_ms" problems={held} />
          </Field>
          <Field label="Body limit (bytes)" field="http_poll.max_body_bytes" problems={held} hint="A body past this is a failed fetch, not a truncated publication.">
            <Text draft={draft} field="max_body_bytes" name="http_poll.max_body_bytes" problems={held} />
          </Field>
          <Field label="Topic" field="http_poll.topic" problems={held}>
            <Text draft={draft} field="topic" name="http_poll.topic" problems={held} placeholder="feeds/state" />
          </Field>
          <Field label="QoS" field="http_poll.qos" problems={held}>
            <Level draft={draft} field="qos" />
          </Field>
          <Check draft={draft} field="retain" label="Retain — usually what a poller wants, and off by default because a retained message outlives the rule that wrote it" />
          <Headers draft={draft} problems={held} field="http_poll.headers" />
        </div>
      ) : null}

      {kind === 'mqtt_bridge' ? (
        <div class="fields">
          <Field label="Address" field="mqtt_bridge.address" problems={held} hint="mqtts://host:port, or mqtt://host:port to this machine.">
            <Text draft={draft} field="address" name="mqtt_bridge.address" problems={held} placeholder="mqtts://remote.example:8883" />
          </Field>
          <Field label="Protocol" field="mqtt_bridge.protocol_level" problems={held}>
            <select
              value={draft.value.protocol_level}
              onChange={(event) =>
                (draft.value = {
                  ...draft.value,
                  protocol_level: (event.target as HTMLSelectElement).value,
                })
              }
            >
              <option value="5">5 — MQTT 5</option>
              <option value="4">4 — MQTT 3.1.1</option>
            </select>
          </Field>
          <Field label="Client identifier" field="mqtt_bridge.client_id" problems={held} hint="What this bridge calls itself to the remote. Required on MQTT 3.1.1.">
            <Text draft={draft} field="client_identifier" name="mqtt_bridge.client_id" problems={held} placeholder="sd-mqtt-bridge" />
          </Field>
          <Field label="User name" problems={held}>
            <Text draft={draft} field="username" problems={held} />
          </Field>
          <Field
            label="Password"
            problems={held}
            hint={creating ? 'Offered in the connect.' : 'Left empty, the stored password is kept.'}
          >
            <Text draft={draft} field="password" problems={held} password placeholder={creating ? '' : 'unchanged'} />
          </Field>
          <Field label="Keep-alive (s)" field="mqtt_bridge.keep_alive_secs" problems={held} hint="A proposal: MQTT 5 lets the remote override it.">
            <Text draft={draft} field="keep_alive_secs" name="mqtt_bridge.keep_alive_secs" problems={held} />
          </Field>
          <Check draft={draft} field="clean_start" label="Clean start — the first connection of a process asks for one whatever this says" />
          <Field label="Session expiry (s)" field="mqtt_bridge.session_expiry_secs" problems={held}>
            <Text draft={draft} field="session_expiry_secs" name="mqtt_bridge.session_expiry_secs" problems={held} />
          </Field>
          <Field label="Receive maximum" field="mqtt_bridge.receive_maximum" problems={held}>
            <Text draft={draft} field="receive_maximum" name="mqtt_bridge.receive_maximum" problems={held} />
          </Field>
          <Field label="Maximum packet size" field="mqtt_bridge.maximum_packet_size" problems={held}>
            <Text draft={draft} field="maximum_packet_size" name="mqtt_bridge.maximum_packet_size" problems={held} />
          </Field>
          <Field label="Topic alias maximum" field="mqtt_bridge.topic_alias_maximum" problems={held}>
            <Text draft={draft} field="topic_alias_maximum" name="mqtt_bridge.topic_alias_maximum" problems={held} />
          </Field>
          <Field label="State topic" problems={held} hint="Where this bridge’s connectivity is reported here, and its will topic on the remote. A topic this bridge would itself carry is refused.">
            <Text draft={draft} field="state_topic" problems={held} placeholder="$sd/bridges/remote/state" />
          </Field>
          <Field label="Reconnect, first wait (ms)" field="mqtt_bridge.reconnect_min_ms" problems={held}>
            <Text draft={draft} field="reconnect_min_ms" name="mqtt_bridge.reconnect_min_ms" problems={held} />
          </Field>
          <Field label="Reconnect, longest wait (ms)" field="mqtt_bridge.reconnect_max_ms" problems={held}>
            <Text draft={draft} field="reconnect_max_ms" name="mqtt_bridge.reconnect_max_ms" problems={held} />
          </Field>
          <Field label="Hop limit" field="mqtt_bridge.max_hops" problems={held} hint="A publication above this is dropped, which is what stops a chain of bridges going round.">
            <Text draft={draft} field="max_hops" name="mqtt_bridge.max_hops" problems={held} />
          </Field>
          <Topics draft={draft} problems={held} />
        </div>
      ) : null}

      <div class="controls">
        <button type="submit" disabled={saving.value}>
          {saving.value ? 'Saving…' : creating ? 'Create the rule' : 'Save'}
        </button>
        <button type="button" onClick={onCancel} disabled={saving.value}>
          Cancel
        </button>
      </div>
      {said.value ? (
        <p class={failed.value ? 'failure' : 'notice'} role="status">
          {said.value}
        </p>
      ) : null}
    </form>
  );
}

// ---------------------------------------------------------------
// Dead letters
// ---------------------------------------------------------------

/**
 * What one rule gave up on, most recent first.
 *
 * **A resource, not a subscription**, which is the admin crate's
 * decision and is worth repeating where it is read: a tap only shows
 * what is published while somebody is watching, and nobody watches a
 * rules page waiting for a dead letter. An operator is told by an
 * alert and opens the page afterwards, by which time a tap would have
 * nothing to show — which looks exactly like a rule that has never
 * dead-lettered anything.
 *
 * The collection is bounded and in memory, so it is what has happened
 * since the broker started and not a history. Everything is on
 * `$sd/rules/<id>/dlq` for whatever wants all of it for ever.
 */
function DeadLetters({ id }: { id: string }) {
  const letters = useSignal<DeadLetterView[]>([]);
  const total = useSignal(0);
  const failure = useSignal<string | null>(null);
  const loading = useSignal(true);

  useSignalEffect(() => {
    generation.value;
    let current = true;
    const load = async () => {
      loading.value = true;
      try {
        const answer: Page<DeadLetterView> = await fetchPage<DeadLetterView>(
          `rules/${encodeURIComponent(id)}/dead-letters`,
          { limit: LETTERS },
        );
        if (!current) {
          return;
        }
        letters.value = answer.items;
        total.value = answer.total;
        failure.value = null;
      } catch (error) {
        if (current) {
          failure.value = error instanceof Error ? error.message : String(error);
        }
      } finally {
        if (current) {
          loading.value = false;
        }
      }
    };
    void load();
    return () => {
      current = false;
    };
  });

  if (failure.value) {
    return (
      <p class="failure" role="alert">
        The dead letters could not be read: {failure.value}
      </p>
    );
  }
  if (loading.value && letters.value.length === 0) {
    return <p class="quiet">Reading…</p>;
  }
  if (letters.value.length === 0) {
    return (
      <p class="quiet">
        Nothing has been dead-lettered since this broker started. They are kept in memory and
        published to <span class="mono">$sd/rules/{id}/dlq</span>, which is where a durable
        subscriber reads them.
      </p>
    );
  }
  return (
    <>
      <p class="quiet">
        {letters.value.length.toLocaleString()} shown of {total.value.toLocaleString()} kept, most
        recent first. Every one was also published to{' '}
        <span class="mono">$sd/rules/{id}/dlq</span>.
      </p>
      <div class="rows">
        {letters.value.map((letter) => (
          <div class="letter" key={letter.sequence}>
            <span class="quiet">
              #{letter.sequence} · {new Date(letter.at * 1000).toLocaleString()}
            </span>
            <pre class="mono">
              {letter.document}
              {letter.truncated ? '\n… cut here; the whole of it is on the topic' : ''}
            </pre>
          </div>
        ))}
      </div>
    </>
  );
}

// ---------------------------------------------------------------
// The page
// ---------------------------------------------------------------

/** What a rule's trigger says in one cell. */
function triggerOf(rule: RuleView): string {
  if (!rule.filter) {
    return '—';
  }
  const narrowed = [
    rule.min_qos == null ? null : `QoS ≥ ${rule.min_qos}`,
    rule.client_id ? `from ${rule.client_id}` : null,
  ].filter(Boolean);
  return narrowed.length > 0 ? `${rule.filter} (${narrowed.join(', ')})` : rule.filter;
}

/** The page. */
export function Rules() {
  const rules = useSignal<RuleView[]>([]);
  const total = useSignal(0);
  const next = useSignal<string | null>(null);
  const failure = useSignal<string | null>(null);
  const loading = useSignal(false);
  const stats = useSignal<Record<string, RuleStatsView>>({});
  const interval = useSignal(1);
  const statsFailure = useSignal<string | null>(null);
  const series = useSignal<SeriesName>('fired');
  const editing = useSignal<RuleView | null>(null);
  const composing = useSignal(false);
  const opened = useSignal<string | null>(null);
  const said = useSignal<string | null>(null);
  const reload = useSignal(0);

  // The rules. Fetched on load, on a resynchronise, and after this
  // page changes one. Not on a timer: see the module note.
  useSignalEffect(() => {
    generation.value;
    reload.value;
    let current = true;
    const load = async () => {
      loading.value = true;
      try {
        const collected: RuleView[] = [];
        let cursor: string | null = null;
        let held = 0;
        for (let page = 0; page < PAGES; page += 1) {
          const answer: Page<RuleView> = await fetchPage<RuleView>('rules', {
            limit: PAGE,
            // Opaque, and passed back exactly as it was given.
            after: cursor,
          });
          if (!current) {
            return;
          }
          collected.push(...answer.items);
          held = answer.total;
          cursor = answer.next;
          if (!cursor) {
            break;
          }
        }
        rules.value = collected;
        total.value = held;
        // Non-null only when the bound above was reached, which is
        // what the footer says out loud.
        next.value = cursor;
        failure.value = null;
      } catch (error) {
        if (current) {
          failure.value = error instanceof Error ? error.message : String(error);
        }
      } finally {
        if (current) {
          loading.value = false;
        }
      }
    };
    void load();
    return () => {
      current = false;
    };
  });

  // The statistics, once a second, one request for the whole table.
  const timer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);
  useEffect(() => {
    let current = true;
    let inFlight = false;
    const read = async () => {
      // `inFlight` is the guard that is always on: a broker slower
      // than the interval would otherwise be sent a second request
      // before it answered the first. Whether the tab is in front is
      // the caller's question, not this function's — see the module
      // note on the first read.
      if (inFlight) {
        return;
      }
      inFlight = true;
      try {
        const answer = await fetchPage<RuleStatsView>('rule-stats', { limit: PAGE });
        if (!current) {
          return;
        }
        const held: Record<string, RuleStatsView> = {};
        for (const row of answer.items) {
          held[row.id] = row;
        }
        stats.value = held;
        if (answer.items.length > 0) {
          interval.value = answer.items[0].interval_seconds;
        }
        statsFailure.value = null;
      } catch (error) {
        if (current) {
          statsFailure.value = error instanceof Error ? error.message : String(error);
        }
      } finally {
        inFlight = false;
      }
    };
    // Unconditional: a hidden tab that has never read anything shows
    // `no samples`, which is a sentence about the broker.
    void read();
    // A hidden tab is a request a second for a drawing nobody is
    // looking at.
    timer.current = setInterval(() => {
      if (!document.hidden) {
        void read();
      }
    }, STATS_POLL);
    // Coming back is not worth up to a second of a stale drawing.
    const woken = () => {
      if (!document.hidden) {
        void read();
      }
    };
    document.addEventListener('visibilitychange', woken);
    return () => {
      current = false;
      clearInterval(timer.current);
      document.removeEventListener('visibilitychange', woken);
    };
  }, []);

  /** Do something to a rule, and say what happened. */
  const act = async (what: string, action: () => Promise<unknown>) => {
    said.value = null;
    try {
      await action();
      said.value = what;
      reload.value += 1;
    } catch (error) {
      said.value =
        error instanceof ApiError ? `The broker refused it (${error.code}): ${error.message}` : String(error);
    }
  };

  const held = rules.value;
  const numbers = stats.value;
  const drawn = series.value;
  const shownSeries = SERIES.find((one) => one.name === drawn) ?? SERIES[0];

  return (
    <section class="page">
      <h1>Rules</h1>

      {failure.value ? (
        <p class="failure" role="alert">
          The rules could not be read: {failure.value}
        </p>
      ) : null}
      {statsFailure.value ? (
        <p class="failure" role="alert">
          The statistics could not be read, so the sparklines are as old as this message says they
          are: {statsFailure.value}
        </p>
      ) : null}
      {said.value ? (
        <p class="notice" role="status">
          {said.value}
        </p>
      ) : null}

      <div class="controls">
        <button type="button" onClick={() => {
          composing.value = true;
          editing.value = null;
        }}>
          Write a rule
        </button>
        <label>
          Sparkline
          <select
            value={drawn}
            onChange={(event) => (series.value = (event.target as HTMLSelectElement).value as SeriesName)}
          >
            {SERIES.map((one) => (
              <option value={one.name} key={one.name}>
                {one.label}
              </option>
            ))}
          </select>
        </label>
        <span class="quiet">
          {held.length.toLocaleString()} of {total.value.toLocaleString()} held · the last minute,
          one point per {interval.value} second{interval.value === 1 ? '' : 's'}, each row scaled to
          its own peak
        </span>
      </div>

      {composing.value || editing.value ? (
        <Editor
          existing={editing.value}
          onDone={(rule) => {
            composing.value = false;
            editing.value = null;
            said.value = `${rule.id} was saved.`;
            reload.value += 1;
          }}
          onCancel={() => {
            composing.value = false;
            editing.value = null;
          }}
        />
      ) : null}

      <div class="table rules-table" role="table" aria-rowcount={held.length}>
        <div class="row head" role="row">
          <span role="columnheader">rule</span>
          <span role="columnheader">action</span>
          <span role="columnheader">trigger</span>
          <span role="columnheader">the last minute</span>
          <span role="columnheader">fired</span>
          <span role="columnheader">delivered</span>
          <span role="columnheader">failed</span>
          <span role="columnheader">dead</span>
          <span role="columnheader">waiting</span>
          <span role="columnheader">what to do</span>
        </div>
        {held.map((rule) => {
          const kind = actionOf(rule);
          const row = numbers[rule.id];
          const totals = row?.totals;
          return (
            <div class={`row${rule.enabled ? '' : ' partial'}`} role="row" key={rule.id}>
              <span role="cell" class="mono" title={rule.name}>
                {rule.id}
                {rule.enabled ? '' : ' (off)'}
              </span>
              <span role="cell">{kind ?? 'unknown'}</span>
              <span role="cell" class="mono">
                {triggerOf(rule)}
              </span>
              <span role="cell">
                {row ? (
                  <Sparkline values={row.series[drawn]} what={shownSeries.label} />
                ) : (
                  // Not `no samples`: that is a sentence about the
                  // rule, and this is a rule whose statistics have
                  // not been read yet — the same distinction the
                  // numbers beside it make with an ellipsis.
                  <span class="spark spark-empty" title="the statistics have not been read yet">
                    …
                  </span>
                )}
              </span>
              <span role="cell">{totals ? totals.fired.toLocaleString() : '…'}</span>
              <span role="cell">{totals ? totals.delivered.toLocaleString() : '…'}</span>
              <span role="cell">{totals ? totals.failed.toLocaleString() : '…'}</span>
              <span role="cell">{totals ? totals.dead_lettered.toLocaleString() : '…'}</span>
              <span role="cell">{totals ? totals.waiting.toLocaleString() : '…'}</span>
              <span role="cell" class="doing">
                <button
                  type="button"
                  onClick={() => {
                    composing.value = false;
                    editing.value = rule;
                  }}
                >
                  Edit
                </button>
                <button
                  type="button"
                  onClick={() =>
                    void act(
                      `${rule.id} is ${rule.enabled ? 'off' : 'on'}.`,
                      () => setRuleEnabled(rule.id, !rule.enabled),
                    )
                  }
                >
                  {rule.enabled ? 'Disable' : 'Enable'}
                </button>
                <button
                  type="button"
                  onClick={() => (opened.value = opened.value === rule.id ? null : rule.id)}
                >
                  {opened.value === rule.id ? 'Hide letters' : 'Dead letters'}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    if (
                      window.confirm(
                        `Delete ${rule.id}? Its outbox goes with it, so this is refused while anything is waiting in one.`,
                      )
                    ) {
                      void act(`${rule.id} is gone.`, () => deleteRule(rule.id));
                    }
                  }}
                >
                  Delete
                </button>
              </span>
            </div>
          );
        })}
      </div>

      {opened.value ? (
        <section class="letters">
          <h2>What {opened.value} gave up on</h2>
          <DeadLetters id={opened.value} />
        </section>
      ) : null}

      <p class="quiet footer">
        {held.length === 0 && !loading.value
          ? 'This node holds no rules. A rule written here is durable: it is stored, and it is installed again when the broker restarts.'
          : null}{' '}
        {next.value
          ? `This node holds ${total.value.toLocaleString()} rules and this table stopped at ${held.length.toLocaleString()}: it reads ${PAGES} pages of ${PAGE} and no more. The rest are running; they are not drawn here.`
          : null}
      </p>
      <p class="hint">
        A rule this node stores runs whether or not the configuration file names it, and deleting a
        table from the file no longer stops one: delete the rule here. An ingest rule is served on
        the listener this node bound at startup, so one created here is reachable only if this node
        was started with an ingest rule already.
      </p>
    </section>
  );
}
