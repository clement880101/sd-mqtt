/**
 * Topics: what the broker is holding, a way to publish, and a way to
 * watch.
 *
 * # The subscribe half, and the mechanism it does not have
 *
 * **The events stream does not carry publications.** Section 11.1:
 * it carries counter deltas, connect and disconnect events, and
 * later rule outcomes. There is no resource and no stream on the
 * admin API through which a browser can see a message pass. So the
 * "subscribe" half of this tool had to choose a mechanism, and the
 * choice is recorded here and in `services/web/README.md` rather
 * than left to be inferred from the code.
 *
 * **What is built: a watcher over the retained store.** The watcher
 * takes an MQTT topic filter, pages the retained resource, matches
 * each topic against the filter with the protocol's own rules, and
 * re-reads on an interval, marking what changed since the last pass.
 *
 * **What that costs, said plainly rather than in a footnote.** It
 * sees retained messages only. A publication that is not retained
 * never appears, so a filter over a topic that is published to
 * constantly but never retained shows nothing at all — and the page
 * says so on screen, because a watcher that draws nothing is
 * otherwise indistinguishable from a broker with no traffic.
 *
 * **Its limit, which is the thing the plan asks for.** A filter of
 * `#` on a busy broker is the obvious abuse of anything that
 * forwards matching messages. Here the abuse is bounded by
 * construction rather than by a rule that has to be enforced: one
 * pass reads at most {@link WATCH_PAGES} pages of
 * {@link WATCH_PAGE_SIZE} topics, so `#` costs exactly what
 * `sensors/kitchen/temperature` costs, and a store larger than that
 * is reported as partially watched rather than silently truncated.
 * The cost is a request per interval per browser, which is bounded
 * and visible, instead of an open-ended fan-out the broker would
 * have to police.
 *
 * **What was not built, and why.** The mechanism that would show
 * live traffic is a *tap*: a real subscriber inside the broker whose
 * deliveries a browser can read. It is buildable on what already
 * exists — `Broker::connect`, `Broker::handle` with a subscribe, and
 * `Shared::register_outbox` — and it would need its own bounded ring
 * per tap, a cap on how many exist, a payload cap and an expiry for
 * one nobody is polling, or a forgotten tap is a session that never
 * leaves. That is a change to the broker and to the admin API, with
 * a new abuse surface and a session-creating code path, and it does
 * not belong inside a commit that writes three pages. Recorded so
 * that it stays a decision rather than becoming an omission.
 */
import { useSignal, useSignalEffect } from '@preact/signals';
import { useEffect, useRef } from 'preact/hooks';
import * as v from 'valibot';

import { ApiError, fetchPage, publish, type Page, type RetainedView } from '../api.js';
import { gauges, generation } from '../live.js';

/** How many retained rows the browser asks for at a time. */
const PAGE = 200;

/** How many pages one pass of the watcher reads. See the module docs. */
const WATCH_PAGES = 5;

/** How many rows each of those pages holds. */
const WATCH_PAGE_SIZE = 200;

/** How often the watcher reads, in milliseconds. */
const WATCH_INTERVAL = 2_000;

/** How long a row stays marked as having just changed. */
const FLASH = 4_000;

// ---------------------------------------------------------------
// Topic filters, by the protocol's rules
// ---------------------------------------------------------------

/**
 * Whether `topic` matches `filter`, as MQTT 4.7 defines it.
 *
 * `+` matches exactly one level and `#` matches the rest, including
 * the level it sits in, so `sport/#` matches `sport`. A filter
 * beginning with a wildcard does not match a topic beginning with
 * `$`, which is the rule that keeps a subscribe-to-everything client
 * away from the broker's own status topics.
 */
export function matches(filter: string, topic: string): boolean {
  const wanted = filter.split('/');
  const actual = topic.split('/');
  if (topic.startsWith('$') && (wanted[0] === '#' || wanted[0] === '+')) {
    return false;
  }
  for (let index = 0; index < wanted.length; index += 1) {
    const level = wanted[index];
    if (level === '#') {
      // [MQTT-4.7.1-2]: the parent level matches too, so `sport/#`
      // matches `sport`, which is why this is not `index < actual.length`.
      return index === wanted.length - 1 && actual.length >= index;
    }
    if (index >= actual.length) {
      return false;
    }
    if (level !== '+' && level !== actual[index]) {
      return false;
    }
  }
  return wanted.length === actual.length;
}

// ---------------------------------------------------------------
// The publish form
// ---------------------------------------------------------------

/**
 * What the publish endpoint will accept, checked before it is sent.
 *
 * Validated here so that the three mistakes an operator actually
 * makes are named beside the field rather than returned as a round
 * trip. The endpoint checks all of this again and is the authority:
 * this is a courtesy, not a gate, and the one rule it must never
 * break is accepting something the endpoint would refuse.
 */
const Publishable = v.object({
  topic: v.pipe(
    v.string(),
    v.minLength(1, 'a topic name is required'),
    v.check((topic) => !topic.includes('+') && !topic.includes('#'), 'a topic name published to cannot contain + or #, which are filter wildcards'),
    v.check((topic) => !topic.includes('\u0000'), 'a topic name cannot contain a null character'),
  ),
  payload: v.string(),
  qos: v.picklist([0, 1, 2], 'the quality of service is 0, 1 or 2'),
  retain: v.boolean(),
  expiry: v.union([
    v.literal(''),
    v.pipe(
      v.string(),
      v.regex(/^\d+$/u, 'an expiry is a whole number of seconds'),
      v.check((text) => Number(text) <= 4_294_967_295, 'an expiry is at most 4294967295 seconds'),
    ),
  ]),
});

/** What a failed validation says, by field. */
type Problems = Partial<Record<'topic' | 'payload' | 'qos' | 'retain' | 'expiry', string>>;

/** The publish tool. */
function Publisher({ onPublished }: { onPublished: () => void }) {
  const topic = useSignal('');
  const payload = useSignal('');
  const qos = useSignal(0);
  const retain = useSignal(false);
  const expiry = useSignal('');
  const problems = useSignal<Problems>({});
  const said = useSignal<string | null>(null);
  const failed = useSignal(false);
  const sending = useSignal(false);

  const send = async (event: Event) => {
    event.preventDefault();
    said.value = null;
    const checked = v.safeParse(Publishable, {
      topic: topic.value,
      payload: payload.value,
      qos: qos.value,
      retain: retain.value,
      expiry: expiry.value,
    });
    if (!checked.success) {
      const found: Problems = {};
      for (const issue of checked.issues) {
        const key = issue.path?.[0]?.key as keyof Problems | undefined;
        if (key && !found[key]) {
          found[key] = issue.message;
        }
      }
      problems.value = found;
      failed.value = true;
      said.value = 'The form was not sent: see the fields marked below.';
      return;
    }
    problems.value = {};
    sending.value = true;
    try {
      const accepted = await publish({
        topic: checked.output.topic,
        payload: checked.output.payload,
        qos: checked.output.qos,
        retain: checked.output.retain,
        ...(checked.output.expiry === ''
          ? {}
          : { message_expiry_interval: Number(checked.output.expiry) }),
      });
      failed.value = false;
      said.value = `Published. ${accepted.delivered} subscriber${
        accepted.delivered === 1 ? '' : 's'
      } received it${accepted.durable ? ', and the broker waited for the store' : ''}.`;
      onPublished();
    } catch (error) {
      failed.value = true;
      if (error instanceof ApiError) {
        said.value = `The broker refused it (${error.code}): ${error.message}`;
        // The API names the parameter at fault when there is one, so
        // the form can mark it without reading the sentence.
        if (error.field) {
          problems.value = { [error.field as keyof Problems]: error.message };
        }
      } else {
        said.value = String(error);
      }
    } finally {
      sending.value = false;
    }
  };

  const problem = problems.value;
  return (
    <form class="publisher" onSubmit={(event) => void send(event)}>
      <h2>Publish</h2>
      <div class="fields">
        <label>
          Topic
          <input
            type="text"
            value={topic.value}
            aria-invalid={problem.topic ? 'true' : undefined}
            onInput={(event) => (topic.value = (event.target as HTMLInputElement).value)}
          />
          {problem.topic ? <span class="problem">{problem.topic}</span> : null}
        </label>
        <label>
          Payload
          <textarea
            rows={2}
            value={payload.value}
            onInput={(event) => (payload.value = (event.target as HTMLTextAreaElement).value)}
          />
          <span class="hint">Text. A binary payload is what an MQTT client is for.</span>
        </label>
        <label>
          QoS
          <select
            value={String(qos.value)}
            onChange={(event) => (qos.value = Number((event.target as HTMLSelectElement).value))}
          >
            <option value="0">0 — at most once</option>
            <option value="1">1 — at least once</option>
            <option value="2">2 — exactly once</option>
          </select>
        </label>
        <label class="inline">
          <input
            type="checkbox"
            checked={retain.value}
            onChange={(event) => (retain.value = (event.target as HTMLInputElement).checked)}
          />
          Retain
        </label>
        <label>
          Expiry (s)
          <input
            type="text"
            inputMode="numeric"
            value={expiry.value}
            aria-invalid={problem.expiry ? 'true' : undefined}
            placeholder="none"
            onInput={(event) => (expiry.value = (event.target as HTMLInputElement).value)}
          />
          {problem.expiry ? <span class="problem">{problem.expiry}</span> : null}
        </label>
        <button type="submit" disabled={sending.value}>
          {sending.value ? 'Publishing…' : 'Publish'}
        </button>
      </div>
      {said.value ? (
        <p class={failed.value ? 'failure' : 'notice'} role="status">
          {said.value}
        </p>
      ) : null}
      <p class="hint">
        Retaining an empty payload is how a topic is cleared, which is the protocol's rule and not
        this form's.
      </p>
    </form>
  );
}

// ---------------------------------------------------------------
// The retained browser
// ---------------------------------------------------------------

/** What one retained row shows for its payload. */
function payloadOf(row: RetainedView): string {
  if (row.text !== null) {
    return row.text;
  }
  return row.text_omitted === 'binary'
    ? `(${row.size} bytes, not text)`
    : `(${row.size} bytes, too large to inline)`;
}

/** The retained browser, with a way to clear a topic. */
function Retained({ reloadKey, onCleared }: { reloadKey: number; onCleared: () => void }) {
  const rows = useSignal<RetainedView[]>([]);
  const total = useSignal(0);
  const next = useSignal<string | null>(null);
  const failure = useSignal<string | null>(null);
  const busy = useSignal(false);
  const needle = useSignal('');

  useSignalEffect(() => {
    // A resynchronise, a publish from this page, and the retained
    // gauge moving — which is how a message retained by somebody
    // else reaches this list.
    generation.value;
    gauges.value?.retained;
    void reloadKey;
    let current = true;
    const load = async () => {
      try {
        const answer: Page<RetainedView> = await fetchPage<RetainedView>('retained', {
          limit: PAGE,
        });
        if (!current) {
          return;
        }
        rows.value = answer.items;
        total.value = answer.total;
        next.value = answer.next;
        failure.value = null;
      } catch (error) {
        if (current) {
          failure.value = error instanceof Error ? error.message : String(error);
        }
      }
    };
    void load();
    return () => {
      current = false;
    };
  });

  const more = async () => {
    if (!next.value || busy.value) {
      return;
    }
    busy.value = true;
    try {
      const answer = await fetchPage<RetainedView>('retained', {
        limit: PAGE,
        after: next.value,
      });
      rows.value = [...rows.value, ...answer.items];
      total.value = answer.total;
      next.value = answer.next;
    } catch (error) {
      failure.value = error instanceof Error ? error.message : String(error);
    } finally {
      busy.value = false;
    }
  };

  /** Clear a topic the way the protocol clears one. */
  const clear = async (topic: string) => {
    if (!window.confirm(`Clear the retained message on ${topic}?`)) {
      return;
    }
    try {
      await publish({ topic, payload: '', qos: 0, retain: true });
      rows.value = rows.value.filter((row) => row.topic !== topic);
      total.value = Math.max(0, total.value - 1);
      onCleared();
    } catch (error) {
      failure.value = error instanceof Error ? error.message : String(error);
    }
  };

  const text = needle.value.trim().toLowerCase();
  const shown = text ? rows.value.filter((row) => row.topic.toLowerCase().includes(text)) : rows.value;

  return (
    <section class="retained">
      <h2>Retained messages</h2>
      {failure.value ? (
        <p class="failure" role="alert">
          {failure.value}
        </p>
      ) : null}
      <div class="controls">
        <label>
          Contains
          <input
            type="search"
            value={needle.value}
            placeholder="part of a topic name"
            onInput={(event) => (needle.value = (event.target as HTMLInputElement).value)}
          />
        </label>
        <span class="quiet">
          {shown.length.toLocaleString()} shown · {rows.value.length.toLocaleString()} fetched ·{' '}
          {total.value.toLocaleString()} held by the broker
        </span>
      </div>
      <div class="table">
        <div class="row head retained-row" role="row">
          <span>topic</span>
          <span>payload</span>
          <span>qos</span>
          <span>bytes</span>
          <span />
        </div>
        <div class="rows">
          {shown.map((row) => (
            <div class="row retained-row" role="row" key={row.topic}>
              <span class="mono" title={row.topic}>
                {row.topic}
              </span>
              <span class="mono" title={payloadOf(row)}>
                {payloadOf(row)}
              </span>
              <span>{row.qos}</span>
              <span>{row.size}</span>
              <span>
                <button type="button" onClick={() => void clear(row.topic)}>
                  Clear
                </button>
              </span>
            </div>
          ))}
          {shown.length === 0 ? <p class="empty">Nothing retained matches.</p> : null}
        </div>
      </div>
      {next.value ? (
        <p class="quiet footer">
          {rows.value.length.toLocaleString()} of {total.value.toLocaleString()} fetched.{' '}
          <button type="button" onClick={() => void more()} disabled={busy.value}>
            Load the next page
          </button>
        </p>
      ) : null}
    </section>
  );
}

// ---------------------------------------------------------------
// The watcher
// ---------------------------------------------------------------

/** What the watcher holds about one topic. */
interface Watched {
  topic: string;
  payload: string;
  /** When this tool last saw the payload change, in milliseconds. */
  changed: number;
  /** How many changes it has seen since the watch started. */
  changes: number;
}

/** The subscribe half. See the module documentation for what it is. */
function Watcher() {
  const filter = useSignal('#');
  const running = useSignal(false);
  const seen = useSignal<Watched[]>([]);
  const truncated = useSignal(false);
  const failure = useSignal<string | null>(null);
  const passes = useSignal(0);
  const held = useRef(new Map<string, Watched>());
  const timer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  const pass = async (wanted: string) => {
    try {
      let cursor: string | null = null;
      const found = new Map<string, string>();
      let more = false;
      for (let page = 0; page < WATCH_PAGES; page += 1) {
        const answer: Page<RetainedView> = await fetchPage<RetainedView>('retained', {
          limit: WATCH_PAGE_SIZE,
          after: cursor,
        });
        for (const row of answer.items) {
          if (matches(wanted, row.topic)) {
            found.set(row.topic, payloadOf(row));
          }
        }
        cursor = answer.next;
        if (!cursor) {
          break;
        }
        more = page === WATCH_PAGES - 1;
      }
      truncated.value = more;
      const now = Date.now();
      const next = new Map<string, Watched>();
      for (const [topic, payload] of found) {
        const before = held.current.get(topic);
        next.set(
          topic,
          before && before.payload === payload
            ? before
            : {
                topic,
                payload,
                changed: now,
                changes: before ? before.changes + 1 : 1,
              },
        );
      }
      held.current = next;
      seen.value = [...next.values()].sort((left, right) => right.changed - left.changed);
      passes.value += 1;
      failure.value = null;
    } catch (error) {
      failure.value = error instanceof Error ? error.message : String(error);
    }
  };

  const start = () => {
    held.current = new Map();
    seen.value = [];
    passes.value = 0;
    running.value = true;
    void pass(filter.value);
    clearInterval(timer.current);
    timer.current = setInterval(() => void pass(filter.value), WATCH_INTERVAL);
  };

  const stop = () => {
    running.value = false;
    clearInterval(timer.current);
    timer.current = undefined;
  };

  useEffect(() => () => clearInterval(timer.current), []);

  const now = Date.now();
  return (
    <section class="watcher">
      <h2>Watch a filter</h2>
      <p class="hint">
        This watches the <strong>retained</strong> store, because the events stream carries counter
        deltas and connection events and no publications. A message that is published without the
        retain flag never appears here, however busy its topic is. One pass reads at most{' '}
        {(WATCH_PAGES * WATCH_PAGE_SIZE).toLocaleString()} topics, so a filter of <code>#</code>{' '}
        costs what any other filter costs.
      </p>
      <div class="controls">
        <label>
          Filter
          <input
            type="text"
            class="mono"
            value={filter.value}
            onInput={(event) => (filter.value = (event.target as HTMLInputElement).value)}
          />
        </label>
        <button type="button" onClick={() => (running.value ? stop() : start())}>
          {running.value ? 'Stop' : 'Watch'}
        </button>
        {running.value ? (
          <span class="quiet">
            {seen.value.length.toLocaleString()} matching · {passes.value} passes
          </span>
        ) : null}
      </div>
      {failure.value ? (
        <p class="failure" role="alert">
          {failure.value}
        </p>
      ) : null}
      {truncated.value ? (
        <p class="notice" role="status">
          The broker holds more retained topics than one pass reads, so this is watching the first{' '}
          {(WATCH_PAGES * WATCH_PAGE_SIZE).toLocaleString()} in topic order and not the whole store.
        </p>
      ) : null}
      {running.value ? (
        <div class="table">
          <div class="row head watch-row" role="row">
            <span>topic</span>
            <span>payload</span>
            <span>changes</span>
            <span>last change</span>
          </div>
          <div class="rows">
            {seen.value.map((row) => (
              <div
                class={`row watch-row${now - row.changed < FLASH ? ' fresh' : ''}`}
                role="row"
                key={row.topic}
              >
                <span class="mono">{row.topic}</span>
                <span class="mono">{row.payload}</span>
                <span>{row.changes}</span>
                <span>{new Date(row.changed).toLocaleTimeString()}</span>
              </div>
            ))}
            {seen.value.length === 0 ? (
              <p class="empty">
                Nothing retained matches this filter yet. If the traffic you are looking for is not
                retained, this tool cannot see it.
              </p>
            ) : null}
          </div>
        </div>
      ) : null}
    </section>
  );
}

/** The page. */
export function Topics() {
  const reloadKey = useSignal(0);
  return (
    <section class="page">
      <h1>Topics</h1>
      <Publisher onPublished={() => (reloadKey.value += 1)} />
      <Retained reloadKey={reloadKey.value} onCleared={() => (reloadKey.value += 1)} />
      <Watcher />
    </section>
  );
}
