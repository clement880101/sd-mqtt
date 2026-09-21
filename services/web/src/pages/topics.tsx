/**
 * Topics: what the broker is holding, a way to publish, and a way to
 * watch.
 *
 * # The subscribe half is a tap
 *
 * **The events stream carries no publications.** Section 11.1: it
 * carries counter deltas, connect and disconnect events, and later
 * rule outcomes. So the subscribe half cannot be built out of the
 * stream, and milestone three built it out of the retained resource
 * instead — paging the store and matching a filter here in the
 * browser. That saw retained messages only and sampled rather than
 * streamed, which the page said on screen and the readme said at
 * length.
 *
 * **What it is now.** The watcher asks the broker to open a *tap*: a
 * real subscriber inside the broker, made by the same
 * `Broker::connect`, `Broker::handle` and `Shared::register_outbox`
 * a TCP connection uses, collecting matching publications into a
 * bounded ring this page polls. See
 * `services/broker/crates/admin/src/taps.rs`, which is where the
 * mechanism and its four limits are written down.
 *
 * Two things follow that are worth naming here, because they are
 * what the page is shaped by.
 *
 * **There is no matcher in this file.** There was one, and it is
 * gone. The filter is handed to the broker and the broker's
 * subscription machinery decides what matches, which is how a
 * shared subscription, the rule that a leading wildcard does not
 * match a `$` topic, and no-local come for free rather than being
 * reimplemented here in TypeScript and drifting the first time
 * either copy is corrected.
 *
 * **Polling, not a second stream.** Section 11.1 allows the
 * interface exactly one events stream per document, and it is
 * already spent. A tap is read once a second with a cursor, which is
 * one request per second per open watcher and is bounded and
 * visible, and the messages arrive in the order the broker caught
 * them however many of them a poll returns.
 *
 * **The limits are the broker's and are shown on screen.** Nothing
 * here hard-codes them: every answer carries them and the page
 * renders what it was told. A watcher drawing nothing has to be
 * distinguishable from a watcher whose ring is being overrun, so the
 * counts — caught, dropped by the ring, lost by the broker, missed
 * by this reader — are on the page beside the rows rather than in a
 * log nobody reads.
 */
import { useSignal, useSignalEffect } from '@preact/signals';
import { useEffect, useRef } from 'preact/hooks';
import * as v from 'valibot';

import {
  ApiError,
  closeTap,
  fetchPage,
  openTap,
  publish,
  readTap,
  type Page,
  type RetainedView,
  type TapLimits,
  type TappedMessage,
  type TapView,
} from '../api.js';
import { gauges, generation } from '../live.js';

/** How many retained rows the browser asks for at a time. */
const PAGE = 200;

/**
 * How often the watcher reads its tap, in milliseconds.
 *
 * A second, the same cadence the events stream is sampled at, so
 * that the two halves of a live page move together. Faster would
 * cost the broker requests for messages that are not there yet;
 * slower would let a busy filter overrun the ring between polls,
 * which is survivable — the tap counts what it dropped and this page
 * says so — but is not worth choosing.
 */
const TAP_POLL = 1_000;

/** How long a row stays marked as having just changed. */
const FLASH = 4_000;

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
// The watcher, which is now a tap
// ---------------------------------------------------------------

/** What one tapped row shows for its payload. */
function tappedPayload(message: TappedMessage): string {
  if (message.text !== null) {
    return message.text;
  }
  return message.text_omitted === 'binary'
    ? `(${message.size} bytes, not text)`
    : `(${message.size} bytes, over the tap's payload cap)`;
}

/** A tap as the page holds it: what it is, and what it is bounded by. */
interface Held {
  view: TapView;
  limits: TapLimits;
}

/** The subscribe half. See the module documentation for what it is. */
function Watcher() {
  const filter = useSignal('#');
  const held = useSignal<Held | null>(null);
  const running = useSignal(false);
  const opening = useSignal(false);
  const rows = useSignal<TappedMessage[]>([]);
  const missed = useSignal(0);
  const polls = useSignal(0);
  const failure = useSignal<string | null>(null);
  const note = useSignal<string | null>(null);
  /** The cursor, which is opaque in the same sense a page's is. */
  const cursor = useRef<number | null>(null);
  const timer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);
  /** What to close on the way out, read from an unmount handler. */
  const openTapId = useRef<number | null>(null);

  const stopPolling = () => {
    clearInterval(timer.current);
    timer.current = undefined;
    running.value = false;
  };

  /** Give the tap back to the broker. */
  const release = () => {
    stopPolling();
    if (openTapId.current !== null) {
      closeTap(openTapId.current);
      openTapId.current = null;
    }
  };

  const poll = async () => {
    const current = held.value;
    if (!current) {
      return;
    }
    try {
      const read = await readTap(current.view.id, cursor.current);
      cursor.current = read.next;
      if (read.messages.length > 0) {
        // Newest first, because that is where a person looks, and
        // trimmed to the broker's own capacity so the document never
        // holds more rows than the ring behind it does. The retained
        // browser is not windowed and says so; this one is bounded
        // instead, which is the cheaper half of the same problem.
        rows.value = [...read.messages].reverse().concat(rows.value).slice(0, read.limits.capacity);
      }
      missed.value += read.missed;
      polls.value += 1;
      held.value = { view: read, limits: read.limits };
      failure.value = null;
      if (read.closed !== null) {
        // The broker ended it: something took its session over.
        // Nothing to give back, so the identifier is dropped rather
        // than deleted.
        openTapId.current = null;
        stopPolling();
        note.value = `${read.closed} Nothing more will arrive. Start another watch.`;
      }
    } catch (error) {
      if (error instanceof ApiError && error.code === 'not_found') {
        openTapId.current = null;
        stopPolling();
        note.value = error.message;
        return;
      }
      failure.value = error instanceof Error ? error.message : String(error);
    }
  };

  const start = async () => {
    release();
    rows.value = [];
    missed.value = 0;
    polls.value = 0;
    cursor.current = null;
    failure.value = null;
    note.value = null;
    held.value = null;
    opening.value = true;
    try {
      const opened = await openTap(filter.value);
      const { limits, ...view } = opened;
      held.value = { view, limits };
      openTapId.current = view.id;
      running.value = true;
      void poll();
      timer.current = setInterval(() => void poll(), TAP_POLL);
    } catch (error) {
      if (error instanceof ApiError) {
        failure.value = `The broker would not open a tap (${error.code}): ${error.message}`;
      } else {
        failure.value = String(error);
      }
    } finally {
      opening.value = false;
    }
  };

  // Two ways out, because they catch different things. Unmount
  // covers navigating to another page of this application, which is
  // the common one; `pagehide` covers the tab being closed or
  // navigated away from, which unmount does not reliably see. Both
  // are courtesies: a tap nobody gives back expires on its own after
  // the idle timeout, which is what that limit is for.
  useEffect(() => {
    const leaving = () => release();
    window.addEventListener('pagehide', leaving);
    return () => {
      window.removeEventListener('pagehide', leaving);
      release();
    };
  }, []);

  const current = held.value;
  const topics = new Set(rows.value.map((row) => row.topic)).size;
  const now = Date.now();
  return (
    <section class="watcher">
      <h2>Watch a filter</h2>
      <p class="hint">
        This opens a <strong>tap</strong>: a real subscriber inside the broker, subscribed with the
        filter below through the same path an MQTT connection uses. It sees live traffic, retained
        or not.{' '}
        {current ? (
          <>
            The broker allows {current.limits.max_taps} taps at once; this one holds the last{' '}
            {current.limits.capacity.toLocaleString()} messages and drops its oldest to make room,
            carries at most {current.limits.max_payload_bytes.toLocaleString()} bytes of a payload
            and describes anything larger, and closes itself if nothing reads it for{' '}
            {current.limits.idle_timeout_seconds} seconds. It is the session{' '}
            <code>{current.view.client_id}</code> on the Clients page, which is what a tap looks
            like from the inside.
          </>
        ) : (
          <>
            It is capped, bounded and expiring: the broker sends its own limits with every answer
            and they are shown here once a tap is open. It does not replay the retained store —
            that is the list above.
          </>
        )}
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
        <button
          type="button"
          disabled={opening.value}
          onClick={() => (running.value ? release() : void start())}
        >
          {opening.value ? 'Opening…' : running.value ? 'Stop' : 'Watch'}
        </button>
        {current ? (
          <span class="quiet">
            {rows.value.length.toLocaleString()} shown across {topics.toLocaleString()} topic
            {topics === 1 ? '' : 's'} · {current.view.delivered.toLocaleString()} caught ·{' '}
            {current.view.dropped.toLocaleString()} dropped by the ring ·{' '}
            {current.view.lost.toLocaleString()} lost by the broker · {polls.value} polls
          </span>
        ) : null}
      </div>
      {failure.value ? (
        <p class="failure" role="alert">
          {failure.value}
        </p>
      ) : null}
      {note.value ? (
        <p class="notice" role="status">
          {note.value}
        </p>
      ) : null}
      {missed.value > 0 ? (
        <p class="notice" role="status">
          {missed.value.toLocaleString()} message{missed.value === 1 ? '' : 's'} arrived and left
          the tap's ring before this page read them, so they are not below. The filter is producing
          more than {current ? current.limits.capacity.toLocaleString() : 'the ring'} messages a
          second; narrow it to see everything.
        </p>
      ) : null}
      {current ? (
        <div class="table">
          <div class="row head tap-row" role="row">
            <span>time</span>
            <span>topic</span>
            <span>payload</span>
            <span>qos</span>
            <span>retain</span>
          </div>
          <div class="rows">
            {rows.value.map((row) => (
              <div
                class={`row tap-row${now - row.at < FLASH ? ' fresh' : ''}`}
                role="row"
                key={row.seq}
              >
                <span class="quiet">{new Date(row.at).toLocaleTimeString()}</span>
                <span class="mono" title={row.topic}>
                  {row.topic}
                </span>
                <span class="mono" title={tappedPayload(row)}>
                  {tappedPayload(row)}
                </span>
                <span>{row.qos}</span>
                <span>{row.retain ? 'yes' : 'no'}</span>
              </div>
            ))}
            {rows.value.length === 0 ? (
              <p class="empty">
                {running.value
                  ? 'Nothing has been published to this filter since the watch started. A tap shows live traffic only, so a topic that was retained an hour ago and has not been published to since is in the list above, not here.'
                  : 'The watch has stopped.'}
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
