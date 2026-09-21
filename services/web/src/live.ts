/**
 * The one live connection, and what it does when it breaks.
 *
 * **One stream for the whole application.** Section 11.1: the admin
 * listener speaks HTTP/1.1, a browser allows six connections to an
 * origin, and an events stream holds one for the life of the tab.
 * Three pages each opening their own would spend half the budget on
 * the same data. So this module owns the connection, and pages read
 * the signals below.
 *
 * # The four things it has to get right
 *
 * **The connection dropping and coming back.** Handled by using
 * `EventSource` rather than a hand-rolled reader over `fetch`. The
 * browser reconnects on its own and resends the last identifier it
 * saw in the `Last-Event-ID` header, which is what lets the server
 * replay from its ring. A reader built on `fetch` would have to
 * reimplement both, and the header cannot be set on a fresh
 * `EventSource` at all — see {@link REOPEN_AFTER_GIVING_UP}.
 *
 * **A resynchronise event means refetch.** The server sends it when a
 * reader has fallen out of the ring, or when the identifier it
 * resumed with belongs to a previous run of the process. It is the
 * server saying *what you hold may be stale*, and an interface that
 * carries on looks perfectly healthy while showing the wrong broker.
 * So it bumps {@link generation}, and every page refetches what it
 * holds when that number changes.
 *
 * **The stream being unavailable at all.** {@link status} says which
 * of the three states this is in, and the shell shows it. An empty
 * page and a page whose data stopped arriving look identical
 * otherwise.
 *
 * **The retry interval is the server's decision.** It sends
 * `retry: 15000` as the first frame of every stream, because a
 * restarted broker does not want every open tab retrying every three
 * seconds while it reads its store. `EventSource` honours that field;
 * nothing here overrides it, and nothing here reimplements the
 * backoff it governs.
 *
 * # What the rate charts are computed from, and the restart
 *
 * A rate is a derivative and the stream carries totals. The server
 * also sends its own `deltas`, and they are **not** what this uses:
 * a delta is only the whole story if every sample arrived, so after
 * a gap in the stream — a reconnect, a resynchronise, a sleeping tab
 * — the deltas add up to less than what happened and the chart
 * quietly under-reports.
 *
 * So the rate is `(totals now − totals then) / elapsed`, and the
 * interesting part is which elapsed.
 *
 * - The browser's own clock — `performance.now()`, which is
 *   monotonic where the wall clock is not — is right when events
 *   were **lost**: the difference of two totals across a gap over
 *   the real time the gap took is the true average over it, rather
 *   than a hole.
 * - It is wrong when events were **replayed**. A reconnect is
 *   answered from the server's ring, so several seconds of events
 *   arrive in one burst; the browser's clock says they were
 *   microseconds apart, and dividing a second's worth of bytes by a
 *   microsecond draws a spike taller than anything real.
 * - The server's own `over_ms` is exactly the other way round. It
 *   always covers the interval its `deltas` were measured over, so
 *   it is right for a replayed burst — and too short across a gap,
 *   because it measures the server's last interval and not the
 *   browser's absence.
 *
 * So the elapsed time is **the greater of the two**. Each is right
 * where the other is wrong, and the larger is the conservative
 * reading in the case neither covers: a rate that is too low is a
 * chart that under-draws, and a rate that is too high is a spike an
 * operator goes looking for.
 *
 * **When a total goes backwards, the broker restarted.** The
 * counters are process totals: they only rise within a run and start
 * again at zero in the next one. The obvious result of not deciding
 * is a large negative spike on every restart, and a negative rate is
 * not a thing that can happen. The decision here is:
 *
 * - the sample is **not** plotted as a number; a gap is plotted
 *   instead, so the chart's line breaks visibly where the run ended
 *   rather than joining two runs into one continuous history;
 * - the baseline is reset to the new run's totals, so the next
 *   sample is a rate of the new run and not a rate across the join;
 * - {@link restarts} counts it, and the Overview page says so, because
 *   a break in a line that nothing explains is a bug report.
 *
 * History from before the restart is kept. It was true when it was
 * drawn and deleting it would lose the very thing an operator is
 * looking at when a broker restarts.
 */
import { signal } from '@preact/signals';

/** Where the stream is. */
export const EVENTS = '/api/v1/events';

/**
 * How long to wait before opening a **new** `EventSource` after the
 * browser has given up on the old one.
 *
 * Not the reconnect interval: that is the server's `retry` field and
 * the browser owns it. This covers the one case the browser will not
 * retry, which is a response it considers fatal — in development,
 * Vite answers a proxied request to a stopped broker with a `500`,
 * and a `500` closes an `EventSource` permanently.
 *
 * **A stream reopened this way has no `Last-Event-ID`.** Script
 * cannot set a header on an `EventSource`, so the server sees a
 * browser that has seen nothing and sends no backlog and no
 * resynchronise. Whatever happened while this tab was away is
 * therefore invisible to the server's own honesty mechanism, and
 * this module makes up for it by refetching itself: see
 * {@link generation}.
 */
const REOPEN_AFTER_GIVING_UP = 15_000;

/** How many rate samples the charts keep. Five minutes at one a second. */
const HISTORY = 300;

/** Where the live connection is. */
export type Status =
  /** Opening, or reopening after a drop. Nothing is arriving yet. */
  | 'connecting'
  /** Open, and events are arriving. */
  | 'live'
  /** It broke. The browser is retrying on the server's interval. */
  | 'lost';

/** The levels the Overview page shows beside the rates. */
export interface Gauges {
  connections: number;
  sessions: number;
  subscriptions: number;
  retained: number;
  queue_depth: number;
}

/** The counters a rate is derived from. */
export interface CounterTotals {
  bytes_received: number;
  bytes_sent: number;
  sockets_opened: number;
}

/** A session gaining or losing its connection. */
export interface Presence {
  kind: 'connected' | 'disconnected';
  mountpoint: string;
  client_id: string;
}

/**
 * What the charts draw, in uPlot's own layout.
 *
 * Parallel arrays rather than an array of samples, because that is
 * what uPlot takes and converting on every event would allocate the
 * whole history once a second. A `null` is a gap: uPlot breaks the
 * line at one, which is what a restart draws.
 */
export interface Rates {
  /** Unix seconds, for the axis. */
  at: number[];
  /** Bytes a second read from client sockets. */
  received: (number | null)[];
  /** Bytes a second written to them. */
  sent: (number | null)[];
  /** Sockets a second opened. */
  opened: (number | null)[];
}

/** Where the live connection is, for the shell to show. */
export const status = signal<Status>('connecting');

/** The levels, from the most recent counters event. */
export const gauges = signal<Gauges | null>(null);

/** The rate history the Overview page draws. */
export const rates = signal<Rates>({ at: [], received: [], sent: [], opened: [] });

/**
 * How many times the counters have gone backwards since this tab
 * loaded, which is how many times the broker has restarted under it.
 */
export const restarts = signal(0);

/**
 * Bumped whenever what a page holds may be stale.
 *
 * Every page refetches its resource when this changes. It moves on a
 * resynchronise event, and on a reopen this module had to make
 * itself — the two cases where the events that would have kept a
 * page in step are gone.
 */
export const generation = signal(0);

/** The identifier of the last event this tab saw, for the footer. */
export const lastEventId = signal<string | null>(null);

/** Everything wanting to be told about a connect or a disconnect. */
const watchers = new Set<(presence: Presence) => void>();

/**
 * Be told when a session gains or loses its connection.
 *
 * A callback rather than a signal holding "the most recent one",
 * because a signal is a value and a listener that reads it can only
 * ever see the last write. Two clients disconnecting in the same
 * tick would be one update, and the Clients page would leave a row
 * for a client that has gone.
 */
export function onPresence(watcher: (presence: Presence) => void): () => void {
  watchers.add(watcher);
  return () => watchers.delete(watcher);
}

/** What the previous counters event said, for the derivative. */
interface Baseline {
  totals: CounterTotals;
  /** `performance.now()`, which no clock change moves. */
  monotonic: number;
}

let baseline: Baseline | null = null;
let source: EventSource | null = null;
let reopen: ReturnType<typeof setTimeout> | undefined;

/** Append one sample, keeping the history bounded. */
function push(at: number, received: number | null, sent: number | null, opened: number | null) {
  const held = rates.value;
  const next: Rates = {
    at: [...held.at, at],
    received: [...held.received, received],
    sent: [...held.sent, sent],
    opened: [...held.opened, opened],
  };
  if (next.at.length > HISTORY) {
    next.at.shift();
    next.received.shift();
    next.sent.shift();
    next.opened.shift();
  }
  rates.value = next;
}

/** Whether any of the three totals is lower than it was. */
function wentBackwards(now: CounterTotals, then: CounterTotals): boolean {
  return (
    now.bytes_received < then.bytes_received ||
    now.bytes_sent < then.bytes_sent ||
    now.sockets_opened < then.sockets_opened
  );
}

/**
 * Turn one counters event into a rate sample.
 *
 * Exported for the tests, which is also the only way to reach the
 * restart decision without restarting a broker.
 */
export function onCounters(
  totals: CounterTotals,
  levels: Gauges,
  overMs: number,
  monotonic: number,
  wall: number,
) {
  gauges.value = levels;
  const previous = baseline;
  baseline = { totals, monotonic };

  if (!previous) {
    // The first sample of a connection has nothing to be a
    // derivative of. Plotting it as a rate against an assumed
    // interval would draw a number nobody measured.
    return;
  }
  if (wentBackwards(totals, previous.totals)) {
    restarts.value += 1;
    // A gap, not a negative. See the module documentation.
    push(wall, null, null, null);
    return;
  }
  // The greater of the browser's elapsed time and the server's own
  // interval. See the module documentation: a replayed burst makes
  // the first far too small, and a gap makes the second far too
  // small, and they are never both wrong at once.
  const seconds = Math.max((monotonic - previous.monotonic) / 1000, overMs / 1000);
  if (seconds <= 0) {
    // Two events in the same millisecond, from a server that also
    // reported no interval. A division by zero is an infinity that
    // would rescale the whole chart.
    return;
  }
  push(
    wall,
    (totals.bytes_received - previous.totals.bytes_received) / seconds,
    (totals.bytes_sent - previous.totals.bytes_sent) / seconds,
    (totals.sockets_opened - previous.totals.sockets_opened) / seconds,
  );
}

/** Read one frame's JSON, ignoring one this build does not understand. */
function bodyOf<T>(event: MessageEvent): T | null {
  try {
    return JSON.parse(event.data as string) as T;
  } catch {
    return null;
  }
}

/** Everything a page holds may be stale: say so once. */
function resynchronise(reason: string) {
  // Not an error. It is the server doing the work that stops a
  // silent gap, and the only thing to do about it is refetch.
  console.info(`sd-mqtt: refetching because the broker said to resynchronise: ${reason}`);
  // The derivative's baseline is *not* cleared. A resynchronise
  // means events were lost, and the totals are cumulative, so the
  // next sample is still a correct average over the gap.
  generation.value += 1;
}

/** Open the stream, once. */
function connect() {
  const stream = new EventSource(EVENTS);
  source = stream;

  stream.onopen = () => {
    status.value = 'live';
  };

  stream.onerror = () => {
    // `EventSource` reports both "it dropped, I am retrying" and "I
    // have given up" through this one callback, and only
    // `readyState` tells them apart.
    if (stream.readyState === EventSource.CLOSED) {
      status.value = 'lost';
      stream.close();
      if (source === stream) {
        source = null;
        clearTimeout(reopen);
        reopen = setTimeout(() => {
          // See REOPEN_AFTER_GIVING_UP: the new stream cannot carry
          // the last identifier, so the server cannot tell us what
          // we missed. We assume we missed something.
          generation.value += 1;
          status.value = 'connecting';
          connect();
        }, REOPEN_AFTER_GIVING_UP);
      }
      return;
    }
    status.value = 'lost';
  };

  stream.addEventListener('counters', (event) => {
    lastEventId.value = event.lastEventId || lastEventId.value;
    const body = bodyOf<{ totals: CounterTotals; gauges: Gauges; over_ms: number }>(event);
    if (body) {
      onCounters(body.totals, body.gauges, body.over_ms, performance.now(), Date.now() / 1000);
    }
  });

  for (const kind of ['connected', 'disconnected'] as const) {
    stream.addEventListener(kind, (event) => {
      lastEventId.value = event.lastEventId || lastEventId.value;
      const body = bodyOf<{ mountpoint: string; client_id: string }>(event);
      if (!body) {
        return;
      }
      const presence: Presence = { kind, mountpoint: body.mountpoint, client_id: body.client_id };
      for (const watcher of watchers) {
        watcher(presence);
      }
    });
  }

  stream.addEventListener('resynchronise', (event) => {
    // Deliberately carries no identifier, so `lastEventId` is left
    // alone: it is a fact about this reader, not about the broker.
    const body = bodyOf<{ reason: string }>(event);
    resynchronise(body?.reason ?? 'no reason given');
  });
}

/**
 * Start the one connection.
 *
 * Idempotent, so that a component mounting twice — which is what the
 * development server's hot reload does — does not spend a second of
 * the browser's six.
 */
export function start() {
  if (source) {
    return;
  }
  connect();
}

/** Close it, for a test and for hot reload. */
export function stop() {
  clearTimeout(reopen);
  reopen = undefined;
  source?.close();
  source = null;
  status.value = 'connecting';
  baseline = null;
}
