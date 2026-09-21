/**
 * Overview: the headline levels, and the rates they move at.
 *
 * # Where each number comes from, and why it is two places
 *
 * The levels arrive on the events stream, which is the live half.
 * But the sampler only publishes a counters event **when something
 * moved**, deliberately: a counters event every second forever would
 * churn the ring and make a browser that reconnected after a quiet
 * minute resynchronise for nothing.
 *
 * The consequence for this page is the whole reason it also fetches:
 * a tab opened on a quiet broker would otherwise show nothing at all
 * until somebody connected to it, which is indistinguishable from a
 * broken interface. So the levels are read once from the resources,
 * and the stream takes over from there. They are read again whenever
 * the stream says to resynchronise.
 *
 * `queue_depth` is the one level with no resource behind it, so it
 * is the one that reads `—` until the first counters event arrives.
 * Said here rather than filled with a zero, because a zero is a
 * measurement and this is the absence of one.
 */
import { useSignal, useSignalEffect } from '@preact/signals';
import { useMemo } from 'preact/hooks';

import { fetchPage, type NodeView } from '../api.js';
import { Chart, type Line } from '../chart.js';
import { gauges, generation, rates, restarts } from '../live.js';

/** The series of the bytes chart. Module scope: see `chart.tsx`. */
const BYTES: Line[] = [
  { label: 'received', colour: '--series-a' },
  { label: 'sent', colour: '--series-b' },
];

/** The series of the sockets chart. */
const SOCKETS: Line[] = [{ label: 'opened', colour: '--series-c' }];

/** Bytes a second, in the units a person reads. */
function perSecond(value: number): string {
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)} MB/s`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1)} kB/s`;
  }
  return `${Math.round(value)} B/s`;
}

/** The same, short enough for the axis gutter. */
function perSecondTick(value: number): string {
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  }
  if (value >= 1_000) {
    return `${Math.round(value / 1_000)}k`;
  }
  return String(Math.round(value));
}

/** Sockets a second, which is a small number. */
function perSecondPlain(value: number): string {
  return value >= 10 ? `${Math.round(value)}/s` : `${value.toFixed(1)}/s`;
}

/** A duration a person reads rather than a count of seconds. */
function uptimeOf(seconds: number): string {
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  if (days > 0) {
    return `${days}d ${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m ${seconds % 60}s`;
}

/** What the resources say before the stream has said anything. */
interface Fetched {
  node: NodeView | null;
  connections: number;
  sessions: number;
  subscriptions: number;
  retained: number;
  failure: string | null;
}

/** One headline number. */
function Gauge({ label, value }: { label: string; value: number | string | null }) {
  return (
    <div class="gauge">
      <span class="gauge-value">{value === null ? '—' : value.toLocaleString()}</span>
      <span class="gauge-label">{label}</span>
    </div>
  );
}

/** The page. */
export function Overview() {
  const fetched = useSignal<Fetched | null>(null);

  useSignalEffect(() => {
    // Read, so that a resynchronise re-runs this effect. See
    // `live.ts`: the server sends one when what we hold may be
    // stale, and carrying on regardless is the failure it exists to
    // prevent.
    generation.value;
    let current = true;
    const totals = async () => {
      // One row of each, for the `total` in the envelope. The
      // Clients page walks the rows; this page only counts them.
      const one = { limit: 1 };
      try {
        const [nodes, clients, sessions, subscriptions, retained] = await Promise.all([
          fetchPage<NodeView>('nodes', one),
          fetchPage('clients', one),
          fetchPage('sessions', one),
          fetchPage('subscriptions', one),
          fetchPage('retained', one),
        ]);
        if (!current) {
          return;
        }
        fetched.value = {
          node: nodes.items[0] ?? null,
          connections: clients.total,
          sessions: sessions.total,
          subscriptions: subscriptions.total,
          retained: retained.total,
          failure: null,
        };
      } catch (error) {
        if (current) {
          fetched.value = {
            node: null,
            connections: 0,
            sessions: 0,
            subscriptions: 0,
            retained: 0,
            failure: error instanceof Error ? error.message : String(error),
          };
        }
      }
    };
    void totals();
    return () => {
      current = false;
    };
  });

  const live = gauges.value;
  const held = fetched.value;
  const history = rates.value;
  const broke = restarts.value;

  // uPlot's layout, built from the one history the live module
  // keeps. `useMemo` so the plot is handed a new array only when
  // there is a new sample in it.
  const bytes = useMemo(
    () => [history.at, history.received, history.sent] as [number[], (number | null)[], (number | null)[]],
    [history],
  );
  const sockets = useMemo(
    () => [history.at, history.opened] as [number[], (number | null)[]],
    [history],
  );

  return (
    <section class="page">
      <h1>Overview</h1>

      {held?.failure ? (
        <p class="failure" role="alert">
          The resources could not be read: {held.failure}
        </p>
      ) : null}

      <div class="gauges">
        <Gauge label="connections" value={live?.connections ?? held?.connections ?? null} />
        <Gauge label="sessions" value={live?.sessions ?? held?.sessions ?? null} />
        <Gauge label="subscriptions" value={live?.subscriptions ?? held?.subscriptions ?? null} />
        <Gauge label="retained" value={live?.retained ?? held?.retained ?? null} />
        {/* No resource answers this one; see the module comment. */}
        <Gauge label="queued" value={live?.queue_depth ?? null} />
      </div>

      {held?.node ? (
        <p class="quiet">
          Node <strong>{held.node.id}</strong>, version {held.node.version}, up{' '}
          {uptimeOf(held.node.uptime_seconds)}.
        </p>
      ) : null}

      {broke > 0 ? (
        <p class="notice" role="status">
          The broker's counters went backwards {broke === 1 ? 'once' : `${broke} times`} while this
          page was open, which is a restart. The lines break where it happened rather than plotting
          the negative rate that subtracting across a restart would give.
        </p>
      ) : null}

      <div class="charts">
        <Chart
          title="Bytes a second"
          data={bytes}
          lines={BYTES}
          format={perSecond}
          formatTick={perSecondTick}
        />
        <Chart
          title="Sockets opened a second"
          data={sockets}
          lines={SOCKETS}
          format={perSecondPlain}
          formatTick={perSecondPlain}
        />
      </div>

      {history.at.length === 0 ? (
        <p class="empty">
          No rates yet. The broker publishes a counters event only when a number moves, so a quiet
          broker draws nothing until something happens to it.
        </p>
      ) : null}
    </section>
  );
}
