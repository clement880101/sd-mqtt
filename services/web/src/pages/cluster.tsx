/**
 * Cluster: who this node can see, and whether that is everybody.
 *
 * # This page polls, and it is the only one that does
 *
 * Every other page is live because the events stream carries what it
 * is about. Membership is not on that stream and deliberately is not:
 * section 8.2 says membership is **not replicated**, because a node's
 * judgement about a peer is local and may be the wrong half of a
 * one-way partition. There is no cluster-wide fact to broadcast, and
 * an event saying "beta is down" would be one node's opinion
 * presented as news.
 *
 * So this page asks, on an interval. {@link REFRESH} is chosen
 * against the failure detector rather than against what feels
 * responsive: a peer goes from `up` to `down` over
 * `cluster.dead_after_ms`, ten seconds by default, and a page that
 * polled faster than that would redraw the same answer repeatedly to
 * no purpose.
 *
 * # What this node says, not what the cluster says
 *
 * Every row is **this node's view of that peer.** Another node's page
 * may disagree, and during a one-way partition it will: that is the
 * failure the gate in `crates/sdmqtt/tests/cluster.rs` exists to
 * reproduce, and two pages showing two answers is the truth rather
 * than a bug in one of them. The heading says so, because an operator
 * comparing two tabs needs to know it before they conclude something
 * is broken.
 *
 * # "Complete" means less than it looks like, unless it was configured
 *
 * `/health/cluster` answers an expected count that is either
 * `cluster.expected_size` or the number of nodes this one has met,
 * and the two read identically. Unset, a node that came up alone and
 * never heard of the others reports a complete cluster of one — which
 * is why the document carries `expected_configured` and why this page
 * says, in words, which of the two it is looking at.
 */
import { useSignal, useSignalEffect } from '@preact/signals';

import { fetchClusterHealth, fetchPage, type ClusterHealth, type NodeView } from '../api.js';
import { generation } from '../live.js';

/**
 * How often to ask again, in milliseconds.
 *
 * Against the failure detector: `cluster.dead_after_ms` defaults to
 * ten seconds, so a peer's state cannot change faster than that and
 * polling faster buys nothing. Half of it, so a change is seen within
 * one interval of becoming true.
 */
const REFRESH = 5_000;

/** How many rows to ask for. A cluster is tens of nodes, not thousands. */
const LIMIT = 200;

/** What a peer's state means, in words an operator can act on. */
const MEANS: Record<string, string> = {
  up: 'answering',
  suspect: 'missed heartbeats; not yet declared down',
  down: 'silent for longer than the dead interval',
};

/** What the page is holding, or why it is holding nothing. */
interface Fetched {
  nodes: NodeView[];
  health: ClusterHealth | null;
  failure: string | null;
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
  return `${minutes}m`;
}

/**
 * Whether these rows are a node that does not cluster.
 *
 * One row, itself, with nothing to advertise. The address is the
 * test rather than the count, because a cluster of one that others
 * may join is a supported way to run this broker and has an address;
 * a node with `cluster.address` unset has none, and those two are
 * different situations that a count of one cannot tell apart.
 */
function standalone(nodes: NodeView[]): boolean {
  return nodes.length === 1 && nodes[0].this_node && nodes[0].address === '';
}

/** One member's row. */
function Member({ node }: { node: NodeView }) {
  return (
    <div class={`row${node.this_node ? ' row-self' : ''}`}>
      <span>
        {node.name}
        {node.this_node ? <span class="badge">this node</span> : null}
      </span>
      <span class="mono" title={node.id}>
        {node.id}
      </span>
      <span class="mono">{node.address === '' ? '—' : node.address}</span>
      <span>
        <span class={`state state-${node.state}`} title={MEANS[node.state] ?? node.state}>
          {node.state}
        </span>
      </span>
      {/* Null on every peer: membership carries no build and no
          start time, so these are known for this node alone. A dash
          rather than a blank, so the column reads as "not known
          here" rather than as missing data. */}
      <span>{node.version ?? '—'}</span>
      <span>{node.uptime_seconds === null ? '—' : uptimeOf(node.uptime_seconds)}</span>
    </div>
  );
}

/** The page. */
export function Cluster() {
  const fetched = useSignal<Fetched | null>(null);

  useSignalEffect(() => {
    // Read, so a resynchronise re-runs this effect, like every other
    // page. See `live.ts`.
    generation.value;
    let current = true;
    const read = async () => {
      try {
        const [nodes, health] = await Promise.all([
          fetchPage<NodeView>('nodes', { limit: LIMIT }),
          fetchClusterHealth(),
        ]);
        if (current) {
          fetched.value = { nodes: nodes.items, health, failure: null };
        }
      } catch (error) {
        if (current) {
          // The rows already on screen are kept rather than cleared:
          // a failed poll on a page that is working is a worse reason
          // to empty a table than it is to show a line saying the
          // last attempt failed.
          fetched.value = {
            nodes: fetched.value?.nodes ?? [],
            health: fetched.value?.health ?? null,
            failure: error instanceof Error ? error.message : String(error),
          };
        }
      }
    };
    void read();
    const timer = window.setInterval(() => void read(), REFRESH);
    return () => {
      current = false;
      window.clearInterval(timer);
    };
  });

  const held = fetched.value;
  const health = held?.health ?? null;
  const nodes = held?.nodes ?? [];

  return (
    <section class="page">
      <h1>Cluster</h1>

      {held?.failure ? (
        <p class="failure" role="alert">
          The last attempt to read the cluster failed: {held.failure}
        </p>
      ) : null}

      {held === null ? <p class="empty">Reading the cluster…</p> : null}

      {held !== null && standalone(nodes) ? (
        <p class="notice" role="status">
          This node does not cluster. <code>cluster.address</code> is unset, so it has nowhere to
          listen for peers and nothing to advertise — which is the default and a supported way to
          run this broker, not a fault.
        </p>
      ) : null}

      {health !== null && !standalone(nodes) ? (
        <div class="gauges">
          <div class="gauge">
            <span class="gauge-value">
              {health.members.toLocaleString()} / {health.expected.toLocaleString()}
            </span>
            <span class="gauge-label">members answering</span>
          </div>
          <div class="gauge">
            <span class={`gauge-value state-${health.status === 'complete' ? 'up' : 'down'}`}>
              {health.status}
            </span>
            <span class="gauge-label">view</span>
          </div>
        </div>
      ) : null}

      {health !== null && !standalone(nodes) ? (
        <p class="quiet">
          {health.expected_configured ? (
            <>
              The expected size is <strong>configured</strong>, so a node that is missing is
              counted as missing even if this node has never met it.
            </>
          ) : (
            <>
              The expected size is <strong>the number of nodes this one has met</strong>, because{' '}
              <code>cluster.expected_size</code> is unset. That cannot report a node it has never
              heard of, so it says <em>complete</em> whenever every node it knows about is
              answering. Set the key to be told about a node that is missing.
            </>
          )}
        </p>
      ) : null}

      {nodes.length > 0 && !standalone(nodes) ? (
        <>
          <div class="table cluster-table">
            <div class="row head">
              <span>name</span>
              <span>identity</span>
              <span>advertises</span>
              <span>state</span>
              <span>version</span>
              <span>up</span>
            </div>
            {nodes.map((node) => (
              <Member key={node.id} node={node} />
            ))}
          </div>
          <p class="footer quiet">
            Every state is <strong>this node's view</strong> of that peer. Another node may
            disagree, and during a one-way partition it will — two pages showing two answers is
            what a partition looks like, not a fault in one of them.
          </p>
        </>
      ) : null}
    </section>
  );
}
