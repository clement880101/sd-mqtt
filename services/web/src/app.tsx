/**
 * The shell: a navigation bar, the one live connection, and a route
 * per page.
 *
 * The connection is started here and nowhere else. Section 11.1: the
 * interface opens exactly one stream for the whole application,
 * because the admin listener speaks HTTP/1.1 and a browser allows six
 * connections to an origin. See `live.ts`.
 */
import { signal, useSignalEffect } from '@preact/signals';
import { LocationProvider, Route, Router, useLocation } from 'preact-iso';
import { useEffect } from 'preact/hooks';

import { lastEventId, start, status } from './live.js';
import { eventNumber } from './event-id.js';
import { Clients } from './pages/clients.js';
import { Cluster } from './pages/cluster.js';
import { Overview } from './pages/overview.js';
import { Rules } from './pages/rules.js';
import { Settings } from './pages/settings.js';
import { Topics } from './pages/topics.js';

/** The pages section 11.2 names, in the order the navigation shows them. */
const PAGES = [
  { path: '/', label: 'Overview' },
  { path: '/clients', label: 'Clients' },
  { path: '/topics', label: 'Topics' },
  { path: '/rules', label: 'Rules' },
  { path: '/cluster', label: 'Cluster' },
  { path: '/settings', label: 'Settings' },
] as const;

/** What the page at `path` is called, for the tab and the navigation. */
function labelFor(path: string): string {
  return PAGES.find((page) => page.path === path)?.label ?? 'Not found';
}

/**
 * Which section is open.
 *
 * A signal rather than component state because the thing that reads
 * it is the document title, which is outside the tree, and the thing
 * that writes it is the router's callback, which is outside the
 * component that would hold the state.
 *
 * Seeded from the address the document was opened at, so a deep link
 * names itself before any navigation has happened.
 */
const section = signal(labelFor(window.location.pathname));

/** One page that has not been written. */
function Placeholder({ title }: { title: string }) {
  return (
    <section class="page">
      <h1>{title}</h1>
      <p class="empty">No data yet. This page arrives with its own plan.</p>
    </section>
  );
}

/** The navigation, with the current page marked. */
function Navigation() {
  const { path } = useLocation();
  return (
    <nav class="nav" aria-label="Sections">
      <span class="brand">sd-mqtt</span>
      <ul>
        {PAGES.map((page) => (
          <li key={page.path}>
            <a href={page.path} aria-current={page.path === path ? 'page' : undefined}>
              {page.label}
            </a>
          </li>
        ))}
      </ul>
      <LiveStatus />
    </nav>
  );
}

/** What each state of the connection says. */
const SAID = {
  connecting: 'Connecting to the broker',
  live: 'Live',
  lost: 'Lost the connection to the broker. Retrying.',
} as const;

/**
 * Whether the live connection is up.
 *
 * Shown always rather than only when it breaks. A page whose data
 * stopped arriving looks exactly like a quiet broker, and the whole
 * point of this line is that an operator can tell the two apart
 * without reloading to find out.
 */
function LiveStatus() {
  const state = status.value;
  const seen = eventNumber(lastEventId.value);
  return (
    <p class={`live live-${state}`} role="status" aria-live="polite">
      <span class="live-dot" aria-hidden="true" />
      {SAID[state]}
      {state === 'live' && seen ? <span class="live-id"> · event {seen}</span> : null}
    </p>
  );
}

/** What is served at the root of the admin listener. */
export function App() {
  useSignalEffect(() => {
    document.title = `${section.value} · sd-mqtt`;
  });
  // One connection for the application, opened once the shell is
  // mounted. `start` is idempotent so a remount costs nothing.
  useEffect(() => start(), []);
  return (
    <LocationProvider>
      <Navigation />
      <main>
        <Router onRouteChange={(url) => (section.value = labelFor(url))}>
          <Route path="/" component={Overview} />
          <Route path="/clients" component={Clients} />
          <Route path="/topics" component={Topics} />
          <Route path="/rules" component={Rules} />
          <Route path="/cluster" component={Cluster} />
          <Route path="/settings" component={Settings} />
          <Route default component={() => <Placeholder title="Not found" />} />
        </Router>
      </main>
    </LocationProvider>
  );
}
