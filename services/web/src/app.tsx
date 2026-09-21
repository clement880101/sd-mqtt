/**
 * The shell: a navigation bar, and a route per page with nothing in
 * it yet.
 *
 * Deliberately empty. The pipeline that builds this, attributes it,
 * embeds it and serves it is proved before a page is written,
 * because every part of it fails in a way that is hard to see from
 * inside a page.
 */
import { signal, useSignalEffect } from '@preact/signals';
import { LocationProvider, Route, Router, useLocation } from 'preact-iso';

/** The pages section 11.2 names, in the order the navigation shows them. */
const PAGES = [
  { path: '/', label: 'Overview' },
  { path: '/clients', label: 'Clients' },
  { path: '/topics', label: 'Topics' },
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

/** One page, with nothing on it. */
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
    </nav>
  );
}

/** What is served at the root of the admin listener. */
export function App() {
  useSignalEffect(() => {
    document.title = `${section.value} · sd-mqtt`;
  });
  return (
    <LocationProvider>
      <Navigation />
      <main>
        <Router onRouteChange={(url) => (section.value = labelFor(url))}>
          <Route path="/" component={() => <Placeholder title="Overview" />} />
          <Route path="/clients" component={() => <Placeholder title="Clients" />} />
          <Route path="/topics" component={() => <Placeholder title="Topics" />} />
          <Route default component={() => <Placeholder title="Not found" />} />
        </Router>
      </main>
    </LocationProvider>
  );
}
