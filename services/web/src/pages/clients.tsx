/**
 * Clients: every session with a connection attached, in a windowed
 * table.
 *
 * # A windowed table over a paginated resource
 *
 * The resource pages, and a window over it has to decide what
 * happens when the window reaches the end of what has been fetched.
 * Both honest answers are taken here, because either alone is
 * misleading:
 *
 * - **It fetches more.** Scrolling within {@link PREFETCH} rows of
 *   the end asks for the next page, so scrolling does not stop at an
 *   invisible boundary.
 * - **It says how much it holds.** The footer names how many rows
 *   have been fetched and how many the broker says exist, always,
 *   not only when they differ. A table that shows the first page as
 *   though it were the whole collection is a lie about the broker's
 *   state, and the version of that lie which is hardest to catch is
 *   the one where the numbers happen to agree today.
 *
 * The filter is the sharp edge of that. There is no server-side
 * filter on this resource, so it can only match what has been
 * fetched, and a filter that quietly searches a tenth of the broker
 * looks exactly like a filter that found nothing. So when a filter
 * is set and pages remain, the page says which of the two it is.
 *
 * # Why the live updates are not a refetch per event
 *
 * A connect or a disconnect must show without a reload, which is
 * what the stream is for. But refetching the table on every event
 * turns a hundred clients arriving into a hundred requests.
 *
 * So the two events are applied directly: a disconnect removes its
 * row, which is exact, and a connect inserts one built from what the
 * event carries — a mountpoint and a client identifier and nothing
 * else, because that is all it has. Such a row is marked, and its
 * missing fields read `…` rather than zero. A refetch is then
 * debounced behind the burst, and that is what fills them in.
 */
import { useSignal, useSignalEffect } from '@preact/signals';
import {
  elementScroll,
  observeElementOffset,
  observeElementRect,
  Virtualizer,
} from '@tanstack/virtual-core';
import { useEffect, useMemo, useRef, useState } from 'preact/hooks';

import { fetchPage, type Page, type SessionView } from '../api.js';
import { generation, onPresence } from '../live.js';

/**
 * How many rows one request asks for.
 *
 * The resource refuses more than a thousand rather than clamping.
 * Half of that, because a page is a unit of latency as well as of
 * size: this is what a scroll waits for.
 */
const PAGE = 500;

/** How tall a row is, in pixels. Fixed, so nothing has to measure. */
const ROW = 30;

/** How close to the end of the fetched rows a scroll asks for more. */
const PREFETCH = 50;

/** How long the refetch waits behind a burst of connects. */
const SETTLE = 1_500;

/** A row, and whether it is one the stream invented. */
interface Row extends SessionView {
  /**
   * True when this row was built from a connect event rather than
   * fetched, so everything but its identity is unknown.
   */
  partial?: boolean;
}

/** The order the resource pages in: mountpoint, then client. */
function before(left: Row, right: Row): number {
  return (
    left.mountpoint.localeCompare(right.mountpoint) || left.client_id.localeCompare(right.client_id)
  );
}

/** A row for a client the stream says has just connected. */
function invented(mountpoint: string, client_id: string): Row {
  return {
    mountpoint,
    client_id,
    connected: true,
    protocol_version: '',
    subscriptions: 0,
    queued: 0,
    inflight: 0,
    expiry_interval: 0,
    expires_at: null,
    partial: true,
  };
}

/** What a cell shows for a field an invented row does not know. */
function known(row: Row, value: number | string): string {
  return row.partial ? '…' : String(value);
}

/** The page. */
export function Clients() {
  const rows = useSignal<Row[]>([]);
  const total = useSignal(0);
  const next = useSignal<string | null>(null);
  const fetching = useSignal(false);
  const failure = useSignal<string | null>(null);
  const filter = useSignal('');
  // How many pages are held, so a refetch asks for what is held and
  // not for one page or for everything.
  const pages = useRef(1);
  const settle = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  /**
   * Which request owns the `fetching` flag.
   *
   * **The flag cannot be cleared by whoever set it**, because a
   * request can be abandoned: a resynchronise or a burst of connects
   * replaces the effect while its fetch is still in the air. An
   * abandoned request that clears the flag would clear it out from
   * under the request that replaced it; one that skips clearing it
   * leaves it set forever, and the table then quietly refuses to
   * fetch another page for the rest of the session. So each request
   * takes a ticket, and only the holder of the current one clears
   * the flag.
   *
   * **Honestly: this is reasoned, not bisected.** A table that had
   * stopped fetching further pages was seen once, this race was
   * found looking for the cause, and the stall could not afterwards
   * be reproduced on demand either with the guard or without it. So
   * the race is real and closing it is right; whether it is the
   * race that was seen is not established.
   */
  const ticket = useRef(0);

  /** Fetch from the start, replacing what is held. */
  const reload = useSignal(0);

  useSignalEffect(() => {
    // Both are read so that this runs again for either: a
    // resynchronise, and the debounced refetch behind a burst of
    // presence events.
    generation.value;
    reload.value;
    let current = true;
    const wanted = pages.current;
    const mine = (ticket.current += 1);
    const load = async () => {
      fetching.value = true;
      try {
        const collected: Row[] = [];
        let cursor: string | null = null;
        let last = 0;
        for (let page = 0; page < wanted; page += 1) {
          const answer: Page<SessionView> = await fetchPage<SessionView>('clients', {
            limit: PAGE,
            after: cursor,
          });
          if (!current) {
            return;
          }
          collected.push(...answer.items);
          last = answer.total;
          cursor = answer.next;
          if (!cursor) {
            break;
          }
        }
        rows.value = collected;
        total.value = last;
        next.value = cursor;
        failure.value = null;
      } catch (error) {
        if (current) {
          failure.value = error instanceof Error ? error.message : String(error);
        }
      } finally {
        if (ticket.current === mine) {
          fetching.value = false;
        }
      }
    };
    void load();
    return () => {
      current = false;
    };
  });

  /** Ask for the page after the last one held. */
  const more = async () => {
    if (fetching.value || !next.value) {
      return;
    }
    const mine = (ticket.current += 1);
    fetching.value = true;
    try {
      const answer = await fetchPage<SessionView>('clients', { limit: PAGE, after: next.value });
      if (ticket.current !== mine) {
        // A reload started while this page was in the air, and it is
        // fetching from the beginning. Appending to what it is about
        // to replace would duplicate rows.
        return;
      }
      rows.value = [...rows.value, ...answer.items];
      total.value = answer.total;
      next.value = answer.next;
      pages.current += 1;
      failure.value = null;
    } catch (error) {
      if (ticket.current === mine) {
        failure.value = error instanceof Error ? error.message : String(error);
      }
    } finally {
      if (ticket.current === mine) {
        fetching.value = false;
      }
    }
  };

  // The stream, applied to what is held. See the module comment on
  // why this is not a refetch per event.
  useEffect(
    () =>
      onPresence((presence) => {
        const held = rows.value;
        if (presence.kind === 'disconnected') {
          const without = held.filter(
            (row) =>
              row.client_id !== presence.client_id || row.mountpoint !== presence.mountpoint,
          );
          if (without.length !== held.length) {
            rows.value = without;
            total.value = Math.max(0, total.value - 1);
          }
        } else {
          const already = held.some(
            (row) =>
              row.client_id === presence.client_id && row.mountpoint === presence.mountpoint,
          );
          if (!already) {
            const row = invented(presence.mountpoint, presence.client_id);
            const at = held.findIndex((other) => before(row, other) < 0);
            rows.value =
              at < 0 ? [...held, row] : [...held.slice(0, at), row, ...held.slice(at)];
            total.value += 1;
          }
        }
        // Behind the burst, not per event.
        clearTimeout(settle.current);
        settle.current = setTimeout(() => {
          reload.value += 1;
        }, SETTLE);
      }),
    [],
  );

  useEffect(() => () => clearTimeout(settle.current), []);

  const needle = filter.value.trim().toLowerCase();
  const shown = useMemo(
    () =>
      needle
        ? rows.value.filter(
            (row) =>
              row.client_id.toLowerCase().includes(needle) ||
              row.mountpoint.toLowerCase().includes(needle),
          )
        : rows.value,
    [rows.value, needle],
  );

  // ---------------------------------------------------------------
  // The window
  // ---------------------------------------------------------------

  const scroller = useRef<HTMLDivElement>(null);
  // The virtualizer is imperative and tells us when to draw; this is
  // the only thing that state is for.
  const [, redraw] = useState(0);
  const virtualizer = useMemo(
    () =>
      new Virtualizer<HTMLDivElement, HTMLDivElement>({
        count: shown.length,
        getScrollElement: () => scroller.current,
        estimateSize: () => ROW,
        observeElementRect,
        observeElementOffset,
        scrollToFn: elementScroll,
        overscan: 8,
        onChange: () => redraw((tick) => tick + 1),
      }),
    [],
  );
  virtualizer.setOptions({
    ...virtualizer.options,
    count: shown.length,
    getScrollElement: () => scroller.current,
    onChange: () => redraw((tick) => tick + 1),
  });
  useEffect(() => virtualizer._didMount(), [virtualizer]);
  useEffect(() => {
    virtualizer._willUpdate();
  });

  const items = virtualizer.getVirtualItems();
  const lastDrawn = items.length > 0 ? items[items.length - 1].index : 0;

  // Scrolling towards the end asks for the next page. In an effect
  // rather than in the scroll handler, because this is a
  // consequence of what was drawn.
  useEffect(() => {
    if (next.value && !fetching.value && lastDrawn >= shown.length - PREFETCH) {
      void more();
    }
  });

  const held = rows.value;
  return (
    <section class="page">
      <h1>Clients</h1>

      {failure.value ? (
        <p class="failure" role="alert">
          The clients could not be read: {failure.value}
        </p>
      ) : null}

      <div class="controls">
        <label>
          Filter
          <input
            type="search"
            value={filter.value}
            placeholder="client identifier or mountpoint"
            onInput={(event) => (filter.value = (event.target as HTMLInputElement).value)}
          />
        </label>
        <span class="quiet">
          {shown.length.toLocaleString()} shown · {held.length.toLocaleString()} fetched ·{' '}
          {total.value.toLocaleString()} connected to the broker
        </span>
      </div>

      {needle && next.value ? (
        <p class="notice" role="status">
          This filter only sees the {held.length.toLocaleString()} rows fetched so far, of{' '}
          {total.value.toLocaleString()}. The resource has no filter of its own, so a client that
          has not been fetched cannot be matched. Scroll, or load the rest.
        </p>
      ) : null}

      <div class="table" role="table" aria-rowcount={shown.length}>
        <div class="row head" role="row">
          <span role="columnheader">client</span>
          <span role="columnheader">mountpoint</span>
          <span role="columnheader">version</span>
          <span role="columnheader">subs</span>
          <span role="columnheader">queued</span>
          <span role="columnheader">inflight</span>
          <span role="columnheader">expiry</span>
        </div>
        <div class="scroller" ref={scroller}>
          <div class="sizer" style={{ height: `${virtualizer.getTotalSize()}px` }}>
            {items.map((item) => {
              const row = shown[item.index];
              if (!row) {
                return null;
              }
              return (
                <div
                  class={`row${row.partial ? ' partial' : ''}`}
                  role="row"
                  key={`${row.mountpoint}${row.client_id}`}
                  style={{ transform: `translateY(${item.start}px)`, height: `${ROW}px` }}
                >
                  <span role="cell" class="mono" title={row.client_id}>
                    {row.client_id}
                  </span>
                  <span role="cell">{row.mountpoint || '—'}</span>
                  <span role="cell">{known(row, row.protocol_version)}</span>
                  <span role="cell">{known(row, row.subscriptions)}</span>
                  <span role="cell">{known(row, row.queued)}</span>
                  <span role="cell">{known(row, row.inflight)}</span>
                  <span role="cell">{known(row, row.expiry_interval)}</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <p class="quiet footer">
        {shown.length === 0 && !fetching.value
          ? held.length === 0
            ? 'No client is connected.'
            : 'No fetched client matches this filter.'
          : null}{' '}
        {next.value ? (
          <>
            There are more: {held.length.toLocaleString()} of {total.value.toLocaleString()} are
            loaded.{' '}
            <button type="button" onClick={() => void more()} disabled={fetching.value}>
              {fetching.value ? 'Loading…' : 'Load the next page'}
            </button>
          </>
        ) : held.length > 0 ? (
          `All ${held.length.toLocaleString()} are loaded.`
        ) : null}
      </p>
    </section>
  );
}
