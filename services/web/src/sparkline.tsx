/**
 * A sparkline: one SVG path per table row, hand drawn.
 *
 * **One path, not one chart.** `docs/research/component-selection.md`
 * chose this over a chart instance per row and gave the reason: a
 * chart library instance per table row does not scale, and a table of
 * two hundred rules is two hundred of whatever is chosen. uPlot is on
 * the Overview page, where there are two plots and both have axes, a
 * legend and a cursor; none of that is wanted in a 24-pixel cell, and
 * all of it would be paid for two hundred times. What is actually
 * wanted is a polyline, which is one `d` attribute.
 *
 * # The decision is next door, because the decision is tested
 *
 * `./drawing.ts` holds the geometry: what an empty series draws, what
 * one sample draws, where the floor of the scale is. That file has a
 * test — `drawing.test.ts`, run by `npm test` — and this one does
 * not, and the split is exactly that line. Node 22 runs TypeScript by
 * stripping its types and does not strip JSX, so a `.tsx` file cannot
 * be a `node --test` target.
 *
 * **What is therefore still unguarded** is everything below: that the
 * empty case renders words rather than an `<svg>`, that one sample
 * renders a `<circle>` and two render a `<path>`, that the label says
 * the latest value and the peak. Those need a renderer, which needs a
 * browser test runner, which is a decision this milestone did not
 * take. Written here rather than left to be discovered, so it is a
 * deferral and not an oversight — convention 8.
 */

import { drawing, HEIGHT, WIDTH, type Drawing } from './drawing.js';

/** How big the one-sample dot is. */
const DOT = 2;

/** What one is, for a screen reader and for the pointer. */
function said(what: string, values: readonly number[], drawn: Drawing): string {
  if (drawn.kind === 'empty') {
    return `${what}: no samples yet`;
  }
  const latest = values[values.length - 1];
  return `${what}: ${values.length} sample${values.length === 1 ? '' : 's'}, latest ${latest}, peak ${drawn.peak}`;
}

/** What a sparkline is given. */
export interface SparklineProps {
  /** The series, oldest first. */
  values: readonly number[];
  /** What it is a series of, for the label: `fired`, `waiting`. */
  what: string;
}

/**
 * One row's series.
 *
 * `role="img"` with a label rather than a bare `<svg>`, because a
 * drawing with no text is invisible to anything that is not a pair of
 * eyes, and the label carries the two numbers the missing axis would
 * have: the latest value and the peak the scale is against.
 */
export function Sparkline({ values, what }: SparklineProps) {
  const drawn = drawing(values);
  const label = said(what, values, drawn);
  if (drawn.kind === 'empty') {
    // No geometry at all. See `./drawing.ts`: the one thing this must
    // not do is draw something that reads as a measurement.
    return (
      <span class="spark spark-empty" title={label}>
        no samples
      </span>
    );
  }
  return (
    <svg
      class="spark"
      width={WIDTH}
      height={HEIGHT}
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      role="img"
      aria-label={label}
    >
      <title>{label}</title>
      {drawn.kind === 'point' ? (
        <circle class="spark-dot" cx={drawn.x} cy={drawn.y} r={DOT} />
      ) : (
        <path class="spark-line" d={drawn.d} fill="none" />
      )}
    </svg>
  );
}
