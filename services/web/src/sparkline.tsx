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
 * # What an empty series draws, and what a single point draws
 *
 * Both happen constantly — a rule installed a second ago has one
 * sample and a rule installed this instant has none — and **neither is
 * a line**. Two points make a line; one is a position and none is
 * nothing.
 *
 * The failure mode this is written against is a sparkline that draws a
 * convincing flat line when it has nothing to draw. An operator
 * reading a flat line reads "this rule is quiet", which is a fact
 * about the broker; "no series has arrived yet" is a fact about this
 * page, and the two must not look the same. So:
 *
 * - **Empty draws no geometry at all.** There is no `<path>` and no
 *   `<svg>`: the cell says `no samples` in words. Nothing can be
 *   mistaken for a measurement.
 * - **One point draws a dot**, at the right-hand edge, which is where
 *   the newest sample sits at every other length. A dot is visibly not
 *   a line, and it appears exactly once in a rule's life — for one
 *   second, on the way from nothing to a line.
 * - **Two or more points draw the path**, and only then.
 *
 * # Zero is the floor, so a flat line at the bottom means zero
 *
 * The vertical scale runs from zero to the series' own peak, never
 * from its minimum, because these are counts and a count's floor is
 * zero. A rule that fired nothing for a minute is therefore a flat
 * line along the bottom of the cell rather than a flat line through
 * the middle, which is what a minimum-to-maximum scale draws for any
 * unvarying series and which reads as "something, steadily".
 *
 * The peak is at least one, which both avoids dividing by zero and is
 * what makes the paragraph above true.
 *
 * **The scale is each row's own.** A sparkline is a shape — is this
 * rising, did it stop, is it spiky — and the numbers are in the
 * columns beside it. A scale shared across the table would flatten
 * every rule against the busiest one. The peak is in the label, so
 * the one thing the missing axis would have said is still said.
 */

/** How wide one is, in pixels. */
const WIDTH = 96;

/** How tall. A row is 30 pixels and this sits inside one. */
const HEIGHT = 22;

/**
 * The margin the drawing keeps inside the box.
 *
 * Half the stroke would do for the line; the dot's radius is what
 * actually sets it, because a dot at the right-hand edge with no
 * margin is a half dot.
 */
const PAD = 2.5;

/** How big the one-sample dot is. */
const DOT = 2;

/** What a series of a given length draws. */
export type Drawing =
  /** Nothing: there is no series yet. */
  | { kind: 'empty' }
  /** One sample, as a position. */
  | { kind: 'point'; x: number; y: number; peak: number }
  /** Two or more, as a path. */
  | { kind: 'line'; d: string; peak: number };

/** Round to a tenth of a pixel: 200 rows of path data is a lot of text. */
function tidy(value: number): number {
  return Math.round(value * 10) / 10;
}

/**
 * Where one sample sits in the box.
 *
 * `index` is over `count` samples oldest first, so the newest is
 * always at the right-hand edge whatever the length is. That is what
 * makes the single point's position the same rule as every other
 * point's rather than a special case invented for it.
 */
function at(index: number, count: number, value: number, peak: number): [number, number] {
  const span = WIDTH - PAD * 2;
  const rise = HEIGHT - PAD * 2;
  // `count - 1` is zero for one sample, and the caller draws a dot
  // rather than dividing by it.
  const x = count > 1 ? PAD + (index * span) / (count - 1) : WIDTH - PAD;
  const y = HEIGHT - PAD - (value / peak) * rise;
  return [tidy(x), tidy(y)];
}

/**
 * What to draw for this series, as geometry and nothing else.
 *
 * Exported and pure, because this package has no test runner and this
 * is the decision worth checking: `await import('/src/sparkline.tsx')`
 * in a browser console against the development server reaches all
 * three branches with a series typed by hand, and the `d` it returns
 * can be read as text rather than judged by eye. Both were done; see
 * the plan's task 4.
 *
 * A value that is not a finite number is drawn as zero. The resource
 * answers `u32`s and cannot send one, so this is about a build of this
 * page meeting a broker that sends something it does not understand,
 * where a `NaN` in a path attribute silently drops the whole line.
 */
export function drawing(values: readonly number[]): Drawing {
  if (values.length === 0) {
    return { kind: 'empty' };
  }
  const safe = values.map((value) => (Number.isFinite(value) && value > 0 ? value : 0));
  // At least one: see the module note. A series of zeros then draws
  // along the floor rather than dividing by nothing.
  const peak = Math.max(1, ...safe);
  if (safe.length === 1) {
    const [x, y] = at(0, 1, safe[0], peak);
    return { kind: 'point', x, y, peak };
  }
  const steps = safe.map((value, index) => {
    const [x, y] = at(index, safe.length, value, peak);
    return `${index === 0 ? 'M' : 'L'}${x} ${y}`;
  });
  return { kind: 'line', d: steps.join(''), peak };
}

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
    // No geometry at all. See the module note: the one thing this
    // must not do is draw something that reads as a measurement.
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
