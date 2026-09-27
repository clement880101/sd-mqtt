/**
 * What the sparkline draws, checked by something that can run it.
 *
 * # Why this exists, and what it is not
 *
 * The Rules page and the sparkline had no automated test of any kind,
 * which the milestone-four walk called the exit criterion's weakest
 * guard. A real test of the page needs a renderer and therefore a
 * browser test runner, and that is a dependency, a configuration and a
 * decision with weight — deliberately not taken here.
 *
 * What was cheap and honest was this. `drawing` is pure: an array of
 * numbers in, geometry out, no DOM, no signals, no network. Node 22
 * runs TypeScript by stripping its types, and `node --test` has been
 * in the runtime since Node 18. So the cost of this file is **zero
 * new dependencies** and one script in `package.json`, and what it
 * buys is the one decision in the sparkline that can be wrong quietly.
 *
 * It is a unit test of a helper, which convention 5 says is not a test
 * of the caller that uses it: `Sparkline` could stop calling `drawing`
 * altogether and every assertion here would still pass. That is the
 * limit of it and it is recorded rather than talked around.
 *
 * # What it asserts, and why each one can fail
 *
 * Convention 5's question of every test here — what would have to be
 * true for this to fail, and can the input make it true:
 *
 * - **Empty draws nothing.** Fails if `drawing` ever answers geometry
 *   for a series that does not exist, which is the failure mode the
 *   module was written against: a convincing flat line that means "no
 *   data" and reads as "no traffic".
 * - **One sample is a point and two are a line.** Fails if the
 *   boundary moves, which it would if `length === 1` became
 *   `length <= 1` or `< 2`.
 * - **The newest sample is at the right-hand edge**, at every length
 *   including one. Fails if the single point is placed at the left or
 *   the centre, which is the special case the module refuses to make.
 * - **Zero is the floor.** A flat series draws along the bottom, not
 *   through the middle. Fails the moment the scale is taken from the
 *   series' minimum instead of from zero — which is what most
 *   sparkline code does and what would make every quiet rule look
 *   busy.
 * - **The peak sets the top.** Fails if the scale is a constant.
 * - **Nothing that is not a number reaches the path.** Fails if a
 *   `NaN` or an `Infinity` is passed through, which silently drops the
 *   whole line in an SVG rather than drawing a wrong one.
 */

import assert from 'node:assert/strict';
import { test } from 'node:test';

import { drawing, HEIGHT, PAD, WIDTH } from './drawing.ts';

/** Every coordinate in a path's `d`, in order. */
function coordinates(d: string): number[] {
  return d
    .split(/[ML]/)
    .filter((part) => part.length > 0)
    .flatMap((part) => part.split(' ').map(Number));
}

test('no samples draw no geometry at all', () => {
  assert.deepEqual(drawing([]), { kind: 'empty' });
});

test('one sample is a point and not a line', () => {
  const drawn = drawing([3]);
  assert.equal(drawn.kind, 'point');
});

test('two samples are a line and not a point', () => {
  const drawn = drawing([3, 4]);
  assert.equal(drawn.kind, 'line');
});

test('the newest sample sits at the right-hand edge, at every length', () => {
  const one = drawing([5]);
  assert.equal(one.kind, 'point');
  if (one.kind !== 'point') {
    return;
  }
  assert.equal(one.x, WIDTH - PAD);

  for (const length of [2, 3, 17, 60]) {
    const drawn = drawing(Array.from({ length }, (_, index) => index));
    assert.equal(drawn.kind, 'line');
    if (drawn.kind !== 'line') {
      continue;
    }
    const xs = coordinates(drawn.d).filter((_, index) => index % 2 === 0);
    assert.equal(xs.length, length, `a series of ${length} drew ${xs.length} points`);
    assert.equal(xs[0], PAD, `the oldest sample of ${length} is not at the left edge`);
    assert.equal(
      xs[xs.length - 1],
      WIDTH - PAD,
      `the newest sample of ${length} is not at the right edge`,
    );
  }
});

test('a series of zeros draws along the floor and not through the middle', () => {
  const drawn = drawing([0, 0, 0, 0]);
  assert.equal(drawn.kind, 'line');
  if (drawn.kind !== 'line') {
    return;
  }
  const ys = coordinates(drawn.d).filter((_, index) => index % 2 === 1);
  // The bottom of the box, not the middle of it. A scale taken from
  // the series' own minimum would put every one of these at the top.
  assert.deepEqual(ys, [HEIGHT - PAD, HEIGHT - PAD, HEIGHT - PAD, HEIGHT - PAD]);
  assert.equal(drawn.peak, 1, 'the peak of an empty-looking series is one, never zero');
});

test('a flat non-zero series is drawn at the top, because its peak is its own', () => {
  const drawn = drawing([7, 7, 7]);
  assert.equal(drawn.kind, 'line');
  if (drawn.kind !== 'line') {
    return;
  }
  assert.equal(drawn.peak, 7);
  const ys = coordinates(drawn.d).filter((_, index) => index % 2 === 1);
  assert.deepEqual(ys, [PAD, PAD, PAD]);
});

test('the peak is the top of the box and zero is the bottom', () => {
  const drawn = drawing([0, 10]);
  assert.equal(drawn.kind, 'line');
  if (drawn.kind !== 'line') {
    return;
  }
  assert.equal(drawn.peak, 10);
  const ys = coordinates(drawn.d).filter((_, index) => index % 2 === 1);
  assert.deepEqual(ys, [HEIGHT - PAD, PAD]);
});

test('nothing that is not a finite number reaches the path', () => {
  const drawn = drawing([Number.NaN, Number.POSITIVE_INFINITY, -4, 2]);
  assert.equal(drawn.kind, 'line');
  if (drawn.kind !== 'line') {
    return;
  }
  assert.ok(
    coordinates(drawn.d).every((value) => Number.isFinite(value)),
    `a non-finite coordinate drops the whole line: ${drawn.d}`,
  );
  // The three unusable values were read as zero, so the peak is the
  // one real sample.
  assert.equal(drawn.peak, 2);
});

test('every coordinate is rounded to a tenth of a pixel', () => {
  const drawn = drawing([1, 2, 3, 4, 5, 6, 7]);
  assert.equal(drawn.kind, 'line');
  if (drawn.kind !== 'line') {
    return;
  }
  for (const value of coordinates(drawn.d)) {
    assert.equal(
      value,
      Math.round(value * 10) / 10,
      `${value} carries more precision than a 96-pixel cell can show`,
    );
  }
});
