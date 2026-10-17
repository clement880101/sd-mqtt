/**
 * What `event-id.ts` shows, checked by something that can run it.
 *
 * The broker tags each event with the run it came from, so that a
 * browser reconnecting after a restart is told to start over rather
 * than being replayed the wrong run's events. The status line has no
 * use for the run, only for how far the stream has got.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

import { eventNumber } from './event-id.ts';

test('an identifier tagged with its run shows only its number', () => {
  assert.equal(eventNumber('1760659200000-42'), '42');
});

test('a bare number, as an older broker sends, shows as it is', () => {
  assert.equal(eventNumber('42'), '42');
});

test('nothing seen yet shows nothing', () => {
  assert.equal(eventNumber(null), null);
  assert.equal(eventNumber(''), null);
});
