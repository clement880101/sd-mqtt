/**
 * What `access.ts` decides, checked by something that can run it.
 *
 * Like `drawing.test.ts`, a unit test of helpers and not of the page
 * that calls them; the page is checked against a running broker.
 * Each assertion is one a plausible mistake would break:
 *
 * - **A blank key sends no header**, which a check of `key !== null`
 *   alone would get wrong for a pasted space.
 * - **A rule with only one opinion reads as one clause**, and with
 *   both as two; a join that assumed both would print "and" alone.
 * - **The form refuses what `check_rule` refuses**, including a rule
 *   with no opinion, which is the refusal easiest to forget.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { describeRule, looksLikeKey, opinionValue, ruleProblems, withBearer } from './access.ts';

test('a key is sent as a bearer, and a blank one is not sent', () => {
  assert.deepEqual(withBearer({ accept: 'x' }, 'sdmqtt_ab_cd'), {
    accept: 'x',
    authorization: 'Bearer sdmqtt_ab_cd',
  });
  assert.deepEqual(withBearer({}, '  sdmqtt_ab_cd \n'), { authorization: 'Bearer sdmqtt_ab_cd' });
  assert.deepEqual(withBearer({ accept: 'x' }, null), { accept: 'x' });
  assert.deepEqual(withBearer({}, '   '), {});
});

test('an opinion is absent, true or false', () => {
  assert.equal(opinionValue(''), undefined);
  assert.equal(opinionValue('allow'), true);
  assert.equal(opinionValue('deny'), false);
});

test('a rule reads as a sentence with as many clauses as opinions', () => {
  assert.equal(
    describeRule({ id: 'a', order: 10, subject: 'anyone', filter: 'secret/#', subscribe: false }),
    'Anyone may not subscribe to secret/#',
  );
  assert.equal(
    describeRule({
      id: 'b',
      order: 100,
      subject: 'user:alice',
      filter: '#',
      publish: true,
      subscribe: true,
    }),
    'User alice may publish and may subscribe on #',
  );
  assert.equal(
    describeRule({ id: 'c', order: 1, subject: 'client:gw-1', filter: 'fleet/+', publish: true }),
    'Client gw-1 may publish to fleet/+',
  );
});

test('the form refuses what the broker would', () => {
  const good = { id: 'r', order: 100, subject: 'anyone', filter: '#', publish: true };
  assert.deepEqual(ruleProblems(good), {});
  assert.ok(ruleProblems({ ...good, id: ' ' }).id);
  assert.ok(ruleProblems({ ...good, subject: 'everyone' }).subject);
  assert.ok(ruleProblems({ ...good, subject: 'user:' }).subject);
  assert.deepEqual(ruleProblems({ ...good, subject: 'client:gw-1' }), {});
  assert.ok(ruleProblems({ ...good, filter: '' }).filter);
  assert.ok(ruleProblems({ ...good, order: -1 }).order);
  assert.ok(ruleProblems({ ...good, order: 1.5 }).order);
  assert.ok(ruleProblems({ id: 'r', order: 1, subject: 'anyone', filter: '#' }).opinion);
});

test('a minted key is recognised by its prefix', () => {
  assert.ok(looksLikeKey(' sdmqtt_0a1b2c3d4e5f_secret'));
  assert.ok(!looksLikeKey('hunter2'));
});
