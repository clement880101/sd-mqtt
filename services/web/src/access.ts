/**
 * What the Settings page decides without a broker: the credential a
 * request carries, a rule in words, and what a form can refuse before
 * it is sent.
 *
 * Pure, so `access.test.ts` can run it under `node --test` with no
 * renderer — the same bargain `drawing.ts` made. The broker is still
 * the judge of a rule (`directory::check_rule`); these checks only
 * save a round trip for a mistake the form can see, and say it in the
 * same words.
 */

/** The prefix every key the broker mints begins with. */
export const KEY_PREFIX = 'sdmqtt_';

/**
 * `headers` with the bearer added when there is a key.
 *
 * Every request carries it, reads included: a read resource ignores
 * it, and one rule for every request is one fewer place to forget it.
 * A blank key is no key, so pasting whitespace does not send a header
 * the broker would refuse.
 */
export function withBearer(
  headers: Record<string, string>,
  key: string | null,
): Record<string, string> {
  const trimmed = key?.trim() ?? '';
  return trimmed === '' ? headers : { ...headers, authorization: `Bearer ${trimmed}` };
}

/** A rule's three-way opinion on one operation, as the form holds it. */
export type Opinion = 'allow' | 'deny' | '';

/** The opinion as the API spells it: absent, true or false. */
export function opinionValue(opinion: Opinion): boolean | undefined {
  return opinion === '' ? undefined : opinion === 'allow';
}

/** An access control rule, as the API reads and writes it. */
export interface AclRule {
  id: string;
  order: number;
  subject: string;
  filter: string;
  publish?: boolean;
  subscribe?: boolean;
}

/** Who a subject names, in words. */
function whom(subject: string): string {
  if (subject === 'anyone') {
    return 'Anyone';
  }
  if (subject.startsWith('user:')) {
    return `User ${subject.slice('user:'.length)}`;
  }
  if (subject.startsWith('client:')) {
    return `Client ${subject.slice('client:'.length)}`;
  }
  return subject;
}

/** One opinion in words, or nothing when the rule has none. */
function verb(allowed: boolean | undefined, operation: string): string | null {
  if (allowed === undefined) {
    return null;
  }
  return allowed ? `may ${operation}` : `may not ${operation}`;
}

/**
 * A rule as a sentence: "Anyone may not subscribe to secret/#".
 *
 * The form an operator reads to check a rule says what they meant;
 * the columns are there for scanning, and this is there for reading.
 */
export function describeRule(rule: AclRule): string {
  const said = [verb(rule.publish, 'publish'), verb(rule.subscribe, 'subscribe')].filter(
    (part): part is string => part !== null,
  );
  return `${whom(rule.subject)} ${said.join(' and ')} ${said.length === 2 ? 'on' : 'to'} ${rule.filter}`;
}

/** What a rule form says is wrong, by field. Empty when nothing is. */
export type Problems = Partial<Record<'id' | 'subject' | 'filter' | 'order' | 'opinion', string>>;

/**
 * What the broker would refuse that the form can already see.
 *
 * The same four refusals `check_rule` makes, minus the full filter
 * grammar, which is the codec's and is left to it: a filter mistake
 * comes back from the broker, field and all.
 */
export function ruleProblems(rule: AclRule): Problems {
  const problems: Problems = {};
  if (rule.id.trim() === '') {
    problems.id = 'A rule needs an identifier.';
  }
  const named = /^(user|client):.+$/.test(rule.subject);
  if (rule.subject !== 'anyone' && !named) {
    problems.subject = 'anyone, user:<name> or client:<identifier>.';
  }
  if (rule.filter === '') {
    problems.filter = 'A rule needs a topic filter; # is every topic.';
  }
  if (!Number.isInteger(rule.order) || rule.order < 0 || rule.order > 4_294_967_295) {
    problems.order = 'A whole number, 0 or more; lower is consulted first.';
  }
  if (rule.publish === undefined && rule.subscribe === undefined) {
    problems.opinion = 'A rule with no opinion on publishing or subscribing decides nothing.';
  }
  return problems;
}

/** Whether a pasted value looks like a key the broker minted. */
export function looksLikeKey(value: string): boolean {
  return value.trim().startsWith(KEY_PREFIX);
}
