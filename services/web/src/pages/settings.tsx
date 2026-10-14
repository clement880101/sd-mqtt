/**
 * Settings: who may connect, what they may do, who may change it, and
 * what this node listens on. Section 11.2.
 *
 * # Four resources and one credential
 *
 * Users, access control rules and API keys are durable metadata that
 * replicate to every node, so a change made here is a change to the
 * cluster; listeners are this process's, read-only, and say so. Every
 * change needs an API key whose scope covers it — `admin` for all
 * three — which this tab presents once it is pasted at the top. See
 * `credential.ts` for where it is kept and why there.
 *
 * # What is never shown
 *
 * A password goes in and nothing comes back: the listing says which
 * credential forms a user has. A key's text is shown once, in the
 * answer that minted it, because the broker keeps only a digest; the
 * page says so where it shows it, since an operator who navigates
 * away has lost it.
 *
 * # Rules from the file are not here
 *
 * `[[acl]]` tables in the configuration are the node's own and are
 * consulted together with these, by order. The API lists only what it
 * stores, and the page says that rather than presenting an incomplete
 * list as the whole.
 */
import { useSignal, useSignalEffect, type Signal } from '@preact/signals';

import {
  describeRule,
  looksLikeKey,
  opinionValue,
  ruleProblems,
  type AclRule,
  type Opinion,
  type Problems,
} from '../access.js';
import {
  ApiError,
  createKey,
  deleteAcl,
  deleteUser,
  fetchPage,
  putAcl,
  putUser,
  revokeKey,
  type ApiKeyView,
  type IssuedKey,
  type ListenerView,
  type Scope,
  type UserView,
} from '../api.js';
import { key, presentKey } from '../credential.js';
import { generation } from '../live.js';

/** Rows asked for. A directory is tens of entries, not thousands. */
const LIMIT = 500;

/** One resource as the page holds it. */
interface Held<T> {
  rows: T[];
  failure: string | null;
}

/** A failure in words, with what to do when it is about the key. */
function said(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.code === 'unauthorized') {
      return `${error.message}. Paste an API key with admin scope at the top of this page.`;
    }
    if (error.code === 'forbidden') {
      return `${error.message}. Changing users, rules and keys needs a key with admin scope.`;
    }
    return error.message;
  }
  return error instanceof Error ? error.message : String(error);
}

/** When something was made, in the reader's time zone. */
function when(seconds: number): string {
  return new Date(seconds * 1000).toLocaleString();
}

/** Read a resource whole, into `into`, again whenever `generation` or `again` moves. */
function useResource<T>(resource: string, again: Signal<number>): Signal<Held<T> | null> {
  const held = useSignal<Held<T> | null>(null);
  useSignalEffect(() => {
    generation.value;
    again.value;
    let current = true;
    fetchPage<T>(resource, { limit: LIMIT })
      .then((page) => {
        if (current) {
          held.value = { rows: page.items, failure: null };
        }
      })
      .catch((error: unknown) => {
        if (current) {
          held.value = { rows: [], failure: said(error) };
        }
      });
    return () => {
      current = false;
    };
  });
  return held;
}

/**
 * Run a change, report it, and read the resources again.
 *
 * One outcome line per section rather than per row: a change is one
 * thing at a time, and its answer belongs next to the form that made
 * it.
 */
function useChange(again: Signal<number>) {
  const outcome = useSignal<{ text: string; failed: boolean } | null>(null);
  const busy = useSignal(false);
  // An outcome is about the key that made it. A refusal left on the
  // page after the key changed would describe a request nobody is
  // about to make.
  useSignalEffect(() => {
    key.value;
    outcome.value = null;
  });
  const run = async (success: string, change: () => Promise<unknown>) => {
    busy.value = true;
    try {
      await change();
      outcome.value = { text: success, failed: false };
      again.value += 1;
      return true;
    } catch (error) {
      outcome.value = { text: said(error), failed: true };
      return false;
    } finally {
      busy.value = false;
    }
  };
  return { outcome, busy, run };
}

/** The outcome line under a form. */
function Outcome({ outcome }: { outcome: Signal<{ text: string; failed: boolean } | null> }) {
  const value = outcome.value;
  return value ? (
    <p class={value.failed ? 'failure' : 'notice'} role={value.failed ? 'alert' : 'status'}>
      {value.text}
    </p>
  ) : null;
}

/** A resource that could not be read: the broker's own words. */
function Unread({ held }: { held: Held<unknown> | null }) {
  if (held === null) {
    return <p class="quiet">Reading…</p>;
  }
  return held.failure ? (
    <p class="failure" role="alert">
      {held.failure}
    </p>
  ) : null;
}

// ---------------------------------------------------------------
// The key this tab presents
// ---------------------------------------------------------------

/** What is held, shown without the secret half. */
function masked(value: string): string {
  if (!looksLikeKey(value)) {
    return 'a token';
  }
  const [, id] = value.split('_');
  return `key ${id}`;
}

function Credential() {
  const draft = useSignal('');
  const held = key.value;
  return (
    <section>
      <h2>Your key</h2>
      <p class="hint">
        Changes need an API key whose scope covers them; everything on this page needs{' '}
        <strong>admin</strong>. The configured <span class="mono">admin.token</span> is one. On a
        loopback listener with no token configured, none is needed. The key is kept for this tab
        only and forgotten when it closes.
      </p>
      <form
        class="fields"
        onSubmit={(event) => {
          event.preventDefault();
          presentKey(draft.value);
          draft.value = '';
        }}
      >
        <label>
          API key or token
          <input
            type="password"
            autocomplete="off"
            value={draft.value}
            onInput={(event) => (draft.value = event.currentTarget.value)}
            placeholder="sdmqtt_…"
          />
        </label>
        <button type="submit" disabled={draft.value.trim() === ''}>
          Use it
        </button>
        {held ? (
          <button type="button" onClick={() => presentKey(null)}>
            Forget it
          </button>
        ) : null}
      </form>
      <p class="quiet" role="status">
        {held ? `This tab presents ${masked(held)}.` : 'This tab presents no key.'}
      </p>
    </section>
  );
}

// ---------------------------------------------------------------
// Users
// ---------------------------------------------------------------

function Users() {
  const again = useSignal(0);
  const users = useResource<UserView>('users', again);
  const { outcome, busy, run } = useChange(again);
  const name = useSignal('');
  const password = useSignal('');
  const rows = users.value?.rows ?? [];
  return (
    <section>
      <h2>Users</h2>
      <p class="hint">
        A user connects with a password, or with SCRAM-SHA-256 on version 5. The password is
        hashed when it is set and never shown again; setting it for a name that exists changes it.
      </p>
      <form
        class="fields"
        onSubmit={(event) => {
          event.preventDefault();
          const who = name.value.trim();
          void run(`${who} can connect.`, () => putUser(who, password.value)).then((done) => {
            if (done) {
              name.value = '';
              password.value = '';
            }
          });
        }}
      >
        <label>
          Name
          <input
            type="text"
            autocomplete="off"
            value={name.value}
            onInput={(event) => (name.value = event.currentTarget.value)}
          />
        </label>
        <label>
          Password
          <input
            type="password"
            autocomplete="new-password"
            value={password.value}
            onInput={(event) => (password.value = event.currentTarget.value)}
          />
        </label>
        <button
          type="submit"
          disabled={busy.value || name.value.trim() === '' || password.value === ''}
        >
          {busy.value
            ? 'Hashing…'
            : rows.some((user) => user.name === name.value.trim())
              ? 'Change the password'
              : 'Add'}
        </button>
      </form>
      <Outcome outcome={outcome} />
      <Unread held={users.value} />
      {rows.length > 0 ? (
        <div class="table settings-table settings-users">
          <div class="row head">
            <span>Name</span>
            <span>Password</span>
            <span>SCRAM</span>
            <span>Created</span>
            <span />
          </div>
          {rows.map((user) => (
            <div class="row" key={user.name}>
              <span class="mono">{user.name}</span>
              <span>{user.has_password ? 'yes' : 'no'}</span>
              <span>{user.scram ? 'yes' : 'no'}</span>
              <span>{when(user.created_at)}</span>
              <span class="doing">
                <button
                  type="button"
                  disabled={busy.value}
                  onClick={() => {
                    if (window.confirm(`Remove ${user.name}? Their connections are refused from now on, on every node.`)) {
                      void run(`${user.name} is gone.`, () => deleteUser(user.name));
                    }
                  }}
                >
                  Remove
                </button>
              </span>
            </div>
          ))}
        </div>
      ) : users.value && !users.value.failure ? (
        <p class="empty">No users. Who may connect is then the anonymous switch alone.</p>
      ) : null}
    </section>
  );
}

// ---------------------------------------------------------------
// Access control
// ---------------------------------------------------------------

/** The rule form as typed. */
interface RuleDraft {
  id: string;
  order: string;
  subject: string;
  filter: string;
  publish: Opinion;
  subscribe: Opinion;
}

const EMPTY_RULE: RuleDraft = {
  id: '',
  order: '100',
  subject: 'anyone',
  filter: '',
  publish: '',
  subscribe: '',
};

/** The draft as the API reads it. */
function ruleOf(draft: RuleDraft): AclRule {
  const rule: AclRule = {
    id: draft.id.trim(),
    order: draft.order.trim() === '' ? Number.NaN : Number(draft.order),
    subject: draft.subject.trim(),
    filter: draft.filter.trim(),
  };
  const publish = opinionValue(draft.publish);
  const subscribe = opinionValue(draft.subscribe);
  if (publish !== undefined) {
    rule.publish = publish;
  }
  if (subscribe !== undefined) {
    rule.subscribe = subscribe;
  }
  return rule;
}

/** An opinion as a cell. */
function opinion(value: boolean | undefined): string {
  return value === undefined ? '—' : value ? 'allow' : 'deny';
}

function AccessControl() {
  const again = useSignal(0);
  const rules = useResource<AclRule>('acl', again);
  const { outcome, busy, run } = useChange(again);
  const draft = useSignal<RuleDraft>(EMPTY_RULE);
  const problems = useSignal<Problems>({});
  const set = (field: keyof RuleDraft, value: string) => {
    draft.value = { ...draft.value, [field]: value };
  };
  const rows = rules.value?.rows ?? [];
  const field = (label: string, name: keyof RuleDraft, hint?: string) => (
    <label>
      {label}
      <input
        type="text"
        value={draft.value[name]}
        aria-invalid={problems.value[name as keyof Problems] ? 'true' : undefined}
        onInput={(event) => set(name, event.currentTarget.value)}
      />
      {hint ? <span class="hint">{hint}</span> : null}
      {problems.value[name as keyof Problems] ? (
        <span class="problem">{problems.value[name as keyof Problems]}</span>
      ) : null}
    </label>
  );
  const choice = (label: string, name: 'publish' | 'subscribe') => (
    <label>
      {label}
      <select value={draft.value[name]} onChange={(event) => set(name, event.currentTarget.value)}>
        <option value="">no opinion</option>
        <option value="allow">allow</option>
        <option value="deny">deny</option>
      </select>
      <span class="hint">No opinion asks the next rule.</span>
    </label>
  );
  return (
    <section>
      <h2>Access control</h2>
      <p class="hint">
        With no rule, everything is allowed. <strong>The first rule makes everything else
        denied</strong>, so a narrow deny needs a broad allow after it. Rules are consulted by
        order, then identifier, and the first that matches and has an opinion decides. Rules
        written in the configuration file as <span class="mono">[[acl]]</span> are consulted
        with these but are not listed here.
      </p>
      <form
        class="fields"
        onSubmit={(event) => {
          event.preventDefault();
          const rule = ruleOf(draft.value);
          const found = ruleProblems(rule);
          problems.value = found;
          if (Object.keys(found).length > 0) {
            return;
          }
          void run(`${rule.id} is in force on every node.`, () => putAcl(rule)).then((done) => {
            if (done) {
              draft.value = EMPTY_RULE;
            }
          });
        }}
      >
        {field('Identifier', 'id', 'The same one again replaces it.')}
        {field('Order', 'order', 'Lower first.')}
        {field('Subject', 'subject', 'anyone, user:<name> or client:<id>')}
        {field('Topic filter', 'filter', 'Wildcards as in a subscription.')}
        {choice('Publish', 'publish')}
        {choice('Subscribe', 'subscribe')}
        <button type="submit" disabled={busy.value}>
          {rows.some((rule) => rule.id === draft.value.id.trim()) ? 'Replace' : 'Add'}
        </button>
      </form>
      {problems.value.opinion ? <p class="problem">{problems.value.opinion}</p> : null}
      <Outcome outcome={outcome} />
      <Unread held={rules.value} />
      {rows.length > 0 ? (
        <div class="table settings-table settings-acl">
          <div class="row head">
            <span>Order</span>
            <span>Identifier</span>
            <span>Subject</span>
            <span>Filter</span>
            <span>Publish</span>
            <span>Subscribe</span>
            <span>Reads as</span>
            <span />
          </div>
          {rows.map((rule) => (
            <div class="row" key={rule.id}>
              <span class="mono">{rule.order}</span>
              <span class="mono">{rule.id}</span>
              <span class="mono">{rule.subject}</span>
              <span class="mono">{rule.filter}</span>
              <span>{opinion(rule.publish)}</span>
              <span>{opinion(rule.subscribe)}</span>
              <span title={describeRule(rule)}>{describeRule(rule)}</span>
              <span class="doing">
                <button
                  type="button"
                  disabled={busy.value}
                  onClick={() => {
                    if (window.confirm(`Remove the rule ${rule.id}, on every node?`)) {
                      void run(`${rule.id} is gone.`, () => deleteAcl(rule.id));
                    }
                  }}
                >
                  Remove
                </button>
              </span>
            </div>
          ))}
        </div>
      ) : rules.value && !rules.value.failure ? (
        <p class="empty">No rules through the API.</p>
      ) : null}
    </section>
  );
}

// ---------------------------------------------------------------
// API keys
// ---------------------------------------------------------------

/** What each scope allows, in words. */
const SCOPES: Record<Scope, string> = {
  read: 'open taps',
  write: 'also publish and write rules',
  admin: 'also manage users, access control, keys and nodes',
};

function Keys() {
  const again = useSignal(0);
  const keys = useResource<ApiKeyView>('keys', again);
  const { outcome, busy, run } = useChange(again);
  const name = useSignal('');
  const scope = useSignal<Scope>('read');
  const issued = useSignal<IssuedKey | null>(null);
  const rows = keys.value?.rows ?? [];
  return (
    <section>
      <h2>API keys</h2>
      <p class="hint">
        A key is shown once, when it is made: the broker keeps only a digest. Revoking one takes
        effect on every node.
      </p>
      <form
        class="fields"
        onSubmit={(event) => {
          event.preventDefault();
          const label = name.value.trim();
          void run(`${label} was made.`, async () => {
            issued.value = await createKey(label, scope.value);
          }).then((done) => {
            if (done) {
              name.value = '';
            }
          });
        }}
      >
        <label>
          Name
          <input
            type="text"
            autocomplete="off"
            value={name.value}
            onInput={(event) => (name.value = event.currentTarget.value)}
          />
        </label>
        <label>
          Scope
          <select
            value={scope.value}
            onChange={(event) => (scope.value = event.currentTarget.value as Scope)}
          >
            {(Object.keys(SCOPES) as Scope[]).map((each) => (
              <option key={each} value={each}>
                {each}: {SCOPES[each]}
              </option>
            ))}
          </select>
        </label>
        <button type="submit" disabled={busy.value || name.value.trim() === ''}>
          Make a key
        </button>
      </form>
      <Outcome outcome={outcome} />
      {issued.value ? (
        <div class="notice issued" role="status">
          <p>
            <strong>Copy this key now.</strong> It will not be shown again.
          </p>
          <p class="mono issued-key">{issued.value.key}</p>
          <div class="controls">
            <button
              type="button"
              onClick={() => void navigator.clipboard?.writeText(issued.value?.key ?? '')}
            >
              Copy
            </button>
            <button type="button" onClick={() => presentKey(issued.value?.key ?? null)}>
              Use it in this tab
            </button>
            <button type="button" onClick={() => (issued.value = null)}>
              I have it
            </button>
          </div>
        </div>
      ) : null}
      <Unread held={keys.value} />
      {rows.length > 0 ? (
        <div class="table settings-table settings-keys">
          <div class="row head">
            <span>Identifier</span>
            <span>Name</span>
            <span>Scope</span>
            <span>Created</span>
            <span />
          </div>
          {rows.map((each) => (
            <div class="row" key={each.id}>
              <span class="mono">{each.id}</span>
              <span>{each.name}</span>
              <span title={SCOPES[each.scope]}>{each.scope}</span>
              <span>{when(each.created_at)}</span>
              <span class="doing">
                <button
                  type="button"
                  disabled={busy.value}
                  onClick={() => {
                    if (window.confirm(`Revoke ${each.name} (${each.id})? Anything using it is refused from now on.`)) {
                      void run(`${each.name} is revoked.`, async () => {
                        await revokeKey(each.id);
                        // The one-time panel must not go on offering
                        // a key that no longer works.
                        if (issued.value?.id === each.id) {
                          issued.value = null;
                        }
                      });
                    }
                  }}
                >
                  Revoke
                </button>
              </span>
            </div>
          ))}
        </div>
      ) : keys.value && !keys.value.failure ? (
        <p class="empty">No keys. The configured token is the only credential.</p>
      ) : null}
    </section>
  );
}

// ---------------------------------------------------------------
// Listeners
// ---------------------------------------------------------------

/** A protocol as an operator says it. */
const PROTOCOL: Record<ListenerView['protocol'], string> = {
  mqtt: 'MQTT',
  mqtt_tls: 'MQTT over TLS',
  mqtt_ws: 'MQTT over WebSocket',
  mqtt_wss: 'MQTT over secure WebSocket',
  admin: 'Admin API',
  ingest: 'HTTP ingest',
  cluster: 'Cluster link',
};

/** A cell that only means something on an MQTT listener. */
function cell(value: number | boolean | string | null): string {
  if (value === null) {
    return '—';
  }
  return typeof value === 'boolean' ? (value ? 'yes' : 'no') : String(value);
}

/** The publish cap, in words. */
function cap(listener: ListenerView): string {
  if (listener.max_publish_rate === null) {
    return '—';
  }
  if (listener.max_publish_rate === 0) {
    return 'none';
  }
  const burst = Math.max(listener.publish_burst ?? 0, listener.max_publish_rate);
  return `${listener.max_publish_rate}/s, burst ${burst}`;
}

function Listeners() {
  const again = useSignal(0);
  const listeners = useResource<ListenerView>('listeners', again);
  const rows = listeners.value?.rows ?? [];
  return (
    <section>
      <h2>Listeners</h2>
      <p class="hint">
        What this node bound, from its configuration file. Listeners are not replicated and are
        changed in the file, with a restart.
      </p>
      <Unread held={listeners.value} />
      {rows.length > 0 ? (
        <div class="table settings-table settings-listeners">
          <div class="row head">
            <span>Name</span>
            <span>Serves</span>
            <span>Address</span>
            <span>Mountpoint</span>
            <span>Max QoS</span>
            <span>Retain</span>
            <span>Durability</span>
            <span>Publish cap</span>
          </div>
          {rows.map((listener) => (
            <div class="row" key={listener.name}>
              <span class="mono">{listener.name}</span>
              <span>{PROTOCOL[listener.protocol] ?? listener.protocol}</span>
              <span class="mono">{listener.address}</span>
              <span class="mono">{listener.mountpoint === '' ? '—' : listener.mountpoint}</span>
              <span>{cell(listener.max_qos)}</span>
              <span>{cell(listener.retain_available)}</span>
              <span>{cell(listener.durability)}</span>
              <span>{cap(listener)}</span>
            </div>
          ))}
        </div>
      ) : null}
    </section>
  );
}

/** The page. */
export function Settings() {
  return (
    <section class="page settings">
      <h1>Settings</h1>
      <Credential />
      <Users />
      <AccessControl />
      <Keys />
      <Listeners />
    </section>
  );
}
