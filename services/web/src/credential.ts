/**
 * The API key this tab presents. Section 11.1.
 *
 * Mutating requests need a key whose scope covers them. The broker
 * mints keys and shows each once; an operator pastes one here, and
 * every request from this tab carries it. The UI session cookie the
 * spec allows for is not built: a pasted key is one credential the
 * operator already has, rather than a second kind the broker would
 * have to issue, expire and revoke.
 *
 * **Held for the tab's life, in session storage**, so a reload keeps
 * it and closing the tab forgets it. Not local storage, which would
 * leave an admin key on the disk of whatever machine the page was
 * opened on. Storage that throws — a private window, blocked site
 * data — leaves the key in memory for this page only, which is still
 * correct, just shorter.
 */
import { signal } from '@preact/signals';

const STORED = 'sdmqtt.key';

function read(): string | null {
  try {
    return window.sessionStorage.getItem(STORED);
  } catch {
    return null;
  }
}

/** The key, or `null` for none: a loopback listener with no token needs none. */
export const key = signal<string | null>(read());

/** Present `value` from now on, or nothing when it is blank. */
export function presentKey(value: string | null): void {
  const trimmed = value?.trim() ?? '';
  key.value = trimmed === '' ? null : trimmed;
  try {
    if (key.value === null) {
      window.sessionStorage.removeItem(STORED);
    } else {
      window.sessionStorage.setItem(STORED, key.value);
    }
  } catch {
    // Held in memory for this page; see the module notes.
  }
}
