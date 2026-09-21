/**
 * Where the interface starts.
 *
 * Nothing but mounting. The shell is `app.tsx`; keeping the entry
 * this small means the thing the bundler names as the entry chunk is
 * not also a place anybody edits.
 */
import { render } from 'preact';

import { App } from './app.js';
import './app.css';

const root = document.getElementById('app');
if (!root) {
  throw new Error('index.html must provide #app for the interface to mount into');
}
render(<App />, root);
