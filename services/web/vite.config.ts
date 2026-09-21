import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';

import license, { type Dependency } from 'rollup-plugin-license';
import { defineConfig, type Plugin } from 'vite';

/**
 * The interface's build.
 *
 * Two things happen here. The bundle is produced, and the notices for
 * everything in it are generated from the bundle rather than from the
 * package list. The output is a directory the broker's build script
 * precompresses and embeds; nothing here compresses anything, because
 * section 11.2 puts that in the build script that already embeds the
 * bundle: one fewer npm dependency and one fewer compatibility risk
 * with a bundler that recently changed the way it bundles.
 */

/** Where the notices are written, relative to this file. */
const NOTICES = 'THIRD-PARTY-NOTICES.txt';

/**
 * A package we know is in the bundle.
 *
 * The build asserts the notices name it. `rollup-plugin-license`'s
 * declared peer range names Rollup and not Vite, so its compatibility
 * with this bundler is empirical: the version is pinned exactly and
 * this assertion is what would notice the day it silently stopped
 * reporting anything.
 */
const KNOWN_IN_BUNDLE = 'preact';

/**
 * The licences a distributed dependency may carry.
 *
 * The same policy as `deny.toml` applies to the Rust side, minus the
 * identifiers that only ever appear on a crate. Section "License
 * policy" of docs/research/component-selection.md: the project's own
 * code is MIT and third-party code is MIT-compatible permissive,
 * which is a broader statement and not the same one.
 */
const ALLOWED = new Set([
  '0BSD',
  'Apache-2.0',
  'BSD-2-Clause',
  'BSD-3-Clause',
  'CC0-1.0',
  'ISC',
  'MIT',
  'Unlicense',
  'Zlib',
]);

/**
 * Whether an SPDX expression is allowed.
 *
 * Deliberately small: a disjunction passes when any branch is
 * allowed, and anything else must have every identifier allowed. That
 * is stricter than a real SPDX evaluator on expressions this project
 * does not have, and a dependency whose expression it cannot read
 * fails the build rather than passing it.
 */
function licenceIsAllowed(expression: string | null): boolean {
  if (!expression) {
    return false;
  }
  const atoms = expression
    .replace(/[()]/g, ' ')
    .split(/\s+(?:OR|AND)\s+/i)
    .map((atom) => atom.trim())
    .filter((atom) => atom.length > 0);
  if (atoms.length === 0) {
    return false;
  }
  return /\sOR\s/i.test(expression)
    ? atoms.some((atom) => ALLOWED.has(atom))
    : atoms.every((atom) => ALLOWED.has(atom));
}

/** One thing that ships and has to be named. */
interface Notice {
  /** What it is called. */
  name: string;
  /** Which release of it is in the bundle. */
  version: string;
  /** Its SPDX expression. */
  licence: string;
  /** Who holds the copyright, when the manifest says. */
  author: string | null;
  /** The licence text itself, when the package ships one. */
  text: string | null;
  /** Why this is in the notices, for a reader of the file. */
  why: string;
}

/** Read a package's licence file, whatever it is called. */
function licenceTextIn(directory: string): string | null {
  const named = readdirSync(directory).find((entry) =>
    /^licen[sc]e(\.(md|txt))?$/i.test(entry),
  );
  return named ? readFileSync(join(directory, named), 'utf8').trim() : null;
}

/** What `rollup-plugin-license` found, in this file's shape. */
function fromPlugin(dependency: Dependency): Notice {
  return {
    name: dependency.name ?? '(unnamed)',
    version: dependency.version ?? '(unversioned)',
    licence: dependency.license ?? '(none declared)',
    author: dependency.author?.text() ?? null,
    text: dependency.licenseText?.trim() ?? null,
    why: 'JavaScript kept in the bundle after tree-shaking',
  };
}

// ---------------------------------------------------------------
// The two things the plugin cannot see
// ---------------------------------------------------------------

/**
 * Find the package a bare specifier resolves to.
 *
 * Resolved by walking `node_modules` rather than with Node's own
 * resolver, because a stylesheet's `@import` is resolved by the
 * bundler's CSS resolver and not by Node's: Open Props does not list
 * its stylesheets in its `exports` map, and `require.resolve` would
 * refuse the very import that is in the bundle.
 */
function packageDirectoryFor(specifier: string, importer: string): string | null {
  const segments = specifier.split('/');
  const name = specifier.startsWith('@') ? segments.slice(0, 2).join('/') : segments[0];
  let directory = dirname(importer);
  for (;;) {
    const candidate = join(directory, 'node_modules', name);
    if (existsSync(join(candidate, 'package.json'))) {
      return candidate;
    }
    const parent = dirname(directory);
    if (parent === directory) {
      return null;
    }
    directory = parent;
  }
}

/**
 * Every package whose stylesheet reached the bundle.
 *
 * **`rollup-plugin-license` cannot see these.** It reads the modules
 * of a JavaScript chunk, and a stylesheet pulled in by an `@import`
 * is inlined by the bundler's CSS pipeline before that: Open Props'
 * custom properties are in the shipped CSS and its name would be in
 * no chunk. So the import graph is walked here, from the stylesheets
 * the bundle does name, following relative imports through files and
 * bare imports into packages.
 */
function cssPackages(seeds: string[]): Map<string, Notice> {
  const found = new Map<string, Notice>();
  const seen = new Set<string>();
  const queue = [...seeds];
  while (queue.length > 0) {
    const file = queue.pop() as string;
    if (seen.has(file) || !existsSync(file)) {
      continue;
    }
    seen.add(file);
    const source = readFileSync(file, 'utf8');
    const imports = source.matchAll(/@import\s+(?:url\(\s*)?["']([^"']+)["']/g);
    for (const [, raw] of imports) {
      const specifier = raw.split('?')[0].split('#')[0];
      if (/^(?:https?:)?\/\//.test(specifier) || specifier.startsWith('data:')) {
        continue;
      }
      if (specifier.startsWith('.') || isAbsolute(specifier)) {
        queue.push(resolve(dirname(file), specifier));
        continue;
      }
      const directory = packageDirectoryFor(specifier, file);
      if (!directory) {
        throw new Error(
          `the stylesheet ${file} imports ${specifier}, and no package by that ` +
            'name could be found to attribute it to',
        );
      }
      const manifest = JSON.parse(
        readFileSync(join(directory, 'package.json'), 'utf8'),
      ) as { name?: string; version?: string; license?: string; author?: unknown };
      const name = manifest.name ?? specifier;
      if (!found.has(name)) {
        found.set(name, {
          name,
          version: manifest.version ?? '(unversioned)',
          licence: manifest.license ?? '(none declared)',
          author:
            typeof manifest.author === 'string'
              ? manifest.author
              : ((manifest.author as { name?: string } | undefined)?.name ?? null),
          text: licenceTextIn(directory),
          why: 'CSS inlined into the bundle by an @import',
        });
      }
      const subpath = specifier.split('/').slice(name.split('/').length).join('/');
      if (subpath) {
        queue.push(join(directory, subpath));
      }
    }
  }
  return found;
}

/**
 * The bundler's own module preload helper.
 *
 * It ships and it has no package to attribute: it is a virtual
 * module, and a virtual module has no manifest. This emits Vite's own
 * notice when the helper is in the bundle, **and only then**.
 *
 * Only the copyright and the MIT text of Vite's core are copied.
 * Vite's licence file is two and a half thousand lines, nearly all of
 * it covering Node-side tooling that is never distributed, and
 * copying it wholesale would misrepresent what ships.
 */
function viteHelperNotice(root: string): Notice {
  const directory = join(root, 'node_modules', 'vite');
  const manifest = JSON.parse(readFileSync(join(directory, 'package.json'), 'utf8')) as {
    version: string;
    license: string;
  };
  const whole = readFileSync(join(directory, 'LICENSE.md'), 'utf8');
  const start = whole.indexOf('MIT License');
  const end = whole.indexOf('\n# ', start);
  if (start < 0 || end < 0) {
    throw new Error(
      "vite's LICENSE.md no longer starts with its own MIT licence followed by a " +
        'heading, so the core notice cannot be extracted from it; check what ' +
        'changed before trusting the notices',
    );
  }
  return {
    name: 'vite (module preload helper)',
    version: manifest.version,
    licence: manifest.license,
    author: null,
    text: whole.slice(start, end).trim(),
    why: 'the runtime helper the bundler injects into the bundle; a virtual module, so it has no package of its own',
  };
}

// ---------------------------------------------------------------
// The file
// ---------------------------------------------------------------

/** The notices file, from everything that ships. */
function render(notices: Notice[]): string {
  const head = [
    'Third-party notices for the sd-mqtt browser interface',
    '=====================================================',
    '',
    'Generated by the build from the modules the bundler kept after',
    'tree-shaking, plus the stylesheets it inlined and the runtime helper',
    'it injects. It is not a list of installed packages: a dependency that',
    'is installed and never imported does not ship and is not named here.',
    '',
    'Build tooling is absent on purpose. Running a tool to produce an',
    'artifact does not distribute the tool, so it carries no attribution',
    'obligation. What is below is what a user receives.',
    '',
    'Regenerated by `npm run build`. CI fails on any difference between',
    'the regenerated file and this one.',
    '',
  ];
  const blocks = notices.map((notice) => {
    const lines = [
      '-'.repeat(70),
      `${notice.name} ${notice.version}`,
      `License: ${notice.licence}`,
    ];
    if (notice.author) {
      lines.push(`Author: ${notice.author}`);
    }
    lines.push(`Included as: ${notice.why}`, '');
    lines.push(notice.text ?? '(this package ships no licence file)');
    lines.push('');
    return lines.join('\n');
  });
  return [...head, ...blocks].join('\n');
}

/**
 * The half of the attribution the licence plugin cannot do, and the
 * assertions that say the half it does is still working.
 */
function attribution(root: string, bundled: Dependency[]): Plugin {
  let helperShips = false;
  let stylesheets: string[] = [];
  return {
    name: 'sd-mqtt:attribution',
    apply: 'build',
    // After everything, so the chunks read here are the chunks that
    // are written.
    enforce: 'post',
    generateBundle(_options, bundle) {
      helperShips = false;
      stylesheets = [];
      for (const output of Object.values(bundle)) {
        if (output.type !== 'chunk' || !output.modules) {
          continue;
        }
        for (const [id, module] of Object.entries(output.modules)) {
          // The helper is a virtual module of the bundler's own, and
          // it counts only when something of it was actually
          // rendered into the chunk.
          if (id.includes('modulepreload-polyfill') && module.renderedLength > 0) {
            helperShips = true;
          }
          if (/\.(css|scss|sass|less|styl)(\?|$)/.test(id) && isAbsolute(id.split('?')[0])) {
            stylesheets.push(id.split('?')[0]);
          }
        }
      }
    },
    // `writeBundle` rather than `generateBundle`, so that this runs
    // after the licence plugin's own `generateBundle` has handed over
    // what it found whatever order the two are registered in.
    writeBundle() {
      const notices = new Map<string, Notice>();
      for (const dependency of bundled) {
        const notice = fromPlugin(dependency);
        notices.set(notice.name, notice);
      }
      for (const [name, notice] of cssPackages(stylesheets)) {
        // The licence plugin enforces the allow list on what it
        // found; these it never saw, so the same rule is applied
        // here rather than left to a reader of the file.
        if (!licenceIsAllowed(notice.licence)) {
          this.error(
            `${name}@${notice.version} ships in the bundle under "${notice.licence}", ` +
              'which is not in the allow list in vite.config.ts',
          );
        }
        notices.set(name, notice);
      }
      if (helperShips) {
        const notice = viteHelperNotice(root);
        notices.set(notice.name, notice);
      }

      const ordered = [...notices.values()].sort((left, right) =>
        left.name.localeCompare(right.name),
      );
      // **The empirical part.** The plugin's declared peer range does
      // not name this bundler, so "it produced a file" is not enough:
      // a file with nothing in it is what a plugin that has stopped
      // seeing the module graph produces, and it would look like a
      // build with no dependencies.
      if (ordered.length === 0) {
        this.error(
          'the notices are empty. The bundle is not empty, so the licence plugin ' +
            'has stopped reading the module graph; check it against this version ' +
            'of the bundler before shipping.',
        );
      }
      if (!ordered.some((notice) => notice.name === KNOWN_IN_BUNDLE)) {
        this.error(
          `the notices do not name ${KNOWN_IN_BUNDLE}, which is in the bundle. ` +
            'The licence plugin is reporting the wrong set; check it against this ' +
            'version of the bundler before shipping.',
        );
      }
      writeFileSync(join(root, NOTICES), render(ordered), 'utf8');
    },
  };
}

/**
 * Where the broker's admin listener is, in development.
 *
 * `sdmqtt-admin`'s own default port. Overridable because a second
 * broker on a second port is a normal thing to be running.
 */
const BROKER = process.env.SDMQTT_ADMIN ?? 'http://127.0.0.1:1884';

/**
 * What the development server hands to the broker.
 *
 * **This direction, and not the other.** An earlier draft had the
 * broker proxying to the development server, which means a streaming
 * reverse proxy with WebSocket upgrade forwarding living in the
 * shipped binary for the benefit of development. This is the same
 * thing in a few lines of somebody else's configuration.
 *
 * The hot-reload socket is left alone: it is served by this server at
 * its own path and must not be forwarded anywhere.
 */
const PROXIED = {
  // The API, and the events stream with it.
  '/api': BROKER,
  // Liveness, readiness and the cluster view.
  '/health': BROKER,
  // The exposition endpoint.
  '/metrics': BROKER,
};

export default defineConfig(({ command }) => {
  const root = import.meta.dirname;
  // Filled by the licence plugin's output generator during
  // `generateBundle`, read by `attribution` during `writeBundle`.
  const bundled: Dependency[] = [];
  return {
    server: {
      proxy: Object.fromEntries(
        Object.entries(PROXIED).map(([prefix, target]) => [
          prefix,
          {
            target,
            // The broker is on loopback and has no virtual hosts, so
            // rewriting Host would only make its logs harder to read.
            changeOrigin: false,
            // **The events stream must arrive event by event.** A
            // proxy that buffers turns a live interface into one
            // that updates in clumps, which is miserable to
            // diagnose months later. Nothing here compresses or
            // collects the body, and the broker sends
            // `X-Accel-Buffering: no` for the intermediaries that
            // would.
            ws: false,
            timeout: 0,
            proxyTimeout: 0,
          },
        ]),
      ),
    },
    build: {
      // The default, named so that the build script embedding it and
      // the budget script weighing it are both pointing at something
      // this file actually promises.
      outDir: 'dist',
      // The broker serves the bundle from the root of its admin
      // listener and never from a subdirectory, so relative asset
      // URLs would work and absolute ones are clearer in a log.
      assetsDir: 'assets',
      sourcemap: false,
      rolldownOptions: {
        plugins: [
          // **Only on build, and after tree-shaking.** It is
          // registered here rather than as a Vite plugin so that a
          // development server never runs it, and it reads
          // `chunk.modules`, which is what survived.
          //
          // The cast is because the plugin is typed against Rollup
          // and this bundler is Rolldown. That is the compatibility
          // the research calls empirical: the version is pinned
          // exactly, and the assertions in `attribution` are what
          // would catch it going quiet.
          license({
            thirdParty: {
              includePrivate: false,
              includeSelf: false,
              allow: {
                test: (dependency) => licenceIsAllowed(dependency.license),
                // A package with no licence at all fails the build,
                // and so does one outside the list.
                failOnUnlicensed: true,
                failOnViolation: true,
              },
              output: (dependencies) => {
                bundled.length = 0;
                bundled.push(...dependencies);
              },
            },
          }) as unknown as Plugin,
        ],
      },
    },
    plugins: command === 'build' ? [attribution(root, bundled)] : [],
  };
});
