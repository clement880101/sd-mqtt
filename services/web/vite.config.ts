import { defineConfig } from 'vite';

/**
 * The interface's build.
 *
 * The output is a directory the broker's build script precompresses
 * and embeds; nothing here compresses anything. Section 11.2: doing
 * it in the build script that already embeds the bundle is one fewer
 * npm dependency and one fewer compatibility risk with a bundler
 * that recently changed the way it bundles.
 */
export default defineConfig({
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
  },
});
