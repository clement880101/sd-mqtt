//! Precompress the built interface and embed it.
//!
//! Section 11.2: the production bundle is precompressed at build time
//! and embedded into the binary, served from three blobs. Compressing
//! here rather than in a bundler plugin is one fewer npm dependency
//! and one fewer compatibility risk with a bundler that recently
//! changed the way it bundles.
//!
//! # A build of the binary must not require Node
//!
//! Also section 11.2, and the reason this file has two paths. With no
//! `services/web/dist` present, the crate still compiles and the
//! listener still serves a page: one that says the interface was not
//! built and what to run. Every Rust-only contributor and the whole
//! test matrix takes that path, and nobody would notice it breaking,
//! so it is a path this build produces deliberately rather than an
//! error it reports.
//!
//! # The third-party notices are embedded here too
//!
//! Section 11.2 says the interface's attribution is generated from
//! the bundle; it said nothing about the attribution reaching anyone,
//! and it did not. The generated file sat one directory above `dist`
//! and this script embeds only what is inside `dist`, so a user of
//! the binary received preact, uPlot, valibot, the virtual core and
//! Open Props with none of their notices. The Rust half did not exist
//! at all, and this branch is the first to link Apache-2.0-only code
//! — `sync_wrapper`, and `ryu` whose only allowed branch is
//! Apache-2.0 — whose licence text has to travel with the work.
//!
//! So both halves are read here and embedded as one document. This is
//! the crate that puts documents into the binary; it is not the crate
//! that owns the Rust dependency graph, but it is the only one with
//! the machinery, and one document is what a reader wants.
//!
//! The browser half is embedded **only when the interface is**. A
//! binary with no bundle in it ships none of those libraries, and
//! attributing code that is not there would be as untrue in the other
//! direction.
//!
//! # What it writes
//!
//! Three blobs per document into `OUT_DIR` — identity, gzip, brotli —
//! and a source file naming them, which `src/assets.rs` includes.
//! Each blob carries its own entity tag, computed over the bytes that
//! are actually sent: the same document in two encodings is two
//! representations, and one tag across both makes a conditional
//! request answer for the wrong one.

use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets it"));
    // crates/admin -> crates -> services/broker -> services
    let services = manifest
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the crate is inside the repository");
    let dist = services.join("web").join("dist");

    let out = PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets it"));
    let built = dist.join("index.html").is_file();

    // Both notices files, whether or not the interface is built: the
    // Rust half is always embedded, and the browser half being
    // created or removed is itself a change this has to notice.
    println!(
        "cargo:rerun-if-changed={}",
        rust_notices(&services).display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        web_notices(&services).display()
    );

    // **Watch something that exists.** Cargo treats a
    // `rerun-if-changed` path it cannot stat as changed, so naming
    // the output directory in a tree where the interface has never
    // been built re-runs this script — and recompiles this crate and
    // everything above it — on every single build. That is the tree
    // every Rust-only contributor works in, so it is the one that
    // must be fast.
    //
    // So: the output directory when there is one, and the service
    // directory around it when there is not. The second is coarser —
    // editing a `.tsx` file will rebuild this crate — but it only
    // applies until the first `npm run build`, and creating `dist`
    // is exactly the change it has to notice.
    if built {
        println!("cargo:rerun-if-changed={}", dist.display());
    } else {
        println!("cargo:rerun-if-changed={}", services.join("web").display());
    }

    let documents = if built {
        let mut found = Vec::new();
        collect(&dist, &dist, &mut found);
        found.sort_by(|left: &Document, right| left.path.cmp(&right.path));
        for document in &found {
            println!("cargo:rerun-if-changed={}", document.source.display());
        }
        found
    } else {
        Vec::new()
    };

    let mut generated = String::new();
    writeln!(
        generated,
        "/// Whether a built interface was embedded.\n\
         ///\n\
         /// False in a tree where `services/web/dist` was absent when this\n\
         /// crate was compiled. The listener still serves a document at the\n\
         /// root; it is [`PLACEHOLDER`], the one that says so.\n\
         pub const INTERFACE_BUILT: bool = {built};\n"
    )
    .expect("a string can be written to");

    // **Always embedded, built or not.** A document that only exists
    // in half of the builds is a document only half the builds can
    // test, and this is the one every Rust-only contributor sees.
    // Compressed and tagged by the same code as everything else, so
    // there is one way of serving a document rather than two.
    writeln!(
        generated,
        "/// What is served at the root when no interface was built.\n\
         pub(crate) static PLACEHOLDER: Asset = {};\n",
        asset(&out, "placeholder", "/index.html", placeholder().as_bytes())
    )
    .expect("a string can be written to");

    // The attribution, as one document in three encodings.
    //
    // `NOTICES` holds the only copy of the uncompressed bytes and the
    // asset borrows them, rather than each including the same file:
    // two `include_bytes!` of one path are two statics, and whether
    // the linker merges them is not something worth seventy-five
    // kilobytes of hope.
    let notices = notices(&services, built);
    writeln!(
        generated,
        "/// The third-party attribution for everything in this binary.\n\
         ///\n\
         /// Both halves: the crates compiled in, and — when one was\n\
         /// embedded — the libraries in the browser interface. Served at\n\
         /// [`NOTICES_PATH`] and printed by `sdmqtt licenses`.\n\
         pub const NOTICES: &str = include_str!({:?});\n",
        write_notices(&out, &notices)
    )
    .expect("a string can be written to");
    writeln!(
        generated,
        "/// [`NOTICES`] as a document, in the three encodings.\n\
         pub(crate) static NOTICES_ASSET: Asset = {};\n",
        notices_asset(&out, notices.as_bytes())
    )
    .expect("a string can be written to");

    writeln!(
        generated,
        "/// Every document the built interface is made of, path-sorted.\n\
         ///\n\
         /// Empty when there is no built interface; see [`PLACEHOLDER`].\n\
         pub(crate) static ASSETS: &[Asset] = &["
    )
    .expect("a string can be written to");
    for (index, document) in documents.iter().enumerate() {
        writeln!(
            generated,
            "    {},",
            asset(&out, &index.to_string(), &document.path, &document.bytes)
        )
        .expect("a string can be written to");
    }
    generated.push_str("];\n");

    fs::write(out.join("interface.rs"), generated).expect("the generated source is written");
}

/// Where the attribution for the compiled-in crates is.
///
/// Generated by `scripts/rust-notices.py` and committed, for the same
/// reason the browser half is: a file in the repository is one a
/// reader can see without building anything, and one CI can diff
/// against what the graph produces.
fn rust_notices(services: &Path) -> PathBuf {
    services.join("broker").join("THIRD-PARTY-NOTICES.txt")
}

/// Where the attribution for the bundled libraries is.
fn web_notices(services: &Path) -> PathBuf {
    services.join("web").join("THIRD-PARTY-NOTICES.txt")
}

/// The one document naming everything third-party in this binary.
///
/// # Both halves are required, and absence is an error
///
/// Not a warning and not an empty section. The failure this is
/// guarding against is precisely the one that already happened: an
/// attribution that was generated, was correct, and reached nobody.
/// A build that quietly produced a binary with no notices in it is
/// how that stays true, so a missing or empty file stops the build
/// and says which file and what to run.
fn notices(services: &Path, built: bool) -> String {
    let mut document = String::new();
    document.push_str(
        "Third-party notices for sd-mqtt\n\
         ===============================\n\
         \n\
         sd-mqtt is MIT licensed and its own licence is not this file. This is\n\
         the attribution for the third-party code inside this binary, which is\n\
         not all MIT and is never claimed to be.\n\
         \n\
         It is in two halves, produced by two different things from two\n\
         different kinds of artifact, and joined here because a user of the\n\
         binary receives both and should not have to know that.\n\
         \n\
         Also at /third-party-notices.txt on the admin listener.\n\
         \n\
         \n",
    );
    document.push_str(&required(&rust_notices(services)));
    document.push_str("\n\n");
    if built {
        document.push_str(&required(&web_notices(services)));
    } else {
        document.push_str(
            "Third-party notices for the sd-mqtt browser interface\n\
             =====================================================\n\
             \n\
             This binary was compiled without the browser interface, so none of\n\
             its libraries ship in it and none of them are attributed here. See\n\
             services/web/THIRD-PARTY-NOTICES.txt for what a binary built with\n\
             the interface carries.\n",
        );
    }
    document
}

/// Read a notices file, refusing to continue without it.
fn required(path: &Path) -> String {
    let text = fs::read_to_string(path).unwrap_or_else(|error| {
        panic!(
            "cannot read {}: {error}.\n\
             This file is the attribution that ships inside the binary, and a \
             build without it would distribute third-party code with no notice \
             at all. Regenerate it:\n  \
             scripts/rust-notices.py            (the crates)\n  \
             (cd services/web && npm run build) (the bundled libraries)",
            path.display()
        )
    });
    assert!(
        !text.trim().is_empty(),
        "{} is empty, so nothing would be attributed. See the note above it.",
        path.display()
    );
    text
}

/// Write the notices beside the generated source, and name the file.
fn write_notices(out: &Path, text: &str) -> String {
    let file = out.join("third-party-notices.txt");
    fs::write(&file, text).expect("the notices are written");
    file.to_string_lossy().into_owned()
}

/// The notices as an `Asset`, borrowing `NOTICES` for the identity form.
fn notices_asset(out: &Path, bytes: &[u8]) -> String {
    let gzipped = gzip(bytes);
    let brotlied = brotli(bytes);
    format!(
        "Asset {{\n        \
             path: NOTICES_PATH,\n        \
             content_type: \"text/plain; charset=utf-8\",\n        \
             cache_control: \"no-cache\",\n        \
             identity: Encoded {{ bytes: NOTICES.as_bytes(), etag: {identity_tag:?} }},\n        \
             gzip: Encoded {{ bytes: include_bytes!({gzip:?}), etag: {gzip_tag:?} }},\n        \
             brotli: Encoded {{ bytes: include_bytes!({brotli:?}), etag: {brotli_tag:?} }},\n    \
         }}",
        gzip = blob(out, "notices", "gzip", &gzipped),
        brotli = blob(out, "notices", "brotli", &brotlied),
        identity_tag = etag(bytes),
        gzip_tag = etag(&gzipped),
        brotli_tag = etag(&brotlied),
    )
}

/// One document on its way into the binary.
struct Document {
    /// Where it was read from, for `rerun-if-changed`.
    source: PathBuf,
    /// The path it is served at, with a leading slash.
    path: String,
    /// Its uncompressed bytes.
    bytes: Vec<u8>,
}

/// Every file under `directory`, as documents rooted at `root`.
fn collect(root: &Path, directory: &Path, found: &mut Vec<Document>) {
    let entries = fs::read_dir(directory).expect("the built interface can be read");
    for entry in entries {
        let entry = entry.expect("a directory entry can be read");
        let path = entry.path();
        if path.is_dir() {
            collect(root, &path, found);
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .expect("everything found is under the root");
        // Always forward slashes: this is a URL path, not a path on
        // the host this happened to be built on.
        let mut served = String::from("/");
        let mut first = true;
        for component in relative.components() {
            if !first {
                served.push('/');
            }
            first = false;
            served.push_str(&component.as_os_str().to_string_lossy());
        }
        found.push(Document {
            bytes: fs::read(&path).expect("a built file can be read"),
            source: path,
            path: served,
        });
    }
}

/// One `Asset` literal, with its three blobs written beside the
/// generated source.
fn asset(out: &Path, name: &str, path: &str, bytes: &[u8]) -> String {
    let gzipped = gzip(bytes);
    let brotlied = brotli(bytes);
    format!(
        "Asset {{\n        \
             path: {path:?},\n        \
             content_type: {content_type:?},\n        \
             cache_control: {cache_control:?},\n        \
             identity: Encoded {{ bytes: include_bytes!({identity:?}), etag: {identity_tag:?} }},\n        \
             gzip: Encoded {{ bytes: include_bytes!({gzip:?}), etag: {gzip_tag:?} }},\n        \
             brotli: Encoded {{ bytes: include_bytes!({brotli:?}), etag: {brotli_tag:?} }},\n    \
         }}",
        path = path,
        content_type = content_type(path),
        cache_control = cache_control(path),
        identity = blob(out, name, "identity", bytes),
        gzip = blob(out, name, "gzip", &gzipped),
        brotli = blob(out, name, "brotli", &brotlied),
        identity_tag = etag(bytes),
        gzip_tag = etag(&gzipped),
        brotli_tag = etag(&brotlied),
    )
}

/// Write one encoding's bytes beside the generated source.
///
/// The absolute path is what the generated `include_bytes!` names.
/// Relative would resolve against the including file's directory,
/// which is the same directory today and is one assumption fewer to
/// carry.
fn blob(out: &Path, name: &str, encoding: &str, bytes: &[u8]) -> String {
    let file = out.join(format!("asset-{name}.{encoding}"));
    fs::write(&file, bytes).expect("a blob is written");
    file.to_string_lossy().into_owned()
}

/// The gzip form, at the level a build can afford.
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    encoder.write_all(bytes).expect("a vector accepts writes");
    encoder.finish().expect("a vector accepts writes")
}

/// The brotli form, at the quality a build can afford.
///
/// Quality 11 and a 22-bit window, which is what a static asset
/// compressed once and served a million times is worth.
fn brotli(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    {
        let mut writer = brotli::CompressorWriter::new(&mut out, 4096, 11, 22);
        writer.write_all(bytes).expect("a vector accepts writes");
    }
    out
}

/// A strong entity tag over exactly these bytes.
///
/// FNV-1a over the content with the length mixed in. It identifies a
/// representation rather than authenticating it, and a hash function
/// chosen for that is one fewer dependency in a build script.
fn etag(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    format!("\"{:x}-{:016x}\"", bytes.len(), hash)
}

/// What a document of this name is.
fn content_type(path: &str) -> &'static str {
    let extension = path.rsplit_once('.').map(|(_, tail)| tail).unwrap_or("");
    match extension {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "webmanifest" => "application/manifest+json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "gif" => "image/gif",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// How long a document of this name may be kept.
///
/// The bundler puts a hash of the contents in the name of everything
/// under `assets`, so those names are immutable and the documents
/// behind them can be kept forever. Everything else — the entry
/// document above all — is revalidated, which its entity tag makes
/// cheap.
fn cache_control(path: &str) -> &'static str {
    if path.starts_with("/assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

/// The document served when there is no built interface.
fn placeholder() -> String {
    // Deliberately self-contained: no stylesheet, no script, nothing
    // to fetch. It is served by a binary that has no other documents
    // in it.
    "<!doctype html>\n\
     <html lang=\"en\">\n\
     <head>\n\
     <meta charset=\"utf-8\">\n\
     <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
     <title>sd-mqtt: the interface was not built</title>\n\
     <style>\n\
     :root { color-scheme: light dark; }\n\
     body { font: 16px/1.6 system-ui, sans-serif; margin: 0 auto; max-width: 42rem; padding: 3rem 1.5rem; }\n\
     code, pre { font-family: ui-monospace, monospace; }\n\
     pre { background: rgba(127, 127, 127, 0.15); padding: 0.75rem 1rem; overflow-x: auto; }\n\
     </style>\n\
     </head>\n\
     <body>\n\
     <h1>The interface was not built</h1>\n\
     <p>This broker was compiled without the browser interface, so there is\n\
     nothing to show here. That is a supported way to build it: a build of the\n\
     binary must not require Node.</p>\n\
     <p>To build the interface and embed it, from the root of the repository:</p>\n\
     <pre>cd services/web\nnpm ci\nnpm run build\ncd ../broker\ncargo build --release -p sdmqtt</pre>\n\
     <p>The admin API is unaffected and is serving normally on this listener.\n\
     Try <a href=\"/health/ready\">/health/ready</a>, <a href=\"/metrics\">/metrics</a>\n\
     or <a href=\"/api/v1/listeners\">/api/v1/listeners</a>.</p>\n\
     </body>\n\
     </html>\n"
        .to_owned()
}
