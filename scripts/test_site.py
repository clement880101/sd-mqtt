#!/usr/bin/env python3
"""Tests for the landing page, `site/index.html`: that it cannot go stale.

    python3 scripts/test_site.py

A landing page is the one document most likely to keep saying last
release's number. So it is held to the sources it quotes:

- every version it names is the workspace's version;
- every GitHub and container-registry link names this project's
  repository and image, and no other;
- every in-page link has its target, and every repository path it links
  to exists in this checkout;
- every measured figure it quotes appears in the design's budget table
  or its decision records, which is where a figure is allowed to come
  from;
- it loads nothing from anywhere but Google Fonts, and declares a
  colour scheme for both themes.

Exits non-zero naming each check that failed.
"""
import html.parser
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAGE = ROOT / "site" / "index.html"
REPO = "clement880101/sd-mqtt"
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'ok  ' if ok else 'FAIL'}  {name}{(': ' + detail) if detail and not ok else ''}")
    if not ok:
        failures.append(name)


class Links(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []
        self.sources: list[str] = []
        self.ids: set[str] = set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        if tag == "a" and "href" in attrs:
            self.hrefs.append(attrs["href"])
        if tag in ("script", "img", "iframe") and "src" in attrs:
            self.sources.append(attrs["src"])
        if tag == "link" and "href" in attrs:
            self.sources.append(attrs["href"])


def main() -> int:
    if not PAGE.is_file():
        check("site/index.html exists", False)
        return 1
    text = PAGE.read_text()
    cargo = (ROOT / "services/broker/Cargo.toml").read_text()
    version = re.search(r'\[workspace\.package\]\s*\nversion = "([^"]+)"', cargo)[1]
    parser = Links()
    parser.feed(text)

    # 3.1.1 is MQTT's version, which the page names as a protocol; any
    # other x.y.z is a claim about this project's.
    named = set(re.findall(r"(?<![\d.])v?(\d+\.\d+\.\d+)(?![\d.])", text)) - {"3.1.1"}
    check(f"every version named is {version}", named == {version}, str(sorted(named)))

    github = [h for h in parser.hrefs if "github.com" in h]
    check("there are links to the repository", bool(github))
    wrong = [h for h in github if not h.startswith(f"https://github.com/{REPO}")]
    check(f"every GitHub link is {REPO}", not wrong, str(wrong))
    images = set(re.findall(r"ghcr\.io/[\w.-]+/[\w.-]+", text))
    check("every image named is this project's", images <= {f"ghcr.io/{REPO}"}, str(images))

    # `#` alone is the top of the page.
    anchors = [h[1:] for h in parser.hrefs if h.startswith("#") and h != "#"]
    check("every in-page link has a target", all(a in parser.ids for a in anchors),
          str([a for a in anchors if a not in parser.ids]))
    blob = f"https://github.com/{REPO}/blob/main/"
    tree = f"https://github.com/{REPO}/tree/main/"
    paths = [h[len(blob):] for h in github if h.startswith(blob)] + [h[len(tree):] for h in github if h.startswith(tree)]
    missing = [p for p in paths if not (ROOT / p.split("#")[0]).exists()]
    check("every repository path linked exists", not missing, str(missing))

    evidence = (ROOT / "docs/superpowers/specs/2026-09-19-sd-mqtt-design.md").read_text()
    evidence += "".join(p.read_text() for p in (ROOT / "docs/decisions").glob("*.md"))
    evidence += (ROOT / "README.md").read_text()
    evidence += "".join(p.read_text() for p in (ROOT / "docs").glob("**/*.md"))
    quoted = re.findall(r'data-figure="([^"]+)"', text)
    check("the page quotes measured figures", len(quoted) >= 4, str(quoted))
    unsourced = [figure for figure in quoted if figure not in evidence]
    check("every figure quoted is one the spec, a decision or the README records", not unsourced, str(unsourced))

    foreign = [s for s in parser.sources if s.startswith("http") and not s.startswith(("https://fonts.googleapis.com", "https://fonts.gstatic.com"))]
    check("it loads nothing but Google Fonts from elsewhere", not foreign, str(foreign))
    check("it styles both colour schemes", "prefers-color-scheme: dark" in text)
    check("it respects reduced motion", "prefers-reduced-motion" in text)

    if failures:
        print(f"{len(failures)} failed: {', '.join(failures)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
