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
import json
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
        self.canonical: str | None = None
        self.meta: dict[str, str] = {}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        if tag == "a" and "href" in attrs:
            self.hrefs.append(attrs["href"])
        if tag in ("script", "img", "iframe") and "src" in attrs:
            self.sources.append(attrs["src"])
        # A canonical link names this page; it loads nothing.
        if tag == "link" and "href" in attrs and attrs.get("rel") != "canonical":
            self.sources.append(attrs["href"])
        if tag == "link" and attrs.get("rel") == "canonical":
            self.canonical = attrs.get("href")
        if tag == "meta" and "property" in attrs:
            self.meta[attrs["property"]] = attrs.get("content", "")
        if tag == "meta" and "name" in attrs:
            self.meta[attrs["name"]] = attrs.get("content", "")


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

    evidence = (ROOT / "docs/design.md").read_text()
    evidence += (ROOT / "README.md").read_text()
    evidence += "".join(p.read_text() for p in (ROOT / "docs").glob("**/*.md"))
    quoted = re.findall(r'data-figure="([^"]+)"', text)
    check("the page quotes measured figures", len(quoted) >= 4, str(quoted))
    unsourced = [figure for figure in quoted if figure not in evidence]
    check("every figure quoted is one the spec, a decision or the README records", not unsourced, str(unsourced))

    # The comparison with other brokers: every row cites a page, and
    # the sizes carry the day they were read, because both go stale.
    rows = re.findall(r"<tr(?: class=\"ours\")?><td>(.*?)</td>.*?</tr>", text, re.S)
    compared = [row for row in rows if row and not row.startswith("<")]
    check("the comparison names several brokers", len(compared) >= 4, str(compared))
    body = text[text.index("<tbody>"):text.index("</tbody>")] if "<tbody>" in text else ""
    uncited = [r for r in body.split("</tr>") if "<td" in r and "<a href=\"http" not in r]
    check("every compared broker cites a source", not uncited, str(uncited)[:200])
    check("the comparison says when it was read", bool(re.search(r"read on \d{1,2} \w+ 20\d\d", text)))

    # What search engines and assistants read: the page names its own
    # address, a card to show when it is shared, structured data that
    # agrees with what the page says, and a sitemap and llms.txt beside it.
    site = "https://clement880101.github.io/sd-mqtt/"
    check("the page names its canonical address", parser.canonical == site, str(parser.canonical))
    title = re.search(r"<title>(.*?)</title>", text, re.S)[1]
    check("the title fits a search result", len(title) <= 60, f"{len(title)}: {title}")
    description = parser.meta.get("description", "")
    check("the description fits a search result", 50 <= len(description) <= 160, str(len(description)))
    card = parser.meta.get("og:image", "")
    check("the share card is this site's and exists",
          card.startswith(site) and (ROOT / "site" / card[len(site):]).is_file(), card)
    check("the share card is set for every reader",
          all(parser.meta.get(k) for k in ("og:title", "og:description", "og:url", "twitter:card", "twitter:image")))
    blocks = re.findall(r'<script type="application/ld\+json">(.*?)</script>', text, re.S)
    check("there is one block of structured data", len(blocks) == 1, str(len(blocks)))
    graph = {node["@type"]: node for node in json.loads(blocks[0])["@graph"]} if blocks else {}
    check(f"the structured data names version {version}",
          graph.get("SoftwareApplication", {}).get("softwareVersion") == version)
    entities = graph.get("FAQPage", {}).get("mainEntity", [])
    faq = re.search(r'<section class="part" id="faq".*?</section>', text, re.S)
    shown_q = [html.unescape(q) for q in re.findall(r"<dt>(.*?)</dt>", faq[0])] if faq else []
    shown_a = [html.unescape(a) for a in re.findall(r"<dd>(.*?)</dd>", faq[0])] if faq else []
    check("the questions in the structured data are the ones on the page",
          bool(entities) and [e["name"] for e in entities] == shown_q)
    check("and so are the answers", [e["acceptedAnswer"]["text"] for e in entities] == shown_a)
    sitemap = (ROOT / "site/sitemap.xml").read_text() if (ROOT / "site/sitemap.xml").is_file() else ""
    check("the sitemap lists the page", f"<loc>{site}</loc>" in sitemap)
    llms = (ROOT / "site/llms.txt").read_text() if (ROOT / "site/llms.txt").is_file() else ""
    check("llms.txt says what it is", llms.startswith("# sd-mqtt\n") and "MIT" in llms)
    named = re.findall(rf"https://github.com/{REPO}/blob/main/([^)#\s]+)", llms)
    check("every repository path llms.txt names exists",
          bool(named) and all((ROOT / n).exists() for n in named), str([n for n in named if not (ROOT / n).exists()]))
    missing_figures = [f for f in re.findall(r'data-figure="([^"]+)"', text) if f not in llms]
    check("llms.txt quotes the page's figures", not missing_figures, str(missing_figures))

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
