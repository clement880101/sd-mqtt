#!/usr/bin/env python3
"""Every relative link in the Markdown docs points at something that exists.

    python3 scripts/test_docs_links.py

The README, docs/ and the decision records link to each other by
relative path, and a page moved or renamed leaves links behind that
nothing else notices. This follows each `[text](path#anchor)` whose
path is not a URL, requires the file (or directory) to exist, and for a
`#anchor` into a Markdown file requires a heading that GitHub would give
that anchor.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
failures = 0


def anchors(markdown: Path) -> set[str]:
    found = set()
    for line in markdown.read_text().splitlines():
        if line.startswith("#"):
            text = line.lstrip("#").strip().lower()
            text = re.sub(r"[^\w\s-]", "", text).replace(" ", "-")
            found.add(text)
    return found


# The pages a reader is sent to; every relative link in them resolves.
PAGES = [ROOT / "README.md", ROOT / "CHANGELOG.md", ROOT / "services/web/README.md",
         *ROOT.glob("docs/*.md"), *ROOT.glob("docs/features/*.md"), *ROOT.glob("docs/decisions/*.md")]
for page in PAGES:
    for target in LINK.findall(page.read_text()):
        if target.startswith(("http://", "https://", "mailto:")) or "::" in target:
            continue
        path, _, anchor = target.partition("#")
        resolved = (page.parent / path).resolve() if path else page
        if not resolved.exists():
            print(f"  FAIL  {page.relative_to(ROOT)} -> {target}: no such file")
            failures += 1
        elif anchor and resolved.suffix == ".md" and anchor not in anchors(resolved):
            print(f"  FAIL  {page.relative_to(ROOT)} -> {target}: no heading #{anchor}")
            failures += 1
print(f"  {'FAIL' if failures else 'ok  '}  {failures} broken links")
sys.exit(1 if failures else 0)
