#!/usr/bin/env python3
"""Tests for `scripts/rust-notices.py`.

    scripts/test_rust_notices.py

The script had none. The defect that prompted these — an expression
requiring several licences at once being read as offering a choice
between them, because the word "or" appeared inside parentheses — is
the kind a test of the resolver alone catches and nothing else does:
the generated file is checked against itself for drift, so a wrong
summary line agrees with the wrong summary line and the check is
silent.

The allow list used here is `deny.toml`'s, read rather than restated,
for the same reason the script reads it: a licence accepted by the
gate and not by the resolver is a crate whose obligation we would
record wrongly.
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent

_spec = importlib.util.spec_from_file_location(
    "rust_notices", HERE / "rust-notices.py"
)
assert _spec is not None and _spec.loader is not None
notices = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(notices)


class EffectiveLicence(unittest.TestCase):
    """What `effective` resolves an SPDX expression to."""

    def setUp(self) -> None:
        self.allowed = notices.allowed_licences()

    def test_the_allow_list_is_the_gate_s(self) -> None:
        # Not an assertion about the resolver; an assertion that the
        # fixture below is the real thing.
        self.assertIn("MIT", self.allowed)
        self.assertIn("Apache-2.0", self.allowed)
        self.assertIn("BSD-3-Clause", self.allowed)
        self.assertIn("ISC", self.allowed)

    def test_a_single_licence_resolves_to_itself(self) -> None:
        self.assertEqual(notices.effective("MIT", self.allowed), "MIT")
        self.assertEqual(
            notices.effective("Apache-2.0 WITH LLVM-exception", self.allowed),
            "Apache-2.0 WITH LLVM-exception",
        )

    def test_a_choice_resolves_to_one_branch(self) -> None:
        self.assertEqual(notices.effective("MIT OR Apache-2.0", self.allowed), "MIT")
        self.assertEqual(notices.effective("Apache-2.0 OR MIT", self.allowed), "MIT")
        # The old separator, still on a few crates.
        self.assertEqual(notices.effective("MIT/Apache-2.0", self.allowed), "MIT")

    def test_a_branch_off_the_allow_list_is_not_a_candidate(self) -> None:
        # This is what makes `ryu` an Apache-2.0 crate here.
        self.assertEqual(
            notices.effective("Apache-2.0 OR BSL-1.0", self.allowed), "Apache-2.0"
        )

    def test_a_conjunction_resolves_to_all_of_its_parts(self) -> None:
        self.assertEqual(
            notices.effective("MIT AND BSD-3-Clause", self.allowed),
            "MIT AND BSD-3-Clause",
        )

    def test_the_real_expression_from_aws_lc_sys(self) -> None:
        """The one the generator got wrong, and the reason for this file.

        Every one of these licences is required at once. The word
        "or" is inside parentheses, where it offers a choice between
        Apache-2.0 and ISC and nothing else, so reading it as a
        top-level choice labels the package with one licence out of
        four.
        """
        expression = (
            "ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause"
        )
        resolved = notices.effective(expression, self.allowed)
        self.assertEqual(resolved, "ISC AND Apache-2.0 AND MIT AND BSD-3-Clause")
        # Said again as the obligation rather than as a string, because
        # the string is what a careless fix would make agree with
        # itself. Apache-2.0 is the one that asks for its text and any
        # NOTICE file to travel with the distribution, and it is the
        # one the old resolver dropped.
        for required in ("ISC", "Apache-2.0", "MIT", "BSD-3-Clause"):
            self.assertIn(required, resolved.split(" AND "))

    def test_the_real_expression_from_aws_lc_rs(self) -> None:
        """`AND` binds tighter than `OR`, so this is a choice of two.

        SPDX annex D: the operators are `+`, `WITH`, `AND`, `OR`, in
        decreasing precedence. So this reads
        `(ISC AND Apache-2.0) OR ISC`, and the cheaper branch is the
        one the copyright holder offered us.
        """
        self.assertEqual(
            notices.effective("ISC AND Apache-2.0 OR ISC", self.allowed), "ISC"
        )

    def test_a_parenthesised_choice_inside_a_conjunction(self) -> None:
        self.assertEqual(
            notices.effective("(MIT OR Apache-2.0) AND Unicode-3.0", self.allowed),
            "MIT AND Unicode-3.0",
        )

    def test_a_choice_of_conjunctions_prefers_the_smaller_obligation(self) -> None:
        self.assertEqual(
            notices.effective("(MIT AND BSD-3-Clause) OR Apache-2.0", self.allowed),
            "Apache-2.0",
        )

    def test_a_repeated_licence_is_named_once(self) -> None:
        self.assertEqual(notices.effective("MIT AND MIT", self.allowed), "MIT")

    def test_nothing_on_the_allow_list_is_fatal(self) -> None:
        with self.assertRaises(SystemExit):
            notices.effective("GPL-3.0-only", self.allowed)
        with self.assertRaises(SystemExit):
            notices.effective("GPL-3.0-only OR AGPL-3.0-only", self.allowed)
        # One part of a conjunction off the list condemns the whole of
        # it: there is no branch to take.
        with self.assertRaises(SystemExit):
            notices.effective("MIT AND GPL-3.0-only", self.allowed)

    def test_no_licence_at_all_is_fatal(self) -> None:
        with self.assertRaises(SystemExit):
            notices.effective(None, self.allowed)

    def test_an_unbalanced_expression_is_fatal_rather_than_guessed_at(self) -> None:
        with self.assertRaises(SystemExit):
            notices.effective("(MIT OR Apache-2.0", self.allowed)
        with self.assertRaises(SystemExit):
            notices.effective("MIT AND", self.allowed)


if __name__ == "__main__":
    unittest.main()
