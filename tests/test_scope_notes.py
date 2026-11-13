"""delta's package describes a generic ledger; the one regulation-flavoured module says plainly whose rule it is.

The fairness module is the US EEOC four-fifths screen for employment-selection procedures. Using it, or its 0.8 threshold, in another
domain or jurisdiction is the deployer's decision, and the module and the README must say so. Provenance lines (naming OBSERVE or
sentinel_os) may keep the original vocabulary; tests and examples may use domain strings as data.
"""
import re
from pathlib import Path

import delta.fairness

ROOT = Path(__file__).resolve().parents[1]
DOMAIN_WORDS = re.compile(r"patient|sepsis|septic|discharge|vitals?\b|tachy|fever|hypovol|respiratory|oxygen|neonat|infant", re.IGNORECASE)
PROVENANCE = re.compile(r"OBSERVE|observe_consolidated|sentinel_os")


def test_the_fairness_module_says_whose_rule_it_is_and_who_decides_whether_it_applies():
    doc = delta.fairness.__doc__ or ""
    assert "employment-selection" in doc
    assert "deployer's decision" in doc


def test_the_readme_row_for_the_fairness_module_says_the_same():
    row = next(line for line in (ROOT / "README.md").read_text(encoding="utf-8").splitlines() if line.startswith("| `fairness.py`"))
    assert "employment-selection" in row and "deployer's decision" in row


def test_the_package_uses_no_domain_example_outside_provenance_lines():
    offenders = []
    for path in sorted((ROOT / "delta").glob("*.py")):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if DOMAIN_WORDS.search(line) and not PROVENANCE.search(line):
                offenders.append(f"{path.name}:{number}: {line.strip()}")
    assert not offenders, offenders
