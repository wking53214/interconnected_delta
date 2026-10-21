"""delta is independent of CNS, and these tests hold whether or not CNS is installed.

Each runs in a fresh interpreter in which ``cns`` is blocked outright
(``sys.modules['cns'] = None`` makes any import of it fail), so the result does not
depend on what the test environment happens to contain. They never skip.
"""
import ast
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "delta"
BLOCK = "import sys; sys.modules['cns'] = None; sys.modules['cns.gate'] = None\n"
# The subprocess needs the same import path as this run (delta's own root, and beta and zeta, wherever they live).
ENV = {
    "PYTHONPATH": os.pathsep.join([str(ROOT)] + [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]),
    "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
    "PYTHONDONTWRITEBYTECODE": "1",
}
EXTRA_PIN = "cns @ git+https://github.com/wking53214/CNS.git@v1.9.0"


def run(code: str, *, block: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-c", (BLOCK if block else "") + textwrap.dedent(code)],
                          capture_output=True, text=True, env=ENV, timeout=120)


def ok(done: subprocess.CompletedProcess) -> None:
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip().endswith("ok"), done.stdout


def test_the_package_and_its_connector_import_with_cns_blocked():
    ok(run("import delta, delta.cns_connector; print('ok')"))


def test_the_ledger_and_obligations_still_do_their_job_with_cns_blocked():
    ok(run("""
        from datetime import datetime
        from beta import Decision
        from delta import DecisionLedger, DecisionObligationTracker, MaturationRule
        decision = Decision("patient1", "escalate", "ESCALATE", datetime(2026, 1, 1), 0.9, decision_fingerprint="fp-1")
        ledger = DecisionLedger()
        entry = ledger.append(decision)
        assert ledger.verify_chain_integrity() and entry.immutable_hash, entry
        tracker = DecisionObligationTracker({"escalate": MaturationRule(kind="recovery", horizon_seconds=86400.0)})
        obligation = tracker.open_for_decision(decision, domain="clinical", opened_at=0.0)
        assert obligation is not None and obligation.state == "OPEN", obligation
        print('ok')
    """))


def test_the_connector_says_what_is_missing_when_cns_is_blocked():
    ok(run("""
        from datetime import datetime
        from beta import Decision
        from delta import DecisionLedger, DecisionObligationTracker, MaturationRule
        from delta.cns_connector import (CnsNotInstalled, binds, cns_available, entry_content, entry_digest,
                                         obligation_content, obligation_digest)
        decision = Decision("e", "d", "x", datetime(2026, 1, 1), 1.0)
        entry = DecisionLedger().append(decision)
        tracker = DecisionObligationTracker({"d": MaturationRule(kind="k", horizon_seconds=60.0)})
        obligation = tracker.open_for_decision(decision, domain="dom", opened_at=0.0)
        assert cns_available() is False
        assert entry_content(entry)["audit_id"] and obligation_content(obligation)["domain"] == "dom"   # content needs no cns
        for call in (lambda: entry_digest(entry), lambda: obligation_digest(obligation),
                     lambda: binds(object(), entry), lambda: binds(object(), obligation)):
            try:
                call()
            except CnsNotInstalled as e:
                assert isinstance(e, ImportError)
                assert "delta[cns]" in str(e) and "pip install" in str(e), str(e)
            else:
                raise SystemExit("worked without cns")
        print('ok')
    """))


def test_importing_the_package_does_not_load_cns_even_when_it_is_installed():
    ok(run("""
        import sys
        import delta, delta.cns_connector
        assert not [m for m in sys.modules if m == "cns" or m.startswith("cns.")], "cns was loaded"
        print('ok')
    """, block=False))


def test_no_module_of_the_package_imports_cns_by_statement():
    offenders = {}
    sources = sorted(PACKAGE.glob("*.py"))
    assert PACKAGE / "cns_connector.py" in sources
    for path in sources:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names = [a.name for a in node.names] if isinstance(node, ast.Import) else \
                [node.module] if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module else []
            bad = [n for n in names if n == "cns" or n.startswith("cns.")]
            if bad:
                offenders.setdefault(path.name, []).extend(bad)
    assert offenders == {}


# pyproject is read as text: tomllib needs Python 3.11 and the floor here is 3.9.

def _section(text: str, header: str) -> str:
    lines, found, body = text.splitlines(), False, []
    for line in lines:
        if line.startswith("["):
            if found:
                break
            found = line.strip() == f"[{header}]"
        elif found:
            body.append(line)
    assert found, f"no [{header}] table in pyproject.toml"
    return "\n".join(body)


def _list_value(section: str, key: str) -> list:
    match = re.search(rf"^{re.escape(key)}\s*=\s*\[(.*?)\]\s*$", section, re.S | re.M)
    assert match, f"no `{key} = [...]` in the section"
    body = re.sub(r"^\s*#.*$", "", match.group(1), flags=re.M)
    return re.findall(r'"([^"]*)"', body)


def test_the_package_declares_no_runtime_dependency_on_cns_and_pins_the_extra():
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    runtime = _list_value(_section(text, "project"), "dependencies")
    assert runtime, "the runtime dependencies were not found (zeta is one)"
    assert not [d for d in runtime if re.match(r"\s*cns\b", d, re.I)], runtime
    assert _list_value(_section(text, "project.optional-dependencies"), "cns") == [EXTRA_PIN]
    other_extras = _section(text, "project.optional-dependencies").replace(EXTRA_PIN, "")
    assert not re.search(r"\bcns\b", re.sub(r"^cns\s*=", "", other_extras, flags=re.M))
