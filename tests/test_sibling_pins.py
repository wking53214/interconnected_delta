"""The beta and zeta dependencies in pyproject.toml are pinned to reviewed commits.

delta depends on both by git URL. Unpinned, those URLs install whatever each
default branch holds on the day; pinned, every install gets the same code,
which is the code these tests ran against. The constants here move with the
pins, and moving any of them is a reviewed change.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# The commits the runtime dependencies install. Moving one is a reviewed change.
EXPECTED_BETA_PIN = "0507ebebc191c3e6a9997fcf6190918311717e0e"
EXPECTED_ZETA_PIN = "4ba70f621ef40d53a5004809a569303707788cd0"


# pyproject is read as text: tomllib needs Python 3.11 and the floor here is 3.9.

def _runtime_dependencies() -> list:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    body, inside = [], False
    for line in text.splitlines():
        header = re.match(r"^\[([^\]]+)\]\s*$", line)
        if header:
            inside = header.group(1).strip() == "project"
        elif inside:
            body.append(line)
    found = re.search(r"^dependencies\s*=\s*\[(.*?)\]\s*$", "\n".join(body), re.S | re.M)
    assert found, "no `dependencies = [...]` in [project]"
    return re.findall(r'"([^"]*)"', re.sub(r"^\s*#.*$", "", found.group(1), flags=re.M))


def _pinned_sha(name: str, repo: str) -> str:
    deps = [d for d in _runtime_dependencies() if re.match(r"\s*%s\b" % name, d)]
    assert len(deps) == 1, deps
    found = re.fullmatch(
        r"%s @ git\+https://github\.com/wking53214/%s(?:\.git)?@([0-9a-f]{40})" % (name, repo), deps[0]
    )
    assert found is not None, deps[0]
    return found.group(1)


def test_beta_is_pinned_to_the_reviewed_commit():
    assert _pinned_sha("beta", "interconnected_beta") == EXPECTED_BETA_PIN


def test_zeta_is_pinned_to_the_reviewed_commit():
    assert _pinned_sha("zeta", "interconnected_zeta") == EXPECTED_ZETA_PIN
