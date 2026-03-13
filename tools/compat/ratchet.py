import json
import logging
import re
import sys
from pathlib import Path

log = logging.getLogger(__name__)

RATCHET_FILE = Path(__file__).parent / "ratchet.json"

STRICT_SUITES = frozenset({
    "TestDuckDB",
})


def parse_results(lines: list[str]) -> dict[str, dict[str, int]]:
    suite_passed: dict[str, int] = {}
    suite_failed: dict[str, int] = {}
    for line in lines:
        m = re.search(r"::(\w+)::(\w+)\s+(PASSED|FAILED)", line)
        if m:
            suite = m.group(1)
            if m.group(3) == "PASSED":
                suite_passed[suite] = suite_passed.get(suite, 0) + 1
            else:
                suite_failed[suite] = suite_failed.get(suite, 0) + 1

    all_suites = sorted(set(list(suite_passed.keys()) + list(suite_failed.keys())))
    return {
        s: {"passed": suite_passed.get(s, 0), "failed": suite_failed.get(s, 0)}
        for s in all_suites
        if s not in STRICT_SUITES
    }


def load_baseline() -> dict[str, dict[str, int]]:
    if not RATCHET_FILE.exists():
        log.error("No baseline file: %s", RATCHET_FILE)
        sys.exit(1)
    return json.loads(RATCHET_FILE.read_text())


def check(lines: list[str]) -> None:
    baseline = load_baseline()
    current = parse_results(lines)

    regressions: list[str] = []
    improvements: list[str] = []

    for suite, expected in sorted(baseline.items()):
        actual_passed = current.get(suite, {}).get("passed", 0)
        expected_passed = expected["passed"]
        if actual_passed < expected_passed:
            regressions.append(f"  {suite}: {actual_passed} < {expected_passed}")
        elif actual_passed > expected_passed:
            improvements.append(f"  {suite}: {actual_passed} > {expected_passed}")

    if regressions:
        log.error("RATCHET FAILED — regressions detected:\n%s", "\n".join(regressions))
        sys.exit(1)

    log.info("Ratchet OK")
    if improvements:
        log.info(
            "Improvements detected (run 'just test-compat-ratchet-update'):\n%s",
            "\n".join(improvements),
        )


def update(lines: list[str]) -> None:
    current = parse_results(lines)
    baseline = load_baseline() if RATCHET_FILE.exists() else {}

    updated: dict[str, dict[str, int]] = {}
    all_suites = sorted(set(list(current.keys()) + list(baseline.keys())))
    for suite in all_suites:
        cur = current.get(suite, {"passed": 0, "failed": 0})
        old = baseline.get(suite, {"passed": 0, "failed": 0})
        updated[suite] = {
            "passed": max(cur["passed"], old["passed"]),
            "failed": cur["failed"],
        }

    RATCHET_FILE.write_text(json.dumps(updated, indent=2) + "\n")
    log.info("Updated %s", RATCHET_FILE)


def main() -> None:
    logging.basicConfig(format="%(message)s", level=logging.INFO)

    if len(sys.argv) != 2 or sys.argv[1] not in {"--check", "--update"}:  # noqa: PLR2004
        log.error("Usage: %s --check|--update", sys.argv[0])
        log.error("Pipe pytest output to stdin")
        sys.exit(1)

    lines = sys.stdin.readlines()

    if sys.argv[1] == "--check":
        check(lines)
    else:
        update(lines)


if __name__ == "__main__":
    main()
