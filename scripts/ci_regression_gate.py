#!/usr/bin/env python3
"""Регресс-гейт: прогнать тесты и упасть ТОЛЬКО на НОВЫХ падениях против baseline.

Зачем (см. reserc.md #104):
    `pytest.ini` исключал `backend/tests`, а `.github/workflows` не было — поэтому
    канонический набор тестов давно подгнил незаметно: ~76 падений + ~33 ошибки
    (устаревшие тесты, frontend-drift, ожидания старых флагов/версий) — это НЕ
    регрессии прода, а долг по тестам. Чинить весь долг — отдельный backlog.

    Этот гейт фиксирует ТЕКУЩИЙ набор падений как baseline
    (`scripts/ci_known_failures.txt`). Любая НОВАЯ поломка от правки ловится сразу,
    а известный долг просто отслеживается (и сокращается по мере починки).

Использование:
    python scripts/ci_regression_gate.py            # check: exit 1 при НОВЫХ падениях
    python scripts/ci_regression_gate.py --record   # записать текущие падения как baseline

ВАЖНО: baseline зависит от окружения (часть падений завязана на локальные
данные/сервисы). В свежем CI-окружении при первом запуске пересоздать через
`--record` и закоммитить полученный baseline.
"""
from __future__ import annotations

import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASELINE = ROOT / "scripts" / "ci_known_failures.txt"
JUNIT = ROOT / ".ci_last_report.xml"
TEST_PATHS = ["tests", "backend/tests"]


def run_pytest() -> None:
    cmd = [
        sys.executable, "-m", "pytest", *TEST_PATHS,
        "--junitxml", str(JUNIT),
        "-q", "-p", "no:cacheprovider", "--tb=no", "--no-header",
    ]
    # Игнорируем exit-код pytest: вывод анализируем по junit-отчёту.
    subprocess.run(cmd, cwd=str(ROOT))


def collect_failures() -> set[str]:
    if not JUNIT.exists():
        raise SystemExit("[gate] FATAL: junit-отчёт не создан — pytest упал на сборе тестов")
    tree = ET.parse(JUNIT)
    failed: set[str] = set()
    for tc in tree.iter("testcase"):
        if any(child.tag in ("failure", "error") for child in tc):
            cls = tc.get("classname", "")
            name = tc.get("name", "")
            failed.add(f"{cls}::{name}")
    return failed


def load_baseline() -> set[str]:
    if not BASELINE.exists():
        return set()
    return {
        line.strip()
        for line in BASELINE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def write_baseline(failed: set[str]) -> None:
    header = [
        "# Baseline известных падающих тестов (регресс-гейт, reserc.md #104).",
        "# Предсуществующие падения/ошибки на момент уборки 'нет CI'.",
        "# В основном устаревшие тесты / frontend-drift / ожидания старых флагов —",
        "# это НЕ регрессии прода. CI зелёный, если НЕТ новых падений сверх этого списка.",
        "# Пересоздать в свежем окружении: python scripts/ci_regression_gate.py --record",
        f"# Кол-во: {len(failed)}",
        "",
    ]
    BASELINE.write_text("\n".join(header + sorted(failed)) + "\n", encoding="utf-8")


def main() -> int:
    record = "--record" in sys.argv
    run_pytest()
    current = collect_failures()

    if record:
        write_baseline(current)
        print(f"[gate] baseline записан: {len(current)} известных падений -> {BASELINE}")
        return 0

    baseline = load_baseline()
    new = sorted(current - baseline)
    fixed = sorted(baseline - current)
    print(
        f"[gate] падений сейчас: {len(current)} | baseline: {len(baseline)} | "
        f"новых: {len(new)} | стало зелёных: {len(fixed)}"
    )
    if fixed:
        print("[gate] эти baseline-тесты теперь ПРОХОДЯТ (можно убрать из baseline):")
        for t in fixed:
            print(f"    - {t}")
    if new:
        print("[gate] НОВЫЕ падения (регрессия — починить или обосновать):")
        for t in new:
            print(f"    + {t}")
        return 1
    print("[gate] OK — новых падений сверх известного baseline нет.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
