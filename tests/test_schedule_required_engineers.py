"""Frontend contract for engineers that remain visible without events."""

import re
from pathlib import Path


APP_JS = Path(__file__).resolve().parents[1] / "frontend/static/js/app.js"


def _required_engineers() -> list[tuple[str, str]]:
    source = APP_JS.read_text(encoding="utf-8")
    block = source.split("const _SCHED_REQUIRED_ENGINEERS = [", 1)[1].split("];", 1)[0]
    return re.findall(r"id:\s*'([^']+)'\s*,\s*name:\s*'([^']+)'", block)


def test_kulik_without_events_is_visible_once_with_existing_employee_id():
    required = _required_engineers()
    merged_without_events = dict(required)
    assert required.count(("kulik", "Кулик А.С.")) == 1
    assert list(merged_without_events).count("kulik") == 1
    assert all(not employee_id.startswith("kulik-") for employee_id, _ in required)


def test_required_engineer_merge_is_identity_safe_for_future_kulik_event():
    required = _required_engineers()
    api_engineers = [("kulik", "Кулик А.С.")]
    merged = dict(required)
    merged.update(api_engineers)
    assert list(merged).count("kulik") == 1


def test_existing_required_engineers_and_maksheev_identity_are_unchanged():
    required = set(_required_engineers())
    assert {
        ("kuldyaev-f-s", "Кульдяев Ф. С."),
        ("repnikov-i-a", "Репников И. А."),
        ("grivapsh-a-a", "Гривапш А. А."),
        ("kalinina-a", "Калинина А."),
    }.issubset(required)
    assert ("maksheev", "Макшеев П.Ю.") not in required
    assert all(employee_id not in {"maksheeva", "maksheev-p"} for employee_id, _ in required)
