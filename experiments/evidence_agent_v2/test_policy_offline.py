"""Офлайн-тесты ядра EV2 (без ngrok): парсер восприятия + политика голосования."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.evidence_agent_v2.extract import Perception, _parse, _to_perception
from experiments.evidence_agent_v2.verify import _aggregate


def P(contradicts, legible=True, model="m"):
    return Perception(contradicts, legible, "v", "s", "q", "n", {"x": 1}, "", model)


# --- парсер ---
def test_parse_plain_object():
    obj = _parse('{"contradicts_finding":"yes","region_legible":true}')
    assert obj and obj["contradicts_finding"] == "yes"


def test_parse_object_with_surrounding_text():
    obj = _parse('Вот ответ: {"contradicts_finding":"no"} конец')
    assert obj and obj["contradicts_finding"] == "no"


def test_to_perception_invalid_contradicts_falls_back():
    p = _to_perception({"contradicts_finding": "maybe"}, "m")
    assert p.contradicts == "cannot_tell"


def test_yes_without_quote_downgraded_to_cannot_tell():
    # «yes» без дословной цитаты = рассуждательный yes без якоря → cannot_tell
    p = _to_perception({"contradicts_finding": "yes", "evidence_quote": ""}, "m")
    assert p.contradicts == "cannot_tell"


def test_yes_with_quote_survives():
    p = _to_perception({"contradicts_finding": "yes", "evidence_quote": "h=2100"}, "m")
    assert p.contradicts == "yes"


# --- политика ---
def test_two_yes_gives_confident_reject():
    v = _aggregate("F-1", [P("yes"), P("yes")], "graphic", ["B1"])
    assert v.decision == "reject" and v.confidence == 1.0


def test_single_yes_never_rejects():
    v = _aggregate("F-1", [P("yes")], "graphic", ["B1"])
    assert v.decision == "borderline"   # консервативно: один «yes» != reject


def test_one_yes_one_no_is_borderline():
    v = _aggregate("F-1", [P("yes"), P("no")], "graphic", ["B1"])
    assert v.decision == "borderline"


def test_no_majority_accepts():
    v = _aggregate("F-1", [P("no"), P("no")], "graphic", ["B1"])
    assert v.decision == "accept"


def test_cannot_tell_dominant_needs_human():
    v = _aggregate("F-1", [P("cannot_tell"), P("cannot_tell")], "graphic", ["B1"])
    assert v.decision == "needs_human"


def test_all_invalid_needs_human():
    bad = Perception("cannot_tell", False, "", "", "", "", None, "no_json", "m")
    v = _aggregate("F-1", [bad, bad], "graphic", ["B1"])
    assert v.decision == "needs_human" and v.votes["invalid"] == 2


def test_yes_with_one_cannot_tell_still_rejects_if_two_yes():
    v = _aggregate("F-1", [P("yes"), P("yes"), P("cannot_tell")], "graphic", ["B1"])
    assert v.decision == "reject"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
