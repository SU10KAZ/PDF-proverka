"""«Проверить сомнительные пары через Луну» (version_comparison_ai/sheet_match.py) и правка ``ai_link``."""
from __future__ import annotations

import json
import re
import time

import pytest
from PIL import Image

from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import lineage, paths, profile
from backend.app.services.version_comparison_ai import executor as ex
from backend.app.services.version_comparison_ai import sheet_match as sm

OBJ = "obj-sm"


def _rows():
    return [{"cells": {"A": {"page": 1, "confidence": "origin"}, "B": {"page": 1, "confidence": "high"}}},
            {"cells": {"A": {"page": 2, "confidence": "origin"}}},
            {"cells": {"B": {"page": 3, "confidence": "origin"}, "C": {"page": 3, "confidence": "high"}}}]


def test_ai_link_merges_row_with_its_continuation():
    rows = _rows()
    lineage._apply_edits(rows, [{"op": "ai_link", "a": ["A", 2], "b": ["B", 3]}], ["A", "B", "C"])
    assert len(rows) == 2
    merged = rows[1]["cells"]
    assert merged["B"]["page"] == 3 and merged["B"]["confidence"] == "ai" and merged["B"]["ai"] is True
    assert merged["C"]["page"] == 3 and merged["C"]["confidence"] == "high", "продолжение строки B идёт вместе с ней"


def test_ai_link_skips_taken_sheet_and_manual_edit_wins():
    rows = _rows()
    # Лист B стр.1 уже в паре с A стр.1 — пара Луны A2 → B1 не применяется.
    lineage._apply_edits(rows, [{"op": "ai_link", "a": ["A", 2], "b": ["B", 1]}], ["A", "B", "C"])
    assert len(rows) == 3 and "B" not in rows[1]["cells"]
    # Пара Луны, затем ручная правка: лист B стр.3 отвязан — ручное решение главнее.
    rows = _rows()
    lineage._apply_edits(rows, [{"op": "ai_link", "a": ["A", 2], "b": ["B", 3]},
                                {"op": "detach", "page": ["B", 3]}], ["A", "B", "C"])
    assert all(r["cells"].get("B", {}).get("confidence") != "ai" for r in rows)
    assert any(r["cells"].get("B", {}).get("manual") for r in rows)


def _ans(choice, page, conf="high", cands=(2, 3)):
    return {"ok": True, "parsed": {"choice": choice, "page": page, "confidence": conf, "reason": "r"}, "cands": list(cands)}


def test_decide_rules():
    q = {"q": "review", "a": 2, "cands": [2, 3], "multi": False, "taken_b": []}
    rev_ok = _ans(1, 2, cands=(2, 4))
    assert sm.decide(q, _ans(2, 3), rev_ok) == "accepted"
    assert sm.decide(q, _ans(2, 2), rev_ok) == "mismatch", "номер кандидата и страница не сходятся"
    assert sm.decide(q, _ans(0, 0), None) == "none"
    assert sm.decide(q, _ans(2, 3, "medium"), rev_ok) == "hint_confidence"
    assert sm.decide(q, _ans(2, 3), _ans(2, 4, cands=(2, 4))) == "hint_reverse"
    assert sm.decide(q, _ans(2, 3), _ans(1, 2, "medium", cands=(2, 4))) == "hint_reverse"
    assert sm.decide(q | {"multi": True}, _ans(2, 3), rev_ok) == "hint_table"
    assert sm.decide(q | {"taken_b": [3]}, _ans(2, 3), rev_ok) == "hint_taken"
    assert sm.decide(q, {"ok": False, "error_kind": "TIMEOUT"}, None) == "failed"
    m = {"q": "medium", "a": 5, "algo_b": 6, "cands": [5, 6, 7]}
    assert sm.decide(m, _ans(2, 6, cands=(5, 6, 7)), None) == "medium_confirm"
    assert sm.decide(m, _ans(2, 6, "low", cands=(5, 6, 7)), None) == "medium_weak"
    assert sm.decide(m, _ans(3, 7, cands=(5, 6, 7)), None) == "medium_other"
    assert sm.decide(m, _ans(0, 0, cands=(5, 6, 7)), None) == "medium_none"


# ------------------------------------------------------------------------------- задание целиком

SHA_A, SHA_B = "a" * 64, "b" * 64


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("VERSION_COMPARISON_AI_ENABLED", "1")
    monkeypatch.setenv("VC_SM_PARALLEL", "4")
    monkeypatch.setattr(sm, "ACCOUNT_PAUSES_S", (0, 0))
    cat = {"schema": "x", "object_id": OBJ,
           "projects": [{"project_id": "p1", "title": "ЭО", "code": "133/23-ГК-ЭО", "versions": ["A", "B"]}],
           "versions": {"A": {"version_id": "A", "project_id": "p1", "sha256": SHA_A, "user_label": "V1"},
                        "B": {"version_id": "B", "project_id": "p1", "sha256": SHA_B, "user_label": "V2"}}}
    paths.catalog_path(OBJ).parent.mkdir(parents=True, exist_ok=True)
    paths.catalog_path(OBJ).write_text(json.dumps(cat, ensure_ascii=False), encoding="utf-8")
    pages = {SHA_A: [1, 2, 3], SHA_B: [1, 2, 3, 4]}
    monkeypatch.setattr(profile, "load", lambda o, sha: {"pages": [{"page": n, "kind": "drawing", "stamp": {"sheet_no": str(n)}}
                                                                  for n in pages[sha]]})

    def thumb(o, sha, page, w):
        f = tmp_path / f"t_{sha[:1]}_{page}.png"
        if not f.exists():
            Image.new("RGB", (60, 40), "white").save(f)
        return f
    monkeypatch.setattr(profile, "thumbnail", thumb)
    # Матрица без пар Луны: A1↔B1 уверенно, A2 — «⁇» с кандидатами B2/B3, A3↔B4 средняя уверенность.
    result = {"rows": [
        {"cells": {"A": {"state": "matched", "page": 1}, "B": {"state": "matched", "page": 1, "confidence": "high"}}},
        {"cells": {"A": {"state": "matched", "page": 2}, "B": {"state": "unmatched", "candidates": [{"page": 2}, {"page": 3}]}}},
        {"cells": {"A": {"state": "unmatched", "candidates": [{"page": 2}]}, "B": {"state": "matched", "page": 3}}},
        {"cells": {"A": {"state": "absent"}, "B": {"state": "matched", "page": 2}}},
        {"cells": {"A": {"state": "matched", "page": 3}, "B": {"state": "matched", "page": 4, "confidence": "medium"}}},
    ]}
    monkeypatch.setattr(sm, "_base_result", lambda o, p, v: result)
    monkeypatch.setattr(vc_jobs, "matrix", lambda o, p: None)
    yield tmp_path
    ex.set_override(None)


def _fake(answers):
    """Ответ по виду вопроса: прямой (страница A) или встречный (лист B ищет прежний)."""
    def respond(task):
        back = "ПРЕДЫДУЩЕЙ" in task.prompt
        a_page = int(re.search(r"лист A: версия «[^»]+», стр\. (\d+)", task.prompt).group(1))
        cands = [int(x) for x in re.findall(r"кандидат \d+: версия «[^»]+», стр\. (\d+)", task.prompt)]
        page, conf = answers[(back, a_page)]
        return {"choice": cands.index(page) + 1 if page else 0, "page": page, "confidence": conf, "reason": "тест"}
    return ex.FakeDescriber(respond)


def _wait(timeout=20.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = sm.status(OBJ)
        if st["state"] not in sm.ACTIVE:
            return st
        time.sleep(0.05)
    raise AssertionError("задание не закончилось")


def test_job_accepts_confirmed_pair_and_keeps_hints(env):
    fake = _fake({(False, 2): (3, "high"), (True, 3): (2, "high"), (False, 3): (4, "high")})
    ex.set_override(fake)
    sm.start(OBJ, consent=True)
    st = _wait()
    assert st["state"] == "done", st.get("reason")
    assert st["totals"] == {"accepted": 1, "medium_confirm": 1}
    data = json.loads(paths.lineage_path(OBJ, "p1").read_text(encoding="utf-8"))
    assert data["ai"]["edits"] == [{"op": "ai_link", "a": ["A", 2], "b": ["B", 3]}]
    assert data["ai"]["notes"]["A|2|B"]["decision"] == "accepted"
    assert data["ai"]["notes"]["A|3|B"]["decision"] == "medium_confirm"
    assert vc_jobs.edits_all(data)[0]["op"] == "ai_link", "пары Луны — раньше ручных правок"
    calls = len(fake.calls)
    assert calls == 3      # 2 прямых + 1 встречный (подтверждение пары средней уверенности встречного не требует)
    # Повтор по тем же листам — из сохранённых ответов, без обращений к модели.
    sm.start(OBJ, consent=True)
    st = _wait()
    assert st["state"] == "done" and len(fake.calls) == calls and st["progress"]["cached"] == 3
    # «Убрать пары Луны»
    sm.clear(OBJ)
    assert "ai" not in json.loads(paths.lineage_path(OBJ, "p1").read_text(encoding="utf-8"))


def test_job_reverse_disagreement_is_only_hint(env):
    ex.set_override(_fake({(False, 2): (3, "high"), (True, 3): (0, "high"), (False, 3): (3, "high")}))
    sm.start(OBJ, consent=True)
    st = _wait()
    assert st["totals"] == {"hint_reverse": 1, "medium_other": 1}
    data = json.loads(paths.lineage_path(OBJ, "p1").read_text(encoding="utf-8"))
    assert data["ai"]["edits"] == []
    assert data["ai"]["notes"]["A|2|B"]["page"] == 3


def test_quota_stops_job_without_writing(env):
    ex.set_override(ex.FakeDescriber(lambda task: ex.DescribeResult(False, error_kind="QUOTA", error="quota")))
    sm.start(OBJ, consent=True)
    st = _wait()
    assert st["state"] == "paused_quota"
    assert not paths.lineage_path(OBJ, "p1").exists()


def test_start_requires_consent(env):
    with pytest.raises(sm.SheetMatchError):
        sm.start(OBJ, consent=False)


def test_single_auth_failure_is_retried_not_fatal(env):
    """Один отказ «нет авторизации» среди сотен обращений не останавливает задание: вопрос повторяется."""
    answers = {(False, 2): (3, "high"), (True, 3): (2, "high"), (False, 3): (4, "high")}
    good, seen = _fake(answers).responder, []

    def flaky(task):
        seen.append(task.prompt)
        if len(seen) == 1:
            return ex.DescribeResult(False, error_kind="AUTH", error="auth")
        return good(task)
    ex.set_override(ex.FakeDescriber(flaky))
    sm.start(OBJ, consent=True)
    st = _wait()
    assert st["state"] == "done" and st["totals"] == {"accepted": 1, "medium_confirm": 1}
    log = (paths.object_dir(OBJ) / "sheet_match" / "failures.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(log) == 1 and json.loads(log[0])["kind"] == "AUTH"


def test_failure_classification_ignores_numbers_and_answers():
    """«401» внутри счётчиков токенов и идентификаторов, «quota» в ответе модели — не отказ авторизации/квоты."""
    stdout = "\n".join([
        json.dumps({"type": "thread.started", "thread_id": "01a14e10-4012-70c1"}),
        json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "квота 401 листов, billing"}}),
        json.dumps({"type": "turn.completed", "usage": {"input_tokens": 24013, "cached_input_tokens": 4012}}),
        json.dumps({"type": "error", "message": "stream disconnected before completion"}),
    ])
    assert ex.classify(ex.failure_text(stdout, "")) == "TRANSIENT"
    real = json.dumps({"type": "turn.failed", "error": {"message": "unexpected status 401 Unauthorized"}})
    assert ex.classify(ex.failure_text(real, "")) == "AUTH"
    assert ex.classify(ex.failure_text("", "Error: http status 401")) == "AUTH"
    assert ex.classify("input_tokens 24013") != "AUTH"
