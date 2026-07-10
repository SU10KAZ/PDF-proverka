"""Тесты сохранения экспертных вердиктов при переаудите (verdict_preservation).

Ядро матчинга — чистые функции без IO; интеграция snapshot→rehydrate — через
monkeypatch kb-слоя. E2E моделирует ПРОД-предпосылку: на момент регидрации
expert_review.json всё ещё содержит старые решения на старых F-ID (04_review
переживает переаудит) — они должны реконсилироваться, а не блокировать.
"""
import json

import pytest

from backend.app.services.findings import verdict_preservation as vp


def _finding(fid: str, problem: str, sheet: str = "Лист 3", solution: str = "",
             category: str = "cable", severity: str = "КРИТИЧЕСКОЕ") -> dict:
    return {
        "id": fid, "problem": problem, "solution": solution,
        "sheet": sheet, "category": category, "severity": severity,
    }


def _opt(oid: str, current: str, proposed: str, sheet: str = "Лист 1",
         otype: str = "cheaper_analog") -> dict:
    return {"id": oid, "current": current, "proposed": proposed,
            "sheet": sheet, "type": otype}


def _snap_item(old_id: str, source: dict, decision: str = "accepted",
               item_type: str = "finding", timestamp: str = "t0") -> dict:
    return {
        "old_id": old_id, "item_type": item_type, "decision": decision,
        "rejection_reason": None, "reviewer": "Узун А.И.", "timestamp": timestamp,
        "fingerprint": vp.build_fingerprint(source, item_type),
    }


# ─── build_fingerprint ──────────────────────────────────────────────────────

def test_fingerprint_sheet_normalization():
    a = vp.build_fingerprint(_finding("F-001", "x", sheet="Лист 1 (из 1)"), "finding")
    b = vp.build_fingerprint(_finding("F-002", "x", sheet="Лист 1"), "finding")
    assert a["sheet"] == b["sheet"] == "лист 1"


def test_fingerprint_salient_numbers_order_independent():
    a = vp.build_fingerprint(_finding("F-001", "Кабель ВВГнг 3х2,5 вместо 3х4"), "finding")
    b = vp.build_fingerprint(_finding("F-002", "Вместо 3х4 применён кабель ВВГнг 3х2,5"), "finding")
    assert a["numbers"] == b["numbers"]
    assert "2.5" in a["numbers"] and "4" in a["numbers"]


# ─── match_snapshot_to_items: ядро ──────────────────────────────────────────

def test_exact_match_survives_renumbering():
    """F-005 стал F-002 — вердикт находит замечание по фингерпринту."""
    old = _finding("F-005", "Сечение кабеля 2,5 мм2 занижено, требуется 4 мм2")
    new_items = {
        "F-001": _finding("F-001", "Отсутствует заземление щита", sheet="Лист 1"),
        "F-002": _finding("F-002", "Сечение кабеля 2,5 мм2 занижено, требуется 4 мм2"),
    }
    restored, leftovers = vp.match_snapshot_to_items(
        [_snap_item("F-005", old)], new_items, "finding", set())
    assert len(restored) == 1 and not leftovers
    assert restored[0]["new_id"] == "F-002"
    assert restored[0]["match"] == "exact"


def test_fuzzy_match_on_slightly_changed_text():
    old = _finding("F-003", "Не предусмотрено аварийное освещение в электрощитовой на отм. -2,950")
    new_items = {
        "F-007": _finding(
            "F-007",
            "Не предусмотрено аварийное (эвакуационное) освещение в электрощитовой на отм. -2,950"),
        "F-008": _finding("F-008", "Совершенно другая проблема с вентиляцией", sheet="Лист 9"),
    }
    restored, leftovers = vp.match_snapshot_to_items(
        [_snap_item("F-003", old)], new_items, "finding", set())
    assert len(restored) == 1 and not leftovers
    assert restored[0]["new_id"] == "F-007"
    assert restored[0]["match"] == "fuzzy"


def test_unmatched_goes_to_leftovers_not_silently_dropped():
    old = _finding("F-004", "Замечание, которого больше нет в новом прогоне")
    new_items = {"F-001": _finding("F-001", "Абсолютно новое замечание про молниезащиту")}
    restored, leftovers = vp.match_snapshot_to_items(
        [_snap_item("F-004", old)], new_items, "finding", set())
    assert not restored
    assert len(leftovers) == 1 and leftovers[0]["reason"] == "unmatched"


def test_fuzzy_requires_equal_salient_numbers():
    """«2,5 мм2» не должен fuzzy-матчиться на «4 мм2» при похожем тексте."""
    old = _finding("F-001", "Не указано сечение кабеля 2,5 мм2 для линии освещения подвала")
    new_items = {
        "F-003": _finding("F-003", "Не указано сечение кабеля 4 мм2 для линии освещения подвала"),
    }
    restored, leftovers = vp.match_snapshot_to_items(
        [_snap_item("F-001", old)], new_items, "finding", set())
    assert not restored
    assert leftovers[0]["reason"] == "unmatched"


def test_fuzzy_soft_gate_on_category():
    """Разные категории (kind) при похожем тексте — не матчить."""
    old = _finding("F-001", "Условная шаблонная формулировка замечания номер один",
                   category="cable")
    new_items = {
        "F-002": _finding("F-002", "Условная шаблонная формулировка замечания номер два",
                          category="grounding"),
    }
    restored, leftovers = vp.match_snapshot_to_items(
        [_snap_item("F-001", old)], new_items, "finding", set())
    assert not restored


def test_exact_target_occupied_does_not_fall_to_fuzzy_neighbor():
    """Цель exact занята решением → target_already_decided, а не fuzzy-сосед."""
    old = _finding("F-001", "Розетка размещена ближе 0,6 м от мойки в санузле")
    new_items = {
        "F-001": _finding("F-001", "Розетка размещена ближе 0,6 м от мойки в санузле"),
        "F-002": _finding("F-002", "Розетка размещена ближе 0,6 м от мойки в кладовой"),
    }
    restored, leftovers = vp.match_snapshot_to_items(
        [_snap_item("F-001", old)], new_items, "finding", {"F-001"})
    assert not restored
    assert leftovers[0]["reason"] == "target_already_decided"


def test_two_phase_fuzzy_cannot_steal_exact_target():
    """Ранний fuzzy-item не забирает замечание, являющееся exact-целью позднего."""
    fuzzy_old = _finding("F-001", "Не выполнено требование по прокладке кабеля в лотке типа А")
    exact_old = _finding("F-002", "Не выполнено требование по прокладке кабеля в лотке типа Б")
    new_items = {
        # единственный кандидат: exact-цель для F-002 и fuzzy-кандидат для F-001
        "F-009": _finding("F-009", "Не выполнено требование по прокладке кабеля в лотке типа Б"),
    }
    restored, leftovers = vp.match_snapshot_to_items(
        [_snap_item("F-001", fuzzy_old), _snap_item("F-002", exact_old)],
        new_items, "finding", set())
    by_old = {r["snapshot_item"]["old_id"]: r for r in restored}
    assert "F-002" in by_old and by_old["F-002"]["match"] == "exact"
    assert all(l["old_id"] == "F-001" for l in leftovers)


def test_ambiguous_exact_distinct_candidates_not_guessed():
    """>1 различимых exact-кандидатов (разные листы) → ambiguous_exact."""
    old = _finding("F-002", "Отсутствует маркировка кабельных линий", sheet="")
    new_items = {
        "F-005": _finding("F-005", "Отсутствует маркировка кабельных линий", sheet="Лист 5"),
        "F-006": _finding("F-006", "Отсутствует маркировка кабельных линий", sheet="Лист 8"),
    }
    snap = _snap_item("F-002", old)
    snap["fingerprint"]["sheet"] = ""  # лист в снапшоте неизвестен
    restored, leftovers = vp.match_snapshot_to_items([snap], new_items, "finding", set())
    assert not restored
    assert leftovers[0]["reason"] == "ambiguous_exact"


def test_true_duplicates_same_text_same_sheet_take_first():
    """Истинные дубли (текст и лист одинаковы) — берём первый, это безопасно."""
    old = _finding("F-001", "Дубль-замечание про сечение кабеля 2,5 мм2")
    new_items = {
        "F-003": _finding("F-003", "Дубль-замечание про сечение кабеля 2,5 мм2"),
        "F-004": _finding("F-004", "Дубль-замечание про сечение кабеля 2,5 мм2"),
    }
    restored, leftovers = vp.match_snapshot_to_items(
        [_snap_item("F-001", old)], new_items, "finding", set())
    assert len(restored) == 1 and restored[0]["new_id"] == "F-003"


# ─── интеграция snapshot → rehydrate (kb-слой замокан) ──────────────────────

class _KBStub:
    def __init__(self, tmp_path, findings, review, optimizations=None):
        self._dir = tmp_path
        self.findings = findings          # содержимое 03_findings.json
        self.optimizations = optimizations or {"items": []}
        self.review = review
        self.saved_decisions = []
        self.saved_removed_ids = []

    def _review_path(self, project_id):
        return self._dir / "expert_review.json"

    def _analysis_dirs(self, project_id):
        return [self._dir]

    def _load_json(self, path):
        name = path.name
        if name == "03_findings.json":
            return self.findings
        if name == "optimization.json":
            return self.optimizations
        if name == "03a_norms_verified.json":
            # регидрация НЕ должна читать 03a (stale от прошлого прогона)
            raise AssertionError("verdict_preservation must not read 03a_norms_verified.json")
        return None

    def load_expert_review(self, project_id):
        return self.review

    def save_expert_review(self, project_id, decisions, reviewer="",
                           removed_ids=None, stamp_schedule=True):
        assert stamp_schedule is False
        self.saved_decisions.extend(decisions)
        self.saved_removed_ids.extend(removed_ids or [])
        return {"saved": len(decisions)}


@pytest.fixture()
def kb_stub(tmp_path, monkeypatch):
    def _install(findings_map, review, optimizations=None):
        findings = {"findings": list(findings_map.values())}
        stub = _KBStub(tmp_path, findings, review, optimizations)
        import backend.app.services.knowledge_base.knowledge_base_service as kb
        for name in ("_review_path", "_analysis_dirs", "_load_json",
                     "load_expert_review", "save_expert_review"):
            monkeypatch.setattr(kb, name, getattr(stub, name))
        monkeypatch.setattr(vp, "is_enabled", lambda: True)
        monkeypatch.setattr(vp, "is_shadow", lambda: False)
        return stub
    return _install


def test_snapshot_then_rehydrate_with_stale_review(kb_stub, tmp_path):
    """Прод-сценарий: старые решения остаются в review на момент регидрации.

    До переаудита: F-001 accepted, F-002 rejected. После перенумерации старое
    F-001 указывает на ЧУЖОЕ замечание — регидрация должна снять оба stale
    (removed_ids) и восстановить вердикты на новых ID.
    """
    old_findings = {
        "F-001": _finding("F-001", "Отсутствует заземление щита ЩР-1", sheet="Лист 2"),
        "F-002": _finding("F-002", "Сечение кабеля 2,5 мм2 занижено, требуется 4 мм2"),
    }
    review = {"decisions": [
        {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
         "reviewer": "Узун А.И.", "timestamp": "t1"},
        {"item_id": "F-002", "item_type": "finding", "decision": "rejected",
         "rejection_reason": "уже учтено", "reviewer": "Узун А.И.", "timestamp": "t2"},
        {"item_id": "F-003", "item_type": "finding", "decision": ""},  # pending
    ]}
    stub = kb_stub(old_findings, review)

    res = vp.snapshot_for_project("P")
    assert res["status"] == "ok" and res["items"] == 2

    # «переаудит»: те же замечания под другими номерами + одно новое;
    # review НЕ очищен (как в проде) — старые решения всё ещё на F-001/F-002
    stub.findings = {"findings": [
        _finding("F-001", "Новое замечание про молниезащиту", sheet="Лист 9"),
        _finding("F-002", "Отсутствует заземление щита ЩР-1", sheet="Лист 2"),
        _finding("F-003", "Сечение кабеля 2,5 мм2 занижено, требуется 4 мм2"),
    ]}

    res = vp.rehydrate_for_project("P")
    assert res["status"] == "ok"
    assert res["restored"] == 2 and res["unmatched"] == 0
    assert res["stale_removed"] == 2

    # stale-решения прошлого прогона сняты
    assert sorted(stub.saved_removed_ids) == ["F-001", "F-002"]
    by_new = {d.item_id: d for d in stub.saved_decisions}
    assert by_new["F-002"].decision == "accepted"
    assert by_new["F-002"].carried_from_item_id == "F-001"
    assert by_new["F-003"].decision == "rejected"
    assert by_new["F-003"].rejection_reason == "уже учтено"
    assert all(d.carried_over for d in stub.saved_decisions)
    assert by_new["F-002"].reviewer == "Узун А.И."

    report = json.loads((tmp_path / vp.REPORT_FILENAME).read_text(encoding="utf-8"))
    assert report["passes"][-1]["summary"]["restored"] == 2
    # повторная регидрация findings — no-op
    assert vp.rehydrate_for_project("P")["status"] == "already_applied"


def test_fresh_manual_decision_on_collided_id_not_removed(kb_stub, tmp_path):
    """Свежая ручная разметка на совпавшем F-ID не считается stale."""
    old_findings = {"F-001": _finding("F-001", "Отсутствует заземление щита ЩР-1")}
    review = {"decisions": [
        {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
         "reviewer": "Узун А.И.", "timestamp": "t1"},
    ]}
    stub = kb_stub(old_findings, review)
    assert vp.snapshot_for_project("P")["status"] == "ok"

    stub.findings = {"findings": [
        _finding("F-001", "Совсем новое замечание"),
        _finding("F-002", "Отсутствует заземление щита ЩР-1"),
    ]}
    # эксперт УСПЕЛ вручную пере-решить новый F-001 (другой timestamp)
    stub.review = {"decisions": [
        {"item_id": "F-001", "item_type": "finding", "decision": "rejected",
         "reviewer": "Узун А.И.", "timestamp": "t99"},
    ]}

    res = vp.rehydrate_for_project("P")
    assert res["status"] == "ok"
    assert res["stale_removed"] == 0          # ручное решение не снято
    assert "F-001" not in stub.saved_removed_ids
    by_new = {d.item_id: d for d in stub.saved_decisions}
    assert by_new["F-002"].decision == "accepted"  # вердикт восстановлен рядом


def test_optimization_pass_is_separate_and_deferred(kb_stub, tmp_path):
    """OPT-вердикты восстанавливаются вторым проходом после этапа оптимизации."""
    old_findings = {"F-001": _finding("F-001", "Отсутствует заземление щита")}
    old_opts = {"items": [_opt("OPT-001", "8 контейнеров 0,36 м3",
                               "4 контейнера 0,77 м3")]}
    review = {"decisions": [
        {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
         "reviewer": "У", "timestamp": "t1"},
        {"item_id": "OPT-001", "item_type": "optimization", "decision": "accepted",
         "reviewer": "У", "timestamp": "t2"},
    ]}
    stub = kb_stub(old_findings, review, optimizations=old_opts)
    assert vp.snapshot_for_project("P")["status"] == "ok"

    # после merge: findings новые, optimization.json ещё СТАРЫЙ → проход только findings
    stub.findings = {"findings": [_finding("F-004", "Отсутствует заземление щита")]}
    res = vp.rehydrate_for_project("P", item_types=("finding",))
    assert res["status"] == "ok" and res["restored"] == 1
    assert all(d.item_type == "finding" for d in stub.saved_decisions)
    assert "OPT-001" not in stub.saved_removed_ids  # OPT-решение не тронуто

    # этап оптимизации пересобрал optimization.json → второй проход
    stub.optimizations = {"items": [
        _opt("OPT-002", "8 контейнеров 0,36 м3", "4 контейнера 0,77 м3")]}
    res = vp.rehydrate_for_project("P", item_types=("optimization",))
    assert res["status"] == "ok" and res["restored"] == 1
    opt_saved = [d for d in stub.saved_decisions if d.item_type == "optimization"]
    assert opt_saved and opt_saved[0].item_id == "OPT-002"
    # оба типа применены — любые повторы no-op
    assert vp.rehydrate_for_project("P", item_types=("finding",))["status"] == "already_applied"
    assert vp.rehydrate_for_project("P", item_types=("optimization",))["status"] == "already_applied"


def test_snapshot_keeps_existing_when_findings_already_deleted(kb_stub, tmp_path):
    """Двойной вызов (clean → аудит): пустые findings не затирают слепок."""
    old_findings = {"F-001": _finding("F-001", "Отсутствует заземление щита")}
    review = {"decisions": [{"item_id": "F-001", "item_type": "finding",
                             "decision": "accepted", "reviewer": "У", "timestamp": "t"}]}
    stub = kb_stub(old_findings, review)
    assert vp.snapshot_for_project("P")["status"] == "ok"

    stub.findings = {"findings": []}  # findings уже удалены «Очисткой»
    # слепок не применён (kept_unapplied) — прогон ещё не дошёл до регидрации
    assert vp.snapshot_for_project("P")["status"] == "kept_unapplied"
    snap = json.loads((tmp_path / vp.SNAPSHOT_FILENAME).read_text(encoding="utf-8"))
    assert len(snap["items"]) == 1


def test_unapplied_snapshot_not_overwritten_after_crash(kb_stub, tmp_path):
    """Крах между merge и регидрацией: непримёненный слепок не перезаписывается
    отравленной парой (старые id → уже новые findings)."""
    old_findings = {"F-001": _finding("F-001", "Отсутствует заземление щита ЩР-1")}
    review = {"decisions": [{"item_id": "F-001", "item_type": "finding",
                             "decision": "accepted", "reviewer": "У", "timestamp": "t"}]}
    stub = kb_stub(old_findings, review)
    assert vp.snapshot_for_project("P")["status"] == "ok"

    # прогон упал после merge: findings уже НОВЫЕ, review старый, слепок не применён
    stub.findings = {"findings": [_finding("F-001", "Совсем другое новое замечание")]}
    assert vp.snapshot_for_project("P")["status"] == "kept_unapplied"
    snap = json.loads((tmp_path / vp.SNAPSHOT_FILENAME).read_text(encoding="utf-8"))
    assert "заземление" in snap["items"][0]["fingerprint"]["text"]


def test_shadow_mode_reports_but_does_not_write(kb_stub, tmp_path, monkeypatch):
    old_findings = {"F-001": _finding("F-001", "Отсутствует заземление щита")}
    review = {"decisions": [{"item_id": "F-001", "item_type": "finding",
                             "decision": "accepted", "reviewer": "У", "timestamp": "t"}]}
    stub = kb_stub(old_findings, review)
    assert vp.snapshot_for_project("P")["status"] == "ok"
    monkeypatch.setattr(vp, "is_shadow", lambda: True)

    stub.findings = {"findings": [_finding("F-002", "Отсутствует заземление щита")]}
    res = vp.rehydrate_for_project("P")
    assert res["status"] == "ok" and res["restored"] == 1 and res["saved"] == 0
    assert not stub.saved_decisions and not stub.saved_removed_ids
    # снапшот НЕ помечен применённым — боевой прогон сможет применить
    snap = json.loads((tmp_path / vp.SNAPSHOT_FILENAME).read_text(encoding="utf-8"))
    assert snap["applied_types"] == []
    report = json.loads((tmp_path / vp.REPORT_FILENAME).read_text(encoding="utf-8"))
    assert report["passes"][-1]["shadow"] is True
