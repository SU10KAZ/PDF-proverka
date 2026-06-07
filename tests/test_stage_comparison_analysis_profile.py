"""
test_stage_comparison_analysis_profile.py
-----------------------------------------
Явные профили анализа Stage Comparison (default / rich_grsh).

Проблема: качество сравнения ГРЩ зависело от трёх env-флагов глубокого
графического извлечения. Эталон шёл с флагами ON (rich, 38 отличий), массовый
прогон — OFF (fast, 15). Профиль не фиксировался, и быстрый прогон мог молча
затереть богатый результат.

Покрывает критерии ТЗ:
  1. analysis_profile пишется в comparison_result.json;
  2. default → флаги false;
  3. rich_grsh → флаги true;
  4. existing rich_grsh + default rerun не перезаписывает без подтверждения;
  5. API отдаёт profile metadata;
  6. warning для dense GRSH при default profile;
  7. batch default остаётся default (rich-флаги не включаются);
  8. rich можно запустить только явно (override), env остаётся нетронут.

Все провайдеры замоканы — без live Qwen/Opus.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import analysis_profile as ap


# ─── Fixtures / helpers ─────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    # Гарантируем дефолтное окружение профиля (флаги выключены).
    for name in ap.PROFILE_FLAG_NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    yield root


def _make_pair(session_id: str, pair_id: str, tmp_path: Path) -> None:
    from backend.app.services.stage_comparison import paths as paths_mod

    session = {
        "id": session_id,
        "pair_order": [pair_id],
        "warnings": [],
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    paths_mod.session_json_path(session_id).write_text(
        json.dumps(session, ensure_ascii=False), encoding="utf-8")
    pair = {
        "id": pair_id, "status": "matched",
        "left": {"filename": "left.pdf", "pdf_path": "/dev/null/l.pdf",
                 "md_path": str(_write(tmp_path / f"{pair_id}_L.md", "left"))},
        "right": {"filename": "right.pdf", "pdf_path": "/dev/null/r.pdf",
                  "md_path": str(_write(tmp_path / f"{pair_id}_R.md", "right"))},
    }
    paths_mod.pair_json_path(session_id, pair_id).write_text(
        json.dumps(pair, ensure_ascii=False), encoding="utf-8")


def _write(p: Path, content: str) -> Path:
    p.write_text(content, encoding="utf-8")
    return p


def _write_enriched(session_id: str, pair_id: str, side: str, content: str) -> None:
    from backend.app.services.stage_comparison import paths as paths_mod
    p = paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


class _Provider:
    name = "mock"

    def __init__(self, changes=1):
        self._changes = changes

    def check_availability(self):
        return True, None

    def invoke(self, *, system_prompt, user_prompt, model, timeout_sec, work_dir=None):
        from backend.app.services.stage_comparison.text_llm_provider import ProviderResult
        payload = {
            "status": "done",
            "summary": "тест",
            "changes": [
                {
                    "id": f"chg_{i}", "source": "text", "type": "material_changed",
                    "category": "electrical", "severity": "high",
                    "title": f"change {i}", "summary": "5x10 → 5x16",
                    "old_value": "5x10", "new_value": "5x16",
                    "evidence_left": {"quote": "5x10"}, "evidence_right": {"quote": "5x16"},
                }
                for i in range(self._changes)
            ],
            "warnings": [],
        }
        return ProviderResult(status="done", raw_response=json.dumps(payload),
                              duration_sec=0.01, provider=self.name, model=model)


# ─── 1. Чистые тесты модуля ─────────────────────────────────────────────────
def test_flags_for_profile_default_all_false():
    flags = ap.flags_for_profile("default")
    assert flags is not None and all(v is False for v in flags.values())


def test_flags_for_profile_rich_all_true():
    flags = ap.flags_for_profile("rich_grsh")
    assert flags is not None and all(v is True for v in flags.values())


def test_classify_profile():
    assert ap.classify_profile({"graphic_structured_extraction": False,
                                "block_pdf_source": False,
                                "grsh_feeder_extraction": False}) == "default"
    assert ap.classify_profile({"graphic_structured_extraction": True,
                                "block_pdf_source": True,
                                "grsh_feeder_extraction": True}) == "rich_grsh"
    assert ap.classify_profile({"graphic_structured_extraction": True,
                                "block_pdf_source": False,
                                "grsh_feeder_extraction": False}) == "custom"


def test_flag_enabled_override_does_not_touch_env(monkeypatch):
    # env выключен → flag_enabled False
    assert ap.flag_enabled(ap.GRSH_FEEDER_FLAG) is False
    with ap.profile_override(ap.flags_for_profile("rich_grsh")):
        assert ap.flag_enabled(ap.GRSH_FEEDER_FLAG) is True
        assert ap.classify_profile(ap.current_flags()) == "rich_grsh"
    # после выхода — снова env (default), env-переменные не выставлены
    assert ap.flag_enabled(ap.GRSH_FEEDER_FLAG) is False
    import os
    assert os.environ.get(ap.GRSH_FEEDER_FLAG) in (None, "")


def test_profile_override_for_none_is_noop():
    with ap.profile_override(ap.flags_for_profile("rich_grsh")):
        # внутренний None-override НЕ должен затирать внешний rich-контекст
        with ap.profile_override_for(None):
            assert ap.classify_profile(ap.current_flags()) == "rich_grsh"


def test_has_dense_graphics():
    assert ap.has_dense_graphics("... dense_scheme ...") is True
    assert ap.has_dense_graphics("plain text", "GRSH_FEEDERS table") is True
    assert ap.has_dense_graphics("обычный текст без схем") is False


# ─── 2. Запись профиля в результат ──────────────────────────────────────────
def test_result_records_default_profile(tmp_path):
    from backend.app.services.stage_comparison import enriched_comparison as ec
    _make_pair("s_def", "p1", tmp_path)
    _write_enriched("s_def", "p1", "left", "LEFT")
    _write_enriched("s_def", "p1", "right", "RIGHT")
    res = ec.run_enriched_comparison("s_def", "p1", provider=_Provider())
    assert res["status"] == "done"
    # Критерий 1 + 2 + 5: профиль записан, флаги false.
    assert res["analysis_profile"] == "default"
    assert res["analysis_profile_label"] == "Быстрый режим"
    assert res["profile_flags"] == {"graphic_structured_extraction": False,
                                    "block_pdf_source": False,
                                    "grsh_feeder_extraction": False}
    assert "profile_created_at" in res and res["profile_source"] in ("default", "env")
    # И на диске.
    from backend.app.services.stage_comparison import paths as paths_mod
    saved = json.loads(paths_mod.enriched_comparison_result_path("s_def", "p1").read_text(encoding="utf-8"))
    assert saved["analysis_profile"] == "default"


def test_result_records_rich_profile(tmp_path):
    from backend.app.services.stage_comparison import enriched_comparison as ec
    _make_pair("s_rich", "p1", tmp_path)
    _write_enriched("s_rich", "p1", "left", "LEFT")
    _write_enriched("s_rich", "p1", "right", "RIGHT")
    # Критерий 3 + 8: rich только явным override (env остаётся default).
    res = ec.run_enriched_comparison("s_rich", "p1", provider=_Provider(),
                                     analysis_profile="rich_grsh")
    assert res["analysis_profile"] == "rich_grsh"
    assert res["analysis_profile_label"] == "Глубокий ГРЩ"
    assert res["profile_flags"] == {"graphic_structured_extraction": True,
                                    "block_pdf_source": True,
                                    "grsh_feeder_extraction": True}


# ─── 3. Защита от перезаписи rich → default (критерий 4) ─────────────────────
def test_default_rerun_does_not_overwrite_rich_without_confirm(tmp_path):
    from backend.app.services.stage_comparison import enriched_comparison as ec
    from backend.app.services.stage_comparison import paths as paths_mod
    _make_pair("s_guard", "p1", tmp_path)
    _write_enriched("s_guard", "p1", "left", "LEFT")
    _write_enriched("s_guard", "p1", "right", "RIGHT")
    # rich сначала
    ec.run_enriched_comparison("s_guard", "p1", provider=_Provider(changes=3),
                               analysis_profile="rich_grsh")
    # default force rerun без подтверждения → не должен затереть
    res = ec.run_enriched_comparison("s_guard", "p1", provider=_Provider(changes=1),
                                     force=True)
    assert res.get("profile_downgrade_blocked") is True
    assert res["analysis_profile"] == "rich_grsh"
    saved = json.loads(paths_mod.enriched_comparison_result_path("s_guard", "p1").read_text(encoding="utf-8"))
    assert saved["analysis_profile"] == "rich_grsh"  # на диске остался rich
    assert len(saved["changes"]) == 3
    # с явным подтверждением — перезапись разрешена
    res2 = ec.run_enriched_comparison("s_guard", "p1", provider=_Provider(changes=1),
                                      force=True, allow_profile_downgrade=True)
    assert res2["analysis_profile"] == "default"
    assert not res2.get("profile_downgrade_blocked")


# ─── 4. Warning для dense GRSH при default (критерий 6) ──────────────────────
def test_dense_grsh_warning_on_default_profile(tmp_path):
    from backend.app.services.stage_comparison import enriched_comparison as ec
    _make_pair("s_dense", "p1", tmp_path)
    # enriched MD содержит маркер плотной схемы.
    _write_enriched("s_dense", "p1", "left", "## block / dense_scheme / ... ЩА-1")
    _write_enriched("s_dense", "p1", "right", "## block / dense_scheme / ... ЩА-2")
    res = ec.run_enriched_comparison("s_dense", "p1", provider=_Provider())
    assert res["analysis_profile"] == "default"
    assert res.get("dense_graphics_default_profile") is True
    assert any("плотные однолинейные" in w for w in (res.get("warnings") or []))


def test_no_dense_warning_for_rich_profile(tmp_path):
    from backend.app.services.stage_comparison import enriched_comparison as ec
    _make_pair("s_dense_rich", "p1", tmp_path)
    _write_enriched("s_dense_rich", "p1", "left", "dense_scheme ЩА-1")
    _write_enriched("s_dense_rich", "p1", "right", "dense_scheme ЩА-2")
    res = ec.run_enriched_comparison("s_dense_rich", "p1", provider=_Provider(),
                                     analysis_profile="rich_grsh")
    assert res["analysis_profile"] == "rich_grsh"
    assert not res.get("dense_graphics_default_profile")


# ─── 5. Batch job профиль (критерии 7 + 8) ───────────────────────────────────
def test_batch_default_stays_default(tmp_path):
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    _make_pair("s_batch", "p1", tmp_path)
    job = jobs.create_unified_job("s_batch", scope="selected", pair_ids=["p1"],
                                  confirm=True)
    assert job.get("analysis_profile") is None
    assert job.get("allow_profile_downgrade") is False


def test_batch_rich_only_explicit(tmp_path):
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    _make_pair("s_batch2", "p1", tmp_path)
    job = jobs.create_unified_job("s_batch2", scope="selected", pair_ids=["p1"],
                                  analysis_profile="rich_grsh", confirm=True)
    assert job.get("analysis_profile") == "rich_grsh"


# ─── 6. API отдаёт profile metadata (критерий 5) ─────────────────────────────
def test_v2_changes_exposes_profile_metadata(tmp_path):
    from backend.app.services.stage_comparison import enriched_comparison as ec
    from backend.app.services.stage_comparison import v2_review
    _make_pair("s_api", "p1", tmp_path)
    _write_enriched("s_api", "p1", "left", "LEFT")
    _write_enriched("s_api", "p1", "right", "RIGHT")
    ec.run_enriched_comparison("s_api", "p1", provider=_Provider())
    out = v2_review.build_pair_v2_changes("s_api", "p1")
    assert "analysis_profile" in out
    assert out["analysis_profile"]["analysis_profile"] == "default"
    assert out["analysis_profile"]["analysis_profile_label"] == "Быстрый режим"
