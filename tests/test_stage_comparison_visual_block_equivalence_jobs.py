# -*- coding: utf-8 -*-
"""Тесты recompute-only job-слоя visual_block_equivalence (Stage 3A).

Покрывает (см. задачу Stage 3A):
  1. job по одной паре вызывает runner один раз;
  2. job по списку пар вызывает runner для каждой пары;
  3. ошибка на одной паре не валит job — пара failed, остальные идут;
  4. summary агрегируется корректно по всем парам;
  5. cancel до старта и во время обработки останавливает дальнейшие пары;
  6. get/list возвращают корректный статус;
  7. job не запускает Qwen/Opus/pipeline (runner инъектируется; default runner —
     mark-only Stage 2); модуль не импортирует FastAPI/router;
  8. всё на tmp/mocks — никаких файлов в живом comparison/sessions.

Per-pair runner всегда замокан → ни store, ни PDF, ни cv2, ни Qwen/Opus.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import visual_block_equivalence_jobs as j


# ─── изоляция: registry сброшен, диск → tmp (живой comparison/ не трогается) ──


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))
    j._reset_registry_for_tests()
    yield
    j._reset_registry_for_tests()


def _summary(*, links_total=1, identical=1, changed=0, skipped=0):
    compared = identical + changed
    return {
        "links_total": links_total,
        "links_compared": compared,
        "identical_visual": identical,
        "minor_render_noise": 0,
        "changed_visual": changed,
        "uncertain": 0,
        "render_failed": 0,
        "skipped": skipped,
        "potential_qwen_saved": identical,
        "potential_opus_blocks_removed": identical,
    }


def _make_runner(calls: list, *, report=None, raise_on=None):
    """Мок per-pair runner. Сигнатура совпадает с run_pair_visual_block_equivalence."""
    def runner(session_id, pair_id, *, cfg=None, write_artifact=False,
               write_debug=False, **kw):
        calls.append(pair_id)
        if raise_on and pair_id == raise_on:
            raise RuntimeError("mock pair boom")
        return {"summary": (report or _summary())}
    return runner


# ─── 1. одна пара → runner один раз ──────────────────────────────────────────


def test_single_pair_runs_runner_once():
    calls: list = []
    job = j.start_visual_block_equivalence_job(
        "sess", scope="pair", pair_ids=["P1"], write_artifact=False,
        runner_fn=_make_runner(calls))
    assert calls == ["P1"]
    assert job["status"] == j.JOB_COMPLETED
    assert job["type"] == "visual_block_equivalence"
    assert job["scope"] == "pair"
    assert job["total_pairs"] == 1
    assert job["processed_pairs"] == 1
    assert job["pairs"][0]["status"] == j.PAIR_COMPLETED
    assert job["enforced"] is False


def test_scope_pair_requires_exactly_one():
    with pytest.raises(ValueError):
        j.create_visual_block_equivalence_job("s", scope="pair", pair_ids=["P1", "P2"])
    with pytest.raises(ValueError):
        j.create_visual_block_equivalence_job("s", scope="pair", pair_ids=[])


# ─── 2. список пар → runner на каждую ────────────────────────────────────────


def test_selected_list_runs_each_pair():
    calls: list = []
    job = j.start_visual_block_equivalence_job(
        "sess", scope="selected", pair_ids=["P1", "P2", "P3"], write_artifact=False,
        runner_fn=_make_runner(calls))
    assert calls == ["P1", "P2", "P3"]
    assert job["status"] == j.JOB_COMPLETED
    assert job["processed_pairs"] == 3
    assert all(p["status"] == j.PAIR_COMPLETED for p in job["pairs"])


def test_selected_requires_non_empty():
    with pytest.raises(ValueError):
        j.create_visual_block_equivalence_job("s", scope="selected", pair_ids=[])


def test_selected_dedupes_preserving_order():
    job = j.create_visual_block_equivalence_job(
        "s", scope="selected", pair_ids=["P1", "P2", "P1", "P3"], write_artifact=False)
    assert [p["pair_id"] for p in job["pairs"]] == ["P1", "P2", "P3"]


def test_scope_session_resolves_all_pairs(monkeypatch):
    from backend.app.services.stage_comparison import store as store_mod
    monkeypatch.setattr(store_mod, "get_session", lambda s: {
        "pairs": [{"id": "A"}, {"id": "B"}, {"id": "C"}],
    })
    calls: list = []
    job = j.start_visual_block_equivalence_job(
        "sess", scope="session", write_artifact=False, runner_fn=_make_runner(calls))
    assert calls == ["A", "B", "C"]
    assert job["scope"] == "session"
    assert job["total_pairs"] == 3


def test_scope_session_missing_session_raises(monkeypatch):
    from backend.app.services.stage_comparison import store as store_mod
    monkeypatch.setattr(store_mod, "get_session", lambda s: None)
    with pytest.raises(KeyError):
        j.create_visual_block_equivalence_job("nope", scope="session")


# ─── 3. ошибка одной пары не валит job ───────────────────────────────────────


def test_pair_error_does_not_fail_whole_job():
    calls: list = []
    job = j.start_visual_block_equivalence_job(
        "sess", scope="selected", pair_ids=["P1", "P2", "P3"], write_artifact=False,
        runner_fn=_make_runner(calls, raise_on="P2"))
    assert calls == ["P1", "P2", "P3"]            # все пары попытались выполниться
    assert job["status"] == j.JOB_COMPLETED        # job НЕ failed
    by_id = {p["pair_id"]: p for p in job["pairs"]}
    assert by_id["P1"]["status"] == j.PAIR_COMPLETED
    assert by_id["P2"]["status"] == j.PAIR_FAILED
    assert by_id["P2"]["error"] and "boom" in by_id["P2"]["error"]
    assert by_id["P3"]["status"] == j.PAIR_COMPLETED
    assert job["failed_pairs"] == 1
    assert job["processed_pairs"] == 3


# ─── 4. агрегация summary ────────────────────────────────────────────────────


def test_summary_aggregates_across_pairs():
    calls: list = []
    runner = _make_runner(calls, report=_summary(links_total=2, identical=1, changed=1))
    job = j.start_visual_block_equivalence_job(
        "sess", scope="selected", pair_ids=["P1", "P2"], write_artifact=False,
        runner_fn=runner)
    s = job["summary"]
    assert s["links_total"] == 4          # 2 пары × 2
    assert s["links_compared"] == 4       # 2 пары × (1 identical + 1 changed)
    assert s["identical_visual"] == 2
    assert s["changed_visual"] == 2
    assert s["potential_qwen_saved"] == 2
    assert s["potential_opus_blocks_removed"] == 2
    # все обязательные ключи summary присутствуют
    for key in ("links_total", "links_compared", "identical_visual",
                "minor_render_noise", "changed_visual", "uncertain", "skipped",
                "potential_qwen_saved", "potential_opus_blocks_removed"):
        assert key in s


def test_failed_pair_contributes_no_summary():
    calls: list = []
    runner = _make_runner(calls, raise_on="P2")
    job = j.start_visual_block_equivalence_job(
        "sess", scope="selected", pair_ids=["P1", "P2"], write_artifact=False,
        runner_fn=runner)
    # только P1 даёт summary (identical=1)
    assert job["summary"]["identical_visual"] == 1
    assert job["summary"]["potential_qwen_saved"] == 1


# ─── 5. cancel ───────────────────────────────────────────────────────────────


def test_cancel_before_start_runs_nothing():
    calls: list = []
    job = j.create_visual_block_equivalence_job(
        "sess", scope="selected", pair_ids=["P1", "P2"], write_artifact=False)
    jid = job["job_id"]
    cancelled = j.cancel_visual_block_equivalence_job(jid)
    assert cancelled["status"] == j.JOB_CANCELLED
    assert cancelled["cancel_requested"] is True

    done = j.run_visual_block_equivalence_job(jid, runner_fn=_make_runner(calls))
    assert calls == []                              # runner ни разу
    assert done["status"] == j.JOB_CANCELLED
    assert done["processed_pairs"] == 0
    assert all(p["status"] == j.PAIR_CANCELLED for p in done["pairs"])


def test_cancel_during_processing_stops_remaining():
    calls: list = []
    job = j.create_visual_block_equivalence_job(
        "sess", scope="selected", pair_ids=["P1", "P2", "P3"], write_artifact=False)
    jid = job["job_id"]

    def runner(session_id, pair_id, *, cfg=None, write_artifact=False,
               write_debug=False, **kw):
        calls.append(pair_id)
        if pair_id == "P2":
            j.cancel_visual_block_equivalence_job(jid)   # отмена в середине
        return {"summary": _summary()}

    done = j.run_visual_block_equivalence_job(jid, runner_fn=runner)
    assert calls == ["P1", "P2"]                    # P3 не запускался
    assert done["status"] == j.JOB_CANCELLED
    by_id = {p["pair_id"]: p["status"] for p in done["pairs"]}
    assert by_id["P1"] == j.PAIR_COMPLETED
    assert by_id["P2"] == j.PAIR_COMPLETED
    assert by_id["P3"] == j.PAIR_CANCELLED
    assert done["processed_pairs"] == 2


def test_cancel_unknown_job_returns_none():
    assert j.cancel_visual_block_equivalence_job("vbej_nope") is None


# ─── 6. get / list ───────────────────────────────────────────────────────────


def test_get_and_list_return_status():
    calls: list = []
    job = j.start_visual_block_equivalence_job(
        "sessX", scope="pair", pair_ids=["P1"], write_artifact=False,
        runner_fn=_make_runner(calls))
    jid = job["job_id"]

    got = j.get_visual_block_equivalence_job(jid)
    assert got is not None and got["job_id"] == jid
    assert got["status"] == j.JOB_COMPLETED

    listed = j.list_visual_block_equivalence_jobs("sessX")
    assert [x["job_id"] for x in listed] == [jid]
    # фильтр по другой сессии — пусто
    assert j.list_visual_block_equivalence_jobs("other") == []
    # без фильтра — всё
    assert any(x["job_id"] == jid for x in j.list_visual_block_equivalence_jobs())


def test_get_unknown_job_returns_none():
    assert j.get_visual_block_equivalence_job("vbej_missing") is None


def test_get_returns_deepcopy_not_registry_alias():
    job = j.start_visual_block_equivalence_job(
        "s", scope="pair", pair_ids=["P1"], write_artifact=False,
        runner_fn=_make_runner([]))
    snap = j.get_visual_block_equivalence_job(job["job_id"])
    snap["status"] = "MUTATED"
    again = j.get_visual_block_equivalence_job(job["job_id"])
    assert again["status"] == j.JOB_COMPLETED       # внешняя мутация не протекла


def test_cleanup_finished_jobs():
    for n in range(5):
        j.start_visual_block_equivalence_job(
            "s", scope="pair", pair_ids=[f"P{n}"], write_artifact=False,
            runner_fn=_make_runner([]))
    removed = j.cleanup_finished_jobs(keep_last=2)
    assert removed == 3
    assert len(j.list_visual_block_equivalence_jobs()) == 2


# ─── 7. без Qwen/Opus/pipeline/FastAPI ───────────────────────────────────────


def test_default_runner_is_stage2_mark_only():
    from backend.app.services.stage_comparison import visual_block_equivalence as vbe
    # дефолтный runner — это mark-only Stage 2, а НЕ Qwen/Opus/pipeline.
    assert j.run_pair_visual_block_equivalence is vbe.run_pair_visual_block_equivalence


def _collect_imports(path: Path) -> set[str]:
    """Имена модулей из ACTUAL import-выражений (AST) — не из docstring/комментов.

    Включает как top-level, так и lazy (внутри функций) импорты."""
    import ast

    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                names.add(a.name)
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            names.add(mod)
            for a in node.names:
                names.add(f"{mod}.{a.name}" if mod else a.name)
    return names


def test_module_does_not_import_fastapi_router_or_heavy_pipeline():
    imports = _collect_imports(Path(j.__file__))
    blob = "\n".join(sorted(imports))
    # модуль не должен ИМПОРТИРОВАТЬ FastAPI/router/Qwen/Opus/pipeline/md_enrichment
    for token in ("fastapi", "api.routers", "routers",
                  "md_enrichment_jobs", "pipeline_queue", "enriched_comparison",
                  "graphic_llm", "unified_analysis"):
        assert token not in blob, f"unexpected import dependency: {token}\nimports={sorted(imports)}"
    # дозволенные зависимости — только Stage 2 + общие store/paths
    assert any("visual_block_equivalence" in m for m in imports)


def test_no_router_module_imported_by_jobs(monkeypatch):
    # запуск job не должен импортировать роутер stage_comparison
    import sys
    sys.modules.pop("backend.app.api.routers.stage_comparison", None)
    j.start_visual_block_equivalence_job(
        "s", scope="pair", pair_ids=["P1"], write_artifact=False,
        runner_fn=_make_runner([]))
    assert "backend.app.api.routers.stage_comparison" not in sys.modules


# ─── 8. персист опционален (default OFF), при включении пишет только в tmp ────


def test_no_disk_writes_by_default(tmp_path):
    # default jobs_cfg → persist_to_disk False; write_artifact False → ноль файлов
    j.start_visual_block_equivalence_job(
        "sessD", scope="pair", pair_ids=["P1"], write_artifact=False,
        runner_fn=_make_runner([]))
    # под COMPARISON_ROOT (tmp) ничего не записалось
    jobs_dir = tmp_path / "sessions" / "sessD" / "jobs"
    assert not jobs_dir.exists()


def test_optional_persist_writes_only_to_tmp(tmp_path):
    cfg = j.VisualBlockEquivalenceJobsConfig(persist_to_disk=True)
    job = j.start_visual_block_equivalence_job(
        "sessP", scope="pair", pair_ids=["P1"], jobs_cfg=cfg, write_artifact=False,
        runner_fn=_make_runner([]))
    from backend.app.services.stage_comparison import paths as paths_mod
    p = paths_mod.job_json_path("sessP", job["job_id"])
    assert p.exists()
    # под tmp, не в живом дереве
    assert str(tmp_path) in str(p)
    on_disk = json.loads(p.read_text(encoding="utf-8"))
    assert on_disk["type"] == "visual_block_equivalence"
    assert on_disk["status"] == j.JOB_COMPLETED
    # никаких висящих .tmp после атомарной замены
    assert not p.with_suffix(".json.tmp").exists()


def test_async_runner_processes_pairs_and_aggregates():
    # async-обёртка (для будущих endpoint'ов Stage 3B) тоже проходит по парам
    import asyncio
    calls: list = []
    job = j.create_visual_block_equivalence_job(
        "sessAsync", scope="selected", pair_ids=["P1", "P2"], write_artifact=False)
    done = asyncio.run(
        j.run_visual_block_equivalence_job_async(job["job_id"], runner_fn=_make_runner(calls)))
    assert calls == ["P1", "P2"]
    assert done["status"] == j.JOB_COMPLETED
    assert done["processed_pairs"] == 2
    assert done["summary"]["identical_visual"] == 2


def test_artifact_path_recorded_when_write_artifact_true():
    job = j.start_visual_block_equivalence_job(
        "sessA", scope="pair", pair_ids=["P1"], write_artifact=True,
        runner_fn=_make_runner([]))
    ap = job["pairs"][0]["artifact_path"]
    assert ap is not None and ap.endswith("visual_block_equivalence.json")
