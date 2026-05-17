"""
test_pipeline_summary_status_normalization.py
---------------------------------------------
Регресс-тесты для нормализации статусов в `_build_pipeline_summary` и
смежных правил `audit_logger.update_pipeline_log`.

Проблема, которую закрываем: проект в списке отображался как «56/56 — Готово»,
но в детальном «Статус конвейера» строки «Кроп блоков» и
«Gemma OCR enrichment» оставались без отметки выполнения, потому что
pipeline_log.json содержал legacy-`prepare` без `crop_blocks` или
status="partial" для Gemma. UI рисует «·» для status, который не входит в
список done/error/running/skipped/pending.

После фикса:
- pipeline_summary нормализует статусы для crop_blocks и gemma_enrichment;
- partial mode с blocks_ok==blocks_total и blocks_failed==0 трактуется
  как done;
- audit_logger проставляет completed_at для status="partial" и считает его
  терминальным.

Run:
    python -m pytest tests/test_pipeline_summary_status_normalization.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── Helpers ────────────────────────────────────────────────────────────


def _write_pipeline_log(output_dir: Path, stages: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "pipeline_log.json").write_text(
        json.dumps({"version": 1, "stages": stages}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _write_blocks_index(output_dir: Path, blocks: int = 56) -> None:
    """Создать blocks_gemma_100/index.json со списком image-блоков."""
    blocks_dir = output_dir / "blocks_gemma_100"
    blocks_dir.mkdir(parents=True, exist_ok=True)
    (blocks_dir / "index.json").write_text(
        json.dumps({
            "blocks": [
                {"block_id": f"b{i}", "block_type": "image"}
                for i in range(blocks)
            ],
            "total_blocks": blocks,
        }, ensure_ascii=False),
        encoding="utf-8",
    )


def _patch_gemma_state(
    monkeypatch: pytest.MonkeyPatch,
    *,
    ready: bool = True,
    status: str = "ok",
    blocks_ok: int = 56,
    blocks_total: int = 56,
    uncovered: list | None = None,
    migration: bool = False,
    high_detail_skipped_large: int = 0,
) -> None:
    """Перехватить вызовы evaluate/detect, чтобы не строить полную фикстуру."""
    from backend.app.services.common import project_service as ps

    def _fake_evaluate(project_dir, project_info=None):  # noqa: ARG001
        return {
            "ready": ready,
            "status": status,
            "blocks_ok": blocks_ok,
            "blocks_total": blocks_total,
            "uncovered_block_ids": list(uncovered or []),
            "high_detail_skipped_large": high_detail_skipped_large,
        }

    def _fake_migration(project_dir, *, gemma_state=None, project_info=None):  # noqa: ARG001
        return {"migration_required": migration}

    monkeypatch.setattr(ps, "evaluate_gemma_enrichment", _fake_evaluate)
    monkeypatch.setattr(ps, "detect_gemma_migration_state", _fake_migration)


def _summary_by_key(summary: list[dict]) -> dict[str, dict]:
    return {s["key"]: s for s in summary}


# ─── 1. Gemma partial with full coverage → done ─────────────────────────


def test_gemma_partial_with_full_coverage_normalized_to_done(tmp_path, monkeypatch):
    """pipeline_log status=partial, detail blocks_ok=56/total=56, failed=0
    должно отображаться как done в pipeline_summary."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "gemma_enrichment": {
            "status": "partial",
            "message": "partial: OK (56/56 блоков, 659s) — 0 упали; partial mode допущен",
            "detail": {"blocks_ok": 56, "blocks_total": 56, "blocks_failed": 0},
        },
    })
    # Gemma "ready" + status="ok", uncovered=[], partial_failed=0 → done.
    _patch_gemma_state(
        monkeypatch,
        ready=True, status="ok",
        blocks_ok=56, blocks_total=56, uncovered=[],
    )
    # Сводка без blocks_failed → читается как 0.
    (output_dir / "gemma_enrichment_summary.json").write_text(
        json.dumps({"blocks_failed": 0}), encoding="utf-8",
    )

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    entry = summary["gemma_enrichment"]
    assert entry["status"] == "done"
    # Пользовательский message для done содержит счётчик блоков, но НЕ
    # должен начинаться со слова partial и не должен пугать "непокрытыми".
    user_message = entry.get("message") or ""
    assert "56/56" in user_message
    assert not user_message.lower().startswith("partial")
    assert "непокрытые блоки попадут в отчёт" not in user_message
    # Сырой message из pipeline_log сохранён как raw_message (для debug).
    assert entry.get("raw_message") == (
        "partial: OK (56/56 блоков, 659s) — 0 упали; partial mode допущен"
    )


# ─── 2. Gemma partial with failures → partial ───────────────────────────


def test_gemma_partial_with_failures_stays_partial(tmp_path, monkeypatch):
    """pipeline_log status=partial, detail blocks_ok=25/total=26, blocks_failed=1
    должно остаться partial."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "gemma_enrichment": {
            "status": "partial",
            "message": "partial: OK (25/26 блоков) — 1 блок упал",
            "detail": {"blocks_ok": 25, "blocks_total": 26, "blocks_failed": 1},
        },
    })
    # Gemma ready=True но status=partial и есть uncovered → partial.
    _patch_gemma_state(
        monkeypatch,
        ready=True, status="partial",
        blocks_ok=25, blocks_total=26, uncovered=["b25"],
    )
    (output_dir / "gemma_enrichment_summary.json").write_text(
        json.dumps({"blocks_failed": 1}), encoding="utf-8",
    )

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    entry = summary["gemma_enrichment"]
    assert entry["status"] == "partial"
    # Для реального partial (failed>0, uncovered != []) пользовательский
    # message должен явно сообщить про предупреждения и перечислить uncovered.
    user_message = entry.get("message") or ""
    assert "Выполнено с предупреждениями" in user_message
    assert "25/26" in user_message
    assert "b25" in user_message


# ─── 3. crop_blocks отсутствует в log, но есть blocks_gemma_100/index.json ─


def test_crop_blocks_inferred_from_filesystem(tmp_path, monkeypatch):
    """В pipeline_log нет crop_blocks, но index.json существует → done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        # Только text_analysis, чтобы log не был пустым.
        "text_analysis": {"status": "done"},
    })
    _write_blocks_index(output_dir, blocks=56)
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["crop_blocks"]["status"] == "done"


# ─── 4. legacy "prepare" done → crop_blocks done ────────────────────────


def test_crop_blocks_from_legacy_prepare_done(tmp_path, monkeypatch):
    """pipeline_log содержит prepare=done, нет crop_blocks → crop_blocks=done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "prepare": {"status": "done", "message": "Подготовка завершена"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["crop_blocks"]["status"] == "done"


# ─── 5. update_pipeline_log(status="partial") пишет completed_at ────────


def test_update_pipeline_log_partial_sets_completed_at(tmp_path, monkeypatch):
    from backend.app.services.common import audit_logger as al

    output_dir = tmp_path / "_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(al, "_project_output_dir", lambda pid: output_dir)
    # Заблокировать WS-broadcast (синхронные вызовы asyncio.ensure_future).
    monkeypatch.setattr(
        al, "_get_pipeline_status",
        lambda *a, **kw: None, raising=False,
    )

    al.update_pipeline_log(
        "proj", "gemma_enrichment", "partial",
        message="partial: OK (56/56 блоков, 659s)",
        detail={"blocks_ok": 56, "blocks_total": 56, "blocks_failed": 0},
    )
    log_path = output_dir / "pipeline_log.json"
    log = json.loads(log_path.read_text(encoding="utf-8"))
    stage = log["stages"]["gemma_enrichment"]
    assert stage["status"] == "partial"
    assert stage.get("completed_at"), "completed_at должен быть выставлен для partial"


# ─── 6. partial считается терминальным при cascade-reset ────────────────


def test_update_pipeline_log_partial_is_terminal_for_cascade(tmp_path, monkeypatch):
    """Если этап выше перезапускается (running), ниже стоящий partial
    тоже должен сбрасываться, потому что partial — терминальный статус."""
    from backend.app.services.common import audit_logger as al

    output_dir = tmp_path / "_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    # Стартовое состояние: gemma_enrichment partial, text_analysis done.
    (output_dir / "pipeline_log.json").write_text(json.dumps({
        "version": 1,
        "stages": {
            "gemma_enrichment": {"status": "partial"},
            "text_analysis": {"status": "done", "completed_at": "2026-05-16T00:00:00"},
        },
    }), encoding="utf-8")
    monkeypatch.setattr(al, "_project_output_dir", lambda pid: output_dir)
    monkeypatch.setattr(
        al, "_get_pipeline_status",
        lambda *a, **kw: None, raising=False,
    )

    al.update_pipeline_log("proj", "crop_blocks", "running")
    log = json.loads((output_dir / "pipeline_log.json").read_text(encoding="utf-8"))
    # gemma_enrichment ниже crop_blocks по порядку → должен быть сброшен,
    # т.к. partial теперь терминальный.
    assert "gemma_enrichment" not in log["stages"], (
        "partial должен быть терминальным и сбрасываться при cascade-reset"
    )


# ─── 7. Стандартный case: ничего не меняется ────────────────────────────


def test_normal_done_status_pass_through(tmp_path, monkeypatch):
    """Если crop_blocks и gemma_enrichment явно done в логе — статус остаётся."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "gemma_enrichment": {"status": "done"},
    })
    _patch_gemma_state(monkeypatch, ready=True, status="ok",
                       blocks_ok=10, blocks_total=10, uncovered=[])
    (output_dir / "gemma_enrichment_summary.json").write_text(
        json.dumps({"blocks_failed": 0}), encoding="utf-8",
    )

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["crop_blocks"]["status"] == "done"
    assert summary["gemma_enrichment"]["status"] == "done"


# ─── UI/CSS sanity: partial и migration_required корректно отрисованы ──


def test_frontend_index_renders_partial_icon():
    """В шаблоне есть явный case для partial/migration_required → ⚠."""
    text = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
    assert "s.status === 'partial'" in text
    assert "s.status === 'migration_required'" in text
    # Само значок (⚠) выводится из шаблона.
    assert "⚠" in text


def test_styles_css_has_partial_classes():
    """В CSS есть .ps-partial / .ps-migration_required / .pipeline-stage.step-partial."""
    text = (_ROOT / "frontend" / "static" / "css" / "styles.css").read_text(encoding="utf-8")
    assert ".ps-partial" in text
    assert ".ps-migration_required" in text
    assert ".pipeline-stage.step-partial" in text


def test_app_js_step_class_maps_migration_required():
    """stepClass для migration_required возвращает step-partial."""
    text = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    assert "status === 'migration_required'" in text
    # Проверяем, что есть branch step-partial для migration_required.
    assert "step-partial" in text


# ─── 8. migration_required прокидывается в UI ───────────────────────────


def test_gemma_migration_required_surfaced(tmp_path, monkeypatch):
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "gemma_enrichment": {"status": "error"},  # из старой схемы
    })
    _patch_gemma_state(monkeypatch, ready=False, status="schema_mismatch",
                       migration=True)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["gemma_enrichment"]["status"] == "migration_required"


# ─── 9. Gemma summary status=partial из-за high_detail_skipped_large_block ──


def test_gemma_partial_with_skipped_large_block_normalized_to_done(tmp_path, monkeypatch):
    """Реальный кейс проекта 25.12.22_13АВ-РД-АР3-К5-К6_в2.pdf:
    summary.status = "partial" (один блок имеет coverage_status =
    "high_detail_skipped_large_block"), но blocks_ok == blocks_total и
    blocks_failed == 0, а uncovered_block_ids пустой (skipped_large_block
    считается покрытым, потому что final_profile = gemma_100_base).
    Ожидание: статус нормализуется в done.
    """
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "gemma_enrichment": {
            "status": "partial",
            "message": (
                "partial: OK (26/26 блоков, 353s) — 0 упали; "
                "partial mode допущен, непокрытые блоки попадут в отчёт"
            ),
            "detail": {"blocks_ok": 26, "blocks_total": 26, "blocks_failed": 0},
        },
    })
    # blocks_upgraded_to_300=[…] не делает этап partial само по себе;
    # gemma_state.ready=True + uncovered=[] + blocks_failed=0 → done.
    _patch_gemma_state(
        monkeypatch,
        ready=True, status="partial",
        blocks_ok=26, blocks_total=26, uncovered=[],
        high_detail_skipped_large=1,
    )
    (output_dir / "gemma_enrichment_summary.json").write_text(
        json.dumps({
            "blocks_failed": 0,
            "high_detail_skipped_large": 1,
            "blocks_upgraded_to_300": ["9XVF-7RCK-VAK"],
            "large_block_skipped_ids": ["UFGL-X3FG-RGJ"],
        }),
        encoding="utf-8",
    )

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    entry = summary["gemma_enrichment"]
    assert entry["status"] == "done", (
        f"Ожидали done, получили {entry['status']}: {entry}"
    )
    # Пользовательский message для done должен:
    #   - НЕ начинаться со слова partial
    #   - НЕ обещать «непокрытые блоки попадут в отчёт» (uncovered=[])
    #   - упоминать high-detail skipped_large_block с пометкой про fallback на
    #     базовый профиль gemma_100_base
    user_message = entry.get("message") or ""
    assert not user_message.lower().startswith("partial"), user_message
    assert "непокрытые блоки попадут в отчёт" not in user_message, user_message
    assert "26/26" in user_message
    assert "0 упали" in user_message
    assert "high-detail" in user_message
    assert "gemma_100_base" in user_message
    # raw_message сохраняется отдельно для debug/UI «показать оригинал».
    assert entry.get("raw_message") and "partial mode допущен" in entry["raw_message"]


# ─── 10. crop_blocks done по валидному blocks_gemma_100/index.json ─────────


def test_crop_blocks_done_when_only_filesystem_evidence(tmp_path, monkeypatch):
    """Если pipeline_log не содержит ни crop_blocks, ни prepare, но на диске
    есть _output/blocks_gemma_100/index.json со списком блоков и errors == 0 —
    crop_blocks должен быть done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {})
    _write_blocks_index(output_dir, blocks=26)
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["crop_blocks"]["status"] == "done"


# ─── 11. Старый pipeline_log "partial" не важнее свежего summary done ──────


def test_stale_pipeline_log_partial_does_not_override_summary_done(tmp_path, monkeypatch):
    """Если в pipeline_log temporary fail (partial), но новый summary говорит
    26/26 OK без uncovered и без failed — этап должен быть done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "gemma_enrichment": {
            "status": "partial",
            "message": "временный partial от прошлого прогона",
            "detail": {"blocks_ok": 20, "blocks_total": 26, "blocks_failed": 6},
        },
    })
    # Текущий summary — успешный.
    _patch_gemma_state(
        monkeypatch,
        ready=True, status="ok",
        blocks_ok=26, blocks_total=26, uncovered=[],
    )
    (output_dir / "gemma_enrichment_summary.json").write_text(
        json.dumps({"blocks_failed": 0}), encoding="utf-8",
    )

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["gemma_enrichment"]["status"] == "done"


# ─── 12. Static parity: webapp/static дублирует frontend/static ────────────


def test_webapp_static_index_has_partial_branch():
    """webapp/static/index.html (legacy SPA) должен иметь те же ветки
    partial/migration_required, что и frontend/index.html."""
    text = (_ROOT / "webapp" / "static" / "index.html").read_text(encoding="utf-8")
    assert "s.status === 'partial'" in text
    assert "s.status === 'migration_required'" in text
    assert "⚠" in text


def test_webapp_static_css_has_partial_classes():
    text = (_ROOT / "webapp" / "static" / "css" / "styles.css").read_text(encoding="utf-8")
    assert ".ps-partial" in text
    assert ".ps-migration_required" in text
    assert ".pipeline-stage.step-partial" in text


def test_webapp_static_app_js_step_class_maps_migration_required():
    text = (_ROOT / "webapp" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    assert "status === 'migration_required'" in text
    assert "step-partial" in text


# ─── 13. Legacy v4-aliases для AR-проектов (АР0.3, АР0.4, АР1.1-К2) ───────


def test_v4_extraction_alias_maps_to_block_analysis(tmp_path, monkeypatch):
    """В pipeline_log есть legacy v4_extraction вместо block_analysis →
    block_analysis должен быть done с user-friendly message."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "text_analysis": {"status": "done"},
        "v4_extraction": {"status": "done", "message": "Все 4 пакетов OK"},
        "v4_memory": {"status": "done"},
        "v4_candidates": {"status": "done"},
        "v4_formatter": {"status": "done"},
        "findings_critic": {"status": "done", "message": "OK"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    ba = summary["block_analysis"]
    assert ba["status"] == "done", ba
    # message не пустой
    assert ba.get("message"), f"empty message: {ba}"
    # raw_stage_key показывает источник для UI/debug
    assert ba.get("raw_stage_key") == "v4_extraction"


def test_v4_formatter_alias_maps_to_findings_merge(tmp_path, monkeypatch):
    """v4_formatter (legacy) в pipeline_log + 03_findings.json на диске →
    findings_merge должен быть done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "text_analysis": {"status": "done"},
        "v4_formatter": {"status": "done"},
        "findings_critic": {"status": "done"},
    })
    # 03_findings.json как доказательство
    (output_dir / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-001"}]}), encoding="utf-8",
    )
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    fm = summary["findings_merge"]
    assert fm["status"] == "done", fm
    assert fm.get("message"), f"empty message: {fm}"


# ─── 14. Artifact-based inference: 02_blocks_analysis.json ───────────────


def test_block_analysis_done_from_artifact(tmp_path, monkeypatch):
    """В pipeline_log нет block_analysis (ни канонический, ни alias),
    но 02_blocks_analysis.json существует → block_analysis должен быть done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "text_analysis": {"status": "done"},
    })
    (output_dir / "02_blocks_analysis.json").write_text(
        json.dumps({"blocks": [{"page": 1}]}), encoding="utf-8",
    )
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["block_analysis"]["status"] == "done"
    assert summary["block_analysis"].get("message")


def test_text_analysis_done_from_artifact(tmp_path, monkeypatch):
    """В pipeline_log нет text_analysis, но 01_text_analysis.json есть → done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {"crop_blocks": {"status": "done"}})
    (output_dir / "01_text_analysis.json").write_text(
        json.dumps({"text": "x"}), encoding="utf-8",
    )
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["text_analysis"]["status"] == "done"


# ─── 15. Downstream-inference ────────────────────────────────────────────


def test_findings_merge_inferred_from_findings_critic_done(tmp_path, monkeypatch):
    """findings_critic done, но findings_merge ничего не написал в лог →
    findings_merge должен подняться до done (предыдущий обязательный этап).
    """
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "text_analysis": {"status": "done"},
        "findings_critic": {"status": "done", "message": "OK"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["findings_merge"]["status"] == "done"
    assert summary["findings_merge"].get("message")


def test_text_analysis_inferred_from_block_analysis_done(tmp_path, monkeypatch):
    """block_analysis done → text_analysis должен быть done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "block_analysis": {"status": "done"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["text_analysis"]["status"] == "done"


# ─── 16. Fallback message для непустого статуса ──────────────────────────


def test_skipped_status_gets_fallback_message_if_empty(tmp_path, monkeypatch):
    """Если в pipeline_log статус skipped без message → message заполнен
    fallback'ом (не пустой)."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "optimization_corrector": {"status": "skipped"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["optimization_corrector"]["status"] == "skipped"
    assert summary["optimization_corrector"].get("message"), "fallback message ожидается"


def test_skipped_with_existing_message_preserves_it(tmp_path, monkeypatch):
    """Если в pipeline_log skipped + явный message — не перезаписываем fallback'ом."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "optimization_corrector": {
            "status": "skipped",
            "message": "Все предложения прошли Critic",
        },
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["optimization_corrector"]["message"] == "Все предложения прошли Critic"


# ─── 17. block_retry: partial с message о нечитаемых блоках ───────────────


def test_block_retry_done_with_residual_unreadable_preserved(tmp_path, monkeypatch):
    """pipeline_log: block_retry done, message «Осталось 1 нечитаемых…» —
    сохраняем как есть, не дёргаем fallback."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "block_retry": {
            "status": "done",
            "message": "Осталось 1 нечитаемых (макс разрешение)",
        },
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary")

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    br = summary["block_retry"]
    assert br["status"] == "done"
    assert "Осталось 1 нечитаемых" in br["message"]


# ─── 18a. Gemma для legacy v4-проектов → skipped, не pending ─────────────


def test_gemma_legacy_v4_marker_returns_skipped(tmp_path, monkeypatch):
    """Legacy v4-проект (есть v4_extraction в pipeline_log) без gemma artifacts
    должен показать gemma_enrichment как skipped с понятным сообщением,
    а НЕ pending (иначе UI рисует ○ как незавершённое)."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "text_analysis": {"status": "done"},
        "v4_extraction": {"status": "done", "message": "Все 4 пакетов OK"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_blocks",
                       migration=False)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    ge = summary["gemma_enrichment"]
    assert ge["status"] == "skipped", ge
    msg = ge.get("message") or ""
    assert "Пропущено" in msg or "legacy" in msg.lower(), (
        f"Ожидали skipped-message про legacy, получили: {msg!r}"
    )


def test_gemma_legacy_via_downstream_done_returns_skipped(tmp_path, monkeypatch):
    """Если в pipeline_log нет v4-маркера, но downstream block_analysis done
    через 02_blocks_analysis.json — gemma_enrichment тоже skipped (legacy)."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "text_analysis": {"status": "done"},
    })
    # Артефакт block_analysis на диске.
    (output_dir / "02_blocks_analysis.json").write_text(
        json.dumps({"blocks": [{"page": 1}]}), encoding="utf-8",
    )
    _patch_gemma_state(monkeypatch, ready=False, status="missing_blocks",
                       migration=False)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    ge = summary["gemma_enrichment"]
    assert ge["status"] == "skipped", ge
    assert ge.get("message"), "message не должен быть пустым"


def test_gemma_qwen_enrichment_legacy_marker_returns_skipped(tmp_path, monkeypatch):
    """KJ-like проект: в pipeline_log есть qwen_enrichment (старый Qwen).
    Если новый Gemma не запущен и migration_required не выставлено —
    показываем skipped, а не pending."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "qwen_enrichment": {"status": "done", "message": "Qwen OK"},
        "text_analysis": {"status": "done"},
        "block_analysis": {"status": "done"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_blocks",
                       migration=False)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    ge = summary["gemma_enrichment"]
    assert ge["status"] == "skipped", ge


def test_gemma_modern_project_without_evidence_stays_pending(tmp_path, monkeypatch):
    """Современный проект (нет legacy v4/qwen маркеров, downstream НЕ done,
    нет 02_blocks_analysis.json, нет gemma_summary): gemma_enrichment ДОЛЖЕН
    остаться pending, чтобы UI показывал ○ как «ещё не выполнено»."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_blocks",
                       migration=False)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    ge = summary["gemma_enrichment"]
    assert ge["status"] == "pending", ge


def test_gemma_modern_project_no_lifecycle_evidence_stays_pending(tmp_path, monkeypatch):
    """Аналогично: только что зарегистрированный проект, в pipeline_log
    нет ни одного маркера → pending."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {})
    _patch_gemma_state(monkeypatch, ready=False, status="missing_blocks",
                       migration=False)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    ge = summary["gemma_enrichment"]
    assert ge["status"] == "pending", ge


# ─── 18. Full AR-проект сценарий: legacy v4 + missing block_retry ─────────


def test_legacy_ar_project_v4_full_pipeline(tmp_path, monkeypatch):
    """Реальный кейс AR проекта (13АВ-РД-АР0.3-ПА Изм.1):
    pipeline_log использует v4_extraction/memory/candidates/formatter вместо
    block_analysis/findings_merge. На ФС есть 03_findings.json, optimization.json
    и т.д. UI ожидает все ключевые этапы как done."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done", "message": "Pre-cropped"},
        "text_analysis": {"status": "done", "message": "OK"},
        "v4_extraction": {"status": "done", "message": "Все 4 пакетов OK"},
        "v4_memory": {"status": "done"},
        "v4_candidates": {"status": "done"},
        "v4_formatter": {"status": "done"},
        "findings_critic": {"status": "done", "message": "OK"},
        "findings_corrector": {"status": "done", "message": "OK (3 чанков)"},
        "norm_verify": {"status": "done", "message": "OK"},
        "optimization": {"status": "done", "message": "OK"},
        "optimization_critic": {"status": "done", "message": "OK"},
        "optimization_corrector": {"status": "done", "message": "OK"},
        "excel": {"status": "done", "message": "OK"},
    })
    # Артефакты, которые в реальном проекте есть на диске.
    (output_dir / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-001"}]}), encoding="utf-8",
    )
    (output_dir / "optimization.json").write_text(
        json.dumps({"meta": {}, "items": []}), encoding="utf-8",
    )
    _patch_gemma_state(monkeypatch, ready=False, status="missing_summary",
                       migration=True)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    # block_analysis должен быть done через v4_extraction alias
    assert summary["block_analysis"]["status"] == "done"
    # findings_merge → через downstream (findings_critic/corrector done) или
    # через 03_findings.json артефакт
    assert summary["findings_merge"]["status"] == "done"
    # block_retry в v4-конвейере не существовал. Раз есть legacy-маркеры
    # (v4_extraction), block_retry должен быть skipped с понятным message,
    # а не pending (○).
    br = summary["block_retry"]
    assert br["status"] == "skipped", br
    assert br.get("message"), f"empty message: {br}"
    assert "legacy" in br["message"].lower(), br["message"]
    # Все нижестоящие — done из лога
    for k in ("findings_critic", "findings_corrector", "norm_verify",
              "optimization", "optimization_critic", "optimization_corrector",
              "excel"):
        assert summary[k]["status"] == "done", f"{k} not done: {summary[k]}"
        assert summary[k].get("message"), f"{k} message empty: {summary[k]}"


# ─── 19. block_retry: legacy v4 без block_retry в логе → skipped ──────────


def test_block_retry_legacy_v4_marker_returns_skipped(tmp_path, monkeypatch):
    """v4-проект: в pipeline_log есть v4_extraction, нет block_retry →
    block_retry должен быть skipped, не pending (этап не существовал в v4)."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "text_analysis": {"status": "done"},
        "v4_extraction": {"status": "done"},
        "v4_formatter": {"status": "done"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_blocks",
                       migration=False)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    br = summary["block_retry"]
    assert br["status"] == "skipped", br
    assert br.get("message"), "block_retry message не должен быть пустым"


def test_block_retry_modern_project_without_legacy_marker_stays_pending(
        tmp_path, monkeypatch):
    """Современный проект без legacy-маркеров и без записи block_retry →
    block_retry остаётся pending (этап ещё не выполнен в этой эпохе)."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "gemma_enrichment": {"status": "done"},
        "text_analysis": {"status": "done"},
        "block_analysis": {"status": "done"},
    })
    # gemma_state=ready (modern проект)
    _patch_gemma_state(monkeypatch, ready=True, status="ok",
                       blocks_ok=10, blocks_total=10, uncovered=[])
    (output_dir / "gemma_enrichment_summary.json").write_text(
        json.dumps({"blocks_failed": 0}), encoding="utf-8",
    )

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    assert summary["block_retry"]["status"] == "pending", summary["block_retry"]


def test_block_retry_explicit_done_in_log_not_overridden(tmp_path, monkeypatch):
    """Если block_retry уже записан в pipeline_log как done (или partial),
    legacy-heuristic его не должен переписывать."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        # legacy маркер всё ещё может быть, но block_retry явно done
        "v4_extraction": {"status": "done"},
        "block_retry": {"status": "done", "message": "Все блоки читаемы"},
    })
    _patch_gemma_state(monkeypatch, ready=False, status="missing_blocks",
                       migration=False)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    br = summary["block_retry"]
    assert br["status"] == "done", br
    assert br["message"] == "Все блоки читаемы"


# ─── 20. migration_required: проект с попытанным Gemma → не skipped ───────


def test_gemma_migration_required_with_legacy_artifacts_kept(
        tmp_path, monkeypatch):
    """AR0.2-like кейс: detect_gemma_migration_state говорит
    migration_required=True (проект был частично прогнан через Gemma, но
    schema v2 не закрыта). legacy-skipped heuristic не должен этого
    переписывать в skipped — UI должен показать «требуется миграция»."""
    project_dir = tmp_path / "proj"
    output_dir = project_dir / "_output"
    _write_pipeline_log(output_dir, {
        "crop_blocks": {"status": "done"},
        "text_analysis": {"status": "done"},
        "block_analysis": {"status": "done"},
        "block_retry": {"status": "done"},
    })
    # detect_gemma_migration_state → migration_required=True
    _patch_gemma_state(monkeypatch, ready=False, status="missing_blocks",
                       migration=True)

    from backend.app.services.common.project_service import _build_pipeline_summary
    summary = _summary_by_key(_build_pipeline_summary(output_dir))
    ge = summary["gemma_enrichment"]
    assert ge["status"] == "migration_required", ge
    # migration_required всегда имеет message (fallback или явный)
    assert ge.get("message"), ge
