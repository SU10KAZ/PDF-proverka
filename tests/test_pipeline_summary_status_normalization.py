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
