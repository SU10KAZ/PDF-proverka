"""
test_gemma_stage_disabled.py
----------------------------
GEMMA_STAGE_DISABLED: полное отключение OCR-прогона стадии Gemma.

Контракт:
- ВСЕ image-блоки получают синтетический enrichment: с вектор-слоем → чистый
  вектор-текст, скан/растр → placeholder «OCR отключён» (не None — иначе
  Stage 02 пропустит блок вместо анализа по изображению);
- стадия «сухая»: ни adaptive reload, ни preflight, ни CHANDRA_BASE_URL
  не требуются (полная независимость от LM Studio/ngrok);
- summary валиден (schema v2), coverage ok → все downstream-гейты проходят;
- ИНВАРИАНТ: без BLOCK_SOURCE_ROUTER_ENABLED флаг не действует (иначе
  covered-блоки остались бы слепыми на Stage 02).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import backend.app.core.config as config_mod
import backend.app.pipeline.stages.block_grounding.block_source_router as router_mod
import backend.app.pipeline.stages.gemma_enrichment.gemma_enrich as gemma_enrich_mod
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BASE_CROP_POLICY,
    validate_gemma_summary,
)


def _make_project(tmp_path: Path, *, block_ids: list[str]) -> Path:
    """Минимальный проект: MD + валидный blocks_gemma_100/index.json (без PNG —
    в сухом режиме до чтения картинок дело не доходит)."""
    proj = tmp_path / "proj"
    (proj / "_output" / "blocks_gemma_100").mkdir(parents=True)
    md_blocks = "\n".join(
        f"### BLOCK [IMAGE]: {bid}\n\n[IMAGE] Описание Chandra блока {bid}.\n"
        for bid in block_ids
    )
    (proj / "doc_document.md").write_text(
        f"# Документ\n\n## СТРАНИЦА 1\n\n{md_blocks}\n", encoding="utf-8"
    )
    index_payload = {
        **GEMMA_BASE_CROP_POLICY,
        "blocks": [
            {"block_id": bid, "block_type": "image", "page": i + 1}
            for i, bid in enumerate(block_ids)
        ],
    }
    (proj / "_output" / "blocks_gemma_100" / "index.json").write_text(
        json.dumps(index_payload, ensure_ascii=False), encoding="utf-8"
    )
    return proj


def _forbid_lm_studio(monkeypatch):
    """Любое обращение к LM Studio (reload/preflight) — провал теста."""

    async def _boom(*args, **kwargs):  # pragma: no cover — сам вызов = ошибка
        raise AssertionError("Обращение к LM Studio в сухом режиме запрещено")

    monkeypatch.setattr(gemma_enrich_mod, "_adaptive_reload_to_context", _boom)
    monkeypatch.setattr(gemma_enrich_mod, "_preflight_loaded_context", _boom)


def _set_flags(monkeypatch, *, disabled: bool, router: bool, skip_vector: bool = False):
    monkeypatch.setattr(config_mod, "GEMMA_STAGE_DISABLED", disabled, raising=False)
    monkeypatch.setattr(config_mod, "BLOCK_SOURCE_ROUTER_ENABLED", router, raising=False)
    monkeypatch.setattr(
        config_mod, "GEMMA_SKIP_VECTOR_BLOCKS_ENABLED", skip_vector, raising=False
    )


def test_ocr_disabled_enrichment_shape():
    enr = gemma_enrich_mod._ocr_disabled_enrichment()
    # не None и с текстом — иначе Stage 02 скипнет блок (skip_no_enrich)
    assert isinstance(enr, dict)
    assert enr["_gemma_skipped"] == "stage_disabled"
    assert "изображени" in enr["notes"]


@pytest.mark.asyncio
async def test_stage_disabled_dry_run_all_blocks_skipped(tmp_path, monkeypatch):
    """Вектор-блок + скан: оба пропущены, ни одного обращения к LM Studio,
    CHANDRA_BASE_URL не нужен, summary валиден."""
    proj = _make_project(tmp_path, block_ids=["b_vec", "b_scan"])
    _set_flags(monkeypatch, disabled=True, router=True)
    _forbid_lm_studio(monkeypatch)
    monkeypatch.delenv("CHANDRA_BASE_URL", raising=False)
    monkeypatch.delenv("LMSTUDIO_BASE_URL", raising=False)
    monkeypatch.setattr(
        router_mod, "vector_covered_block_ids",
        lambda output_dir: {"b_vec": "QF1 ВА47-29 C16\nКабель ВВГнг 3x2.5"},
    )

    events: list[dict] = []

    async def _cb(event: dict) -> None:
        events.append(event)

    summary = await gemma_enrich_mod.enrich_project(proj, progress_cb=_cb)

    assert summary["status"] == "ok"
    assert summary["blocks_total"] == 2
    assert summary["blocks_failed"] == 0
    by_id = {b["block_id"]: b for b in summary["blocks"]}
    assert by_id["b_vec"]["coverage_status"] == "ok"
    assert by_id["b_scan"]["coverage_status"] == "ok"
    # телеметрия различает covered-блок и слепой placeholder скана
    assert by_id["b_vec"]["base_response_source"] == "vector_skip"
    assert by_id["b_scan"]["base_response_source"] == "stage_disabled_skip"

    # событие полного отключения с раскладкой вектор/скан
    disabled_events = [e for e in events if e.get("type") == "gemma_stage_disabled"]
    assert disabled_events and disabled_events[0]["vector_blocks"] == 1
    assert disabled_events[0]["image_only_blocks"] == 1

    # summary проходит боевой валидатор (downstream-гейты не изменятся)
    validation = validate_gemma_summary(proj, md_path=proj / "doc_document.md")
    assert validation["valid"] is True

    # MD получил оба синтетических enrichment
    md_text = (proj / "doc_document.md").read_text(encoding="utf-8")
    assert "ВВГнг 3x2.5" in md_text                                   # вектор-текст covered-блока
    assert "OCR-обогащение блока отключено" in md_text                # placeholder скана
    assert "вектор-слоя PDF (Gemma пропущена" in md_text              # subject covered-блока
    # в мастер-MD не утекают имена env-флагов
    assert "GEMMA_STAGE_DISABLED" not in md_text


@pytest.mark.asyncio
async def test_no_image_blocks_needs_no_lm_studio(tmp_path, monkeypatch):
    """Текст-only проект (0 image-блоков): ветка no_blocks не требует
    CHANDRA_BASE_URL — base_url резолвится после неё."""
    proj = _make_project(tmp_path, block_ids=[])
    _set_flags(monkeypatch, disabled=True, router=True)
    _forbid_lm_studio(monkeypatch)
    monkeypatch.delenv("CHANDRA_BASE_URL", raising=False)
    monkeypatch.delenv("LMSTUDIO_BASE_URL", raising=False)

    summary = await gemma_enrich_mod.enrich_project(proj)

    assert summary["status"] == "no_blocks"
    assert summary["blocks_total"] == 0


@pytest.mark.asyncio
async def test_stage_disabled_requires_router(tmp_path, monkeypatch):
    """ИНВАРИАНТ: без роутера флаг игнорируется → стадия идёт обычным путём
    (здесь это видно по hard error на отсутствующем CHANDRA_BASE_URL)."""
    proj = _make_project(tmp_path, block_ids=["b1"])
    _set_flags(monkeypatch, disabled=True, router=False)
    monkeypatch.delenv("CHANDRA_BASE_URL", raising=False)
    monkeypatch.delenv("LMSTUDIO_BASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="CHANDRA_BASE_URL"):
        await gemma_enrich_mod.enrich_project(proj)


@pytest.mark.asyncio
async def test_vector_skip_partial_still_needs_lm_studio(tmp_path, monkeypatch):
    """Регресс старого поведения: только GEMMA_SKIP_VECTOR_BLOCKS_ENABLED и есть
    скан без вектор-слоя → стадия НЕ сухая, CHANDRA_BASE_URL обязателен."""
    proj = _make_project(tmp_path, block_ids=["b_vec", "b_scan"])
    _set_flags(monkeypatch, disabled=False, router=True, skip_vector=True)
    monkeypatch.delenv("CHANDRA_BASE_URL", raising=False)
    monkeypatch.delenv("LMSTUDIO_BASE_URL", raising=False)
    monkeypatch.setattr(
        router_mod, "vector_covered_block_ids",
        lambda output_dir: {"b_vec": "QF1 ВА47-29 C16 — длинный вектор-текст блока"},
    )

    with pytest.raises(RuntimeError, match="CHANDRA_BASE_URL"):
        await gemma_enrich_mod.enrich_project(proj)


@pytest.mark.asyncio
async def test_retry_failed_blocks_dry_when_stage_disabled(tmp_path, monkeypatch):
    """retry на старом summary с unresolved high-detail при отключённой стадии НЕ идёт
    в LM Studio — делегирует в «сухой» enrich_project(force=True)."""
    proj = _make_project(tmp_path, block_ids=["b1"])
    _set_flags(monkeypatch, disabled=True, router=True)
    _forbid_lm_studio(monkeypatch)
    monkeypatch.delenv("CHANDRA_BASE_URL", raising=False)
    monkeypatch.delenv("LMSTUDIO_BASE_URL", raising=False)
    monkeypatch.setattr(
        router_mod, "vector_covered_block_ids",
        lambda output_dir: {},
    )
    # старый валидный summary: base ok, но high-detail failed → раньше шли в HD-ветку
    monkeypatch.setattr(
        gemma_enrich_mod, "validate_gemma_summary",
        lambda *a, **kw: {"valid": True},
    )
    # force=True в делегированном enrich_project зовёт recrop — ортогонально проверке
    monkeypatch.setattr(
        gemma_enrich_mod, "_ensure_crop_index",
        lambda project_dir, **kw: json.loads(
            (proj / "_output" / "blocks_gemma_100" / "index.json").read_text(encoding="utf-8")
        ),
    )
    (proj / "_output" / "gemma_enrichment_summary.json").write_text(
        json.dumps({
            "blocks_total": 1,
            "blocks": [{
                "block_id": "b1",
                "base_status": "ok",
                "high_detail_status": "failed",
            }],
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    result = await gemma_enrich_mod.retry_failed_blocks(proj)

    assert result["retry_mode"] == "stage_disabled_dry_rerun"
    assert result["status"] == "ok"
    assert result["blocks_failed"] == 0


def test_locate_finds_pdf_in_v2_primary_run_dir(tmp_path):
    """_locate: output_dir = <version>/03_analysis/runs/<run_id> (v2-primary) — PDF в
    <version>/02_work на 3 уровня выше. Раньше один od.parent его не находил, и
    роутер/пропуск Gemma молча выключались на всех v2-primary прогонах."""
    version = tmp_path / "v002"
    run_dir = version / "03_analysis" / "runs" / "run_abc"
    run_dir.mkdir(parents=True)
    (run_dir / "document_graph.json").write_text("{}", encoding="utf-8")
    (version / "02_work").mkdir(parents=True)
    (version / "02_work" / "document.pdf").write_bytes(b"%PDF-1.4 fake")

    pdf, dgp = router_mod._locate(run_dir)

    assert pdf == version / "02_work" / "document.pdf"
    assert dgp == run_dir / "document_graph.json"


def test_locate_legacy_layout_unchanged(tmp_path):
    """_locate: legacy projects/<name>/_output — PDF в корне проекта под любым именем."""
    proj = tmp_path / "proj"
    out = proj / "_output"
    out.mkdir(parents=True)
    (out / "document_graph.json").write_text("{}", encoding="utf-8")
    (proj / "чертёж_ЭОМ.pdf").write_bytes(b"%PDF-1.4 fake")

    pdf, dgp = router_mod._locate(out)

    assert pdf == proj / "чертёж_ЭОМ.pdf"
    assert dgp == out / "document_graph.json"


@pytest.mark.asyncio
async def test_vector_skip_full_coverage_goes_dry(tmp_path, monkeypatch):
    """Бонус старого флага: если вектор-слой покрыл ВСЕ блоки, стадия сухая и
    без GEMMA_STAGE_DISABLED — LM Studio не нужен."""
    proj = _make_project(tmp_path, block_ids=["b_vec"])
    _set_flags(monkeypatch, disabled=False, router=True, skip_vector=True)
    _forbid_lm_studio(monkeypatch)
    monkeypatch.delenv("CHANDRA_BASE_URL", raising=False)
    monkeypatch.delenv("LMSTUDIO_BASE_URL", raising=False)
    monkeypatch.setattr(
        router_mod, "vector_covered_block_ids",
        lambda output_dir: {"b_vec": "ЩО-1 ввод ВА88-32 100А, отходящие 12 линий"},
    )

    summary = await gemma_enrich_mod.enrich_project(proj)

    assert summary["status"] == "ok"
    assert summary["blocks_total"] == 1
    assert summary["blocks_failed"] == 0
