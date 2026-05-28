"""Unit tests for MD image enrichment pipeline.

Покрытие:
  1. MD parser находит image/imagine-блоки в Chandra-формате;
  2. parser сохраняет text-блоки;
  3. enriched MD содержит original imagine + Qwen description;
  4. Original MD не меняется после enrich_side;
  5. run_model=False не вызывает provider;
  6. cache работает (повторный вызов берёт из кеша);
  7. provider error → status=error в items, enriched MD всё равно создаётся;
  8. endpoint GET md-enrichment возвращает not_run;
  9. endpoint POST md-enrichment dry-run не вызывает модель;
  10. job без confirm не запускается;
  11. describe_image_local не использует external paid hosts;
  12. graphic-diff compare_images_local не сломан (smoke).
"""
from __future__ import annotations

import asyncio
import io
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_test"))
    (tmp_path / "comparison_test").mkdir(exist_ok=True)
    yield


@pytest.fixture
def _local_env(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://test-ngrok.example.com")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen/qwen3.6-35b-a3b")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_FALLBACK_MODEL", "qwen3.6-35b-a3b-mtp")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_AUTH", "basic")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROTECT_MODELS", "chandra-ocr-2")
    monkeypatch.setenv("NGROK_AUTH_USER", "test_user")
    monkeypatch.setenv("NGROK_AUTH_PASS", "test_pass")


def _png(width: int = 32, height: int = 32, color: tuple[int, int, int] = (200, 200, 200)) -> bytes:
    from PIL import Image
    img = Image.new("RGB", (width, height), color=color)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _write_png(path: Path, **kw) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_png(**kw))
    return path


CHANDRA_MD_SAMPLE = """### СТРАНИЦА 1

### BLOCK [TEXT]: T-001
Описание раздела: спецификация электрощита.
Кабельная линия ВВГнг(А)-FRLS 5x16.

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001
Узел крепления фасада.

### BLOCK [TEXT]: T-002
Дополнительный комментарий.

### СТРАНИЦА 2

### BLOCK [IMAGE]: img-002
Штамп: ОАО "Тест"
Стадия: РД
Шифр: 13АВ
"""


# ─── 1, 2: parser находит image/text-блоки ───────────────────────────────


def test_parser_finds_image_blocks_in_chandra_md():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    blocks = m.parse_md_blocks(CHANDRA_MD_SAMPLE)
    image_blocks = [b for b in blocks if b.is_image]
    assert len(image_blocks) == 2
    assert image_blocks[0].block_id == "img-001"
    assert image_blocks[0].page == 1
    assert image_blocks[1].block_id == "img-002"
    assert image_blocks[1].page == 2
    # Image blocks numbered per-page
    assert image_blocks[0].image_order_on_page == 1
    assert image_blocks[1].image_order_on_page == 1


def test_parser_preserves_text_blocks():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    blocks = m.parse_md_blocks(CHANDRA_MD_SAMPLE)
    text_blocks = [b for b in blocks if b.kind == "text"]
    # Page heading + text-блок1 формируют первый text-блок
    assert text_blocks
    text_joined = "".join(b.text for b in text_blocks)
    assert "Кабельная линия ВВГнг(А)-FRLS 5x16" in text_joined
    assert "Дополнительный комментарий" in text_joined


def test_parser_handles_image_tag_block():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    md = """Some text
<image>
description of image
</image>
Trailing text
"""
    blocks = m.parse_md_blocks(md)
    image_blocks = [b for b in blocks if b.is_image]
    assert len(image_blocks) == 1
    assert "description of image" in image_blocks[0].text


def test_parser_handles_image_fence():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    md = "Header\n```image\ndescription\n```\nFooter"
    blocks = m.parse_md_blocks(md)
    image_blocks = [b for b in blocks if b.is_image]
    assert len(image_blocks) == 1
    assert "description" in image_blocks[0].text


# ─── 3, 4: enriched MD content + original MD untouched ────────────────────


@pytest.mark.asyncio
async def test_enriched_md_contains_original_and_qwen(_local_env, tmp_path):
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")
    orig_hash = src_md.read_text(encoding="utf-8")

    # render: каждый block_id мапится в маленький PNG (разные цвета, чтобы
    # cache key получался разный — иначе sha256 совпадёт и второй блок
    # схватит cached описание первого).
    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    image_a = _write_png(crop_dir / "img-001.png", color=(10, 20, 30))
    image_b = _write_png(crop_dir / "img-002.png", color=(200, 100, 50))

    def render(side_block_id):
        return crop_dir / f"{side_block_id}.png"

    # describe: всегда успешный ответ
    async def fake_describe(image_path, prompt):
        return g.DescribeResult(
            status="done",
            provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b",
            model_used="qwen/qwen3.6-35b-a3b",
            fallback_used=False,
            parsed={
                "status": "done",
                "image_kind": "drawing",
                "summary": f"OK for {image_path.name}",
                "design_solutions": ["solution"],
                "materials": [],
                "equipment": [],
                "numeric_parameters": [],
                "requirements": [],
                "tables": [],
                "visible_text": [],
                "comparison_relevant_facts": [],
                "uncertainties": [],
                "confidence": 0.9,
            },
            raw_response_excerpt="raw",
            duration_sec=0.01,
        )

    # подсунем result.json с image-блоками
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [
            {"page_number": 1, "width": 1000, "height": 1000, "blocks": [
                {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
            ]},
            {"page_number": 2, "width": 1000, "height": 1000, "blocks": [
                {"id": "img-002", "block_type": "image", "coords_px": [0, 0, 100, 100]},
            ]},
        ],
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess1", "pair1", "left",
        md_path=str(src_md),
        result_json_path=str(result_json),
        render_crop=render,
        describe_fn=fake_describe,
        run_model=True,
    )

    assert summary.image_blocks == 2
    assert summary.described == 2
    assert summary.errors == 0
    assert summary.status == "done"

    # Original MD не меняется
    assert src_md.read_text(encoding="utf-8") == orig_hash

    # Enriched MD: новый формат replace_image_blocks_v1
    enriched_path = Path(summary.enriched_md_path)
    enriched = enriched_path.read_text(encoding="utf-8")
    # Wrapper present
    assert "QWEN_IMAGE_DESCRIPTION_START" in enriched
    assert "QWEN_IMAGE_DESCRIPTION_END" in enriched
    assert "format_version: replace_image_blocks_v1" in enriched
    # Старые маркеры append_v0 — удалены
    assert "original_imagine_start" not in enriched
    assert "original_imagine_end" not in enriched
    # Тело описаний
    assert "OK for img-001.png" in enriched
    assert "OK for img-002.png" in enriched
    # Текст оригинала (НЕ image-блок) тоже остался
    assert "Кабельная линия ВВГнг(А)-FRLS 5x16" in enriched


# ─── 5: run_model=False не вызывает provider ───────────────────────────


@pytest.mark.asyncio
async def test_dry_run_does_not_call_provider(_local_env, tmp_path):
    from backend.app.services.stage_comparison import md_image_enrichment as m

    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")

    calls = {"count": 0}

    async def fake_describe(image_path, prompt):
        calls["count"] += 1
        raise AssertionError("should not be called in dry-run")

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(1, 2, 3))
    _write_png(crop_dir / "img-002.png", color=(4, 5, 6))

    def render(side_block_id):
        return crop_dir / f"{side_block_id}.png"

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [
            {"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]}]},
            {"page_number": 2, "blocks": [{"id": "img-002", "block_type": "image", "coords_px": [0, 0, 100, 100]}]},
        ],
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess1", "pair1", "left",
        md_path=str(src_md),
        result_json_path=str(result_json),
        render_crop=render,
        describe_fn=fake_describe,
        run_model=False,
    )
    assert calls["count"] == 0
    assert summary.pending == 2
    assert summary.described == 0


# ─── 6: cache ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cache_skips_repeat_calls(_local_env, tmp_path):
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [TEXT]: T1
some text

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001
""", encoding="utf-8")

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")

    def render(side_block_id):
        return crop_dir / f"{side_block_id}.png"

    call_count = {"n": 0}

    async def fake_describe(image_path, prompt):
        call_count["n"] += 1
        return g.DescribeResult(
            status="done", provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
            parsed={"status": "done", "summary": "cached-test", "confidence": 1.0},
            raw_response_excerpt="raw", duration_sec=0.0,
        )

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]}]}]
    }), encoding="utf-8")

    # 1-й вызов: реальный
    s1 = await m.enrich_side(
        "sess1", "pair1", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=render, describe_fn=fake_describe, run_model=True,
    )
    assert call_count["n"] == 1
    assert s1.described == 1
    assert s1.from_cache == 0

    # 2-й вызов: должен взять из кеша
    s2 = await m.enrich_side(
        "sess1", "pair1", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=render, describe_fn=fake_describe, run_model=True,
    )
    assert call_count["n"] == 1, "Qwen вызвалась повторно вместо использования кеша"
    assert s2.described == 1
    assert s2.from_cache == 1


# ─── 7: provider error → status=error, enriched MD всё равно создаётся ───


@pytest.mark.asyncio
async def test_provider_error_records_status_error(_local_env, tmp_path):
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [TEXT]: T1
text

### BLOCK [IMAGE]: img-001
[IMAGE]
""", encoding="utf-8")

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")

    def render(side_block_id):
        return crop_dir / f"{side_block_id}.png"

    async def fake_describe(image_path, prompt):
        return g.DescribeResult(
            status="error", provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
            error="http_500",
        )

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]}]}]
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess1", "pair1", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=render, describe_fn=fake_describe, run_model=True,
    )
    assert summary.errors >= 1
    assert summary.status in ("error", "partial")
    # Enriched MD всё равно создаётся в новом replace_image_blocks_v1 формате.
    assert summary.enriched_md_path and Path(summary.enriched_md_path).exists()
    enriched = Path(summary.enriched_md_path).read_text(encoding="utf-8")
    assert "QWEN_IMAGE_DESCRIPTION_START" in enriched
    # Error блок остаётся в обёртке c явным status: error / error: …
    assert "status: error" in enriched
    assert "Графический блок не распознан" in enriched


# ─── 8: endpoint GET md-enrichment возвращает not_run ─────────────────────


def test_endpoint_get_returns_not_run(_local_env, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.stage_comparison import store

    # Подготовим сессию с парой
    fake_session = {
        "id": "s1",
        "pairs": [{
            "id": "p1",
            "status": "active",
            "left": {"md_path": str(tmp_path / "L.md")},
            "right": {"md_path": str(tmp_path / "R.md")},
        }],
    }
    monkeypatch.setattr(store, "get_session", lambda sid: fake_session if sid == "s1" else None)

    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)

    r = client.get("/api/stage-comparison/sessions/s1/pairs/p1/md-enrichment")
    assert r.status_code == 200
    body = r.json()
    assert body["pair_id"] == "p1"
    assert body["left"]["status"] == "not_run"
    assert body["right"]["status"] == "not_run"


# ─── 9: endpoint POST dry-run не вызывает модель ──────────────────────────


def test_endpoint_post_dry_run_does_not_call_model(_local_env, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.stage_comparison import store
    from backend.app.services.stage_comparison import graphic_llm_local as g

    md_left = tmp_path / "L.md"
    md_left.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")
    md_right = tmp_path / "R.md"
    md_right.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")

    fake_session = {
        "id": "s1",
        "pairs": [{
            "id": "p1",
            "status": "active",
            "left": {"md_path": str(md_left), "result_json_path": None},
            "right": {"md_path": str(md_right), "result_json_path": None},
        }],
    }
    monkeypatch.setattr(store, "get_session", lambda sid: fake_session if sid == "s1" else None)

    call_count = {"n": 0}
    async def boom(*a, **kw):
        call_count["n"] += 1
        raise AssertionError("describe_image_local should not be called in dry-run")
    monkeypatch.setattr(g, "describe_image_local", boom)

    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)

    r = client.post(
        "/api/stage-comparison/sessions/s1/pairs/p1/md-enrichment",
        json={"side": "both", "force": False, "run_model": False},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["pair_id"] == "p1"
    assert call_count["n"] == 0


# ─── 10: job без confirm не запускается ───────────────────────────────────


def test_job_without_confirm_is_rejected(_local_env, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.stage_comparison import store

    fake_session = {
        "id": "s1",
        "pairs": [{"id": "p1", "status": "active",
                   "left": {"md_path": str(tmp_path / "L.md")},
                   "right": {"md_path": str(tmp_path / "R.md")}}],
    }
    monkeypatch.setattr(store, "get_session", lambda sid: fake_session if sid == "s1" else None)

    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)

    r = client.post(
        "/api/stage-comparison/sessions/s1/md-enrichment-jobs",
        json={"scope": "session", "side": "both", "confirm": False},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "rejected_no_confirm"


# ─── 11: describe_image_local не использует external paid hosts ────────────


@pytest.mark.parametrize("base_url", [
    "https://openrouter.ai/api/v1",
    "https://api.openai.com/v1",
    "https://generativelanguage.googleapis.com",
    "https://api.anthropic.com/v1",
])
def test_describe_image_local_blocks_external_paid_hosts(monkeypatch, base_url, tmp_path):
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", base_url)
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "x")
    monkeypatch.setenv("NGROK_AUTH_USER", "u")
    monkeypatch.setenv("NGROK_AUTH_PASS", "p")

    img = tmp_path / "img.png"
    _write_png(img)

    cfg = g.load_local_graphic_llm_config()
    ok, reason = g.check_local_graphic_llm_available(cfg)
    assert not ok
    assert "external_paid_host_blocked" in (reason or "")

    res = asyncio.run(g.describe_image_local(img, "describe", cfg=cfg))
    assert res.status == "provider_unavailable"
    assert "external_paid_host_blocked" in (res.error or "")


# ─── 12: smoke — compare_images_local не сломан ───────────────────────────


def test_compare_images_local_still_works(_local_env, tmp_path):
    """Smoke: убедимся, что добавление describe_image_local не сломало signature
    или impl compare_images_local."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    cfg = g.load_local_graphic_llm_config()
    ok, reason = g.check_local_graphic_llm_available(cfg)
    assert ok, f"local provider should be available in tests, got {reason}"
    # compare_images_local — coroutine, не вызываем; только проверяем что
    # функция существует с правильной сигнатурой.
    import inspect
    sig = inspect.signature(g.compare_images_local)
    assert "left_image_path" in sig.parameters
    assert "right_image_path" in sig.parameters


# ─── Scheme analysis (v2_scheme_analysis) ─────────────────────────────────


def test_prompt_contains_scheme_analysis_instructions():
    """Prompt должен явно требовать анализ структурных/однолинейных схем."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p = m.QWEN_IMAGE_DESCRIPTION_PROMPT
    assert "электрическая" in p.lower() or "electrical_single_line" in p
    assert "hvac_air_flow" in p
    assert "water_or_liquid_flow" in p
    assert "automation_signal" in p
    assert "process_scheme" in p
    assert "structural_scheme" in p
    assert "scheme_analysis" in p
    assert "is_scheme" in p
    assert "nodes" in p and "connections" in p
    assert "sequence_summary" in p
    assert "independent_circuits" in p


def test_prompt_forbids_inventing_directions():
    """Prompt должен запрещать выдумывать направление, если оно не видно."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p = m.QWEN_IMAGE_DESCRIPTION_PROMPT
    # Должны явно требовать не выдумывать
    low = p.lower()
    assert "не выдумывай" in low
    # Должны разрешать «не определено» / «предположительно» для направлений
    assert ("не определено" in low) or ("не определены" in low) or ("предположительно" in low)


def test_prompt_version_bumped():
    """PROMPT_VERSION должен быть v4_compact (или новее).

    Бамп истории:
      * v2_scheme_analysis — добавлен scheme_analysis;
      * v3_no_ellipsis_chunking — анти-многоточие + continues/coverage_notes;
      * v4_compact — короткий prompt без агрессивных «ВНИМАНИЕ», explicit
        limits на массивы. Live benchmark 2026-05-26 на 4 heavy HVAC блоках:
        4/4 success vs 2/4 на v2, avg 27s vs 96s.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    assert m.PROMPT_VERSION == "v4_compact"


def test_prompt_v4_compact_has_explicit_array_limits():
    """Compact prompt должен явно указывать лимиты массивов, иначе
    модель пытается «перечислить всё» и упирается в max_tokens.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p = m.QWEN_IMAGE_DESCRIPTION_PROMPT
    # Прямые упоминания лимитов в человекочитаемом виде
    assert "nodes`: до 30" in p
    assert "connections`: до 30" in p
    assert "numeric_parameters`: до 40" in p
    assert "comparison_relevant" in p
    # И в COMPACT_PROMPT_LIMITS константе
    assert m.COMPACT_PROMPT_LIMITS["nodes"] == 30
    assert m.COMPACT_PROMPT_LIMITS["connections"] == 30
    assert m.COMPACT_PROMPT_LIMITS["uncertainties"] == 5


def test_prompt_v4_compact_dropped_aggressive_meta_instructions():
    """V4 не должен содержать V2-style повторы «ВНИМАНИЕ»/«ПОВТОРЯЮ»:
    они выталкивали модель в chain-of-thought, особенно fallback mtp.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p = m.QWEN_IMAGE_DESCRIPTION_PROMPT
    # Эти маркеры из V2 prompt'а должны исчезнуть
    assert "ВНИМАНИЕ:" not in p
    assert "ПОВТОРЯЮ:" not in p
    assert "ФИНАЛЬНАЯ ПРОВЕРКА" not in p


def test_cache_key_changes_with_prompt_version():
    """Cache key обязан меняться при смене PROMPT_VERSION — иначе старые
    cache-описания без scheme_analysis считались бы валидными."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    img_bytes = b"fake-image-bytes"
    k_v1 = m.compute_image_cache_key(img_bytes, "qwen/qwen3.6-35b-a3b", "v1")
    k_v2 = m.compute_image_cache_key(img_bytes, "qwen/qwen3.6-35b-a3b", "v2_scheme_analysis")
    assert k_v1 != k_v2


@pytest.mark.asyncio
async def test_old_cache_not_used_after_version_bump(_local_env, tmp_path):
    """Если в кеше лежит описание с прошлой prompt-version, оно не должно
    использоваться для текущей версии — описание перегенерируется."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1
### BLOCK [TEXT]: T
some text

### BLOCK [IMAGE]: img-001
[IMAGE]
""", encoding="utf-8")

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(100, 50, 25))
    img_bytes = (crop_dir / "img-001.png").read_bytes()

    # Положим в кеш «старое» описание под ключом prompt_version=v1
    old_key = m.compute_image_cache_key(img_bytes, "qwen/qwen3.6-35b-a3b", "v1")
    m.write_cache("sess1", "pair1", old_key, {
        "status": "done",
        "description": {"status": "done", "summary": "OLD-CACHED"},
        "model_used": "qwen/qwen3.6-35b-a3b",
        "raw_response_excerpt": "old",
        "cache_key": old_key,
        "prompt_version": "v1",
    })

    call_count = {"n": 0}

    async def fake(image_path, prompt):
        call_count["n"] += 1
        return g.DescribeResult(
            status="done", provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
            parsed={"status": "done", "summary": "NEW-RESPONSE"},
            raw_response_excerpt="raw", duration_sec=0.0,
        )

    result_json = tmp_path / "r.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}],
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess1", "pair1", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=lambda b: crop_dir / f"{b}.png",
        describe_fn=fake, run_model=True,
    )

    # Кеш под v1 не подхватился — модель вызвана и summary=NEW-RESPONSE
    assert call_count["n"] == 1
    enriched = Path(summary.enriched_md_path).read_text(encoding="utf-8")
    assert "NEW-RESPONSE" in enriched
    assert "OLD-CACHED" not in enriched


def test_build_enriched_md_renders_scheme_block():
    """build_enriched_md должен рендерить scheme_analysis-поля."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    block = m.MdBlock(kind="image", text="### BLOCK [IMAGE]: x", page=1, block_id="x", order=1, image_order_on_page=1)
    desc = {
        "order": 1,
        "model_used": "qwen/qwen3.6-35b-a3b",
        "description": {
            "status": "done",
            "summary": "Однолинейная схема ВРУ",
            "scheme_analysis": {
                "is_scheme": True,
                "scheme_type": "electrical_single_line",
                "flow_medium": "electricity",
                "nodes": [
                    {"id": "node_1", "label": "Ввод", "type": "input", "visible_mark": "0,4 кВ", "parameters": ["U=380 В"], "confidence": 0.9},
                    {"id": "node_2", "label": "ВРУ", "type": "panel", "confidence": 0.85},
                    {"id": "node_3", "label": "ГРЩ", "type": "panel"},
                ],
                "connections": [
                    {"from": "node_1", "to": "node_2", "direction": "left_to_right", "line_label": "L1", "evidence": "сплошная линия со стрелкой", "confidence": 0.8},
                    {"from": "node_2", "to": "node_3", "direction": "unknown", "line_label": "", "evidence": "линия без стрелки", "confidence": 0.5},
                ],
                "sequence_summary": [
                    "Ввод → ВРУ → ГРЩ → нагрузка",
                ],
                "independent_circuits": [
                    {"name": "Контур 1", "sequence": "Ввод → ВРУ → ГРЩ", "notes": "основной"},
                ],
                "comparison_relevant_scheme_facts": [
                    "Появилась байпасная линия между ВРУ и ГРЩ",
                ],
                "uncertainties": [
                    "Направление между ВРУ и ГРЩ не определено",
                ],
            },
        },
    }
    enriched = m.build_enriched_md([block], [desc])
    assert "Схемный анализ:" in enriched
    assert "Тип схемы: electrical_single_line" in enriched
    assert "Среда / поток: electricity" in enriched
    assert "Узлы:" in enriched
    assert "node_1" in enriched and "Ввод" in enriched
    assert "Связи:" in enriched
    assert "node_1 → node_2" in enriched
    assert "left_to_right" in enriched
    assert "Последовательность:" in enriched
    assert "Ввод → ВРУ → ГРЩ → нагрузка" in enriched
    assert "Независимые контуры:" in enriched
    assert "Существенно для сравнения (схема)" in enriched
    assert "Неопределённости (схема)" in enriched
    assert "Направление между ВРУ и ГРЩ не определено" in enriched


def test_build_enriched_md_handles_non_scheme():
    """Если is_scheme=false, enriched MD не должен ломаться и должен короткой
    меткой пометить, что схема не применима."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    block = m.MdBlock(kind="image", text="### BLOCK [IMAGE]: x", page=1, block_id="x", order=1, image_order_on_page=1)
    desc = {
        "order": 1,
        "model_used": "qwen/qwen3.6-35b-a3b",
        "description": {
            "status": "done",
            "summary": "Фасад здания, плоскость",
            "scheme_analysis": {
                "is_scheme": False,
                "scheme_type": "unknown_scheme",
                "flow_medium": "unknown",
                "nodes": [],
                "connections": [],
                "sequence_summary": [],
                "independent_circuits": [],
                "comparison_relevant_scheme_facts": [],
                "uncertainties": [],
            },
        },
    }
    enriched = m.build_enriched_md([block], [desc])
    # Не падаем, не выводим nodes/connections-разделы.
    assert "Узлы:" not in enriched
    assert "Связи:" not in enriched
    assert "Схемный анализ" in enriched
    assert "не применимо" in enriched.lower()


def test_parser_accepts_scheme_analysis_in_response():
    """parse_diff_json (общий парсер) принимает ответ с полем scheme_analysis."""
    from backend.app.services.stage_comparison.graphic_llm_local import parse_diff_json
    raw = """{
  "status": "done",
  "image_kind": "scheme",
  "summary": "однолинейка",
  "scheme_analysis": {
    "is_scheme": true,
    "scheme_type": "electrical_single_line",
    "flow_medium": "electricity",
    "nodes": [{"id":"node_1","label":"ВРУ","type":"panel","confidence":0.9}],
    "connections": [],
    "sequence_summary": ["Ввод → ВРУ → нагрузка"],
    "independent_circuits": [],
    "comparison_relevant_scheme_facts": [],
    "uncertainties": []
  },
  "confidence": 0.7
}"""
    parsed = parse_diff_json(raw)
    assert parsed is not None
    assert parsed["scheme_analysis"]["is_scheme"] is True
    assert parsed["scheme_analysis"]["scheme_type"] == "electrical_single_line"
    assert parsed["scheme_analysis"]["nodes"][0]["label"] == "ВРУ"


# ─── Дополнительные сценарии: enriched md содержит pending для dry-run ────


@pytest.mark.asyncio
async def test_enriched_md_marks_pending_in_dry_run(_local_env, tmp_path):
    from backend.app.services.stage_comparison import md_image_enrichment as m

    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(10, 20, 30))
    _write_png(crop_dir / "img-002.png", color=(40, 50, 60))

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [
            {"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]}]},
            {"page_number": 2, "blocks": [{"id": "img-002", "block_type": "image", "coords_px": [0, 0, 100, 100]}]},
        ],
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess1", "pair1", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=lambda b: crop_dir / f"{b}.png",
        describe_fn=None,  # не должен быть вызван — run_model=False
        run_model=False,
    )
    enriched = Path(summary.enriched_md_path).read_text(encoding="utf-8")
    assert "status: pending" in enriched
    assert "Кабельная линия ВВГнг(А)-FRLS 5x16" in enriched


# ─── 14: on_block_progress callback (background-job progress hook) ────────


@pytest.mark.asyncio
async def test_enrich_side_calls_on_block_progress_per_block(_local_env, tmp_path):
    """on_block_progress должен быть вызван 1 раз на каждый image-блок —
    включая блоки, упавшие на resolve (no_image) и dry-run pending."""
    from backend.app.services.stage_comparison import md_image_enrichment as m

    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")  # содержит 2 image-блока

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")
    _write_png(crop_dir / "img-002.png")

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [
            {"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image",
                                            "coords_px": [0, 0, 100, 100]}]},
            {"page_number": 2, "blocks": [{"id": "img-002", "block_type": "image",
                                            "coords_px": [0, 0, 100, 100]}]},
        ],
    }), encoding="utf-8")

    events: list[dict] = []

    def _on_block(payload: dict) -> None:
        events.append(dict(payload))

    summary = await m.enrich_side(
        "sess_cb", "pair_cb", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=lambda b: crop_dir / f"{b}.png",
        run_model=False,                          # без live calls
        on_block_progress=_on_block,
    )
    assert summary.image_blocks == 2
    assert [e["block_index"] for e in events] == [1, 2]
    assert all(e["total"] == 2 for e in events)
    assert all("status" in e and e["status"] for e in events)
    # block_id и page тоже передаются для UI
    assert all("block_id" in e for e in events)
    assert all("page" in e for e in events)


@pytest.mark.asyncio
async def test_enrich_side_swallows_on_block_progress_exceptions(_local_env, tmp_path):
    """Исключение в callback не должно валить enrich_side."""
    from backend.app.services.stage_comparison import md_image_enrichment as m

    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")
    _write_png(crop_dir / "img-002.png")

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [
            {"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image",
                                            "coords_px": [0, 0, 100, 100]}]},
            {"page_number": 2, "blocks": [{"id": "img-002", "block_type": "image",
                                            "coords_px": [0, 0, 100, 100]}]},
        ],
    }), encoding="utf-8")

    def _boom(_payload):
        raise RuntimeError("boom in progress callback")

    summary = await m.enrich_side(
        "sess_cb2", "pair_cb2", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=lambda b: crop_dir / f"{b}.png",
        run_model=False,
        on_block_progress=_boom,
    )
    # enrich_side всё равно дошёл до конца
    assert summary.image_blocks == 2
    assert summary.pending == 2


# ─── 15: md_enrichment_jobs records per-block "current" progress ─────────


@pytest.mark.asyncio
async def test_md_enrichment_job_records_current_block_progress(_local_env, tmp_path, monkeypatch):
    """Job должен записывать item.current.{block_index,total} после каждого
    блока, чтобы UI мог рендерить прогресс «блок N/M» без live-калла LLM."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import store as store_mod
    from backend.app.services.stage_comparison import paths as paths_mod

    # Подготовим source MD + result.json + crop
    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")
    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")
    _write_png(crop_dir / "img-002.png")
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [
            {"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image",
                                            "coords_px": [0, 0, 100, 100]}]},
            {"page_number": 2, "blocks": [{"id": "img-002", "block_type": "image",
                                            "coords_px": [0, 0, 100, 100]}]},
        ],
    }), encoding="utf-8")

    fake_session = {
        "id": "sess_jobcurr",
        "pairs": [{"id": "pair_jobcurr", "status": "active",
                   "left": {"md_path": str(src_md), "result_json_path": str(result_json)},
                   "right": {"md_path": str(src_md), "result_json_path": str(result_json)}}],
    }
    monkeypatch.setattr(store_mod, "get_session", lambda sid: fake_session if sid == "sess_jobcurr" else None)
    # рендерим из crop_dir, имитируя реальный store.render_block_crop
    monkeypatch.setattr(store_mod, "render_block_crop",
                        lambda sid, pid, side, bid: crop_dir / f"{bid}.png")

    # Подменим describe_image_local — никаких live calls; зафиксируем «снэпшот»
    # job.json после каждого блока, чтобы убедиться, что current обновляется.
    from backend.app.services.stage_comparison import graphic_llm_local as g

    job = jobs.create_md_enrichment_job(
        "sess_jobcurr", scope="pair", pair_id="pair_jobcurr",
        side="left", force=True, confirm=True,
    )
    assert job["status"] == "queued"

    snapshots: list[dict] = []
    original_enrich = m.enrich_side

    async def _spy_enrich(*args, **kwargs):
        on_block = kwargs.get("on_block_progress")
        def _wrap(payload):
            # перед записью job.json дернуть оригинальный callback
            on_block(payload)
            data = json.loads(paths_mod.job_json_path("sess_jobcurr", job["id"]).read_text())
            snapshots.append(data)
        kwargs["on_block_progress"] = _wrap
        # фейковый describe_fn — не зовём LLM, но возвращаем done
        async def _fake_describe(image_path, prompt):
            return g.DescribeResult(
                status="done", provider="local_openai_compatible",
                model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
                parsed={"status": "done", "summary": "ok", "confidence": 1.0},
                raw_response_excerpt="raw", duration_sec=0.0,
            )
        kwargs["describe_fn"] = _fake_describe
        return await original_enrich(*args, **kwargs)

    monkeypatch.setattr(m, "enrich_side", _spy_enrich)
    monkeypatch.setattr(jobs.md_mod, "enrich_side", _spy_enrich)

    final = await jobs.run_md_enrichment_job("sess_jobcurr", job["id"])
    assert final["status"] == "done"
    assert len(snapshots) == 2  # ровно по числу image-блоков
    # Каждый snapshot содержит item с обновлённым current.block_index
    first = snapshots[0]["items"][0]
    second = snapshots[1]["items"][0]
    assert first.get("current", {}).get("block_index") == 1
    assert first.get("current", {}).get("total") == 2
    assert second.get("current", {}).get("block_index") == 2
    assert second.get("current", {}).get("total") == 2


# ─── 16: cancel job ─────────────────────────────────────────────────────


def test_md_enrichment_job_can_be_cancelled(_local_env, tmp_path, monkeypatch):
    """cancel_job на queued/running job → status=cancelled, item-ы cancelled."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import store as store_mod

    fake_session = {
        "id": "sess_cancel",
        "pairs": [{"id": "p", "status": "active",
                   "left": {"md_path": str(tmp_path / "L.md")},
                   "right": {"md_path": str(tmp_path / "R.md")}}],
    }
    monkeypatch.setattr(store_mod, "get_session", lambda sid: fake_session if sid == "sess_cancel" else None)

    job = jobs.create_md_enrichment_job(
        "sess_cancel", scope="pair", pair_id="p",
        side="both", force=False, confirm=True,
    )
    assert job["status"] == "queued"
    cancelled = jobs.cancel_job("sess_cancel", job["id"])
    assert cancelled is not None
    assert cancelled["status"] == "cancelled"
    assert all(it["status"] == "cancelled" for it in cancelled["items"])


# ─── 17: UI больше не вызывает sync run_model=true ─────────────────────


def test_frontend_ui_does_not_call_sync_md_enrichment_with_run_model_true():
    """Регрессионный тест: UI должен запускать Qwen через md-enrichment-jobs,
    а не через POST /pairs/{pid}/md-enrichment с run_model=true. Иначе мы
    возвращаемся к HTTP 524 на ngrok/Cloudflare."""
    app_js = (Path(__file__).resolve().parent.parent
              / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    # должны быть вызовы job endpoint
    assert "/md-enrichment-jobs" in app_js, "UI должен использовать md-enrichment-jobs endpoint"
    assert "scPollMdEnrichmentJob" in app_js, "UI должен иметь polling функцию"
    assert "scCancelMdEnrichmentJob" in app_js, "UI должен иметь cancel функцию"
    # А вот sync run_model=true должен исчезнуть из UI (dry-run run_model=false остаётся)
    assert "run_model: true" not in app_js, (
        "UI не должен вызывать sync md-enrichment с run_model=true — это даёт HTTP 524"
    )
    assert "run_model:true" not in app_js, (
        "UI не должен вызывать sync md-enrichment с run_model=true (без пробела)"
    )


# ─── Diagnostics propagation: новые поля попадают в items[] ───────────────


@pytest.mark.asyncio
async def test_enrich_side_records_extended_diagnostics_in_item(_local_env, tmp_path):
    """После реального вызова describe_fn новые диагностические поля
    (finish_reason, usage, response_char_count, parse_error_detail) +
    raw_response_path должны попасть в items[] и descriptions JSON.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001
""", encoding="utf-8")

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(11, 22, 33))

    def render(side_block_id):
        return crop_dir / f"{side_block_id}.png"

    async def fake_describe(image_path, prompt):
        # Эмулируем длинный raw, который НЕ должен потеряться при сохранении.
        long_raw = '{"status": "done", "summary": "X", "confidence": 0.9}' + (" " * 2000)
        return g.DescribeResult(
            status="done",
            provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b",
            model_used="qwen/qwen3.6-35b-a3b",
            parsed={"status": "done", "summary": "X", "confidence": 0.9},
            raw_response_excerpt=long_raw[:1500] + "…",
            duration_sec=12.34,
            finish_reason="length",
            usage={"prompt_tokens": 950, "completion_tokens": 5500, "total_tokens": 6450},
            response_char_count=len(long_raw),
            parse_error_detail=None,
            full_raw_response=long_raw,
        )

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}]
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess_diag", "pair_diag", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=render, describe_fn=fake_describe, run_model=True,
    )

    assert summary.described == 1
    assert summary.errors == 0
    # items должны нести новые диагностические поля
    item = summary.items[0]
    assert item.get("finish_reason") == "length"
    assert item.get("usage") == {"prompt_tokens": 950, "completion_tokens": 5500, "total_tokens": 6450}
    assert item.get("response_char_count") > 1500  # длиннее excerpt'а
    assert item.get("parse_error_detail") is None
    # raw_response_path должен указывать на FULL файл, а не на excerpt
    raw_path = item.get("raw_response_path")
    assert raw_path, "raw_response_path missing"
    full_file = Path(raw_path)
    assert full_file.exists()
    full_bytes = full_file.read_text(encoding="utf-8")
    # Полный raw сохранён без обрезания excerpt'ом
    assert len(full_bytes) > 1500
    assert full_bytes.endswith(" " * 100)  # хвост сохранён


@pytest.mark.asyncio
async def test_enrich_side_records_parse_error_detail_on_invalid_json(_local_env, tmp_path):
    """Когда describe_fn возвращает invalid_json с parse_error_detail=
    markdown_reasoning — это значение должно сохраниться в item для UI
    диагностики «почему упал»."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001
""", encoding="utf-8")

    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(99, 88, 77))

    def render(side_block_id):
        return crop_dir / f"{side_block_id}.png"

    md_text = "1.  **Analyze the Request:**\n    *   **Task:** Analyze drawing."

    async def fake_describe(image_path, prompt):
        return g.DescribeResult(
            status="invalid_json",
            provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b",
            model_used="qwen/qwen3.6-35b-a3b",
            parsed=None,
            raw_response_excerpt=md_text,
            duration_sec=29.5,
            error="json_parse_failed",
            finish_reason="length",
            usage={"prompt_tokens": 900, "completion_tokens": 1800, "total_tokens": 2700},
            response_char_count=len(md_text),
            parse_error_detail="markdown_reasoning",
            full_raw_response=md_text,
        )

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}]
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess_diag", "pair_diag_md", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=render, describe_fn=fake_describe, run_model=True,
    )

    item = summary.items[0]
    assert item.get("status") == "error"
    assert item.get("parse_error_detail") == "markdown_reasoning"
    assert item.get("finish_reason") == "length"
    # Полный raw сохранён, чтобы forensics не заглохли на excerpt'е
    raw_path = item.get("raw_response_path")
    if raw_path:
        # raw_path может указывать на excerpt-файл, если full == excerpt;
        # но _save_prompt_and_raw возвращает Path в любом случае, если что-то записалось.
        assert Path(raw_path).exists()


@pytest.mark.asyncio
async def test_md_enrichment_job_calls_ensure_lmstudio_preflight(_local_env, tmp_path, monkeypatch):
    """В начале run_md_enrichment_job должен быть вызван
    ensure_lmstudio_model_loaded(primary, allow_fallback=False).

    Без этого LM Studio JIT поднимает модель с дефолтным ctx=4096 и большой
    v4_compact prompt оставляет ~1800 токенов на ответ — JSON хронически
    обрезается. Preflight гарантирует, что primary loaded с
    cfg.load_context_length до первого describe call.
    """
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import store as store_mod
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001
""", encoding="utf-8")
    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}],
    }), encoding="utf-8")

    fake_session = {
        "id": "sess_pf",
        "pairs": [{"id": "pair_pf", "status": "active",
                   "left": {"md_path": str(src_md), "result_json_path": str(result_json)},
                   "right": {"md_path": str(src_md), "result_json_path": str(result_json)}}],
    }
    monkeypatch.setattr(store_mod, "get_session", lambda sid: fake_session if sid == "sess_pf" else None)
    monkeypatch.setattr(store_mod, "render_block_crop",
                        lambda sid, pid, side, bid: crop_dir / f"{bid}.png")

    preflight_calls = []

    async def _fake_preflight(model_name, *, cfg=None, allow_fallback=True):
        preflight_calls.append({"model": model_name, "allow_fallback": allow_fallback})
        return {"ok": True, "model_used": model_name, "fallback_used": False,
                "endpoint_available": True, "messages": ["ensured"],
                "actual_ctx": cfg.load_context_length if cfg else 16000,
                "desired_ctx": cfg.load_context_length if cfg else 16000}

    monkeypatch.setattr(g, "ensure_lmstudio_model_loaded", _fake_preflight)
    monkeypatch.setattr(jobs.graphic_local_mod, "ensure_lmstudio_model_loaded", _fake_preflight)

    async def _fake_describe(image_path, prompt):
        return g.DescribeResult(
            status="done", provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
            parsed={"status": "done", "summary": "ok", "confidence": 1.0},
            raw_response_excerpt="raw", duration_sec=0.0,
        )

    original_enrich = m.enrich_side

    async def _spy_enrich(*args, **kwargs):
        kwargs["describe_fn"] = _fake_describe
        return await original_enrich(*args, **kwargs)

    monkeypatch.setattr(m, "enrich_side", _spy_enrich)
    monkeypatch.setattr(jobs.md_mod, "enrich_side", _spy_enrich)

    job = jobs.create_md_enrichment_job(
        "sess_pf", scope="pair", pair_id="pair_pf",
        side="left", force=True, confirm=True,
    )
    final = await jobs.run_md_enrichment_job("sess_pf", job["id"])
    assert final["status"] == "done"

    # Preflight вызван ровно один раз — на primary, без fallback.
    assert len(preflight_calls) == 1, f"expected 1 preflight call, got {preflight_calls}"
    assert preflight_calls[0]["model"] == "qwen/qwen3.6-35b-a3b"
    assert preflight_calls[0]["allow_fallback"] is False


# ─── Session-level diagnostics aggregation ────────────────────────────────


@pytest.mark.asyncio
async def test_aggregate_job_progress_emits_session_diagnostics(_local_env, tmp_path, monkeypatch):
    """После прогона job, aggregate_job_progress должен возвращать
    `diagnostics` с avg_duration_sec, p95, parse_error_distribution и
    salvage/continuation/fallback rate'ами. Это то, что UI рендерит в
    «здоровье прогона» для оператора.
    """
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import store as store_mod
    from backend.app.services.stage_comparison import graphic_llm_local as g
    from backend.app.services.stage_comparison import paths as paths_mod

    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")
    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(10, 20, 30))
    _write_png(crop_dir / "img-002.png", color=(50, 60, 70))
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [
            {"page_number": 1, "blocks": [
                {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
            ]},
            {"page_number": 2, "blocks": [
                {"id": "img-002", "block_type": "image", "coords_px": [0, 0, 100, 100]},
            ]},
        ],
    }), encoding="utf-8")

    fake_session = {
        "id": "sess_agg",
        "pairs": [{"id": "pair_agg", "status": "active",
                   "left": {"md_path": str(src_md), "result_json_path": str(result_json)},
                   "right": {"md_path": str(src_md), "result_json_path": str(result_json)}}],
    }
    monkeypatch.setattr(store_mod, "get_session",
                        lambda sid: fake_session if sid == "sess_agg" else None)
    monkeypatch.setattr(store_mod, "render_block_crop",
                        lambda sid, pid, side, bid: crop_dir / f"{bid}.png")
    # отключаем реальный preflight (HTTP)
    async def _no_preflight(model_name, **kwargs):
        return {"ok": True, "model_used": model_name, "fallback_used": False,
                "endpoint_available": True, "messages": []}
    monkeypatch.setattr(g, "ensure_lmstudio_model_loaded", _no_preflight)
    monkeypatch.setattr(jobs.graphic_local_mod, "ensure_lmstudio_model_loaded", _no_preflight)

    call_n = {"v": 0}

    async def _fake_describe(image_path, prompt):
        # block 1 = done, block 2 = salvaged_partial + continuation
        call_n["v"] += 1
        # Реальный duration_sec в item пишется enrich_side'ом через
        # time.monotonic(), а не из DescribeResult.duration_sec. Чтобы
        # avg_duration_sec в diagnostics не округлился до 0, дадим задержку.
        await asyncio.sleep(0.05)
        if call_n["v"] == 1:
            return g.DescribeResult(
                status="done", provider="local_openai_compatible",
                model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
                parsed={"status": "done", "summary": "OK", "confidence": 0.9,
                        "chunks_count": 1, "continued": False},
                raw_response_excerpt="raw", duration_sec=12.0,
                finish_reason="stop",
                usage={"prompt_tokens": 800, "completion_tokens": 1500, "total_tokens": 2300},
                response_char_count=3500, parse_error_detail=None,
                full_raw_response="full raw 1",
            )
        return g.DescribeResult(
            status="partial", provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
            parsed={"status": "salvaged_partial", "summary": "Partial", "_salvaged": True,
                    "chunks_count": 3, "continued": True,
                    "scheme_analysis": {"is_scheme": True, "nodes": [{"id": "n1"}]}},
            raw_response_excerpt="raw2", duration_sec=45.0, error="salvaged_partial_json",
            finish_reason="length",
            usage={"prompt_tokens": 900, "completion_tokens": 5500, "total_tokens": 6400},
            response_char_count=12500, parse_error_detail="truncated_json",
            full_raw_response="full raw 2",
        )

    original_enrich = m.enrich_side
    async def _spy_enrich(*args, **kwargs):
        kwargs["describe_fn"] = _fake_describe
        return await original_enrich(*args, **kwargs)
    monkeypatch.setattr(m, "enrich_side", _spy_enrich)
    monkeypatch.setattr(jobs.md_mod, "enrich_side", _spy_enrich)

    job = jobs.create_md_enrichment_job(
        "sess_agg", scope="pair", pair_id="pair_agg",
        side="left", force=True, confirm=True,
    )
    final = await jobs.run_md_enrichment_job("sess_agg", job["id"])
    assert final["status"] == "done"

    progress = jobs.aggregate_job_progress("sess_agg", final)
    diag = progress.get("diagnostics") or {}

    # Block-level counts
    assert diag.get("blocks_done") == 1
    assert diag.get("blocks_partial") == 1
    assert diag.get("blocks_error") == 0
    assert diag.get("blocks_total_with_data") == 2

    # Durations: enrich_side замеряет реальное wall-clock через time.monotonic(),
    # а не использует DescribeResult.duration_sec. Из-за asyncio.sleep(0.05)
    # avg будет ~50ms — главное, что не 0.
    assert diag.get("avg_duration_sec") > 0
    assert diag.get("max_duration_sec") >= diag.get("avg_duration_sec")

    # Rates
    assert diag.get("continuation_rate") == 0.5   # 1 of 2 continued
    assert diag.get("salvage_rate") == 0.5        # 1 of 2 salvaged
    assert diag.get("fallback_rate") == 0.0       # 0 fallback used
    assert diag.get("compact_mode_rate") == 1.0   # v4_compact

    # Token totals
    tokens = diag.get("tokens") or {}
    assert tokens.get("prompt") == 800 + 900
    assert tokens.get("completion") == 1500 + 5500
    assert tokens.get("total") == 2300 + 6400

    # Continuation totals
    assert diag.get("total_chunks") == 4   # 1 + 3
    assert diag.get("total_continuation_count") == 2  # 0 + 2

    # Parse error distribution (block 2 had truncated_json)
    ped = diag.get("parse_error_distribution") or {}
    assert ped.get("truncated_json") == 1

    # final_status_reason distribution
    fsr = diag.get("final_status_reason_distribution") or {}
    assert "primary_done" in fsr or "salvaged_with_continuation" in fsr


@pytest.mark.asyncio
async def test_aggregate_pair_statuses_include_problem_hint_on_error(_local_env, tmp_path, monkeypatch):
    """Если у pair есть partial/error блоки — pair_statuses[pid].problem_hint
    должен содержать человекочитаемое объяснение (не None, не raw enum).
    """
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import store as store_mod
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001
""", encoding="utf-8")
    crop_dir = tmp_path / "crops"; crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}],
    }), encoding="utf-8")

    fake_session = {"id": "sess_hint", "pairs": [{"id": "pair_hint", "status": "active",
                    "left": {"md_path": str(src_md), "result_json_path": str(result_json)},
                    "right": {"md_path": str(src_md), "result_json_path": str(result_json)}}]}
    monkeypatch.setattr(store_mod, "get_session",
                        lambda sid: fake_session if sid == "sess_hint" else None)
    monkeypatch.setattr(store_mod, "render_block_crop",
                        lambda sid, pid, side, bid: crop_dir / f"{bid}.png")
    async def _no_preflight(model_name, **kwargs):
        return {"ok": True, "model_used": model_name, "fallback_used": False,
                "endpoint_available": True, "messages": []}
    monkeypatch.setattr(g, "ensure_lmstudio_model_loaded", _no_preflight)
    monkeypatch.setattr(jobs.graphic_local_mod, "ensure_lmstudio_model_loaded", _no_preflight)

    # Все блоки уходят в error из-за markdown_reasoning
    async def _fake_describe(image_path, prompt):
        return g.DescribeResult(
            status="invalid_json", provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b", model_used="qwen3.6-35b-a3b-mtp",
            parsed=None, raw_response_excerpt="markdown raw", duration_sec=29.0,
            error="json_parse_failed", finish_reason="length",
            usage={"prompt_tokens": 900, "completion_tokens": 1800, "total_tokens": 2700},
            response_char_count=2200, parse_error_detail="markdown_reasoning",
            full_raw_response="markdown raw",
        )

    original_enrich = m.enrich_side
    async def _spy_enrich(*args, **kwargs):
        kwargs["describe_fn"] = _fake_describe
        return await original_enrich(*args, **kwargs)
    monkeypatch.setattr(m, "enrich_side", _spy_enrich)
    monkeypatch.setattr(jobs.md_mod, "enrich_side", _spy_enrich)

    job = jobs.create_md_enrichment_job(
        "sess_hint", scope="pair", pair_id="pair_hint",
        side="left", force=True, confirm=True,
    )
    final = await jobs.run_md_enrichment_job("sess_hint", job["id"])
    progress = jobs.aggregate_job_progress("sess_hint", final)

    ps = progress.get("pair_statuses", {}).get("pair_hint", {})
    assert ps.get("status") in ("error", "partial")
    hint = ps.get("problem_hint") or ""
    assert "markdown" in hint.lower(), f"expected human hint about markdown, got: {hint!r}"


@pytest.mark.asyncio
async def test_session_job_one_pair_failure_does_not_kill_others(_local_env, tmp_path, monkeypatch):
    """Если enrich_side для одной пары бросает исключение, job НЕ должен
    останавливаться — следующая пара должна обработаться. Это
    session-level graceful degradation.
    """
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import store as store_mod
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001
""", encoding="utf-8")
    crop_dir = tmp_path / "crops"; crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}],
    }), encoding="utf-8")

    # 2 пары с одной стороной каждая
    fake_session = {"id": "sess_gd", "pairs": [
        {"id": "pair_fail", "status": "active",
         "left": {"md_path": str(src_md), "result_json_path": str(result_json)},
         "right": {"md_path": str(src_md), "result_json_path": str(result_json)}},
        {"id": "pair_ok", "status": "active",
         "left": {"md_path": str(src_md), "result_json_path": str(result_json)},
         "right": {"md_path": str(src_md), "result_json_path": str(result_json)}},
    ]}
    monkeypatch.setattr(store_mod, "get_session",
                        lambda sid: fake_session if sid == "sess_gd" else None)
    monkeypatch.setattr(store_mod, "render_block_crop",
                        lambda sid, pid, side, bid: crop_dir / f"{bid}.png")
    async def _no_preflight(model_name, **kwargs):
        return {"ok": True, "model_used": model_name, "fallback_used": False,
                "endpoint_available": True, "messages": []}
    monkeypatch.setattr(g, "ensure_lmstudio_model_loaded", _no_preflight)
    monkeypatch.setattr(jobs.graphic_local_mod, "ensure_lmstudio_model_loaded", _no_preflight)

    call_ix = {"n": 0}

    async def _spy_enrich(session_id, pair_id, side, **kwargs):
        call_ix["n"] += 1
        if pair_id == "pair_fail":
            raise RuntimeError("simulated transport boom")
        return m.EnrichSideSummary(
            side=side, status="done", md_path="md", md_exists=True,
            image_blocks=1, described=1, from_cache=0, errors=0, pending=0, salvaged=0,
            warnings=[], items=[],
        )

    monkeypatch.setattr(m, "enrich_side", _spy_enrich)
    monkeypatch.setattr(jobs.md_mod, "enrich_side", _spy_enrich)

    job = jobs.create_md_enrichment_job(
        "sess_gd", scope="session", side="left", force=True, confirm=True,
    )
    final = await jobs.run_md_enrichment_job("sess_gd", job["id"])

    # Job всё равно должен завершиться (не упасть).
    assert final["status"] == "done"
    # 1 фейлится, 1 успешен
    items_by_pair = {it["pair_id"]: it for it in final["items"]}
    assert items_by_pair["pair_fail"]["status"] == "failed"
    assert items_by_pair["pair_ok"]["status"] == "done"
    # счётчики корректные
    assert final["progress"]["done"] == 1
    assert final["progress"]["failed"] == 1


@pytest.mark.asyncio
async def test_session_job_continues_after_cancellation_signal_lost(_local_env, tmp_path, monkeypatch):
    """Если job в running и cancel signal приходит между блоками — должен
    отрабатать чисто (не уходить в infinite loop, не ломать item state)."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import store as store_mod
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001
""", encoding="utf-8")
    crop_dir = tmp_path / "crops"; crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png")
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}],
    }), encoding="utf-8")

    fake_session = {"id": "sess_cancel", "pairs": [
        {"id": "pair_a", "status": "active",
         "left": {"md_path": str(src_md), "result_json_path": str(result_json)},
         "right": {"md_path": str(src_md), "result_json_path": str(result_json)}},
        {"id": "pair_b", "status": "active",
         "left": {"md_path": str(src_md), "result_json_path": str(result_json)},
         "right": {"md_path": str(src_md), "result_json_path": str(result_json)}},
    ]}
    monkeypatch.setattr(store_mod, "get_session",
                        lambda sid: fake_session if sid == "sess_cancel" else None)
    monkeypatch.setattr(store_mod, "render_block_crop",
                        lambda sid, pid, side, bid: crop_dir / f"{bid}.png")
    async def _no_preflight(model_name, **kwargs):
        return {"ok": True, "model_used": model_name, "fallback_used": False,
                "endpoint_available": True, "messages": []}
    monkeypatch.setattr(g, "ensure_lmstudio_model_loaded", _no_preflight)
    monkeypatch.setattr(jobs.graphic_local_mod, "ensure_lmstudio_model_loaded", _no_preflight)

    job = jobs.create_md_enrichment_job(
        "sess_cancel", scope="session", side="left", force=True, confirm=True,
    )

    async def _spy_enrich(session_id, pair_id, side, **kwargs):
        # после первой пары симулируем cancel
        if pair_id == "pair_a":
            return m.EnrichSideSummary(
                side=side, status="done", md_path="md", md_exists=True,
                image_blocks=1, described=1, from_cache=0, errors=0, pending=0, salvaged=0,
                warnings=[], items=[],
            )
        # На второй паре — cancel УЖЕ сработал; enrich_side не вызывается из-за
        # check'а в начале каждой итерации цикла. Но если всё же вызовут — fail.
        raise AssertionError("pair_b should not have been processed after cancel")

    monkeypatch.setattr(m, "enrich_side", _spy_enrich)
    monkeypatch.setattr(jobs.md_mod, "enrich_side", _spy_enrich)

    # Эмулируем cancel ПОСЛЕ обработки первой пары:
    # перехватим _write_job чтобы как только pair_a уйдёт в done, поставить cancel.
    original_write = jobs._write_job
    flipped = {"v": False}

    def _spy_write(session_id, job_dict):
        if not flipped["v"]:
            for it in (job_dict.get("items") or []):
                if it.get("pair_id") == "pair_a" and it.get("status") == "done":
                    job_dict["status"] = "cancelled"
                    flipped["v"] = True
                    break
        original_write(session_id, job_dict)

    monkeypatch.setattr(jobs, "_write_job", _spy_write)
    final = await jobs.run_md_enrichment_job("sess_cancel", job["id"])

    # job должен быть cancelled
    assert final["status"] == "cancelled"
    # pair_a done, pair_b остался queued (не cancelled на item-level, потому что
    # cancel прервал loop до достижения pair_b. Это нормально.)
    items_by_pair = {it["pair_id"]: it for it in final["items"]}
    assert items_by_pair["pair_a"]["status"] == "done"
    assert items_by_pair["pair_b"]["status"] in ("queued", "cancelled")


# ─── Stale job detection (uvicorn restart / crash recovery) ──────────────


def _setup_minimal_session(monkeypatch, session_id: str = "sess_stale", n_pairs: int = 1):
    """Минимальная фейк-сессия для create_md_enrichment_job без реальных PDF."""
    from backend.app.services.stage_comparison import store as store_mod

    pairs = []
    for i in range(n_pairs):
        pid = f"pair_{i}"
        pairs.append(
            {
                "id": pid,
                "status": "active",
                "left": {"md_path": f"/tmp/{pid}_left.md", "result_json_path": f"/tmp/{pid}_left.json"},
                "right": {"md_path": f"/tmp/{pid}_right.md", "result_json_path": f"/tmp/{pid}_right.json"},
            }
        )
    fake = {"id": session_id, "pairs": pairs}
    monkeypatch.setattr(store_mod, "get_session", lambda sid: fake if sid == session_id else None)


def test_stale_running_job_marked_failed_interrupted(monkeypatch):
    """Job со status=running на диске + нет живой таски → failed_interrupted."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs

    _setup_minimal_session(monkeypatch, "sess_stale")
    job = jobs.create_md_enrichment_job(
        "sess_stale", scope="session", side="both", confirm=True,
    )
    job_id = job["id"]
    raw = jobs._read_job_raw("sess_stale", job_id)
    raw["status"] = "running"
    raw["updated_at"] = "2020-01-01T00:00:00Z"  # very old
    jobs._write_job("sess_stale", raw)
    jobs._active_tasks.pop("sess_stale", None)

    result = jobs.get_job("sess_stale", job_id)
    assert result["status"] == "failed_interrupted"
    assert "Backend перезапустился" in (result.get("error") or "")
    for it in result["items"]:
        assert it["status"] in ("failed_interrupted", "skipped"), it


def test_stale_running_job_with_alive_task_not_marked(monkeypatch):
    """Если таска жива — stale-detection НЕ срабатывает."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs

    _setup_minimal_session(monkeypatch, "sess_alive")
    job = jobs.create_md_enrichment_job(
        "sess_alive", scope="session", side="both", confirm=True,
    )
    job_id = job["id"]
    raw = jobs._read_job_raw("sess_alive", job_id)
    raw["status"] = "running"
    raw["updated_at"] = "2020-01-01T00:00:00Z"
    jobs._write_job("sess_alive", raw)

    class _FakeAliveTask:
        def done(self):
            return False

    jobs._active_tasks["sess_alive"] = {job_id: _FakeAliveTask()}
    try:
        result = jobs.get_job("sess_alive", job_id)
        assert result["status"] == "running"
    finally:
        jobs._active_tasks.pop("sess_alive", None)


def test_queued_job_within_grace_period_not_marked(monkeypatch):
    """Job со status=queued и свежим updated_at → НЕ marked (60s grace)."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from datetime import datetime, timezone, timedelta

    _setup_minimal_session(monkeypatch, "sess_grace")
    job = jobs.create_md_enrichment_job(
        "sess_grace", scope="session", side="both", confirm=True,
    )
    job_id = job["id"]
    # updated_at = 10 секунд назад
    recent = (datetime.now(timezone.utc) - timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    raw = jobs._read_job_raw("sess_grace", job_id)
    raw["status"] = "queued"
    raw["updated_at"] = recent
    jobs._write_job("sess_grace", raw)
    jobs._active_tasks.pop("sess_grace", None)

    result = jobs.get_job("sess_grace", job_id)
    assert result["status"] == "queued"


def test_queued_job_past_grace_period_marked(monkeypatch):
    """Job со status=queued + старый updated_at (> 60s) + нет таски → interrupted."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from datetime import datetime, timezone, timedelta

    _setup_minimal_session(monkeypatch, "sess_grace_expired")
    job = jobs.create_md_enrichment_job(
        "sess_grace_expired", scope="session", side="both", confirm=True,
    )
    job_id = job["id"]
    old = (datetime.now(timezone.utc) - timedelta(seconds=120)).strftime("%Y-%m-%dT%H:%M:%SZ")
    raw = jobs._read_job_raw("sess_grace_expired", job_id)
    raw["status"] = "queued"
    raw["updated_at"] = old
    jobs._write_job("sess_grace_expired", raw)
    jobs._active_tasks.pop("sess_grace_expired", None)

    result = jobs.get_job("sess_grace_expired", job_id)
    assert result["status"] == "failed_interrupted"


def test_terminal_status_not_marked(monkeypatch):
    """done / failed / cancelled / failed_interrupted не перезаписываются."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs

    _setup_minimal_session(monkeypatch, "sess_terminal")
    for status in ("done", "failed", "cancelled", "rejected_no_confirm", "failed_interrupted"):
        job = jobs.create_md_enrichment_job(
            "sess_terminal", scope="session", side="both", confirm=True,
        )
        job_id = job["id"]
        raw = jobs._read_job_raw("sess_terminal", job_id)
        raw["status"] = status
        raw["updated_at"] = "2020-01-01T00:00:00Z"
        jobs._write_job("sess_terminal", raw)
        jobs._active_tasks.pop("sess_terminal", None)

        result = jobs.get_job("sess_terminal", job_id)
        assert result["status"] == status, f"{status} got rewritten"


def test_get_job_with_progress_marks_stale(monkeypatch):
    """API surface — get_job_with_progress тоже должен помечать stale."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs

    _setup_minimal_session(monkeypatch, "sess_api")
    job = jobs.create_md_enrichment_job(
        "sess_api", scope="session", side="both", confirm=True,
    )
    job_id = job["id"]
    raw = jobs._read_job_raw("sess_api", job_id)
    raw["status"] = "running"
    raw["updated_at"] = "2020-01-01T00:00:00Z"
    jobs._write_job("sess_api", raw)
    jobs._active_tasks.pop("sess_api", None)

    result = jobs.get_job_with_progress("sess_api", job_id)
    assert result["status"] == "failed_interrupted"


def test_list_jobs_marks_stale(monkeypatch):
    """list_md_enrichment_jobs тоже применяет stale-detection."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs

    _setup_minimal_session(monkeypatch, "sess_list")
    job = jobs.create_md_enrichment_job(
        "sess_list", scope="session", side="both", confirm=True,
    )
    job_id = job["id"]
    raw = jobs._read_job_raw("sess_list", job_id)
    raw["status"] = "running"
    raw["updated_at"] = "2020-01-01T00:00:00Z"
    jobs._write_job("sess_list", raw)
    jobs._active_tasks.pop("sess_list", None)

    listed = jobs.list_md_enrichment_jobs("sess_list")
    assert listed[0]["status"] == "failed_interrupted"


def test_find_active_session_job_skips_stale(monkeypatch):
    """find_active_session_job не должен возвращать stale-job как active."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs

    _setup_minimal_session(monkeypatch, "sess_active")
    job = jobs.create_md_enrichment_job(
        "sess_active", scope="session", side="both", confirm=True,
    )
    job_id = job["id"]
    raw = jobs._read_job_raw("sess_active", job_id)
    raw["status"] = "running"
    raw["updated_at"] = "2020-01-01T00:00:00Z"
    jobs._write_job("sess_active", raw)
    jobs._active_tasks.pop("sess_active", None)

    active = jobs.find_active_session_job("sess_active")
    # find_active_session_job возвращает самую свежую (никакой active не найден),
    # но статус не должен быть "running".
    assert active is not None
    assert active["status"] == "failed_interrupted"


def test_cancel_failed_interrupted_is_noop(monkeypatch):
    """cancel_job на failed_interrupted = no-op (терминальный статус)."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs

    _setup_minimal_session(monkeypatch, "sess_cancel_intrr")
    job = jobs.create_md_enrichment_job(
        "sess_cancel_intrr", scope="session", side="both", confirm=True,
    )
    job_id = job["id"]
    raw = jobs._read_job_raw("sess_cancel_intrr", job_id)
    raw["status"] = "failed_interrupted"
    jobs._write_job("sess_cancel_intrr", raw)

    result = jobs.cancel_job("sess_cancel_intrr", job_id)
    assert result is not None
    # статус не меняется
    assert result["status"] == "failed_interrupted"


# ─── done_with_salvage status model ───────────────────────────────────────


@pytest.mark.asyncio
async def test_enrich_side_salvage_only_results_in_done_with_salvage(_local_env, tmp_path):
    """Когда все блоки описаны без errors и pending, но хотя бы один блок
    восстановлен salvage'ом — сторона должна иметь status='done_with_salvage'.

    Регрессия: раньше любой salvaged-блок переводил всю сторону в 'partial'
    и UI помечал такую пару как «проблемную», хотя enriched MD пригоден.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [IMAGE]: img-001
[IMAGE]: img-001

### BLOCK [IMAGE]: img-002
[IMAGE]: img-002
""", encoding="utf-8")
    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(1, 2, 3))
    _write_png(crop_dir / "img-002.png", color=(50, 60, 70))

    def render(side_block_id):
        return crop_dir / f"{side_block_id}.png"

    # img-001 — primary_done, img-002 — salvaged_partial с continuation.
    call_idx = {"n": 0}

    async def fake_describe(image_path, prompt):
        call_idx["n"] += 1
        if call_idx["n"] == 1:
            return g.DescribeResult(
                status="done",
                provider="local_openai_compatible",
                model="qwen/qwen3.6-35b-a3b",
                model_used="qwen/qwen3.6-35b-a3b",
                parsed={"status": "done", "summary": "clean", "confidence": 1.0},
                raw_response_excerpt="raw",
                duration_sec=0.01,
            )
        return g.DescribeResult(
            status="partial",  # salvage-path
            provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b",
            model_used="qwen/qwen3.6-35b-a3b",
            parsed={
                "status": "done",
                "summary": "salvaged",
                "chunks_count": 2,
                "continued": True,
            },
            raw_response_excerpt="raw",
            duration_sec=0.02,
            error="salvaged_partial_json",
            parse_error_detail="truncated_json",
            full_raw_response="raw",
        )

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]},
            {"id": "img-002", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}],
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess_dws", "pair_dws", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=render, describe_fn=fake_describe, run_model=True,
    )

    assert summary.image_blocks == 2
    assert summary.described == 2
    assert summary.errors == 0
    assert summary.pending == 0
    assert summary.salvaged == 1
    assert summary.status == "done_with_salvage", (
        f"Ожидался done_with_salvage, получен {summary.status}"
    )
    # enriched MD должен существовать, диагностика — присутствовать в items
    items = summary.items
    statuses = [it.get("status") for it in items]
    assert "done" in statuses
    assert "partial" in statuses  # сам блок salvaged по-прежнему partial
    final_reasons = [it.get("final_status_reason") for it in items]
    assert "salvaged_with_continuation" in final_reasons


@pytest.mark.asyncio
async def test_enrich_side_clean_run_remains_done(_local_env, tmp_path):
    """Если salvage не задействован, статус должен остаться 'done'."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text("""### СТРАНИЦА 1

### BLOCK [IMAGE]: img-only
[IMAGE]: img-only
""", encoding="utf-8")
    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-only.png")

    def render(side_block_id):
        return crop_dir / f"{side_block_id}.png"

    async def fake_describe(image_path, prompt):
        return g.DescribeResult(
            status="done",
            provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b",
            model_used="qwen/qwen3.6-35b-a3b",
            parsed={"status": "done", "summary": "X", "confidence": 1.0},
            raw_response_excerpt="raw",
            duration_sec=0.01,
        )

    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({
        "pages": [{"page_number": 1, "blocks": [
            {"id": "img-only", "block_type": "image", "coords_px": [0, 0, 100, 100]},
        ]}],
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "sess_clean", "pair_clean", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=render, describe_fn=fake_describe, run_model=True,
    )
    assert summary.status == "done"
    assert summary.salvaged == 0


def test_read_summary_only_backward_compat_salvage_legacy(_local_env, tmp_path, monkeypatch):
    """Старые artifact'ы с status='partial' + salvaged>0 + errors=0 должны
    интерпретироваться как done_with_salvage (без регенерации).

    Это backward compatibility: пользовательские сессии, написанные до
    введения done_with_salvage в enum, не должны помечаться партиально.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    from backend.app.services.stage_comparison import paths as paths_mod

    legacy = {
        "version": 1,
        "side": "left",
        "image_blocks_total": 3,
        "described": 3,
        "from_cache": 0,
        "errors": 0,
        "pending": 0,
        "salvaged": 2,
        "enriched_md_path": str(tmp_path / "fake_enriched.md"),
        "items": [],
    }
    # Симулируем существование descriptions JSON через monkeypatch reader.
    def fake_read(session_id, pair_id, side):
        return legacy
    monkeypatch.setattr(m, "_read_image_descriptions", fake_read)
    summary = m.read_summary_only("sess_legacy", "pair_legacy", "left")
    assert summary["status"] == "done_with_salvage"


def test_pair_summary_is_done_accepts_done_with_salvage(_local_env, tmp_path, monkeypatch):
    """_pair_summary_is_done должен принимать done_with_salvage как готовое."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import md_image_enrichment as m

    enriched = tmp_path / "fake_enriched.md"
    enriched.write_text("# enriched\n", encoding="utf-8")

    def fake_summary(session_id, pair_id, side):
        return {
            "side": side,
            "status": "done_with_salvage",
            "image_blocks": 5,
            "described": 5,
            "errors": 0,
            "pending": 0,
            "salvaged": 2,
            "enriched_md_path": str(enriched),
        }
    monkeypatch.setattr(m, "read_summary_only", fake_summary)
    # И тоже инжект в jobs-модуль, т.к. jobs импортирует md_image_enrichment
    # лениво внутри функции, поэтому monkeypatch на m достаточно.

    assert jobs._pair_summary_is_done("sess_x", "p_x", "left") is True


def test_pair_summary_is_done_rejects_partial_with_errors(_local_env, tmp_path, monkeypatch):
    """partial с errors > 0 не должен считаться готовым."""
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs
    from backend.app.services.stage_comparison import md_image_enrichment as m

    enriched = tmp_path / "fake_enriched.md"
    enriched.write_text("# enriched\n", encoding="utf-8")

    def fake_summary(session_id, pair_id, side):
        return {
            "side": side,
            "status": "partial",
            "image_blocks": 5,
            "described": 4,
            "errors": 1,
            "pending": 0,
            "salvaged": 1,
            "enriched_md_path": str(enriched),
        }
    monkeypatch.setattr(m, "read_summary_only", fake_summary)
    assert jobs._pair_summary_is_done("sess_x", "p_x", "left") is False


def test_aggregate_done_with_salvage_pair_counts_and_ready(monkeypatch, tmp_path):
    """aggregate_job_progress: пара done_with_salvage + done должна:
      - попасть в done_with_salvage_pairs (не в partial_pairs);
      - иметь ready_for_unified_analysis = True;
      - иметь pair_status = done_with_salvage;
      - иметь problem_hint = None.
    """
    from backend.app.services.stage_comparison import md_enrichment_jobs as jobs

    monkeypatch.setattr(jobs, "_read_side_descriptions_metrics",
                        lambda *a, **k: {
                            "block_metrics_available": False,
                            "blocks_done": 0, "blocks_partial": 0, "blocks_error": 0,
                            "blocks_continued": 0, "blocks_salvaged": 0,
                            "blocks_fallback_used": 0, "blocks_compact_mode": 0,
                            "total_chunks": 0, "total_continuation_count": 0,
                            "duration_sec_sum": 0.0, "duration_sec_max": 0.0,
                            "duration_sec_list": [],
                            "total_prompt_tokens": 0, "total_completion_tokens": 0,
                            "total_tokens": 0,
                            "parse_error_distribution": {},
                            "final_status_reason_distribution": {},
                            "finish_reason_distribution": {},
                        })
    monkeypatch.setattr(jobs, "_pair_label", lambda sid, pid: "L ↔ R")

    job = {
        "id": "j1", "status": "running",
        "started_at": "2026-05-26T09:00:00Z",
        "updated_at": "2026-05-26T09:10:00Z",
        "items": [
            {"pair_id": "pA", "side": "left",
             "status": "done", "summary_status": "done_with_salvage",
             "image_blocks": 10, "described": 10, "from_cache": 0,
             "errors": 0, "pending": 0},
            {"pair_id": "pA", "side": "right",
             "status": "done", "summary_status": "done",
             "image_blocks": 6, "described": 6, "from_cache": 0,
             "errors": 0, "pending": 0},
            {"pair_id": "pB", "side": "left",
             "status": "done", "summary_status": "partial",
             "image_blocks": 5, "described": 4, "from_cache": 0,
             "errors": 1, "pending": 0},
            {"pair_id": "pB", "side": "right",
             "status": "done", "summary_status": "done",
             "image_blocks": 5, "described": 5, "from_cache": 0,
             "errors": 0, "pending": 0},
        ],
        "current": {},
    }
    agg = jobs.aggregate_job_progress("sess_agg", job)

    assert agg["done_with_salvage_pairs"] == 1
    assert agg["done_pairs"] == 0  # done+done_with_salvage → done_with_salvage
    assert agg["partial_pairs"] == 1  # pB остаётся проблемной из-за errors=1
    assert agg["error_pairs"] == 0

    pA = agg["pair_statuses"]["pA"]
    assert pA["status"] == "done_with_salvage"
    assert pA["ready_for_unified_analysis"] is True
    assert pA["problem_hint"] is None

    pB = agg["pair_statuses"]["pB"]
    assert pB["status"] == "partial"
    assert pB["ready_for_unified_analysis"] is False


def test_ui_retry_errors_filters_only_real_errors():
    """JS-side: scRecogRetryErrors собирает pair_ids только по
    status in ('error','partial'). done_with_salvage сюда не попадает.
    Этот тест читает app.js как текст, чтобы поймать регрессию в фильтре.
    """
    app_js = (Path(__file__).resolve().parent.parent
              / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    # Должен быть точечный фильтр по error/partial
    assert "p.status === 'error' || p.status === 'partial'" in app_js, (
        "Фильтр retry-errors должен оставаться по 'error' || 'partial', не "
        "захватывать done_with_salvage"
    )
    assert "p.status === 'done_with_salvage'" not in app_js, (
        "done_with_salvage не должен попадать в кнопку «Повторить ошибки»"
    )


def test_ui_badge_includes_done_with_salvage_as_matched():
    """JS-side: scRecogPairBadge маркирует done_with_salvage как matched
    (зелёный), не как maybe (жёлтый)."""
    app_js = (Path(__file__).resolve().parent.parent
              / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    # ищем blok с done_with_salvage в карте бейджей
    assert "done_with_salvage:" in app_js, "Бейдж done_with_salvage отсутствует"
    # должен использовать sc-status-matched (зелёный) — проверим, что строка
    # с done_with_salvage не использует sc-status-maybe
    start = app_js.index("done_with_salvage:")
    snippet = app_js[start:start + 240]
    assert "sc-status-matched" in snippet, (
        f"done_with_salvage должен быть matched, snippet: {snippet}"
    )
    assert "sc-status-maybe" not in snippet, (
        f"done_with_salvage не должен быть maybe (жёлтым), snippet: {snippet}"
    )


def test_ui_problem_pairs_filter_excludes_done_with_salvage():
    """Index.html: блок «Проблемные пары» должен фильтровать только по
    ['error','partial']. done_with_salvage не упоминается в этом списке.
    """
    index_html = (Path(__file__).resolve().parent.parent
                  / "frontend" / "index.html").read_text(encoding="utf-8")
    # фильтр должен оставаться строгим
    assert "['error','partial']" in index_html, (
        "Фильтр «Проблемные пары» должен быть ['error','partial']"
    )
    # done_with_salvage НЕ должен встречаться в условии problem-list
    # (он может встречаться в template'е бейджа отдельно; проверим только
    # включение в фильтр problem-list).
    # Strict check: «'done_with_salvage'» внутри массива фильтра.
    assert "['error','partial','done_with_salvage']" not in index_html


# ─── Discrepancies UI refactor: debug-tabs gated, table → cards ─────────


def test_ui_debug_subtabs_gated_behind_scDevTools():
    """Кнопки «Текст (debug)» и «Графика (debug)» должны быть скрыты от
    обычного пользователя за dev-флагом v-if=\"scDevTools\".
    """
    index_html = (Path(__file__).resolve().parent.parent
                  / "frontend" / "index.html").read_text(encoding="utf-8")
    # Кнопки существуют, но v-if гейт обязателен
    text_btn = 'Текст (debug)'
    graphic_btn = 'Графика (debug)'
    assert text_btn in index_html, "debug-кнопка «Текст» должна остаться в коде, но за gate'ом"
    assert graphic_btn in index_html, "debug-кнопка «Графика» должна остаться в коде, но за gate'ом"
    # И обе должны быть гейчены scDevTools
    # Грубая проверка: рядом с кнопками встречается v-if=\"scDevTools\"
    text_idx = index_html.index(text_btn)
    graphic_idx = index_html.index(graphic_btn)
    window_text = index_html[max(0, text_idx - 400):text_idx]
    window_graphic = index_html[max(0, graphic_idx - 400):graphic_idx]
    assert 'v-if="scDevTools"' in window_text, "кнопка «Текст (debug)» не гейтится scDevTools"
    assert 'v-if="scDevTools"' in window_graphic, "кнопка «Графика (debug)» не гейтится scDevTools"


def test_ui_session_wide_opus_button_gated_behind_scDevTools():
    """Кнопка «Проанализировать всю сессию» — тяжёлая dev-операция; должна
    быть скрыта от обычного пользователя за scDevTools.
    """
    index_html = (Path(__file__).resolve().parent.parent
                  / "frontend" / "index.html").read_text(encoding="utf-8")
    btn_label = 'Проанализировать всю сессию'
    assert btn_label in index_html, "кнопка должна остаться доступной из dev"
    idx = index_html.index(btn_label)
    window = index_html[max(0, idx - 600):idx]
    assert 'v-if="scDevTools"' in window, (
        "кнопка «Проанализировать всю сессию» не гейтится scDevTools — "
        "обычный пользователь её увидит"
    )


def test_ui_findings_use_card_grid_not_table_columns():
    """Расхождения должны рендериться карточками (sc-unified-grid), а не
    отдельными колонками «Источник / Тип / Категория / Важность» в таблице.
    """
    index_html = (Path(__file__).resolve().parent.parent
                  / "frontend" / "index.html").read_text(encoding="utf-8")
    # Старая таблица unified-расхождений с заголовками-колонками должна
    # исчезнуть (нет class="sc-unified-table" в шаблоне).
    assert 'class="sc-unified-table"' not in index_html, (
        "Старая таблица расхождений ещё присутствует — должна быть заменена card grid'ом"
    )
    # Заголовки старых колонок не должны висеть как <th> в основной таблице.
    # «Источник», «Тип», «Категория», «Важность» могут встречаться в других
    # таблицах (например проектов), поэтому проверяем только sequence в одном
    # месте — отсутствие комбинации с sc-unified-table уже гарантирует, что
    # они не показываются как основные столбцы.
    # Карточки должны существовать.
    assert 'sc-unified-grid' in index_html, "card grid sc-unified-grid не найден"
    assert 'sc-unified-card' in index_html, "карточка sc-unified-card не найдена"
    # И в карточке есть ожидаемые поля: «Было», «Стало», «Влияние», «Перейти к месту».
    assert 'Перейти к месту' in index_html, "кнопка «Перейти к месту» не найдена в карточке"


def test_ui_analysis_mode_hint_in_discrepancies_page():
    """В блоке расхождений по текущей паре должно быть человеческое
    пояснение: «Сравнивается весь документ целиком» + поясняющий хвост
    для concept_no_block_links / block_links.
    """
    index_html = (Path(__file__).resolve().parent.parent
                  / "frontend" / "index.html").read_text(encoding="utf-8")
    assert 'Сравнивается весь документ целиком' in index_html, (
        "Не вижу основной фразы об full-document comparison"
    )
    # Для двух режимов даны поясняющие хвосты
    assert 'Связи блоков не используются' in index_html, (
        "Нет пояснения для concept_no_block_links"
    )
    assert 'Связанные блоки используются как ориентиры' in index_html, (
        "Нет пояснения для block_links (anchors)"
    )


# ─── Replace-image-blocks v1 ─────────────────────────────────────────────


def test_build_enriched_md_replaces_image_block_with_qwen_description():
    """В новом формате `replace_image_blocks_v1` исходный image-блок ПОЛНОСТЬЮ
    заменён на Qwen-описание. Никакого <!-- original_imagine_start --> рядом."""
    from backend.app.services.stage_comparison import md_image_enrichment as m

    blocks = [
        m.MdBlock(kind="text", text="### СТРАНИЦА 1\n\nНекий текст из спецификации.\n", page=1),
        m.MdBlock(
            kind="image",
            text="<image>some original imagine description that must NOT survive</image>\n",
            page=1,
            block_id="img-xyz",
            order=1,
            image_order_on_page=1,
        ),
        m.MdBlock(kind="text", text="Хвостовой текст после картинки.\n", page=1),
    ]
    desc = {
        "order": 1,
        "status": "done",
        "model_used": "qwen/qwen3.6-35b-a3b",
        "used_prompt_version": "v4_compact",
        "description": {
            "status": "done",
            "summary": "Однолинейная схема ВРУ.",
            "equipment": ["ВРУ-1"],
            "materials": [],
            "visible_text": ["шильд: ВРУ-1, 380В"],
            "numeric_parameters": [{"name": "U", "value": "380", "unit": "В"}],
            "scheme_analysis": {"is_scheme": True, "scheme_type": "electrical_single_line", "flow_medium": "electricity", "nodes": [], "connections": [], "sequence_summary": [], "independent_circuits": [], "comparison_relevant_scheme_facts": [], "uncertainties": []},
            "confidence": 0.87,
        },
    }
    enriched = m.build_enriched_md(blocks, [desc])
    # Wrapper присутствует
    assert "<!-- QWEN_IMAGE_DESCRIPTION_START" in enriched
    assert "<!-- QWEN_IMAGE_DESCRIPTION_END -->" in enriched
    # Format version в header'е
    assert "format_version: replace_image_blocks_v1" in enriched
    # Метаданные блока
    assert "block_id: img-xyz" in enriched
    assert "page: 1" in enriched
    assert "status: done" in enriched
    assert "prompt_version: v4_compact" in enriched
    # Старая обёртка отсутствует
    assert "original_imagine_start" not in enriched
    assert "original_imagine_end" not in enriched
    # Исходный imagine-контент НЕ протёк
    assert "some original imagine description that must NOT survive" not in enriched
    # Описания на месте
    assert "Однолинейная схема ВРУ" in enriched
    # Документ-уровневый header
    assert "<!-- ENRICHED_MD_FORMAT: replace_image_blocks_v1 -->" in enriched
    # Текстовые блоки сохранены
    assert "Некий текст из спецификации" in enriched
    assert "Хвостовой текст после картинки" in enriched


def test_build_enriched_md_replaces_failed_block_with_explicit_error_placeholder():
    """Если описание упало в error — в enriched.md остаётся явная заглушка,
    но НЕ старое описание."""
    from backend.app.services.stage_comparison import md_image_enrichment as m

    blocks = [m.MdBlock(
        kind="image",
        text="<image>BAD ORIGINAL OCR — must NOT survive</image>\n",
        page=4, block_id="bid", order=1, image_order_on_page=1,
    )]
    desc = {"order": 1, "status": "error", "error": "json_parse_failed", "description": {"status": "error"}}
    enriched = m.build_enriched_md(blocks, [desc])
    assert "<!-- QWEN_IMAGE_DESCRIPTION_START" in enriched
    assert "status: error" in enriched
    assert "error: json_parse_failed" in enriched
    assert "Графический блок не распознан" in enriched
    # старое описание не выживает
    assert "BAD ORIGINAL OCR" not in enriched


def test_build_enriched_md_preserves_document_order():
    """Текст до image, image, текст после — порядок сохраняется."""
    from backend.app.services.stage_comparison import md_image_enrichment as m

    blocks = [
        m.MdBlock(kind="text", text="TEXT-BEFORE\n", page=1),
        m.MdBlock(kind="image", text="<image>orig</image>\n", page=1, block_id="b1", order=1, image_order_on_page=1),
        m.MdBlock(kind="text", text="TEXT-AFTER\n", page=1),
    ]
    desc = {"order": 1, "status": "done", "description": {"status": "done", "summary": "QWEN-SUM"}}
    enriched = m.build_enriched_md(blocks, [desc])
    i_before = enriched.index("TEXT-BEFORE")
    i_block = enriched.index("QWEN_IMAGE_DESCRIPTION_START")
    i_after = enriched.index("TEXT-AFTER")
    i_qwen = enriched.index("QWEN-SUM")
    assert i_before < i_block < i_qwen < i_after


def test_detect_enriched_md_format():
    """Format detection: replace_image_blocks_v1 vs append_v0."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    new_fmt = "<!-- ENRICHED_MD_FORMAT: replace_image_blocks_v1 -->\n\n### BLOCK [TEXT]\nfoo"
    assert m.detect_enriched_md_format(new_fmt) == "replace_image_blocks_v1"
    old_fmt = "### BLOCK [IMAGE]\n<!-- original_imagine_start -->\n<image>x</image>\n<!-- original_imagine_end -->\n\n#### QWEN_IMAGE_DESCRIPTION\n..."
    assert m.detect_enriched_md_format(old_fmt) == "append_v0"
    # Empty file → legacy (rebuild safe no-op)
    assert m.detect_enriched_md_format("") == "unknown"
    assert m.detect_enriched_md_format(None) == "unknown"


@pytest.mark.asyncio
async def test_rebuild_enriched_md_from_existing_descriptions(_local_env, tmp_path):
    """rebuild_enriched_md_from_descriptions: не вызывает модель, использует
    существующие items из image_descriptions.json и переписывает enriched.md
    в новом формате."""
    from backend.app.services.stage_comparison import md_image_enrichment as m

    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")

    # Подготовим crops + result.json
    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(10, 20, 30))
    _write_png(crop_dir / "img-002.png", color=(40, 50, 60))
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({"pages": [
        {"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]}]},
        {"page_number": 2, "blocks": [{"id": "img-002", "block_type": "image", "coords_px": [0, 0, 100, 100]}]},
    ]}), encoding="utf-8")

    # Запустим enrich_side с fake-describe — получим валидные image_descriptions.json
    async def fake(image_path, prompt):
        from backend.app.services.stage_comparison.graphic_llm_local import DescribeResult
        name = Path(image_path).name
        return DescribeResult(
            status="done",
            parsed={"status": "done", "summary": f"REBUILD-FIXTURE {name}"},
            error=None,
            raw_response_excerpt=f'{{"status":"done","summary":"REBUILD-FIXTURE {name}"}}',
            model_used="qwen/qwen3.6-35b-a3b",
        )

    summary = await m.enrich_side(
        "sess_rebuild", "pair_rebuild", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=lambda b: crop_dir / f"{b}.png",
        describe_fn=fake, run_model=True,
    )
    assert summary.status == "done"

    # Симулируем legacy enriched.md (запишем как append_v0) — потом rebuild.
    enriched_path = Path(summary.enriched_md_path)
    legacy = (
        "### BLOCK [TEXT]\nЛегаси текст\n\n"
        "### BLOCK [IMAGE]\n<!-- original_imagine_start -->\n"
        "<image>legacy original</image>\n<!-- original_imagine_end -->\n\n"
        "#### QWEN_IMAGE_DESCRIPTION\nstatus: done\nОписание:\nlegacy desc\n"
    )
    enriched_path.write_text(legacy, encoding="utf-8")
    assert m.detect_enriched_md_format(legacy) == "append_v0"

    # Rebuild — без describe_fn, никаких сетевых вызовов.
    info = m.rebuild_enriched_md_from_descriptions("sess_rebuild", "pair_rebuild", "left", md_path=str(src_md))
    assert info["status"] == "rebuilt"
    assert info["enriched_md_format_version"] == "replace_image_blocks_v1"
    assert info["original_image_blocks"] == 2
    assert info["replaced_image_blocks"] == 2
    assert info["qwen_description_blocks"] == 2

    new_text = enriched_path.read_text(encoding="utf-8")
    assert "QWEN_IMAGE_DESCRIPTION_START" in new_text
    assert "original_imagine_start" not in new_text
    assert "legacy original" not in new_text
    # Описания подхватились из существующего image_descriptions.json:
    assert "REBUILD-FIXTURE img-001.png" in new_text
    assert "REBUILD-FIXTURE img-002.png" in new_text


@pytest.mark.asyncio
async def test_read_summary_only_reports_replacement_mode(_local_env, tmp_path):
    from backend.app.services.stage_comparison import md_image_enrichment as m

    src_md = tmp_path / "left.md"
    src_md.write_text(CHANDRA_MD_SAMPLE, encoding="utf-8")
    crop_dir = tmp_path / "crops"; crop_dir.mkdir()
    _write_png(crop_dir / "img-001.png", color=(10, 20, 30))
    _write_png(crop_dir / "img-002.png", color=(20, 30, 40))
    result_json = tmp_path / "result.json"
    result_json.write_text(json.dumps({"pages": [
        {"page_number": 1, "blocks": [{"id": "img-001", "block_type": "image", "coords_px": [0, 0, 100, 100]}]},
        {"page_number": 2, "blocks": [{"id": "img-002", "block_type": "image", "coords_px": [0, 0, 100, 100]}]},
    ]}), encoding="utf-8")

    async def fake(image_path, prompt):
        from backend.app.services.stage_comparison.graphic_llm_local import DescribeResult
        return DescribeResult(
            status="done",
            parsed={"status": "done", "summary": "OK"},
            error=None,
            raw_response_excerpt='{"status":"done","summary":"OK"}',
            model_used="qwen/qwen3.6-35b-a3b",
        )

    await m.enrich_side(
        "sess_sum", "pair_sum", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=lambda b: crop_dir / f"{b}.png",
        describe_fn=fake, run_model=True,
    )
    s = m.read_summary_only("sess_sum", "pair_sum", "left")
    assert s["enriched_md_format_version"] == "replace_image_blocks_v1"
    assert s["replacement_mode"] is True
    assert s["original_image_blocks"] == 2
    assert s["replaced_image_blocks"] == 2
    assert s["qwen_description_blocks"] == 2


# ─── Phase 1: block_type classifier ───────────────────────────────────────


def _mk_md_block(text, page=None, block_id=None, order=0):
    from backend.app.services.stage_comparison import md_image_enrichment as m
    return m.MdBlock(
        kind="image", text=text, page=page, block_id=block_id, order=order,
    )


def test_classify_image_block_scheme_markers():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    mb = _mk_md_block("### BLOCK [IMAGE]: blk1\n[IMAGE]\n")
    surrounding = "Однолинейная схема ВРУ-2 с.ш.1 ЩР-1а с автоматом QF3"
    assert m.classify_image_block(mb, surrounding_context=surrounding) == m.BLOCK_TYPE_SCHEME


def test_classify_image_block_dense_scheme_by_marker_count():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    mb = _mk_md_block("### BLOCK [IMAGE]: blk1\n[IMAGE]\n")
    surrounding = (
        "Однолинейная схема ВРУ ЩР ЩО QF QS кабель автомат линия "
        "QF1 QF2 кабельная линия ВВГнг ЩР-1а ЩР-2 ЩР-3 ВРУ-2 с.ш."
    )
    assert m.classify_image_block(mb, surrounding_context=surrounding) == m.BLOCK_TYPE_DENSE_SCHEME


def test_classify_image_block_dense_scheme_by_area_ratio():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    mb = _mk_md_block("### BLOCK [IMAGE]: blk1\n[IMAGE]\n")
    surrounding = "Схема электроснабжения ЩР"
    side_block = {"area_ratio": 0.55}
    assert m.classify_image_block(mb, side_block=side_block, surrounding_context=surrounding) == m.BLOCK_TYPE_DENSE_SCHEME


def test_classify_image_block_table_legend():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    mb = _mk_md_block("### BLOCK [IMAGE]: t\n[IMAGE]\n")
    surrounding = "Спецификация оборудования. Таблица 2. Условные обозначения"
    assert m.classify_image_block(mb, surrounding_context=surrounding) == m.BLOCK_TYPE_TABLE_LEGEND


def test_classify_image_block_stamp():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    mb = _mk_md_block("### BLOCK [IMAGE]: st\n[IMAGE]\n")
    surrounding = "Стадия Лист Изм. Подп. Дата Шифр"
    assert m.classify_image_block(mb, surrounding_context=surrounding) == m.BLOCK_TYPE_STAMP


def test_classify_image_block_plan():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    mb = _mk_md_block("### BLOCK [IMAGE]: p\n[IMAGE]\n")
    surrounding = "План этажа. Помещение по оси А-Б. Трасса кабеля."
    assert m.classify_image_block(mb, surrounding_context=surrounding) == m.BLOCK_TYPE_PLAN


def test_classify_image_block_general():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    mb = _mk_md_block("### BLOCK [IMAGE]: g\n[IMAGE]\n")
    surrounding = "Какое-то фото с объекта."
    assert m.classify_image_block(mb, surrounding_context=surrounding) == m.BLOCK_TYPE_GENERAL


def test_block_type_config_has_higher_sizing_for_scheme():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    gen = m.get_block_type_config(m.BLOCK_TYPE_GENERAL)
    sch = m.get_block_type_config(m.BLOCK_TYPE_SCHEME)
    dense = m.get_block_type_config(m.BLOCK_TYPE_DENSE_SCHEME)

    # Scheme/dense должны иметь БОЛЬШЕ render/image, чем general
    # (нужно читать мелкие маркировки).
    assert sch["render_target_long_side"] > gen["render_target_long_side"]
    assert sch["image_input_long_side"] > gen["image_input_long_side"]
    assert dense["render_target_long_side"] >= sch["render_target_long_side"]
    assert dense["image_input_long_side"] >= sch["image_input_long_side"]
    # Tokens override is set for scheme/dense — но в production-safe пределах.
    # После v5 validation report (2026-05-27): worst-case generation
    # не должен превышать ~10k tokens на блок (max_tokens × (cont+1)).
    assert sch["max_tokens"] is not None and sch["max_tokens"] > 0
    assert dense["max_tokens"] is not None and dense["max_tokens"] >= sch["max_tokens"]
    # Worst-case generation per block = max_tokens × (max_continuations + 1).
    # Должно быть <= 10000 чтобы Qwen не висел на одном блоке часами.
    sch_worst = sch["max_tokens"] * (sch["max_continuations"] + 1)
    dense_worst = dense["max_tokens"] * (dense["max_continuations"] + 1)
    assert sch_worst <= 10000, f"scheme worst-case {sch_worst} > 10000"
    assert dense_worst <= 10000, f"dense_scheme worst-case {dense_worst} > 10000"
    # dense_scheme max_continuations не должен быть выше 1 по дефолту
    # (после 245s/block validation report).
    assert dense["max_continuations"] <= 1


def test_block_type_config_env_overrides(monkeypatch):
    """Env override должен подменять default render/max_tokens per-type."""
    import importlib
    from backend.app.services.stage_comparison import md_image_enrichment as m
    monkeypatch.setenv("STAGE_COMPARISON_DENSE_SCHEME_MAX_TOKENS", "1234")
    monkeypatch.setenv("STAGE_COMPARISON_DENSE_SCHEME_IMAGE_LONG_SIDE", "999")
    importlib.reload(m)
    dense = m.get_block_type_config(m.BLOCK_TYPE_DENSE_SCHEME)
    assert dense["max_tokens"] == 1234
    assert dense["image_input_long_side"] == 999
    # cleanup
    monkeypatch.delenv("STAGE_COMPARISON_DENSE_SCHEME_MAX_TOKENS", raising=False)
    monkeypatch.delenv("STAGE_COMPARISON_DENSE_SCHEME_IMAGE_LONG_SIDE", raising=False)
    importlib.reload(m)


def test_get_prompt_for_block_type_returns_v5_for_scheme():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p_gen, v_gen = m.get_prompt_for_block_type(m.BLOCK_TYPE_GENERAL)
    p_sch, v_sch = m.get_prompt_for_block_type(m.BLOCK_TYPE_SCHEME)
    p_dense, v_dense = m.get_prompt_for_block_type(m.BLOCK_TYPE_DENSE_SCHEME)
    assert v_gen == m.PROMPT_VERSION_GENERAL
    assert v_sch == m.PROMPT_VERSION_SCHEME
    assert v_dense == m.PROMPT_VERSION_SCHEME
    assert p_sch is not p_gen
    assert p_dense is p_sch  # тот же prompt


# ─── Phase 2: v5 scheme prompt content ────────────────────────────────────


def test_v5_scheme_prompt_requires_literal_raw_text():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p = m.QWEN_SCHEME_DIFF_ANCHORS_PROMPT
    # raw_text должен быть буквальной видимой надписью
    assert "raw_text" in p
    # Запреты на нормализацию маркировки
    assert "ЩР-1а" in p
    assert "Щит 1" in p
    # explicit forbidden normalization (ВРУ → вводное)
    assert ("ВРУ-2" in p) or ("ВРУ" in p)
    # generic катологи запрещены
    assert ("100А" in p or "каталог" in p.lower() or "номиналы" in p.lower())


def test_v5_scheme_prompt_has_diff_anchors_schema():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p = m.QWEN_SCHEME_DIFF_ANCHORS_PROMPT
    assert "diff_anchors" in p
    assert "labels" in p and "ratings" in p and "connections" in p
    assert "uncertain_text" in p
    # Поля внутри labels
    assert "normalized_type" in p
    # Категории connections
    assert "from_raw" in p and "to_raw" in p


def test_v5_scheme_prompt_forbids_artificial_sequences():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p = m.QWEN_SCHEME_DIFF_ANCHORS_PROMPT
    low = p.lower()
    assert "искусственн" in low or "не перечисляй" in low


def test_format_qwen_description_md_renders_diff_anchors_before_summary():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "done",
        "summary": "Однолинейная схема ВРУ → ЩР",
        "confidence": 0.7,
        "diff_anchors": {
            "labels": [
                {"raw_text": "ЩР-1а", "normalized_type": "panel", "confidence": 0.9},
                {"raw_text": "ВРУ-2 с.ш.1", "normalized_type": "switchgear", "confidence": 0.85},
                {"raw_text": "QF3", "normalized_type": "breaker", "confidence": 0.7},
            ],
            "ratings": [
                {"raw_text": "1000А", "value_type": "current_rating", "related_to": "ВРУ-2 с.ш.1"},
                {"raw_text": "4х185", "value_type": "cable_section"},
            ],
            "connections": [
                {"from_raw": "ВРУ-2 с.ш.1", "to_raw": "ЩР-1а", "relation": "питает"},
            ],
            "uncertain_text": [
                {"possible_text": "ЩР-1?", "alternatives": ["ЩО-1?"], "confidence": 0.4,
                 "why_uncertain": "мелкий шрифт"},
            ],
        },
        "visible_text": ["ЩР-1а", "1000А"],
    }
    body = m._format_qwen_description_md(payload, model="qwen", page=24, block_id="b1")
    # DIFF_ANCHORS секции присутствуют ДО «Краткое описание»
    pos_anchors = body.find("DIFF_ANCHORS")
    pos_summary = body.find("Краткое описание")
    assert pos_anchors >= 0
    assert pos_summary >= 0
    assert pos_anchors < pos_summary
    # raw маркировки сохранены БУКВАЛЬНО
    assert "ЩР-1а" in body
    assert "ВРУ-2 с.ш.1" in body
    assert "QF3" in body
    assert "1000А" in body
    assert "4х185" in body
    # Секция связи
    assert "ВРУ-2 с.ш.1 → ЩР-1а" in body
    # uncertain text
    assert "ЩР-1?" in body


# ─── Phase 3: IMAGE_DIFF_INDEX ────────────────────────────────────────────


def test_build_image_diff_index_includes_anchors():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    descriptions = [
        {
            "order": 1, "page": 24, "md_block_id": "blk-A",
            "block_type": "scheme", "usable_for_diff": True, "warnings": [],
            "status": "done",
            "description": {
                "status": "done", "confidence": 0.74,
                "diff_anchors": {
                    "labels": [
                        {"raw_text": "ЩР-1а"},
                        {"raw_text": "ЩР-2"},
                        {"raw_text": "ВРУ-2 с.ш.1"},
                        {"raw_text": "QF3"},
                    ],
                    "ratings": [
                        {"raw_text": "1000А"},
                        {"raw_text": "4х185"},
                    ],
                    "connections": [
                        {"from_raw": "ВРУ-2 с.ш.1", "to_raw": "ЩР-1а"},
                        {"from_raw": "ВРУ-2 с.ш.1", "to_raw": "ЩР-2"},
                    ],
                },
            },
        },
        {
            "order": 2, "page": 26, "md_block_id": "blk-B",
            "block_type": "dense_scheme", "usable_for_diff": False,
            "warnings": ["hallucination_suspected", "continuation_salvaged",
                         "repeated_pattern_detected"],
            "status": "partial",
            "description": {
                "status": "salvaged_partial", "confidence": 0.41,
                "diff_anchors": {"labels": [{"raw_text": "ВРП-1?"}, {"raw_text": "ВРП-2?"}]},
            },
        },
    ]
    idx = m.build_image_diff_index(descriptions)
    assert "<!-- IMAGE_DIFF_INDEX_START -->" in idx
    assert "<!-- IMAGE_DIFF_INDEX_END -->" in idx
    assert "scheme" in idx and "dense_scheme" in idx
    assert "usable_for_diff=true" in idx
    assert "usable_for_diff=false" in idx
    assert "ЩР-1а" in idx
    assert "1000А" in idx
    assert "ВРУ-2 с.ш.1 -> ЩР-1а" in idx
    assert "hallucination_suspected" in idx


def test_build_image_diff_index_fallback_for_v4_blocks():
    """v4 блоки без diff_anchors всё равно дают что-то в индекс через
    visible_text / scheme_analysis.nodes."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    descriptions = [
        {
            "order": 1, "page": 5, "md_block_id": "blk-v4",
            "block_type": "scheme", "usable_for_diff": True, "warnings": [],
            "status": "done",
            "description": {
                "status": "done", "confidence": 0.6,
                "visible_text": ["ЩР-1а", "1000А"],
                "scheme_analysis": {
                    "is_scheme": True,
                    "nodes": [
                        {"visible_mark": "ВРУ-2", "label": "ВРУ"},
                        {"visible_mark": "ЩР-1а"},
                    ],
                    "connections": [
                        {"from": "ВРУ-2", "to": "ЩР-1а"},
                    ],
                },
            },
        },
    ]
    idx = m.build_image_diff_index(descriptions)
    assert "ЩР-1а" in idx
    assert "ВРУ-2 -> ЩР-1а" in idx


def test_build_enriched_md_inserts_diff_index_near_top():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    text = "### СТРАНИЦА 1\nfoo\n\n### BLOCK [IMAGE]: b1\n[IMAGE]\n"
    blocks = m.parse_md_blocks(text)
    descriptions = [{
        "order": 1, "page": 1, "md_block_id": "b1",
        "block_type": "scheme", "usable_for_diff": True, "warnings": [],
        "status": "done",
        "description": {
            "status": "done", "confidence": 0.8,
            "diff_anchors": {"labels": [{"raw_text": "ЩР-1а"}]},
            "summary": "x",
        },
    }]
    enriched = m.build_enriched_md(blocks, descriptions)
    pos_header = enriched.find("ENRICHED_MD_FORMAT")
    pos_index_start = enriched.find("IMAGE_DIFF_INDEX_START")
    pos_first_qwen = enriched.find("QWEN_IMAGE_DESCRIPTION_START")
    assert 0 <= pos_header < pos_index_start < pos_first_qwen


# ─── Phase 4: hallucination heuristics ────────────────────────────────────


def test_analyze_quality_flags_artificial_sequence_alone_is_not_fatal():
    """После v5 production tuning: simple repeated series ≥6 (без других
    сигналов) — это repeated_pattern_detected (suspicious, info-level),
    НО не hallucination_suspected и НЕ выключает usable_for_diff.

    В МКД проекте 6-8 квартирных щитов или групповых линий могут быть
    реальными. Эскалация до hallucination только при суперпозиции сигналов.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "done",
        "diff_anchors": {
            "labels": [{"raw_text": f"ВРП-{i}"} for i in range(1, 9)],
        },
        "confidence": 0.6,
    }
    res = m.analyze_qwen_description_quality(payload, {"block_type": "scheme"})
    assert "repeated_pattern_detected" in res["warnings"]
    # alone-flag не должен убить usable_for_diff
    assert "hallucination_suspected" not in res["warnings"]
    assert res["usable_for_diff"] is True


def test_analyze_quality_flags_artificial_sequence_with_truncation_is_fatal():
    """repeated_pattern + truncated_output → hallucination_suspected → usable=False."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "salvaged_partial",
        "diff_anchors": {
            "labels": [{"raw_text": f"ВРП-{i}"} for i in range(1, 9)],
        },
        "confidence": 0.6,
    }
    ctx = {"block_type": "scheme", "salvaged": True, "parse_error_detail": "truncated_json"}
    res = m.analyze_qwen_description_quality(payload, ctx)
    assert "repeated_pattern_detected" in res["warnings"]
    assert "hallucination_suspected" in res["warnings"]
    assert "truncated_output" in res["warnings"]
    assert res["usable_for_diff"] is False


def test_analyze_quality_flags_generic_rating_list_without_labels():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "done",
        "confidence": 0.6,
        "diff_anchors": {
            "labels": [],
            "ratings": [
                {"raw_text": "4x16"}, {"raw_text": "4x25"},
                {"raw_text": "4x35"}, {"raw_text": "4x50"},
                {"raw_text": "4x70"}, {"raw_text": "4x95"},
                {"raw_text": "4x120"},
            ],
        },
    }
    res = m.analyze_qwen_description_quality(payload, {"block_type": "scheme"})
    assert "generic_rating_list_without_labels" in res["warnings"]


def test_analyze_quality_propagates_continuation_warnings():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "done",
        "confidence": 0.7,
        "diff_anchors": {"labels": [{"raw_text": "ЩР-1а"}]},
    }
    ctx = {
        "block_type": "scheme",
        "continuation_warnings": ["hint_repeated", "cap_reached_chunk_3"],
        "salvaged": True,
        "parse_error_detail": "truncated_json",
    }
    res = m.analyze_qwen_description_quality(payload, ctx)
    assert "continuation_salvaged" in res["warnings"]
    assert "continuation_repeated" in res["warnings"]
    assert "truncated_output" in res["warnings"]


def test_analyze_quality_truncated_alone_with_valid_anchors_stays_usable():
    """Smoke validation 2026-05-28: dense_scheme штатно truncates на
    prompt cap=25/20/15 + max_tokens=4000. truncated_output БЕЗ других
    hallucination-сигналов (нет ряда, нет chain, нет identical comments,
    нет generic ratings) НЕ должен делать блок usable_for_diff=False.

    Это покрывает реальный случай smoke 2 (right o4): 28 различных labels
    ВРУ2-ПП1-N (mixed series, не один длинный ряд), truncated_json.
    Раньше len(labels)>=23 + truncated → 2 signals → hallucination →
    usable=False. После fix: truncated alone → 1 signal → usable=True.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    # 25 различных labels — mixed series, не single long-range ряд.
    # Имитирует cap-bound вывод модели с разнообразными panel marks.
    labels = (
        [{"raw_text": f"ВРУ2-ПП1-{i}"} for i in range(1, 9)]
        + [{"raw_text": f"ВРУ2-ПП2-{i}"} for i in range(1, 9)]
        + [{"raw_text": f"ВРУ2-ПП3-{i}"} for i in range(1, 10)]
    )
    payload = {
        "status": "salvaged_partial",
        "diff_anchors": {"labels": labels},
        "confidence": 0.5,
    }
    ctx = {
        "block_type": "dense_scheme",
        "salvaged": True,
        "parse_error_detail": "truncated_json",
    }
    res = m.analyze_qwen_description_quality(payload, ctx)
    assert "truncated_output" in res["warnings"]
    # Эти НЕ должны быть установлены на mixed-series без catalog-fill.
    assert "hallucination_suspected" not in res["warnings"], res["warnings"]
    assert "repeated_pattern_detected" not in res["warnings"], res["warnings"]
    assert "serial_chain_connection_detected" not in res["warnings"], res["warnings"]
    # Главное: truncated alone не убивает usable_for_diff.
    assert res["usable_for_diff"] is True, res["warnings"]


def test_analyze_quality_low_label_recall_for_scheme():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "done",
        "confidence": 0.5,
        "diff_anchors": {
            "labels": [
                {"raw_text": "[маркировка не читается]"},
                {"raw_text": "Щит 1"},
            ],
        },
    }
    res = m.analyze_qwen_description_quality(payload, {"block_type": "scheme"})
    assert "low_literal_label_recall" in res["warnings"]
    assert res["usable_for_diff"] is False


# ─── Phase 5: enriched_comparison evidence handling ────────────────────────


def test_normalize_change_preserves_evidence_array():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    raw = {
        "title": "Замена кабеля",
        "summary": "Сечение изменилось с 4х95 на 4х185",
        "source": "mixed",
        "evidence": [
            {"origin": "text", "side": "left", "page": 12, "quote": "ВВГнг 4х95"},
            {"origin": "image_enrichment", "side": "right", "page": 12, "quote": "4х185",
             "block_id": "blk-22"},
        ],
    }
    out = ec._normalize_change(raw)
    assert out is not None
    assert "evidence" in out
    assert len(out["evidence"]) == 2
    assert out["evidence"][1]["origin"] == "image_enrichment"
    assert out["evidence"][1]["block_id"] == "blk-22"


def test_normalize_change_forces_mixed_when_visual_and_nonvisual_evidence():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    raw = {
        "title": "Замена ЩР",
        "summary": "видно и в тексте и на схеме",
        "source": "text",  # Opus может ошибочно поставить text
        "evidence": [
            {"origin": "text", "side": "left", "page": 5, "quote": "ЩР-1"},
            {"origin": "scheme_analysis", "side": "right", "page": 5, "quote": "ЩР-1а"},
        ],
    }
    out = ec._normalize_change(raw)
    # Принуждение source → mixed
    assert out is not None
    assert out["source"] == "mixed"


def test_normalize_change_forces_visual_when_text_with_visual_evidence_only():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    raw = {
        "title": "Новая позиция QF",
        "summary": "появилась только на схеме",
        "source": "text",
        "evidence": [
            {"origin": "image_enrichment", "side": "right", "page": 8, "quote": "QF3"},
        ],
    }
    out = ec._normalize_change(raw)
    assert out is not None
    assert out["source"] == "image_enrichment"


def test_normalize_change_old_changes_without_evidence_still_work():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    raw = {
        "title": "Текстовое изменение",
        "summary": "что-то",
        "source": "text",
    }
    out = ec._normalize_change(raw)
    assert out is not None
    assert "evidence" not in out  # не плодим пустых массивов
    assert out["source"] == "text"


def test_enriched_comparison_prompt_mentions_image_diff_index_and_mixed_rule():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    sp = ec.SYSTEM_PROMPT
    # IMAGE_DIFF_INDEX упоминается явно
    assert "IMAGE_DIFF_INDEX" in sp
    # Запрет text для визуальных evidence
    assert "source=text" in sp or "`text`" in sp
    # Mixed rule обязателен
    assert "mixed" in sp.lower()
    # Пояснение про textual sheet lists
    assert "scheme_analysis" in sp


# ─── Phase 6: unified_findings metrics ────────────────────────────────────


def test_unified_findings_empty_summary_has_visual_evidence_counters():
    from backend.app.services.stage_comparison import unified_findings as uf
    s = uf._empty_summary()
    assert "visual_evidence_changes" in s
    assert "mixed_evidence_changes" in s
    assert "image_enrichment_evidence_changes" in s
    assert "scheme_analysis_evidence_changes" in s
    assert "image_diff_index_evidence_changes" in s


def test_change_has_visual_evidence_by_source_and_evidence_array():
    from backend.app.services.stage_comparison import unified_findings as uf
    # source=text + no evidence → не visual
    assert uf._change_has_visual_evidence({"source": "text"}) is False
    # source=mixed → visual
    assert uf._change_has_visual_evidence({"source": "mixed"}) is True
    # source=image_enrichment → visual
    assert uf._change_has_visual_evidence({"source": "image_enrichment"}) is True
    # source=text + evidence содержит image_diff_index → visual
    assert uf._change_has_visual_evidence({
        "source": "text",
        "evidence": [{"origin": "image_diff_index", "quote": "ЩР-1а"}],
    }) is True


# ─── v5 production tuning (2026-05-27 follow-up): bounded caps ────────────


def test_v5_prompt_has_bounded_caps():
    """v5 prompt должен явно ограничивать массивы 25/20/15/10 и запрещать
    extrapolated series + chain connections."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    p = m.QWEN_SCHEME_DIFF_ANCHORS_PROMPT
    # Жёсткие лимиты
    assert "labels` ≤ **25**" in p or "labels ≤ 25" in p or "labels` ≤ 25" in p, "labels cap missing"
    assert "ratings` ≤ **20**" in p or "ratings ≤ 20" in p, "ratings cap missing"
    assert "connections` ≤ **15**" in p or "connections ≤ 15" in p, "connections cap missing"
    assert "uncertain_text` ≤ **10**" in p or "uncertain_text ≤ 10" in p, "uncertain cap missing"
    # Анти-экстраполяция и анти-цепочка явно прописаны
    assert "АНТИ-ЭКСТРАПОЛЯЦИЯ" in p or "ЭКСТРАПОЛЯЦИИ" in p or "не достраивай" in p.lower()
    assert "АНТИ-ЦЕПОЧКА" in p or "ЦЕПОЧКА" in p or "цепочк" in p.lower()
    # Анти-дубликатные комментарии
    assert "comment" in p.lower() and ("одинаков" in p.lower() or "идентичн" in p.lower() or "повтор" in p.lower())


def test_block_type_config_worst_case_is_bounded():
    """После v5 tuning: worst-case generation = max_tokens × (cont+1)
    должно быть <= 10000 для scheme/dense_scheme, чтобы один блок не
    тянулся часами."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    for bt in (m.BLOCK_TYPE_SCHEME, m.BLOCK_TYPE_DENSE_SCHEME):
        cfg = m.get_block_type_config(bt)
        worst = cfg["max_tokens"] * (cfg["max_continuations"] + 1)
        assert worst <= 10000, f"{bt}: worst-case {worst} > 10000"
    dense = m.get_block_type_config(m.BLOCK_TYPE_DENSE_SCHEME)
    assert dense["max_continuations"] <= 1
    # image_input_long_side не должен быть 2800 по дефолту (validation
    # report показал что 2800 + 10k tokens — это ~4 мин/блок)
    assert dense["image_input_long_side"] < 2800


# ─── Subindex artificial sequence detection ───────────────────────────────


def test_parse_anchor_series_key_top_level():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    assert m.parse_anchor_series_key("ЩР-1") == ("ЩР", 1)
    assert m.parse_anchor_series_key("ЩР-10") == ("ЩР", 10)
    assert m.parse_anchor_series_key("ВРУ-2") == ("ВРУ", 2)
    assert m.parse_anchor_series_key("QF12") == ("QF", 12)
    assert m.parse_anchor_series_key("QF-3") == ("QF", 3)


def test_parse_anchor_series_key_subindex():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    assert m.parse_anchor_series_key("ЩА-1.5") == ("ЩА-1", 5)
    assert m.parse_anchor_series_key("ЩР-2.10") == ("ЩР-2", 10)
    assert m.parse_anchor_series_key("ЩО-1-12") == ("ЩО-1", 12)
    assert m.parse_anchor_series_key("QF-3.7") == ("QF-3", 7)


def test_parse_anchor_series_key_non_series():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    # Не маркировка серии — None
    assert m.parse_anchor_series_key("") is None
    assert m.parse_anchor_series_key("просто текст") is None
    assert m.parse_anchor_series_key("[маркировка не читается]") is None


def test_detect_artificial_sequences_top_level():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    labels = [{"raw_text": f"ЩР-{i}"} for i in range(1, 9)]  # 8 подряд
    res = m._detect_artificial_sequences(labels)
    assert "ЩР" in res


def test_detect_artificial_sequences_subindex_ЩА():
    """ЩА-1.1 ... ЩА-1.8 теперь ловится (после v5 production tuning)."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    labels = [{"raw_text": f"ЩА-1.{i}"} for i in range(1, 9)]
    res = m._detect_artificial_sequences(labels)
    assert "ЩА-1" in res, f"expected ЩА-1 series detected, got {res}"


def test_detect_artificial_sequences_subindex_ЩР_2():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    labels = [{"raw_text": f"ЩР-2.{i}"} for i in range(1, 11)]  # 10 номеров
    res = m._detect_artificial_sequences(labels)
    assert "ЩР-2" in res


def test_detect_artificial_sequences_subindex_QF_3():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    labels = [{"raw_text": f"QF-3.{i}"} for i in range(1, 9)]
    res = m._detect_artificial_sequences(labels)
    assert "QF-3" in res


def test_detect_artificial_sequences_short_sparse():
    """Короткие/разреженные серии (3-4 элемента) — не считаются hallucination."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    labels = [{"raw_text": "ЩА-1.1"}, {"raw_text": "ЩА-1.2"}, {"raw_text": "ЩА-1.7"}]
    res = m._detect_artificial_sequences(labels)
    assert res == [], f"short sparse list should not flag, got {res}"


# ─── Serial chain connection detector ─────────────────────────────────────


def test_detect_serial_chain_connections_subindex_chain():
    """ЩА-1.1 → ЩА-1.2 → ЩА-1.3 → ... → ЩА-1.6 (5+ шагов) — это галлюцинация."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    connections = [
        {"from_raw": f"ЩА-1.{i}", "to_raw": f"ЩА-1.{i+1}", "relation": "питает"}
        for i in range(1, 7)
    ]
    res = m._detect_serial_chain_connections(connections)
    assert "ЩА-1" in res


def test_detect_serial_chain_connections_star_topology():
    """Звезда: ВРУ-2 → ЩА-1.1, ВРУ-2 → ЩА-1.2, ... — нормальная топология,
    не должна флагаться."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    connections = [
        {"from_raw": "ВРУ-2", "to_raw": f"ЩА-1.{i}", "relation": "питает"}
        for i in range(1, 8)
    ]
    res = m._detect_serial_chain_connections(connections)
    assert res == []


def test_detect_serial_chain_connections_short_chain():
    """3 шага цепочки — слишком мало для уверенного флага."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    connections = [
        {"from_raw": f"ЩА-1.{i}", "to_raw": f"ЩА-1.{i+1}", "relation": "питает"}
        for i in range(1, 4)
    ]
    res = m._detect_serial_chain_connections(connections)
    assert res == []


# ─── Composite hallucination detection ────────────────────────────────────


def test_analyze_quality_subindex_alone_is_repeated_but_usable():
    """ЩА-1.1 ... ЩА-1.8 без других сигналов: repeated_pattern_detected,
    но НЕ hallucination_suspected (МКД проект может реально иметь 8 квартир).
    usable_for_diff остаётся True."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "done",
        "diff_anchors": {
            "labels": [{"raw_text": f"ЩА-1.{i}"} for i in range(1, 9)],
        },
        "confidence": 0.7,
    }
    res = m.analyze_qwen_description_quality(payload, {"block_type": "scheme"})
    assert "repeated_pattern_detected" in res["warnings"]
    assert "hallucination_suspected" not in res["warnings"]
    assert res["usable_for_diff"] is True
    # confidence чуть снижается даже для repeated-alone
    assert res["adjusted_confidence"] is not None
    assert res["adjusted_confidence"] < 0.7


def test_analyze_quality_subindex_plus_identical_comments_is_hallucination():
    """ЩА-1.1 ... ЩА-1.20 + одинаковые comments → hallucination_suspected → usable=False."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "done",
        "diff_anchors": {
            "labels": [
                {"raw_text": f"ЩА-1.{i}", "comment": "читается в левой части схемы"}
                for i in range(1, 21)
            ],
        },
        "confidence": 0.6,
    }
    res = m.analyze_qwen_description_quality(payload, {"block_type": "scheme"})
    assert "repeated_pattern_detected" in res["warnings"]
    assert "identical_comments_detected" in res["warnings"]
    assert "hallucination_suspected" in res["warnings"]
    assert res["usable_for_diff"] is False


def test_analyze_quality_subindex_plus_serial_chain_is_hallucination():
    """ЩА-1.1 ... ЩА-1.10 labels + chain connections → hallucination_suspected.
    Это в точности тот паттерн, что вернул Qwen в validation report 2026-05-27.
    """
    from backend.app.services.stage_comparison import md_image_enrichment as m
    payload = {
        "status": "done",
        "diff_anchors": {
            "labels": [{"raw_text": f"ЩА-1.{i}"} for i in range(1, 11)],
            "connections": [
                {"from_raw": f"ЩА-1.{i}", "to_raw": f"ЩА-1.{i+1}", "relation": "питает"}
                for i in range(1, 7)
            ],
        },
        "confidence": 0.5,
    }
    res = m.analyze_qwen_description_quality(payload, {"block_type": "dense_scheme"})
    assert "repeated_pattern_detected" in res["warnings"]
    assert "serial_chain_connection_detected" in res["warnings"]
    assert "hallucination_suspected" in res["warnings"]
    assert res["usable_for_diff"] is False


def test_comments_mostly_identical_helper():
    from backend.app.services.stage_comparison import md_image_enrichment as m
    # 12 одинаковых comments
    labels = [{"raw_text": f"X-{i}", "comment": "одно и то же"} for i in range(1, 13)]
    assert m._comments_mostly_identical(labels) is True
    # Все разные
    labels2 = [{"raw_text": f"X-{i}", "comment": f"уникальный коммент {i}"} for i in range(1, 13)]
    assert m._comments_mostly_identical(labels2) is False
    # Слишком мало labels
    labels3 = [{"raw_text": f"X-{i}", "comment": "одинаково"} for i in range(1, 5)]
    assert m._comments_mostly_identical(labels3) is False

