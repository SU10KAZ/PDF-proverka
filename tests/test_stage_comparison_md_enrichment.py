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

    # Enriched MD содержит обе части
    enriched_path = Path(summary.enriched_md_path)
    enriched = enriched_path.read_text(encoding="utf-8")
    assert "original_imagine_start" in enriched
    assert "original_imagine_end" in enriched
    assert "QWEN_IMAGE_DESCRIPTION" in enriched
    assert "OK for img-001.png" in enriched
    assert "OK for img-002.png" in enriched
    # Текст оригинала тоже остался
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
    # Enriched MD всё равно создаётся
    assert summary.enriched_md_path and Path(summary.enriched_md_path).exists()
    enriched = Path(summary.enriched_md_path).read_text(encoding="utf-8")
    assert "QWEN_IMAGE_DESCRIPTION" in enriched
    assert "status: error" in enriched


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
    """PROMPT_VERSION должен быть v2_scheme_analysis (или новее)."""
    from backend.app.services.stage_comparison import md_image_enrichment as m
    assert m.PROMPT_VERSION == "v2_scheme_analysis"


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
