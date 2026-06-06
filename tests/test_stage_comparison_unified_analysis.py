"""Unit tests for unified stage comparison analysis pipeline.

Покрывает критерии из ТЗ:

  1.  enriched_comparison prompt содержит инструкции сравнивать enriched MD;
  2.  Opus provider disabled → status="disabled";
  3.  provider_not_available → prompt.md сохраняется;
  4.  valid Opus JSON → comparison_result.json сохраняется со status="done";
  5.  invalid JSON → status="invalid_json";
  6.  unified preflight считает image_blocks / enriched_ready / compare_ready;
  7.  unified run_pair вызывает enrichment, затем compare;
  8.  если enriched MD есть — enrichment skipped;
  9.  если force_enrichment=true — enrichment перезапускается;
  10. если force_compare=true — comparison перезапускается;
  11. unified job по session проходит несколько pair items;
  12. cancel job работает;
  13. unified-diff-flat агрегирует changes;
  14. UI subtab «Расхождения» по умолчанию = unified (не text/graphic);
  15. Old endpoints не сломаны (импорт + регистрация);
  16. No live Qwen/Opus calls (все провайдеры замоканы).
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest


# ─── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    # Disable both Opus and Qwen by default
    monkeypatch.delenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", raising=False)
    yield root


def _make_pair(session_id: str, pair_id: str, *,
               left_md: Path | None, right_md: Path | None) -> dict:
    """Создать session.json + pair.json с минимальными данными."""
    from backend.app.services.stage_comparison import paths as paths_mod

    session = {
        "id": session_id,
        "pair_order": [pair_id],
        "warnings": [],
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    paths_mod.session_json_path(session_id).write_text(
        json.dumps(session, ensure_ascii=False), encoding="utf-8",
    )
    pair = {
        "id": pair_id,
        "status": "matched",
        "left": {
            "filename": "left.pdf",
            "pdf_path": "/dev/null/left.pdf",
            "md_path": (str(left_md) if left_md else None),
        },
        "right": {
            "filename": "right.pdf",
            "pdf_path": "/dev/null/right.pdf",
            "md_path": (str(right_md) if right_md else None),
        },
    }
    paths_mod.pair_json_path(session_id, pair_id).write_text(
        json.dumps(pair, ensure_ascii=False), encoding="utf-8",
    )
    return pair


def _write_md(p: Path, content: str) -> Path:
    p.write_text(content, encoding="utf-8")
    return p


def _write_enriched(session_id: str, pair_id: str, side: str, content: str) -> Path:
    """Записать enriched MD в pair-папку (минует Qwen)."""
    from backend.app.services.stage_comparison import paths as paths_mod
    p = paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return p


# ─── Mock providers ─────────────────────────────────────────────────────


class _UnavailableProvider:
    """Provider, который check_availability() возвращает False."""
    name = "mock_unavailable"

    def check_availability(self):
        return False, "binary_not_found"

    def invoke(self, **kwargs):
        raise AssertionError("invoke should not be called when unavailable")


class _AvailableProvider:
    """Provider, который возвращает кастомный raw_response."""

    name = "mock_available"

    def __init__(self, raw_response: str, status: str = "done"):
        self.raw_response = raw_response
        self.status = status
        self.invoke_calls = []

    def check_availability(self):
        return True, None

    def invoke(self, *, system_prompt, user_prompt, model, timeout_sec, work_dir=None):
        from backend.app.services.stage_comparison.text_llm_provider import ProviderResult
        self.invoke_calls.append({
            "model": model, "system_prompt_len": len(system_prompt),
            "user_prompt_len": len(user_prompt), "timeout_sec": timeout_sec,
            "work_dir": str(work_dir) if work_dir else None,
        })
        return ProviderResult(
            status=self.status,
            raw_response=self.raw_response,
            duration_sec=0.01,
            provider=self.name,
            model=model,
        )


# ─── Tests ──────────────────────────────────────────────────────────────


# 1. Prompt содержит инструкции по enriched MD.
def test_enriched_comparison_prompt_mentions_enriched_md_and_scheme():
    from backend.app.services.stage_comparison import enriched_comparison as ec

    system_prompt, user_prompt = ec.build_prompts("LEFT", "RIGHT")
    # Системный prompt объясняет, что приходит enriched MD и упоминает scheme_analysis
    assert "enriched" in system_prompt.lower()
    assert "QWEN_IMAGE_DESCRIPTION" in system_prompt
    assert "scheme_analysis" in system_prompt or "Схемный" in system_prompt
    # Источники для каждого change
    assert "image_enrichment" in system_prompt
    assert "scheme_analysis" in system_prompt
    # User-prompt содержит обе стороны
    assert "<OLD_ENRICHED_MD>" in user_prompt and "<NEW_ENRICHED_MD>" in user_prompt
    assert "LEFT" in user_prompt and "RIGHT" in user_prompt


# 1b. Prompt описывает новый формат enriched MD (replace_image_blocks_v1).
def test_enriched_comparison_prompt_mentions_replacement_format():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    system_prompt, _ = ec.build_prompts("L", "R")
    # Упоминаются новые маркеры обёртки и format version.
    assert "QWEN_IMAGE_DESCRIPTION_START" in system_prompt
    assert "QWEN_IMAGE_DESCRIPTION_END" in system_prompt
    assert "replace_image_blocks_v1" in system_prompt
    assert "ЗАМЕНЁН" in system_prompt or "заменён" in system_prompt.lower()
    # Чётко указано, что Qwen не дублируется со старым OCR.
    assert "Старого OCR" in system_prompt or "OCR-описания image" in system_prompt.lower()


# 1c. Prompt трактует block_links как anchors / focus, а не exclusive scope.
def test_enriched_comparison_prompt_block_links_are_anchors_not_scope():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    system_prompt, _ = ec.build_prompts("L", "R")
    sys_lc = system_prompt.lower()
    assert "anchors" in sys_lc or "якор" in sys_lc
    # И прямо сказано про full-document scope.
    assert "весь" in sys_lc or "всему" in sys_lc or "целиком" in sys_lc
    # Explicit «не exclusive scope»: предупреждение, что искать надо и вне.
    assert "вне" in sys_lc


# 1d. build_user_prompt with block_links includes <BLOCK_LINKS> section.
def test_build_user_prompt_includes_block_links_when_provided():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    links = [
        {"left_block_id": "L1", "right_block_id": "R1", "left_page": 1, "right_page": 2, "method": "manual", "score": 1.0},
        {"left_block_id": "L2", "right_block_id": "R2", "left_page": 1, "right_page": 2, "method": "iou", "score": 0.8},
    ]
    user_prompt = ec.build_user_prompt(
        "LEFT-MD-BODY", "RIGHT-MD-BODY",
        block_links=links, analysis_mode="block_links",
    )
    assert "<BLOCK_LINKS>" in user_prompt
    assert "</BLOCK_LINKS>" in user_prompt
    assert "L1" in user_prompt and "R1" in user_prompt
    # Heavy fields не передаются (нет таких ключей):
    assert "base64" not in user_prompt.lower()
    # Режим явно указан
    assert "block_links" in user_prompt
    # Тэг режима concept_no_block_links не должен попасть.
    assert "concept_no_block_links" not in user_prompt


# 1e. concept_no_block_links → BLOCK_LINKS не передаются даже если есть.
def test_build_user_prompt_concept_no_block_links_omits_block_links_section():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    links = [{"left_block_id": "L1", "right_block_id": "R1", "score": 1.0}]
    user_prompt = ec.build_user_prompt(
        "L", "R", block_links=links, analysis_mode="concept_no_block_links",
    )
    assert "<BLOCK_LINKS>" not in user_prompt
    assert "Связи блоков не используются" in user_prompt


# 1f. build_block_links_context strips heavy raw fields.
def test_build_block_links_context_drops_heavy_fields():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    links = [
        {
            "left_block_id": "L1", "right_block_id": "R1",
            "method": "manual", "score": 0.9,
            "left_page": 1, "right_page": 1,
            "crop_base64": "AAAA" * 5000,  # heavy → должно отрезаться
            "raw_blob": "x" * 10000,
            "label": "Цепь освещения",
        },
    ]
    ctx = ec.build_block_links_context(links)
    assert "<BLOCK_LINKS>" in ctx
    assert "L1" in ctx and "R1" in ctx
    assert "Цепь освещения" in ctx
    # heavy поля выкинуты
    assert "crop_base64" not in ctx
    assert "raw_blob" not in ctx
    assert "AAAA" not in ctx


# 2. Opus provider disabled → status="disabled".
def test_enriched_comparison_disabled_returns_disabled_status(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.delenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", raising=False)
    _make_pair("sess_disabled", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched("sess_disabled", "p1", "left",  "enriched LEFT")
    _write_enriched("sess_disabled", "p1", "right", "enriched RIGHT")

    result = ec.run_enriched_comparison("sess_disabled", "p1")
    assert result["status"] == "disabled"
    assert result["changes"] == []
    # Файл должен быть записан
    from backend.app.services.stage_comparison import paths as paths_mod
    assert paths_mod.enriched_comparison_result_path("sess_disabled", "p1").exists()


# 3. provider_not_available → prompt сохраняется.
def test_enriched_comparison_provider_unavailable_saves_prompt(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_pna", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched("sess_pna", "p1", "left",  "LEFT_ENRICHED")
    _write_enriched("sess_pna", "p1", "right", "RIGHT_ENRICHED")

    result = ec.run_enriched_comparison(
        "sess_pna", "p1",
        provider=_UnavailableProvider(),
    )
    assert result["status"] == "provider_not_available"
    from backend.app.services.stage_comparison import paths as paths_mod
    prompt_file = paths_mod.enriched_comparison_prompt_path("sess_pna", "p1")
    assert prompt_file.exists()
    text = prompt_file.read_text(encoding="utf-8")
    assert "## System" in text and "## User" in text
    assert "LEFT_ENRICHED" in text and "RIGHT_ENRICHED" in text


# 4. valid Opus JSON → comparison_result.json со status="done".
def test_enriched_comparison_valid_json_done(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_done", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched("sess_done", "p1", "left",  "LEFT")
    _write_enriched("sess_done", "p1", "right", "RIGHT")

    payload = {
        "status": "done",
        "summary": "Изменена марка кабеля",
        "changes": [
            {
                "id": "chg_001",
                "source": "text",
                "type": "material_changed",
                "category": "electrical",
                "severity": "high",
                "title": "Изменён кабель",
                "summary": "ВВГнг(А)-FRLS 5x10 → ВВГнг(А)-FRLS 5x16",
                "old_value": "5x10",
                "new_value": "5x16",
                "construction_impact": "Перерасчёт нагрузок",
                "cost_impact": "likely",
                "requires_human_review": True,
                "confidence": 0.92,
                "evidence_left": {"quote": "5x10", "section": "Кабели", "approx_location": "стр. 4"},
                "evidence_right": {"quote": "5x16", "section": "Кабели", "approx_location": "стр. 4"},
            }
        ],
        "warnings": [],
    }
    provider = _AvailableProvider(raw_response=json.dumps(payload), status="done")
    result = ec.run_enriched_comparison("sess_done", "p1", provider=provider)
    assert result["status"] == "done"
    assert len(result["changes"]) == 1
    assert result["changes"][0]["source"] == "text"
    assert result["changes"][0]["type"] == "material_changed"
    assert result["changes"][0]["severity"] == "high"
    assert result["summary"].startswith("Изменена марка")

    # Проверяем что файл реально записался
    from backend.app.services.stage_comparison import paths as paths_mod
    f = paths_mod.enriched_comparison_result_path("sess_done", "p1")
    assert f.exists()
    saved = json.loads(f.read_text(encoding="utf-8"))
    assert saved["status"] == "done"
    assert len(saved["changes"]) == 1


# 5. invalid JSON → status="invalid_json".
def test_enriched_comparison_invalid_json(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_inv", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched("sess_inv", "p1", "left",  "LEFT")
    _write_enriched("sess_inv", "p1", "right", "RIGHT")

    provider = _AvailableProvider(
        raw_response="это не JSON а просто markdown",
        status="done",
    )
    result = ec.run_enriched_comparison("sess_inv", "p1", provider=provider)
    assert result["status"] == "invalid_json"
    assert result["changes"] == []
    assert result["error"]


def test_enriched_comparison_not_ready_without_enriched_md(tmp_path, monkeypatch):
    """Бонус: если enriched MD одной из сторон нет → status='not_ready'."""
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_nr", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    # Только одна сторона enriched
    _write_enriched("sess_nr", "p1", "left", "LEFT")

    provider = _AvailableProvider(raw_response="{}", status="done")
    result = ec.run_enriched_comparison("sess_nr", "p1", provider=provider)
    assert result["status"] == "not_ready"
    assert not provider.invoke_calls


def test_enriched_comparison_too_large(tmp_path, monkeypatch):
    """Если total enriched > max_chars → too_large; LLM не вызывается."""
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS", "100")
    _make_pair("sess_big", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched("sess_big", "p1", "left",  "L" * 1000)
    _write_enriched("sess_big", "p1", "right", "R" * 1000)

    provider = _AvailableProvider(raw_response="{}", status="done")
    result = ec.run_enriched_comparison("sess_big", "p1", provider=provider)
    assert result["status"] == "too_large"
    assert not provider.invoke_calls


# ─── r6 self-check на основном пути ────────────────────────────────────────


def _selfcheck_payload() -> dict:
    """Один real change (число есть в MD) + один phantom (ничего не grounded)."""
    return {
        "status": "done",
        "summary": "Сверка номиналов",
        "changes": [
            {
                "id": "chg_real", "source": "text", "type": "changed",
                "title": "Номинал вводного автомата", "old_value": "160А",
                "new_value": "250А",
                "evidence_left": {"quote": "ном."},
                "evidence_right": {"quote": "ном."},
            },
            {
                "id": "chg_phantom", "source": "text", "type": "added",
                "title": "Фантомный щит", "old_value": "",
                "new_value": "ЩО-7 на 999А",
                "evidence_left": {"quote": ""},
                "evidence_right": {"quote": "фантомный щит ЩО-7 999А"},
            },
        ],
        "warnings": [],
    }


def _make_selfcheck_pair(session_id: str, tmp_path):
    _make_pair(session_id, "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched(session_id, "p1", "left", "Вводной автомат 160А для ВРУ-1.")
    _write_enriched(session_id, "p1", "right", "Вводной автомат 250А для ВРУ-1.")


def test_enriched_comparison_selfcheck_mark_mode(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    monkeypatch.setenv("STAGE_COMPARISON_SELFCHECK_ENABLED", "true")
    monkeypatch.delenv("STAGE_COMPARISON_SELFCHECK_DROP_UNGROUNDED", raising=False)
    _make_selfcheck_pair("sess_sc_mark", tmp_path)

    provider = _AvailableProvider(raw_response=json.dumps(_selfcheck_payload()), status="done")
    res = ec.run_enriched_comparison("sess_sc_mark", "p1", provider=provider)

    assert res["status"] == "done"
    assert res["selfcheck"]["mode"] == "mark"
    assert res["selfcheck"]["rescued_by_number"] == 1
    assert res["selfcheck"]["ungrounded"] == 1
    assert res["selfcheck"]["marked_review"] == 1
    # в мягком режиме ничего не удалено
    assert len(res["changes"]) == 2
    by_id = {c["id"]: c for c in res["changes"]}
    assert by_id["chg_real"]["evidence_verified"] is True
    assert by_id["chg_real"]["evidence_verified_by"] == "number"
    assert by_id["chg_phantom"]["requires_human_review"] is True
    assert by_id["chg_phantom"].get("evidence_verified") is False
    assert "selfcheck_note" in by_id["chg_phantom"]


def test_enriched_comparison_selfcheck_drop_mode(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    monkeypatch.setenv("STAGE_COMPARISON_SELFCHECK_ENABLED", "true")
    monkeypatch.setenv("STAGE_COMPARISON_SELFCHECK_DROP_UNGROUNDED", "true")
    _make_selfcheck_pair("sess_sc_drop", tmp_path)

    provider = _AvailableProvider(raw_response=json.dumps(_selfcheck_payload()), status="done")
    res = ec.run_enriched_comparison("sess_sc_drop", "p1", provider=provider)

    assert res["status"] == "done"
    assert res["selfcheck"]["mode"] == "drop"
    assert res["selfcheck"]["dropped"] == 1
    # phantom удалён, real остался
    assert len(res["changes"]) == 1
    assert res["changes"][0]["id"] == "chg_real"


def test_enriched_comparison_selfcheck_disabled_by_default(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    monkeypatch.delenv("STAGE_COMPARISON_SELFCHECK_ENABLED", raising=False)
    _make_selfcheck_pair("sess_sc_off", tmp_path)

    provider = _AvailableProvider(raw_response=json.dumps(_selfcheck_payload()), status="done")
    res = ec.run_enriched_comparison("sess_sc_off", "p1", provider=provider)

    assert res["status"] == "done"
    # self-check выключен → поле None, ничего не помечено/не удалено
    assert res["selfcheck"] is None
    assert len(res["changes"]) == 2
    by_id = {c["id"]: c for c in res["changes"]}
    assert "selfcheck_note" not in by_id["chg_phantom"]
    # requires_human_review остаётся как пришло от модели (False по умолчанию)
    assert by_id["chg_phantom"]["requires_human_review"] is False


# ─── r5: контракт Opus (present_one_side / disputed) ───────────────────────


def test_normalize_change_present_one_side_forces_review():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    out = ec._normalize_change({
        "type": "present_one_side", "title": "Щит виден только справа",
        "new_value": "ЩО-7", "old_value": "не описано (возможно, не распознано)",
        "requires_human_review": False,
    })
    assert out is not None
    assert out["type"] == "present_one_side"
    # present_one_side по определению неоднозначно → принудительно на ручную проверку
    assert out["requires_human_review"] is True
    assert out["disputed"] is False


def test_normalize_change_disputed_passthrough_and_forces_review():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    out = ec._normalize_change({
        "type": "changed", "title": "Сомнительная дельта",
        "old_value": "A", "new_value": "B",
        "disputed": True, "requires_human_review": False,
    })
    assert out["disputed"] is True
    assert out["requires_human_review"] is True


def test_normalize_change_disputed_defaults_false_and_keeps_review():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    out = ec._normalize_change({
        "type": "changed", "title": "Обычная дельта",
        "old_value": "A", "new_value": "B",
    })
    assert out["disputed"] is False
    assert out["requires_human_review"] is False


# ─── r4: словарь синонимов + выравнивание по потребителю ───────────────────


def test_consumer_synonyms_shipped_file_loads():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    groups = ec.load_consumer_synonyms()
    assert isinstance(groups, list) and groups
    flat = [name for g in groups for name in g]
    assert "ШУ-ХЦ" in flat and "ВРУ-ХЦ" in flat


def test_consumer_synonyms_context_format_and_empty():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    ctx = ec.build_consumer_synonyms_context([["ШУ-ХЦ", "ВРУ-ХЦ"], ["ЩО", "щит освещения"]])
    assert "<CONSUMER_SYNONYMS>" in ctx and "</CONSUMER_SYNONYMS>" in ctx
    assert "ШУ-ХЦ = ВРУ-ХЦ" in ctx
    # пустой список групп → тег не добавляется
    assert ec.build_consumer_synonyms_context([]) == ""


def test_system_prompt_has_consumer_alignment_rule():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    sp, _ = ec.build_prompts("LEFT", "RIGHT")
    assert "ВЫРАВНИВАНИЕ ОТХОДЯЩИХ ЛИНИЙ" in sp
    assert "по ИМЕНИ потребителя" in sp
    assert "CONSUMER_SYNONYMS" in sp
    # явный запрет позиционного/по-аппарату сопоставления
    assert "1QF8" in sp or "QF8" in sp


def test_user_prompt_injects_synonyms_tag():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    up = ec.build_user_prompt("LEFT", "RIGHT")
    assert "<CONSUMER_SYNONYMS>" in up


def test_consumer_synonyms_env_override(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec
    f = tmp_path / "syn.json"
    f.write_text('{"groups": [["AAA", "BBB", "ccc"]]}', encoding="utf-8")
    monkeypatch.setenv("STAGE_COMPARISON_CONSUMER_SYNONYMS_FILE", str(f))
    groups = ec.load_consumer_synonyms()
    assert ["AAA", "BBB", "ccc"] in groups


def test_consumer_synonyms_missing_file_fail_soft(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec
    monkeypatch.setenv("STAGE_COMPARISON_CONSUMER_SYNONYMS_FILE",
                       str(tmp_path / "nope.json"))
    assert ec.load_consumer_synonyms() == []
    # без синонимов тег в user-prompt не появляется
    assert "<CONSUMER_SYNONYMS>" not in ec.build_user_prompt("L", "R")


# 6. unified preflight считает image_blocks / enriched_ready / compare_ready.
def test_unified_preflight_counts_blocks_and_status(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import unified_analysis as ua

    _make_pair("sess_pf", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))

    pre = ua.preflight_pair("sess_pf", "p1")
    assert pre.pair_id == "p1"
    assert pre.has_md is True
    # enrichment_ready False (нет enriched MD)
    assert pre.enrichment_ready is False
    # will_run_enrichment True
    assert pre.will_run_enrichment is True
    # No image blocks → cache_hits=0
    assert pre.image_blocks_left == 0
    assert pre.image_blocks_right == 0
    # comparison_ready False
    assert pre.comparison_ready is False
    d = pre.as_dict()
    assert "enrichment" in d and "comparison" in d
    assert "qwen_calls_estimated" in d["enrichment"]


def test_unified_preflight_when_enriched_ready(tmp_path, monkeypatch):
    """Если enriched MD уже есть, preflight отражает enrichment_ready=True."""
    from backend.app.services.stage_comparison import unified_analysis as ua

    _make_pair("sess_pfr", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched("sess_pfr", "p1", "left",  "LEFT")
    _write_enriched("sess_pfr", "p1", "right", "RIGHT")

    pre = ua.preflight_pair("sess_pfr", "p1")
    assert pre.enrichment_ready is True
    assert pre.will_run_enrichment is False


# 7. unified run_pair вызывает enrichment, затем compare.
@pytest.mark.asyncio
async def test_unified_run_pair_runs_enrichment_then_compare(tmp_path, monkeypatch):
    """run_pair: нет enriched MD → enrichment → ставим enriched MD руками →
    compare запускается через mock-провайдер.
    """
    from backend.app.services.stage_comparison import unified_analysis as ua
    from backend.app.services.stage_comparison import md_image_enrichment as md_mod
    from backend.app.services.stage_comparison import enriched_comparison as ec
    from backend.app.services.stage_comparison.graphic_llm_local import LocalGraphicLLMConfig

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_run", "p1",
               left_md=_write_md(tmp_path / "L.md", "left text"),
               right_md=_write_md(tmp_path / "R.md", "right text"))

    enrichment_calls = []

    async def fake_enrich_side(session_id, pair_id, side, **kw):
        enrichment_calls.append((session_id, pair_id, side, kw.get("run_model"), kw.get("force")))
        # имитируем успешный enrichment без image-блоков
        _write_enriched(session_id, pair_id, side, f"{side.upper()}_ENRICHED")
        from backend.app.services.stage_comparison.md_image_enrichment import EnrichSideSummary
        return EnrichSideSummary(
            side=side, status="done",
            md_path=str(kw.get("md_path") or ""),
            md_exists=True,
            enriched_md_path=str(md_mod.paths_mod.text_enrichment_md_path(
                session_id, pair_id, side
            )),
            image_blocks=0, described=0, errors=0, pending=0,
        )

    payload = {
        "status": "done",
        "summary": "no changes",
        "changes": [{
            "id": "x", "source": "text", "type": "added",
            "category": "general", "severity": "low",
            "title": "demo", "summary": "demo",
            "old_value": "", "new_value": "",
            "construction_impact": "", "cost_impact": "none",
            "requires_human_review": False, "confidence": 0.5,
            "evidence_left": {}, "evidence_right": {},
        }],
        "warnings": [],
    }

    fake_provider = _AvailableProvider(raw_response=json.dumps(payload), status="done")

    # 1) Patch md_mod.enrich_side
    monkeypatch.setattr(md_mod, "enrich_side", fake_enrich_side)
    # 2) Patch enriched_comparison provider lookup → возвращаем наш mock.
    monkeypatch.setattr(
        ec, "_REGISTRY", {"claude_code": lambda: fake_provider},
    )
    # _REGISTRY у нас dict[str, type]; в run_enriched_comparison делается cls() —
    # lambda без аргументов вернёт provider. Это безопасно.

    res = await ua.run_pair("sess_run", "p1", force_enrichment=False, force_compare=False)
    assert res.status == "done", f"status={res.status} error={res.error}"
    assert res.enrichment_status == "done"
    assert res.comparison_status == "done"
    assert res.changes_count == 1
    # Enrichment был вызван для обеих сторон
    sides_called = {c[2] for c in enrichment_calls}
    assert sides_called == {"left", "right"}
    # И compare-провайдер тоже был вызван
    assert len(fake_provider.invoke_calls) == 1


# 8. Если enriched MD есть — enrichment skipped.
@pytest.mark.asyncio
async def test_unified_run_pair_skips_enrichment_if_ready(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import unified_analysis as ua
    from backend.app.services.stage_comparison import md_image_enrichment as md_mod
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_skip", "p1",
               left_md=_write_md(tmp_path / "L.md", "left text"),
               right_md=_write_md(tmp_path / "R.md", "right text"))
    _write_enriched("sess_skip", "p1", "left",  "LEFT_ENRICHED")
    _write_enriched("sess_skip", "p1", "right", "RIGHT_ENRICHED")

    enrichment_calls = []

    async def fake_enrich_side(*args, **kw):
        enrichment_calls.append((args, kw))
        raise AssertionError("enrichment should not be called when enriched MD exists")

    payload = {"status": "done", "summary": "", "changes": [], "warnings": []}
    fake_provider = _AvailableProvider(raw_response=json.dumps(payload), status="done")

    monkeypatch.setattr(md_mod, "enrich_side", fake_enrich_side)
    monkeypatch.setattr(ec, "_REGISTRY", {"claude_code": lambda: fake_provider})

    res = await ua.run_pair("sess_skip", "p1",
                            force_enrichment=False, force_compare=False)
    assert res.enrichment_status == "skipped"
    assert res.status == "done"
    assert not enrichment_calls


# 9. force_enrichment=True перезапускает enrichment.
@pytest.mark.asyncio
async def test_unified_run_pair_force_enrichment(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import unified_analysis as ua
    from backend.app.services.stage_comparison import md_image_enrichment as md_mod
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_fe", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched("sess_fe", "p1", "left",  "LEFT")
    _write_enriched("sess_fe", "p1", "right", "RIGHT")

    enrich_count = []

    async def fake_enrich_side(session_id, pair_id, side, **kw):
        enrich_count.append(side)
        from backend.app.services.stage_comparison.md_image_enrichment import EnrichSideSummary
        return EnrichSideSummary(
            side=side, status="done",
            md_path=str(kw.get("md_path") or ""), md_exists=True,
            enriched_md_path=str(md_mod.paths_mod.text_enrichment_md_path(
                session_id, pair_id, side)),
            image_blocks=0, described=0,
        )

    payload = {"status": "done", "summary": "", "changes": [], "warnings": []}
    fake_provider = _AvailableProvider(raw_response=json.dumps(payload), status="done")
    monkeypatch.setattr(md_mod, "enrich_side", fake_enrich_side)
    monkeypatch.setattr(ec, "_REGISTRY", {"claude_code": lambda: fake_provider})

    res = await ua.run_pair("sess_fe", "p1", force_enrichment=True, force_compare=False)
    assert res.enrichment_status == "done"
    assert sorted(enrich_count) == ["left", "right"]


# 10. force_compare=True перезапускает comparison.
@pytest.mark.asyncio
async def test_unified_run_pair_force_compare(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import unified_analysis as ua
    from backend.app.services.stage_comparison import md_image_enrichment as md_mod
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_fc", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    _write_enriched("sess_fc", "p1", "left",  "LEFT")
    _write_enriched("sess_fc", "p1", "right", "RIGHT")

    # Кладём готовый comparison_result.json со status=done
    from backend.app.services.stage_comparison import paths as paths_mod
    f = paths_mod.enriched_comparison_result_path("sess_fc", "p1")
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps({
        "status": "done", "summary": "old", "changes": [],
        "warnings": [], "input_stats": {},
    }), encoding="utf-8")

    payload = {"status": "done", "summary": "NEW", "changes": [], "warnings": []}
    fake_provider = _AvailableProvider(raw_response=json.dumps(payload), status="done")

    async def fake_enrich_side(*a, **kw):
        raise AssertionError("enrichment should be skipped")
    monkeypatch.setattr(md_mod, "enrich_side", fake_enrich_side)
    monkeypatch.setattr(ec, "_REGISTRY", {"claude_code": lambda: fake_provider})

    res = await ua.run_pair("sess_fc", "p1",
                            force_enrichment=False, force_compare=True)
    assert res.comparison_status == "done"
    # И провайдер был вызван (значит force_compare сработал)
    assert len(fake_provider.invoke_calls) == 1
    # А обновлённый файл содержит summary="NEW"
    new = json.loads(f.read_text(encoding="utf-8"))
    assert new["summary"] == "NEW"


# 11. unified job по session проходит несколько pair items.
@pytest.mark.asyncio
async def test_unified_job_session_multiple_pairs(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    from backend.app.services.stage_comparison import unified_analysis as ua
    from backend.app.services.stage_comparison import md_image_enrichment as md_mod
    from backend.app.services.stage_comparison import enriched_comparison as ec
    from backend.app.services.stage_comparison import paths as paths_mod

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    # Две пары в одной сессии
    session_id = "sess_multi"
    paths_mod.session_json_path(session_id).write_text(
        json.dumps({
            "id": session_id,
            "pair_order": ["p1", "p2"],
        }), encoding="utf-8",
    )
    for pid in ("p1", "p2"):
        l = _write_md(tmp_path / f"{pid}-L.md", "L")
        r = _write_md(tmp_path / f"{pid}-R.md", "R")
        pair = {
            "id": pid,
            "status": "matched",
            "left":  {"filename": f"{pid}-l.pdf", "pdf_path": "/x.pdf", "md_path": str(l)},
            "right": {"filename": f"{pid}-r.pdf", "pdf_path": "/x.pdf", "md_path": str(r)},
        }
        paths_mod.pair_json_path(session_id, pid).write_text(
            json.dumps(pair), encoding="utf-8",
        )
        _write_enriched(session_id, pid, "left", f"{pid}_L")
        _write_enriched(session_id, pid, "right", f"{pid}_R")

    payload = {"status": "done", "summary": "", "changes": [], "warnings": []}
    fake_provider = _AvailableProvider(raw_response=json.dumps(payload), status="done")
    monkeypatch.setattr(ec, "_REGISTRY", {"claude_code": lambda: fake_provider})

    async def no_enrich(*a, **kw):
        raise AssertionError("enrichment should not run (skipped)")
    monkeypatch.setattr(md_mod, "enrich_side", no_enrich)

    job = jobs.create_unified_job(session_id, scope="session", confirm=True)
    assert job["status"] == "queued"
    assert len(job["items"]) == 2
    finished = await jobs.run_unified_job(session_id, job["id"])
    assert finished["status"] == "done"
    assert finished["progress"]["done"] == 2
    # Провайдер вызван 2 раза (по одному на пару)
    assert len(fake_provider.invoke_calls) == 2


# 12. cancel job работает.
def test_unified_job_cancel(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs

    _make_pair("sess_cancel", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    job = jobs.create_unified_job("sess_cancel", scope="pair",
                                  pair_id="p1", confirm=True)
    assert job["status"] == "queued"
    cancelled = jobs.cancel_job("sess_cancel", job["id"])
    assert cancelled["status"] == "cancelled"
    # Все queued items → cancelled
    for it in cancelled["items"]:
        assert it["status"] == "cancelled"


# 13. unified-diff-flat агрегирует changes.
def test_unified_diff_flat_aggregates_changes(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import unified_findings as uf
    from backend.app.services.stage_comparison import paths as paths_mod

    session_id = "sess_flat"
    paths_mod.session_json_path(session_id).write_text(
        json.dumps({
            "id": session_id,
            "pair_order": ["p1", "p2"],
        }), encoding="utf-8",
    )
    for pid in ("p1", "p2"):
        pair = {
            "id": pid,
            "status": "matched",
            "left":  {"filename": f"{pid}-l.pdf", "md_path": "/x.md"},
            "right": {"filename": f"{pid}-r.pdf", "md_path": "/y.md"},
        }
        paths_mod.pair_json_path(session_id, pid).write_text(
            json.dumps(pair), encoding="utf-8",
        )
        comp = {
            "status": "done",
            "summary": f"summary for {pid}",
            "changes": [
                {"id": f"{pid}_c1", "source": "text", "type": "changed",
                 "category": "general", "severity": "high",
                 "title": f"{pid} t1", "summary": "...",
                 "old_value": "", "new_value": "",
                 "construction_impact": "", "cost_impact": "unknown",
                 "requires_human_review": True, "confidence": 0.8,
                 "evidence_left": {}, "evidence_right": {}},
                {"id": f"{pid}_c2", "source": "scheme_analysis", "type": "scheme_sequence_changed",
                 "category": "electrical", "severity": "medium",
                 "title": f"{pid} t2", "summary": "...",
                 "old_value": "", "new_value": "",
                 "construction_impact": "", "cost_impact": "possible",
                 "requires_human_review": False, "confidence": 0.7,
                 "evidence_left": {}, "evidence_right": {}},
            ],
            "warnings": [],
            "input_stats": {},
        }
        f = paths_mod.enriched_comparison_result_path(session_id, pid)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(comp), encoding="utf-8")

    flat = uf.build_unified_flat(session_id)
    assert flat["session_id"] == session_id
    assert flat["summary"]["total_pairs"] == 2
    assert flat["summary"]["done_pairs"] == 2
    assert flat["summary"]["total_changes"] == 4
    assert flat["summary"]["by_source"]["text"] == 2
    assert flat["summary"]["by_source"]["scheme_analysis"] == 2
    assert flat["summary"]["by_severity"]["high"] == 2
    assert flat["summary"]["by_severity"]["medium"] == 2
    assert flat["summary"]["requires_human_review"] == 2
    assert len(flat["items"]) == 4
    # каждый item имеет source_layer и pair_label
    for it in flat["items"]:
        assert it["source_layer"] in ("text", "scheme_analysis")
        assert it["pair_label"]

    # ── Тот же session, но с pair_id-фильтром ───────────────────────────
    # Когда pair_id передан, items/summary должны быть только по этой паре.
    flat_p1 = uf.build_unified_flat(session_id, pair_id="p1")
    assert flat_p1["summary"]["total_pairs"] == 1
    assert flat_p1["summary"]["done_pairs"] == 1
    assert flat_p1["summary"]["total_changes"] == 2
    assert len(flat_p1["items"]) == 2
    assert all(it["pair_id"] == "p1" for it in flat_p1["items"])
    # pair_modes тоже отфильтрован
    assert [pm["pair_id"] for pm in flat_p1["pair_modes"]] == ["p1"]

    flat_p2 = uf.build_unified_flat(session_id, pair_id="p2")
    assert len(flat_p2["items"]) == 2
    assert all(it["pair_id"] == "p2" for it in flat_p2["items"])

    # Неизвестный pair_id → пустой flat
    flat_unknown = uf.build_unified_flat(session_id, pair_id="ghost")
    assert flat_unknown["summary"]["total_pairs"] == 0
    assert flat_unknown["summary"]["total_changes"] == 0
    assert flat_unknown["items"] == []
    assert flat_unknown["pair_modes"] == []


def test_unified_diff_flat_endpoint_supports_pair_id_query(tmp_path, monkeypatch):
    """HTTP-endpoint /unified-diff-flat?pair_id=X прокидывает фильтр."""
    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    from backend.app.services.stage_comparison import paths as paths_mod
    from backend.app.api.routers import stage_comparison as router_mod

    session_id = "sess_flat_q"
    paths_mod.session_json_path(session_id).write_text(
        json.dumps({"id": session_id, "pair_order": ["a", "b"]}),
        encoding="utf-8",
    )
    for pid in ("a", "b"):
        pair = {
            "id": pid, "status": "matched",
            "left":  {"filename": f"{pid}-l.pdf", "md_path": "/x.md"},
            "right": {"filename": f"{pid}-r.pdf", "md_path": "/y.md"},
        }
        paths_mod.pair_json_path(session_id, pid).write_text(json.dumps(pair), encoding="utf-8")
        comp = {
            "status": "done",
            "changes": [{
                "id": f"{pid}_c", "source": "text", "type": "changed",
                "category": "general", "severity": "low",
                "title": f"{pid} title", "summary": "...",
                "old_value": "", "new_value": "",
                "construction_impact": "", "cost_impact": "unknown",
                "requires_human_review": False, "confidence": 0.5,
                "evidence_left": {}, "evidence_right": {},
            }],
            "warnings": [], "input_stats": {},
        }
        f = paths_mod.enriched_comparison_result_path(session_id, pid)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(comp), encoding="utf-8")

    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)

    # Без query — обе пары
    r = client.get(f"/api/stage-comparison/sessions/{session_id}/unified-diff-flat")
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["total_pairs"] == 2
    assert body["summary"]["total_changes"] == 2
    assert {it["pair_id"] for it in body["items"]} == {"a", "b"}

    # С pair_id=a — только а
    r = client.get(f"/api/stage-comparison/sessions/{session_id}/unified-diff-flat?pair_id=a")
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["total_pairs"] == 1
    assert body["summary"]["total_changes"] == 1
    assert [it["pair_id"] for it in body["items"]] == ["a"]

    # С неизвестным pair_id — пусто
    r = client.get(f"/api/stage-comparison/sessions/{session_id}/unified-diff-flat?pair_id=ghost")
    assert r.status_code == 200
    body = r.json()
    assert body["summary"]["total_pairs"] == 0
    assert body["items"] == []


def test_frontend_ui_diffs_tab_loads_current_pair_only_by_default():
    """frontend/static/js/app.js должен при load unified flat по умолчанию
    передавать pair_id активной пары, чтобы вкладка «Расхождения» не
    показывала stale findings другой пары."""
    p = Path(__file__).resolve().parent.parent / "frontend" / "static" / "js" / "app.js"
    text = p.read_text(encoding="utf-8")
    # Признак новой логики:
    assert "scUnifiedShowAllPairs" in text, "Toggle «текущая пара» / «вся сессия» должен присутствовать"
    assert "?pair_id=${encodeURIComponent(scopePid)}" in text, (
        "scLoadUnifiedFlat должен передавать pair_id активной пары в query"
    )
    assert "scUnifiedToggleShowAllPairs" in text, "Кнопка-toggle должна быть"
    # Watcher на смену активной пары для перезагрузки flat
    assert "watch(() => scActivePair.value && scActivePair.value.id" in text or \
           "scActivePair.value && scActivePair.value.id" in text, (
        "Должен быть watcher активной пары для перезагрузки unified flat"
    )


# 14. UI subtab «Расхождения» по умолчанию = unified.
def test_ui_default_subtab_is_unified():
    """frontend/static/js/app.js: scDiffSubtab default == 'unified'.

    Это означает что primary UX = unified, а text/graphic = debug-вкладки.
    """
    p = Path(__file__).resolve().parent.parent / "frontend" / "static" / "js" / "app.js"
    assert p.exists()
    text = p.read_text(encoding="utf-8")
    # Точный default
    assert "ref('unified')" in text, "scDiffSubtab default must be 'unified'"
    # primary button «Расхождения» должна быть в index.html
    html_p = Path(__file__).resolve().parent.parent / "frontend" / "index.html"
    html = html_p.read_text(encoding="utf-8")
    assert ">Расхождения<" in html
    # «Проанализировать и сравнить» button присутствует
    assert "Проанализировать и сравнить" in html


# 15. Old endpoints не сломаны.
def test_old_endpoints_still_registered():
    """Старые маршруты graphic-diff / text-llm / md-enrichment остаются."""
    from backend.app.api.routers import stage_comparison as router_mod
    routes = {r.path for r in router_mod.router.routes}
    # Не удалили старые маршруты
    must_exist = [
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/text-diff",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/text-llm-diff",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/graphic-diff",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/md-enrichment",
        "/api/stage-comparison/sessions/{session_id}/text-llm-diff-flat",
        "/api/stage-comparison/sessions/{session_id}/text-llm-diff-jobs",
    ]
    for r in must_exist:
        assert r in routes, f"missing legacy route: {r}"
    # Новые маршруты тоже зарегистрированы
    new_must_exist = [
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/unified-analysis",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/unified-analysis/preflight",
        "/api/stage-comparison/sessions/{session_id}/unified-analysis-jobs",
        "/api/stage-comparison/sessions/{session_id}/unified-analysis-jobs/{job_id}",
        "/api/stage-comparison/sessions/{session_id}/unified-analysis-jobs/{job_id}/cancel",
        "/api/stage-comparison/sessions/{session_id}/unified-diff-flat",
        "/api/stage-comparison/enriched-compare-config",
    ]
    for r in new_must_exist:
        assert r in routes, f"missing new route: {r}"


# 16. No live Qwen/Opus calls in tests.
def test_no_live_calls_were_made(monkeypatch):
    """Защита: при отключённом env и без provider-моков live-вызов
    Claude Code subprocess не происходит.

    Проверяем, что без enable env-флага run_enriched_comparison сразу
    возвращает disabled и НЕ пытается импортировать subprocess.
    """
    from backend.app.services.stage_comparison import enriched_comparison as ec

    cfg = ec.load_config()
    assert cfg.enabled is False
    # disabled → даже provider не строится
    prov, _ = ec.resolve_provider(cfg)
    assert prov is None


def test_unified_jobs_rejects_without_confirm(tmp_path, monkeypatch):
    """Без confirm=true job создаётся в статусе rejected_no_confirm и НЕ запускается."""
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs

    _make_pair("sess_noconf", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    job = jobs.create_unified_job("sess_noconf", scope="pair",
                                  pair_id="p1", confirm=False)
    assert job["status"] == "rejected_no_confirm"


def test_unified_pair_run_without_confirm_returns_preflight(tmp_path, monkeypatch):
    """POST /unified-analysis без confirm возвращает preflight, не запускает run."""
    from fastapi.testclient import TestClient
    from backend.app.main import app
    _make_pair("sess_uipair", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    client = TestClient(app)
    r = client.post(
        "/api/stage-comparison/sessions/sess_uipair/pairs/p1/unified-analysis",
        json={"confirm": False, "force_enrichment": False, "force_compare": False},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is False
    assert data["status"] == "rejected_no_confirm"
    assert "preflight" in data


# Дополнительно — что enriched_comparison умеет dry-run statuses без mocking
# провайдера: timeout/error отрабатываются через ProviderResult.

def test_enriched_comparison_timeout_status(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    _make_pair("sess_to", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    _write_enriched("sess_to", "p1", "left",  "LEFT")
    _write_enriched("sess_to", "p1", "right", "RIGHT")

    class _TimeoutProvider:
        name = "mock_timeout"
        def check_availability(self): return True, None
        def invoke(self, **kw):
            from backend.app.services.stage_comparison.text_llm_provider import ProviderResult
            return ProviderResult(
                status="timeout", error="timed_out_after_60s",
                duration_sec=60.0, provider=self.name, model=kw.get("model"),
            )

    res = ec.run_enriched_comparison("sess_to", "p1", provider=_TimeoutProvider())
    assert res["status"] == "timeout"
    assert "timed_out" in str(res.get("error") or "")


# ════════════════════════════════════════════════════════════════════════
# «Блоки без связей» — analysis_mode (Tasks 1–11 из ТЗ)
# ════════════════════════════════════════════════════════════════════════


# 1. Default analysis_mode = block_links.
def test_default_analysis_mode_is_block_links(tmp_path):
    from backend.app.services.stage_comparison import store
    _make_pair("sess_am_def", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    mode = store.get_pair_analysis_mode("sess_am_def", "p1")
    assert mode == "block_links"


# 2. POST /analysis-mode меняет mode на concept_no_block_links.
def test_set_analysis_mode_to_concept_via_helper(tmp_path):
    from backend.app.services.stage_comparison import store
    _make_pair("sess_am_concept", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    meta = store.set_pair_analysis_mode("sess_am_concept", "p1", "concept_no_block_links")
    assert meta["analysis_mode"] == "concept_no_block_links"
    assert store.get_pair_analysis_mode("sess_am_concept", "p1") == "concept_no_block_links"


# 3. POST /analysis-mode умеет вернуть block_links.
def test_set_analysis_mode_back_to_block_links(tmp_path):
    from backend.app.services.stage_comparison import store
    _make_pair("sess_am_back", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    store.set_pair_analysis_mode("sess_am_back", "p1", "concept_no_block_links")
    assert store.get_pair_analysis_mode("sess_am_back", "p1") == "concept_no_block_links"
    store.set_pair_analysis_mode("sess_am_back", "p1", "block_links")
    assert store.get_pair_analysis_mode("sess_am_back", "p1") == "block_links"


# 4. Invalid mode → ValueError из helper / HTTP 422 из endpoint.
def test_set_analysis_mode_invalid_raises(tmp_path):
    from backend.app.services.stage_comparison import store
    _make_pair("sess_am_inv", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    import pytest as _pytest
    with _pytest.raises(ValueError):
        store.set_pair_analysis_mode("sess_am_inv", "p1", "some_bogus_mode")
    # Mode не должен измениться
    assert store.get_pair_analysis_mode("sess_am_inv", "p1") == "block_links"


def test_set_analysis_mode_http_422_on_invalid(tmp_path):
    from fastapi.testclient import TestClient
    from backend.app.main import app
    _make_pair("sess_am_http_inv", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    client = TestClient(app)
    r = client.post(
        "/api/stage-comparison/sessions/sess_am_http_inv/pairs/p1/analysis-mode",
        json={"mode": "totally_invalid"},
    )
    assert r.status_code == 422


def test_set_analysis_mode_http_ok_for_concept(tmp_path):
    from fastapi.testclient import TestClient
    from backend.app.main import app
    _make_pair("sess_am_http_ok", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    client = TestClient(app)
    r = client.post(
        "/api/stage-comparison/sessions/sess_am_http_ok/pairs/p1/analysis-mode",
        json={"mode": "concept_no_block_links"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["analysis_mode"] == "concept_no_block_links"
    # GET тоже должен возвращать новое значение
    r = client.get("/api/stage-comparison/sessions/sess_am_http_ok/pairs/p1/analysis-mode")
    assert r.json()["analysis_mode"] == "concept_no_block_links"


# 5. Pair.json сохраняет mode (passthrough survives re-save).
def test_analysis_mode_survives_pair_resave(tmp_path):
    from backend.app.services.stage_comparison import store
    from backend.app.services.stage_comparison import paths as paths_mod
    import json as _json
    _make_pair("sess_persist", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    store.set_pair_analysis_mode("sess_persist", "p1", "concept_no_block_links")
    raw = _json.loads(paths_mod.pair_json_path("sess_persist", "p1").read_text(encoding="utf-8"))
    assert raw["analysis_mode"] == "concept_no_block_links"
    assert raw.get("analysis_mode_updated_at")


# 6. Unified preflight для concept_no_block_links не требует linked blocks.
def test_unified_preflight_concept_mode_does_not_require_links(tmp_path):
    from backend.app.services.stage_comparison import store
    from backend.app.services.stage_comparison import unified_analysis as ua
    _make_pair("sess_pf_concept", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    store.set_pair_analysis_mode("sess_pf_concept", "p1", "concept_no_block_links")
    pre = ua.preflight_pair("sess_pf_concept", "p1")
    d = pre.as_dict()
    assert d["analysis_mode"] == "concept_no_block_links"
    # Не должен ставить errors про отсутствие linked blocks
    assert not d["errors"]
    # В warnings должна быть подсказка про режим
    assert any("Блоки без связей" in w for w in d["warnings"])
    # can_run должен быть True (есть MD)
    assert d["can_run"] is True


# 7. Unified-diff-flat возвращает analysis_mode.
def test_unified_diff_flat_includes_analysis_mode(tmp_path):
    from backend.app.services.stage_comparison import store, unified_findings as uf
    from backend.app.services.stage_comparison import paths as paths_mod
    session_id = "sess_flat_am"
    paths_mod.session_json_path(session_id).write_text(
        json.dumps({"id": session_id, "pair_order": ["p_concept", "p_blocks"]}),
        encoding="utf-8",
    )
    for pid, mode in (("p_concept", "concept_no_block_links"), ("p_blocks", "block_links")):
        pair = {
            "id": pid, "status": "matched",
            "left":  {"filename": f"{pid}-l.pdf", "md_path": "/x.md"},
            "right": {"filename": f"{pid}-r.pdf", "md_path": "/y.md"},
        }
        paths_mod.pair_json_path(session_id, pid).write_text(
            json.dumps(pair), encoding="utf-8",
        )
        # Через helper, чтобы заодно проверить сохранение
        store.set_pair_analysis_mode(session_id, pid, mode)
        comp = {
            "status": "done", "summary": "", "changes": [{
                "id": f"{pid}_c", "source": "text", "type": "changed",
                "category": "general", "severity": "low",
                "title": "demo", "summary": "demo",
                "old_value": "", "new_value": "",
                "construction_impact": "", "cost_impact": "none",
                "requires_human_review": False, "confidence": 0.5,
                "evidence_left": {}, "evidence_right": {},
            }],
            "warnings": [], "input_stats": {},
        }
        f = paths_mod.enriched_comparison_result_path(session_id, pid)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(comp), encoding="utf-8")

    flat = uf.build_unified_flat(session_id)
    # Каждый item имеет analysis_mode
    assert all("analysis_mode" in it for it in flat["items"])
    by_pair = {it["pair_id"]: it["analysis_mode"] for it in flat["items"]}
    assert by_pair["p_concept"] == "concept_no_block_links"
    assert by_pair["p_blocks"] == "block_links"
    # pair_modes mapping тоже есть
    assert "pair_modes" in flat
    modes_by_pair = {pm["pair_id"]: pm["analysis_mode"] for pm in flat["pair_modes"]}
    assert modes_by_pair["p_concept"] == "concept_no_block_links"
    assert modes_by_pair["p_blocks"] == "block_links"


# 8. UI содержит кнопку «Блоки без связей» перед «Авто-связь по IoU».
def test_ui_has_no_block_links_button_before_auto_link():
    from pathlib import Path
    html = Path(__file__).resolve().parent.parent / "frontend" / "index.html"
    text = html.read_text(encoding="utf-8")
    # Ищем взаимное расположение
    idx_no_links = text.find("Блоки без связей")
    idx_auto_link = text.find("Авто-связь по IoU")
    assert idx_no_links > -1, "Кнопка «Блоки без связей» должна быть в HTML"
    assert idx_auto_link > -1, "Кнопка «Авто-связь по IoU» должна быть в HTML"
    assert idx_no_links < idx_auto_link, (
        "Кнопка «Блоки без связей» должна быть ПЕРЕД «Авто-связь по IoU»"
    )
    # И она вызывает scToggleAnalysisMode
    assert "scToggleAnalysisMode" in text


# 9. UI показывает активный режим concept_no_block_links.
def test_ui_shows_active_concept_mode_banner():
    from pathlib import Path
    html = Path(__file__).resolve().parent.parent / "frontend" / "index.html"
    text = html.read_text(encoding="utf-8")
    # Активное состояние через scAnalysisMode==='concept_no_block_links'
    assert "scAnalysisMode==='concept_no_block_links'" in text
    # Баннер режима
    assert "концептуальное сравнение без связей блоков" in text
    # Badge в unified-таблице (lowercase post-redesign 2026-05-27)
    assert "без связей</span>" in text
    # JS ref зарегистрирован
    js = Path(__file__).resolve().parent.parent / "frontend" / "static" / "js" / "app.js"
    js_text = js.read_text(encoding="utf-8")
    assert "scAnalysisMode" in js_text
    assert "scToggleAnalysisMode" in js_text


# 10. Preflight свежей пары считает image blocks из MD без image_descriptions.json.
def test_preflight_counts_image_blocks_via_md_when_no_cache(tmp_path):
    from backend.app.services.stage_comparison import unified_analysis as ua

    md_with_image = """### СТРАНИЦА 1

### BLOCK [TEXT]: T-1
some text here

### BLOCK [IMAGE]: img-1
[IMAGE]: img-1
description of image

### BLOCK [IMAGE]: img-2
[IMAGE]: img-2
another image
"""
    _make_pair("sess_pf_md", "p1",
               left_md=_write_md(tmp_path / "L.md", md_with_image),
               right_md=_write_md(tmp_path / "R.md", md_with_image))
    pre = ua.preflight_pair("sess_pf_md", "p1")
    d = pre.as_dict()
    # image_descriptions.json не существует → fallback на parse_md_blocks
    assert d["enrichment"]["image_blocks_left"] == 2
    assert d["enrichment"]["image_blocks_right"] == 2
    # image_blocks_source = parsed_md (а не cache)
    assert d["image_blocks_source"] == "parsed_md"


# 11. .env.example содержит рекомендованный default для MAX_TOKENS под v4_compact.
def test_env_example_recommends_max_tokens_default():
    from pathlib import Path
    p = Path(__file__).resolve().parent.parent / ".env.example"
    text = p.read_text(encoding="utf-8")
    # Default может быть 4000 или 5500 (production-cutover 2026-05-26 под v4_compact);
    # главное — не legacy 1800 и не вообще отсутствует.
    assert "STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS=" in text
    assert "STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS=1800" not in text


# 12. Никаких live model calls (защита: дефолтное состояние pipeline disabled).
def test_no_live_model_calls_in_concept_mode(tmp_path, monkeypatch):
    """Concept mode preflight НЕ должен ходить ни в LM Studio, ни в Claude.

    Защита: monkeypatch httpx/asyncio.subprocess чтобы любой случайный live-call
    падал с явной ошибкой.
    """
    import httpx

    def _block_httpx(*a, **kw):
        raise RuntimeError("LIVE httpx call detected — test must not network!")

    monkeypatch.setattr(httpx, "post", _block_httpx, raising=False)
    monkeypatch.setattr(httpx, "get", _block_httpx, raising=False)

    from backend.app.services.stage_comparison import store, unified_analysis as ua
    _make_pair("sess_no_live", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    store.set_pair_analysis_mode("sess_no_live", "p1", "concept_no_block_links")
    # Preflight НЕ должен делать live calls
    pre = ua.preflight_pair("sess_no_live", "p1")
    assert pre.analysis_mode == "concept_no_block_links"
    # Live model calls в этой задаче не запускались — set/get analysis_mode и
    # preflight read-only по дизайну.


# Бонус: invalid analysis_mode в store passthrough → graceful fallback на block_links.
def test_get_analysis_mode_fallback_when_pair_json_has_bogus_mode(tmp_path):
    from backend.app.services.stage_comparison import store
    from backend.app.services.stage_comparison import paths as paths_mod
    import json as _json
    _make_pair("sess_bogus", "p1",
               left_md=_write_md(tmp_path / "L.md", "L"),
               right_md=_write_md(tmp_path / "R.md", "R"))
    # Подменяем mode на мусор, минуя helper
    pp = paths_mod.pair_json_path("sess_bogus", "p1")
    data = _json.loads(pp.read_text(encoding="utf-8"))
    data["analysis_mode"] = "garbage"
    pp.write_text(_json.dumps(data), encoding="utf-8")
    # Helper должен вернуть default
    assert store.get_pair_analysis_mode("sess_bogus", "p1") == "block_links"


# ─── Replacement-format preflight ────────────────────────────────────────


def test_preflight_detects_outdated_enriched_md_format(tmp_path, monkeypatch):
    """Если enriched MD на диске в legacy append_v0 — preflight должен это
    обнаружить и поставить needs_rebuild=True + outdated_format=True."""
    from backend.app.services.stage_comparison import unified_analysis as ua

    _make_pair("sess_outdated", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    # legacy enriched MD (append_v0)
    legacy_md = (
        "### BLOCK [IMAGE]\n<!-- original_imagine_start -->\n"
        "<image>x</image>\n<!-- original_imagine_end -->\n\n"
        "#### QWEN_IMAGE_DESCRIPTION\nstatus: done\n"
    )
    _write_enriched("sess_outdated", "p1", "left",  legacy_md)
    _write_enriched("sess_outdated", "p1", "right", legacy_md)

    pre = ua.preflight_pair("sess_outdated", "p1")
    assert pre.enrichment_ready is True
    assert pre.enriched_md_outdated_format is True
    assert pre.needs_rebuild is True
    # И в warnings — человекочитаемая фраза.
    assert any("устаревшем формате" in w or "пересборка" in w for w in pre.warnings)


def test_preflight_replacement_format_does_not_warn(tmp_path):
    """Свежий replace_image_blocks_v1 MD — preflight не помечает outdated."""
    from backend.app.services.stage_comparison import unified_analysis as ua

    _make_pair("sess_replfmt", "p1",
               left_md=_write_md(tmp_path / "L.md", "left"),
               right_md=_write_md(tmp_path / "R.md", "right"))
    new_md = (
        "<!-- ENRICHED_MD_FORMAT: replace_image_blocks_v1 -->\n\n"
        "### BLOCK [TEXT]\nObservation\n\n"
        "<!-- QWEN_IMAGE_DESCRIPTION_START\n"
        "format_version: replace_image_blocks_v1\nstatus: done\n-->\n"
        "### Графический блок / схема\n\nКраткое описание: ...\n\n"
        "<!-- QWEN_IMAGE_DESCRIPTION_END -->\n"
    )
    _write_enriched("sess_replfmt", "p1", "left", new_md)
    _write_enriched("sess_replfmt", "p1", "right", new_md)

    pre = ua.preflight_pair("sess_replfmt", "p1")
    assert pre.enrichment_ready is True
    assert pre.enriched_md_outdated_format is False
    assert pre.needs_rebuild is False


# ─── Session-level Opus batch (этап «Загрузка документации») ────────────


def _write_replacement_enriched(sid: str, pid: str, side: str, content_body: str) -> Path:
    """Записать enriched MD в свежем replace_image_blocks_v1 формате."""
    text = (
        "<!-- ENRICHED_MD_FORMAT: replace_image_blocks_v1 -->\n\n"
        f"### BLOCK [TEXT]\n{content_body}\n"
    )
    return _write_enriched(sid, pid, side, text)


def test_preflight_session_for_batch_classifies_not_ready_too_large_done(tmp_path, monkeypatch):
    """preflight_session_for_batch правильно классифицирует пары:
    not_ready (нет enriched), too_large (превышен лимит), done (уже посчитано),
    run (можно запустить).
    """
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    from backend.app.services.stage_comparison import paths as paths_mod
    from backend.app.services.stage_comparison import enriched_comparison as ec

    sid = "sess_batch_preflight"
    paths_mod.session_json_path(sid).write_text(json.dumps({
        "id": sid,
        "pair_order": ["p_run", "p_not_ready", "p_too_large", "p_done"],
    }), encoding="utf-8")
    for pid in ("p_run", "p_not_ready", "p_too_large", "p_done"):
        l = _write_md(tmp_path / f"{pid}-L.md", "L")
        r = _write_md(tmp_path / f"{pid}-R.md", "R")
        paths_mod.pair_json_path(sid, pid).write_text(json.dumps({
            "id": pid,
            "status": "matched",
            "left":  {"filename": f"{pid}-l.pdf", "pdf_path": "/x.pdf", "md_path": str(l)},
            "right": {"filename": f"{pid}-r.pdf", "pdf_path": "/x.pdf", "md_path": str(r)},
        }), encoding="utf-8")

    # p_run — enriched есть, не too_large, comparison не done
    _write_replacement_enriched(sid, "p_run", "left",  "small")
    _write_replacement_enriched(sid, "p_run", "right", "small")

    # p_not_ready — без enriched MD

    # p_too_large — enriched есть, но размер > limit
    big = "x" * 200_000
    _write_replacement_enriched(sid, "p_too_large", "left",  big)
    _write_replacement_enriched(sid, "p_too_large", "right", big)
    # Понизим лимит в env, чтобы too_large сработал детерминированно.
    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS", "50000")
    # Перечитать config (load_config читает env каждый раз)

    # p_done — enriched есть + готовый comparison_result.json
    _write_replacement_enriched(sid, "p_done", "left",  "small")
    _write_replacement_enriched(sid, "p_done", "right", "small")
    paths_mod.enriched_comparison_result_path(sid, "p_done").write_text(json.dumps({
        "status": "done",
        "summary": "",
        "changes": [],
    }), encoding="utf-8")

    pre = jobs.preflight_session_for_batch(sid, scope="session", force_compare=False)
    actions = {it["pair_id"]: it["action"] for it in pre["items"]}
    assert actions["p_run"] == "run"
    assert actions["p_not_ready"] == "skip_not_ready"
    assert actions["p_too_large"] == "skip_too_large"
    assert actions["p_done"] == "skip_done"
    assert pre["will_run"] == 1
    assert pre["skip_not_ready"] == 1
    assert pre["skip_too_large"] == 1
    assert pre["skip_done"] == 1


def test_create_unified_job_with_skip_ineligible_marks_skipped(tmp_path, monkeypatch):
    """create_unified_job(skip_ineligible=True) помечает not_ready/too_large/done
    как status='skipped' и не пытается их выполнять.
    """
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    from backend.app.services.stage_comparison import paths as paths_mod

    sid = "sess_create_skip"
    paths_mod.session_json_path(sid).write_text(json.dumps({
        "id": sid, "pair_order": ["p_run", "p_not_ready"],
    }), encoding="utf-8")
    for pid in ("p_run", "p_not_ready"):
        l = _write_md(tmp_path / f"{pid}-L.md", "L")
        r = _write_md(tmp_path / f"{pid}-R.md", "R")
        paths_mod.pair_json_path(sid, pid).write_text(json.dumps({
            "id": pid, "status": "matched",
            "left":  {"filename": "l.pdf", "pdf_path": "/x.pdf", "md_path": str(l)},
            "right": {"filename": "r.pdf", "pdf_path": "/x.pdf", "md_path": str(r)},
        }), encoding="utf-8")
    _write_replacement_enriched(sid, "p_run", "left",  "small")
    _write_replacement_enriched(sid, "p_run", "right", "small")

    job = jobs.create_unified_job(
        sid, scope="session", confirm=True, skip_ineligible=True,
        force_compare=True,
    )
    items_by_pid = {it["pair_id"]: it for it in job["items"]}
    assert items_by_pid["p_run"]["status"] == "queued"
    assert items_by_pid["p_not_ready"]["status"] == "skipped"
    assert items_by_pid["p_not_ready"]["preflight_action"] == "skip_not_ready"
    assert job["progress"]["skipped"] == 1
    assert job["progress"]["total"] == 2


def test_create_unified_job_all_skipped_finishes_done(tmp_path, monkeypatch):
    """Если все пары отфильтрованы — job сразу done, без запуска."""
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    from backend.app.services.stage_comparison import paths as paths_mod

    sid = "sess_all_skipped"
    paths_mod.session_json_path(sid).write_text(json.dumps({
        "id": sid, "pair_order": ["p1"],
    }), encoding="utf-8")
    l = _write_md(tmp_path / "L.md", "L")
    r = _write_md(tmp_path / "R.md", "R")
    paths_mod.pair_json_path(sid, "p1").write_text(json.dumps({
        "id": "p1", "status": "matched",
        "left":  {"filename": "l.pdf", "pdf_path": "/x.pdf", "md_path": str(l)},
        "right": {"filename": "r.pdf", "pdf_path": "/x.pdf", "md_path": str(r)},
    }), encoding="utf-8")
    # Нет enriched — p1 будет skip_not_ready.

    job = jobs.create_unified_job(
        sid, scope="session", confirm=True, skip_ineligible=True,
    )
    assert job["status"] == "done"
    assert job["progress"]["skipped"] == 1


def test_find_active_unified_session_job_prefers_running(tmp_path, monkeypatch):
    """find_active_session_job отдаёт queued/running, иначе самый свежий done."""
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    from backend.app.services.stage_comparison import paths as paths_mod

    sid = "sess_active"
    paths_mod.session_json_path(sid).write_text(json.dumps({
        "id": sid, "pair_order": ["p1"],
    }), encoding="utf-8")
    paths_mod.pair_json_path(sid, "p1").write_text(json.dumps({
        "id": "p1", "status": "matched",
        "left":  {"filename": "l.pdf", "pdf_path": "/x.pdf", "md_path": "/x.md"},
        "right": {"filename": "r.pdf", "pdf_path": "/x.pdf", "md_path": "/y.md"},
    }), encoding="utf-8")
    # Создадим два job: один done, один queued.
    done_job = jobs.create_unified_job(sid, scope="pair", pair_id="p1", confirm=True)
    # вручную пометим done.
    p_done = paths_mod.job_json_path(sid, done_job["id"])
    d = json.loads(p_done.read_text(encoding="utf-8"))
    d["status"] = "done"
    p_done.write_text(json.dumps(d), encoding="utf-8")

    queued_job = jobs.create_unified_job(sid, scope="session", confirm=True)
    active = jobs.find_active_session_job(sid)
    assert active is not None
    assert active["id"] == queued_job["id"]
    assert "aggregate" in active


def test_find_active_unified_returns_none_when_no_jobs(tmp_path):
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    from backend.app.services.stage_comparison import paths as paths_mod
    sid = "sess_no_jobs"
    paths_mod.session_json_path(sid).write_text(json.dumps({
        "id": sid, "pair_order": [],
    }), encoding="utf-8")
    assert jobs.find_active_session_job(sid) is None


def test_aggregate_job_progress_counts_done_failed_skipped():
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    job = {
        "items": [
            {"pair_id": "a", "status": "done", "changes_count": 5, "duration_sec": 10.0},
            {"pair_id": "b", "status": "failed", "changes_count": 0, "duration_sec": 4.0},
            {"pair_id": "c", "status": "skipped", "preflight_action": "skip_too_large",
             "changes_count": 0, "duration_sec": 0.0},
            {"pair_id": "d", "status": "skipped", "preflight_action": "skip_not_ready",
             "changes_count": 0, "duration_sec": 0.0},
            {"pair_id": "e", "status": "running"},
            {"pair_id": "f", "status": "queued"},
        ],
    }
    agg = jobs.aggregate_job_progress(job)
    assert agg["total_pairs"] == 6
    assert agg["done"] == 1
    assert agg["failed"] == 1
    assert agg["skipped"] == 2
    assert agg["skipped_too_large"] == 1
    assert agg["skipped_not_ready"] == 1
    assert agg["total_changes"] == 5
    # current = first non-terminal (running) → "e"
    assert agg["current_pair_id"] == "e"
    assert agg["current_pair_status"] == "running"
    # avg_duration: для already-finished с duration_sec > 0
    assert agg["avg_duration_sec"] > 0


@pytest.mark.asyncio
async def test_unified_job_skips_pre_marked_skipped_items(tmp_path, monkeypatch):
    """run_unified_job не должен запускать пары, помеченные skip_ineligible'ом."""
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    from backend.app.services.stage_comparison import unified_analysis as ua
    from backend.app.services.stage_comparison import paths as paths_mod
    from backend.app.services.stage_comparison import enriched_comparison as ec

    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED", "true")
    sid = "sess_skip_run"
    paths_mod.session_json_path(sid).write_text(json.dumps({
        "id": sid, "pair_order": ["p_run", "p_skip"],
    }), encoding="utf-8")
    for pid in ("p_run", "p_skip"):
        paths_mod.pair_json_path(sid, pid).write_text(json.dumps({
            "id": pid, "status": "matched",
            "left":  {"filename": "l.pdf", "pdf_path": "/x.pdf", "md_path": "/x.md"},
            "right": {"filename": "r.pdf", "pdf_path": "/x.pdf", "md_path": "/y.md"},
        }), encoding="utf-8")
    _write_replacement_enriched(sid, "p_run", "left",  "small")
    _write_replacement_enriched(sid, "p_run", "right", "small")

    # Mock provider, чтобы tests не звали Claude Code.
    payload = {"status": "done", "summary": "", "changes": [], "warnings": []}
    fake_provider = _AvailableProvider(raw_response=json.dumps(payload), status="done")
    monkeypatch.setattr(ec, "_REGISTRY", {"claude_code": lambda: fake_provider})

    job = jobs.create_unified_job(
        sid, scope="session", confirm=True, skip_ineligible=True,
        force_compare=True,
    )
    items_by_pid = {it["pair_id"]: it for it in job["items"]}
    assert items_by_pid["p_skip"]["status"] == "skipped"

    finished = await jobs.run_unified_job(sid, job["id"])
    # p_skip остался skipped, p_run прошёл.
    out_by_pid = {it["pair_id"]: it for it in finished["items"]}
    assert out_by_pid["p_skip"]["status"] == "skipped"
    assert out_by_pid["p_run"]["status"] == "done"
    # Provider вызван 1 раз (только для p_run).
    assert len(fake_provider.invoke_calls) == 1


def test_aggregate_job_progress_counts_comparing_enriching_as_running():
    """comparing/enriching — активные подсостояния running, должны учитываться."""
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    job = {
        "items": [
            {"pair_id": "a", "status": "done", "duration_sec": 10.0},
            {"pair_id": "b", "status": "comparing"},
            {"pair_id": "c", "status": "enriching"},
            {"pair_id": "d", "status": "queued"},
        ],
    }
    agg = jobs.aggregate_job_progress(job)
    assert agg["done"] == 1
    assert agg["running"] == 2  # comparing + enriching
    assert agg["queued"] == 1
    # current = первый non-terminal не-queued → b (comparing)
    assert agg["current_pair_id"] == "b"
    assert agg["current_pair_status"] == "comparing"


def test_read_job_marks_stale_running_as_failed_interrupted(tmp_path, monkeypatch):
    """_maybe_mark_interrupted: если на диске status=running, но воркера нет,
    job помечается failed_interrupted и items с активным статусом — тоже."""
    from backend.app.services.stage_comparison import unified_analysis_jobs as jobs
    from backend.app.services.stage_comparison import paths as paths_mod

    sid = "sess_stale"
    job_id = "uajob_stale_test"
    paths_mod.jobs_root(sid).mkdir(parents=True, exist_ok=True)
    stale = {
        "id": job_id,
        "session_id": sid,
        "type": "unified_stage_comparison",
        "status": "running",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "items": [
            {"pair_id": "a", "status": "done"},
            {"pair_id": "b", "status": "comparing"},
            {"pair_id": "c", "status": "queued"},
        ],
    }
    paths_mod.job_json_path(sid, job_id).write_text(
        json.dumps(stale, ensure_ascii=False), encoding="utf-8",
    )
    # Никакого _active_tasks для (sid, job_id) → _is_task_alive=False.
    jobs._active_tasks.pop(sid, None)

    read = jobs._read_job(sid, job_id)
    assert read is not None
    assert read["status"] == "failed_interrupted"
    item_statuses = {it["pair_id"]: it["status"] for it in read["items"]}
    assert item_statuses["a"] == "done"           # терминальный — не трогаем
    assert item_statuses["b"] == "failed_interrupted"
    assert item_statuses["c"] == "failed_interrupted"

    # Файл на диске тоже обновлён.
    disk = json.loads(paths_mod.job_json_path(sid, job_id).read_text(encoding="utf-8"))
    assert disk["status"] == "failed_interrupted"
