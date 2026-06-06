"""r1: фиксированная доменная JSON-схема Qwen с null-for-absent (flag-gated).

Покрывает:
  * флаг OFF (default) → prompt/версия/поведение идентичны прежним (no-op);
  * флаг ON → схемный prompt получает domain_fields-suffix и версию v6,
    cache-key меняется (через версию), `_coerce_domain_fields` детерминированно
    добивает недостающие слоты «не указано», сохраняя видимые значения;
  * рендер DOMAIN_FIELDS в enriched MD (включая «не указано»);
  * не-схемные block_type не затрагиваются даже при ON.

Живой Qwen не вызывается — тестируются чистые функции.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import md_image_enrichment as m


def test_domain_fields_off_by_default(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED", raising=False)
    # prompt и версия схемы не меняются
    prompt, ver = m.get_prompt_for_block_type(m.BLOCK_TYPE_SCHEME)
    assert ver == m.PROMPT_VERSION_SCHEME
    assert "domain_fields" not in prompt
    # coerce — no-op: ключ не добавляется
    payload = {"status": "done", "summary": "x"}
    out = m._coerce_domain_fields(payload, m.BLOCK_TYPE_SCHEME)
    assert "domain_fields" not in out


def test_domain_fields_on_changes_prompt_and_version(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED", "true")
    prompt, ver = m.get_prompt_for_block_type(m.BLOCK_TYPE_SCHEME)
    assert ver == m.PROMPT_VERSION_SCHEME_DOMAIN
    assert "domain_fields" in prompt
    assert "feeders" in prompt and "compensation" in prompt
    # cache-key завязан на версию → ключи v5 и v6 различаются
    img = b"\x89PNG_fake_bytes"
    k_v5 = m.compute_image_cache_key(img, "qwen", m.PROMPT_VERSION_SCHEME)
    k_v6 = m.compute_image_cache_key(img, "qwen", m.PROMPT_VERSION_SCHEME_DOMAIN)
    assert k_v5 != k_v6


def test_coerce_fills_absent_slots_and_keeps_values(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED", "true")
    payload = {
        "status": "done",
        "domain_fields": {"feeders": ["ЩР-1а 5х10"], "compensation": ""},
    }
    out = m._coerce_domain_fields(payload, m.BLOCK_TYPE_DENSE_SCHEME)
    df = out["domain_fields"]
    # все фиксированные слоты присутствуют
    for slot in m.DOMAIN_FIXED_SLOTS[m.BLOCK_TYPE_DENSE_SCHEME]:
        assert slot in df
    # видимое значение сохранено
    assert df["feeders"] == ["ЩР-1а 5х10"]
    # пустое/отсутствующее → «не указано»
    assert df["compensation"] == m._DOMAIN_FIELD_ABSENT
    assert df["earthing"] == m._DOMAIN_FIELD_ABSENT


def test_coerce_noop_for_non_scheme_block(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED", "true")
    # general/plan/table/stamp не в DOMAIN_FIXED_SLOTS → не трогаем
    payload = {"status": "done"}
    out = m._coerce_domain_fields(payload, m.BLOCK_TYPE_GENERAL)
    assert "domain_fields" not in out
    # и prompt для general остаётся v4
    prompt, ver = m.get_prompt_for_block_type(m.BLOCK_TYPE_GENERAL)
    assert ver == m.PROMPT_VERSION_GENERAL
    assert "domain_fields" not in prompt


def test_format_md_renders_domain_fields_including_absent():
    payload = {
        "status": "done",
        "domain_fields": {
            "feeders": ["ЩР-1а 5х10", "ВРУ-ХЦ 4х185"],
            "compensation": m._DOMAIN_FIELD_ABSENT,
            "main_breakers": "QF1 1000А",
        },
    }
    md = m._format_qwen_description_md(payload, model="qwen", page=24, block_id="b1")
    assert "DOMAIN_FIELDS" in md
    assert "feeders: ЩР-1а 5х10; ВРУ-ХЦ 4х185" in md
    # явное «не указано» рендерится (не скрывается)
    assert f"compensation: {m._DOMAIN_FIELD_ABSENT}" in md
    assert "main_breakers: QF1 1000А" in md
