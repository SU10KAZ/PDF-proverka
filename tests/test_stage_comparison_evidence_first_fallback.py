"""Unit tests for evidence_first_s2_fallback strategy.

Покрытие:
  1. build_fact_index парсит страницы / штамп / классификацию;
  2. classifier: текстовые (img=0) → pz, «схема расположения» → structural,
     «План N этажа» → architectural;
  3. build_scope_map разделяет left_only / right_only / common;
  4. deterministic_fact_diff: штамп + scope-only разделы (1 grouped change);
  5. scope_aware_section_split укладывается в chunk budget;
  6. build_shared_header содержит штамп и материальную сводку;
  7. evidence verification: grounded quote проходит, выдуманный — нет;
  8. merge_and_dedup схлопывает кросс-чанковые дубли;
  9. orchestrator end-to-end с mock provider: drop ungrounded + dedup;
 10. orchestrator никогда не бросает (broken provider → status=error);
 11. флаг выключен → load_fallback_config().enabled is False.
"""
from __future__ import annotations

import json

import pytest

from backend.app.services.stage_comparison import evidence_first_fallback as ef
from backend.app.services.stage_comparison import enriched_comparison as ec


# ── Fixtures: synthetic enriched MD ────────────────────────────────────────

def _qwen_block(block_id: str, page: int, visible: str) -> str:
    return (
        f"<!-- QWEN_IMAGE_DESCRIPTION_START\n"
        f"format_version: replace_image_blocks_v1\nblock_id: {block_id}\n"
        f"page: {page}\nstatus: done\n-->\n\n### Графический блок / схема\n\n"
        f"Видимый текст:\n{visible}\n\n<!-- QWEN_IMAGE_DESCRIPTION_END -->\n"
    )


LEFT_MD = (
    "## СТРАНИЦА 1\n**Штамп:** Шифр: АА-ДС3-КР | Стадия: П | Объект: ЖК Тест | Организация: OldArch\n\n"
    "## СТРАНИЦА 2\n**Наименование листа:** Пояснительная записка. Общие указания\n"
    + ("Класс бетона фундаментной плиты по прочности В30, W8, F150. " * 8) + "\n\n"
    "## СТРАНИЦА 3\n**Лист:** 1\n**Наименование листа:** Схема расположения фундаментной плиты на отм. -9.800\n"
    + _qwen_block("BLK-F1", 3, "- отм. -9.800\n- Бетон В30 W8 F150") + "\n"
    "## СТРАНИЦА 4\n**Лист:** 2\n**Наименование листа:** Корпуса 1, 2. Схема расположения монолитных конструкций 1-го этажа\n"
    + _qwen_block("BLK-K12", 4, "- отм. +0.200\n- толщина плиты 200 мм") + "\n"
    "## СТРАНИЦА 5\n**Лист:** 10\n**Наименование листа:** Часть 1. Архитектурные решения. Планы. Корпус 4. План 5 этажа\n"
    + _qwen_block("BLK-AR", 5, "- план 5 этажа") + "\n"
    "## СТРАНИЦА 6\n**Лист:** 40\n**Наименование листа:** Узлы гидроизоляции подземной части\n"
    + _qwen_block("BLK-DET", 6, "- узел гидроизоляции") + "\n"
)

# RIGHT: нет ПЗ (стр.2), нет АР (стр.5), нет деталей (стр.6). Штамп другой.
# Бетон фундамента изменён: W6 F200.
RIGHT_MD = (
    "## СТРАНИЦА 1\n**Штамп:** Стадия: П | Объект: ЖК Тест | Организация: NewProject\n\n"
    "## СТРАНИЦА 2\n**Лист:** 01\n**Наименование листа:** Схема расположения фундаментной плиты на отм. -9.800\n"
    + _qwen_block("BLK-F1R", 2, "- отм. -9.800\n- Бетон В30 W6 F200") + "\n"
    "## СТРАНИЦА 3\n**Лист:** 02\n**Наименование листа:** Корпуса 1, 2. Схема расположения монолитных конструкций 1-го этажа\n"
    + _qwen_block("BLK-K12R", 3, "- отм. +0.150\n- толщина плиты 220 мм") + "\n"
)


# ── 1-2. fact index + classifier ───────────────────────────────────────────

def test_build_fact_index_parses_pages_and_stamp():
    idx = ef.build_fact_index("left", LEFT_MD)
    assert len(idx.pages) == 6
    assert "АА-ДС3-КР" in idx.stamp
    assert idx.total_chars == len(LEFT_MD)


def test_classifier_text_page_is_pz():
    idx = ef.build_fact_index("left", LEFT_MD)
    p2 = next(p for p in idx.pages if p.page == 2)
    assert p2.section_class == "pz"  # img=0, длинный текст


def test_classifier_sheet_kinds():
    idx = ef.build_fact_index("left", LEFT_MD)
    by_page = {p.page: p for p in idx.pages}
    assert by_page[3].section_class == "structural"        # «схема расположения»
    assert by_page[4].section_class == "structural"
    assert by_page[4].building_part == "1,2"
    assert by_page[5].section_class == "architectural"     # «архитектурные ... план»
    assert by_page[6].section_class == "sections_details"  # «узлы гидроизоляции»


# ── 3. scope map ────────────────────────────────────────────────────────────

def test_scope_map_separates_sides():
    sm = ef.build_scope_map(ef.build_fact_index("left", LEFT_MD),
                            ef.build_fact_index("right", RIGHT_MD))
    # ПЗ, АР, детали — только слева.
    assert "pz|общий" in sm.left_only
    assert "architectural|общий" in sm.left_only
    assert "sections_details|общий" in sm.left_only
    assert sm.right_only == []
    # Общие структурные — обе стороны.
    assert any(k.startswith("structural") for k in sm.common)


# ── 4. deterministic fact diff ──────────────────────────────────────────────

def test_deterministic_diff_stamp_and_scope_groups():
    sm = ef.build_scope_map(ef.build_fact_index("left", LEFT_MD),
                            ef.build_fact_index("right", RIGHT_MD))
    det = ef.deterministic_fact_diff(sm)
    types = [c["type"] for c in det]
    assert "stamp_changed" in types
    # ПЗ / АР / детали — каждая ОДНОЙ grouped section_changed, не по листам.
    titles = " ".join(c["title"] for c in det)
    assert "Пояснительная записка" in titles
    assert "Архитектурные листы" in titles
    assert "Разрезы / детали" in titles
    # все детерминированные считаются grounded по построению
    for c in det:
        ef.verify_change_evidence(c, ef._norm_text(LEFT_MD), ef._norm_text(RIGHT_MD), ef.FallbackConfig())
        assert c["evidence_verified"] is True


# ── 5. section split budget ─────────────────────────────────────────────────

def test_section_split_respects_budget():
    sm = ef.build_scope_map(ef.build_fact_index("left", LEFT_MD),
                            ef.build_fact_index("right", RIGHT_MD))
    cfg = ef.FallbackConfig(chunk_max_chars=2000)
    chunks = ef.scope_aware_section_split(sm, cfg)
    assert chunks, "должен быть хотя бы один чанк по общим scope"
    for c in chunks:
        assert c.total_chars <= cfg.chunk_max_chars * 1.5


def test_section_split_caps_chunk_count():
    sm = ef.build_scope_map(ef.build_fact_index("left", LEFT_MD),
                            ef.build_fact_index("right", RIGHT_MD))
    cfg = ef.FallbackConfig(chunk_max_chars=100, max_chunks=2)
    chunks = ef.scope_aware_section_split(sm, cfg)
    assert len(chunks) <= 2


# ── 6. shared header ────────────────────────────────────────────────────────

def test_shared_header_has_stamp_and_materials():
    sm = ef.build_scope_map(ef.build_fact_index("left", LEFT_MD),
                            ef.build_fact_index("right", RIGHT_MD))
    hdr = ef.build_shared_header(sm, ef.FallbackConfig())
    assert "SHARED_GLOBAL_HEADER" in hdr
    assert "OldArch" in hdr and "NewProject" in hdr
    assert "В30" in hdr  # материальная сводка подтянута


# ── 7. evidence verification ────────────────────────────────────────────────

def test_evidence_verification_grounded_vs_hallucinated():
    cfg = ef.FallbackConfig()
    ln = ef._norm_text(LEFT_MD)
    rn = ef._norm_text(RIGHT_MD)
    grounded = {
        "provenance": "llm_chunk", "title": "x",
        "evidence_left": {"quote": "Класс бетона фундаментной плиты по прочности В30, W8, F150"},
        "evidence_right": {"quote": "Бетон В30 W6 F200"},
    }
    ef.verify_change_evidence(grounded, ln, rn, cfg)
    assert grounded["evidence_verified"] is True

    halluc = {
        "provenance": "llm_chunk", "title": "y",
        "evidence_left": {"quote": ""},
        "evidence_right": {"quote": "ВРП-99 полностью выдуманный щит которого нет нигде 9999А"},
    }
    ef.verify_change_evidence(halluc, ln, rn, cfg)
    assert halluc["evidence_verified"] is False


# ── 8. merge + dedup ────────────────────────────────────────────────────────

def test_merge_dedup_collapses_cross_chunk_duplicates():
    c1 = ef._mk_change(type_="material_changed", source="mixed", severity="high",
                       title="Изменена марка бетона фундаментной плиты", summary="a",
                       provenance="llm_chunk")
    c2 = dict(c1); c2["id"] = "other"   # тот же title/type → дубль
    merged, dups = ef.merge_and_dedup([], [c1, c2])
    assert len(merged) == 1
    assert dups == 1


def test_merge_dedup_collapses_global_stamp_singletons():
    # shared header заставляет Opus повторять смену штампа в каждом чанке,
    # формулируя по-разному → сигнатурная дедупликация не ловит. Singleton-collapse
    # схлопывает их в один, отдавая приоритет детерминированному.
    det = ef._mk_change(type_="stamp_changed", source="stamp", severity="high",
                        title="Изменён штамп комплекта", summary="d", provenance="deterministic")
    llm1 = ef._mk_change(type_="stamp_changed", source="stamp", severity="high",
                         title="Сменился штамп: шифр и организация", summary="l1", provenance="llm_chunk")
    llm2 = ef._mk_change(type_="stamp_changed", source="stamp", severity="high",
                         title="Изменён разработчик в штампе", summary="l2", provenance="llm_chunk")
    merged, dups = ef.merge_and_dedup([det], [llm1, llm2])
    stamps = [c for c in merged if c["type"] == "stamp_changed"]
    assert len(stamps) == 1
    assert stamps[0]["provenance"] == "deterministic"
    assert dups == 2


def test_deterministic_diff_no_within_scope_sheet_dups():
    # Стейдж 3.3 (added/removed листов внутри ОБЩЕГО scope) убран — эти изменения
    # сообщает LLM per-sheet. Детерминированно остаются только штамп (3.1) и
    # scope-only разделы целиком (3.2).
    sm = ef.build_scope_map(ef.build_fact_index("left", LEFT_MD),
                            ef.build_fact_index("right", RIGHT_MD))
    det = ef.deterministic_fact_diff(sm)
    titles = [c["title"] for c in det]
    # нет "Изъяты/Добавлены листы из раздела ..." (это 3.3)
    assert not any("листы из раздел" in t.lower() or "листы в раздел" in t.lower() for t in titles)
    # но scope-only разделы остаются
    assert any("Пояснительная записка" in t for t in titles)


def test_merge_dedup_deterministic_wins():
    det = ef._mk_change(type_="stamp_changed", source="stamp", severity="high",
                        title="Изменён штамп комплекта", summary="d",
                        provenance="deterministic")
    llm = ef._mk_change(type_="stamp_changed", source="stamp", severity="high",
                        title="Изменён штамп комплекта", summary="l",
                        provenance="llm_chunk")
    merged, _ = ef.merge_and_dedup([det], [llm])
    assert len(merged) == 1
    assert merged[0]["provenance"] == "deterministic"


# ── 9-10. orchestrator end-to-end ───────────────────────────────────────────

class _FakeResult:
    def __init__(self, raw):
        self.status = "done"; self.raw_response = raw
        self.error = None; self.duration_sec = 0.1


class _FakeProvider:
    name = "fake"

    def check_availability(self):
        return True, None

    def invoke(self, *, system_prompt, user_prompt, model, timeout_sec, work_dir=None):
        payload = {"status": "done", "summary": "chunk", "changes": [
            {"source": "mixed", "type": "material_changed", "severity": "high",
             "title": "Изменена марка бетона фундаментной плиты", "summary": "W8/F150->W6/F200",
             "confidence": 0.8, "requires_human_review": True,
             "evidence_left": {"quote": "Класс бетона фундаментной плиты по прочности В30, W8, F150"},
             "evidence_right": {"quote": "Бетон В30 W6 F200"}},
            {"source": "image_enrichment", "type": "added", "severity": "medium",
             "title": "Добавлен вымышленный щит ВРП-99", "summary": "hallucination",
             "confidence": 0.4, "requires_human_review": True,
             "evidence_left": {"quote": ""},
             "evidence_right": {"quote": "ВРП-99 9999А полностью выдуманный текст которого нет"}},
        ], "warnings": []}
        return _FakeResult(json.dumps(payload))


def _run(provider, cfg=None):
    return ef.run_evidence_first_fallback(
        left_md=LEFT_MD, right_md=RIGHT_MD, provider=provider,
        system_prompt=ec.SYSTEM_PROMPT, model="opus", timeout_sec=10,
        parse_extract_fn=ec._extract_model_payload,
        parse_json_fn=ec._parse_model_json,
        normalize_change_fn=ec._normalize_change,
        config=cfg or ef.FallbackConfig(enabled=True),
        base_input_stats={"total_chars": len(LEFT_MD) + len(RIGHT_MD)},
    )


def test_orchestrator_drops_ungrounded_and_dedups():
    res = _run(_FakeProvider())
    assert res["status"] == "done"
    assert res["strategy"] == ef.STRATEGY
    assert res["fallback"] is True
    d = res["diagnostics"]
    # каждый чанк вернул 1 hallucinated → все выкинуты
    assert d["llm_changes_dropped_ungrounded"] >= 1
    # grounded material change встретился в нескольких чанках → дедуп до 1
    material = [c for c in res["changes"] if c["type"] == "material_changed"]
    assert len(material) == 1
    # ни один вымышленный ВРП-99 не дожил
    assert not any("ВРП-99" in c["title"] for c in res["changes"])
    # детерминированные scope-changes присутствуют
    assert any(c["provenance"] == "deterministic" for c in res["changes"])


class _BrokenProvider:
    name = "broken"

    def check_availability(self):
        return True, None

    def invoke(self, **kwargs):
        raise RuntimeError("boom")


def test_orchestrator_never_raises_on_provider_error():
    res = _run(_BrokenProvider())
    # pipeline не падает; детерминированные изменения всё равно есть
    assert res["status"] in ("done", "error")
    assert any(c["provenance"] == "deterministic" for c in res["changes"]) or res["status"] == "error"
    # все чанки помечены ошибкой в warnings
    assert res["diagnostics"]["llm_changes_raw"] == 0


# ── 11. flag default off ────────────────────────────────────────────────────

def test_fallback_disabled_by_default(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED", raising=False)
    assert ef.load_fallback_config().enabled is False


# ── 11b. controlled rollout flags ───────────────────────────────────────────

def test_rollout_flags_defaults_preserve_behavior(monkeypatch):
    for v in ("STAGE_COMPARISON_EVIDENCE_S2_VERIFY_ENABLED",
              "STAGE_COMPARISON_EVIDENCE_S2_DEDUP_ENABLED",
              "STAGE_COMPARISON_EVIDENCE_S2_LOW_CONF_IMAGE_CAN_CONFIRM"):
        monkeypatch.delenv(v, raising=False)
    cfg = ef.load_fallback_config()
    assert cfg.verify_enabled is True
    assert cfg.dedup_enabled is True
    assert cfg.low_conf_image_can_confirm is True  # дефолт сохраняет поведение


def test_rollout_flags_parsed_from_env(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_EVIDENCE_S2_VERIFY_ENABLED", "false")
    monkeypatch.setenv("STAGE_COMPARISON_EVIDENCE_S2_DEDUP_ENABLED", "false")
    monkeypatch.setenv("STAGE_COMPARISON_EVIDENCE_S2_LOW_CONF_IMAGE_CAN_CONFIRM", "false")
    cfg = ef.load_fallback_config()
    assert cfg.verify_enabled is False
    assert cfg.dedup_enabled is False
    assert cfg.low_conf_image_can_confirm is False


def test_low_conf_image_gate_blocks_when_disabled():
    ln, rn = ef._norm_text(LEFT_MD), ef._norm_text(RIGHT_MD)
    # low-confidence чисто визуальное изменение с grounded quote
    base = dict(provenance="llm_chunk", title="z", source="image_enrichment",
                confidence=0.3,
                evidence_left={"quote": "Класс бетона фундаментной плиты по прочности В30, W8, F150"},
                evidence_right={"quote": ""})
    # can_confirm=True (дефолт) → проходит
    c1 = dict(base)
    ef.verify_change_evidence(c1, ln, rn, ef.FallbackConfig(low_conf_image_can_confirm=True))
    assert c1["evidence_verified"] is True
    # can_confirm=False → блокируется (нет non-visual evidence)
    c2 = dict(base)
    ef.verify_change_evidence(c2, ln, rn, ef.FallbackConfig(low_conf_image_can_confirm=False))
    assert c2["evidence_verified"] is False
    assert c2.get("low_conf_image_blocked") is True


def test_low_conf_gate_allows_high_conf_and_nonvisual(monkeypatch):
    ln, rn = ef._norm_text(LEFT_MD), ef._norm_text(RIGHT_MD)
    cfg = ef.FallbackConfig(low_conf_image_can_confirm=False)
    # высокая уверенность → не блокируется даже визуальное
    hi = dict(provenance="llm_chunk", title="h", source="image_enrichment", confidence=0.9,
              evidence_left={"quote": "Класс бетона фундаментной плиты по прочности В30, W8, F150"},
              evidence_right={"quote": ""})
    ef.verify_change_evidence(hi, ln, rn, cfg)
    assert hi["evidence_verified"] is True
    # текстовый source с низкой уверенностью → не визуальный, не блокируется
    txt = dict(provenance="llm_chunk", title="t", source="text", confidence=0.2,
               evidence_left={"quote": "Класс бетона фундаментной плиты по прочности В30, W8, F150"},
               evidence_right={"quote": ""})
    ef.verify_change_evidence(txt, ln, rn, cfg)
    assert txt["evidence_verified"] is True


# ── 12. batch preflight behavior (acceptance) ───────────────────────────────
#
# Требование:
#   fallback disabled: too_large → skip_too_large
#   fallback enabled:  too_large → run + analysis_strategy=evidence_first_s2_fallback

from backend.app.services.stage_comparison import unified_analysis_jobs as uaj_mod


def _make_large_pair(tmp_path, monkeypatch, *, total_over_limit: bool):
    """Создать на диске пару с enriched MD обеих сторон под tmp COMPARISON_ROOT."""
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "cmp"))
    from backend.app.services.stage_comparison import paths as paths_mod
    sid, pid = "sess_test", "pair_test"
    # лимит 1000; over → 600 на сторону (1200>1000), under → 100 на сторону (200<1000)
    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS", "1000")
    body = "## СТРАНИЦА 1\n" + ("x" * (600 if total_over_limit else 100))
    for side in ("left", "right"):
        p = paths_mod.text_enrichment_md_path(sid, pid, side)
        p.write_text(body, encoding="utf-8")
    return sid, pid


def test_batch_preflight_too_large_skips_when_fallback_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED", "false")
    sid, pid = _make_large_pair(tmp_path, monkeypatch, total_over_limit=True)
    info = uaj_mod._classify_pair_for_batch(sid, pid, force_compare=False)
    assert info["too_large"] is True
    assert info["action"] == "skip_too_large"
    assert "analysis_strategy" not in info


def test_batch_preflight_too_large_runs_when_fallback_enabled(tmp_path, monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED", "true")
    sid, pid = _make_large_pair(tmp_path, monkeypatch, total_over_limit=True)
    info = uaj_mod._classify_pair_for_batch(sid, pid, force_compare=False)
    assert info["too_large"] is True
    assert info["fallback_enabled"] is True
    assert info["action"] == "run"
    assert info["analysis_strategy"] == ef.STRATEGY


def test_batch_preflight_small_pair_runs_without_strategy(tmp_path, monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED", "true")
    sid, pid = _make_large_pair(tmp_path, monkeypatch, total_over_limit=False)
    info = uaj_mod._classify_pair_for_batch(sid, pid, force_compare=False)
    assert info["too_large"] is False
    assert info["action"] == "run"
    assert "analysis_strategy" not in info


def test_batch_preflight_summary_counts_fallback_runs(tmp_path, monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED", "true")
    sid, pid = _make_large_pair(tmp_path, monkeypatch, total_over_limit=True)
    # get_session агрегирует пары из pairs/-папок; для unit-теста счётчика
    # достаточно вернуть session dict с нужной парой.
    monkeypatch.setattr(
        uaj_mod.store_mod, "get_session",
        lambda _sid: {"id": sid, "pairs": [{"id": pid, "status": "matched"}]},
    )
    summary = uaj_mod.preflight_session_for_batch(sid, scope="session")
    assert summary["total_pairs"] == 1
    assert summary["skip_too_large"] == 0
    assert summary["will_run"] == 1
    assert summary["will_run_fallback"] == 1
