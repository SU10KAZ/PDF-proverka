"""GRSH dense single-line mode — классификация, prompt, Chandra-словарь,
validation/dedup, IMAGE_DIFF_INDEX, domain_fields, совместимость r4/r5.

Сводит контролируемый эксперимент (attempt_05 single-shot + Chandra anchors +
anti-series + deterministic dedup) с production-пайплайном Qwen-enrichment.

Живой Qwen / Opus / unified-analysis НЕ вызываются — только чистые функции.
"""
from __future__ import annotations

import copy

import pytest

from backend.app.services.stage_comparison import md_image_enrichment as m
from backend.app.services.stage_comparison import problem_block_retry as pbr


@pytest.fixture(autouse=True)
def _default_domain_fields_off(monkeypatch):
    """Hermetic default: GRSH baseline tests assert the v7 (no domain_fields)
    prompt unless a test explicitly opts in. Without this, an operator `.env`
    with STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED=true (loaded at import) would flip
    the GRSH prompt to v8 and fail the baseline assertions. The explicit
    `test_grsh_domain_fields_on_*` test re-sets it to true and overrides this."""
    monkeypatch.delenv("STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED", raising=False)


# ─── Inline fixtures (CI-safe, не зависят от runtime experiments/) ─────────

# Реалистичный Chandra-OCR текст плотной однолинейной схемы ГРЩ.
CHANDRA_GRSH_RAW = """### BLOCK [IMAGE]: 763U-YFTA-DVQ
Тип: Схема. Краткое описание: Однолинейная схема ГРЩ.
ГРЩ1, 1ГРЩ, 2ГРЩ, ГРЩ1-РП1, ГРЩ1-РП2, ГРЩ1-КУ1, ГРЩ1-КУ2.
Вводы от ТП1 и ТП2, трансформаторы Т1, Т2.
ВРУ1, ВРУ2, ВРУ3, ВРУ4, ВРУа, ВРУ-ИТП, ВРУ-ХЦ, ВРУ-АПТ, ВРУ-НСТ.
ШУ-ХЦ, ШУ-АПТ, ШУ-ХВС.
1QF1, 1QF2, 2QF10, QS1, QFD3, Wh1, Wh2.
Меркурий 234, АУКРМ, АКВРМ, АВР.
Кабели: ППГнг(А)-HF 5х120, КППГнг(А)-HF 5х16, ПуГПнг(А)-HF-1х25.
Шинопровод 3200А. 3200А, 2500А, 2000А, 800А, 630А, 320А, 200А, 63А, 50А.
380/220В, 40кА. Py=625,0 кВт, Рр=30,0кВт.
1ГРЩ-ВРУ1, 1ГРЩ-ВРУ2, ГРЩ1-РП1-1.
"""

MUST_HAVE_GRSH = [
    "ВРУ1", "ВРУ2", "ВРУ3", "ВРУ4", "ВРУа", "ВРУ-ИТП",
    "ВРУ-ХЦ", "ВРУ-АПТ", "ВРУ-НСТ", "ТП1", "ТП2",
]


def _clean_grsh_payload() -> dict:
    """attempt_05-подобный чистый GRSH-вывод (всё grounded, без ложных рядов)."""
    return {
        "status": "done",
        "sheet_kind": "electrical_single_line",
        "summary": "Однолинейная схема ГРЩ1 с двумя вводами от ТП1 и ТП2.",
        "verified_anchors": {
            "labels": [
                "ГРЩ1", "ГРЩ1-РП1", "ГРЩ1-РП2", "ВРУ1", "ВРУ2", "ВРУ3", "ВРУ4",
                "ВРУа", "ВРУ-ИТП", "ВРУ-ХЦ", "ВРУ-АПТ", "ВРУ-НСТ", "ТП1", "ТП2",
            ],
            "cables": ["ППГнг(А)-HF 5х120", "КППГнг(А)-HF 5х16"],
            "ratings": ["3200А", "800А", "630А", "63А"],
            "equipment": ["Меркурий 234", "АУКРМ", "АВР"],
        },
        "visual_unverified_anchors": [],
        "rejected_anchors": [],
        "panels": [
            {"name": "ГРЩ1 РП1", "type": "main_switchboard_section",
             "fed_from": "ТП1", "input": {"label": "Ввод 1 к ТП1", "busbar": "3L/PEN Al 3200А"}},
        ],
        "circuits": [
            {"id": "1ГРЩ-ВРУ1", "source": "ГРЩ1 РП1", "breaker": "1QF6",
             "cable": "ППГнг(А)-HF 5х120", "consumer": "ВРУ1", "confidence": 0.8},
        ],
        "connections": [
            {"from": "ТП1", "to": "ГРЩ1 РП1", "via": "шинопровод 3200А",
             "status": "verified_by_chandra", "confidence": 0.7},
        ],
        "uncertainties": [],
        "confidence": 0.74,
    }


def _hallucinated_grsh_payload() -> dict:
    """attempt_06-подобный вывод с достроенными ложными рядами."""
    p = _clean_grsh_payload()
    # Tile mode достроил несуществующие ряды:
    p["verified_anchors"]["labels"] += [f"ТП{i}" for i in range(3, 23)]  # ТП3…ТП22
    p["verified_anchors"]["labels"] += [f"ГРЩ1-РП1-{i}" for i in range(8, 16)]  # РП1-8…15
    p["verified_anchors"]["labels"] += [f"ГРЩ1-РП{i}" for i in range(3, 10)]  # РП3…РП9
    p["connections"] += [
        {"from": f"ГРЩ1-РП1-{i}", "to": f"ГРЩ1-РП1-{i+1}", "via": "питает"}
        for i in range(8, 14)
    ]
    return p


def _mk_md_block(text: str, page: int = 21) -> "m.MdBlock":
    return m.MdBlock(kind="image", text=text, page=page, block_id="763U-YFTA-DVQ")


# ─── 1. Классификация ──────────────────────────────────────────────────────

def test_grsh_block_classified_as_dense_grsh_singleline():
    mb = _mk_md_block(CHANDRA_GRSH_RAW)
    bt = m.classify_image_block(mb, surrounding_context="Однолинейная схема ГРЩ")
    assert bt == m.BLOCK_TYPE_DENSE_GRSH


def test_grsh_priority_over_dense_scheme():
    # Тот же блок без GRSH-заголовка → обычная (dense_)scheme, не GRSH.
    no_grsh = CHANDRA_GRSH_RAW.replace("Однолинейная схема ГРЩ", "Схема")
    no_grsh = no_grsh.replace("ГРЩ1", "ЩР1").replace("1ГРЩ", "1ЩР").replace("2ГРЩ", "2ЩР")
    no_grsh = no_grsh.replace("ГРЩ", "ЩР")
    mb = _mk_md_block(no_grsh)
    bt = m.classify_image_block(mb, surrounding_context="Схема электрическая")
    assert bt != m.BLOCK_TYPE_DENSE_GRSH
    assert bt in (m.BLOCK_TYPE_SCHEME, m.BLOCK_TYPE_DENSE_SCHEME)


def test_plain_scheme_mention_of_grsh_is_not_grsh():
    # Упоминание «ГРЩ» в сноске плана без однолинейной схемы → не GRSH.
    mb = _mk_md_block("### BLOCK [IMAGE]: x\nПлан этажа. Питание от ГРЩ.")
    bt = m.classify_image_block(mb, surrounding_context="План 1 этажа, оси 1-5")
    assert bt != m.BLOCK_TYPE_DENSE_GRSH


# ─── 2. Запрет tile mode для GRSH ──────────────────────────────────────────

def test_grsh_forbids_tile_mode_even_when_enabled(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_QWEN_TILE_ALLOW_GRSH", raising=False)
    cfg = pbr.ProblemBlockRetryConfig(enabled=True, mode="tiled", proactive_for_dense=True)
    result = {"block_type": "dense_grsh_singleline", "status": "error",
              "usable_for_diff": False, "description": {"error": "timeout"}}
    do_retry, reason = pbr.should_retry_problem_block(result, None, {}, cfg)
    assert do_retry is False
    assert reason == "grsh_no_tiling"


def test_grsh_proactive_dense_does_not_trigger_for_grsh(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_QWEN_TILE_ALLOW_GRSH", raising=False)
    cfg = pbr.ProblemBlockRetryConfig(enabled=True, mode="tiled", proactive_for_dense=True)
    # Полностью «здоровый» GRSH блок: proactive_for_dense сработал бы для
    # dense_scheme, но для GRSH — нет.
    result = {"block_type": "dense_grsh_singleline", "status": "done",
              "usable_for_diff": True,
              "description": {"summary": "ok", "verified_anchors": {"labels": ["ВРУ1", "ВРУ2"]}},
              "confidence_adjusted": 0.8}
    do_retry, reason = pbr.should_retry_problem_block(result, None, {}, cfg)
    assert do_retry is False and reason == "grsh_no_tiling"
    # А обычный «здоровый» dense_scheme при proactive_for_dense — тайлится.
    healthy_dense = {
        "block_type": "dense_scheme", "status": "done", "usable_for_diff": True,
        "finish_reason": "stop", "confidence_adjusted": 0.8,
        "description": {
            "summary": "Однолинейная схема",
            "confidence": 0.8,
            "diff_anchors": {
                "labels": [{"raw_text": "ЩР1"}, {"raw_text": "ЩР2"}, {"raw_text": "ЩР3"}],
                "ratings": [{"raw_text": "63А"}, {"raw_text": "100А"}],
                "connections": [{"from_raw": "ВРУ", "to_raw": "ЩР1"}],
            },
        },
    }
    do_retry2, reason2 = pbr.should_retry_problem_block(healthy_dense, None, {}, cfg)
    assert do_retry2 is True and reason2 == "large_graphic_proactive"


def test_grsh_tile_allowed_only_with_debug_override(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_QWEN_TILE_ALLOW_GRSH", "true")
    cfg = pbr.ProblemBlockRetryConfig(enabled=True, mode="tiled")
    result = {"block_type": "dense_grsh_singleline", "status": "error",
              "description": {"error": "timeout"}}
    do_retry, reason = pbr.should_retry_problem_block(result, None, {}, cfg)
    # Override снимает GRSH-блок → обычная логика problem-block (timeout → retry).
    assert do_retry is True and reason == "timeout"


# ─── 3. Chandra anchor extractor ───────────────────────────────────────────

def test_extract_chandra_anchors_covers_grsh_markings():
    a = m.extract_chandra_anchors(CHANDRA_GRSH_RAW)
    labels = a["labels"]
    equip = a["equipment"]
    cables = a["cables"]
    # labels: ГРЩ / ВРУ / ТП / Т1 / Т2
    assert "ГРЩ1" in labels
    assert "ВРУ1" in labels and "ВРУ-ХЦ" in labels
    assert "ТП1" in labels and "ТП2" in labels
    assert "Т1" in labels and "Т2" in labels
    # equipment: QF / QS / Wh / Меркурий / АУКРМ
    assert any("QF" in e for e in equip)
    assert any(e.startswith("QS") for e in equip)
    assert any("Меркурий" in e for e in equip)
    # cables: ППГнг / КППГнг
    joined_cables = " ".join(cables)
    assert "ППГнг(А)-HF" in joined_cables
    assert "КППГнг(А)-HF" in joined_cables
    # raw_tokens always present (vocabulary, not full description)
    assert a["raw_tokens"]


def test_grsh_vocab_block_and_prompt_injection():
    vocab = m.build_grsh_anchor_vocab_block(CHANDRA_GRSH_RAW)
    assert "СЛОВАРЬ Chandra-OCR" in vocab
    prompt, ver = m.get_prompt_for_block_type(m.BLOCK_TYPE_DENSE_GRSH, chandra_raw=CHANDRA_GRSH_RAW)
    assert ver == m.PROMPT_VERSION_GRSH
    assert "СЛОВАРЬ Chandra-OCR" in prompt           # vocab injected
    assert "ОДНОЛИНЕЙНАЯ СХЕМА ГРЩ" in prompt          # GRSH prompt body
    assert "НЕ достраивай" in prompt or "ЗАПРЕЩЕНО придумывать числовые ряды" in prompt
    # cache-версия GRSH ≠ scheme/general → старый кеш не подхватывается
    img = b"\x89PNG_fake"
    assert m.compute_image_cache_key(img, "qwen", m.PROMPT_VERSION_GRSH) != \
        m.compute_image_cache_key(img, "qwen", m.PROMPT_VERSION_SCHEME)


def test_grsh_prompt_without_chandra_is_still_valid():
    # fail-soft: пустой Chandra → vocab не добавляется, prompt всё равно валиден
    prompt, ver = m.get_prompt_for_block_type(m.BLOCK_TYPE_DENSE_GRSH, chandra_raw="")
    assert ver == m.PROMPT_VERSION_GRSH
    assert "ОДНОЛИНЕЙНАЯ СХЕМА ГРЩ" in prompt
    assert "СЛОВАРЬ Chandra-OCR" not in prompt


# ─── 4 + 5. Достроенные ряды отбрасываются, реальные остаются verified ──────

def test_tp_series_rejected_when_absent_from_chandra():
    payload = _clean_grsh_payload()
    payload["verified_anchors"]["labels"] += [f"ТП{i}" for i in range(3, 23)]
    out = m.apply_grsh_validation(copy.deepcopy(payload), CHANDRA_GRSH_RAW)
    verified = [str(x) for x in out["verified_anchors"]["labels"]]
    rejected = [str(x) for x in out["rejected_anchors"]]
    # ТП3…ТП22 → rejected, НЕ verified
    for i in range(3, 23):
        assert f"ТП{i}" in rejected
        assert f"ТП{i}" not in verified


def test_tp1_tp2_stay_verified():
    payload = _clean_grsh_payload()
    payload["verified_anchors"]["labels"] += [f"ТП{i}" for i in range(3, 23)]
    out = m.apply_grsh_validation(copy.deepcopy(payload), CHANDRA_GRSH_RAW)
    verified = [str(x) for x in out["verified_anchors"]["labels"]]
    assert "ТП1" in verified and "ТП2" in verified


def test_named_vru_consumers_preserved():
    out = m.apply_grsh_validation(_clean_grsh_payload(), CHANDRA_GRSH_RAW)
    verified = [str(x) for x in out["verified_anchors"]["labels"]]
    for must in MUST_HAVE_GRSH:
        assert must in verified, f"{must} must stay verified"
    # никаких ложных отбраковок на чистом выводе
    assert out["rejected_anchors"] == []


# ─── 6. ocr_only: Chandra-only маркировки не теряются ──────────────────────

def test_chandra_only_anchors_preserved_as_ocr_only():
    # Qwen «забыл» ВРУ-НСТ и ШУ-ХВС, но они есть в Chandra → ocr_only_anchors
    payload = _clean_grsh_payload()
    payload["verified_anchors"]["labels"] = [
        x for x in payload["verified_anchors"]["labels"] if x != "ВРУ-НСТ"
    ]
    out = m.apply_grsh_validation(copy.deepcopy(payload), CHANDRA_GRSH_RAW)
    ocr_only = [str(x) for x in out.get("ocr_only_anchors", [])]
    verified = [str(x) for x in out["verified_anchors"]["labels"]]
    # ВРУ-НСТ не потеряна: либо ocr_only, либо verified — но точно не пропала
    assert "ВРУ-НСТ" in ocr_only or "ВРУ-НСТ" in verified


# ─── 7. attempt_05-подобный (clean) проходит validation ────────────────────

def test_clean_payload_passes_validation():
    out = m.apply_grsh_validation(_clean_grsh_payload(), CHANDRA_GRSH_RAW)
    assert out.get("_grsh_validated") is True
    assert out["rejected_anchors"] == []            # нет ложных рядов
    rep = out.get("_grsh_validation", {})
    assert rep.get("series_rejected") == []
    assert rep.get("verified_count", 0) >= len(MUST_HAVE_GRSH)


# ─── 8. attempt_06-подобный (false series) НЕ проходит как verified ─────────

def test_hallucinated_series_not_verified():
    out = m.apply_grsh_validation(_hallucinated_grsh_payload(), CHANDRA_GRSH_RAW)
    verified = [str(x) for x in out["verified_anchors"]["labels"]]
    rejected = [str(x) for x in out["rejected_anchors"]]
    # ни один достроенный член ряда не остался в verified
    for i in range(3, 23):
        assert f"ТП{i}" not in verified
    for i in range(8, 16):
        assert f"ГРЩ1-РП1-{i}" not in verified
    for i in range(3, 10):
        assert f"ГРЩ1-РП{i}" not in verified
    # и они действительно в rejected
    assert any("ТП" in r for r in rejected)
    assert len(rejected) >= 10


def test_detect_chandra_artificial_series_reports_issues():
    labels = [f"ТП{i}" for i in range(1, 23)]
    issues = m.detect_chandra_artificial_series(labels, CHANDRA_GRSH_RAW)
    assert any("TP_series" in s or "artificial_sequence" in s for s in issues)
    # На чистом наборе — без issues
    assert m.detect_chandra_artificial_series(["ТП1", "ТП2", "ВРУ1", "ВРУ2"], CHANDRA_GRSH_RAW) == []


# ─── 9. IMAGE_DIFF_INDEX: rejected НЕ как evidence ─────────────────────────

def test_image_diff_index_separates_verified_unverified_rejected():
    payload = _hallucinated_grsh_payload()
    payload["visual_unverified_anchors"] = ["ЩитНеизвестный"]
    validated = m.apply_grsh_validation(copy.deepcopy(payload), CHANDRA_GRSH_RAW)
    desc_item = {
        "status": "done", "page": 21, "md_block_id": "763U-YFTA-DVQ",
        "block_type": "dense_grsh_singleline", "usable_for_diff": True,
        "warnings": [], "description": validated,
    }
    idx = m.build_image_diff_index([desc_item])
    assert "rejected (NOT evidence" in idx
    # rejected ТП-ряд НЕ должен попасть в evidence-секцию labels
    labels_part = idx.split("rejected (NOT evidence")[0]
    if "labels:" in labels_part:
        labels_block = labels_part.split("labels:")[1]
        assert "ТП15" not in labels_block
    # но в самом индексе rejected раздел присутствует
    assert "ТП15" in idx


def test_extract_anchors_grsh_shape():
    validated = m.apply_grsh_validation(_hallucinated_grsh_payload(), CHANDRA_GRSH_RAW)
    out = m._extract_anchors_from_description({"description": validated})
    # labels = verified + ocr_only (evidence); rejected/visual отдельно
    assert "ВРУ1" in out["labels"]
    assert "rejected" in out and any("ТП" in r for r in out["rejected"])
    # ни один rejected не утёк в labels
    for r in out["rejected"]:
        assert r not in out["labels"]


# ─── 10. domain_fields совместимость с GRSH ────────────────────────────────

def test_grsh_domain_fields_off_by_default(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED", raising=False)
    prompt, ver = m.get_prompt_for_block_type(m.BLOCK_TYPE_DENSE_GRSH, chandra_raw=CHANDRA_GRSH_RAW)
    assert ver == m.PROMPT_VERSION_GRSH
    assert "domain_fields" not in prompt


def test_grsh_domain_fields_on_full_slots_and_cache_version(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED", "true")
    prompt, ver = m.get_prompt_for_block_type(m.BLOCK_TYPE_DENSE_GRSH, chandra_raw=CHANDRA_GRSH_RAW)
    assert ver == m.PROMPT_VERSION_GRSH_DOMAIN
    assert "domain_fields" in prompt and "feeders" in prompt
    # cache-версия GRSH-domain ≠ GRSH
    img = b"\x89PNG"
    assert m.compute_image_cache_key(img, "q", m.PROMPT_VERSION_GRSH_DOMAIN) != \
        m.compute_image_cache_key(img, "q", m.PROMPT_VERSION_GRSH)
    # полный набор слотов проставляется coerce'ом
    payload = {"status": "done", "verified_anchors": {"labels": ["ВРУ1"]},
               "domain_fields": {"feeders": ["ВРУ1 3200А"]}}
    out = m._coerce_domain_fields(payload, m.BLOCK_TYPE_DENSE_GRSH)
    for slot in m.DOMAIN_FIXED_SLOTS[m.BLOCK_TYPE_DENSE_GRSH]:
        assert slot in out["domain_fields"]
    assert out["domain_fields"]["compensation"] == m._DOMAIN_FIELD_ABSENT


# ─── 11. Совместимость с consumer synonyms (r4) и r5/r6 контрактом Opus ─────

def test_consumer_synonyms_load_and_context():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    groups = ec.load_consumer_synonyms()
    assert any("ШУ-ХЦ" in g and "ВРУ-ХЦ" in g for g in groups)
    ctx = ec.build_consumer_synonyms_context(groups)
    assert "<CONSUMER_SYNONYMS>" in ctx and "ШУ-ХЦ" in ctx
    # GRSH prompt (Qwen-сторона) тоже несёт anti-rename для ШУ-ХЦ/ВРУ-ХЦ —
    # обе стороны согласованы, конфликта нет.
    assert "ШУ-ХЦ" in m.QWEN_GRSH_SINGLELINE_PROMPT
    assert "ВРУ-ХЦ" in m.QWEN_GRSH_SINGLELINE_PROMPT


def test_r5_present_one_side_and_disputed_still_work():
    from backend.app.services.stage_comparison import enriched_comparison as ec
    from backend.app.services.stage_comparison import v2_review as v2
    norm = ec._normalize_change({
        "type": "present_one_side", "title": "ВРУ-НСТ только в новой стадии",
        "summary": "Появилась отходящая линия ВРУ-НСТ",
        "evidence_right": "ВРУ-НСТ", "disputed": False,
    })
    assert norm is not None
    assert norm.get("type") == "present_one_side"
    assert norm.get("requires_human_review") is True  # present_one_side → ручная проверка
    # disputed → questionable в v2 quality label
    assert v2.derive_quality_label({"disputed": True}) == "questionable"


# ─── 12. Никаких сетевых вызовов Qwen/Opus в этих тестах ───────────────────

def test_no_network_calls_in_grsh_unit_layer(monkeypatch):
    """Smoke: весь GRSH unit-слой работает без httpx/network."""
    import httpx

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network call attempted in GRSH unit layer")

    monkeypatch.setattr(httpx, "AsyncClient", _boom)
    monkeypatch.setattr(httpx, "Client", _boom)
    # Полный цикл: extract → vocab → prompt → validate → render → index
    a = m.extract_chandra_anchors(CHANDRA_GRSH_RAW)
    assert a["labels"]
    prompt, _ = m.get_prompt_for_block_type(m.BLOCK_TYPE_DENSE_GRSH, chandra_raw=CHANDRA_GRSH_RAW)
    out = m.apply_grsh_validation(_hallucinated_grsh_payload(), CHANDRA_GRSH_RAW)
    body = m._format_qwen_description_md(out, model="qwen", page=21, block_id="x")
    assert "GRSH_VERIFIED_ANCHORS" in body
    assert "GRSH_REJECTED" in body
    idx = m.build_image_diff_index([{"status": "done", "page": 21,
                                     "md_block_id": "x", "block_type": "dense_grsh_singleline",
                                     "usable_for_diff": True, "warnings": [], "description": out}])
    assert "IMAGE_DIFF_INDEX_START" in idx


# ─── End-to-end: enrich_side проводит GRSH блок через весь пайплайн ─────────

GRSH_MD = """### СТРАНИЦА 21

### BLOCK [IMAGE]: img-001
Тип: Схема. Краткое описание: Однолинейная схема ГРЩ.
ГРЩ1, ВРУ1, ВРУ2, ВРУ3, ВРУ4, ВРУа, ВРУ-ИТП, ВРУ-ХЦ, ВРУ-АПТ, ВРУ-НСТ.
Вводы от ТП1 и ТП2, трансформаторы Т1, Т2. 1QF1, QS1, Wh1, Меркурий 234, АУКРМ.
Кабели ППГнг(А)-HF 5х120, КППГнг(А)-HF 5х16. Шинопровод 3200А. 3200А, 800А, 63А.
"""


def _write_min_png(path, color=(10, 20, 30)):
    from PIL import Image
    Image.new("RGB", (40, 30), color).save(path, format="PNG")
    return path


@pytest.mark.asyncio
async def test_enrich_side_grsh_end_to_end(tmp_path, monkeypatch):
    """enrich_side: GRSH блок классифицируется, получает GRSH prompt,
    результат проходит Chandra-validation, enriched MD содержит GRSH-секции,
    IMAGE_DIFF_INDEX не даёт rejected как evidence. Tile-retry не запускается."""
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "cmp"))
    (tmp_path / "cmp").mkdir(exist_ok=True)
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://test.example.com")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen/qwen3.6-35b-a3b")
    # tile-retry включён глобально — но для GRSH должен быть заблокирован
    monkeypatch.setenv("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ENABLED", "true")
    monkeypatch.delenv("STAGE_COMPARISON_QWEN_TILE_ALLOW_GRSH", raising=False)

    from backend.app.services.stage_comparison import graphic_llm_local as g

    src_md = tmp_path / "left.md"
    src_md.write_text(GRSH_MD, encoding="utf-8")
    crop_dir = tmp_path / "crops"
    crop_dir.mkdir()

    seen_prompt = {}

    def render(side_block_id, **kwargs):
        return _write_min_png(crop_dir / f"{side_block_id}.png")

    async def fake_describe(image_path, prompt):
        seen_prompt["prompt"] = prompt
        # Галлюцинированный GRSH-вывод с достроенными рядами ТП3..ТП22
        return g.DescribeResult(
            status="done", provider="local_openai_compatible",
            model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
            fallback_used=False,
            parsed=_hallucinated_grsh_payload(),
            raw_response_excerpt="raw", duration_sec=0.01,
        )

    result_json = tmp_path / "result.json"
    import json as _json
    result_json.write_text(_json.dumps({
        "pages": [{"page_number": 21, "width": 1000, "height": 1000, "blocks": [
            {"id": "img-001", "block_type": "image", "coords_px": [0, 0, 900, 600]},
        ]}],
    }), encoding="utf-8")

    summary = await m.enrich_side(
        "s_grsh", "p_grsh", "left",
        md_path=str(src_md), result_json_path=str(result_json),
        render_crop=render, describe_fn=fake_describe, run_model=True,
    )
    assert summary.described == 1 and summary.errors == 0

    # GRSH prompt реально применён (со словарём Chandra)
    assert "ОДНОЛИНЕЙНАЯ СХЕМА ГРЩ" in seen_prompt["prompt"]
    assert "СЛОВАРЬ Chandra-OCR" in seen_prompt["prompt"]

    descs = m._read_image_descriptions("s_grsh", "p_grsh", "left")
    item = descs["items"][0]
    assert item["block_type"] == "dense_grsh_singleline"
    assert item["used_prompt_version"] == m.PROMPT_VERSION_GRSH
    # validation применена в пайплайне
    desc = item["description"]
    assert desc.get("_grsh_validated") is True
    verified = [str(x) for x in desc["verified_anchors"]["labels"]]
    assert "ТП15" not in verified and "ТП1" in verified
    assert any("ТП" in str(x) for x in desc["rejected_anchors"])
    # tile-retry для GRSH не запускался
    assert "grsh_tile_retry_skipped" in item["warnings"]
    assert item.get("method_used") != "tiled_retry"

    # enriched MD: GRSH-секции + rejected помечены НЕ evidence
    enriched = (src_md.parent)  # noqa: just ensure path call below
    enriched_md = m.paths_mod.text_enrichment_md_path("s_grsh", "p_grsh", "left").read_text(encoding="utf-8")
    assert "GRSH_VERIFIED_ANCHORS" in enriched_md
    assert "rejected (NOT evidence" in enriched_md or "GRSH_REJECTED" in enriched_md
    assert "IMAGE_DIFF_INDEX_START" in enriched_md
