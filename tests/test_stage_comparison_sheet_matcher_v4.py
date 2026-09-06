"""Sheet Matcher v4 за флагом STAGE_COMPARISON_SHEET_MATCHER_V4_ENABLED.

Две группы инвариантов.  Флаг выключен (прод по умолчанию): поведение v3
сохранено буквально — включая известные дефекты, которые v4 исправляет,
потому что база должна оставаться побайтово прежней.  Флаг включён: каждый
тест закрывает ДОКАЗАННЫЙ дефект или инвариант исследования 2026-09-06
(детерминизм, 0 обращений к модели, нет правил под конкретные страницы,
приоритет ручного сопоставления, сохранение 1→N / N→1, страж
неоднозначности HIGH).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import production_orchestrator as po
from backend.app.services.stage_comparison import sheet_matcher as sm
from backend.app.services.stage_comparison import sheet_matcher_flags as flags
from backend.app.services.stage_comparison import sheet_passport as sp
from backend.app.services.stage_comparison import sheet_scope_policy
from backend.app.services.stage_comparison.sheet_identity import (
    extract_sheet_identities,
    parse_stamp_title,
)

FLAG = flags.FEATURE_FLAG


@pytest.fixture
def v4_on(monkeypatch):
    monkeypatch.setenv(FLAG, "true")


@pytest.fixture
def v4_off(monkeypatch):
    monkeypatch.setenv(FLAG, "false")


def _sheet(page, *, functional=(), entities=(), topology=(), title=None, sheet_type=()):
    return {
        "pdf_page": page,
        "title": title,
        "functional_content": list(functional),
        "main_entities": list(entities),
        "relationships": list(topology),
        "sheet_type": list(sheet_type),
    }


def _tokens(prefix, n, start=1):
    return [f"{prefix}{i}" for i in range(start, start + n)]


# ------------------------------------------------------------------ flag ---

def test_flag_defaults_to_v3(monkeypatch):
    monkeypatch.delenv(FLAG, raising=False)
    assert flags.v4_enabled() is False
    assert flags.resolve_algorithm() == flags.ALGORITHM_V3
    assert sm.ALGORITHM_VERSION == flags.ALGORITHM_V3


@pytest.mark.parametrize("raw, expected", [
    ("true", True), ("1", True), ("on", True), ("yes", True),
    ("false", False), ("0", False), ("", False), ("maybe", False),
])
def test_flag_parsing(monkeypatch, raw, expected):
    monkeypatch.setenv(FLAG, raw)
    assert flags.v4_enabled() is expected


def test_explicit_algorithm_argument_overrides_the_flag(v4_on):
    F = _tokens("f", 10)
    left = [_sheet(1, functional=F, entities=F, topology=F)]
    right = [_sheet(1, functional=F, entities=F, topology=F), _sheet(9, functional=F, entities=F, topology=F)]
    forced_v3 = sm.match_sheets(left, right, algorithm=flags.ALGORITHM_V3)
    assert forced_v3["algorithm_version"] == flags.ALGORITHM_V3
    assert "ambiguous_high_demoted" not in forced_v3["diagnostics"]
    with pytest.raises(ValueError):
        sm.match_sheets(left, right, algorithm="production-sheet-matcher.v9")


def test_flags_snapshot_hides_allowlist_identifiers(monkeypatch):
    monkeypatch.setenv(FLAG, "false")
    monkeypatch.setenv(flags.SHADOW_FLAG, "true")
    monkeypatch.setenv(flags.SHADOW_PAIR_ALLOWLIST, "p_secret_1, p_secret_2")
    snapshot = flags.snapshot()
    assert snapshot == {
        "algorithm": flags.ALGORITHM_V3,
        "v4_enabled": False,
        "shadow_enabled": True,
        "shadow_pair_allowlist_configured": True,
        "shadow_run_allowlist_configured": False,
    }
    assert "p_secret" not in json.dumps(snapshot)
    assert flags.shadow_pair_allowlist() == frozenset({"p_secret_1", "p_secret_2"})


# --------------------------------------------- flag OFF = frozen v3 --------

def test_v3_keeps_the_factless_candidate_ahead_by_page_number(v4_off):
    """Известный дефект v3 сохранён намеренно: база остаётся побайтово прежней."""
    left = [_sheet(7, functional=_tokens("f", 10), entities=_tokens("e", 10), topology=_tokens("t", 10), title="Общие данные")]
    observed = _sheet(40, functional=_tokens("f", 2), entities=[], topology=[])
    factless = _sheet(7, title="Общие данные")
    result = sm.match_sheets(left, [factless, observed])
    top = result["candidate_search"][0]["top_candidates"]
    assert [item["right_page"] for item in top][0] == 7
    assert result["algorithm_version"] == flags.ALGORITHM_V3
    assert set(top[0]["signals"]) == {"functional", "entities", "sheet_type", "graphic", "title", "page_proximity"}
    assert "substantive_observed" not in top[0]


def test_v3_does_not_demote_an_ambiguous_high(v4_off):
    F = _tokens("f", 10)
    left = [_sheet(1, functional=F, entities=F, topology=F)]
    right = [_sheet(1, functional=F, entities=F, topology=F), _sheet(9, functional=F, entities=F, topology=F)]
    result = sm.match_sheets(left, right)
    relation = next(r for r in result["relations"] if r["left_pages"] == [1] and r["right_pages"])
    assert relation["status"] == "HIGH"
    assert "high_candidate_ambiguous" not in relation["reason_codes"]
    assert "ambiguous_high_demoted" not in result["diagnostics"]
    assert relation["provenance"]["algorithm"] == flags.ALGORITHM_V3


def test_v3_reads_the_axis_preposition_as_the_axis(v4_off):
    """Дефект оси штампа v3 сохранён за выключенным флагом (26 страниц АР2 корпуса)."""
    identity = parse_stamp_title("Корпус 1, 2. Фасад в осях 3.К-1.А", page=1)
    assert identity is not None and identity.section_axis == "в"
    assert parse_stamp_title("Корпус 1, 2. Фасад в осях 3.К-1.А", page=1, axis_preposition=True).section_axis == "3.к-1.а"


def test_v3_index_has_no_passport(v4_off, tmp_path):
    pair = _pair_with_markdown(tmp_path)
    indexes = po._production_sheet_indexes(pair, with_sheet_identity=False)
    for side in ("left", "right"):
        assert all("passport" not in (record.get("content_fingerprint") or {}) for record in indexes[side])


# ---------------------------------------------- flag ON: pass-1 ------------

def test_factless_candidate_never_outranks_an_observed_one(v4_on):
    left = [_sheet(7, functional=_tokens("f", 10), entities=_tokens("e", 10), topology=_tokens("t", 10), title="Общие данные")]
    observed = _sheet(40, functional=_tokens("f", 2), entities=[], topology=[])  # общий словарь — мал, но наблюдаем
    factless = _sheet(7, title="Общие данные")  # тот же номер страницы и тот же заголовок, фактов нет
    result = sm.match_sheets(left, [factless, observed])
    top = [item["right_page"] for item in result["candidate_search"][0]["top_candidates"]]
    assert top[0] == 40, "страница без фактов не должна занимать первое место окна за номер страницы и заголовок"
    assert result["algorithm_version"] == flags.ALGORITHM_V4


def test_pass1_window_is_the_deep_top_k_by_construction(v4_on):
    left = [_sheet(1, functional=_tokens("f", 6), entities=_tokens("e", 6), topology=_tokens("t", 12))]
    rights = []
    for page in range(1, 13):
        # topology — единственное различие: раньше pass-1 её не считал
        rights.append(_sheet(page, functional=_tokens("f", 3), entities=_tokens("e", 3), topology=_tokens("t", page)))
    result = sm.match_sheets(left, rights, top_k=5)
    search = result["candidate_search"][0]
    window = [int(item["right_page"]) for item in search["top_candidates"]]
    deep_scores = {int(item["right_page"]): item["score"] for item in search["deep_candidates"]}
    norm_left = sm.normalize_sheet(left[0], side="LEFT")
    full = sorted(
        (sm._deep(norm_left, sm.normalize_sheet(r, side="RIGHT")) for r in rights),
        key=sm._candidate_sort_key,
    )
    oracle = [int(item["right_page"]) for item in full[:5]]
    assert sorted(window) == sorted(oracle)
    assert all(page in deep_scores for page in oracle)


# --------------------------------------------- flag ON: stamp --------------

@pytest.mark.parametrize(
    "text, axis",
    [
        ("Норм. контр. Корпус 1, 2. Фасад в осях 3.К-1.А М1_200", "3.к-1.а"),
        ("Корпус 4. Фасад в осях 14.1-8.1 М1_200", "14.1-8.1"),
        ("Разрез по осям А-Б", "а-б"),
        ("Корпуса 1, 2. Разрез 1-1", "1-1"),
        ("Фасад 1-19", "1-19"),
    ],
)
def test_facade_and_section_axis_skips_the_preposition(v4_on, text, axis):
    identity = parse_stamp_title(text, page=1)
    assert identity is not None and identity.section_axis == axis


def test_a_bare_preposition_is_not_an_axis(v4_on):
    assert parse_stamp_title("Корпус 1, 2. Фасад в", page=1) is None


def test_distinct_facades_get_distinct_stamp_keys(v4_on):
    keys = {
        parse_stamp_title(text, page=i).stamp_key
        for i, text in enumerate([
            "Корпус 1, 2. Фасад в осях 3.К-1.А",
            "Корпус 1, 2. Фасад в осях 1-19",
            "Корпус 1, 2. Фасад в осях 2.Г-2.И, 6-1",
        ], start=1)
    }
    assert len(keys) == 3


def test_plan_titles_parse_identically_under_both_algorithms():
    text = "Корпуса 1, 2. План 3 этажа"
    assert parse_stamp_title(text, page=1, axis_preposition=False) == parse_stamp_title(text, page=1, axis_preposition=True)


# ------------------------------------------ flag ON: passport --------------

def test_passport_removes_document_wide_terms_and_keeps_page_terms():
    pages = {}
    for page in range(1, 11):
        pages[page] = f"Шумоглушитель круглый Хомут соединительный насос НС{page} котельная-{page} узел крепления ЩР-{page}"
    passports = sp.build_passports(pages)
    assert set(passports) == set(pages)
    for page, fp in passports.items():
        rare = set(fp["rare_terms"])
        assert f"нс{page}" in rare and f"щр-{page}" in rare
        assert "шумоглушитель" not in rare and "хомут" not in rare and "насос" not in rare
        assert fp["passport"]["document_frequency_limit"] == 3


def test_passport_only_adds_facts_and_never_removes_existing_ones():
    records = [
        {"pdf_page": 1, "title": None, "content_fingerprint": {"version": 1, "purpose_terms": ["plan"], "system_names": ["существующий"], "unique_designations": [], "equipment_codes": [], "node_names": [], "section_names": [], "rare_terms": ["существующий"], "structural_tokens": []}},
        {"pdf_page": 2, "title": None},
    ]
    passports = sp.build_passports({1: "новый факт первой страницы", 2: "факт второй страницы"})
    counts = sp.extend_sheet_index(records, passports, source="MARKDOWN_BODY", mode="MERGE")
    assert counts == {"added": 1, "merged": 1, "unchanged": 0}
    assert records[0]["content_fingerprint"]["rare_terms"][0] == "существующий"
    assert records[0]["content_fingerprint"]["purpose_terms"] == ["plan"]
    assert records[1]["content_fingerprint"]["passport"]["source"] == "MARKDOWN_BODY"
    records2 = [dict(records[0]), {"pdf_page": 2, "title": None}]
    records2[0]["content_fingerprint"] = {**records[0]["content_fingerprint"], "rare_terms": ["только-это"]}
    counts2 = sp.extend_sheet_index(records2, passports, source="MARKDOWN_BODY", mode="FALLBACK")
    assert counts2["unchanged"] == 1 and records2[0]["content_fingerprint"]["rare_terms"] == ["только-это"]


def test_passport_service_lines_are_not_facts():
    md = "## Page 1\n### BLOCK #1 [TEXT]: blk_abcdef0123456789abcdef0123456789\n> **Crop:** [Crop](https://x/y)\n> **Stamp:** Code: АА/БЭ | Organization: ООО «НПО ИСП»\nСодержательная строка листа\n"
    bodies = sp.page_bodies_from_markdown(md)
    assert "blk_abcdef" not in bodies[1] and "Crop" not in bodies[1] and "Содержательная" in bodies[1]


def _pair_with_markdown(tmp_path: Path) -> dict:
    """Пара из двух минимальных документов: PDF-заглушка + Markdown с телом страниц."""
    import fitz

    pair = {}
    for side in ("left", "right"):
        folder = tmp_path / side
        folder.mkdir()
        document = fitz.open()
        for _ in range(3):
            document.new_page()
        document.save(str(folder / "document.pdf"))
        document.close()
        (folder / "document.md").write_text(
            "## Page 1\nОбщие данные насос НС1 щит ЩР-1 узел крепления\n"
            "## Page 2\nПлан котельной насос НС2 щит ЩР-2 узел крепления\n"
            "## Page 3\nСхема насос НС3 щит ЩР-3 узел крепления\n",
            encoding="utf-8",
        )
        pair[side] = {"pdf_path": str(folder / "document.pdf"), "md_path": str(folder / "document.md")}
    return pair


def test_v4_index_adds_a_markdown_body_passport(v4_on, tmp_path):
    pair = _pair_with_markdown(tmp_path)
    indexes = po._production_sheet_indexes(pair, with_sheet_identity=False)
    for side in ("left", "right"):
        fingerprints = [record.get("content_fingerprint") for record in indexes[side]]
        assert all(isinstance(fp, dict) for fp in fingerprints), side
        assert all(fp["passport"]["source"] == "MARKDOWN_BODY" for fp in fingerprints)
        # общедокументные термины удалены по частоте, уникальные — остались
        for page, fp in enumerate(fingerprints, 1):
            rare = set(fp["rare_terms"])
            assert f"нс{page}" in rare
            assert "насос" not in rare and "узел" not in rare


def test_v4_index_is_selected_by_argument_not_only_by_flag(v4_off, tmp_path):
    pair = _pair_with_markdown(tmp_path)
    indexes = po._production_sheet_indexes(pair, with_sheet_identity=False, sheet_matcher_v4=True)
    assert all("passport" in (r.get("content_fingerprint") or {}) for r in indexes["left"])
    relations, _ = po._run_sheet_matcher(pair, algorithm=flags.ALGORITHM_V4)
    assert relations["algorithm_version"] == flags.ALGORITHM_V4
    relations_v3, _ = po._run_sheet_matcher(pair)
    assert relations_v3["algorithm_version"] == flags.ALGORITHM_V3


# ---------------------------------------- flag ON: ambiguity guard ---------

def test_high_with_a_dominated_alternative_stays_high(v4_on):
    F, G = _tokens("f", 10), _tokens("g", 10)
    left = [
        _sheet(1, functional=F, entities=F, topology=F),
        _sheet(2, functional=G[:9] + F[:8], entities=G[:9] + F[:8], topology=G[:9] + F[:8]),
    ]
    right = [_sheet(1, functional=F, entities=F, topology=F), _sheet(2, functional=G, entities=G, topology=G)]
    result = sm.match_sheets(left, right)
    matched = {tuple(r["left_pages"]): r for r in result["relations"] if r["left_pages"] and r["right_pages"]}
    assert matched[(1,)]["right_pages"] == [1] and matched[(1,)]["status"] == "HIGH"
    assert matched[(2,)]["right_pages"] == [2] and matched[(2,)]["status"] == "HIGH"
    assert "high_candidate_ambiguous" not in matched[(1,)]["reason_codes"]
    assert result["diagnostics"]["ambiguous_high_demoted"] == 0


def test_high_with_an_undominated_alternative_is_demoted_to_a_question(v4_on):
    F = _tokens("f", 10)
    left = [_sheet(1, functional=F, entities=F, topology=F)]
    right = [_sheet(1, functional=F, entities=F, topology=F), _sheet(9, functional=F, entities=F, topology=F)]
    result = sm.match_sheets(left, right)
    relation = next(r for r in result["relations"] if r["left_pages"] == [1] and r["right_pages"])
    assert relation["status"] == "POSSIBLE"
    assert "high_candidate_ambiguous" in relation["reason_codes"]
    assert any(item["kind"] == "UNDOMINATED_HIGH_ALTERNATIVE" for item in relation["conflicting_evidence"])
    assert result["diagnostics"]["ambiguous_high_demoted"] == 1
    assert relation["provenance"]["algorithm"] == flags.ALGORITHM_V4
    assert sheet_scope_policy.is_pending_confirmation(relation)


def test_a_demoted_pair_becomes_effective_only_through_a_human_decision(v4_on):
    F = _tokens("f", 10)
    left = [_sheet(1, functional=F, entities=F, topology=F)]
    right = [_sheet(1, functional=F, entities=F, topology=F), _sheet(9, functional=F, entities=F, topology=F)]
    relation = next(r for r in sm.match_sheets(left, right)["relations"] if r["left_pages"] == [1] and r["right_pages"])
    assert not sheet_scope_policy.is_effective(relation)
    answered = {**relation, "human_decision": {"decision_id": "d1", "answer": "YES"}}
    assert sheet_scope_policy.is_effective(answered)


# ------------------------------------------- cardinality / determinism -----

@pytest.mark.parametrize("algorithm", [flags.ALGORITHM_V3, flags.ALGORITHM_V4])
def test_stamp_floor_range_still_yields_split_and_merged(algorithm):
    container = {"pdf_page": 1, "title": None, "sheet_identity": {"page": 1, "sheet_kind": "PLAN", "buildings": ["1"], "floors": ["3", "4"], "floor_range": {"from": 3, "to": 4}}}
    members = [
        {"pdf_page": 5, "title": None, "sheet_identity": {"page": 5, "sheet_kind": "PLAN", "buildings": ["1"], "floors": ["3"]}},
        {"pdf_page": 6, "title": None, "sheet_identity": {"page": 6, "sheet_kind": "PLAN", "buildings": ["1"], "floors": ["4"]}},
    ]
    split = sm.match_sheets([container], members, algorithm=algorithm)
    assert any(r["relation_type"] == "SPLIT" and r["primary_source"] == "STAMP_GROUP" for r in split["relations"])
    merged = sm.match_sheets(members, [container], algorithm=algorithm)
    assert any(r["relation_type"] == "MERGED" and r["primary_source"] == "STAMP_GROUP" for r in merged["relations"])


@pytest.mark.parametrize("algorithm", [flags.ALGORITHM_V3, flags.ALGORITHM_V4])
def test_two_replays_are_byte_identical_and_call_no_model(algorithm):
    F, G = _tokens("f", 8), _tokens("g", 8)
    left = [_sheet(1, functional=F, entities=F, topology=F), _sheet(2, functional=G, entities=G, topology=G)]
    right = [_sheet(1, functional=G, entities=G, topology=G), _sheet(2, functional=F, entities=F, topology=F)]
    a = sm.match_sheets(left, right, generated_at="2026-09-06T00:00:00+00:00", algorithm=algorithm)
    b = sm.match_sheets(left, right, generated_at="2026-09-06T00:00:00+00:00", algorithm=algorithm)
    assert json.dumps(a, sort_keys=True, ensure_ascii=False) == json.dumps(b, sort_keys=True, ensure_ascii=False)
    assert a["diagnostics"]["uses_model"] is False


def test_v3_and_v4_signatures_differ_so_stale_artifacts_are_recomputed():
    F = _tokens("f", 8)
    left = [_sheet(1, functional=F, entities=F, topology=F)]
    right = [_sheet(1, functional=F, entities=F, topology=F)]
    a = sm.match_sheets(left, right, algorithm=flags.ALGORITHM_V3)
    b = sm.match_sheets(left, right, algorithm=flags.ALGORITHM_V4)
    assert a["input_signature"] != b["input_signature"]


def test_no_page_or_document_specific_rules_in_the_matcher_sources():
    root = Path(sm.__file__).parent
    forbidden = re.compile(r"(АА_БЭ|13АВ|LEFT\s*\d+|RIGHT\s*\d+|page\s*==\s*\d+|pdf_page\s*==\s*\d+)")
    for name in ("sheet_matcher.py", "sheet_passport.py", "sheet_identity.py", "sheet_matcher_flags.py"):
        source = (root / name).read_text(encoding="utf-8")
        code = "\n".join(line for line in source.splitlines() if not line.strip().startswith("#"))
        assert not forbidden.search(code), name


# -------------------------------------------------------------- shadow -----

def _gate(**env):
    return env


def test_shadow_gate_is_fail_closed_by_default(monkeypatch):
    for name in (FLAG, flags.SHADOW_FLAG, flags.SHADOW_PAIR_ALLOWLIST, flags.SHADOW_RUN_ALLOWLIST):
        monkeypatch.delenv(name, raising=False)
    gate = po._sheet_matcher_v4_shadow_gate(pair_id="p1", run_id="r1", input_mode="DOCUMENT")
    assert gate["allowed"] is False and gate["diagnostic_reason"] == po.SHEET_MATCHER_V4_SHADOW_DISABLED


def test_shadow_gate_requires_an_allowlisted_identifier(monkeypatch):
    monkeypatch.setenv(FLAG, "false")
    monkeypatch.setenv(flags.SHADOW_FLAG, "true")
    monkeypatch.delenv(flags.SHADOW_PAIR_ALLOWLIST, raising=False)
    monkeypatch.delenv(flags.SHADOW_RUN_ALLOWLIST, raising=False)
    # флаг один, списки пустые → никто
    gate = po._sheet_matcher_v4_shadow_gate(pair_id="p1", run_id="r1", input_mode="DOCUMENT")
    assert gate["allowed"] is False and gate["diagnostic_reason"] == po.SHEET_MATCHER_V4_SHADOW_DISABLED
    monkeypatch.setenv(flags.SHADOW_PAIR_ALLOWLIST, "p1,p2")
    assert po._sheet_matcher_v4_shadow_gate(pair_id="p1", run_id="r1", input_mode="DOCUMENT")["allowed"] is True
    other = po._sheet_matcher_v4_shadow_gate(pair_id="p3", run_id="r1", input_mode="DOCUMENT")
    assert other["allowed"] is False and other["diagnostic_reason"] == po.SHEET_MATCHER_V4_SHADOW_PAIR_NOT_ALLOWED
    monkeypatch.setenv(flags.SHADOW_RUN_ALLOWLIST, "r9")
    assert po._sheet_matcher_v4_shadow_gate(pair_id="p3", run_id="r9", input_mode="DOCUMENT")["allowed"] is True
    # PAGE-режим — выбор пользователя, тени нет
    page = po._sheet_matcher_v4_shadow_gate(pair_id="p1", run_id="r1", input_mode="PAGE")
    assert page["allowed"] is False and page["diagnostic_reason"] == po.SHEET_MATCHER_V4_SHADOW_DISABLED


def test_shadow_gate_is_meaningless_when_v4_is_production(monkeypatch):
    monkeypatch.setenv(FLAG, "true")
    monkeypatch.setenv(flags.SHADOW_FLAG, "true")
    monkeypatch.setenv(flags.SHADOW_PAIR_ALLOWLIST, "p1")
    gate = po._sheet_matcher_v4_shadow_gate(pair_id="p1", run_id="r1", input_mode="DOCUMENT")
    assert gate["allowed"] is False and gate["diagnostic_reason"] == po.SHEET_MATCHER_V4_SHADOW_V4_IS_PRODUCTION


def test_shadow_artifact_carries_both_results_and_touches_nothing(v4_off):
    F = _tokens("f", 10)
    left = [_sheet(1, functional=F, entities=F, topology=F)]
    right = [_sheet(1, functional=F, entities=F, topology=F), _sheet(9, functional=F, entities=F, topology=F)]
    production = sm.match_sheets(left, right, generated_at="2026-09-06T00:00:00+00:00")
    before = json.dumps(production, sort_keys=True)
    shadow = sm.match_sheets(left, right, algorithm=flags.ALGORITHM_V4, generated_at="2026-09-06T00:00:00+00:00")
    gate = po._sheet_matcher_v4_shadow_gate(pair_id="p1", run_id="r1", input_mode="DOCUMENT")
    artifact = po.build_sheet_matcher_v4_shadow(
        pair_id="p1", run_id="r1", production_sheet_relations=production,
        shadow_sheet_relations=shadow, gate=gate, generated_at="2026-09-06T00:00:00+00:00",
    )
    assert json.dumps(production, sort_keys=True) == before
    assert artifact["kind"] == po.SHEET_MATCHER_V4_SHADOW_KIND
    assert artifact["affects_production"] is False and artifact["uses_model"] is False
    assert artifact["production"]["algorithm_version"] == flags.ALGORITHM_V3
    assert artifact["shadow"]["algorithm_version"] == flags.ALGORITHM_V4
    assert artifact["production"]["relation_counts"] != artifact["shadow"]["relation_counts"]
    assert artifact["left_page_status_transitions"] == {"HIGH->POSSIBLE": 1}
    assert artifact["sheet_relations"]["algorithm_version"] == flags.ALGORITHM_V4
    assert "sheet_matcher_v4_shadow" in po.production_store.ARTIFACT_PATHS


def test_shadow_run_writes_only_the_diagnostic_artifact(monkeypatch, tmp_path):
    """Тень пишет свой артефакт и заметку в state; боевой sheet_relations не трогает."""
    monkeypatch.setenv(FLAG, "false")
    monkeypatch.setenv(flags.SHADOW_FLAG, "true")
    monkeypatch.setenv(flags.SHADOW_PAIR_ALLOWLIST, "pair-1")
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    pair = _pair_with_markdown(tmp_path)
    production, _ = po._run_sheet_matcher(pair)
    po.production_store.save_artifact("s1", "pair-1", "sheet_relations", production)
    po.production_store.save_artifact("s1", "pair-1", "state", {"run_id": "run-1", "revision": 1})
    before = po.production_store.load_artifact("s1", "pair-1", "sheet_relations")
    diagnostic = po._maybe_run_sheet_matcher_v4_shadow(
        "s1", "pair-1", run_id="run-1", input_mode="DOCUMENT", pair=pair,
        production_sheet_relations=production,
    )
    assert diagnostic is not None and diagnostic["executed"] is True
    assert diagnostic["diagnostic_reason"] == po.SHEET_MATCHER_V4_SHADOW_EXECUTED
    assert po.production_store.load_artifact("s1", "pair-1", "sheet_relations") == before
    artifact = po.production_store.load_artifact("s1", "pair-1", "sheet_matcher_v4_shadow")
    assert artifact["shadow_status"] == "COMPLETED"
    assert artifact["shadow"]["algorithm_version"] == flags.ALGORITHM_V4
    state = po.production_store.load_artifact("s1", "pair-1", "state")
    assert state["sheet_matcher_v4_shadow"]["executed"] is True
    # не в allowlist — ничего не считается и не пишется
    assert po._maybe_run_sheet_matcher_v4_shadow(
        "s1", "pair-2", run_id="run-2", input_mode="DOCUMENT", pair=pair,
        production_sheet_relations=production,
    ) is None
    assert po.production_store.load_artifact("s1", "pair-2", "sheet_matcher_v4_shadow") is None


def test_extract_sheet_identities_accepts_the_axis_rule(tmp_path):
    import fitz

    document = fitz.open()
    document.new_page()
    document.save(str(tmp_path / "empty.pdf"))
    document.close()
    assert extract_sheet_identities(str(tmp_path / "empty.pdf"), axis_preposition=True) == {}
    assert extract_sheet_identities(str(tmp_path / "empty.pdf"), axis_preposition=False) == {}
