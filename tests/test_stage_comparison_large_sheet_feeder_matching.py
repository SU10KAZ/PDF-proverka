"""Tests for large_sheet_feeder_matching (offline consumer/load matching)."""

from __future__ import annotations

import importlib

import pytest

fm = importlib.import_module(
    "backend.app.services.stage_comparison.large_sheet_feeder_matching"
)


def _circ(cid, load_name="", power=None, current=None, cable=None, breaker_params=None):
    return {
        "id": cid,
        "load_name": load_name,
        "calculated_power_kw": power,
        "calculated_current_a": current,
        "cable": cable,
        "breaker_params": breaker_params,
    }


def _pe(circuits):
    return {"schema_version": 1, "circuits": circuits}


def _match(old_circuits, new_circuits):
    olds = fm.extract_feeders(_pe(old_circuits))
    news = fm.extract_feeders(_pe(new_circuits))
    return fm.match_feeders(olds, news)


def _status_for(result, *, old=None, new=None):
    for p in result.pairs:
        if old is not None and (p.old is None or p.old.designation != old):
            continue
        if new is not None and (p.new is None or p.new.designation != new):
            continue
        if old is None and p.old is not None:
            continue
        if new is None and p.new is not None:
            continue
        return p
    return None


# ─── flag ───────────────────────────────────────────────────────────────────

def test_flag_disabled_by_default(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_FEEDER_MATCHING_ENABLED", raising=False)
    assert fm.feeder_matching_enabled() is False


def test_flag_enabled(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_FEEDER_MATCHING_ENABLED", "true")
    assert fm.feeder_matching_enabled() is True


# ─── normalization ──────────────────────────────────────────────────────────

def test_normalize_classifies_known_systems():
    cases = {
        "Подземная автостоянка": "parking",
        "Индивидуальный тепловой пункт": "itp",
        "Шкаф управления холодильным центром": "cooling_center",
        "Насосная АПТ а/ст": "apt",
        "Хозпитьевое водоснабжение": "water_pump",
        "Холодильная машина (чиллер)": "chiller",
        "Резервные баки ГВС": "gvs",
        "АУКРМ №1": "aukrm",
        "Охладитель ДР2-ХМ2": "cooler",  # cooler wins over chiller
    }
    for raw, key in cases.items():
        nc = fm.normalize_consumer(raw)
        assert nc.system_key == key, f"{raw!r} -> {nc.system_key} (expected {key})"


def test_normalize_unit_extraction():
    assert fm.normalize_consumer("Холодильная машина ХМ2").unit == 2
    assert fm.normalize_consumer("АУКРМ №1").unit == 1
    assert fm.normalize_consumer("ВРУ3").unit == 3
    assert fm.normalize_consumer("Охладитель ДР1-ХМ1").unit == 1  # cooler unit = ДР1


def test_normalize_falls_back_to_designation_when_load_empty():
    nc = fm.normalize_consumer("", "ВРУ3")
    assert nc.system_key == "vru_input"
    assert nc.unit == 3


def test_normalize_latin_ocr_transliteration():
    assert fm.normalize_consumer("EB-GVS", "EB-GVS").system_key == "gvs"
    assert fm.normalize_consumer("XM2", "XM2").system_key == "chiller"


# ─── matching: core scenarios ───────────────────────────────────────────────

def test_exact_consumer_match():
    r = _match(
        [_circ("ВРУ3", "ВРУ3", power=143.2)],
        [_circ("ГРЩ1-РП2-3", "ВРУ3 - Ввод 2 Корпус 3", power=140.9)],
    )
    p = _status_for(r, old="ВРУ3", new="ГРЩ1-РП2-3")
    assert p is not None
    assert p.status in ("matched_high_confidence", "matched_medium_confidence")


def test_synonym_consumer_match():
    # «Шкаф упр. насосов хладоцентра» ↔ «Шкаф управления холодильным центром»
    r = _match(
        [_circ("ШУ-ХЦ", "Шкаф упр. насосов хладоцентра", power=27.5)],
        [_circ("ГРЩ1-РП1-7", "Шкаф управления холодильным центром", power=75.8)],
    )
    p = _status_for(r, old="ШУ-ХЦ", new="ГРЩ1-РП1-7")
    assert p is not None
    assert p.status.startswith("matched") or p.status == "ambiguous"


def test_renamed_feeder_same_consumer_matches():
    # designation полностью переименована, потребитель тот же → должно матчиться
    r = _match(
        [_circ("ВРУ4", "ВРУ4", power=233.6, current=387.2)],
        [_circ("ГРЩ1-РП2-4", "ВРУ4 - Ввод 2 Корпус 4", power=190.6, current=296.7)],
    )
    p = _status_for(r, old="ВРУ4", new="ГРЩ1-РП2-4")
    assert p is not None and p.status.startswith("matched")


def test_designation_rename_does_not_block_match():
    # имена щитов совсем разные, но та же нагрузка/система
    r = _match(
        [_circ("XYZ-OLD-1", "Насосная АПТ", power=22.5)],
        [_circ("TOTALLY-NEW-99", "Насосная АПТ а/ст", power=14.0)],
    )
    p = _status_for(r, old="XYZ-OLD-1", new="TOTALLY-NEW-99")
    assert p is not None and p.status.startswith("matched")


def test_xm_to_chiller_match():
    r = _match(
        [_circ("ХМ1", "Холодильная машина ХМ1", power=157.5)],
        [_circ("ГРЩ1-РП1-12", "Холодильная машина (чиллер)", power=676.8)],
    )
    p = _status_for(r, old="ХМ1", new="ГРЩ1-РП1-12")
    assert p is not None and p.old.nc.system_key == "chiller"
    assert p.new.nc.system_key == "chiller"
    # >2x рост мощности должен попасть в suspected_change
    assert "нагрузк" in p.suspected_change.lower()


def test_cooling_center_synonyms():
    r = _match(
        [_circ("ШУ-ХЦ", "хладоцентр насосы", power=27.5)],
        [_circ("РП-7", "холодильный центр", power=37.5)],
    )
    p = _status_for(r, old="ШУ-ХЦ", new="РП-7")
    assert p is not None and p.status.startswith("matched") or p and p.status == "ambiguous"


def test_gvs_tanks_synonym():
    r = _match(
        [_circ("ЭБ-ГВС", "Резервные баки ГВС ЭБ-ГВС", power=60)],
        [_circ("ГРЩ1-РП1-14", "Резервные баки ГВС", power=193.3)],
    )
    p = _status_for(r, old="ЭБ-ГВС", new="ГРЩ1-РП1-14")
    assert p is not None and p.old.nc.system_key == "gvs" and p.new.nc.system_key == "gvs"


def test_apt_match():
    r = _match(
        [_circ("ШУ-АПТ", "Насосная АПТ", power=22.5)],
        [_circ("ГРЩ1-РП1-8", "Насосная АПТ а/ст", power=14.0)],
    )
    p = _status_for(r, old="ШУ-АПТ", new="ГРЩ1-РП1-8")
    assert p is not None and p.status.startswith("matched")


def test_water_pump_nst_hvs_match():
    r = _match(
        [_circ("ШУ-ХВС", "Насосная ХП+против. водопровод.", power=15.0)],
        [_circ("ГРЩ1-РП1-9", "Хозпитьевое водоснабжение", power=16.4)],
    )
    p = _status_for(r, old="ШУ-ХВС", new="ГРЩ1-РП1-9")
    assert p is not None and p.old.nc.system_key == "water_pump"
    assert p.new.nc.system_key == "water_pump"


def test_old_only():
    r = _match(
        [_circ("OLD-DROP", "Наружное освещение ЩНО", power=2.0)],
        [_circ("NEW-X", "Подземная автостоянка", power=103.9)],
    )
    p = _status_for(r, old="OLD-DROP", new=None)
    assert p is not None and p.status == "old_only"


def test_new_only():
    r = _match(
        [_circ("OLD-X", "Подземная автостоянка", power=103.9)],
        [_circ("NEW-ADD", "Собственные нужды ТП", power=5.0)],
    )
    p = _status_for(r, old=None, new="NEW-ADD")
    assert p is not None and p.status == "new_only"


def test_ambiguous_two_identical_consumers():
    # NEW укрупнённый чиллер (unit None) ↔ два OLD ХМ1/ХМ2 с одинаковой мощностью
    r = _match(
        [_circ("ХМ1", "Холодильная машина ХМ1", power=157.5),
         _circ("ХМ2", "Холодильная машина ХМ2", power=157.5)],
        [_circ("РП-12", "Холодильная машина (чиллер)", power=157.5)],
    )
    statuses = [p.status for p in r.pairs if p.new and p.new.designation == "РП-12"]
    # выбранная пара должна быть помечена ambiguous (две неотличимые OLD-нагрузки)
    assert "ambiguous" in statuses


def test_different_consumer_no_high_confidence():
    # АПТ (OLD) vs ИТП (NEW) — разные потребители, high-confidence запрещён
    r = _match(
        [_circ("ШУ-АПТ", "Насосная АПТ", power=22.5)],
        [_circ("РП-6", "Индивидуальный тепловой пункт", power=75.8)],
    )
    for p in r.pairs:
        if p.old and p.new:
            assert p.status != "matched_high_confidence"


def test_different_unit_not_high_confidence():
    # ВРУ2 vs ВРУ3 — разные корпуса/вводы, не должны слипнуться в high
    r = _match(
        [_circ("ВРУ2", "ВРУ2", power=25.4)],
        [_circ("РП-3", "ВРУ3 - Ввод 2 Корпус 3", power=143.2)],
    )
    p = _status_for(r, old="ВРУ2", new="РП-3")
    # либо нет пары (раскинуты в old_only/new_only), либо точно не high
    assert p is None or p.status != "matched_high_confidence"


# ─── report / render ────────────────────────────────────────────────────────

def test_report_and_render_section():
    r = _match(
        [_circ("ВРУ4", "ВРУ4", power=233.6, current=387.2, cable="2x(5x120)")],
        [_circ("ГРЩ1-РП2-4", "ВРУ4 - Ввод 2 Корпус 4", power=190.6, current=296.7,
               cable="3хППГнг(А)-HF 5х120мм²")],
    )
    report = fm.build_feeder_match_report(r, meta={"pair": "ptest"})
    assert report["schema_version"] == 1
    assert report["summary"]["old_circuits"] == 1
    assert report["rows"] and report["rows"][0]["consumer_key"] == "vru_input_4"
    md = fm.render_feeder_match_md_section(r)
    assert "Сопоставление фидеров по потребителю/нагрузке" in md
    assert "ВРУ4" in md and "ГРЩ1-РП2-4" in md
    assert "|" in md  # markdown table


def test_run_offline_returns_triplet():
    old = _pe([_circ("ВРУ1", "ВРУ1", power=365.7)])
    new = _pe([_circ("ГРЩ1-РП1-1", "ВРУ1", power=450.0)])
    result, report, md = fm.run_offline_feeder_match(old, new)
    assert isinstance(report, dict)
    assert isinstance(md, str) and md
    assert result.summary["old_circuits"] == 1


def test_feeder_section_for_pair():
    old = _pe([_circ("ВРУ4", "ВРУ4", power=233.6, cable="2x(5x120)")])
    new = _pe([_circ("ГРЩ1-РП2-4", "ВРУ4 - Ввод 2 Корпус 4", power=190.6,
                     cable="3хППГнг(А)-HF 5х120мм²")])
    section = fm.feeder_section_for_pair(old, new)
    assert "Сопоставление фидеров по потребителю/нагрузке" in section
    assert "ВРУ4" in section and "ГРЩ1-РП2-4" in section


def test_feeder_section_empty_when_no_circuits():
    assert fm.feeder_section_for_pair({}, _pe([_circ("X", "ВРУ1")])) == ""
    assert fm.feeder_section_for_pair(_pe([_circ("X", "ВРУ1")]), {}) == ""


# ─── candidate changes ───────────────────────────────────────────────────────

def _candidates(old_circuits, new_circuits):
    r = _match(old_circuits, new_circuits)
    return fm.build_feeder_candidate_changes(r)


def test_candidate_changes_flag_disabled_by_default(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_LARGE_SHEET_FEEDER_CANDIDATE_CHANGES_ENABLED", raising=False)
    assert fm.feeder_candidate_changes_enabled() is False


def test_candidate_changes_flag_enabled(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_LARGE_SHEET_FEEDER_CANDIDATE_CHANGES_ENABLED", "true")
    assert fm.feeder_candidate_changes_enabled() is True


def test_candidate_section_off_by_default_in_feeder_md():
    # feeder_md_for_pair без include_candidates → нет секции кандидатов
    old = _pe([_circ("ВРУ4", "ВРУ4", power=233.6, current=387.2, cable="2x(5x120)")])
    new = _pe([_circ("ГРЩ1-РП2-4", "ВРУ4 - Ввод 2 Корпус 4", power=190.6, current=296.7,
                     cable="3хППГнг(А)-HF 5х120мм²")])
    md = fm.feeder_md_for_pair(old, new, include_candidates=False)
    assert "Сопоставление фидеров по потребителю/нагрузке" in md
    assert "Кандидаты пофидерных изменений" not in md


def test_candidate_section_appears_when_enabled():
    old = _pe([_circ("ВРУ4", "ВРУ4", power=233.6, current=387.2, cable="2x(5x120)")])
    new = _pe([_circ("ГРЩ1-РП2-4", "ВРУ4 - Ввод 2 Корпус 4", power=190.6, current=296.7,
                     cable="3хППГнг(А)-HF 5х120мм²")])
    md = fm.feeder_md_for_pair(old, new, include_candidates=True)
    assert "Кандидаты пофидерных изменений из таблицы сопоставления" in md
    assert "FEEDER_CANDIDATE_CHANGES" in md           # prompt signal
    assert "recommended_finding_title" in md           # table header


def test_candidate_high_medium_with_cable_delta():
    cands = _candidates(
        [_circ("ВРУ-ХЦ", "Шкаф управления холодильным центром", power=37.5, cable="5x70")],
        [_circ("ГРЩ1-РП1-7", "Шкаф управления холодильным центром", power=37.5, cable="5x120")],
    )
    cc = [c for c in cands if c.consumer_key == "cooling_center"]
    assert cc and "cable_changed" in cc[0].detected_delta
    assert cc[0].confidence > 0 and cc[0].recommended_finding_title


def test_candidate_power_current_delta():
    cands = _candidates(
        [_circ("ЭБ-ГВС", "Резервные баки ГВС", power=60, current=91.3)],
        [_circ("ГРЩ1-РП1-14", "Резервные баки ГВС", power=193.3, current=193.3)],
    )
    cc = [c for c in cands if c.consumer_key == "gvs"]
    assert cc
    assert "power_changed" in cc[0].detected_delta or "current_changed" in cc[0].detected_delta
    assert cc[0].requires_human_review is False


def test_candidate_parallel_lines_delta():
    cands = _candidates(
        [_circ("ВРУ4", "ВРУ4", power=233.6, cable="2x(5x120)")],
        [_circ("ГРЩ1-РП2-4", "ВРУ4 - Ввод 2 Корпус 4", power=233.6, cable="3хППГнг(А)-HF 5х120мм²")],
    )
    cc = [c for c in cands if c.consumer_key == "vru_input_4"]
    assert cc and "parallel_lines_changed" in cc[0].detected_delta


def test_candidate_ambiguous_goes_to_human_review():
    # два неотличимых OLD ХМ → ambiguous → requires_human_review, не firm
    cands = _candidates(
        [_circ("ХМ1", "Холодильная машина ХМ1", power=157.5),
         _circ("ХМ2", "Холодильная машина ХМ2", power=157.5)],
        [_circ("ГРЩ1-РП1-12", "Холодильная машина (чиллер)", power=676.8)],
    )
    ch = [c for c in cands if c.consumer_key.startswith("chiller")]
    assert ch
    assert all(c.requires_human_review for c in ch)
    assert not any(c.requires_human_review is False and c.kind == "matched" and
                   c.consumer_key.startswith("chiller") for c in cands)


def test_candidate_no_engineering_delta_no_candidate():
    # идентичные нагрузка/кабель → нет дельты → нет кандидата
    cands = _candidates(
        [_circ("ВРУ3", "ВРУ3", power=143.2, current=246.6, cable="2x(5x95)")],
        [_circ("ГРЩ1-РП2-3", "ВРУ3 - Ввод 2 Корпус 3", power=143.2, current=246.6, cable="2x(5x95)")],
    )
    assert [c for c in cands if c.consumer_key == "vru_input_3"] == []


def test_candidate_duplicate_old_only_no_false_finding():
    # потребитель уже matched + дублирующее OLD-представление → не плодим
    # ложный feeder_removed (rule 5)
    cands = _candidates(
        [_circ("ШУ-АПТ", "Насосная АПТ", power=22.5, current=52.7),
         _circ("1ГРЩ-ЩУ.АПТ", "ЩУ.АПТ", current=52.7)],   # дубль укрупнённого представления
        [_circ("ГРЩ1-РП1-8", "Насосная АПТ а/ст", power=14.0, current=14.0)],
    )
    apt = [c for c in cands if c.consumer_key == "apt"]
    # есть matched-кандидат, но НЕ должно быть feeder_removed для apt
    assert apt
    assert not any("feeder_removed" in c.detected_delta for c in apt)


def test_candidate_added_feeder_is_human_review():
    # новый потребитель без пары → feeder_added, но как requires_human_review
    cands = _candidates(
        [_circ("ВРУ1", "ВРУ1", power=365.7)],
        [_circ("ГРЩ1-РП1-1", "ВРУ1", power=365.7),
         _circ("ГРЩ1-РП1-15", "АУКРМ №1", power=272.7, cable="5x185")],
    )
    added = [c for c in cands if "feeder_added" in c.detected_delta]
    assert added and all(c.requires_human_review for c in added)


def test_candidate_render_empty_when_no_candidates():
    r = _match(
        [_circ("ВРУ3", "ВРУ3", power=143.2, current=246.6, cable="2x(5x95)")],
        [_circ("ГРЩ1-РП2-3", "ВРУ3 - Ввод 2 Корпус 3", power=143.2, current=246.6, cable="2x(5x95)")],
    )
    assert fm.render_feeder_candidate_changes_md_section(r) == ""
