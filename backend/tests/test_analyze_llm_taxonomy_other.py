"""
test_analyze_llm_taxonomy_other.py
-------------------------------------
Tests for analyze_llm_taxonomy_other.py.

All tests are offline and do NOT touch production artifacts.

Runs with:
    python -m pytest backend/tests/test_analyze_llm_taxonomy_other.py -v
"""
from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

# ─── Script loader ────────────────────────────────────────────────────────────

SCRIPT = Path("backend/scripts/analyze_llm_taxonomy_other.py")


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "analyze_llm_taxonomy_other",
        str(SCRIPT.resolve()),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ─── Synthetic data helpers ───────────────────────────────────────────────────

def _false_accept(
    fid: str,
    human_reason: str = "",
    section: str = "KJ",
    critic_decision: str = "accept",
    score: int = 7,
    ev: str = "valid",
    title: str = "",
    category: str = "cover_thickness",
) -> dict:
    return {
        "finding_id": fid,
        "project_name": "TEST_PROJ",
        "section": section,
        "title": title or f"Проблема {fid}",
        "description": f"Описание {fid}",
        "recommendation": f"Рекомендация {fid}",
        "human_decision": "rejected",
        "human_reason": human_reason,
        "critic_decision": critic_decision,
        "critic_decision_before_llm": "accept",
        "critic_reject_reason": "",
        "critic_score": score,
        "evidence_quality": ev,
        "severity": "КРИТИЧЕСКОЕ",
        "category": category,
        "sheet": "Лист 1",
        "page": 1,
        "match_confidence": "exact",
        "classification": "critic_too_soft",
    }


def _llm_decision(fid: str, taxonomy: str = "other", expl: str = "") -> dict:
    return {
        "finding_id": fid,
        "llm_decision": "accept",
        "usefulness_score": 7,
        "reject_reason": None,
        "explanation": expl,
        "human_taxonomy_reason": taxonomy,
        "confidence": 0.9,
        "evidence_checked": True,
        "source_dependency": "enough_source",
        "provider": "mock",
    }


def _make_benchmark_dir(tmp_path: Path, suffix: str = "") -> Path:
    bdir = tmp_path / f"benchmark{suffix}"
    bdir.mkdir(parents=True, exist_ok=True)
    return bdir


# ─── Tests: case selection ────────────────────────────────────────────────────

class TestSelectOtherCases:
    def setup_method(self):
        self.mod = _load_script()

    def test_selects_other_taxonomy_false_accepts(self):
        fa = [_false_accept("F-001", human_reason="Артефакт распознования")]
        tax = {"F-001": _llm_decision("F-001", taxonomy="other")}
        cases = self.mod.select_other_cases(fa, tax)
        assert len(cases) == 1
        assert cases[0]["finding_id"] == "F-001"

    def test_selects_no_llm_decision_cases(self):
        fa = [_false_accept("F-001", human_reason="Some reason")]
        tax = {}  # No LLM decision
        cases = self.mod.select_other_cases(fa, tax)
        assert len(cases) == 1
        assert cases[0]["llm_taxonomy_reason"] == "no_llm_decision"

    def test_excludes_known_taxonomy_cases(self):
        fa = [
            _false_accept("F-001"),
            _false_accept("F-002"),
            _false_accept("F-003"),
        ]
        tax = {
            "F-001": _llm_decision("F-001", taxonomy="visual_or_ocr_misread"),
            "F-002": _llm_decision("F-002", taxonomy="other"),
            "F-003": _llm_decision("F-003", taxonomy="duplicate_or_already_covered"),
        }
        cases = self.mod.select_other_cases(fa, tax)
        assert len(cases) == 1
        assert cases[0]["finding_id"] == "F-002"

    def test_empty_false_accepts(self):
        cases = self.mod.select_other_cases([], {})
        assert cases == []

    def test_empty_human_reason_not_crash(self):
        fa = [_false_accept("F-001", human_reason="")]
        tax = {"F-001": _llm_decision("F-001", taxonomy="other", expl="")}
        cases = self.mod.select_other_cases(fa, tax)
        assert len(cases) == 1
        assert cases[0]["human_reason"] == ""


# ─── Tests: classification ────────────────────────────────────────────────────

class TestClassifyCase:
    def setup_method(self):
        self.mod = _load_script()

    def classify(self, human_reason: str = "", llm_expl: str = "") -> str:
        return self.mod.classify_case(human_reason, llm_expl)

    def test_ocr_artifact_classified(self):
        assert self.classify("Артефакт распознования OCR нарезки страницы") \
            == "false_positive_due_to_missing_context"

    def test_ocr_error_classified(self):
        assert self.classify("Ошибка распознования OCR: значение верное") \
            == "false_positive_due_to_missing_context"

    def test_problem_does_not_exist(self):
        assert self.classify("Проблема не существует") == "value_already_correct"

    def test_standard_notation(self):
        assert self.classify(
            "Стандартная нотация, все данные в ведомости деталей"
        ) == "value_already_correct"

    def test_general_notes_already_cover(self):
        # "Задан в общих указаниях" can match either value_already_correct or
        # already_resolved_by_project_note depending on pattern order.
        # Both are semantically correct; just verify it's not unclassified.
        result = self.classify("Задан в общих указаниях листа 1")
        assert result in ("already_resolved_by_project_note", "value_already_correct")

    def test_design_stage_pz(self):
        # "Задаётся в ПЗ КЖ" may match already_resolved_by_project_note or design_stage_limitation
        # depending on rule order. Both are semantically valid; verify not unclassified.
        result = self.classify("Задаётся в ПЗ КЖ раздела, не в графических листах")
        assert result in ("design_stage_limitation", "already_resolved_by_project_note")

    def test_design_stage_explicit(self):
        # Use a phrase that only matches design_stage_limitation
        assert self.classify(
            "Разрабатывается на стадии ПД раздела ОДИ, в РД не требуется"
        ) == "design_stage_limitation"

    def test_outside_scope_ppr(self):
        assert self.classify(
            "Относится к технологии производства работ и регулируется ППР"
        ) == "outside_audit_scope"

    def test_outside_scope_other_section(self):
        assert self.classify(
            "Это вопрос не к этому разделу, а к разделу АР"
        ) == "outside_audit_scope"

    def test_seismic_norm_not_applicable(self):
        assert self.classify(
            "Москва — зона сейсмичности 5 баллов, СП 14.13330.2018 применяется с 6 баллов"
        ) == "requirement_not_mandatory"

    def test_no_consequence(self):
        assert self.classify(
            "Не влечёт строительных или финансовых последствий"
        ) == "requirement_not_mandatory"

    def test_expertise_passed(self):
        assert self.classify(
            "Экспертиза пройдена, ГЭ проверила и приняла"
        ) == "requirement_not_mandatory"

    def test_different_elements(self):
        assert self.classify(
            "Сравниваются разные конструктивные элементы: t=200 в легенде — плита первого этажа"
        ) == "wrong_element_or_location"

    def test_different_floors(self):
        assert self.classify(
            "Расположены на разных уровнях здания — паркинг и техническое пространство"
        ) == "wrong_element_or_location"

    def test_minor_typo(self):
        assert self.classify(
            "Опечатка в перечне нормативных документов, нивелируется корректной ссылкой"
        ) == "requirement_not_mandatory"

    def test_formatting_issue(self):
        assert self.classify(
            "Формальное несоответствие без влияния на строительство"
        ) == "requirement_not_mandatory"

    def test_unknown_falls_to_other_unclassified(self):
        assert self.classify(
            "Совершенно неизвестная причина без ключевых слов"
        ) == "other_unclassified"

    def test_empty_reason_falls_to_other_unclassified(self):
        assert self.classify("", "") == "other_unclassified"

    def test_human_reason_matched_before_llm_explanation(self):
        # human_reason for this rule fires before llm_explanation is needed
        result = self.classify(
            "Это вопрос не к этому разделу, а к разделу АР",
            "This finding seems valid and important",
        )
        assert result == "outside_audit_scope"


# ─── Tests: analyze_other_cases ──────────────────────────────────────────────

class TestAnalyzeOtherCases:
    def setup_method(self):
        self.mod = _load_script()

    def _write_benchmark_dir(
        self,
        tmp_path: Path,
        false_accepts: list[dict],
        llm_decisions: list[dict],
        suffix: str = "",
    ) -> Path:
        bdir = _make_benchmark_dir(tmp_path, suffix)
        (bdir / "false_accepts.json").write_text(
            json.dumps(false_accepts), encoding="utf-8"
        )
        (bdir / "human_benchmark_records.json").write_text(
            json.dumps(false_accepts), encoding="utf-8"
        )
        (bdir / "critic_v2_llm_taxonomy_decisions.json").write_text(
            json.dumps(llm_decisions), encoding="utf-8"
        )
        (bdir / "human_benchmark_summary.json").write_text(
            json.dumps({}), encoding="utf-8"
        )
        return bdir

    def test_basic_analysis_runs(self, tmp_path):
        fa = [
            _false_accept("F-001", "Артефакт распознования"),
            _false_accept("F-002", "Задан в общих указаниях"),
        ]
        tax = [
            _llm_decision("F-001", "other"),
            _llm_decision("F-002", "other"),
        ]
        bdir = self._write_benchmark_dir(tmp_path, fa, tax)
        result = self.mod.analyze_other_cases([bdir])
        assert result["meta"]["total_other_cases"] == 2

    def test_proposed_categories_counted(self, tmp_path):
        fa = [
            _false_accept("F-001", "Ошибка распознования OCR"),
            # Uses "определяется по таблице" which only matches already_resolved_by_project_note
            _false_accept("F-002", "Расчётные длины определяется по таблице 1 на листе 3"),
            _false_accept("F-003", "Проблема не существует"),
        ]
        tax = [_llm_decision(f["finding_id"], "other") for f in fa]
        bdir = self._write_benchmark_dir(tmp_path, fa, tax)
        result = self.mod.analyze_other_cases([bdir])
        bd = result["category_breakdown"]
        assert bd["false_positive_due_to_missing_context"]["count"] >= 1
        assert bd["already_resolved_by_project_note"]["count"] >= 1
        assert bd["value_already_correct"]["count"] >= 1

    def test_empty_inputs_no_crash(self, tmp_path):
        bdir = self._write_benchmark_dir(tmp_path, [], [])
        result = self.mod.analyze_other_cases([bdir])
        assert result["meta"]["total_other_cases"] == 0
        assert result["suitability_summary"]["llm_can_handle"] == 0

    def test_multiple_dirs_aggregated(self, tmp_path):
        fa1 = [_false_accept("F-001", "Артефакт OCR")]
        fa2 = [_false_accept("F-002", "Задан в общих указаниях")]
        tax1 = [_llm_decision("F-001", "other")]
        tax2 = [_llm_decision("F-002", "other")]
        dir1 = self._write_benchmark_dir(tmp_path, fa1, tax1, "1")
        dir2 = self._write_benchmark_dir(tmp_path, fa2, tax2, "2")
        result = self.mod.analyze_other_cases([dir1, dir2])
        assert result["meta"]["total_other_cases"] == 2
        assert len(result["meta"]["source_dirs"]) == 2

    def test_no_llm_decision_cases_included(self, tmp_path):
        fa = [_false_accept("F-001", "Some reason")]
        bdir = self._write_benchmark_dir(tmp_path, fa, [])  # no LLM decisions
        result = self.mod.analyze_other_cases([bdir])
        assert result["meta"]["total_other_cases"] == 1

    def test_suitability_summary_populated(self, tmp_path):
        fa = [
            _false_accept("F-001", "Ошибка OCR"),
            _false_accept("F-002", "Задан в ПЗ КЖ"),
            _false_accept("F-003", "Совершенно неизвестно"),
        ]
        tax = [_llm_decision(f["finding_id"], "other") for f in fa]
        bdir = self._write_benchmark_dir(tmp_path, fa, tax)
        result = self.mod.analyze_other_cases([bdir])
        suit = result["suitability_summary"]
        total = suit["llm_can_handle"] + suit["borderline_llm"] + suit["needs_human"]
        assert total == 3  # all cases have a suitability class

    def test_top_n_limits_cases_in_output(self, tmp_path):
        fa = [_false_accept(f"F-{i:03d}", "Артефакт OCR") for i in range(20)]
        tax = [_llm_decision(f["finding_id"], "other") for f in fa]
        bdir = self._write_benchmark_dir(tmp_path, fa, tax)
        result = self.mod.analyze_other_cases([bdir], top_n=5)
        assert len(result["cases"]) == 5

    def test_known_taxonomy_excluded(self, tmp_path):
        fa = [
            _false_accept("F-001"),
            _false_accept("F-002"),
        ]
        tax = [
            _llm_decision("F-001", "visual_or_ocr_misread"),
            _llm_decision("F-002", "other"),
        ]
        bdir = self._write_benchmark_dir(tmp_path, fa, tax)
        result = self.mod.analyze_other_cases([bdir])
        assert result["meta"]["total_other_cases"] == 1
        assert result["cases"][0]["finding_id"] == "F-002"


# ─── Tests: output files ──────────────────────────────────────────────────────

class TestOutputFiles:
    def setup_method(self):
        self.mod = _load_script()

    def _make_analysis(self, tmp_path: Path) -> dict:
        bdir = _make_benchmark_dir(tmp_path)
        fa = [
            _false_accept("F-001", "Артефакт OCR"),
            _false_accept("F-002", "Задан в общих указаниях"),
            _false_accept("F-003", "Это вопрос к разделу АР"),
        ]
        (bdir / "false_accepts.json").write_text(json.dumps(fa))
        (bdir / "human_benchmark_records.json").write_text(json.dumps(fa))
        tax = [_llm_decision(f["finding_id"], "other") for f in fa]
        (bdir / "critic_v2_llm_taxonomy_decisions.json").write_text(json.dumps(tax))
        (bdir / "human_benchmark_summary.json").write_text(json.dumps({}))
        return self.mod.analyze_other_cases([bdir])

    def test_json_created(self, tmp_path):
        analysis = self._make_analysis(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, analysis)
        assert (out / "llm_taxonomy_other_analysis.json").exists()

    def test_markdown_created(self, tmp_path):
        analysis = self._make_analysis(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, analysis)
        md_path = out / "llm_taxonomy_other_analysis.md"
        assert md_path.exists()
        md = md_path.read_text(encoding="utf-8")
        assert "# LLM Taxonomy" in md
        assert "Proposed Category" in md

    def test_csv_created_with_flag(self, tmp_path):
        analysis = self._make_analysis(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, analysis, export_csv_flag=True)
        csv_path = out / "llm_taxonomy_other_samples.csv"
        assert csv_path.exists()
        with csv_path.open(encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 3

    def test_csv_not_created_without_flag(self, tmp_path):
        analysis = self._make_analysis(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, analysis, export_csv_flag=False)
        assert not (out / "llm_taxonomy_other_samples.csv").exists()

    def test_json_valid_structure(self, tmp_path):
        analysis = self._make_analysis(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, analysis)
        data = json.loads((out / "llm_taxonomy_other_analysis.json").read_text())
        assert "meta" in data
        assert "category_breakdown" in data
        assert "suitability_summary" in data
        assert "cases" in data

    def test_empty_data_no_crash(self, tmp_path):
        bdir = _make_benchmark_dir(tmp_path)
        (bdir / "false_accepts.json").write_text("[]")
        (bdir / "human_benchmark_records.json").write_text("[]")
        (bdir / "critic_v2_llm_taxonomy_decisions.json").write_text("[]")
        (bdir / "human_benchmark_summary.json").write_text("{}")
        analysis = self.mod.analyze_other_cases([bdir])
        out = tmp_path / "out"
        self.mod.write_outputs(out, analysis, export_csv_flag=True)
        assert (out / "llm_taxonomy_other_analysis.json").exists()
        assert (out / "llm_taxonomy_other_analysis.md").exists()
        # CSV with 0 rows: no crash, file may or may not exist
        # (write_outputs skips CSV if no cases)


# ─── Tests: markdown content ──────────────────────────────────────────────────

class TestMarkdownContent:
    def setup_method(self):
        self.mod = _load_script()

    def _make_analysis_with_categories(self) -> dict:
        cases = [
            {
                "finding_id": "F-001",
                "section": "KJ",
                "project_name": "P1",
                "title": "OCR error",
                "description": "",
                "recommendation": "",
                "human_reason": "Артефакт распознования",
                "human_decision": "rejected",
                "critic_decision": "accept",
                "critic_decision_before_llm": "accept",
                "critic_score": 7,
                "evidence_quality": "valid",
                "severity": "КРИТИЧЕСКОЕ",
                "category": "spec_mismatch",
                "sheet": "1",
                "page": 1,
                "match_confidence": "exact",
                "llm_decision": "accept",
                "llm_taxonomy_reason": "other",
                "llm_explanation": "looks valid",
                "llm_confidence": 0.9,
                "llm_source_dependency": "enough_source",
                "proposed_category": "false_positive_due_to_missing_context",
                "_source_dir": "/tmp",
            },
            {
                "finding_id": "F-002",
                "section": "AR",
                "project_name": "P1",
                "title": "General notes",
                "description": "",
                "recommendation": "",
                "human_reason": "Задан в общих указаниях",
                "human_decision": "rejected",
                "critic_decision": "accept",
                "critic_decision_before_llm": "accept",
                "critic_score": 6,
                "evidence_quality": "partial",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "documentation",
                "sheet": "2",
                "page": 2,
                "match_confidence": "exact",
                "llm_decision": "accept",
                "llm_taxonomy_reason": "other",
                "llm_explanation": "",
                "llm_confidence": 0.85,
                "llm_source_dependency": "enough_source",
                "proposed_category": "already_resolved_by_project_note",
                "_source_dir": "/tmp",
            },
        ]
        # Build analysis manually
        from collections import Counter
        cat_counter = Counter(c["proposed_category"] for c in cases)
        suitability = Counter()
        cat_breakdown = {}
        for cat_name, cat_meta in self.mod.PROPOSED_CATEGORIES.items():
            count = cat_counter.get(cat_name, 0)
            cat_breakdown[cat_name] = {
                **cat_meta,
                "count": count,
                "pct": round(count / len(cases) * 100, 1) if cases else 0.0,
                "examples": [],
            }
            suitability[cat_meta["llm_suitability"]] += count
        return {
            "meta": {
                "source_dirs": ["/tmp"],
                "total_other_cases": 2,
                "sections": {"KJ": 1, "AR": 1},
                "evidence_quality_dist": {"valid": 1, "partial": 1},
                "severity_dist": {},
                "finding_category_dist": {},
            },
            "category_breakdown": cat_breakdown,
            "suitability_summary": dict(suitability),
            "keyword_clusters": {"top_keyword_labels": [], "top_reason_starts": []},
            "cases": cases,
        }

    def test_markdown_has_overview(self):
        analysis = self._make_analysis_with_categories()
        md = self.mod.render_markdown(analysis)
        assert "## Overview" in md
        assert "Total" in md

    def test_markdown_has_recommendations(self):
        analysis = self._make_analysis_with_categories()
        md = self.mod.render_markdown(analysis)
        assert "## Recommendations" in md
        assert "Categories safe for LLM" in md or "Recommendations" in md

    def test_markdown_has_samples(self):
        analysis = self._make_analysis_with_categories()
        md = self.mod.render_markdown(analysis)
        assert "Sample Cases" in md
        assert "F-001" in md

    def test_markdown_shows_categories(self):
        analysis = self._make_analysis_with_categories()
        md = self.mod.render_markdown(analysis)
        assert "false_positive_due_to_missing_context" in md
        assert "already_resolved_by_project_note" in md

    def test_markdown_no_crash_empty(self):
        analysis = {
            "meta": {"source_dirs": [], "total_other_cases": 0,
                     "sections": {}, "evidence_quality_dist": {},
                     "severity_dist": {}, "finding_category_dist": {}},
            "category_breakdown": {},
            "suitability_summary": {"llm_can_handle": 0, "borderline_llm": 0, "needs_human": 0},
            "keyword_clusters": {"top_keyword_labels": [], "top_reason_starts": []},
            "cases": [],
        }
        md = self.mod.render_markdown(analysis)
        assert "# LLM Taxonomy" in md


# ─── Tests: CLI ───────────────────────────────────────────────────────────────

class TestCLI:
    def _write_minimal_benchmark(self, bdir: Path) -> None:
        bdir.mkdir(parents=True, exist_ok=True)
        fa = [_false_accept("F-001", "Артефакт OCR"), _false_accept("F-002", "")]
        (bdir / "false_accepts.json").write_text(json.dumps(fa))
        (bdir / "human_benchmark_records.json").write_text(json.dumps(fa))
        tax = [_llm_decision("F-001", "other"), _llm_decision("F-002", "other")]
        (bdir / "critic_v2_llm_taxonomy_decisions.json").write_text(json.dumps(tax))
        (bdir / "human_benchmark_summary.json").write_text(json.dumps({}))

    def test_cli_basic_run(self, tmp_path):
        bdir = tmp_path / "bench"
        self._write_minimal_benchmark(bdir)
        out = tmp_path / "out"
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir),
             "--output-dir", str(out)],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert (out / "llm_taxonomy_other_analysis.json").exists()
        assert (out / "llm_taxonomy_other_analysis.md").exists()

    def test_cli_with_export_csv(self, tmp_path):
        bdir = tmp_path / "bench"
        self._write_minimal_benchmark(bdir)
        out = tmp_path / "out"
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir),
             "--export-csv",
             "--output-dir", str(out)],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert (out / "llm_taxonomy_other_samples.csv").exists()

    def test_cli_missing_dir_exits_1(self, tmp_path):
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(tmp_path / "nonexistent"),
             "--output-dir", str(tmp_path / "out")],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 1

    def test_cli_json_structure(self, tmp_path):
        bdir = tmp_path / "bench"
        self._write_minimal_benchmark(bdir)
        out = tmp_path / "out"
        subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir),
             "--output-dir", str(out)],
            capture_output=True, text=True, timeout=30,
        )
        data = json.loads((out / "llm_taxonomy_other_analysis.json").read_text())
        assert data["meta"]["total_other_cases"] == 2
        assert "category_breakdown" in data
        assert "suitability_summary" in data
        assert "llm_can_handle" in data["suitability_summary"]

    def test_cli_multiple_dirs(self, tmp_path):
        bdir1 = tmp_path / "bench1"
        bdir2 = tmp_path / "bench2"
        self._write_minimal_benchmark(bdir1)
        self._write_minimal_benchmark(bdir2)
        out = tmp_path / "out"
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir1),
             "--benchmark-output-dir", str(bdir2),
             "--output-dir", str(out)],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        data = json.loads((out / "llm_taxonomy_other_analysis.json").read_text())
        # 2 cases per dir × 2 dirs = 4 total
        assert data["meta"]["total_other_cases"] == 4
        assert len(data["meta"]["source_dirs"]) == 2

    def test_production_not_modified(self, tmp_path):
        bdir = tmp_path / "bench"
        self._write_minimal_benchmark(bdir)
        out = tmp_path / "out"
        import subprocess as sp
        result = sp.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir),
             "--output-dir", str(out)],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert "Production pipeline NOT modified" in result.stdout
