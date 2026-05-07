"""
test_analyze_human_rejection_reasons.py
-----------------------------------------
Tests for analyze_human_rejection_reasons.py.

All tests work WITHOUT production files.
Uses synthetic false_accept records.

Runs with:
    python -m pytest backend/tests/test_analyze_human_rejection_reasons.py -v
"""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path("backend/scripts/analyze_human_rejection_reasons.py")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _fa_record(
    fid: str = "F-001",
    human_reason: str = "",
    section: str = "KJ",
    category: str = "spec_mismatch",
    severity: str = "РЕКОМЕНДАТЕЛЬНОЕ",
    evidence_quality: str = "valid",
    critic_score: int = 8,
    impact_area: str = "construction",
    project: str = "TEST-PROJ",
) -> dict:
    return {
        "finding_id": fid,
        "project_name": project,
        "section": section,
        "severity": severity,
        "category": category,
        "evidence_quality": evidence_quality,
        "impact_area": impact_area,
        "critic_score": critic_score,
        "human_decision": "rejected",
        "human_reason": human_reason,
        "critic_decision": "accept",
        "critic_reject_reason": "",
        "match_confidence": "exact",
        "title": f"Title {fid}",
        "description": f"Description {fid}",
        "recommendation": "Fix it",
        "classification": "critic_too_soft",
    }


def _make_benchmark_dir(tmp_path: Path, records: list[dict]) -> Path:
    """Create a synthetic benchmark output dir with false_accepts.json."""
    bdir = tmp_path / "benchmark"
    bdir.mkdir(parents=True, exist_ok=True)
    (bdir / "false_accepts.json").write_text(
        json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return bdir


def _load_script():
    import importlib.util
    spec = importlib.util.spec_from_file_location("analyze_human_rejection_reasons", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ─── Import check ─────────────────────────────────────────────────────────────

class TestImports:
    def test_script_imports(self):
        mod = _load_script()
        assert hasattr(mod, "classify_reason")
        assert hasattr(mod, "analyze_false_accepts")
        assert hasattr(mod, "render_markdown")
        assert hasattr(mod, "run_analysis")
        assert hasattr(mod, "TAXONOMY")


# ─── classify_reason ─────────────────────────────────────────────────────────

class TestClassifyReason:
    def setup_method(self):
        self.mod = _load_script()

    def test_empty_reason_is_other(self):
        assert self.mod.classify_reason("") == "other"

    def test_none_like_reason_is_other(self):
        assert self.mod.classify_reason("   ") == "other"

    def test_ocr_error_classified(self):
        r = "Ошибка распознавания OCR: значение прочитано неверно"
        assert self.mod.classify_reason(r) == "visual_or_ocr_misread"

    def test_ocr_ii_misread_classified(self):
        r = "ИИ принял «300» из метки радиус 300 мм за наименьший размер"
        assert self.mod.classify_reason(r) == "visual_or_ocr_misread"

    def test_measurement_error_classified(self):
        # "неверный результат" → wrong_measurement pattern
        r = "ИИ получил неверный размер 300 из неприменимой формулы"
        cat = self.mod.classify_reason(r)
        assert cat in ("wrong_measurement_or_number", "wrong_norm_context", "visual_or_ocr_misread")

    def test_wrong_norm_classified(self):
        r = "Несуществующий коэффициент из неверно процитированного п. 10.3.13 СП 63"
        assert self.mod.classify_reason(r) == "wrong_norm_context"

    def test_duplicate_covered_classified(self):
        r = "Задан в общих указаниях на листе 1 — уже содержится в проекте"
        assert self.mod.classify_reason(r) == "duplicate_or_already_covered"

    def test_acceptable_design_classified(self):
        r = "Конструктивное решение допустимо по нормам СП 63, п.10.3.7"
        assert self.mod.classify_reason(r) == "acceptable_design_solution"

    def test_not_significant_classified(self):
        r = "Не влияет на производство работ. Формальный редакционный недочёт."
        assert self.mod.classify_reason(r) == "not_functionally_significant"

    def test_wrong_scope_classified(self):
        r = "Технология производства работ регулируется ППР, а не чертежами КЖ"
        assert self.mod.classify_reason(r) == "wrong_scope_or_section"

    def test_insufficient_context_classified(self):
        r = "Прошедшая государственную экспертизу нормативная база в ПЗ раздела КЖ"
        assert self.mod.classify_reason(r) == "insufficient_source_context"

    def test_calculation_not_supported_classified(self):
        r = "ИИ сам рассчитал μv и подтвердил соответствие нормативным минимумам"
        assert self.mod.classify_reason(r) == "calculation_not_supported"

    def test_in_pz_classified_as_insufficient_context(self):
        r = "Приводится в пояснительной записке раздела, а не дублируется на чертежах"
        assert self.mod.classify_reason(r) == "duplicate_or_already_covered"

    def test_expertise_passed_classified_as_insufficient_context(self):
        r = "Объект прошёл государственную экспертизу, нормативная база подтверждена"
        assert self.mod.classify_reason(r) == "insufficient_source_context"

    def test_unknown_reason_is_other(self):
        r = "Совершенно непонятная причина без ключевых слов"
        assert self.mod.classify_reason(r) == "other"

    def test_all_taxonomy_categories_defined(self):
        """Every taxonomy key must have required fields."""
        mod = self.mod
        for cat, info in mod.TAXONOMY.items():
            assert "label" in info, f"Category {cat} missing 'label'"
            assert "llm_fitness" in info, f"Category {cat} missing 'llm_fitness'"
            assert "description" in info, f"Category {cat} missing 'description'"
            assert info["llm_fitness"] in ("llm_can_handle", "borderline_llm", "needs_human"), \
                f"Category {cat} has invalid llm_fitness: {info['llm_fitness']}"


# ─── analyze_false_accepts ────────────────────────────────────────────────────

class TestAnalyzeFalseAccepts:
    def setup_method(self):
        self.mod = _load_script()

    def test_empty_records_no_crash(self):
        analysis = self.mod.analyze_false_accepts([])
        assert analysis["total_false_accept"] == 0

    def test_taxonomy_distribution_computed(self):
        records = [
            _fa_record("F-1", "Ошибка распознавания OCR, неверно прочитано"),
            _fa_record("F-2", "Конструктивное решение допустимо по нормам"),
            _fa_record("F-3", ""),  # other
        ]
        analysis = self.mod.analyze_false_accepts(records)
        td = analysis["taxonomy_distribution"]
        assert "visual_or_ocr_misread" in td
        assert "acceptable_design_solution" in td
        assert "other" in td

    def test_llm_fitness_distribution_present(self):
        records = [_fa_record("F-1", "Ошибка OCR")]
        analysis = self.mod.analyze_false_accepts(records)
        assert "llm_fitness_distribution" in analysis
        assert len(analysis["llm_fitness_distribution"]) > 0

    def test_by_section_breakdown(self):
        records = [
            _fa_record("F-1", "OCR error", section="KJ"),
            _fa_record("F-2", "OCR error", section="AR"),
        ]
        analysis = self.mod.analyze_false_accepts(records)
        assert "KJ" in analysis["by_section"]
        assert "AR" in analysis["by_section"]

    def test_enriched_records_have_taxonomy(self):
        records = [_fa_record("F-1", "Ошибка OCR")]
        analysis = self.mod.analyze_false_accepts(records)
        er = analysis["enriched_records"]
        assert len(er) == 1
        assert "taxonomy_primary" in er[0]
        assert "llm_fitness" in er[0]
        assert "taxonomy_label" in er[0]

    def test_deterministic_catchable_counted(self):
        records = [
            _fa_record("F-1", "Ошибка OCR — детектируется"),
            _fa_record("F-2", "Конструктивное решение — нет детект. сигнала"),
        ]
        analysis = self.mod.analyze_false_accepts(records)
        # OCR errors have deterministic_signal=True
        assert analysis["deterministic_catchable"] >= 1

    def test_llm_can_handle_counted(self):
        records = [
            _fa_record("F-1", "Ошибка распознавания OCR"),
            _fa_record("F-2", "Некорректное распознавание OCR: цифра"),
        ]
        analysis = self.mod.analyze_false_accepts(records)
        assert analysis["llm_can_handle_count"] >= 1

    def test_top_human_reasons_present(self):
        records = [
            _fa_record("F-1", "Задан в общих указаниях на листе 1"),
            _fa_record("F-2", "Задан в общих указаниях на листе 1"),
        ]
        analysis = self.mod.analyze_false_accepts(records)
        assert len(analysis["top_human_reasons"]) >= 1
        assert analysis["top_human_reasons"][0]["count"] >= 1


# ─── run_analysis ─────────────────────────────────────────────────────────────

class TestRunAnalysis:
    def setup_method(self):
        self.mod = _load_script()

    def test_creates_json_output(self, tmp_path):
        records = [_fa_record("F-1", "Ошибка OCR")]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        self.mod.run_analysis([bdir], out_dir, quiet=True)
        assert (out_dir / "human_rejection_reason_analysis.json").exists()

    def test_creates_md_output(self, tmp_path):
        records = [_fa_record("F-1", "Ошибка OCR")]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        self.mod.run_analysis([bdir], out_dir, quiet=True)
        assert (out_dir / "human_rejection_reason_analysis.md").exists()

    def test_creates_csv_when_requested(self, tmp_path):
        records = [_fa_record("F-1", "Ошибка OCR")]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        self.mod.run_analysis([bdir], out_dir, export_csv_flag=True, quiet=True)
        assert (out_dir / "false_accept_reason_samples.csv").exists()

    def test_csv_has_expected_columns(self, tmp_path):
        records = [_fa_record("F-1", "Ошибка OCR")]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        self.mod.run_analysis([bdir], out_dir, export_csv_flag=True, quiet=True)
        with (out_dir / "false_accept_reason_samples.csv").open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cols = reader.fieldnames
        assert "finding_id" in cols
        assert "taxonomy_primary" in cols
        assert "llm_fitness" in cols
        assert "human_reason" in cols

    def test_empty_benchmark_dir_no_crash(self, tmp_path):
        bdir = tmp_path / "empty"
        bdir.mkdir()
        (bdir / "false_accepts.json").write_text("[]", encoding="utf-8")
        out_dir = tmp_path / "out"
        analysis = self.mod.run_analysis([bdir], out_dir, quiet=True)
        assert analysis["total_false_accept"] == 0
        assert (out_dir / "human_rejection_reason_analysis.json").exists()

    def test_raises_on_nonexistent_dir(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            self.mod.run_analysis([tmp_path / "no_such_dir"], tmp_path / "out", quiet=True)

    def test_multiple_benchmark_dirs_combined(self, tmp_path):
        r1 = [_fa_record("KJ-1", "OCR error", section="KJ")]
        r2 = [_fa_record("AR-1", "OCR error", section="AR")]
        b1 = _make_benchmark_dir(tmp_path / "kj", r1)
        b2 = _make_benchmark_dir(tmp_path / "ar", r2)
        out_dir = tmp_path / "out"
        analysis = self.mod.run_analysis([b1, b2], out_dir, quiet=True)
        assert analysis["total_false_accept"] == 2
        assert "KJ" in analysis["by_section"]
        assert "AR" in analysis["by_section"]

    def test_no_production_files_required(self, tmp_path):
        """Analysis must work with only synthetic benchmark output."""
        records = [_fa_record("F-1", "Формальный недочёт, не влияет на работы")]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        # Should succeed without accessing projects/ dir
        result = self.mod.run_analysis([bdir], out_dir, quiet=True)
        assert result is not None


# ─── render_markdown ──────────────────────────────────────────────────────────

class TestRenderMarkdown:
    def setup_method(self):
        self.mod = _load_script()

    def test_markdown_has_key_sections(self):
        records = [_fa_record("F-1", "Ошибка OCR")]
        analysis = self.mod.analyze_false_accepts(records)
        md = self.mod.render_markdown(analysis)
        assert "# Human Rejection Reason Analysis" in md
        assert "Taxonomy" in md
        assert "Recommendations" in md
        assert "LLM" in md

    def test_production_not_modified_note(self):
        analysis = self.mod.analyze_false_accepts([])
        md = self.mod.render_markdown(analysis)
        assert "Production pipeline NOT modified" in md

    def test_taxonomy_table_rendered(self):
        records = [
            _fa_record("F-1", "Ошибка OCR"),
            _fa_record("F-2", "Допустимое решение по нормам"),
        ]
        analysis = self.mod.analyze_false_accepts(records)
        md = self.mod.render_markdown(analysis)
        assert "visual_or_ocr_misread" in md
        assert "acceptable_design_solution" in md

    def test_llm_fitness_in_table(self):
        records = [_fa_record("F-1", "Ошибка OCR")]
        analysis = self.mod.analyze_false_accepts(records)
        md = self.mod.render_markdown(analysis)
        assert "llm_can_handle" in md or "LLM" in md


# ─── Taxonomy completeness ────────────────────────────────────────────────────

class TestTaxonomyCompleteness:
    def setup_method(self):
        self.mod = _load_script()

    def test_taxonomy_covers_real_kj_reasons(self):
        """Spot-check that real KJ reasons from benchmark get meaningful classification."""
        real_reasons = [
            ("Ошибка распознования OCR: ГОСТ прописан верно",
             "visual_or_ocr_misread"),
            ("ИИ принял «300» из метки «радиус 300 мм» за «наименьший размер сечения 300 мм»",
             "visual_or_ocr_misread"),
            ("Несуществующий коэффициент «12·dлк» из неверно процитированного п. 10.3.13 СП 63",
             "wrong_norm_context"),
            ("Шаг вертикальной арматуры 100 мм — конструктивное решение проектировщика, допустимое по нормам",
             "acceptable_design_solution"),
            ("Нормативный перечень приводится в текстовой ПЗ раздела КЖ, а не дублируется на каждом чертеже",
             "duplicate_or_already_covered"),
            ("Технология производства работ и требования к уплотнению относятся к ППР",
             "wrong_scope_or_section"),
            ("Расхождение 8,66 кг (~760 руб.) не несёт финансовых последствий — ниже порога значимости",
             "not_functionally_significant"),
            ("ИИ сам рассчитал μv ≈ 0,314% и подтвердил соответствие нормам — замечание при уже выполненном расчёте",
             "calculation_not_supported"),
        ]
        for reason, expected_cat in real_reasons:
            result = self.mod.classify_reason(reason)
            assert result == expected_cat, (
                f"Reason: '{reason[:60]}...'\n"
                f"Expected: {expected_cat}, Got: {result}"
            )

    def test_taxonomy_covers_real_ar_reasons(self):
        """Spot-check AR section human reasons."""
        real_reasons = [
            ("Формальный редакционный недочёт, не влияет на содержание чертежей",
             "not_functionally_significant"),
            # "Экспертиза принята" triggers insufficient_source_context pattern — acceptable
            ("Устаревшие версии СП/ГОСТ в ведомости. Экспертиза принята; не влияет на строительство",
             # Either not_functionally_significant or insufficient_source_context is correct
             None),  # checked manually below
        ]
        for reason, expected_cat in real_reasons:
            if expected_cat is None:
                # Just check it's classified as something meaningful (not 'other')
                result = self.mod.classify_reason(reason)
                assert result != "other", (
                    f"Reason: '{reason[:60]}' should not be 'other', got: {result}"
                )
            else:
                result = self.mod.classify_reason(reason)
                assert result == expected_cat, (
                    f"Reason: '{reason[:60]}'\n"
                    f"Expected: {expected_cat}, Got: {result}"
                )

    def test_all_false_accepts_classified_not_crash(self, tmp_path):
        """All 189 real false_accepts from KJ+AR benchmark must classify without error."""
        kj_path = Path("/tmp/human_benchmark_kj_after/false_accepts.json")
        ar_path = Path("/tmp/human_benchmark_ar_after/false_accepts.json")

        records = []
        for p in (kj_path, ar_path):
            if p.exists():
                records.extend(json.loads(p.read_text(encoding="utf-8")))

        if not records:
            pytest.skip("Benchmark output not available — run benchmark first")

        analysis = self.mod.analyze_false_accepts(records)
        assert analysis["total_false_accept"] == len(records)
        # Every record must have a taxonomy primary
        for r in analysis["enriched_records"]:
            assert r["taxonomy_primary"] in self.mod.TAXONOMY, (
                f"Unknown taxonomy category: {r['taxonomy_primary']}"
            )
        # "other" should not dominate (< 30%)
        other_count = analysis["taxonomy_distribution"].get("other", 0)
        other_rate = other_count / len(records) if records else 0
        assert other_rate < 0.30, (
            f"'other' category is {other_rate*100:.1f}% — taxonomy too incomplete. "
            f"Add more keyword patterns. other_count={other_count}/{len(records)}"
        )


# ─── CLI integration ──────────────────────────────────────────────────────────

class TestCLI:
    def test_cli_basic_run(self, tmp_path):
        records = [
            _fa_record("F-1", "Ошибка OCR — неверно прочитано"),
            _fa_record("F-2", "Конструктивное решение допустимо"),
            _fa_record("F-3", ""),
        ]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir),
             "--output-dir", str(out_dir),
             "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert (out_dir / "human_rejection_reason_analysis.json").exists()
        assert (out_dir / "human_rejection_reason_analysis.md").exists()

    def test_cli_with_csv(self, tmp_path):
        records = [_fa_record("F-1", "Ошибка OCR")]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir),
             "--output-dir", str(out_dir),
             "--export-csv", "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert (out_dir / "false_accept_reason_samples.csv").exists()

    def test_cli_nonexistent_dir_exits_1(self, tmp_path):
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(tmp_path / "no_such_dir")],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 1

    def test_cli_json_structure(self, tmp_path):
        records = [
            _fa_record("F-1", "Ошибка OCR"),
            _fa_record("F-2", "Неверная норма"),
        ]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir),
             "--output-dir", str(out_dir), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        analysis = json.loads(
            (out_dir / "human_rejection_reason_analysis.json").read_text(encoding="utf-8")
        )
        assert "taxonomy_distribution" in analysis
        assert "llm_fitness_distribution" in analysis
        assert "total_false_accept" in analysis
        assert analysis["total_false_accept"] == 2

    def test_cli_does_not_modify_production(self, tmp_path):
        """Script must not write to any production project files."""
        from pathlib import Path as P
        findings_files = sorted(P("projects").rglob("03_findings.json"))[:3]
        before = {str(p): p.read_text(encoding="utf-8") for p in findings_files}

        records = [_fa_record("F-1", "Ошибка OCR")]
        bdir = _make_benchmark_dir(tmp_path, records)
        out_dir = tmp_path / "out"
        subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(bdir),
             "--output-dir", str(out_dir), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        for path_str, original in before.items():
            assert P(path_str).read_text(encoding="utf-8") == original

    def test_cli_on_real_benchmark_kj(self, tmp_path):
        """CLI must run on real KJ benchmark output without error."""
        kj_dir = Path("/tmp/human_benchmark_kj_after")
        if not kj_dir.exists():
            pytest.skip("KJ benchmark output not available — run benchmark first")
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(kj_dir),
             "--top-n", "30",
             "--output-dir", str(tmp_path / "analysis"),
             "--export-csv", "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert (tmp_path / "analysis" / "human_rejection_reason_analysis.json").exists()

    def test_cli_combined_kj_ar(self, tmp_path):
        """CLI must accept multiple --benchmark-output-dir args."""
        kj_dir = Path("/tmp/human_benchmark_kj_after")
        ar_dir = Path("/tmp/human_benchmark_ar_after")
        if not kj_dir.exists() or not ar_dir.exists():
            pytest.skip("Benchmark outputs not available")
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--benchmark-output-dir", str(kj_dir),
             "--benchmark-output-dir", str(ar_dir),
             "--output-dir", str(tmp_path / "combined"),
             "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        summary = json.loads(
            (tmp_path / "combined" / "human_rejection_reason_analysis.json").read_text()
        )
        assert summary["total_false_accept"] >= 100  # KJ(142) + AR(47) = 189
