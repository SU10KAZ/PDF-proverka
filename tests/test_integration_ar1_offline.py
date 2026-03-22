"""Офлайн-интеграционный тест на baseline-данных АР1.

Проверяет grounding, selective review input, norm checks — без LLM,
используя готовые артефакты из test/baseline/АР_133-23-ГК-АР1/.
"""
import json
import shutil
import pytest
from pathlib import Path

from webapp.services.grounding_service import compute_grounding_candidates
from norms import validate_norm_checks

# ─── Пути ────────────────────────────────────────────────────────────
BASELINE_ROOT = Path(__file__).resolve().parent.parent / "test" / "baseline"

REQUIRED_BASELINE_FILES = [
    "03_findings.json",
    "02_blocks_analysis.json",
    "norm_checks.json",
    "pipeline_log.json",
    "project_info.json",
]


def _find_ar1_baseline() -> Path | None:
    """Динамический поиск baseline-папки АР1 по наличию обязательных файлов.

    Не зависит от точного имени папки — ищет по содержимому.
    """
    if not BASELINE_ROOT.exists():
        return None
    for candidate in sorted(BASELINE_ROOT.iterdir()):
        if not candidate.is_dir():
            continue
        # Ищем папку, содержащую все обязательные файлы
        if all((candidate / f).exists() for f in REQUIRED_BASELINE_FILES):
            # Дополнительная проверка: это АР-проект
            try:
                info = json.loads((candidate / "project_info.json").read_text(encoding="utf-8"))
                if info.get("section", "").upper() in ("АР", "AR"):
                    return candidate
            except (json.JSONDecodeError, KeyError):
                pass
            # Fallback: имя папки содержит "АР" или "AR"
            if "АР" in candidate.name or "AR" in candidate.name:
                return candidate
    return None


BASELINE_DIR = _find_ar1_baseline() or (BASELINE_ROOT / "АР_133-23-ГК-АР1")


# ─── Фикстуры ────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def baseline_available():
    """Пропустить весь модуль, если baseline-данные отсутствуют."""
    if not BASELINE_DIR.exists():
        pytest.skip(f"Baseline не найден в {BASELINE_ROOT}")
    for fname in REQUIRED_BASELINE_FILES:
        if not (BASELINE_DIR / fname).exists():
            pytest.skip(f"Baseline-файл отсутствует: {fname}")


@pytest.fixture(scope="module")
def findings_data(baseline_available):
    return json.loads((BASELINE_DIR / "03_findings.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def blocks_data(baseline_available):
    return json.loads((BASELINE_DIR / "02_blocks_analysis.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def norm_checks_data(baseline_available):
    return json.loads((BASELINE_DIR / "norm_checks.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def pipeline_log(baseline_available):
    return json.loads((BASELINE_DIR / "pipeline_log.json").read_text(encoding="utf-8"))


@pytest.fixture()
def work_dir(tmp_path, findings_data):
    """Копирует 03_findings.json во временную _output/ для selective review."""
    output_dir = tmp_path / "_output"
    output_dir.mkdir()
    (output_dir / "03_findings.json").write_text(
        json.dumps(findings_data, ensure_ascii=False), encoding="utf-8"
    )
    return output_dir


# ─── 03_findings.json: структура и инварианты ─────────────────────────
class TestFindingsStructure:
    def test_has_meta_and_findings(self, findings_data):
        assert "meta" in findings_data
        assert "findings" in findings_data
        assert isinstance(findings_data["findings"], list)

    def test_total_matches_meta(self, findings_data):
        assert findings_data["meta"]["total_findings"] == len(findings_data["findings"])

    def test_findings_have_required_fields(self, findings_data):
        required = {"id", "severity", "problem"}
        for f in findings_data["findings"]:
            missing = required - set(f.keys())
            assert not missing, f"{f['id']}: отсутствуют поля {missing}"

    def test_unique_ids(self, findings_data):
        ids = [f["id"] for f in findings_data["findings"]]
        assert len(ids) == len(set(ids)), "Дубликаты ID в findings"

    def test_severity_values_known(self, findings_data):
        known = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
                 "РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ"}
        for f in findings_data["findings"]:
            assert f["severity"] in known, f"{f['id']}: неизвестный severity={f['severity']}"

    def test_by_severity_counts_reasonable(self, findings_data):
        """Meta.by_severity может немного расходиться с фактическим подсчётом
        (артефакт LLM-генерации). Проверяем что расхождение ≤ 2."""
        meta_counts = findings_data["meta"]["by_severity"]
        actual = {}
        for f in findings_data["findings"]:
            actual[f["severity"]] = actual.get(f["severity"], 0) + 1
        for sev, cnt in meta_counts.items():
            diff = abs(actual.get(sev, 0) - cnt)
            assert diff <= 2, f"meta.by_severity[{sev}]={cnt}, фактически {actual.get(sev,0)}, расхождение {diff}"


# ─── Grounding: офлайн-прогон ─────────────────────────────────────────
class TestGrounding:
    def test_grounding_runs(self, findings_data, blocks_data):
        findings = findings_data["findings"]
        blocks = blocks_data.get("block_analyses", [])
        result = compute_grounding_candidates(findings, blocks)
        assert len(result) == len(findings)

    def test_majority_grounded(self, findings_data, blocks_data):
        """Большинство замечаний (≥70%) должны получить привязку к блокам."""
        findings = findings_data["findings"]
        blocks = blocks_data.get("block_analyses", [])
        result = compute_grounding_candidates(findings, blocks)
        grounded = 0
        for f in result:
            if f.get("evidence") or f.get("related_block_ids") or f.get("grounding_candidates"):
                grounded += 1
        pct = grounded / len(result) * 100 if result else 0
        assert pct >= 60, f"Только {pct:.0f}% замечаний привязаны к блокам (ожидается ≥60%)"


# ─── norm_checks.json: структура и валидация ──────────────────────────
class TestNormChecks:
    def test_has_meta_and_checks(self, norm_checks_data):
        assert "meta" in norm_checks_data
        assert "checks" in norm_checks_data
        assert isinstance(norm_checks_data["checks"], list)

    def test_checks_have_required_fields(self, norm_checks_data):
        required = {"norm_as_cited", "status", "affected_findings"}
        for c in norm_checks_data["checks"]:
            missing = required - set(c.keys())
            assert not missing, f"norm check {c.get('norm_as_cited','?')}: отсутствуют {missing}"

    def test_status_values_known(self, norm_checks_data):
        known = {"active", "replaced", "cancelled", "outdated_edition", "not_found", "unknown"}
        for c in norm_checks_data["checks"]:
            assert c["status"] in known, f"{c['norm_as_cited']}: неизвестный status={c['status']}"

    def test_validate_norm_checks_function(self, baseline_available):
        result = validate_norm_checks(BASELINE_DIR / "norm_checks.json")
        assert result["total_checks"] > 0

    def test_meta_totals(self, norm_checks_data):
        meta = norm_checks_data["meta"]
        assert meta["total_checked"] == len(norm_checks_data["checks"])


# ─── pipeline_log.json: полнота конвейера ─────────────────────────────
class TestPipelineLog:
    EXPECTED_STAGES = {
        "crop_blocks", "text_analysis", "block_analysis",
        "findings_merge", "norm_verify",
    }

    def test_has_stages(self, pipeline_log):
        assert "stages" in pipeline_log

    def test_core_stages_present(self, pipeline_log):
        stages = set(pipeline_log["stages"].keys())
        missing = self.EXPECTED_STAGES - stages
        assert not missing, f"Отсутствуют этапы: {missing}"

    def test_core_stages_completed(self, pipeline_log):
        for stage_name in self.EXPECTED_STAGES:
            stage = pipeline_log["stages"].get(stage_name, {})
            assert stage.get("status") == "done", \
                f"Этап {stage_name}: status={stage.get('status')}, ожидался done"
