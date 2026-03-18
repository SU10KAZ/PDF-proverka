"""Тесты для quality_metrics — проверка структуры metrics summary."""
import json
import pytest
from pathlib import Path

# Добавим tools/ в path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from quality_metrics import compute_metrics, format_summary

BASELINE_ROOT = Path(__file__).resolve().parent.parent / "test" / "baseline"


def _find_any_baseline() -> Path | None:
    """Найти любую baseline-папку с 03_findings.json."""
    if not BASELINE_ROOT.exists():
        return None
    for d in sorted(BASELINE_ROOT.iterdir()):
        if d.is_dir() and (d / "03_findings.json").exists():
            return d
    return None


@pytest.fixture(scope="module")
def baseline_dir():
    d = _find_any_baseline()
    if not d:
        pytest.skip("Baseline не найден")
    return d


class TestComputeMetrics:
    def test_returns_dict(self, baseline_dir):
        m = compute_metrics(baseline_dir)
        assert isinstance(m, dict)

    def test_has_findings_section(self, baseline_dir):
        m = compute_metrics(baseline_dir)
        f = m.get("findings", {})
        assert "total" in f
        assert "evidence_coverage" in f
        assert "related_block_ids_coverage" in f
        assert "by_severity" in f

    def test_has_norms_section(self, baseline_dir):
        m = compute_metrics(baseline_dir)
        n = m.get("norms", {})
        assert "total_checks" in n
        assert "deterministic_count" in n
        assert "policy_violations_count" in n

    def test_has_pipeline_section(self, baseline_dir):
        m = compute_metrics(baseline_dir)
        p = m.get("pipeline", {})
        assert "total_stages" in p
        assert "completed" in p

    def test_coverage_values_valid(self, baseline_dir):
        m = compute_metrics(baseline_dir)
        f = m.get("findings", {})
        for key in ["evidence_coverage", "related_block_ids_coverage"]:
            val = f.get(key, 0)
            assert 0 <= val <= 1.0, f"{key}={val} вне диапазона [0,1]"

    def test_total_findings_positive(self, baseline_dir):
        m = compute_metrics(baseline_dir)
        assert m["findings"]["total"] > 0


class TestFormatSummary:
    def test_format_not_empty(self, baseline_dir):
        m = compute_metrics(baseline_dir)
        text = format_summary(m)
        assert isinstance(text, str)
        assert len(text) > 50
        assert "Findings" in text


class TestMinimalMetrics:
    def test_empty_dir(self, tmp_path):
        """compute_metrics на пустой папке не падает."""
        m = compute_metrics(tmp_path)
        assert m["findings"] == {}
        assert m["norms"] == {}
        assert m["pipeline"] == {}
