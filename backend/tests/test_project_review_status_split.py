"""
Tests for findings_review_status / optimization_review_status в ProjectStatus.

Контекст: до этого был один общий expert_review_status, и UI рисовал одну
галочку. Эксперт хочет видеть две независимые галочки (замечания + оптимизации),
чтобы понять, что именно отработано.

Проверки используют synthetic _output/ структуру с минимальным набором файлов,
чтобы не задевать pipeline.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.app.services.common import project_service  # noqa: E402


def _write_minimal_project(
    base: Path,
    project_id: str,
    findings: list[dict] | None = None,
    optimizations: list[dict] | None = None,
    expert_decisions: list[dict] | None = None,
) -> Path:
    proj_dir = base / project_id
    proj_dir.mkdir(parents=True, exist_ok=True)
    out_dir = proj_dir / "_output"
    out_dir.mkdir(exist_ok=True)
    (proj_dir / "project_info.json").write_text(
        json.dumps({"project_id": project_id, "name": project_id, "section": "EOM"}),
        encoding="utf-8",
    )
    if findings is not None:
        (out_dir / "03_findings.json").write_text(
            json.dumps({"findings": findings}), encoding="utf-8"
        )
    if optimizations is not None:
        (out_dir / "optimization.json").write_text(
            json.dumps({
                "items": optimizations,
                "meta": {
                    "total_items": len(optimizations),
                    "by_type": {},
                    "estimated_savings_pct": 0,
                },
            }),
            encoding="utf-8",
        )
    if expert_decisions is not None:
        (out_dir / "expert_review.json").write_text(
            json.dumps({"decisions": expert_decisions}), encoding="utf-8"
        )
    return proj_dir


@pytest.fixture
def projects_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: tmp_path)
    return tmp_path


def _status(projects_root: Path, project_id: str):
    return project_service.get_project_status(project_id)


def _findings(n: int) -> list[dict]:
    return [{"id": f"F-{i:03d}", "severity": "КРИТИЧЕСКОЕ"} for i in range(1, n + 1)]


def _opts(n: int) -> list[dict]:
    return [{"id": f"O-{i:03d}"} for i in range(1, n + 1)]


def _decisions(
    findings_decided: int = 0, opts_decided: int = 0, decision: str = "accepted"
) -> list[dict]:
    out = []
    for i in range(1, findings_decided + 1):
        out.append({"item_id": f"F-{i:03d}", "item_type": "finding", "decision": decision})
    for i in range(1, opts_decided + 1):
        out.append({"item_id": f"O-{i:03d}", "item_type": "optimization", "decision": decision})
    return out


class TestSplitStatus:
    def test_no_review_file_leaves_both_empty(self, projects_root: Path):
        _write_minimal_project(
            projects_root, "P1", findings=_findings(3), optimizations=_opts(2)
        )
        s = _status(projects_root, "P1")
        assert s.findings_review_status == ""
        assert s.optimization_review_status == ""
        assert s.expert_review_status == ""

    def test_findings_complete_opts_empty(self, projects_root: Path):
        _write_minimal_project(
            projects_root, "P1",
            findings=_findings(3), optimizations=_opts(2),
            expert_decisions=_decisions(findings_decided=3, opts_decided=0),
        )
        s = _status(projects_root, "P1")
        assert s.findings_review_status == "complete"
        assert s.optimization_review_status == ""
        # Объединённый статус должен быть partial (2 опт ещё не отработаны).
        assert s.expert_review_status == "partial"

    def test_both_complete(self, projects_root: Path):
        _write_minimal_project(
            projects_root, "P1",
            findings=_findings(3), optimizations=_opts(2),
            expert_decisions=_decisions(findings_decided=3, opts_decided=2),
        )
        s = _status(projects_root, "P1")
        assert s.findings_review_status == "complete"
        assert s.optimization_review_status == "complete"
        assert s.expert_review_status == "complete"

    def test_findings_partial_opts_complete(self, projects_root: Path):
        _write_minimal_project(
            projects_root, "P1",
            findings=_findings(5), optimizations=_opts(2),
            expert_decisions=_decisions(findings_decided=2, opts_decided=2),
        )
        s = _status(projects_root, "P1")
        assert s.findings_review_status == "partial"
        assert s.optimization_review_status == "complete"

    def test_no_findings_only_opts_complete(self, projects_root: Path):
        # Если у проекта нет замечаний, findings_review_status должен остаться ""
        # — индикатор просто не рисуется.
        _write_minimal_project(
            projects_root, "P1",
            findings=[], optimizations=_opts(2),
            expert_decisions=_decisions(findings_decided=0, opts_decided=2),
        )
        s = _status(projects_root, "P1")
        assert s.findings_review_status == ""
        assert s.optimization_review_status == "complete"

    def test_no_opts_only_findings_complete(self, projects_root: Path):
        _write_minimal_project(
            projects_root, "P1",
            findings=_findings(3), optimizations=[],
            expert_decisions=_decisions(findings_decided=3, opts_decided=0),
        )
        s = _status(projects_root, "P1")
        assert s.findings_review_status == "complete"
        assert s.optimization_review_status == ""

    def test_partial_both(self, projects_root: Path):
        _write_minimal_project(
            projects_root, "P1",
            findings=_findings(4), optimizations=_opts(3),
            expert_decisions=_decisions(findings_decided=1, opts_decided=1),
        )
        s = _status(projects_root, "P1")
        assert s.findings_review_status == "partial"
        assert s.optimization_review_status == "partial"
        assert s.expert_review_status == "partial"


class TestFrontendCardWiring:
    """Smoke check: HTML card uses the two new fields."""
    def test_index_renders_split_indicators(self):
        html = (_PROJECT_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
        assert "findings_review_status" in html
        assert "optimization_review_status" in html
        # И обратная совместимость через expert_review_status сохраняется.
        assert "expert_review_status" in html
