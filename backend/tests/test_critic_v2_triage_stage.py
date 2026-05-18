"""
Tests for the new `critic_v2_triage` post-processing stage runner.

Контракт:
    * Runner НЕ модифицирует 03_findings.json / 03_findings_review.json /
      expert_review.json / production artifacts.
    * Runner пишет ТОЛЬКО в <project>/_output/<output_subdir>/.
    * Runner gracefully работает без 02_blocks_analysis.json и без
      document_graph.json.
    * Runner не запускает LLM ни при каких условиях в этой версии.
    * Default config flag CRITIC_V2_ENABLED=False — production pipeline
      не подключает stage.
"""
from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


from backend.app.pipeline.stages.critic_v2_triage import run_critic_v2_triage  # noqa: E402
from backend.app.pipeline.stages.critic_v2_triage.runner import (  # noqa: E402
    ARTIFACT_INLINE_MAP,
    ARTIFACT_METRICS,
    ARTIFACT_STAGE_SUMMARY,
    ARTIFACT_TRIAGE,
    ARTIFACT_TRIAGE_UI,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

_SAMPLE_FINDINGS = [
    {
        "id": "F-001",
        "title": "Несоответствие сечения кабеля расчётному току",
        "description": "Кабель ВВГнг(А)-LS 3x2.5 не проходит по нагреву "
                       "при токе нагрузки 32 А (требуется 4 мм²).",
        "severity": "КРИТИЧЕСКОЕ",
        "section": "EOM",
        "recommendation": "Заменить кабель на ВВГнг(А)-LS 3x4.",
        "evidence": [{"type": "block_reference", "block_id": "BLK-001",
                      "page": 5, "text": "Кабель ВВГнг(А)-LS 3x2.5"}],
        "related_block_ids": ["BLK-001"],
        "impact_area": "safety",
        "has_evidence": True,
        "has_action": True,
        "has_impact": True,
    },
    {
        "id": "F-002",
        "title": "Опечатка в обозначении схемы",
        "description": "На листе 7 указано «УЗО» вместо «АВДТ».",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "section": "EOM",
        "recommendation": "Исправить обозначение.",
        "evidence": [],
        "related_block_ids": [],
    },
    {
        "id": "F-003",
        "title": "Нечитаемая отметка",
        "description": "Текст блока неразборчив, возможно OCR-артефакт.",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "section": "EOM",
        "recommendation": "Перепроверить.",
        "evidence": [],
    },
]

_SAMPLE_BLOCKS = {
    "blocks": [
        {"id": "BLK-001", "page": 5, "text": "Кабель ВВГнг(А)-LS 3x2.5",
         "start": 0, "end": 30, "type": "paragraph"},
    ]
}


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Synthetic project layout with _output/03_findings.json + blocks."""
    p = tmp_path / "synthetic-project"
    out = p / "_output"
    out.mkdir(parents=True)
    (out / "03_findings.json").write_text(
        json.dumps({"findings": _SAMPLE_FINDINGS}, ensure_ascii=False),
        encoding="utf-8",
    )
    (out / "02_blocks_analysis.json").write_text(
        json.dumps(_SAMPLE_BLOCKS, ensure_ascii=False),
        encoding="utf-8",
    )
    return p


# ─── Core runner behaviour ────────────────────────────────────────────────────


def test_runner_creates_all_artifacts(project_dir: Path):
    res = run_critic_v2_triage(project_dir, project_id=project_dir.name)
    assert res.success, res.error
    assert res.findings_total == 3
    assert res.triage_total >= 1

    artifacts_dir = project_dir / "_output" / "critic_v2"
    assert artifacts_dir.is_dir()
    for fname in (ARTIFACT_TRIAGE, ARTIFACT_TRIAGE_UI, ARTIFACT_INLINE_MAP,
                  ARTIFACT_METRICS, ARTIFACT_STAGE_SUMMARY):
        path = artifacts_dir / fname
        assert path.exists(), f"missing {fname}"
        assert path.stat().st_size > 0, f"empty {fname}"


def test_runner_does_not_modify_production_artifacts(project_dir: Path):
    findings_path = project_dir / "_output" / "03_findings.json"
    blocks_path = project_dir / "_output" / "02_blocks_analysis.json"
    before_f = findings_path.stat().st_mtime_ns
    before_b = blocks_path.stat().st_mtime_ns
    before_content_f = findings_path.read_bytes()
    before_content_b = blocks_path.read_bytes()

    res = run_critic_v2_triage(project_dir, project_id="x")
    assert res.success

    assert findings_path.stat().st_mtime_ns == before_f
    assert blocks_path.stat().st_mtime_ns == before_b
    assert findings_path.read_bytes() == before_content_f
    assert blocks_path.read_bytes() == before_content_b


def test_runner_does_not_create_03_findings_review(project_dir: Path):
    """Не должен создавать legacy critic artifact."""
    res = run_critic_v2_triage(project_dir, project_id="x")
    assert res.success
    assert not (project_dir / "_output" / "03_findings_review.json").exists()
    assert not (project_dir / "_output" / "expert_review.json").exists()


def test_runner_writes_only_in_output_subdir(project_dir: Path):
    """Все новые файлы должны быть строго под _output/critic_v2/."""
    out = project_dir / "_output"
    files_before = {p.relative_to(out) for p in out.rglob("*") if p.is_file()}
    res = run_critic_v2_triage(project_dir, project_id="x")
    assert res.success
    files_after = {p.relative_to(out) for p in out.rglob("*") if p.is_file()}
    new_files = files_after - files_before
    assert new_files, "no new artifacts created"
    for rel in new_files:
        assert rel.parts[0] == "critic_v2", (
            f"runner wrote outside _output/critic_v2/: {rel}"
        )


def test_runner_works_without_blocks_analysis(tmp_path: Path):
    """02_blocks_analysis.json отсутствует → должно работать (без evidence_quality boost)."""
    p = tmp_path / "no-blocks-project"
    (p / "_output").mkdir(parents=True)
    (p / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": _SAMPLE_FINDINGS}, ensure_ascii=False),
        encoding="utf-8",
    )
    res = run_critic_v2_triage(p, project_id="x")
    assert res.success, res.error
    assert (p / "_output" / "critic_v2" / ARTIFACT_TRIAGE_UI).exists()
    summary = json.loads((p / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY).read_text())
    assert summary["inputs"]["blocks_analysis_present"] is False
    assert summary["inputs"]["document_graph_present"] is False


def test_runner_fails_gracefully_without_findings(tmp_path: Path):
    """Нет 03_findings.json → success=False, error читаемый, ничего не пишется."""
    p = tmp_path / "empty-project"
    (p / "_output").mkdir(parents=True)
    res = run_critic_v2_triage(p, project_id="x")
    assert not res.success
    assert "03_findings.json" in (res.error or "")
    # И главное: НИКАКОЙ critic_v2/ папки не создано
    assert not (p / "_output" / "critic_v2").exists()


def test_runner_handles_empty_findings_list(tmp_path: Path):
    p = tmp_path / "zero-findings"
    (p / "_output").mkdir(parents=True)
    (p / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": []}, ensure_ascii=False), encoding="utf-8",
    )
    res = run_critic_v2_triage(p, project_id="x")
    assert not res.success
    assert "no findings" in (res.error or "").lower()


def test_runner_supports_alt_findings_schema_items(tmp_path: Path):
    """Старая schema {'items': [...]} тоже должна распарситься."""
    p = tmp_path / "alt-schema"
    (p / "_output").mkdir(parents=True)
    (p / "_output" / "03_findings.json").write_text(
        json.dumps({"items": _SAMPLE_FINDINGS}, ensure_ascii=False), encoding="utf-8",
    )
    res = run_critic_v2_triage(p, project_id="x")
    assert res.success
    assert res.findings_total == 3


def test_runner_invalid_profile_falls_back(project_dir: Path):
    res = run_critic_v2_triage(project_dir, project_id="x", profile="nonsense")
    assert res.success
    assert res.profile == "conservative"  # fallback


def test_runner_accepts_known_profiles(project_dir: Path):
    for prof in ("conservative", "assisted", "aggressive", "assisted_round1"):
        # каждый раз перезаписываем — runner идемпотентен
        res = run_critic_v2_triage(project_dir, project_id="x", profile=prof)
        assert res.success, f"{prof}: {res.error}"
        assert res.profile == prof


def test_runner_does_not_call_llm_even_when_flag_true(project_dir: Path, caplog):
    """llm_enabled=True должен дать warning + всё равно offline-режим."""
    res = run_critic_v2_triage(project_dir, project_id="x", llm_enabled=True)
    assert res.success
    summary_path = project_dir / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["llm_called"] is False
    assert summary["llm_enabled"] is False


# ─── Artifact shape ───────────────────────────────────────────────────────────


def test_triage_ui_artifact_shape(project_dir: Path):
    res = run_critic_v2_triage(project_dir, project_id="synthetic-project")
    assert res.success
    ui = json.loads(
        (project_dir / "_output" / "critic_v2" / ARTIFACT_TRIAGE_UI).read_text()
    )
    # Совместимость с experimental UI
    assert ui["experimental"] is True
    assert "summary" in ui and "tabs" in ui and "items" in ui
    assert ui["profile"] == "conservative"
    assert ui.get("generated_at")
    assert ui["source_project"]["project_id"] == "synthetic-project"
    assert ui["production_pipeline_modified"] is False
    assert len(ui["tabs"]) == 4
    # Каждый item должен содержать минимальный набор полей
    for item in ui["items"]:
        for key in ("finding_id", "tab", "queue", "reason", "evidence_quality",
                    "score", "taxonomy_reason", "source_dependency"):
            assert key in item, f"item missing {key}: {item}"


def test_inline_map_artifact_shape(project_dir: Path):
    res = run_critic_v2_triage(project_dir, project_id="x")
    assert res.success
    inline = json.loads(
        (project_dir / "_output" / "critic_v2" / ARTIFACT_INLINE_MAP).read_text()
    )
    assert "map" in inline and inline["map"]
    for fid, entry in inline["map"].items():
        assert isinstance(fid, str)
        for key in ("score", "label", "queue", "reason",
                    "hidden_by_default", "evidence_quality",
                    "taxonomy_reason", "source_dependency"):
            assert key in entry, f"{fid} missing {key}: {entry}"
        assert 0 <= entry["score"] <= 100
        assert isinstance(entry["hidden_by_default"], bool)


def test_stage_summary_artifact_shape(project_dir: Path):
    res = run_critic_v2_triage(project_dir, project_id="x")
    assert res.success
    summary = json.loads(
        (project_dir / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY).read_text()
    )
    assert summary["experimental"] is True
    assert summary["production_pipeline_modified"] is False
    assert summary["llm_called"] is False
    assert "counts" in summary
    assert "artifacts" in summary
    assert ARTIFACT_TRIAGE_UI in summary["artifacts"]


# ─── Config defaults: production pipeline OFF ────────────────────────────────


def test_default_config_flags_keep_critic_v2_disabled(monkeypatch: pytest.MonkeyPatch):
    """Без env vars CRITIC_V2_ENABLED должен быть False."""
    for k in ("CRITIC_V2_ENABLED", "CRITIC_V2_PROFILE", "CRITIC_V2_LLM_ENABLED",
              "CRITIC_V2_FAILS_PIPELINE", "CRITIC_V2_OUTPUT_SUBDIR"):
        monkeypatch.delenv(k, raising=False)

    import backend.app.core.config as cfg
    importlib.reload(cfg)
    try:
        assert cfg.CRITIC_V2_ENABLED is False
        assert cfg.CRITIC_V2_LLM_ENABLED is False
        assert cfg.CRITIC_V2_FAILS_PIPELINE is False
        assert cfg.CRITIC_V2_PROFILE == "conservative"
        assert cfg.CRITIC_V2_OUTPUT_SUBDIR == "critic_v2"
    finally:
        # Восстанавливаем модуль на случай, если другие тесты зависят
        importlib.reload(cfg)


def test_env_overrides_apply(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("CRITIC_V2_ENABLED", "true")
    monkeypatch.setenv("CRITIC_V2_PROFILE", "assisted")
    monkeypatch.setenv("CRITIC_V2_OUTPUT_SUBDIR", "critic_v2_alt")
    import backend.app.core.config as cfg
    importlib.reload(cfg)
    try:
        assert cfg.CRITIC_V2_ENABLED is True
        assert cfg.CRITIC_V2_PROFILE == "assisted"
        assert cfg.CRITIC_V2_OUTPUT_SUBDIR == "critic_v2_alt"
    finally:
        for k in ("CRITIC_V2_ENABLED", "CRITIC_V2_PROFILE", "CRITIC_V2_OUTPUT_SUBDIR"):
            monkeypatch.delenv(k, raising=False)
        importlib.reload(cfg)


# ─── No LLM / no network sanity ──────────────────────────────────────────────


def test_no_network_calls_during_runner(project_dir: Path, monkeypatch):
    """Если runner попробует выйти в сеть — должен упасть. В offline-режиме не должен."""
    import socket
    orig_socket = socket.socket

    def _blocked(*a, **kw):
        raise RuntimeError("network call attempted in offline runner")

    monkeypatch.setattr(socket, "socket", _blocked)
    try:
        res = run_critic_v2_triage(project_dir, project_id="x")
    finally:
        monkeypatch.setattr(socket, "socket", orig_socket)
    assert res.success, res.error


def test_runner_idempotent(project_dir: Path):
    """Повторный запуск перезаписывает artifacts без падения."""
    res1 = run_critic_v2_triage(project_dir, project_id="x")
    assert res1.success
    res2 = run_critic_v2_triage(project_dir, project_id="x")
    assert res2.success
    assert res1.triage_total == res2.triage_total


def test_runner_uses_custom_output_subdir(project_dir: Path):
    res = run_critic_v2_triage(project_dir, project_id="x", output_subdir="critic_v2_alt")
    assert res.success
    assert (project_dir / "_output" / "critic_v2_alt" / ARTIFACT_STAGE_SUMMARY).exists()
    assert not (project_dir / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY).exists()
