"""
Статус конвейера в projects_v2: журнал этапов и артефакты анализа лежат в
РАЗНЫХ папках версии.

Мигрированные версии хранят `pipeline_log.json` в `99_service/`, а
`01/02/03_*.json` — в `03_analysis/latest/`. `_build_pipeline_summary`
получает папку журнала, поэтому inference «артефакт на ФС → done» искал
артефакты не там и завершённый аудит показывался пустым конвейером: замечания
на вкладке есть, а все этапы — pending (реальный случай КЖ6-К1К2 V2, 36
замечаний при двух done в статусе).

Гермётичны (tmp_path).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root
from backend.app.services.common.project_service import (  # noqa: E402
    _build_pipeline_summary,
)
from backend.app.services.storage.projects_v2_adapter import (  # noqa: E402
    ProjectsV2Adapter,
)
from backend.app.services.storage.read_canary import _v2_pipeline_summary  # noqa: E402

OBJF = "214_Alia_ASTERUS"
CODE = "13АВ-РД-КЖ6-К1К2"
VID = "v002"

# Журнал обрывочный: основной аудит писал его по старому пути проекта, а
# после переименования папки в новом месте записались только эти этапы.
PARTIAL_LOG = {
    "version": 1,
    "stages": {
        "crop_blocks": {"status": "done", "message": "OK (0 новых, 78 пропущено, 0 ошибок)"},
        "gemma_enrichment": {"status": "done", "message": "OK (78/78 блоков, 888s)"},
    },
}

# Артефакты завершённого аудита — в latest.
LATEST_ARTIFACTS = (
    "02_text_analysis.json",
    "01_blocks_analysis.json",
    "03_findings.json",
    "norm_checks.json",
    "optimization.json",
)


def _w(p: Path, data) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _build_tree(tmp_path: Path) -> tuple[Path, Path, Path]:
    """v2-дерево: журнал в 99_service, артефакты в latest.

    Возвращает (v2_root, doc_dir, latest_dir).
    """
    v2 = tmp_path / "projects_v2"
    _w(v2 / "objects" / OBJF / "object.json",
       {"object_id": "73a0e59a", "display_name": "214 Alia", "folder_name": OBJF,
        "legacy_path": "/legacy/214"})
    doc = v2 / "objects" / OBJF / "disciplines" / "KJ" / "documents" / CODE
    _w(doc / "document.json",
       {"schema_version": 1, "document_code": CODE, "object_id": "73a0e59a",
        "discipline": "KJ", "kind": "container",
        "versions": [{"version_id": VID, "version_no": 2}], "current_version": VID})
    _w(doc / "current_version.txt", VID)
    vroot = doc / "versions" / VID
    _w(vroot / "version.json",
       {"schema_version": 1, "version_id": VID, "version_no": 2,
        "analysis_status": "complete"})
    _w(vroot / "99_service" / "pipeline_log.json", PARTIAL_LOG)
    latest = vroot / "03_analysis" / "latest"
    for name in LATEST_ARTIFACTS:
        _w(latest / name, {"items": [{"id": "F-001"}]})
    return v2, doc, latest


def _by_key(summary: list[dict]) -> dict[str, str]:
    return {s["key"]: s["status"] for s in summary}


def test_artifacts_dir_recovers_stages_missing_from_log(tmp_path):
    """Артефакты в latest → этапы done, хотя журнал о них молчит."""
    _, doc, latest = _build_tree(tmp_path)
    log_dir = doc / "versions" / VID / "99_service"

    st = _by_key(_build_pipeline_summary(log_dir, artifacts_dir=latest))

    for key in ("text_analysis", "block_analysis", "findings_merge",
                "norm_verify", "optimization"):
        assert st[key] == "done", f"{key} должен быть done по артефакту в latest"
    # Записи журнала остаются приоритетными и не теряются.
    assert st["crop_blocks"] == "done"
    assert st["gemma_enrichment"] == "done"


def test_without_artifacts_dir_legacy_behaviour_unchanged(tmp_path):
    """Без artifacts_dir поведение прежнее: артефакты ищутся рядом с журналом."""
    _, doc, _ = _build_tree(tmp_path)
    log_dir = doc / "versions" / VID / "99_service"

    st = _by_key(_build_pipeline_summary(log_dir))

    assert st["crop_blocks"] == "done"
    for key in ("text_analysis", "block_analysis", "findings_merge", "optimization"):
        assert st[key] == "pending"


def test_artifacts_dir_does_not_override_log_status(tmp_path):
    """Журнал остаётся источником истины: error не превращается в done."""
    _, doc, latest = _build_tree(tmp_path)
    log_dir = doc / "versions" / VID / "99_service"
    log = json.loads((log_dir / "pipeline_log.json").read_text(encoding="utf-8"))
    log["stages"]["findings_merge"] = {"status": "error", "error": "провайдер отказал"}
    _w(log_dir / "pipeline_log.json", log)

    entry = next(s for s in _build_pipeline_summary(log_dir, artifacts_dir=latest)
                 if s["key"] == "findings_merge")

    assert entry["status"] == "error"
    assert entry["error"] == "провайдер отказал"


def test_read_canary_summary_wires_latest_as_artifacts_dir(tmp_path):
    """Живой путь фронта (read_canary) сам находит папку артефактов."""
    v2, doc, _ = _build_tree(tmp_path)
    adapter = ProjectsV2Adapter(v2)

    st = _by_key(_v2_pipeline_summary(adapter, doc, VID))

    assert st["findings_merge"] == "done"
    assert st["text_analysis"] == "done"
    assert st["block_analysis"] == "done"
    assert st["crop_blocks"] == "done"
