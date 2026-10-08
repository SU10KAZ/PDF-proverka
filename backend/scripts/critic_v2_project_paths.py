"""Пути проектов для офлайн-скриптов critic_v2 в раскладке projects_v2.

`batch_critic_v2.py` и `benchmark_critic_v2_against_human.py` писались под
legacy `projects/<КОД>/<имя>/_output/`. Эта папка выведена из эксплуатации и
удалена, и оба скрипта молча находили ноль проектов.

Единица здесь — папка ТЕКУЩЕЙ версии документа
(`projects_v2/objects/<объект>/disciplines/<КОД>/documents/<документ>/versions/vNNN`):
старые версии и снимки `03_analysis/runs/<run_id>/` задвоили бы выборку.
Каждый артефакт ищется сначала по канону v2, затем в `_output/` — у части
версий он ещё лежит по-старому.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator

_ROOT = Path(__file__).resolve().parents[2]
PROJECTS_ROOT = _ROOT / "projects_v2" / "objects"

# имя артефакта → подпапки версии по приоритету
_LOCATIONS: dict[str, tuple[str, ...]] = {
    "expert_review.json": ("04_review", "03_analysis/latest", "_output"),
    "03_findings_review.json": ("03_analysis/latest", "04_review", "_output"),
    "project_info.json": ("01_input", "."),
}
_DEFAULT_LOCATIONS = ("03_analysis/latest", "_output")


def artifact_path(project_dir: Path, name: str) -> Path:
    """Путь к артефакту версии; если его нет нигде — канонический путь v2."""
    project_dir = Path(project_dir)
    locations = _LOCATIONS.get(name, _DEFAULT_LOCATIONS)
    for location in locations:
        candidate = project_dir / location / name
        if candidate.exists():
            return candidate
    return project_dir / locations[0] / name


def artifact_dir(project_dir: Path) -> Path:
    """Каталог артефактов анализа версии (для resolve_existing)."""
    project_dir = Path(project_dir)
    latest = project_dir / "03_analysis" / "latest"
    if latest.is_dir() or not (project_dir / "_output").is_dir():
        return latest
    return project_dir / "_output"


def current_version_dirs(root: Path | None = None) -> Iterator[Path]:
    """Папки текущих версий всех документов под `root`."""
    base = Path(root) if root is not None else PROJECTS_ROOT
    for document in sorted(base.glob("*/disciplines/*/documents/*")):
        marker = document / "current_version.txt"
        if not marker.is_file():
            continue
        version = document / "versions" / marker.read_text(encoding="utf-8").strip()
        if version.is_dir():
            yield version


def discipline_code(project_dir: Path) -> str | None:
    """Код дисциплины из пути версии (`.../disciplines/<КОД>/documents/...`)."""
    parts = Path(project_dir).parts
    if "disciplines" in parts:
        index = parts.index("disciplines")
        if index + 1 < len(parts):
            return parts[index + 1]
    return None


def project_label(project_dir: Path) -> str:
    """Имя для отчёта: у версии v2 — папка документа, а не «v001»."""
    project_dir = Path(project_dir)
    if project_dir.parent.name == "versions":
        return project_dir.parent.parent.name
    return project_dir.name
