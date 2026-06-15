"""
readiness.py — оценка готовности legacy-проектов к миграции в projects_v2.

Чистая логика классификации (`classify_readiness`) отделена от сбора сигналов
с файловой системы, чтобы её можно было покрыть юнит-тестами без реального
дерева. READ-ONLY к legacy — ничего не копирует и не меняет.

Группы:
  AUTO_SAFE                   — мигрировать можно автоматически
  CAN_MIGRATE_WITH_WARNINGS   — мигрировать можно, но есть мелкие warning
  MANUAL_REVIEW_REQUIRED      — требуется ручной разбор перед миграцией
  SKIP_EMPTY_OR_INVALID       — пусто/мусор/не проект
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402

AUTO_SAFE = "AUTO_SAFE"
ALREADY_MIGRATED = "ALREADY_MIGRATED"
CAN_MIGRATE_WITH_WARNINGS = "CAN_MIGRATE_WITH_WARNINGS"
MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
SKIP_EMPTY_OR_INVALID = "SKIP_EMPTY_OR_INVALID"

READINESS_GROUPS = (
    AUTO_SAFE,
    ALREADY_MIGRATED,
    CAN_MIGRATE_WITH_WARNINGS,
    MANUAL_REVIEW_REQUIRED,
    SKIP_EMPTY_OR_INVALID,
)

# --- warning-policy подгруппы (для уже-warning проектов) ---
WARNINGS_AUTO_CANDIDATE = "WARNINGS_AUTO_CANDIDATE"
WARNINGS_NEED_POLICY = "WARNINGS_NEED_POLICY"
WARNINGS_BLOCKED = "WARNINGS_BLOCKED"

WARNING_POLICY_GROUPS = (
    WARNINGS_AUTO_CANDIDATE,
    WARNINGS_NEED_POLICY,
    WARNINGS_BLOCKED,
)

# рекомендации
REC_CAN_BATCH = "can_batch_migrate"
REC_NEEDS_POLICY = "needs_policy"
REC_MANUAL_ONLY = "manual_only"

# warning-теги: безопасные для авто-батча vs требующие политики
_AUTO_CANDIDATE_WARNINGS = frozenset({"messy_legacy_artifacts", "no_analysis"})
_NEED_POLICY_WARNINGS = frozenset({
    "missing_ocr_html", "object_id_not_in_registry", "v2_present_not_in_map",
})

# поля сигнала, которые читает classify_readiness (для тестов — конструируются напрямую)
SIGNAL_DEFAULTS = {
    "object": "",
    "object_id": "",
    "discipline": "",
    "project_name": "",
    "document_code": "",
    "kind": "plain",            # plain | container
    "version_count": 1,
    "has_pdf": False,
    "has_document_md": False,
    "has_ocr_html": False,
    "has_result_json": False,
    "has_project_info": False,
    "has_output": False,
    "has_analysis": False,
    "has_version_group": False,
    "pdf_named_version_folder": False,
    "multiple_pdf": False,
    "multiple_document_md": False,
    "multiple_result_json": False,
    "messy_legacy_artifacts": False,
    "v2_already_migrated": False,   # document.json существует в projects_v2
    "recorded_in_map": False,       # запись есть в old_to_new_map.json
    "document_code_conflict": False,
    "object_resolved": True,    # object_id найден в objects.json (не fallback-хэш)
    "legacy_path": "",
}


def _compute_blockers(s: dict) -> list[str]:
    """Жёсткие блокеры (→ MANUAL / WARNINGS_BLOCKED)."""
    blockers: list[str] = []
    full_required = s["has_pdf"] and s["has_document_md"] and s["has_result_json"]
    if not full_required:
        blockers.append("incomplete_input_quad")
    if s["multiple_pdf"]:
        blockers.append("multiple_pdf")
    if s["multiple_document_md"]:
        blockers.append("multiple_document_md")
    if s["multiple_result_json"]:
        blockers.append("multiple_result_json")
    if not s["has_project_info"]:
        blockers.append("missing_project_info")
    if s["document_code_conflict"]:
        blockers.append("document_code_conflict")
    if s["kind"] == "container" and not s["has_version_group"]:
        blockers.append("container_without_version_group")
    return blockers


def _compute_warnings(s: dict) -> list[str]:
    """Минорные warning'и (миграция технически возможна)."""
    warnings: list[str] = []
    if s["pdf_named_version_folder"]:
        warnings.append("pdf_in_version_folder_name")
    if not s["has_ocr_html"]:
        warnings.append("missing_ocr_html")
    if not s["has_analysis"]:
        warnings.append("no_analysis")
    if s["messy_legacy_artifacts"]:
        warnings.append("messy_legacy_artifacts")
    if not s["object_resolved"]:
        warnings.append("object_id_not_in_registry")
    # v2 есть, но карта его не знает — несогласованное частичное состояние
    if s["v2_already_migrated"] and not s.get("recorded_in_map", False):
        warnings.append("v2_present_not_in_map")
    return warnings


def is_already_migrated(s: dict) -> bool:
    """Проект мигрирован, если есть document.json в projects_v2 И запись в old_to_new_map."""
    return bool(s.get("v2_already_migrated") and s.get("recorded_in_map"))


def classify_readiness(signal: dict) -> dict:
    """Чистая классификация одного проекта.

    Возвращает {group, warnings, blockers}. Приоритет (worst-first):
    ALREADY_MIGRATED > SKIP > MANUAL > WARNINGS > AUTO_SAFE.
    """
    s = {**SIGNAL_DEFAULTS, **signal}

    if is_already_migrated(s):
        return {"group": ALREADY_MIGRATED, "warnings": [], "blockers": []}

    blockers = _compute_blockers(s)
    warnings = _compute_warnings(s)

    empty = (
        not s["has_pdf"] and not s["has_document_md"]
        and not s["has_result_json"] and not s["has_output"]
    )
    if empty:
        group = SKIP_EMPTY_OR_INVALID
    elif blockers:
        group = MANUAL_REVIEW_REQUIRED
    elif warnings:
        group = CAN_MIGRATE_WITH_WARNINGS
    else:
        group = AUTO_SAFE

    return {"group": group, "warnings": warnings, "blockers": blockers}


def classify_warning_policy(signal: dict) -> dict:
    """Политика для НЕ-мигрированных, НЕ-AUTO_SAFE проектов.

    Делит на WARNINGS_AUTO_CANDIDATE / WARNINGS_NEED_POLICY / WARNINGS_BLOCKED.
    Приоритет: blocker > need_policy > auto_candidate.
    Возвращает {policy_group, recommendation, warning_tags, blockers, reason}.
    """
    s = {**SIGNAL_DEFAULTS, **signal}

    blockers = _compute_blockers(s)
    if blockers:
        return {
            "policy_group": WARNINGS_BLOCKED,
            "recommendation": REC_MANUAL_ONLY,
            "warning_tags": _compute_warnings(s),
            "blockers": blockers,
            "reason": "blockers: " + ", ".join(blockers),
        }

    warnings = _compute_warnings(s)
    auto_tags: list[str] = []
    need_tags: list[str] = []
    for w in warnings:
        if w == "pdf_in_version_folder_name":
            # .pdf в имени папки версии безопасно только при корректном version_group
            (auto_tags if s["has_version_group"] else need_tags).append(w)
        elif w in _AUTO_CANDIDATE_WARNINGS:
            auto_tags.append(w)
        elif w in _NEED_POLICY_WARNINGS:
            need_tags.append(w)
        else:
            need_tags.append(w)  # неизвестный warning → консервативно needs_policy

    if need_tags:
        return {
            "policy_group": WARNINGS_NEED_POLICY,
            "recommendation": REC_NEEDS_POLICY,
            "warning_tags": warnings,
            "blockers": [],
            "reason": "needs policy: " + ", ".join(need_tags),
        }
    return {
        "policy_group": WARNINGS_AUTO_CANDIDATE,
        "recommendation": REC_CAN_BATCH,
        "warning_tags": warnings,
        "blockers": [],
        "reason": ("auto-safe warnings: " + ", ".join(auto_tags)) if auto_tags
                  else "no blocking signals",
    }


# ---------------------------------------------------------------------------
# Сбор сигналов с файловой системы (read-only)
# ---------------------------------------------------------------------------


def _count_suffix(version_dir: Path, suffix: str, *, files_only_pdf: bool = False) -> int:
    if not version_dir.is_dir():
        return 0
    n = 0
    for e in version_dir.iterdir():
        if not e.is_file():
            continue
        if files_only_pdf:
            if e.name.lower().endswith(".pdf"):
                n += 1
        elif e.name.endswith(suffix):
            n += 1
    return n


def _has_messy_legacy(version_dir: Path) -> bool:
    """Признаки «грязной» legacy-структуры (НЕ нормальные _output intermediate).

    Нормальные block_batch_*/03_findings_pre_* в _output не считаем грязью —
    мигратор копирует _output целиком verbatim. Грязь — это .broken/.bak в
    корне версии, legacy `_versions/`, *.bak_* / _backup_* папки.
    """
    if not version_dir.is_dir():
        return False
    if (version_dir / "_versions").is_dir():
        return True
    for e in version_dir.iterdir():
        nm = e.name
        if nm.endswith((".broken", ".bak")):
            return True
        if e.is_dir() and (".bak_" in nm or nm.startswith("_backup")):
            return True
    return False


def load_migrated_keys(v2_root: Path) -> set:
    """Множество (object_id, document_code) из old_to_new_map.json."""
    map_path = v2_root / "_system" / "old_to_new_map.json"
    keys: set = set()
    if not map_path.exists():
        return keys
    try:
        data = v2lib.load_old_to_new_map(map_path)
    except Exception:
        return keys
    for m in data.get("migrations", []):
        keys.add((m.get("object_id"), m.get("document_code")))
    return keys


def build_signal(object_dir: Path, discipline: str, project_path: Path,
                 objects_map: dict, *, v2_root: Optional[Path] = None,
                 migrated_keys: Optional[set] = None) -> dict:
    """Собирает сигнал для одного legacy-проекта/контейнера (read-only)."""
    kind = "container" if v2lib.is_version_container(project_path) else "plain"
    versions = v2lib.enumerate_versions(project_path)
    primary = versions[0]
    vdir = primary.legacy_folder

    object_id = v2lib.object_id_for(object_dir, objects_map)
    object_resolved = (
        str(project_path.parent.parent.resolve()) in objects_map.get("by_path", {})
        or object_dir.name in objects_map.get("by_name", {})
    )
    document_code = v2lib.document_code_for(project_path)

    quad = v2lib.find_input_quad(vdir)
    output_dir = vdir / "_output"
    has_output = output_dir.is_dir()
    has_analysis = has_output and (
        (output_dir / "03_findings.json").exists()
        or (output_dir / "01_text_analysis.json").exists()
    )

    # multiple-file / messy / .pdf-name проверяем по ВСЕМ версиям
    multiple_pdf = multiple_md = multiple_result = False
    pdf_named = False
    messy = False
    for v in versions:
        if _count_suffix(v.legacy_folder, ".pdf", files_only_pdf=True) > 1:
            multiple_pdf = True
        if _count_suffix(v.legacy_folder, "_document.md") > 1:
            multiple_md = True
        if _count_suffix(v.legacy_folder, "_result.json") > 1:
            multiple_result = True
        if v.legacy_name.lower().endswith(".pdf"):
            pdf_named = True
        if _has_messy_legacy(v.legacy_folder):
            messy = True

    v2_migrated = False
    if v2_root is not None:
        doc_dir = v2lib.document_dir_in_v2(v2_root, object_id, discipline, document_code)
        v2_migrated = (doc_dir / "document.json").exists()
    recorded_in_map = bool(migrated_keys is not None
                           and (object_id, document_code) in migrated_keys)

    return {
        "object": object_dir.name,
        "object_id": object_id,
        "discipline": discipline,
        "project_name": project_path.name,
        "document_code": document_code,
        "kind": kind,
        "version_count": len(versions),
        "has_pdf": quad["pdf"] is not None,
        "has_document_md": quad["document_md"] is not None,
        "has_ocr_html": quad["ocr_html"] is not None,
        "has_result_json": quad["result_json"] is not None,
        "has_project_info": (vdir / "project_info.json").exists(),
        "has_output": has_output,
        "has_analysis": has_analysis,
        "has_version_group": (project_path / v2lib.VERSION_GROUP_FILENAME).exists(),
        "pdf_named_version_folder": pdf_named,
        "multiple_pdf": multiple_pdf,
        "multiple_document_md": multiple_md,
        "multiple_result_json": multiple_result,
        "messy_legacy_artifacts": messy,
        "v2_already_migrated": v2_migrated,
        "recorded_in_map": recorded_in_map,
        "object_resolved": bool(object_resolved),
        "legacy_path": str(project_path),
        "document_code_conflict": False,  # заполняется на втором проходе
    }


def detect_document_code_conflicts(signals: list[dict]) -> dict:
    """Помечает document_code_conflict для проектов с одинаковым
    (object_id, discipline, document_code). Возвращает карту конфликтов.
    """
    groups: dict[tuple, list[dict]] = {}
    for s in signals:
        key = (s["object_id"], s["discipline"], s["document_code"])
        groups.setdefault(key, []).append(s)
    conflicts = {}
    for key, members in groups.items():
        if len(members) > 1:
            conflicts[key] = [m["legacy_path"] for m in members]
            for m in members:
                m["document_code_conflict"] = True
    return conflicts


def find_bare_pdfs(projects_root: Path) -> list[str]:
    """Голые PDF-файлы прямо в папке дисциплины (без папки проекта)."""
    bare: list[str] = []
    if not projects_root.is_dir():
        return bare
    for object_dir in sorted(p for p in projects_root.iterdir() if p.is_dir()):
        if object_dir.name.startswith((".", "_")):
            continue
        for disc_dir in sorted(p for p in object_dir.iterdir() if p.is_dir()):
            if disc_dir.name.startswith((".", "_")) or disc_dir.name == "__BATCH__":
                continue
            for e in disc_dir.iterdir():
                if e.is_file() and e.name.lower().endswith(".pdf"):
                    bare.append(str(e))
    return bare


def build_readiness(projects_root: Path, objects_map: dict, *,
                    v2_root: Optional[Path] = None,
                    migrated_keys: Optional[set] = None) -> list[dict]:
    """Полный read-only проход: сигналы + конфликты + классификация."""
    if migrated_keys is None and v2_root is not None:
        migrated_keys = load_migrated_keys(v2_root)
    signals = []
    for object_dir, discipline, project_path in v2lib.iter_legacy_projects(projects_root):
        signals.append(build_signal(object_dir, discipline, project_path,
                                    objects_map, v2_root=v2_root,
                                    migrated_keys=migrated_keys))
    detect_document_code_conflicts(signals)
    rows = []
    for s in signals:
        verdict = classify_readiness(s)
        rows.append({**s,
                     "group": verdict["group"],
                     "warnings": verdict["warnings"],
                     "blockers": verdict["blockers"]})
    return rows
