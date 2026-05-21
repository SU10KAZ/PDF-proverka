"""External Register — persistence, finding mutation, coverage queries."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from backend.app.core.config import (
    EXTERNAL_REGISTERS_DIR,
    EXTERNAL_REGISTER_MATCH_THRESHOLD,
    EXTERNAL_REGISTER_REVIEW_THRESHOLD,
)
from backend.app.services.common.object_service import (
    get_object_by_id,
    get_projects_dir_for,
)
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.services.external_register import parser, section_map
from backend.app.services.external_register.models import (
    CustomerResponse,
    FindingMatch,
    MatchStatus,
    RegisterEntry,
    RegisterFile,
)

logger = logging.getLogger(__name__)


# ─── Хранилище реестров ───────────────────────────────────────────────────


def _register_path(object_id: str, register_id: str) -> Path:
    EXTERNAL_REGISTERS_DIR.mkdir(parents=True, exist_ok=True)
    safe_object = object_id.replace("/", "_")
    safe_register = register_id.replace("/", "_")
    return EXTERNAL_REGISTERS_DIR / f"{safe_object}__{safe_register}.json"


def load_register(object_id: str, register_id: str) -> Optional[RegisterFile]:
    path = _register_path(object_id, register_id)
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load register %s: %s", path, e)
        return None
    try:
        return RegisterFile(**raw)
    except Exception as e:
        logger.error("Register %s validation failed: %s", path, e)
        return None


def save_register(register: RegisterFile) -> None:
    path = _register_path(register.object_id, register.register_id)
    payload = json.loads(register.model_dump_json())
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def list_registers(object_id: str) -> list[str]:
    EXTERNAL_REGISTERS_DIR.mkdir(parents=True, exist_ok=True)
    prefix = f"{object_id.replace('/', '_')}__"
    out: list[str] = []
    for f in EXTERNAL_REGISTERS_DIR.glob(f"{prefix}*.json"):
        name = f.stem[len(prefix):]
        out.append(name)
    return sorted(out)


# ─── Импорт из markdown ───────────────────────────────────────────────────


def import_register(
    object_id: str,
    register_id: str,
    source_md_path: str | Path,
) -> RegisterFile:
    """Парсит markdown реестра и сохраняет в EXTERNAL_REGISTERS_DIR.

    Если такой register_id уже существовал — перезаписывает entries, но
    сохраняет user_confirmed_by/at пометки из старой версии по key совпадению.
    """
    src = Path(source_md_path)
    if not src.exists():
        raise FileNotFoundError(f"Source not found: {src}")

    entries = parser.parse_file(src)

    unmapped: set[str] = set()
    for e in entries:
        if not section_map.lookup(e.section_code):
            unmapped.add(e.section_code or "(empty)")

    new_register = RegisterFile(
        register_id=register_id,
        object_id=object_id,
        source_md=str(src),
        imported_at=datetime.utcnow().isoformat(),
        entries=entries,
        unmapped_sections=sorted(unmapped),
    )

    # Сохранить ранее-проставленные подтверждения
    existing = load_register(object_id, register_id)
    if existing is not None:
        prior_by_key = {e.key: e for e in existing.entries}
        for entry in new_register.entries:
            prior = prior_by_key.get(entry.key)
            if prior and prior.user_confirmed_by:
                entry.match_status = prior.match_status
                entry.match = prior.match
                entry.user_confirmed_by = prior.user_confirmed_by
                entry.user_confirmed_at = prior.user_confirmed_at

    save_register(new_register)
    return new_register


# ─── Применение результата matcher'а ──────────────────────────────────────


def apply_auto_match(
    object_id: str,
    register_id: str,
    entry_key: str,
    match: FindingMatch,
) -> Optional[RegisterEntry]:
    """Зафиксировать в register'е результат LLM-сопоставления + проставить
    `external_register` поле на findin'е.

    Низкая confidence не пишется (matcher вызовом не передаёт такие).
    """
    register = load_register(object_id, register_id)
    if register is None:
        return None

    entry = _find_entry(register, entry_key)
    if entry is None:
        return None

    # Не перетирать ранее подтверждённый ручной match
    if entry.match_status == MatchStatus.CONFIRMED:
        return entry

    entry.match = match
    if match.confidence >= EXTERNAL_REGISTER_MATCH_THRESHOLD:
        entry.match_status = MatchStatus.AUTO_MATCHED
        _write_finding_external_register(object_id, register_id, entry, auto=True, user_confirmed=False)
    elif match.confidence >= EXTERNAL_REGISTER_REVIEW_THRESHOLD:
        entry.match_status = MatchStatus.NEEDS_REVIEW
    else:
        entry.match_status = MatchStatus.UNMATCHED
        entry.match = None

    save_register(register)
    return entry


def confirm_match(
    object_id: str,
    register_id: str,
    entry_key: str,
    user: str = "unknown",
    finding_id: Optional[str] = None,
    project_id: Optional[str] = None,
) -> Optional[RegisterEntry]:
    """Пользователь подтверждает (или вручную задаёт) match."""
    register = load_register(object_id, register_id)
    if register is None:
        return None
    entry = _find_entry(register, entry_key)
    if entry is None:
        return None

    if finding_id and project_id:
        entry.match = FindingMatch(
            project_id=project_id,
            finding_id=finding_id,
            confidence=1.0,
            rationale="Manual confirm",
        )

    if entry.match is None:
        return entry  # ничего не подтверждать

    entry.match_status = MatchStatus.CONFIRMED
    entry.user_confirmed_by = user
    entry.user_confirmed_at = datetime.utcnow().isoformat()

    _write_finding_external_register(object_id, register_id, entry, auto=False, user_confirmed=True)
    save_register(register)
    return entry


def reject_match(
    object_id: str,
    register_id: str,
    entry_key: str,
    user: str = "unknown",
) -> Optional[RegisterEntry]:
    """Пользователь отвергает предложенный match. Очищает поле на finding'е."""
    register = load_register(object_id, register_id)
    if register is None:
        return None
    entry = _find_entry(register, entry_key)
    if entry is None:
        return None

    if entry.match:
        _clear_finding_external_register(
            object_id, entry.match.project_id, entry.match.finding_id, entry.key
        )

    entry.match = None
    entry.match_status = MatchStatus.REJECTED
    entry.user_confirmed_by = user
    entry.user_confirmed_at = datetime.utcnow().isoformat()

    save_register(register)
    return entry


# ─── Сводки ───────────────────────────────────────────────────────────────


def coverage(object_id: str, register_id: str) -> Optional[dict]:
    register = load_register(object_id, register_id)
    if register is None:
        return None

    total = len(register.entries)
    status_counts: dict[str, int] = {}
    response_counts: dict[str, int] = {}
    section_counts: dict[str, dict[str, int]] = {}

    for entry in register.entries:
        status_counts[entry.match_status.value] = status_counts.get(entry.match_status.value, 0) + 1
        response_counts[entry.customer_response.value] = response_counts.get(entry.customer_response.value, 0) + 1

        sec = entry.section_code or "(unknown)"
        sec_bucket = section_counts.setdefault(sec, {"total": 0, "matched": 0})
        sec_bucket["total"] += 1
        if entry.match_status in (MatchStatus.AUTO_MATCHED, MatchStatus.CONFIRMED):
            sec_bucket["matched"] += 1

    return {
        "register_id": register.register_id,
        "object_id": register.object_id,
        "imported_at": register.imported_at,
        "matched_at": register.matched_at,
        "total": total,
        "matched": status_counts.get(MatchStatus.AUTO_MATCHED.value, 0)
                   + status_counts.get(MatchStatus.CONFIRMED.value, 0),
        "needs_review": status_counts.get(MatchStatus.NEEDS_REVIEW.value, 0),
        "unmatched": status_counts.get(MatchStatus.UNMATCHED.value, 0)
                     + status_counts.get(MatchStatus.REJECTED.value, 0),
        "by_status": status_counts,
        "by_customer_response": response_counts,
        "by_section": section_counts,
        "unmapped_sections": register.unmapped_sections,
    }


# ─── Mutating finding's 03_findings.json ──────────────────────────────────


def _write_finding_external_register(
    object_id: str,
    register_id: str,
    entry: RegisterEntry,
    *,
    auto: bool,
    user_confirmed: bool,
) -> None:
    """Записать external_register dict в 03_findings.json данного finding'а.

    Использует тот же паттерн, что и discussion_service._update_item_status:
    in-place мутация JSON-файла без бэкапа (atomicity на уровне ОС достаточно).
    """
    if entry.match is None:
        return

    proj_dir = resolve_project_dir(entry.match.project_id, object_id=object_id)
    findings_path = proj_dir / "_output" / "03_findings.json"
    if not findings_path.exists():
        logger.warning("Findings file missing for %s: %s", entry.match.project_id, findings_path)
        return

    try:
        data = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to read %s: %s", findings_path, e)
        return

    items = data.get("findings", data.get("items", []))
    payload = {
        "register_id": register_id,
        "comment_key": entry.key,
        "customer_response": entry.customer_response.value,
        "customer_comment": entry.customer_comment,
        "confidence": entry.match.confidence,
        "auto_matched": auto,
        "user_confirmed": user_confirmed,
        "updated_at": datetime.utcnow().isoformat(),
    }

    matched_any = False
    for item in items:
        if item.get("id") == entry.match.finding_id:
            item["external_register"] = payload
            matched_any = True
            break

    if not matched_any:
        logger.warning(
            "Finding id %s not found in %s",
            entry.match.finding_id,
            findings_path,
        )
        return

    findings_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _clear_finding_external_register(
    object_id: str,
    project_id: str,
    finding_id: str,
    expected_key: str,
) -> None:
    """Снять external_register поле с finding'а (только если ключ совпадает)."""
    proj_dir = resolve_project_dir(project_id, object_id=object_id)
    findings_path = proj_dir / "_output" / "03_findings.json"
    if not findings_path.exists():
        return
    try:
        data = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return

    items = data.get("findings", data.get("items", []))
    changed = False
    for item in items:
        if item.get("id") != finding_id:
            continue
        ext = item.get("external_register") or {}
        if ext.get("comment_key") == expected_key:
            item.pop("external_register", None)
            changed = True
        break

    if changed:
        findings_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


# ─── helpers ──────────────────────────────────────────────────────────────


def _find_entry(register: RegisterFile, key: str) -> Optional[RegisterEntry]:
    for e in register.entries:
        if e.key == key:
            return e
    return None


def mark_matched_at(object_id: str, register_id: str) -> None:
    register = load_register(object_id, register_id)
    if register is None:
        return
    register.matched_at = datetime.utcnow().isoformat()
    save_register(register)


def get_unmatched_findings(
    object_id: str,
    register_id: str,
) -> list[dict]:
    """Список finding_id, к которым в реестре есть match'и, vs все findings —
    для подсчёта «не принято» (по нашей стороне)."""
    register = load_register(object_id, register_id)
    if register is None:
        return []

    matched_ids: set[tuple[str, str]] = set()
    for e in register.entries:
        if e.match and e.match_status in (MatchStatus.AUTO_MATCHED, MatchStatus.CONFIRMED):
            matched_ids.add((e.match.project_id, e.match.finding_id))

    return [{"project_id": p, "finding_id": f} for (p, f) in sorted(matched_ids)]
