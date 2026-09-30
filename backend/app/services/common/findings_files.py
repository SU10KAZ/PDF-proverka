"""Выбор файла замечаний: устаревший 03a_norms_verified.json не побеждает 03_findings.json.

03a — снимок замечаний после верификации норм, поэтому читатели предпочитают
его. Но если после этого свод (03) перезапустили без повторной верификации норм,
в папке остаётся 03a от ПРЕЖНЕГО прогона. Портал тогда показывал старый набор,
а экспорт пакета/Excel — новый (03_findings.json), и решения эксперта по
номерам F-NNN ложились на чужие замечания (13АВ-РД-ЭМ-К2 V2: 84 против 28,
ЭМ-К4 V2: 49 против 54, 30.09.2026).

Правило: 03a устарел, когда он СТАРШЕ 03_findings.json по mtime И описывает
другой набор замечаний (другая последовательность id). Оба условия нужны:
синхронные копии (тот же набор, правки текста) по-прежнему читаются из 03a,
а legacy-папки, где 03a новее огрызка 03, не меняют поведения.
"""
from __future__ import annotations

import json
from pathlib import Path

VERIFIED_FINDINGS_FILENAME = "03a_norms_verified.json"
FINDINGS_FILENAME = "03_findings.json"


def _finding_ids(path: Path) -> list | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    items = data if isinstance(data, list) else (
        data.get("findings", data.get("items", [])) if isinstance(data, dict) else None
    )
    if not isinstance(items, list):
        return None
    return [str(x.get("id")) for x in items if isinstance(x, dict)]


def verified_findings_is_stale(verified: Path, main: Path) -> bool:
    """True, если 03a остался от прежнего прогона и 03_findings.json новее."""
    try:
        if not (verified.is_file() and main.is_file()):
            return False
        if verified.stat().st_mtime >= main.stat().st_mtime:
            return False
    except OSError:
        return False
    ids_verified = _finding_ids(verified)
    ids_main = _finding_ids(main)
    if ids_verified is None or ids_main is None:
        return False
    return ids_verified != ids_main


def use_verified_findings(output_dir: Path) -> bool:
    """Читать ли 03a_norms_verified.json в этой папке (есть и не устарел)."""
    verified = output_dir / VERIFIED_FINDINGS_FILENAME
    if not verified.is_file():
        return False
    return not verified_findings_is_stale(verified, output_dir / FINDINGS_FILENAME)
