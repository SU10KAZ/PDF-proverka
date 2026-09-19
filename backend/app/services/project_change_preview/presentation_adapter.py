"""Serve-time text repair over a sealed ProjectChange snapshot. The snapshot is never written.

A separate file, bound by sha256 to exactly one sealed snapshot, replaces an
exact corrupted value with text copied from an uncorrupted authoritative
source.  A value corrupted in every source is replaced by an explicit
"unresolved" marker — never reconstructed.  Binding the cards to the object's
real comparison pairs is done by ``project_change_v3.presentation``.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

REPAIR_SCHEMA = 'project-change-presentation-repair/1'


class RepairMismatch(ValueError):
    """The repair file does not belong to the sealed snapshot being served."""


def load_repair(path: Path, *, manifest_sha256: str, presentation_sha256: str) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    repair = json.loads(path.read_text(encoding='utf-8'))
    if repair.get('schema') != REPAIR_SCHEMA:
        raise RepairMismatch('Unknown presentation repair schema')
    if (repair.get('snapshot_manifest_sha256') != manifest_sha256
            or repair.get('snapshot_presentation_sha256') != presentation_sha256):
        raise RepairMismatch('Presentation repair belongs to another snapshot')
    return repair


def _item_pairs(item: dict[str, Any]) -> set[str]:
    return {str(e.get('pair_id')) for e in item.get('evidence') or []}


def _apply(value: Any, rule: dict[str, Any]) -> tuple[Any, bool]:
    if rule.get('match', 'EXACT') == 'EXACT':
        if value != rule['corrupted']:
            return value, False
        return (rule['display_ru'] if rule['status'] == 'UNRESOLVED' else rule['value']), True
    if not isinstance(value, str) or rule['corrupted'] not in value:  # SUBSTRING
        return value, False
    return value.replace(rule['corrupted'], rule['value']), True


def repair_envelope(envelope: dict[str, Any], repair: dict[str, Any] | None) -> dict[str, Any]:
    if not repair:
        return envelope
    rules = [r for r in repair['rules'] if r['target'] == 'item']
    for item in envelope['items']:
        pairs = _item_pairs(item)
        unresolved = []
        for rule in rules:
            if rule['pair_id'] != '*' and rule['pair_id'] not in pairs:
                continue
            item[rule['field']], changed = _apply(item.get(rule['field']), rule)
            if changed and rule['status'] == 'UNRESOLVED':
                unresolved.append(rule['field'])
        if unresolved:
            item['presentation_repair'] = {'unresolved_corrupted_fields': unresolved, 'repair_id': repair['repair_id']}
    envelope['presentation_repair'] = {'repair_id': repair['repair_id'], 'schema': repair['schema']}
    return envelope


def repair_manifest(manifest: dict[str, Any], repair: dict[str, Any] | None) -> dict[str, Any]:
    out = copy.deepcopy(manifest)
    for rule in (repair or {}).get('rules', []):
        if rule['target'] == 'manifest' and rule['field'] in out:
            out[rule['field']], _ = _apply(out[rule['field']], rule)
    return out


def adapt(envelope: dict[str, Any], repair: dict[str, Any] | None) -> dict[str, Any]:
    """Repaired copy of a sealed envelope (the caller passes a deep copy)."""
    return repair_envelope(envelope, repair)


def file_sha256(path: Path) -> str:
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()
