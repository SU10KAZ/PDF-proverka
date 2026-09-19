"""Serve-time adapter over a sealed ProjectChange snapshot. The snapshot is never written.

Two corrections, both applied to a deep copy of the sealed envelope:

1. Pair binding. A sealed envelope without ``viewer_session`` gets the canonical
   viewer session built from the snapshot's OWN pair registry
   (``presentation.json → pairs[*].pair``).  Every card then binds to its real
   comparison pair through the version-pinned PDF path of each evidence item
   (``ProjectChangeView.pairBinding``); nothing is keyed by pair letters.
2. Presentation repair. A separate file, bound by sha256 to exactly one sealed
   snapshot, replaces an exact corrupted value with text copied from an
   uncorrupted authoritative source.  A value corrupted in every source is
   replaced by an explicit "unresolved" marker — never reconstructed.
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


def viewer_session(data: dict[str, Any], presentation_sha256: str) -> dict[str, Any]:
    """Viewer session of the snapshot's own pairs (same shape as a stage-comparison session)."""
    pairs = [copy.deepcopy(entry['pair']) for entry in data['pairs'].values()]
    left = [p['left'] for p in pairs]
    right = [p['right'] for p in pairs]
    return {
        'id': 'pc-preview-' + presentation_sha256[:20],
        'documents': {'stage_1': left, 'stage_2': right},
        'pairs': pairs,
        'document_pairing': {
            'version': 1,
            'left_order': [d['pdf_path'] for d in left],
            'right_order': [d['pdf_path'] for d in right],
            'confirmed_pairs': [{'left_pdf': p['left']['pdf_path'], 'right_pdf': p['right']['pdf_path']} for p in pairs],
        },
    }


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


def adapt(envelope: dict[str, Any], data: dict[str, Any], *, presentation_sha256: str,
          repair: dict[str, Any] | None) -> dict[str, Any]:
    """Pair-bound, repaired copy of a sealed envelope (the caller passes a deep copy)."""
    if envelope.get('viewer_session') is None and data.get('pairs'):
        envelope['viewer_session'] = viewer_session(data, presentation_sha256)
        envelope['mode'] = 'BACKEND_PREVIEW'
    return repair_envelope(envelope, repair)


def file_sha256(path: Path) -> str:
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()
