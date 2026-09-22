"""Versioned V3 artifacts. The pointer is the only mutable default selection."""
from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from backend.app.services.stage_comparison import paths
from backend.app.services.human_mapping_production.storage import require_safe_id

ACTIVE = contextvars.ContextVar('v3_run_storage', default=None)
TERMINAL = {'COMPLETED', 'FROZEN', 'COMPLETED_FROZEN'}


def now():
    return datetime.now(timezone.utc).isoformat()


def root(session_id, pair_id):
    require_safe_id(session_id, 'session')
    require_safe_id(pair_id, 'pair')
    return paths.production_dir(session_id, pair_id)


def run_dir(session_id, pair_id, run_id):
    return root(session_id, pair_id) / 'runs' / require_safe_id(run_id, 'run')


def read(path):
    return json.loads(path.read_bytes()) if path.is_file() else None


def sha(path):
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def current(session_id, pair_id):
    pointer = read(root(session_id, pair_id) / 'current_run.json')
    return require_safe_id(pointer['run_id'], 'run') if pointer else None


@contextlib.contextmanager
def selected(session_id, pair_id, run_id):
    token = ACTIVE.set((session_id, pair_id, require_safe_id(run_id, 'run')))
    try:
        yield
    finally:
        ACTIVE.reset(token)


def active_dir(session_id, pair_id):
    scope = ACTIVE.get()
    return run_dir(*scope) if scope and scope[:2] == (session_id, pair_id) else None


def artifact_path(session_id, pair_id, name, run_id=None):
    directory = run_dir(session_id, pair_id, run_id) if run_id else active_dir(session_id, pair_id)
    if directory is None:
        rid = current(session_id, pair_id)
        directory = run_dir(session_id, pair_id, rid) if rid else root(session_id, pair_id)
    return directory / (name + '.json')


def create(session_id, pair_id, run_id, object_id=None):
    directory = run_dir(session_id, pair_id, run_id)
    directory.mkdir(parents=True, exist_ok=False)  # IDs can never be reused, including failed runs.
    atomic(directory / 'run_manifest.json', {
        'schema': 'projectchange-versioned-run/1', 'run_id': run_id,
        'session_id': session_id, 'comparison_id': session_id, 'pair_id': pair_id,
        'object_id': object_id, 'created_at': now(), 'started_at': now(),
        'completed_at': None, 'state': 'RUNNING', 'frozen': False,
    })
    return directory


def save(session_id, pair_id, name, value):
    directory = active_dir(session_id, pair_id)
    if directory is None:
        raise RuntimeError('run write requires explicit active run')
    manifest = read(directory / 'run_manifest.json')
    if not manifest or manifest['state'] in TERMINAL:
        raise RuntimeError('completed run is immutable')
    atomic(directory / (name + '.json'), value)


def validate(session_id, pair_id, run_id):
    directory = run_dir(session_id, pair_id, run_id)
    manifest = read(directory / 'run_manifest.json')
    if not manifest or manifest['state'] not in TERMINAL:
        raise ValueError('run is not completed')
    if (manifest['run_id'], manifest['session_id'], manifest['pair_id']) != (run_id, session_id, pair_id):
        raise ValueError('manifest scope mismatch')
    for name, artifact in manifest['artifacts'].items():
        if Path(artifact['ref']).name != artifact['ref'] or sha(directory / artifact['ref']) != artifact['sha256']:
            raise ValueError('run artifact hash mismatch: ' + name)
    return manifest


def select_current(session_id, pair_id, run_id):
    validate(session_id, pair_id, run_id)
    atomic(root(session_id, pair_id) / 'current_run.json', {'schema': 'projectchange-current-run/1', 'run_id': run_id})


def finalize(session_id, pair_id, run_id, state):
    directory = run_dir(session_id, pair_id, run_id)
    manifest = read(directory / 'run_manifest.json')
    if manifest['state'] in TERMINAL:
        raise ValueError('run already finalized')
    if state.get('reason_code') != 'v3_completed' or state.get('status') not in {'COMPLETED', 'REVIEW', *TERMINAL}:
        atomic(directory / 'run_manifest.json', {**manifest, 'state': state['status'], 'completed_at': state.get('completed_at')})
        return False
    result = read(directory / 'project_change_v3_result.json')
    mapping = read(directory / 'project_change_v3_semantic_map.json')
    hm = read(directory / 'human_mapping' / 'ui_data.json')
    if not result or not mapping or not hm or result['run_id'] != run_id or hm['run_id'] != run_id:
        raise ValueError('incomplete final artifacts')
    if result['session_id'] != session_id or result['pair_id'] != pair_id or state['run_id'] != run_id:
        raise ValueError('result scope mismatch')
    if not isinstance(result.get('projectchanges'), list):
        raise ValueError('invalid result')
    source = result['source_manifest']
    for side in ('old', 'new'):
        if len(source[side + '_pdf_sha256']) != 64:
            raise ValueError('source hash missing')
    atomic(directory / 'unresolved_hints.json', result.get('unresolved_hints', []))
    artifacts = {name: {'ref': name + '.json', 'sha256': sha(directory / (name + '.json'))}
                 for name in ('project_change_v3_result', 'project_change_v3_semantic_map', 'state', 'unresolved_hints')}
    prov = result['provenance']
    manifest = {**manifest, **{k: prov.get(k) for k in (
        'engine', 'engine_version', 'provider', 'model', 'reasoning', 'thinking',
        'mapper_prompt_version', 'mapper_prompt_sha256', 'miner_prompt_version', 'miner_prompt_sha256',
        'dedupe_version', 'source_packaging_version', 'transport_version')},
        'object_id': result.get('object_id') or hm.get('object_id'),
        'old_pdf_sha256': source['old_pdf_sha256'], 'new_pdf_sha256': source['new_pdf_sha256'],
        'started_at': state.get('started_at'), 'completed_at': state.get('completed_at'),
        'state': 'COMPLETED_FROZEN', 'frozen': True, 'artifacts': artifacts,
        'projectchange_count': len(result['projectchanges']),
        'unresolved_hint_count': len(result.get('unresolved_hints') or []),
        'semantic_region_count': len(mapping.get('regions') or [])}
    for artifact in directory.rglob('*'):
        if artifact.is_file():
            with artifact.open('rb') as handle:
                os.fsync(handle.fileno())
    atomic(directory / 'run_manifest.json', manifest)
    validate(session_id, pair_id, run_id)
    select_current(session_id, pair_id, run_id)
    return True


def run_ids(session_id, pair_id):
    directory = root(session_id, pair_id) / 'runs'
    return sorted(p.name for p in directory.iterdir() if p.is_dir() and (p / 'run_manifest.json').is_file()) if directory.is_dir() else []


def adopt_legacy(session_id, pair_id):
    """Copy a completed legacy generation without modifying any source byte."""
    import shutil
    from backend.app.services.human_mapping_production import storage
    legacy = root(session_id, pair_id)
    if current(session_id, pair_id):
        return None
    state = read(legacy / 'state.json')
    result = read(legacy / 'project_change_v3_result.json')
    if not state or not result or state.get('reason_code') != 'v3_completed':
        return None
    rid = result['run_id']
    if state.get('run_id') != rid or state.get('status') not in {'COMPLETED', 'REVIEW'}:
        raise ValueError('legacy ownership cannot be proven')
    pointer = read(legacy / 'project_change_v3_human_mapping_ui.json')
    if not pointer or pointer.get('run_id') != rid or pointer.get('session_id') != session_id:
        raise ValueError('legacy HM ownership cannot be proven')
    hm_dir = storage.pair_dir(pointer['object_id'], pair_id)
    hm = read(hm_dir / 'ui_data.json')
    if not hm or hm.get('run_id') != rid or hm.get('session_id') != session_id:
        raise ValueError('legacy HM context mismatch')
    dest = create(session_id, pair_id, rid, pointer['object_id'])
    sources = [(p, dest / p.name) for p in legacy.glob('project_change_v3*.json')]
    sources.append((legacy / 'state.json', dest / 'state.json'))
    for base, target in [(legacy / 'project_change_v3', dest / 'project_change_v3'), (hm_dir, dest / 'human_mapping')]:
        sources += [(p, target / p.relative_to(base)) for p in base.rglob('*') if p.is_file() and not p.name.endswith('.lock')]
    rows = []
    for source, target in sources:
        before = sha(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        with target.open('rb') as handle:
            os.fsync(handle.fileno())
        after = sha(target)
        if before != after or sha(source) != before:
            raise ValueError('migration source changed')
        rows.append({'old_source_path': str(source), 'new_versioned_path': str(target), 'old_sha256': before, 'new_sha256': after})
    review_counts = {name: len(storage.read_jsonl(hm_dir / name)) for name in ('reviews.jsonl', 'human_block_link_edits.jsonl')}
    receipt = {'schema': 'versioned-run-migration/1', 'pair_id': pair_id, 'session_id': session_id,
               'run_id': rid, 'status': 'PASS', 'content_mutated': False, 'artifacts': rows,
               'artifact_count': len(rows), 'old_source_path': str(legacy), 'new_versioned_path': str(dest),
               'projectchange_ids': [c['projectchange_id'] for c in result['projectchanges']],
               'counts': {'projectchanges': len(result['projectchanges']), 'unresolved_hints': len(result['unresolved_hints']),
                          'semantic_regions': len(read(dest / 'project_change_v3_semantic_map.json')['regions'])},
               'source_hashes': result['source_manifest'], 'provenance': result['provenance'],
               'human_mapping_history_counts': review_counts, 'human_mapping_history_empty': not any(review_counts.values()),
               'result_sha256': sha(dest / 'project_change_v3_result.json'),
               'mapping_sha256': sha(dest / 'project_change_v3_semantic_map.json')}
    atomic(dest / 'migration_receipt.json', receipt)
    finalize(session_id, pair_id, rid, state)
    return receipt
