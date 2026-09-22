"""Opt-in, read-only Human Mapping -> V3 bridge. No provider discovery/inference.

V1 rejects a Mapper response that violates constraints; it never invents
confidence, edits engineering regions or expands an N x N group into edges.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from backend.app.services.human_mapping_production import storage, validation
from backend.app.services.stage_comparison import paths
from . import run_storage as rs
from .contracts import MAP_SCHEMA, MAPPER_PROMPT
from .validate import validate_map

VERSION = 'human_mapping_bridge/1'
REF_KEYS = ('important_text_blocks', 'important_table_blocks', 'important_graphic_blocks')


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


class BridgeError(ValueError):
    def __init__(self, reason, *, code='BRIDGE_CONFLICT_REVIEW_REQUIRED', details=None):
        self.code, self.reason, self.details = code, reason, details or {}
        super().__init__(f'{code}: {reason}')


def require(condition, reason):
    if not condition:
        raise BridgeError(reason)


@dataclass(frozen=True)
class Snapshot:
    """Canonical immutable bytes; callers receive detached dictionaries."""
    content: bytes

    def __post_init__(self):
        require(type(self.content) is bytes, 'IMMUTABLE_SNAPSHOT_BYTES_REQUIRED')

    @property
    def sha256(self):
        return hashlib.sha256(self.content).hexdigest()

    def value(self):
        try:
            value = json.loads(self.content)
            require(isinstance(value, dict), 'INVALID_SNAPSHOT_JSON')
            return value
        except (ValueError, UnicodeDecodeError) as exc:
            raise BridgeError('INVALID_SNAPSHOT_JSON') from exc

    def write(self, path: Path):
        # Exclusive creation: never update an already frozen input.
        with Path(path).open('xb') as handle:
            handle.write(self.content)
            handle.flush()
            os.fsync(handle.fileno())
        return self.sha256


def _index(structure):
    out = {}
    for page in structure:
        side = page['side']
        require(side in {'OLD', 'NEW'}, 'WRONG_BLOCK_SIDE')
        for block in page['blocks']:
            key = (side, block['block_id'])
            require(key not in out, 'DUPLICATE_BLOCK_ID')
            out[key] = (page['physical_page'], block)
    return out


def _load_source(object_id, comparison_id, pair_id, source_run_id):
    """Exact generation only. Never uses ACTIVE/RESULT_SCOPE/current fallback."""
    from .scope import object_id_for_session
    from .engine import _resolve_pair_paths
    for kind, value in [('object', object_id), ('comparison', comparison_id),
                        ('pair', pair_id), ('run', source_run_id)]:
        storage.require_safe_id(value, kind)
    require(paths.session_json_path(comparison_id).is_file(), 'COMPARISON_NOT_FOUND')
    require(paths.pair_json_path(comparison_id, pair_id).is_file(), 'PAIR_NOT_FOUND')
    # Guard the legacy registry helper against its create-default side effect.
    from backend.app.services.common import object_service
    registry = object_service._load_objects()
    require(any(o['id'] == object_id for o in registry['objects']), 'OBJECT_NOT_FOUND')
    require(object_id_for_session(comparison_id) == object_id, 'OBJECT_SCOPE_MISMATCH')
    manifest = rs.validate(comparison_id, pair_id, source_run_id)
    require(manifest.get('frozen') is True, 'SOURCE_NOT_FROZEN')
    require(manifest.get('object_id') == object_id and manifest.get('comparison_id') == comparison_id,
            'SOURCE_SCOPE_MISMATCH')
    directory = rs.run_dir(comparison_id, pair_id, source_run_id)
    old, new, _ = _resolve_pair_paths(comparison_id, pair_id)
    for side, resolved in [('old', old), ('new', new)]:
        require(rs.sha(Path(resolved['pdf'])) == manifest[side + '_pdf_sha256'], 'STALE_SOURCE_PDF')
    result = rs.read(directory / 'project_change_v3_result.json')
    require(result and result.get('run_id') == source_run_id, 'SOURCE_RESULT_MISMATCH')
    for side in ('old', 'new'):
        require(result['source_manifest'][side + '_pdf_sha256'] == manifest[side + '_pdf_sha256'],
                'SOURCE_HASH_MISMATCH')
    structure = rs.read(directory / 'project_change_v3' / 'DOCUMENT_STRUCTURE.json')
    require(isinstance(structure, list), 'SOURCE_STRUCTURE_MISSING')
    structure_file = directory / 'project_change_v3' / 'DOCUMENT_STRUCTURE.json'
    require(rs.sha(structure_file) == result['source_manifest']['structure_sha256'], 'SOURCE_STRUCTURE_CHANGED')
    hm = rs.read(directory / 'human_mapping' / 'ui_data.json')
    require(hm and (hm.get('run_id'), hm.get('session_id'), hm.get('object_id'), hm.get('pair_key')) ==
            (source_run_id, comparison_id, object_id, pair_id), 'HUMAN_MAPPING_SCOPE_MISMATCH')
    mapping = rs.read(directory / 'project_change_v3_semantic_map.json')
    require({r['id'] for r in hm['regions']} == {r['region_id'] for r in mapping['regions']}, 'STALE_REGION')
    # HM membership is derived from the immutable map. Validate it independently.
    from .hm_builder import build_human_mapping_ui_data
    expected = build_human_mapping_ui_data(pair_id=pair_id, object_id=object_id, semantic_map=mapping,
                                          work_dir=directory / 'project_change_v3')
    require(hm['regions'] == expected['regions'], 'HUMAN_MAPPING_SOURCE_CHANGED')
    return {'object_id': object_id, 'comparison_id': comparison_id, 'pair_id': pair_id,
            'source_run_id': source_run_id, 'manifest': manifest, 'structure': structure,
            'hm': hm, 'directory': directory}


def load_source(object_id, comparison_id, pair_id, source_run_id):
    try:
        return _load_source(object_id, comparison_id, pair_id, source_run_id)
    except BridgeError:
        raise
    except (KeyError, TypeError, ValueError, OSError, LookupError) as exc:
        raise BridgeError('INVALID_SOURCE') from exc


def event_time(event):
    try:
        value = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        require(value.tzinfo is not None, 'EVENT_TIMEZONE_MISSING')
        return value
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise BridgeError('INVALID_EVENT_TIMESTAMP') from exc


def _history(source):
    directory = source['directory'] / 'human_mapping'
    # Read each append-only file once. Invalid/incomplete JSON fails closed.
    files = [directory / name for name in ('reviews.jsonl', 'human_block_link_edits.jsonl')]
    before = [p.read_bytes() if p.exists() else b'' for p in files]
    rows = [[json.loads(line) for line in raw.splitlines() if line.strip()] for raw in before]
    after = [p.read_bytes() if p.exists() else b'' for p in files]
    require(before == after, 'HUMAN_HISTORY_CHANGED_DURING_READ')
    return tuple(rows)


def _build(source, reviews, edits, created_at):
    event_time({'timestamp': created_at})
    index = _index(source['structure'])
    regions = {r['id']: r for r in source['hm']['regions']}
    event_ids = set()
    for event in [*reviews, *edits]:
        # Historical HM uses comparison_id=pair_id, NOT the session ID.
        require((event.get('object_id'), event.get('pair_key'), event.get('comparison_id'), event.get('run_id')) ==
                (source['object_id'], source['pair_id'], source['pair_id'], source['source_run_id']),
                'HUMAN_EVENT_SCOPE_MISMATCH')
        require(event.get('region_id') in regions, 'STALE_REGION')
        eid = event.get('review_id') or event.get('event_id')
        require(isinstance(eid, str) and eid and eid not in event_ids, 'DUPLICATE_OR_MISSING_EVENT_ID')
        event_ids.add(eid)
        event_time(event)
        for side in ('OLD', 'NEW'):
            ids = event.get(side.lower() + '_block_ids') if 'review_id' in event else [event.get(side.lower() + '_block_id')]
            require(isinstance(ids, list) and bool(ids), 'EMPTY_REVIEW_ENDPOINTS')
            require(all((side, bid) in index for bid in ids), 'UNKNOWN_OR_WRONG_SIDE_BLOCK')
            require(set(ids) <= validation.allowed_block_ids(regions[event['region_id']], side), 'BLOCK_OUTSIDE_REGION')
    replayed = []
    for event in edits:
        validation.validate_block_link_event(event_type=event['event_type'], region=regions[event['region_id']],
            events=replayed, link_id=event['link_id'], old_block_id=event['old_block_id'],
            new_block_id=event['new_block_id'], previous_link_id=event.get('previous_link_id'))
        replayed.append(event)
    latest = {}
    for event in reviews:
        require(event['status'] in {'HUMAN_CONFIRMED', 'HUMAN_REJECTED', 'HUMAN_UNCERTAIN'}, 'INVALID_STATUS')
        # Same rule as the established UI latest(region): last append wins.
        latest[event['region_id']] = event
    anchors, rejected = [], []
    effective = []
    for rid, region in regions.items():
        review = latest.get(rid)
        links = validation.effective_links(region, edits)
        if not review or review['status'] == 'HUMAN_UNCERTAIN':
            continue
        for link in links:
            if link['old_block_id'] not in review['old_block_ids'] or link['new_block_id'] not in review['new_block_ids']:
                continue
            origin = next((e for e in reversed(edits) if e['link_id'] == link['link_id']
                           and e['region_id'] == rid and e['event_type'] != 'DELETE_BLOCK_LINK'), None)
            # An old region confirmation never confirms a link created later.
            if origin and event_time(origin) > event_time(review):
                continue
            row = {**link, 'region_id': rid, 'source_run_id': source['source_run_id'],
                   'source': 'HUMAN_MANUAL' if origin else review['status'],
                   'review_event_ids': [review['review_id']],
                   'source_event_sha256': [digest(review)] + ([digest(origin)] if origin else [])}
            effective.append(row)
            if review['status'] == 'HUMAN_CONFIRMED':
                anchors.append({**row, 'anchor_id': digest(row), 'old_block_ids': [link['old_block_id']],
                                'new_block_ids': [link['new_block_id']], 'constraint': 'HARD_ANCHOR'})
            else:
                rejected.append({**row, 'reason': 'HUMAN_REJECTED'})
    edge = lambda r: (r['old_block_id'], r['new_block_id'])
    require(not ({edge(r) for r in anchors} & {edge(r) for r in rejected}), 'CONFIRMED_AND_REJECTED_EXACT_EDGE')
    constrained = {(side, r[side.lower() + '_block_id']) for r in effective for side in ('OLD', 'NEW')}
    value = {'bridge_version': VERSION, 'mapping_mode': 'human_anchored', 'created_at': created_at,
             **{k: source[k] for k in ('object_id', 'comparison_id', 'pair_id', 'source_run_id')},
             **{k: source['manifest'][k] for k in ('old_pdf_sha256', 'new_pdf_sha256')},
             'source_manifest_sha256': digest(source['manifest']), 'source_structure_sha256': digest(source['structure']),
             'review_snapshot': {'reviews': reviews, 'block_link_edits': edits},
             'review_snapshot_sha256': digest({'reviews': reviews, 'block_link_edits': edits}),
             'source_event_hashes': [digest(e) for e in [*reviews, *edits]],
             'confirmed_anchors': anchors, 'rejected_links': rejected,
             'unconstrained': {'block_ids': {side: sorted(b for s, b in index if s == side and (s, b) not in constrained)
                                           for side in ('OLD', 'NEW')},
                               'region_ids': sorted(set(regions) - {r['region_id'] for r in effective}),
                               'policy': 'All edges except exact rejections remain candidates; anchors are non-exclusive.'},
             'conflicts': []}
    return Snapshot(canonical(value))


def build_snapshot(*, object_id, comparison_id, pair_id, source_run_id, created_at=None):
    try:
        source = load_source(object_id, comparison_id, pair_id, source_run_id)
        reviews, edits = _history(source)
        return _build(source, reviews, edits, created_at or rs.now())
    except BridgeError:
        raise
    except (KeyError, TypeError, ValueError, OSError, LookupError) as exc:
        raise BridgeError('INVALID_SOURCE_OR_HISTORY') from exc


def validate_snapshot(snapshot, *, object_id, comparison_id, pair_id, source_run_id, expected_sha256):
    require(isinstance(snapshot, Snapshot) and snapshot.sha256 == expected_sha256, 'SNAPSHOT_HASH_MISMATCH')
    v = snapshot.value()
    require(v.get('bridge_version') == VERSION and v.get('mapping_mode') == 'human_anchored', 'INVALID_BRIDGE_VERSION')
    require(tuple(v.get(k) for k in ('object_id', 'comparison_id', 'pair_id', 'source_run_id')) ==
            (object_id, comparison_id, pair_id, source_run_id), 'SNAPSHOT_SCOPE_MISMATCH')
    source = load_source(object_id, comparison_id, pair_id, source_run_id)
    history = v['review_snapshot']
    current_reviews, current_edits = _history(source)
    for key, current in [('reviews', current_reviews), ('block_link_edits', current_edits)]:
        frozen = history[key]
        require(isinstance(frozen, list) and current[:len(frozen)] == frozen, 'SNAPSHOT_HISTORY_NOT_SOURCE_PREFIX')
    # Replay the frozen history, NEVER replace it by today's human decisions.
    rebuilt = _build(source, history['reviews'], history['block_link_edits'], v['created_at'])
    require(rebuilt.content == snapshot.content, 'SNAPSHOT_CONTENT_MISMATCH')
    return source


def mapper_package(pair_id, structure, *, mapping_mode='baseline', snapshot=None):
    """Baseline payload is exactly the current engine payload. No prompt tuning."""
    require(mapping_mode in {'baseline', 'human_anchored'}, 'INVALID_MAPPING_MODE')
    data = {'pair': pair_id, 'pages': copy.deepcopy(structure)}
    if mapping_mode == 'baseline':
        require(snapshot is None, 'BASELINE_WITH_BRIDGE')
    else:
        require(isinstance(snapshot, Snapshot), 'BRIDGE_SNAPSHOT_REQUIRED')
        v = snapshot.value()
        require(v['pair_id'] == pair_id and v['source_structure_sha256'] == digest(structure), 'MAPPER_SOURCE_MISMATCH')
        data['human_mapping_constraints'] = {
            'bridge_version': VERSION, 'bridge_snapshot_sha256': snapshot.sha256,
            'HUMAN_CONFIRMED_MAPPING_ANCHORS': v['confirmed_anchors'],
            'HUMAN_REJECTED_EXACT_LINKS': v['rejected_links'],
            'unreviewed_policy': 'UNCONSTRAINED',
        }
    from .source_prep import mapping_images
    return {'stage': 'MAPPING', 'call_id': f'{pair_id}_SEMANTIC_MAPPING', 'pair_id': pair_id,
            'prompt': MAPPER_PROMPT, 'data': data, 'schema': copy.deepcopy(MAP_SCHEMA), 'images': mapping_images(structure)}


def enforce(snapshot, mapping, structure):
    """Reject the entire response, identifying only violating exact edges.

    No block blacklist; a later response can freely map either endpoint to any
    other block. No filtering/injection that could alter engineering meaning.
    """
    import jsonschema
    jsonschema.validate(mapping, MAP_SCHEMA)
    validate_map(snapshot.value()['pair_id'], mapping, structure)
    index = _index(structure)
    edges = set()
    for region in mapping['regions']:
        refs = [r for key in REF_KEYS for r in region[key]]
        for ref in refs:
            record = index.get((ref['side'], ref['block_id']))
            require(record is not None and record[0] == ref['physical_page'] and
                    record[1]['modality'] == ref['block_type'] and
                    ref['physical_page'] in region[ref['side'].lower() + '_pages'], 'UNTRACEABLE_MAPPER_BLOCK')
        projected = {'id': region['region_id'], **{side.lower() + '_blocks':
                     [{'id': bid} for bid in dict.fromkeys(r['block_id'] for r in refs if r['side'] == side)]
                     for side in ('OLD', 'NEW')}}
        edges.update((l['old_block_id'], l['new_block_id']) for l in validation.base_proposed_links(projected))
    v = snapshot.value()
    required = {(r['old_block_id'], r['new_block_id']) for r in v['confirmed_anchors']}
    forbidden = {(r['old_block_id'], r['new_block_id']) for r in v['rejected_links']}
    missing, violations = sorted(required - edges), sorted(forbidden & edges)
    if missing or violations:
        raise BridgeError('MAPPER_CONSTRAINT_VIOLATION', code='BRIDGE_MAPPING_REJECTED',
                          details={'missing_anchors': missing, 'rejected_exact_links': violations,
                                   'bridge_snapshot_sha256': snapshot.sha256, 'enforcement': 'REJECT_RESPONSE'})
    return {'enforcement': 'VALIDATED_EXACT_LINKS', 'bridge_snapshot_sha256': snapshot.sha256,
            'confirmed_anchor_count': len(v['confirmed_anchors']), 'rejected_link_count': len(v['rejected_links'])}


def fake_mapper_run(*, snapshot, expected_sha256, object_id, comparison_id, pair_id, source_run_id,
                    new_run_id, provider):
    """No live entry point: V1 only accepts the concrete in-process FakeProvider.

    Return a detached future-run artifact; no run creation/pointer publication.
    """
    from .provider import FakeProvider
    require(type(provider) is FakeProvider, 'FAKE_PROVIDER_REQUIRED')
    storage.require_safe_id(new_run_id, 'run')
    require(new_run_id != source_run_id and not rs.run_dir(comparison_id, pair_id, new_run_id).exists(), 'NEW_RUN_ID_REQUIRED')
    source = validate_snapshot(snapshot, object_id=object_id, comparison_id=comparison_id,
        pair_id=pair_id, source_run_id=source_run_id, expected_sha256=expected_sha256)
    package = mapper_package(pair_id, source['structure'], mapping_mode='human_anchored', snapshot=snapshot)
    mapping = provider.complete(**package)
    receipt = enforce(snapshot, mapping, source['structure'])
    return {'schema': 'human_anchored_mapper_dry_run/1', 'object_id': object_id,
            'comparison_id': comparison_id, 'session_id': comparison_id, 'pair_id': pair_id,
            'source_manifest': {k: source['manifest'][k] for k in ('old_pdf_sha256', 'new_pdf_sha256')},
            'run_id': new_run_id, 'source_run_id': source_run_id, 'semantic_map': copy.deepcopy(mapping),
            'provenance': {'mapping_mode': 'human_anchored', 'bridge_version': VERSION,
                           'bridge_snapshot_id': snapshot.sha256, 'source_run_id': source_run_id,
                           **receipt}, 'model_calls': 0, 'published': False}
