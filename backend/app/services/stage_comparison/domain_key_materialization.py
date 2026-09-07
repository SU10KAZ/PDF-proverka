"""Additive v1 metadata at the completed production materialization boundary.

The legacy producer graph continues consuming its original payloads. Publication
prepares and validates every key first; readers explicitly opt in to metadata.
No historical session scan or backfill is performed by this module.
"""
from __future__ import annotations

from collections import Counter
import copy
from functools import lru_cache
import os
from pathlib import Path
from typing import Any, Mapping

from . import domain_keys as dk
from . import text_fact_ownership

ARTIFACT_ROWS = {
    "sheet_relations": (("relations", "relation_id"),),
    "text_fact_production": (("facts", "fact_id"),),
    "text_atoms": (("atoms", "atom_id"),),
    "entity_relations": (("relations", "relation_id"),),
    "automatic_unified_synthesis": (("changes", "change_id"), ("review_items", "review_evidence_id")),
    "unified_synthesis": (("changes", "change_id"), ("review_items", "review_evidence_id")),
    "review_questions": (("questions", "question_id"),),
}
ROW_FIELDS = frozenset({"domain_key", "domain_key_version", "domain_key_sha256", "legacy_id", "run_instance_key", "provenance_keys", "provenance_key", "occurrence_id"})


def enabled() -> bool:
    return os.environ.get("STAGE_COMPARISON_DOMAIN_KEYS_V1_EMIT", "false").lower() in {"1", "true", "yes", "on"}


def legacy_payload(payload: dict) -> dict:
    """Remove only metadata produced by this materializer, for legacy readers."""
    if (payload.get("stable_domain_keys") or {}).get("schema") != "stable-domain-materialization.v1":
        return payload
    result = copy.deepcopy(payload)
    result.pop("stable_domain_keys")
    for rows in ARTIFACT_ROWS.values():
        for field, _ in rows:
            for row in result.get(field, []):
                for key in ROW_FIELDS:
                    row.pop(key, None)
    return result


@lru_cache(maxsize=1)
def producer_registry() -> list[dict]:
    root = Path(__file__).parent
    files = ["sheet_matcher.py", "text_fact_producer.py", "text_atom_builder.py", "entity_matcher.py", "review_queue.py", "domain_keys.py", "domain_key_materialization.py", "production_text_flow.py", "production_orchestrator.py", "unified_change_synthesizer/synthesizer.py", "unified_change_synthesizer/identity.py"]
    return [{"producer_name": name, "producer_version": "production-source.v1", "output_schema_version": "legacy+domain-keys.v1", "domain_key_schema_versions": ["v1"], "semantic_config_digest": dk.signature({}), "semantic_code_digest": __import__('hashlib').sha256((root/name).read_bytes()).hexdigest(), "change_class": "ID_ONLY" if name.startswith('domain_key') else "SEMANTIC"} for name in files]


class Materializer:
    def __init__(self, *, pair: dict, document_contents: dict, preparation: dict, session_id: str, pair_id: str, run_id: str):
        self.registry = dk.KeyRegistry()
        self.key_objects = {}
        self.documents = {}
        self.entities = {}
        self.units = {}
        self.unit_provenance = {}
        self.fragments = {}
        self.ownership = text_fact_ownership.fragment_ownership_index(preparation)
        self.unresolved_by_legacy = {}
        self.unresolved_physical_scopes = {}
        self.aliases = {}
        self.run = dk.run_instance(session_id=session_id, pair_id=pair_id, run_id=run_id)
        for side in ("left", "right"):
            doc = pair[side]
            content = document_contents[side]
            key = self.add(dk.document_version(doc.get('document_code'), doc.get('version_id'), content))
            self.documents[side] = key
        self.left, self.right = (self.documents[s].key for s in ('left', 'right'))
        for side in ('left', 'right'):
            fragments = (preparation.get('fragments') or {}).get(side, [])
            counts = Counter()
            # Source document order, never current artifact/list order. Ties
            # for identical units with no source position are not guessed.
            ordered = sorted(fragments, key=lambda f: (f.get('pdf_page') or 0, f.get('order') or 0, dk.canonical(f.get('source_location')), dk.canonical(f.get('bboxes') or [])))
            positions = set()
            for f in ordered:
                self.fragments[side, f['id']] = f
                semantic = (f.get('source_kind'), f.get('source'), ' '.join(str(f.get('text') or '').split()))
                position = (semantic, f.get('pdf_page'), f.get('order'), dk.canonical(f.get('source_location')), dk.canonical(f.get('bboxes') or []))
                if position in positions:
                    raise ValueError('ambiguous duplicate text-unit document position')
                positions.add(position)
                counts[semantic] += 1
                key = self.add(dk.text_unit(self.documents[side].key, semantic[0], semantic[1], semantic[2], counts[semantic]))
                self.units[side, f['id']] = key
                boxes = [{k: format(float(v), '.6f') for k, v in b.items()} for b in f.get('bboxes') or []]
                provenance = self.add(dk.evidence(self.documents[side].key, 'text-preparation.v1', dk.signature(document_contents[side]), text_unit_key=key.key, sheet_key=self.sheet(side, f['pdf_page']).key if f.get('pdf_page') else None, span={k: v for k, v in (f.get('source_location') or {}).items() if k in ('word_start', 'word_end')}, bbox=boxes))
                self.unit_provenance[side, f['id']] = provenance

    def add(self, key):
        key = self.registry.add(key)
        self.key_objects.setdefault(key.key, key)
        return key

    def sheet(self, side, page):
        return self.add(dk.sheet(self.documents[side].key, int(page)))

    def entity(self, side, subject):
        if not subject:
            return None
        # text_entity:<canonical designation> is the explicit deterministic
        # fact schema. Runtime project refs must never be used as fallback.
        if not str(subject).startswith('text_entity:'):
            raise ValueError(f'unsupported semantic entity reference: {subject}')
        designation = subject.split(':', 1)[1]
        key = self.add(dk.entity(self.documents[side].key, 'TEXT_ENTITY', designation))
        self.entities[key.key] = key
        return key

    def unresolved_endpoint(self, side: str, fragment_id: str, *, endpoint_role: str = 'SUBJECT'):
        fragment = self.fragments.get((side, fragment_id))
        record = self.ownership.get(fragment_id)
        if fragment is None or record is None:
            raise ValueError(f'unresolved endpoint has no ownership record: {side}:{fragment_id}')
        if str(record.get('side') or '').upper() != side.upper():
            raise ValueError(f'unresolved endpoint ownership side mismatch: {side}:{fragment_id}')
        if record.get('ownership_status') != 'PROVEN':
            raise ValueError(f'unresolved endpoint ownership is not proved: {side}:{fragment_id}')
        owner_kind = str(record.get('owner_kind') or '').upper()
        scope_kind = str(record.get('scope') or '').upper()
        channel = str(record.get('ownership_channel') or '').upper()
        source_kind = str(fragment.get('source_kind') or '')
        container = record.get('table_title')
        subject = record.get('row_key')
        fields = list(record.get('header_cells') or record.get('fields') or [])
        if channel == 'EXPLICIT_KEY_VALUE':
            subject = fields[0] if fields else None
            container = None
        supported = (
            owner_kind == 'TABLE_ROW'
            and scope_kind == 'TABLE_LOCAL'
            and channel in {'EXACT_TABLE_ROW', 'EXPLICIT_LABEL'}
        ) or (
            channel == 'EXPLICIT_KEY_VALUE'
            and owner_kind == 'SHEET'
            and scope_kind in {'SHEET_SHARED', 'DOCUMENT_SHARED'}
        )
        if not supported:
            raise ValueError(
                'unsupported unresolved endpoint semantic shape: '
                f'{owner_kind}/{scope_kind}/{channel}'
            )
        page = fragment.get('pdf_page')
        sheet_key = None if scope_kind == 'DOCUMENT_SHARED' else (
            self.sheet(side, page).key if page else None
        )
        key = self.add(dk.unresolved_endpoint(
            self.documents[side].key,
            endpoint_role=endpoint_role,
            scope_kind=scope_kind,
            owner_kind=owner_kind,
            source_kind=source_kind,
            sheet_key=sheet_key,
            container_label=container,
            subject_label=subject,
            field_schema=fields,
        ))
        # Runtime source groups are diagnostic only: they detect an ambiguity
        # but never enter the canonical payload or select a key.
        if scope_kind == 'TABLE_LOCAL':
            physical = str(record.get('table_group') or '')
            if not physical:
                raise ValueError(f'unresolved table endpoint lacks structural occurrence: {side}:{fragment_id}')
            old = self.unresolved_physical_scopes.setdefault(key.key, physical)
            if old != physical:
                raise ValueError(f'ambiguous duplicate unresolved endpoint semantic scope: {key.key}')
        return key

    def register_unresolved_row(self, row: Mapping[str, Any]) -> None:
        subject = str(row.get('subject_ref') or '')
        if not subject.startswith('text_scope_subject_'):
            return
        locations = self.locations(row)
        if not locations:
            raise ValueError(f'unresolved endpoint has no content-bound owner evidence: {subject}')
        for side in ('left', 'right'):
            keys = {
                self.unresolved_endpoint(side, fragment_id).key
                for actual_side, fragment_id in locations
                if actual_side == side
            }
            if not keys:
                continue
            bucket = self.unresolved_by_legacy.setdefault((side, subject), set())
            bucket.update(keys)

    def semantic_endpoint(self, side: str, subject: Any, *, row: Mapping[str, Any] | None = None):
        if not subject:
            return None
        value = str(subject)
        if value.startswith('text_entity:'):
            return self.entity(side, value)
        if not value.startswith('text_scope_subject_'):
            raise ValueError(f'unsupported semantic entity reference: {value}')
        if row is not None:
            refs = [fragment_id for actual_side, fragment_id in self.locations(row) if actual_side == side]
            keys = {self.unresolved_endpoint(side, fragment_id).key for fragment_id in refs}
        else:
            keys = set(self.unresolved_by_legacy.get((side, value)) or ())
        if len(keys) != 1:
            diagnostic = 'unbound' if not keys else 'ambiguous'
            raise ValueError(f'{diagnostic} unresolved endpoint for {side}:{value}; candidates={sorted(keys)}')
        key_value = next(iter(keys))
        key = self.key_objects.get(key_value)
        if key is None:
            raise ValueError(f'unregistered unresolved endpoint: {key_value}')
        return key

    def claim(self, row):
        side = 'right' if row.get('direction') == 'ADDED' else 'left'
        owner = self.semantic_endpoint(side, row.get('subject_ref'), row=row)
        return dk.Claim(self.left, self.right, owner.key if owner else None, row.get('facet_ref'), row.get('dimension') or 'UNKNOWN_DIMENSION', row.get('direction'), row.get('outcome'), dk.typed(row.get('before_value'), missing='before_value' not in row), dk.typed(row.get('after_value'), missing='after_value' not in row))

    def locations(self, row):
        p = row.get('provenance') or {}
        sources = [p, p.get('source_atom') or {}]
        sources += [x.get('provenance') or {} for x in p.get('source_atoms') or []]
        refs = set()
        for source in sources:
            for field in ('locations', 'source_anchors'):
                for side, locations in (source.get(field) or {}).items():
                    for loc in locations:
                        if loc.get('fragment_id'):
                            refs.add((side.lower(), loc['fragment_id']))
            for side, fragments in (source.get('source_fragment_ids') or {}).items():
                refs.update((side.lower(), f) for f in fragments)
        missing = refs - self.units.keys()
        if missing:
            raise ValueError(f'unbound content text units: {sorted(missing)[:3]}')
        return sorted(refs)

    def attach(self, row, key, legacy_id):
        key = self.add(key)
        row.update(key.fields())
        row['legacy_id'] = legacy_id
        row['run_instance_key'] = self.run.key
        row['occurrence_id'] = dk.signature({'run_instance_key': self.run.key, 'legacy_id': legacy_id})
        refs = self.locations(row)
        if refs:
            row['provenance_keys'] = sorted({self.unit_provenance[r].key for r in refs})
            if len(row['provenance_keys']) == 1:
                row['provenance_key'] = row['provenance_keys'][0]
        old = self.aliases.setdefault(legacy_id, key.key)
        if old != key.key:
            raise ValueError(f'ambiguous legacy alias: {legacy_id}')

    def apply(self, artifacts):
        result = {name: copy.deepcopy(legacy_payload(payload)) for name, payload in artifacts.items()}
        for row in result.get('sheet_relations', {}).get('relations', []):
            key = dk.sheet_relation(self.left, self.right, [self.sheet('left', p).key for p in row.get('left_pages') or []], [self.sheet('right', p).key for p in row.get('right_pages') or []], row['relation_type'])
            self.attach(row, key, row['relation_id'])
        for name in ('text_fact_production', 'text_atoms'):
            rows_field, id_field = ARTIFACT_ROWS[name][0]
            for row in result.get(name, {}).get(rows_field, []):
                self.register_unresolved_row(row)
                claim = self.claim(row)
                unit_keys = [self.units[r].key for r in self.locations(row)] if not row.get('subject_ref') else []
                key = dk.text_fact(claim) if name == 'text_fact_production' else dk.text_atom(claim, unit_keys)
                # Bound atoms may acquire a changed semantic owner; aliases
                # remain artifact-scoped, never guessed across projections.
                self.attach(row, key, row[id_field])
        for row in result.get('entity_relations', {}).get('relations', []):
            left = self.semantic_endpoint('left', row['left_entity_ref'])
            right = self.semantic_endpoint('right', row['right_entity_ref'])
            key = dk.entity_relation(self.left, self.right, left.key, right.key, row['relation'])
            self.attach(row, key, row['relation_id'])
        for name in ('automatic_unified_synthesis', 'unified_synthesis'):
            for row in result.get(name, {}).get('changes', []):
                self.register_unresolved_row(row)
                self.attach(row, dk.atomic_change(self.claim(row), field=row.get('field', row.get('facet_ref'))), row['change_id'])
            for row in result.get(name, {}).get('review_items', []):
                self.register_unresolved_row(row)
                claim = self.claim(row)
                key = dk.atomic_review(claim, text_unit_keys=[self.units[r].key for r in self.locations(row)], reason_family=row.get('reason_codes') or [], field=row.get('field', row.get('facet_ref')))
                self.attach(row, key, row['review_evidence_id'])
        for row in result.get('review_questions', {}).get('questions', []):
            context = row.get('context') or {}
            category = row.get('category')
            question_class = {'SHEET': 'SHEET_MATCHING', 'ENTITY': 'ENTITY_MATCHING', 'CHANGE': 'CHANGE_CONFIRMATION', 'MISSING_DATA': 'MISSING_DATA', 'CONFLICT': 'CONFLICT'}.get(category)
            targets = []
            if category == 'SHEET':
                # Candidate presentation groups are not semantic targets.
                # Materialize the actual candidate edges as sheet relations.
                edges = context.get('candidate_edges') or []
                for edge in edges:
                    targets.append(self.add(dk.sheet_relation(self.left, self.right, [self.sheet('left', edge['left_page']).key], [self.sheet('right', edge['right_page']).key], 'CANDIDATE')).key)
            if not targets:
                for dep in row.get('dependencies') or []:
                    ref = dep.get('ref')
                    if ref not in self.aliases:
                        raise ValueError(f'unbound question dependency: {ref}')
                    targets.append(self.aliases[ref])
            endpoints = []
            if category == 'SHEET':
                for side in ('left', 'right'):
                    pages = context.get(side+'_pages') or [s['page'] for s in context.get(side+'_sheets') or []]
                    endpoints += [self.sheet(side, page).key for page in pages]
            self.attach(row, dk.question(self.left, self.right, question_class, targets, endpoints), row['question_id'])
        return result


def publish(session_id: str, pair_id: str, *, pair: dict, state: dict, document_paths: dict):
    """Called only for a newly computed run, while its pair lock is held."""
    if not enabled():
        return
    from . import production_store
    from .production_artifacts import file_content_identity
    artifacts = {name: production_store.load_artifact(session_id, pair_id, name) or {} for name in ARTIFACT_ROWS}
    preparation = production_store.load_artifact(session_id, pair_id, 'text_preparation') or {}
    contents = {}
    for side in ('left', 'right'):
        contents[side] = []
        for kind, path in document_paths[side].items():
            info = file_content_identity(path)
            contents[side].append({'kind': {'html': 'ocr_html'}.get(kind, kind), 'sha256': info['sha256'], 'size': info['size'], 'missing': info['sha256'] is None})
    m = Materializer(pair=pair, document_contents=contents, preparation=preparation, session_id=session_id, pair_id=pair_id, run_id=state.get('run_id'))
    output = m.apply(artifacts)
    manual_keys = []
    for link in (state.get('manual_page_pairing') or {}).get('links', []):
        manual_keys.append(m.add(dk.sheet_relation(m.left, m.right, [m.sheet('left', p).key for p in link['left_pages']], [m.sheet('right', p).key for p in link['right_pages']], 'MANUAL')).key)
    semantic_scope = {k: v for k, v in (state.get('selection') or {}).items() if k in ('input_mode', 'left_pages', 'right_pages', 'left_block_ids', 'right_block_ids')}
    meta = {'schema': 'stable-domain-materialization.v1', 'domain_key_version': 'v1', 'input_content_signature': dk.input_signature(m.left, m.right, manual_keys, semantic_scope), 'algorithm_signature': dk.algorithm_signature(producer_registry(), {'ai_mode': (state.get('selection') or {}).get('ai_mode'), 'sheet_algorithm': artifacts['sheet_relations'].get('algorithm_version'), 'entity_algorithm': artifacts['entity_relations'].get('algorithm_version')}), 'run_instance_signature': m.run.digest, 'document_versions': {side: key.fields() for side, key in m.documents.items()}, 'consumption_enabled': False}
    for name, artifact in output.items():
        if not artifact:
            continue
        rows = [row for field, _ in ARTIFACT_ROWS[name] for row in artifact.get(field, [])]
        artifact['stable_domain_keys'] = {**meta, 'result_signature': dk.result_signature([{'domain_key': row['domain_key'], 'resolution_state': row.get('status', row.get('review_status', row.get('outcome')))} for row in rows])}
    # Every mapping/collision is validated before the first durable write.
    for name, artifact in output.items():
        if artifact:
            production_store.save_artifact(session_id, pair_id, name, artifact)
    state['stable_domain_keys'] = meta
