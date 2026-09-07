"""Opt-in append-only DecisionRegistry foundation; no automatic migration.

No database is opened by the comparison pipeline. Callers must explicitly
supply a store path and exact version-bound keys. Generated PENDING records
and legacy links are never imported as human authority.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import uuid
from datetime import datetime, timezone

AUTHORITY = {'DETERMINISTIC_SUGGESTION': 0, 'AI': 1, 'HUMAN': 2}
STATES = {'ACTIVE', 'STALE', 'SUPERSEDED', 'LOCKED'}


class RegistryConflict(ValueError):
    pass


class DecisionRegistry:
    def __init__(self, path: str | Path):
        self.path = str(path)
        with self._connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS events (revision INTEGER PRIMARY KEY AUTOINCREMENT, decision_id TEXT UNIQUE NOT NULL, domain_key TEXT NOT NULL, scope TEXT NOT NULL, payload TEXT NOT NULL)')
            db.execute('CREATE INDEX IF NOT EXISTS target ON events(domain_key, scope, revision)')

    def _connect(self):
        return sqlite3.connect(self.path, timeout=10)

    def history(self, domain_key: str, scope: str) -> list[dict]:
        with self._connect() as db:
            return [json.loads(r[0]) for r in db.execute('SELECT payload FROM events WHERE domain_key=? AND scope=? ORDER BY revision', (domain_key, scope))]

    def revision(self) -> int:
        with self._connect() as db:
            return db.execute('SELECT COALESCE(MAX(revision),0) FROM events').fetchone()[0]

    def append(self, record: dict, *, expected_revision: int, current_input_signature: str, authorized_human_supersession=False) -> dict:
        row = dict(record)
        required = ('domain_key', 'domain_key_version', 'target_kind', 'decision_scope', 'decision', 'authority', 'based_on_input_content_signature', 'based_on_algorithm_signature')
        if any(not row.get(k) for k in required) or row['domain_key_version'] != 'v1' or not row['domain_key'].startswith('dk1_'):
            raise ValueError('complete version-bound registry record required')
        if row['authority'] not in AUTHORITY or row['decision'] == 'PENDING_REVIEW':
            raise ValueError('generated or unknown authority cannot create a decision')
        if row['authority'] == 'HUMAN' and not row.get('author'):
            raise ValueError('named human author required')
        if row.get('state', 'ACTIVE') not in STATES:
            raise ValueError('invalid lifecycle state')
        if row['based_on_input_content_signature'] != current_input_signature:
            raise RegistryConflict('input changed')
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            revision = db.execute('SELECT COALESCE(MAX(revision),0) FROM events').fetchone()[0]
            if revision != expected_revision:
                raise RegistryConflict('registry revision changed')
            prior = db.execute('SELECT payload FROM events WHERE domain_key=? AND scope=? ORDER BY revision DESC LIMIT 1', (row['domain_key'], row['decision_scope'])).fetchone()
            if prior:
                previous = json.loads(prior[0])
                if row.get('supersedes_decision_id') != previous['decision_id']:
                    raise RegistryConflict('explicit supersession link required')
                if AUTHORITY[row['authority']] < AUTHORITY[previous['authority']]:
                    raise RegistryConflict('lower authority cannot replace higher authority')
                if previous['state'] == 'LOCKED' and not (row['authority'] == 'HUMAN' and authorized_human_supersession):
                    raise RegistryConflict('locked decision requires authorized human supersession')
            elif row.get('supersedes_decision_id'):
                raise RegistryConflict('unknown supersession target')
            row.update(decision_id='dec_'+uuid.uuid4().hex, registry_schema_version='decision-registry.v1', created_at=datetime.now(timezone.utc).isoformat(), state=row.get('state', 'ACTIVE'), validation_state=row.get('validation_state', 'VALID'))
            db.execute('INSERT INTO events(decision_id,domain_key,scope,payload) VALUES (?,?,?,?)', (row['decision_id'], row['domain_key'], row['decision_scope'], json.dumps(row, ensure_ascii=False, allow_nan=False)))
        return row

    def resolve(self, domain_key: str, scope: str, input_signature: str, algorithm_signature: str, *, compatibility_policy: dict | None = None) -> dict:
        history = self.history(domain_key, scope)
        if not history:
            return {'decision': None, 'state': 'STALE', 'reason': 'no exact domain/scope alias'}
        row = history[-1]
        if row['state'] not in {'ACTIVE', 'LOCKED'} or row['based_on_input_content_signature'] != input_signature:
            return {'decision': None, 'state': 'STALE', 'record': row}
        validation = row.get('validation_state', 'VALID')
        if row['based_on_algorithm_signature'] != algorithm_signature:
            policy = compatibility_policy or {}
            compatible = (policy.get('from_algorithm_signature') == row['based_on_algorithm_signature'] and policy.get('to_algorithm_signature') == algorithm_signature and policy.get('change_class') in {'ID_ONLY', 'EVIDENCE_ONLY'} and policy.get('decision_policy') in {'REUSE', 'REUSE_WITH_EVIDENCE_REVISION'} and bool(policy.get('approved_by')))
            if not compatible:
                validation = 'REQUIRES_REVALIDATION'
        return {'decision': row['decision'] if validation == 'VALID' else None, 'state': row['state'], 'validation_state': validation, 'record': row}


def legacy_alias_policy(row: dict, *, exact_document_versions=False, unique_domain_key=None, named_author=None) -> dict:
    """Produce a reviewable migration proposal, never import or assert truth."""
    eligible = row.get('source') == 'manual' and 'user_corrected' in (row.get('reason') or [])
    bound = eligible and exact_document_versions and unique_domain_key and named_author
    return {'legacy_record': row, 'domain_key': unique_domain_key if bound else None, 'migration_state': 'REQUIRES_EXPLICIT_IMPORT' if bound else 'REQUIRES_REVALIDATION', 'authority': None, 'historical_rewrite': False}
