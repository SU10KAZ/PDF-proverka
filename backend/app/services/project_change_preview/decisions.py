"""Append-only local preview decisions. No legacy/human-truth import or writes."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

ACTIONS = {'CONFIRM': 'CONFIRMED', 'NOT_A_CHANGE': 'REJECTED', 'UNSURE': 'UNDETERMINED', 'BROKEN_CASE': 'PROBLEM'}


class DecisionConflict(ValueError):
    pass


class PreviewDecisions:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS decisions (
                    revision INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_key TEXT NOT NULL, lineage_key TEXT NOT NULL,
                    project_change_id TEXT NOT NULL, source_run_id TEXT NOT NULL,
                    candidate_version TEXT NOT NULL, binding_signature TEXT NOT NULL,
                    decision TEXT NOT NULL CHECK(decision IN ('CONFIRM','NOT_A_CHANGE','UNSURE','BROKEN_CASE')),
                    actor TEXT NOT NULL, timestamp TEXT NOT NULL, comment TEXT NOT NULL,
                    evidence_snapshot TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS decision_target ON decisions(decision_key, revision);
                CREATE INDEX IF NOT EXISTS decision_lineage ON decisions(lineage_key, revision);
                CREATE TRIGGER IF NOT EXISTS decisions_no_update BEFORE UPDATE ON decisions
                    BEGIN SELECT RAISE(ABORT, 'decision history is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS decisions_no_delete BEFORE DELETE ON decisions
                    BEGIN SELECT RAISE(ABORT, 'decision history is append-only'); END;
            ''')

    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA busy_timeout=10000')
        return db

    def read(self):
        with self.connect() as db:
            rows = [dict(r) for r in db.execute('SELECT * FROM decisions ORDER BY revision')]
        for row in rows:
            row['evidence_snapshot'] = json.loads(row['evidence_snapshot'])
        return rows, rows[-1]['revision'] if rows else 0

    def append(self, item, action, actor, comment, expected_revision):
        if action not in ACTIONS or not actor.strip():
            raise ValueError('A supported decision and server actor are required')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT coalesce(max(revision),0) FROM decisions').fetchone()[0]
            if current != expected_revision:
                raise DecisionConflict('Решения обновились. Обновите данные перед повторным сохранением.')
            record = {k: item[k] for k in ['decision_key', 'lineage_key', 'source_run_id',
                'candidate_version', 'binding_signature']}
            record.update(project_change_id=item['id'], decision=action, actor=actor,
                timestamp=datetime.now(timezone.utc).isoformat(), comment=comment,
                evidence_snapshot=json.dumps(item['_evidence_snapshot'], ensure_ascii=False, allow_nan=False))
            columns = ','.join(record)
            placeholders = ','.join('?' for _ in record)
            cursor = db.execute(f'INSERT INTO decisions ({columns}) VALUES ({placeholders})', list(record.values()))
            return cursor.lastrowid

    @staticmethod
    def effective(item, history):
        exact = [r for r in history if r['decision_key'] == item['decision_key']]
        if not exact:
            related = any(r['lineage_key'] == item['lineage_key'] for r in history)
            return {'state': 'STALE_DECISION' if related else 'NONE', 'record': None}
        record = exact[-1]  # Never resurrect an older approval after a newer decision.
        same_instance = record['source_run_id'] == item['source_run_id'] and record['project_change_id'] == item['id']
        if (record['binding_signature'] != item['binding_signature']
                or record['candidate_version'] != item['candidate_version']
                or not (item['identity_reusable'] or same_instance)):
            return {'state': 'STALE_DECISION', 'record': None}
        return {'state': 'ACTIVE', 'record': {k: v for k, v in record.items() if k != 'evidence_snapshot'}}
