"""Human assessments of exact published V3 results, separate from frozen artifacts."""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import sqlite3

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Literal

from backend.app.services.human_mapping_production.storage import require_safe_id
from backend.app.services.stage_comparison.paths import comparison_root_path
from . import run_storage


class ReviewConflict(ValueError):
    pass


class ReviewUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    session_id: str = Field(min_length=1, max_length=128)
    pair_id: str = Field(min_length=1, max_length=128)
    run_id: str = Field(min_length=1, max_length=128)
    change_id: str = Field(min_length=1, max_length=300)
    decision: Literal['accepted', 'rejected'] | None
    reason: str = Field(default='', max_length=4000)
    expected_revision: int = Field(ge=0)

    @model_validator(mode='after')
    def validate_reason(self):
        self.reason = self.reason.strip()
        if self.decision == 'rejected' and not self.reason:
            raise ValueError('Укажите причину отказа')
        if self.decision is None:
            self.reason = ''
        return self


class ReviewRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    updates: list[ReviewUpdate] = Field(min_length=1, max_length=1000)


def _path():
    return comparison_root_path() / 'project_change_expert_reviews.sqlite3'


def _connect(*, write=False):
    path = _path()
    if write:
        path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path) if write else path.as_uri() + '?mode=ro', uri=not write, timeout=15)
    db.row_factory = sqlite3.Row
    if write:
        db.execute('''CREATE TABLE IF NOT EXISTS reviews (
            revision INTEGER PRIMARY KEY AUTOINCREMENT,
            object_id TEXT NOT NULL, session_id TEXT NOT NULL, pair_id TEXT NOT NULL,
            run_id TEXT NOT NULL, change_id TEXT NOT NULL, result_sha256 TEXT NOT NULL,
            decision TEXT CHECK(decision IN ('accepted','rejected') OR decision IS NULL),
            reason TEXT NOT NULL, actor TEXT NOT NULL, timestamp TEXT NOT NULL)''')
        db.execute('CREATE INDEX IF NOT EXISTS review_scope ON reviews(object_id, session_id, pair_id, run_id, revision)')
        db.commit()
    return db


def read_run(object_id, session_id, pair_id, run_id, result_sha256):
    if not _path().is_file():
        return {}
    with closing(_connect()) as db:
        rows = db.execute('''SELECT * FROM reviews WHERE object_id=? AND session_id=? AND pair_id=?
            AND run_id=? AND result_sha256=? ORDER BY revision''',
            (object_id, session_id, pair_id, run_id, result_sha256)).fetchall()
    return {r['change_id']: dict(r) for r in rows}


def save(object_id: str, updates: list[ReviewUpdate], *, actor: str):
    from .scope import object_id_for_session
    from .presentation import published_run

    require_safe_id(object_id, 'object')
    contexts, keys, prepared = {}, set(), []
    for update in updates:
        key = (update.session_id, update.pair_id, update.run_id)
        for value, label in zip(key, ('session', 'pair', 'run')):
            require_safe_id(value, label)
        if key not in contexts:
            if object_id_for_session(update.session_id) != object_id:
                raise LookupError('Результат не относится к выбранному объекту')
            manifest = run_storage.validate(*key)
            if manifest.get('object_id') != object_id:
                raise LookupError('Объект результата не совпадает')
            with run_storage.selected(*key):
                published = published_run(update.session_id, update.pair_id)
            if published is None:
                raise LookupError('Опубликованный результат не найден')
            result = published[1]
            if (result.get('session_id'), result.get('pair_id'), result.get('run_id')) != key:
                raise LookupError('Привязка результата не совпадает')
            contexts[key] = (manifest['artifacts']['project_change_v3_result']['sha256'],
                             {str(c['projectchange_id']) for c in result['projectchanges']})
        result_sha, change_ids = contexts[key]
        if update.change_id not in change_ids:
            raise LookupError('Изменение не найдено в этом запуске')
        identity = (*key, update.change_id)
        if identity in keys:
            raise ValueError('Повторное решение для одного изменения')
        keys.add(identity)
        prepared.append((update, result_sha))
    saved = []
    with closing(_connect(write=True)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        # Validate every revision before appending any assessment: all or nothing.
        for update, result_sha in prepared:
            identity = (object_id, update.session_id, update.pair_id, update.run_id, update.change_id, result_sha)
            row = db.execute('''SELECT revision FROM reviews WHERE object_id=? AND session_id=? AND pair_id=?
                AND run_id=? AND change_id=? AND result_sha256=? ORDER BY revision DESC LIMIT 1''', identity).fetchone()
            if (row['revision'] if row else 0) != update.expected_revision:
                raise ReviewConflict('Оценка уже изменена другим пользователем. Обновите страницу и повторите решение.')
        for update, result_sha in prepared:
            timestamp = datetime.now(timezone.utc).isoformat()
            cursor = db.execute('''INSERT INTO reviews
                (object_id,session_id,pair_id,run_id,change_id,result_sha256,decision,reason,actor,timestamp)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (object_id, update.session_id, update.pair_id, update.run_id, update.change_id, result_sha,
                 update.decision, update.reason, actor, timestamp))
            saved.append({**update.model_dump(exclude={'expected_revision'}), 'revision': cursor.lastrowid,
                          'actor': actor, 'timestamp': timestamp})
    return {'items': saved}
