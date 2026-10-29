"""Atomic human-review queues; forecasts never overwrite review history or deadlines."""
from __future__ import annotations

import hashlib
import json
import unicodedata
import uuid
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
from math import floor
from collections import Counter

from backend.app.services.common import reserve_sources as sources, reserve_store as store
from backend.app.services.common.version_service import _version_no_from_id

ACTIVE = {'preparation', 'ready', 'in_progress'}


class ReserveError(ValueError):
    def __init__(self, message: str, code: str = 'invalid', status: int = 400):
        super().__init__(message)
        self.code, self.status = code, status


def now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def name_key(value):
    return ' '.join(unicodedata.normalize('NFC', value).split()).lower()


def _task(engineer_id, title, actor, linked=None):
    linked = linked or {}
    stamp = now()
    return {'id': str(uuid.uuid4()), 'engineer_id': engineer_id, 'position': 0, 'title': title,
            'forecast_date': None,
            **{k: linked.get(k) for k in ('object_id', 'project_id', 'version_id')},
            'check_type': 'expert_review', 'state': 'preparation', 'started_at': None,
            'completed_at': None, 'created_at': stamp, 'updated_at': stamp, 'created_by': actor}


def _normalize(conn, engineers):
    order = {e['id']: i for i, e in enumerate(engineers)}
    rows = sorted(store.tasks(conn), key=lambda r: (order.get(r['engineer_id'], len(order)),
                  r['engineer_id'], r['state'] != 'in_progress', r['position'], r['id']))
    positions = {}
    for row in rows:
        if row['state'] not in ACTIVE:
            continue
        key = (row['engineer_id'], row.get('forecast_date'))
        positions[key] = positions.get(key, 0) + 1
        if row['position'] != positions[key]:
            row['position'] = positions[key]
            row['updated_at'] = now()
            store.save_task(conn, row)


def _sync(conn, catalog, engineers):
    """Reconcile upstream readiness/progress/completion, retaining queue IDs and order."""
    by_identity = {sources.identity(c): c for c in catalog}
    rows = store.tasks(conn)
    known = {sources.identity(r) for r in rows if r['project_id']}
    active_engineers = {e['id'] for e in engineers}
    changed = False
    # Existing human review activity is the platform's pre-calendar start event.
    for item in catalog:
        if item['completed_at'] or not item['started_at'] or sources.identity(item) in known:
            continue
        engineer = sources.reviewer_id(item['reviewer'])
        if engineer in active_engineers:
            row = _task(engineer, item['title'], 'platform:expert-review', item)
            row.update(state='in_progress', started_at=item['started_at'], position=len(rows)+1)
            store.save_task(conn, row)
            rows.append(row)
            known.add(sources.identity(item))
            changed = True
    for row in rows:
        if not row['project_id'] or row['state'] not in ACTIVE:
            continue
        item = by_identity.get(sources.identity(row))
        before = dict(row)
        if item is None:
            row['state'] = 'cancelled'  # deleted/hidden/replaced upstream version
        elif item['completed_at']:
            row.update(state='completed', completed_at=item['completed_at'])
        elif item['started_at']:
            row['state'] = 'in_progress'
            row['started_at'] = row['started_at'] or item['started_at']
            # A real review outside the calendar establishes its actual engineer.
            actual = sources.reviewer_id(item['reviewer'])
            if actual in active_engineers:
                row['engineer_id'] = actual
        elif row['state'] != 'in_progress':
            row['state'] = 'ready' if item['ready'] else 'preparation'
        if row != before:
            row['updated_at'] = now()
            store.save_task(conn, row)
            changed = True
    if changed:
        _normalize(conn, engineers)
        store.bump(conn)


def forecast(rows, engineers, config, day):
    """Cumulative fractional quota on *available* workdays, bounded at ten years."""
    output = []
    for engineer in engineers:
        queue = [dict(r) for r in rows if r['engineer_id'] == engineer['id'] and r['state'] in ACTIVE]
        queue.sort(key=lambda r: r['position'])
        rate = (Fraction(engineer['rate_numerator'], engineer['rate_denominator'])
                if 'rate_numerator' in engineer else Fraction(engineer['rate']))
        dates = []
        if rate > 0 and queue:
            available = 0
            for offset in range(3660):
                current = day + timedelta(days=offset)
                if not sources.is_workday(current, config, engineer):
                    continue
                available += 1
                previous = floor(rate * (available-1))
                cumulative = floor(rate * available)
                quota = max(0, cumulative-previous - (engineer.get('completed_today', 0) if current == day else 0))
                dates.extend([current.isoformat()] * min(len(queue)-len(dates), quota))
                if len(dates) >= len(queue):
                    break
        conditional = False
        for i, row in enumerate(queue):
            conditional = conditional or row['state'] == 'preparation'
            started = row['state'] == 'in_progress'
            scheduled = started or i < len(dates)
            row.update(forecast_date=day.isoformat() if started else dates[i] if i < len(dates) else None,
                       conditional=conditional, unscheduled_reason=(None if scheduled else
                         'missing_plan' if engineer['rate_source'] == 'missing_plan' else 'zero_capacity' if rate == 0 else 'horizon_exceeded'))
            output.append(row)
    return output


def _completed_counts(catalog):
    reviewers = {c.get('reviewer', '') for c in catalog if c.get('completed_at')}
    ids = {name: sources.reviewer_id(name) for name in reviewers}
    return Counter((ids.get(c.get('reviewer', '')), str(c['completed_at'])[:10])
                   for c in catalog if c.get('completed_at'))


def _dated_counts(rows):
    return Counter((r['engineer_id'], r['forecast_date']) for r in rows
                   if r['state'] in ACTIVE and r.get('forecast_date'))


def _portal_dates(count, engineer, config, start, occupied=None):
    """Place new assignments in free slots, including earlier assignment batches."""
    occupied = occupied or {}
    dates = []
    for offset in range(3660):
        if len(dates) >= count:
            break
        current = start + timedelta(days=offset)
        quota = max(0, sources.daily_capacity(engineer, config, current)
                    - occupied.get((engineer['id'], current.isoformat()), 0))
        dates.extend([current.isoformat()] * min(count - len(dates), quota))
    if len(dates) < count:
        raise ReserveError(f"{engineer['name']}: нет свободных дней с заданной нормой. "
                           'Укажите месячный план в разделе «Выполнено».', 'capacity_missing')
    return dates


def _check_capacity(before, after, engineers, config, catalog):
    """Validate net increases only: legacy overload can still be reduced/reordered."""
    old, new = _dated_counts(before), _dated_counts(after)
    completed = _completed_counts(catalog)
    people = {e['id']: e for e in engineers}
    for (eid, iso), count in new.items():
        if count <= old[(eid, iso)]:
            continue
        engineer = people.get(eid)
        limit = sources.daily_capacity(engineer, config, date.fromisoformat(iso)) if engineer else 0
        done = completed[(eid, iso)]
        if count + done > limit:
            name = engineer['name'] if engineer else eid
            raise ReserveError(
                f'{name}, {date.fromisoformat(iso).strftime("%d.%m.%Y")}: лимит {limit} проектов в день. '
                f'После переноса будет {count} в очереди, уже выполнено {done}. '
                'Выберите другой день или инженера. Норма задаётся в разделе «Выполнено».',
                'daily_capacity_exceeded')


def _snapshot(conn, catalog, engineers, config, actor, view_start=None):
    rows = store.tasks(conn)
    active_engineer_ids = {e['id'] for e in engineers}
    # Keep assignments visible if an employee is subsequently deactivated.
    inactive_ids = ({r['engineer_id'] for r in rows if r['state'] in ACTIVE} - active_engineer_ids
                    - sources.RESERVE_EXCLUDED_ENGINEER_IDS)
    for eid in sorted(inactive_ids):
        from backend.app.services.common import user_service
        user = user_service.get_user_by_id(eid) or {}
        engineers.append({'id': eid, 'name': user.get('name') or eid, 'direction': 'Неактивные сотрудники',
                          'rate': '0', 'rate_source': 'inactive', 'active': False})
    view_start = view_start or sources.today()
    completed = _completed_counts(catalog)
    for engineer in engineers:
        engineer['day_limits'] = {}
        engineer['day_completed'] = {}
        for offset in range(28):
            day = view_start + timedelta(days=offset)
            engineer['day_limits'][day.isoformat()] = sources.daily_capacity(engineer, config, day)
            engineer['day_completed'][day.isoformat()] = completed[(engineer['id'], day.isoformat())]
        # Internal plan records are unnecessary in the calendar response.
        engineer.pop('capacity_plans', None)
    by_identity = {sources.identity(c): c for c in catalog}
    reviewers = {c.get('reviewer', '') for c in catalog if str(c.get('completed_at') or '')[:10] == sources.today().isoformat()}
    reviewer_ids = {name: sources.reviewer_id(name) for name in reviewers}
    for engineer in engineers:
        engineer['completed_today'] = sum(
            str(c.get('completed_at') or '')[:10] == sources.today().isoformat()
            and reviewer_ids.get(c.get('reviewer', '')) == engineer['id'] for c in catalog)
    # Dates are explicitly assigned by a person. Forecasting from engineer
    # capacity is intentionally not part of the reserve calendar anymore.
    projected = [dict(r, conditional=False, unscheduled_reason=None) for r in rows if r['state'] in ACTIVE]
    for row in projected:
        item = by_identity.get(sources.identity(row), {})
        row.update({k: item.get(k) for k in ('object_name', 'code', 'section', 'progress', 'total')})
        # Readiness must still be enforced for an already started project if its
        # upstream artifacts were removed. A click never changes this flag.
        row['documents_ready'] = bool(item.get('ready'))
    for engineer in engineers:
        assigned = [r for r in projected if r['engineer_id'] == engineer['id']]
        engineer.update(remaining=len(assigned), preparation=sum(r['state'] == 'preparation' for r in assigned),
                        ready=sum(r['state'] == 'ready' for r in assigned),
                        running=sum(r['state'] == 'in_progress' for r in assigned),
                through=(max((r.get('forecast_date') for r in assigned if r.get('forecast_date')), default=None)
                         if all(r.get('forecast_date') for r in assigned) else None))
    last = conn.execute('SELECT * FROM reserve_operations WHERE actor=? ORDER BY rowid DESC LIMIT 1', (actor,)).fetchone()
    undo_id = last['id'] if last and last['kind'] in ('move', 'delete', 'add_preparation', 'assign') and not last['undone_by'] else None
    assigned_ids = {sources.identity(r) for r in rows if r['project_id']}
    # Select the latest version before excluding completed/assigned entries:
    # otherwise an obsolete unfinished version reappears in the picker.
    latest = {}
    for item in catalog:
        project_key = (item['object_id'], item['project_id'], item.get('check_type', 'expert_review'))
        previous = latest.get(project_key)
        if previous is None or _version_no_from_id(item['version_id']) > _version_no_from_id(previous['version_id']):
            latest[project_key] = item
    return {'revision': store.revision(conn), 'today': sources.today().isoformat(), 'engineers': engineers,
            'tasks': projected, 'undo_operation_id': undo_id,
            'available_projects': [c for c in latest.values() if not c['completed_at'] and sources.identity(c) not in assigned_ids],
            'calendar': {'holidays': config.get('holidays', []), 'working_dates': config.get('working_dates', []),
                         'configured': bool(config)},
            'totals': {'remaining': len(projected), 'ready': sum(r['state'] == 'ready' for r in projected),
                       'preparation': sum(r['state'] == 'preparation' for r in projected),
                       'running': sum(r['state'] == 'in_progress' for r in projected)}}


def snapshot(actor='', view_start=None):
    config = sources.calendar_config()
    engineers = sources.engineers(config, sources.today())
    # Read sources inside the queue lock so an earlier source snapshot cannot
    # reverse a concurrent start/completion reconciled by another request.
    with store.transaction() as conn:
        catalog = sources.catalog()
        _sync(conn, catalog, engineers)
        return _snapshot(conn, catalog, engineers, config, actor, view_start)


def _queue_snapshot(conn, engineer_ids):
    return [r for r in store.tasks(conn) if r['engineer_id'] in engineer_ids and r['state'] in ACTIVE]


def _set_order(conn, ids, engineer_id):
    by_id = {r['id']: r for r in store.tasks(conn)}
    for position, tid in enumerate(ids, 1):
        row = by_id[tid]
        if row['position'] != position or row['engineer_id'] != engineer_id:
            row.update(position=position, engineer_id=engineer_id, updated_at=now())
            store.save_task(conn, row)


def _find_catalog(catalog, value):
    key = sources.identity(value)
    matches = [c for c in catalog if sources.identity(c) == key]
    if len(matches) != 1:
        raise ReserveError('Выбранная версия проекта не найдена.', 'project_not_found', 404)
    return matches[0]


def mutate(payload, actor, view_start=None):
    operation_id = payload['operation_id']
    digest = hashlib.sha256(encode(payload).encode()).hexdigest()
    config = sources.calendar_config()
    engineers = sources.engineers(config, sources.today())
    engineer_ids = {e['id'] for e in engineers}
    kind = payload['kind']
    with store.transaction() as conn:
        previous = conn.execute('SELECT * FROM reserve_operations WHERE id=?', (operation_id,)).fetchone()
        if previous and (previous['actor'] != actor or previous['request_hash'] != digest):
            raise ReserveError('Ключ запроса уже использован для другой операции.', 'idempotency_conflict', 409)
        catalog = sources.catalog()
        _sync(conn, catalog, engineers)
        if previous:
            return {**_snapshot(conn, catalog, engineers, config, actor, view_start), 'operation': json.loads(previous['result_json']), 'replayed': True}
        if payload['revision'] != store.revision(conn):
            # Commit reconciliation before reporting the conflict (no user mutation).
            conflict = True
        else:
            conflict = False
            rows = [r for r in store.tasks(conn) if r['state'] in ACTIVE]
            original_rows = [dict(r) for r in rows]
            by_id = {r['id']: r for r in rows}
            affected = set()
            result = {'id': operation_id, 'kind': kind}
            if kind in ('add_preparation', 'assign'):
                items = payload.get('items') or []
                if not items or len(items) > 1000:
                    raise ReserveError('Добавьте от 1 до 1000 проектов.')
                prepared, seen = [], set()
                all_tasks = store.tasks(conn)
                known = {sources.identity(r) for r in all_tasks if r['project_id']}
                for item in items:
                    eid = item.get('engineer_id')
                    if eid not in engineer_ids:
                        raise ReserveError('Активный инженер не найден.')
                    if kind == 'assign':
                        linked = _find_catalog(catalog, item)
                        if linked['completed_at'] or sources.identity(linked) in known:
                            raise ReserveError('Эта версия уже назначена или завершена.', 'duplicate_task', 409)
                        known.add(sources.identity(linked))
                        row = _task(eid, linked['title'], actor, linked)
                        row['state'] = 'ready' if linked['ready'] else 'preparation'
                    else:
                        raw_title = item.get('title')
                        if not isinstance(raw_title, str):
                            raise ReserveError('Укажите название проекта в каждой строке.')
                        title = raw_title.strip()
                        if not title or len(title) > 4000 or any(c in title for c in '\r\n\u2028\u2029'):
                            raise ReserveError('Название должно занимать одну непустую строку, не более 4000 символов.')
                        key = name_key(title)
                        if key in seen:
                            raise ReserveError('Повтор названия в пакете: ' + title, 'duplicate_name')
                        seen.add(key)
                        # Only unlinked drafts in the same preparation context
                        # can be duplicates by text. Real identities never merge by title.
                        if any(not r['project_id'] and r['engineer_id'] == eid and name_key(r['title']) == key for r in rows):
                            raise ReserveError('У инженера уже есть такая запись подготовки: ' + title, 'duplicate_name')
                        row = _task(eid, title, actor)
                    affected.add(eid)
                    prepared.append(row)
                before = _queue_snapshot(conn, affected)
                schedule_dates = {}
                if kind == 'assign':
                    by_engineer = {e['id']: e for e in engineers}
                    counts = {}
                    for row in prepared:
                        counts[row['engineer_id']] = counts.get(row['engineer_id'], 0) + 1
                    start = sources.today() + timedelta(days=1)
                    for eid, count in counts.items():
                        schedule_dates[eid] = _portal_dates(count, by_engineer[eid], config, start,
                                                           _dated_counts(rows) + _completed_counts(catalog))
                scheduled_index = {eid: 0 for eid in schedule_dates}
                for row in prepared:
                    if kind == 'assign':
                        row['forecast_date'] = schedule_dates[row['engineer_id']][scheduled_index[row['engineer_id']]]
                        scheduled_index[row['engineer_id']] += 1
                    row['position'] = max((r['position'] for r in rows
                                           if r['engineer_id'] == row['engineer_id']
                                           and r.get('forecast_date') == row.get('forecast_date')), default=0) + 1
                    store.save_task(conn, row)
                    rows.append(row)
                result['created_ids'] = [r['id'] for r in prepared]
            elif kind == 'move':
                ids = payload.get('task_ids') or []
                target = payload.get('engineer_id')
                if target not in engineer_ids or not ids or len(ids) != len(set(ids)) or any(tid not in by_id for tid in ids):
                    raise ReserveError('Выберите существующие задания и активного инженера.')
                selected = set(ids)
                display_order = {e['id']: i for i, e in enumerate(engineers)}
                block = sorted((by_id[tid] for tid in ids), key=lambda r: (display_order.get(r['engineer_id'], len(engineers)), r['engineer_id'], r['position']))
                if any(r['state'] == 'in_progress' for r in block):
                    raise ReserveError('Начатую проверку нельзя переносить.', 'already_started', 409)
                affected = {target} | {r['engineer_id'] for r in block}
                before = _queue_snapshot(conn, affected)
                target_queue = [r for r in rows if r['engineer_id'] == target and r['id'] not in selected]
                anchor = payload.get('anchor_id')
                has_target_date = 'forecast_date' in payload
                target_date = payload.get('forecast_date')
                if has_target_date and target_date is not None:
                    try:
                        target_date = date.fromisoformat(str(target_date)).isoformat()
                    except ValueError:
                        raise ReserveError('Некорректная дата переноса.')
                if anchor and has_target_date:
                    anchor_row = next((r for r in target_queue if r['id'] == anchor), None)
                    if anchor_row and anchor_row.get('forecast_date') != target_date:
                        raise ReserveError('Место вставки изменилось. Обновите календарь.', 'anchor_changed', 409)
                if anchor:
                    if anchor not in {r['id'] for r in target_queue}:
                        raise ReserveError('Место вставки изменилось. Обновите календарь.', 'anchor_changed', 409)
                    index = next(i for i, r in enumerate(target_queue) if r['id'] == anchor) + (payload.get('side') == 'after')
                else:
                    index = len(target_queue)
                index = max(index, sum(r['state'] == 'in_progress' for r in target_queue))
                target_ids = [r['id'] for r in target_queue]
                target_ids[index:index] = [r['id'] for r in block]
                old_dates = {r.get('forecast_date') for r in block}
                if has_target_date:
                    for row in block:
                        row['forecast_date'] = target_date
                for eid in affected - {target}:
                    remaining = [r for r in rows if r['engineer_id'] == eid and r['id'] not in selected]
                    for day in {r.get('forecast_date') for r in remaining}:
                        _set_order(conn, [r['id'] for r in remaining if r.get('forecast_date') == day], eid)
                # Preserve the order of every untouched day; only the target
                # day's insertion order and the moved rows are changed.
                move_dates = {r.get('forecast_date') for r in block} if not has_target_date else {target_date}
                for day in {r.get('forecast_date') for r in target_queue} | old_dates | move_dates:
                    day_ids = [tid for tid in target_ids if by_id[tid].get('forecast_date') == day]
                    moved_ids = [r['id'] for r in block if r['id'] in day_ids]
                    if day == target_date:
                        day_ids = [tid for tid in target_ids if tid in day_ids or tid in moved_ids]
                    for pos, tid in enumerate(day_ids, 1):
                        row = by_id[tid]
                        row['engineer_id'] = target
                        if has_target_date and tid in moved_ids:
                            row['forecast_date'] = target_date
                        row['position'] = pos
                        row['updated_at'] = now()
                        store.save_task(conn, row)
            elif kind == 'delete':
                ids = payload.get('task_ids') or []
                if not ids or len(ids) != len(set(ids)) or any(tid not in by_id for tid in ids):
                    raise ReserveError('Выберите существующие задания для удаления.')
                selected = set(ids)
                deleted = [by_id[tid] for tid in ids]
                if any(row['state'] == 'in_progress' for row in deleted):
                    raise ReserveError('Начатую проверку нельзя удалить.', 'already_started', 409)
                affected = {row['engineer_id'] for row in deleted}
                before = _queue_snapshot(conn, affected)
                for tid in ids:
                    conn.execute('DELETE FROM reserve_tasks WHERE id=?', (tid,))
                _normalize(conn, engineers)
                result['deleted_ids'] = ids
            elif kind in ('start', 'link'):
                row = by_id.get(payload.get('task_id'))
                if not row:
                    raise ReserveError('Задание больше не находится в активной очереди.', 'task_not_found', 404)
                affected = {row['engineer_id']}
                before = _queue_snapshot(conn, affected)
                if kind == 'link':
                    if row['project_id'] or row['state'] == 'in_progress':
                        raise ReserveError('Задание уже связано с документами.', 'already_linked', 409)
                    item = _find_catalog(catalog, payload.get('project') or {})
                    if item['completed_at'] or any(sources.identity(r) == sources.identity(item) for r in store.tasks(conn)):
                        raise ReserveError('Эта версия уже назначена или завершена.', 'duplicate_task', 409)
                    row.update({k: item[k] for k in ('object_id', 'project_id', 'version_id')})
                    row['state'] = 'ready' if item['ready'] else 'preparation'
                else:
                    item = _find_catalog(catalog, row) if row['project_id'] else None
                    if item is None or not item['ready'] or row['state'] not in ('ready', 'in_progress'):
                        raise ReserveError('Документы ещё не готовы к экспертной проверке.', 'not_ready', 409)
                    if row['state'] != 'in_progress':
                        row.update(state='in_progress', started_at=now())
                row['updated_at'] = now()
                store.save_task(conn, row)
                _normalize(conn, engineers)
            elif kind == 'undo':
                original = conn.execute('SELECT * FROM reserve_operations WHERE id=? AND actor=?', (payload.get('undo_id'), actor)).fetchone()
                if not original or original['undone_by'] or original['kind'] not in ('move', 'delete', 'add_preparation', 'assign'):
                    raise ReserveError('Это действие уже нельзя отменить.', 'undo_conflict', 409)
                old_before, old_after = json.loads(original['before_json']), json.loads(original['after_json'])
                affected = {r['engineer_id'] for r in old_before+old_after}
                before = _queue_snapshot(conn, affected)
                if original['kind'] in ('add_preparation', 'assign'):
                    created = set(json.loads(original['result_json'])['created_ids'])
                    expected = [r for r in old_after if r['id'] in created]
                    actual = [r for r in before if r['id'] in created]
                    if actual != expected:
                        raise ReserveError('Добавленные задания уже изменены. Отмена недоступна.', 'undo_conflict', 409)
                    for tid in created:
                        conn.execute('DELETE FROM reserve_tasks WHERE id=?', (tid,))
                else:
                    if before != old_after:
                        raise ReserveError('Очередь изменилась после действия. Отмена не затронет новые изменения.', 'undo_conflict', 409)
                    for row in old_before:
                        row['updated_at'] = now()
                        store.save_task(conn, row)
                _normalize(conn, engineers)
                conn.execute('UPDATE reserve_operations SET undone_by=? WHERE id=?', (operation_id, original['id']))
            else:
                raise ReserveError('Неизвестная операция.')
            if kind in ('move', 'assign', 'undo'):
                _check_capacity(original_rows, store.tasks(conn), engineers, config, catalog)
            store.bump(conn)
            conn.execute('INSERT INTO reserve_operations (id,actor,request_hash,kind,before_json,after_json,result_json,created_at) VALUES (?,?,?,?,?,?,?,?)',
                         (operation_id, actor, digest, kind, encode(before), encode(_queue_snapshot(conn, affected)), encode(result), now()))
            response = {**_snapshot(conn, catalog, engineers, config, actor, view_start), 'operation': result}
    if conflict:
        raise ReserveError('Очередь уже изменена другим пользователем. Загружены актуальные данные; повторите действие.', 'revision_conflict', 409)
    return response


def reconcile_review():
    """Called after the existing review save/import. No duplicate completion facts."""
    snapshot('platform:expert-review')
