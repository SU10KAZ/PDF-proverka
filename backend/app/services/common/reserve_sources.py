"""Read-only bridge to platform staff, versioned projects, plans and review facts.

No inference, document extraction, uploads or pipeline launches are performed here.
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from backend.app.core.config import APP_DATA_DIR
from backend.app.services.common import object_service, project_service, schedule_service, user_service, version_service
from backend.app.services.common.employee_identity import reviewer_matches_user
from backend.app.services.storage.projects_v2_adapter import document_lookup_snapshot


def today() -> date:
    return datetime.now(ZoneInfo('Europe/Moscow')).date()


def identity(item: dict) -> tuple:
    return tuple(item.get(k) for k in ('object_id', 'project_id', 'version_id')) + (item.get('check_type', 'expert_review'),)


@contextmanager
def project_scope(object_id: str, version_id: str | None = None):
    token = project_service.bind_object(object_id)
    try:
        with version_service.pinned_version(version_id):
            yield
    finally:
        project_service.unbind_object(token)


def calendar_config() -> dict:
    path = Path(os.environ.get('AUDIT_WORK_CALENDAR_FILE') or APP_DATA_DIR / 'work_calendar.json')
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('work_calendar.json: ожидается объект')
    return data


def is_workday(day: date, config: dict, engineer: dict | None = None) -> bool:
    key = day.isoformat()
    if key in config.get('holidays', []):
        return False
    if day.weekday() >= 5 and key not in config.get('working_dates', []):
        return False
    for absence in (engineer or {}).get('unavailable', []):
        if absence['from'] <= key <= absence['to']:
            return False
    return True


def engineers(config: dict, day: date) -> list[dict]:
    plans, warning = schedule_service.load_work_plans()
    if warning:
        raise ValueError(warning)
    out = []
    for user in user_service.list_users():
        if user.get('active') is False or user.get('is_active') is False or user.get('status') in ('blocked', 'inactive', 'deleted'):
            continue
        settings = config.get('engineers', {}).get(user['id'], {})
        rate = settings.get('daily_rate', user.get('reserve_daily_rate'))
        capacity = {}
        source = 'configured' if rate is not None else 'missing_plan'
        if rate is None:
            schedule_id, _ = schedule_service.engineer_identity(user.get('name', ''))
            candidates = [p for p in plans.get('plans', [])
                          if not p.get('object_id') and p.get('engineer_id') in (user['id'], schedule_id)
                          and p.get('period_start', '') <= day.isoformat() <= p.get('period_end', '')]
            candidates.sort(key=lambda p: (p.get('period_type') == 'week', p.get('updated_at', '')), reverse=True)
            if candidates:
                plan = candidates[0]
                first, last = date.fromisoformat(plan['period_start']), date.fromisoformat(plan['period_end'])
                count = sum(is_workday(first + timedelta(days=i), config, settings) for i in range((last-first).days+1))
                rate = Decimal(str(plan['plan'])) / count if count else 0
                capacity = {'rate_numerator': int(plan['plan']), 'rate_denominator': count or 1}
                source = 'work_plan'
        value = Decimal(str(rate or 0))
        if not value.is_finite() or value < 0 or value > 10000:
            raise ValueError('Некорректная производительность инженера: ' + user['id'])
        out.append({'id': user['id'], 'name': user.get('name') or user['id'],
                    'direction': settings.get('direction') or user.get('direction') or user.get('section') or 'Без направления',
                    'rate': str(value), 'rate_source': source, 'unavailable': settings.get('unavailable', []), **capacity})
    return sorted(out, key=lambda e: (e['direction'], e['name']))


def catalog() -> list[dict]:
    """Use platform resolvers, including v2-native versions and hidden projects.

    Expert review is one task per object / volume (platform project) / version.
    A completion of another version is never inherited.
    """
    from backend.app.services.knowledge_base import knowledge_base_service as kb
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
    from backend.app.services.storage.storage_read_facade import production_uses_v2

    result = []
    v2 = production_uses_v2()
    adapter = ProjectsV2Adapter() if v2 else None
    if v2 and not adapter.is_available():
        raise OSError('Хранилище проектов недоступно. Очереди сохранены без изменений.')
    documents = adapter.list_documents() if v2 else []
    hidden = project_service._load_hidden_projects()
    completions = schedule_service.load_schedule_completions()
    with document_lookup_snapshot():
        for obj in object_service.list_objects():
            with project_scope(obj['id']):
                if v2:
                    projects = [(doc['document_code'], adapter.list_versions(Path(doc['doc_dir'])), doc) for doc in documents
                                if doc['object_id'] == obj['id'] and doc['document_code'] not in hidden]
                else:
                    projects = [(card.project_id, card.versions_summary or [{'version_id': card.version_id}], None)
                                for card in project_service.list_projects()]
                for project_id, versions, doc in projects:
                    for entry in versions:
                        vid = entry['version_id']
                        with version_service.pinned_version(vid):
                            ctx = version_service.resolve_project_version_context(project_id, vid)
                            vid = ctx['version_id']
                            if v2:
                                directory = Path(doc['doc_dir'])
                                meta = adapter.read_version_json(directory, vid)
                                if meta is None:
                                    raise OSError('Не удалось прочитать версию проекта: ' + project_id)
                                document_meta = adapter.read_document_json(directory) or {}
                                info = meta.get('project_info') or document_meta.get('project_info') or {}
                                title = info.get('name') or document_meta.get('display_name') or project_id
                                section = info.get('section') or doc['discipline']
                                inputs = adapter.source_input_files(directory, vid)
                                has_source = (any(str(name).lower().endswith(('.pdf', '.md')) for name in inputs)
                                              or (ctx['version_dir'] / '02_work/document.md').is_file())
                            else:
                                status = project_service.get_project_status(project_id, version_id=vid)
                                if status is None:
                                    raise OSError('Не удалось прочитать статус проекта: ' + project_id)
                                title, section = status.name, status.section
                                has_source = status.has_pdf or status.has_md_file
                            review = kb.load_expert_review(project_id) or {}
                            source_maps = kb._load_source_item_maps(project_id)
                            total = sum(map(len, source_maps))
                            decisions = [d for d in review.get('decisions', []) if isinstance(d, dict) and d.get('decision')]
                            complete = kb._project_completion_day(project_id, review=review, source_maps=source_maps)
                            complete = complete or completions.get((obj['id'], project_id, vid.lower()))
                            ready = total > 0 and has_source
                            timestamps = sorted(str(d.get('timestamp')) for d in decisions if d.get('timestamp'))
                            result.append({'object_id': obj['id'], 'object_name': obj['name'],
                                           'project_id': project_id, 'version_id': vid,
                                           'check_type': 'expert_review', 'title': title,
                                           'code': project_id, 'section': section,
                                           'ready': ready, 'completed_at': complete,
                                           'reviewer': review.get('reviewer', ''),
                                           'started_at': timestamps[0] if timestamps else review.get('reviewed_at') if decisions else None,
                                           'progress': len(decisions), 'total': total})
    return result


def reviewer_id(name: str) -> str | None:
    matches = [u['id'] for u in user_service.list_users() if reviewer_matches_user(name, u)]
    return matches[0] if len(matches) == 1 else None
