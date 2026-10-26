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
                source = 'work_plan'
        value = Decimal(str(rate or 0))
        if not value.is_finite() or value < 0 or value > 10000:
            raise ValueError('Некорректная производительность инженера: ' + user['id'])
        out.append({'id': user['id'], 'name': user.get('name') or user['id'],
                    'direction': settings.get('direction') or user.get('direction') or user.get('section') or 'Без направления',
                    'rate': str(value), 'rate_source': source, 'unavailable': settings.get('unavailable', [])})
    return sorted(out, key=lambda e: (e['direction'], e['name']))


def catalog() -> list[dict]:
    """Use platform resolvers, including v2-native versions and hidden projects.

    Expert review is one task per object / volume (platform project) / version.
    A completion of another version is never inherited.
    """
    from backend.app.services.knowledge_base import knowledge_base_service as kb
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter, is_v2_backend_enabled

    result = []
    if is_v2_backend_enabled() and not ProjectsV2Adapter().is_available():
        raise OSError('Хранилище проектов недоступно. Очереди сохранены без изменений.')
    completions = schedule_service.load_schedule_completions()
    with document_lookup_snapshot():
        for obj in object_service.list_objects():
            with project_scope(obj['id']):
                for card in project_service.list_projects():
                    versions = card.versions_summary or [{'version_id': card.version_id}]
                    for entry in versions:
                        vid = entry['version_id']
                        with version_service.pinned_version(vid):
                            ctx = version_service.resolve_project_version_context(card.project_id, vid)
                            vid = ctx['version_id']
                            status = project_service.get_project_status(card.project_id, version_id=vid)
                            if status is None:
                                continue
                            review = kb.load_expert_review(card.project_id) or {}
                            decisions = [d for d in review.get('decisions', []) if isinstance(d, dict) and d.get('decision')]
                            complete = kb._project_completion_day(card.project_id)
                            complete = complete or completions.get((obj['id'], card.project_id, vid.lower()))
                            has_results = status.findings_count + status.optimization_count > 0
                            ready = has_results and (status.has_pdf or status.has_md_file)
                            timestamps = sorted(str(d.get('timestamp')) for d in decisions if d.get('timestamp'))
                            result.append({'object_id': obj['id'], 'object_name': obj['name'],
                                           'project_id': card.project_id, 'version_id': vid,
                                           'check_type': 'expert_review', 'title': status.name,
                                           'code': card.project_id, 'section': status.section,
                                           'ready': ready, 'completed_at': complete,
                                           'reviewer': review.get('reviewer', ''),
                                           'started_at': timestamps[0] if timestamps else review.get('reviewed_at') if decisions else None,
                                           'progress': len(decisions), 'total': status.findings_count + status.optimization_count})
    return result


def reviewer_id(name: str) -> str | None:
    matches = [u['id'] for u in user_service.list_users() if reviewer_matches_user(name, u)]
    return matches[0] if len(matches) == 1 else None
