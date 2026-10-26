"""Read-only run comparison. Counts and speed never select a quality winner."""
from datetime import datetime


def metrics(result, state):
    prov = result.get('provenance') or {}
    calls = prov.get('transport_calls') or []
    duration = None
    try:
        duration = (datetime.fromisoformat(state['completed_at'].replace('Z', '+00:00')) -
                    datetime.fromisoformat(state['started_at'].replace('Z', '+00:00'))).total_seconds()
    except (KeyError, TypeError, ValueError):
        pass
    return {'run_id': result.get('run_id'), 'comparison_mode': prov.get('comparison_mode', 'standard'),
            'model': prov.get('model'), 'reasoning': prov.get('reasoning'), 'elapsed_seconds': duration,
            'model_calls': result.get('model_calls'), 'receipts': len(calls),
            'usage_total': prov.get('usage_total'),
            'receipts_without_usage': sum(not row.get('usage') for row in calls),
            'projectchanges': len(result.get('projectchanges') or []),
            'unresolved_hints': len(result.get('unresolved_hints') or []),
            'quality': result.get('quality'), 'page_coverage': result.get('coverage'),
            'search_coverage': prov.get('subject_search'),
            'resumed': bool(prov.get('resumed_from') or prov.get('subject_recovery')),
            'quality_adjudication': 'not_evaluated'}


def compare(standard, subjects, *, source_equal):
    """Arguments are (result, state) tuples; source_equal includes MD, blocks and crops."""
    left, right = standard[0], subjects[0]
    a, b = left.get('provenance') or {}, right.get('provenance') or {}
    reasons = []
    if a.get('comparison_mode', 'standard') != 'standard' or b.get('comparison_mode') != 'subjects':
        reasons.append('expected_standard_and_subjects')
    if (left.get('session_id'), left.get('pair_id')) != (right.get('session_id'), right.get('pair_id')):
        reasons.append('different_pair')
    if not source_equal:
        reasons.append('different_sources')
    for key in ('model', 'provider', 'reasoning', 'source_packaging_version'):
        if not a.get(key) or a.get(key) != b.get(key):
            reasons.append('different_' + key)
    rows = [metrics(*standard), metrics(*subjects)]
    if any(row['resumed'] for row in rows):
        reasons.append('resume_requires_separate_lineage_cost_accounting')
    for result, state in (standard, subjects):
        if state.get('reason_code') != 'v3_completed':
            reasons.append('incomplete_run')
    return {'schema': 'projectchange_mode_comparison/1', 'comparable': not reasons,
            'limitations': sorted(set(reasons)), 'runs': rows,
            'same_engine_version': a.get('engine_version') == b.get('engine_version'),
            'quality_adjudication': 'not_evaluated', 'winner': None,
            'note': 'Количество карточек не измеряет полноту или точность. Нужна независимая оценка по источникам.'}
