"""Offline candidate preserving a valid primary location; no runtime wiring."""
from .navigation_candidate import propose_navigation


def propose_navigation_preserving_primary(finding, graph):
    result = propose_navigation(finding, graph)
    if result['status'] == 'unresolved':
        return result
    old = finding.get('page')
    old_pages = old if isinstance(old, list) else [old] if old is not None else []
    valid_primary = bool(old_pages) and all(
        type(page) is int and page in result['pages'] for page in old_pages
    )
    if valid_primary:
        result['patch'].pop('page', None)
    result['primary_location_preserved'] = valid_primary
    result['supporting_pages'] = list(result['pages'])
    result['status'] = 'proposed' if result['patch'] else 'consistent'
    return result
