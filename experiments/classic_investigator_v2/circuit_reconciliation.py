"""Addressed circuit-list comparison; graph readings and demand remain unverified."""
import copy
import re
from collections import Counter


def circuit_id(value):
    # Preserve original separately. Only the visually identical C prefix varies.
    text = str(value).strip().upper()
    if not re.fullmatch('[CС][0-9]+', text):
        raise ValueError('Unsupported circuit identifier')
    return 'C' + text[1:]


def table_rows(text):
    rows = []
    for line in text.splitlines():
        if not line.startswith('|'):
            continue
        cells = [c.strip() for c in line.strip('|').split('|')]
        if not re.fullmatch('[CС][0-9]+', cells[0], re.I):
            continue
        if len(cells) < 3:
            raise ValueError('Incomplete circuit table row')
        rows.append({'id': cells[0], 'consumer': cells[1], 'exact_line': line})
    return rows


def reconcile(rows, observations, *, same_panel_scope_confirmed=False):
    table_counts = Counter(circuit_id(r['id']) for r in rows)
    graph_counts = Counter(circuit_id(r['id']) for r in observations)
    results = []
    for observation in observations:
        item = copy.deepcopy(observation)
        key = circuit_id(item['id'])
        state = item.get('state')
        if state not in {'active', 'reserve', 'unknown'}:
            raise ValueError('Missing explicit circuit state')
        if not item.get('evidence_quote'):
            raise ValueError('Graph evidence quote required')
        if not same_panel_scope_confirmed:
            status = 'scope_unresolved'
        elif graph_counts[key] > 1 or table_counts[key] > 1:
            status = 'ambiguous_repeated_id'
        elif state == 'unknown':
            status = 'needs_graph_reading'
        elif state == 'reserve':
            status = 'reserve_no_missing_load_claim'
        elif table_counts[key]:
            status = 'table_id_present_parameters_unchecked'
        else:
            status = 'active_line_missing_table_row_question'
        results.append({'observation': item, 'normalized_id': key, 'status': status,
                        'technical_defect_confirmed': False})
    return {'rows': results, 'counts': dict(Counter(x['status'] for x in results)),
            'limits': 'Only observed circuit identifiers in the supplied same-panel table. '
                      'Graphic transcription and extraction completeness require validation. '
                      'No missing-power quantity, equipment inadequacy or procurement deficit inferred.',
            'complete_engineering_check': False}
