"""Exact decimal conflict checks; facts retain their independent provenance."""
from decimal import Decimal
import re
from .applicability import component_rows

NUMBER = r'[+-]?\d+(?:[.,]\d+)?'
TOTAL = re.compile(r'(?:итог[о]?|total)\s*[—–:]?\s*(' + NUMBER + ')', re.I)


def dec(value):
    return Decimal(str(value).replace(',', '.'))


def groups(value):
    """Only explicit row/total syntax; never invent an aggregate from a page."""
    chunks = re.split(r'(Корпус\s+\d+(?:\.\d+)?\s*:)', value, flags=re.I)
    parts = [(chunks[i].rstrip(':').strip().casefold(), chunks[i + 1]) for i in range(1, len(chunks), 2)] if len(chunks) > 1 else [('state', value)]
    result = []
    for scope, text in parts:
        rows = component_rows(text)
        total = TOTAL.search(text)
        if not rows or not total or len({r[0] for r in rows}) != len(rows):
            continue
        result.append(dict(scope=scope, rows=[dict(row_id=k, value=str(dec(v))) for k, v in rows],
                           claimed_value=str(dec(total[1])), derived_value=str(sum((dec(v) for _, v in rows), Decimal(0)))))
    return result


class NumericConflictGuard:
    """Source facts are supplied by a hash-checked delivered-only extractor.

    No epsilon is used. Unit/precision/rounding receipts must be explicit before
    any future rounding policy can be introduced; V2 never waives a difference.
    """
    def evaluate(self, raw, facts, primary_complete):
        conflicts, checked = [], []
        old_groups = {g['scope']: g for g in groups(raw['old_state']['value'])}
        for side in ('old', 'new'):
            state = raw[side + '_state']
            for g in groups(state['value']):
                observations = [f for f in facts if f['side'] == side and f['scope'] == g['scope']
                                and f['evidence_id'] in state.get('evidence_ids', [])
                                and f.get('unit') == state.get('unit') and f.get('grounded')]
                for fact in observations:
                    printed, derived, claimed = dec(fact['printed_value']), dec(g['derived_value']), dec(g['claimed_value'])
                    checked.append(dict(side=side, scope=g['scope'], fact=fact, derived=g))
                    if printed == derived == claimed:
                        continue
                    old = old_groups.get(g['scope'])
                    existence = side == 'new' and primary_complete and old and all(
                        v != dec(old['claimed_value']) for v in (printed, derived, claimed))
                    conflicts.append(dict(kind='PRINTED_DERIVED_CONFLICT' if printed != derived else 'CLAIM_PRINTED_CONFLICT',
                        side=side, scope=g['scope'], printed_value=str(printed), derived_value=str(derived),
                        claimed_value=str(claimed), delta=str(printed - derived), unit=state.get('unit'),
                        source_references=[fact], derived_rows=g['rows'],
                        derived_provenance='Saved response row values at bound primary evidence; not a replacement printed total.',
                        CHANGE_EXISTENCE_SUPPORTED='YES' if existence else 'UNKNOWN', EXACT_VALUE_SUPPORTED='NO',
                        rounding_rule='UNPROVEN_NO_TOLERANCE', resolved=False))
        return dict(schema='NumericConflictGuard/2', blocking=bool(conflicts), conflicts=conflicts,
                    checked=checked, raw_values_unchanged=True, tolerance=None,
                    coverage='Delivered text and same-raster OCR facts only; unparsed numeric prose is not certified.')
