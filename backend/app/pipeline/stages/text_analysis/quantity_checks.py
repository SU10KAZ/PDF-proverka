"""Exact decimal comparisons over explicitly comparable, source-addressed values.

Identity/basis are extracted assertions, not independently certified facts. No
universal tolerances or automatic defect publication are introduced here.
"""
from collections import Counter
from decimal import Decimal, InvalidOperation, localcontext

_DIMENSIONS = ('object', 'parameter', 'unit', 'basis', 'regime')


def evaluate_quantity_checks(operations, known_blocks: set[str]) -> dict:
    operations = operations if isinstance(operations, list) else []
    ids = Counter(op.get('id') for op in operations if isinstance(op, dict) and isinstance(op.get('id'), str))
    rows = []
    for op in operations:
        row = {'status': 'not_checked', 'source_interpretation_verified': False}
        rows.append(row)
        if not isinstance(op, dict) or not isinstance(op.get('id'), str) or not op['id'].strip() or ids[op['id']] != 1:
            row['reason'] = 'invalid_or_duplicate_operation_id'
            continue
        row['id'] = op['id']
        if op.get('kind') not in ('compare', 'sum'):
            row['reason'] = 'unsupported_operation'
            continue
        terms, target = op.get('terms'), op.get('target')
        if (not isinstance(terms, list) or not terms or not isinstance(target, dict)
                or (op['kind'] == 'compare' and len(terms) != 1)):
            row['reason'] = 'invalid_operands'
            continue
        expected = op.get('expected_term_ids')
        row['term_inventory_status'] = 'not_supplied'
        if expected is not None:
            actual = [term.get('term_id') if isinstance(term, dict) else None for term in terms]
            if (not isinstance(expected, list) or not expected
                    or any(not isinstance(x, str) or not x.strip() for x in expected + actual)):
                row['reason'] = 'invalid_term_inventory'
                continue
            if len(set(expected)) != len(expected) or len(set(actual)) != len(actual):
                row['reason'] = 'duplicate_term_identity'
                continue
            row['missing_term_ids'] = sorted(set(expected) - set(actual))
            row['unexpected_term_ids'] = sorted(set(actual) - set(expected))
            if row['missing_term_ids'] or row['unexpected_term_ids']:
                row['reason'] = 'term_inventory_mismatch'
                continue
            row['term_inventory_status'] = 'matches_declared_inventory_not_source_verified'
        values = []
        error = None
        for operand in terms + [target]:
            if not isinstance(operand, dict) or any(not isinstance(operand.get(k), str) or not operand[k].strip() for k in _DIMENSIONS):
                error = 'missing_comparison_basis'
                break
            if any(operand[k] != target.get(k) for k in _DIMENSIONS):
                error = 'incomparable_operands'
                break
            refs = operand.get('source_refs')
            if not isinstance(refs, list) or not refs or any(not isinstance(r, str) or r not in known_blocks for r in refs):
                error = 'unresolved_sources'
                break
            # Require decimal strings: no binary floating-point rounding or
            # guesses about comma/thousands notation in authoritative TEXT.
            raw = operand.get('value')
            if not isinstance(raw, str) or len(raw) > 80:
                error = 'invalid_decimal'
                break
            try:
                value = Decimal(raw)
                if not value.is_finite() or abs(value.adjusted()) > 100:
                    raise InvalidOperation
            except (InvalidOperation, ValueError):
                error = 'invalid_decimal'
                break
            values.append(value)
        if error:
            row['reason'] = error
            continue
        with localcontext() as ctx:
            # Preserve every decimal place admitted by validation, including
            # widely separated exponents and carries from many terms.
            highest = max(v.adjusted() for v in values)
            lowest = min(v.as_tuple().exponent for v in values)
            ctx.prec = max(1, highest - lowest + 1) + len(str(len(values))) + 2
            total = sum(values[:-1], Decimal(0))
            difference = total - values[-1]
        row.update(status='computed', kind=op['kind'],
                   comparison_basis={k: target[k] for k in _DIMENSIONS},
                   term_sources=[list(term['source_refs']) for term in terms],
                   term_values=[term['value'] for term in terms],
                   term_ids=[term.get('term_id') for term in terms],
                   target_sources=list(target['source_refs']),
                   calculated=str(total), stated=str(values[-1]), difference=str(difference),
                   numerically_equal=difference == 0,
                   publication='review_scope_rounding_and_consequences_before_publication')
    return {'operations': rows, 'computed': sum(r['status'] == 'computed' for r in rows),
            'not_checked': sum(r['status'] != 'computed' for r in rows), 'coverage_complete': False}
