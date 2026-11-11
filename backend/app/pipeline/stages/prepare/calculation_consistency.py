"""Trace requirements for arithmetic/physical balances, separate from item counts."""
CALCULATION_INSTRUCTIONS = '''
For calculation_consistency inspect the object's engineering arithmetic and
physical balances (for example flow, power, heat or current), not merely item
counts, lengths or geometric sizes. Recompute source substitutions and compare
linked branch totals only after identifying the same system boundary, operating
mode, simultaneity, units and physical basis. Distinguish mass from volume flow;
do not invent a density, coefficient, tolerance or conversion. Separate rounded
inputs from unrounded ones. A selected pump duty point is not its universal maximum;
working and reserve machines do not automatically operate together. Numerical
source disagreement can survive while physical insufficiency remains unproven.
Retrieve missing counterpart sources using context_queries. If no applicable
calculation is present, explain not_applicable within the inspected scope; absence
from this packet alone is needs_data, not project-wide absence.
Every checked calculation_consistency receipt must contain nonempty
calculation_checks: [{expression:string, result:string, unit:string,
basis:string, source_refs:[original block IDs]}]. State the actual substituted
expression, numerical result, unit and the verified system/operating basis in each
entry. Conditional arithmetic may also be recorded under needs_data, explicitly
stating the unresolved physical basis. These traces are reviewable model claims,
not independent proof of correct arithmetic or applicability.
'''


def has_calculation_trace(receipt, allowed):
    checks = receipt.get('calculation_checks')
    if not isinstance(checks, list) or not checks:
        return False
    for check in checks:
        if not isinstance(check, dict) or any(
            not isinstance(check.get(k), str) or not check[k].strip()
            for k in ('expression', 'result', 'unit', 'basis')
        ):
            return False
        refs = check.get('source_refs')
        if not isinstance(refs, list) or not refs or any(
            not isinstance(ref, str) or ref not in allowed for ref in refs
        ):
            return False
    return True
