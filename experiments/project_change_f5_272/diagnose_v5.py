"""Read-only V3/V4 regression diagnosis; stop before repair for mixed causes.

Input: the pre-change REGRESSION_9_MANIFEST and explicit local source review.
No inference, truth labels, tuning, package mutation or rebuild is performed.
The allocation counterfactual is diagnostic, never a proposed dependency waiver.
"""
import argparse
from collections import Counter
import json
from pathlib import Path
import sys
from types import SimpleNamespace

from .common import fingerprint, write
from .repair_v3 import OUT as V3
from .repair_v4 import OUT as V4
from .requirements_v4 import allocation_plan
from .subject_v4 import refine_candidate

OUT = V4.with_name('fresh_dev_sample_f5_pipeline_v5')


def read(path):
    return json.loads(Path(path).read_text())


def delivered_pages(package):
    return {s: sorted({e['page'] for e in package['evidence_packet']['evidence'][s]})
            for s in ('old', 'new')}


def page_losses(before, after):
    a, b = delivered_pages(before), delivered_pages(after)
    return {s: sorted(set(a[s]) - set(b[s])) for s in a}


def select_events(before, after, selected_ids):
    """Technical selection only; the six are within the pre-existing twelve."""
    rows = []
    if set(before) != set(after):
        raise ValueError('Package roster changed')
    for cid in sorted(before):
        p, q = before[cid], after[cid]
        if (cid in selected_ids and any(page_losses(p, q).values()) and any(
                v['confidence'] != 'SUBJECT_UNRESOLVED' for v in q['canonical_subjects'].values())):
            rows.append(('DELIVERY', cid))
        if p['candidate_subject']['confidence'] == 'STRONG' and q['candidate_subject']['confidence'] != 'STRONG':
            rows.append(('STRONG', cid))
    return rows


def allocation_selection(requirements, receipts, priorities, budget):
    """Replay the unchanged allocator's page selection, retaining multiplicity.

    One source raster per side/document/version/page in these frozen packages.
    Repeated requirements still consume round-robin turns, not extra raster slots.
    """
    chosen = set()
    for priority in sorted({r['priority'] for r in priorities.values()}):
        sides = {s: sorted((r for r in requirements if r['side'] == s and
                            priorities[r['requirement_id']]['priority'] == priority),
                           key=lambda r: (r['page'], r['requirement_id'])) for s in ('OLD', 'NEW')}
        for index in range(max(map(len, sides.values()))):
            for side in ('OLD', 'NEW'):
                if index >= len(sides[side]):
                    continue
                req = sides[side][index]
                receipt = receipts[req['requirement_id']]
                if receipt['available'] and receipt['type_match'] == 'YES' and len(chosen) < budget:
                    chosen.add((side, req['document'], req['document_version'], req['page']))
    return chosen


def selection_receipt(package):
    body = package['f1_requirement_package']
    reqs = body['requirements']
    receipts = {r['requirement_id']: r for r in body['raster_allocation']['requirements']}
    actual = allocation_selection(reqs, receipts, receipts, body['raster_allocation']['budget'])
    recorded = {(r['side'], r['document'], r['document_version'], r['page']) for r in reqs
                if receipts[r['requirement_id']]['selected']}
    if actual != recorded:
        raise ValueError('Allocation replay differs from frozen receipt')
    ordinary = [r for r in reqs if not r['provenance'].get('dependency_of')]
    priorities = allocation_plan([SimpleNamespace(**r) for r in ordinary])
    counterfactual = allocation_selection(ordinary, receipts, priorities, body['raster_allocation']['budget'])
    return dict(status='PASS', selected=sorted(actual),
                without_dependencies=sorted(counterfactual),
                requirements=len(reqs), dependency_count=len(reqs) - len(ordinary),
                evidence_role_counts=dict(Counter(r['evidence_role'] for r in reqs)),
                limitations='Selection-only counterfactual; it neither delivers bytes nor waives required continuations.')


def classify_loss(location, without_dependencies):
    support = location['support']
    if support and all(r['reason'] == 'WRONG_DOCUMENT_OR_CIPHER' for r in support):
        return dict(category='WRONG_DOCUMENT_GUARD_SIDE_EFFECT',
                    subtype='CORRECT_QUARANTINE_NOT_A_REPAIR_TARGET', repair_target=False)
    if any(r['delivery']['omission_reason'] == 'BUDGET_LIMIT' for r in location['v4_requirements']):
        restored = any(row[0].lower() == location['side'] and row[3] == location['page']
                       for row in without_dependencies)
        return dict(category='CONTINUATION_SIDE_EFFECT' if restored else 'OTHER',
                    subtype='DEPENDENCY_PRIORITY_DISPLACEMENT' if restored else 'FIXED_BUDGET_REQUIREMENT_ORDER',
                    repair_target=True, subject_already_accepted=any(r['accepted'] for r in support))
    if any(r['accepted'] for r in support) and not location['v4_requirements']:
        return dict(category='CORRESPONDENCE_RULE_SIDE_EFFECT',
                    subtype='ONE_SIDED_IDENTITY_PROBE_DROPS_ACCEPTED_CONTEXT', repair_target=True)
    if any(r['source_type'] == 'GRAPHIC' and r['reason'] == 'NO_NATIVE_SUBJECT_SUPPORT' for r in support):
        return dict(category='GRAPHIC_SUBJECT_DROPPED',
                    subtype='LITERAL_NATIVE_FUNCTION_REQUIRED_FOR_GRAPHIC_REGION', repair_target=True,
                    requires_raster_confirmation=True)
    return dict(category='OTHER', subtype='UNEXPLAINED_REQUIRES_REVIEW', repair_target=True)


class DiagnosticAccess:
    """Exact artifact read allowlist, output-only writes, no external execution."""
    def __init__(self, output, allowed):
        self.output = output.resolve()
        self.allowed = {Path(p).resolve() for p in allowed}
        self.reads, self.denied = set(), []

    def check(self, event, args):
        if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
            raise PermissionError('F5 V5 diagnosis is offline')
        if event != 'open' or not isinstance(args[0], (str, bytes, Path)):
            return
        path = Path(args[0]).resolve()
        mode, flags = args[1:3]
        writing = (isinstance(mode, str) and any(c in mode for c in 'wax+')) or bool(flags & 3)
        if writing and not path.is_relative_to(self.output):
            raise PermissionError('Only V5 diagnostic output is writable')
        sensitive = path.is_relative_to(V4.parent.parent) or 'projects_v2' in path.parts
        if sensitive and not path.is_relative_to(self.output) and path not in self.allowed:
            self.denied.append(str(path))
            raise PermissionError('Unapproved diagnostic input: ' + str(path))
        if sensitive and not writing:
            self.reads.add(str(path))


def diagnose(output):
    index = read(V4 / 'PACKAGE_INDEX.json')
    before = {r['candidate_id']: read(V3 / r['package']) for r in index}
    after = {r['candidate_id']: read(V4 / r['package']) for r in index}
    roster = {r['package_id'] for r in read(V4 / 'ROSTER.json')['selected']}
    manifest = read(output / 'REGRESSION_9_MANIFEST.json')
    events = manifest['records']
    if sorted(select_events(before, after, roster)) != sorted((e['kind'], e['package_id']) for e in events):
        raise ValueError('Frozen regression selection does not reproduce')
    reviews = {r['event_id']: r for r in read(output / 'SOURCE_IDENTITY_REVIEW.json')['rows']}
    rows = []
    for e in events:
        cid = e['package_id']
        if page_losses(before[cid], after[cid]) != e['lost_pages']:
            raise ValueError('Frozen page loss changed')
        allocation = {v: selection_receipt(p[cid]) for v, p in [('V3', before), ('V4', after)]}
        candidate = refine_candidate(before[cid]['candidate_subject'], after[cid]['canonical_subjects'])
        if candidate['confidence'] != after[cid]['candidate_subject']['confidence']:
            raise ValueError('V4 correspondence downgrade does not reproduce')
        locations = [loc | classify_loss(loc, allocation['V4']['without_dependencies'])
                     for loc in e['exact_loss_locations']]
        categories = sorted({r['category'] for r in locations})
        review = reviews[e['event_id']]
        # A visual review confirms the missing native-function route; it does
        # not certify complete systems or promote any correspondence.
        rows.append(dict(event_id=e['event_id'], package_id=cid, package=e['package'], kind=e['kind'],
            subject=e['v3_status']['candidate']['subject'], scope=e['v3_status']['candidate']['scope'],
            categories=categories, locations=locations, visibility=review,
            allocation_replay=allocation,
            v3_routes=sorted({route for s in ('old', 'new') for unit in e['evidence_refs']['V3'][s]
                              for route in unit['delivered_routes']}),
            candidate_replay_status='PASS', v3_confidence=e['v3_status']['confidence'],
            v4_confidence=e['v4_status']['confidence'], v5_confidence=None,
            repair_replay='NOT_RUN_ROOT_CAUSE_NOT_CONFIRMED'))
    delivery = [r for r in rows if r['kind'] == 'DELIVERY']
    strong = [r for r in rows if r['kind'] == 'STRONG']
    mixed = any(loc['repair_target'] and loc['category'] != 'GRAPHIC_SUBJECT_DROPPED'
                for row in delivery for loc in row['locations'])
    if not mixed:
        raise ValueError('Stop conclusion no longer supported; perform a fresh diagnosis')
    result = dict(status='ROOT_CAUSE_NOT_CONFIRMED', root_cause_confirmed='NO',
        hypothesis_scope='A graphic-aware-only change explains and repairs all nine events',
        narrower_graphic_asymmetry_confirmed=True, regressions_analyzed=len(rows),
        graphic_only_subject_losses=sum(r['visibility']['graphic_only'] for r in strong),
        graphic_only_definition='Lost endpoint has no explicit native text declaration or subject table; indirect room identity is not a declaration. Roof is excluded: native labels and OLD coating table provide context.',
        graphically_supported_strong_losses=3, delivery_with_graphic_region_loss=1,
        delivery_with_budget_loss=sum(any('BUDGET' in loc.get('subtype', '') or loc['category'] == 'CONTINUATION_SIDE_EFFECT'
                                           for loc in r['locations']) for r in delivery),
        continuation_displacement_packages=sum(any(loc['category'] == 'CONTINUATION_SIDE_EFFECT' for loc in r['locations']) for r in delivery),
        one_sided_identity_loss_packages=sum(any(loc['category'] == 'CORRESPONDENCE_RULE_SIDE_EFFECT' for loc in r['locations']) for r in delivery),
        algorithm_changed=False, package_rebuild='NOT_RUN', regression_9_repair_replay='NOT_RUN',
        diagnostic_receipt_replay='PASS', rows=rows,
        recommendation='F5_STILL_NEEDS_REPAIR', model_calls=dict(Codex=0, OpenRouter=0, Claude=0),
        validation='NOT OPENED', final_holdout='NOT OPENED', other_projects='NO', production='UNCHANGED')
    write(output / 'REGRESSION_ROOT_CAUSE.json', result)
    write(output / 'DELIVERY_REGRESSION_BEFORE_AFTER.json', dict(before=6, after=None,
        unchanged_unrepaired=6, repair_executed=False, rows=delivery))
    write(output / 'STRONG_BEFORE_AFTER.json', dict(before=3, after=None,
        unchanged_unrepaired=3, repair_executed=False, rows=strong))
    write(output / 'MULTISOURCE_SUBJECT_AUDIT.json', dict(status='FAIL', implementation='V4_UNCHANGED',
        finding='Native literal function is mandatory even for GRAPHIC; accepted single-side identity is conflated with pair completeness in requirement generation.',
        rows=[dict(event_id=r['event_id'], visibility=r['visibility'], categories=r['categories']) for r in rows]))
    write(output / 'GRAPHIC_SUBJECT_DISCOVERY.json', dict(status='FAIL', implementation='V4_UNCHANGED',
        can_graphic_support_subject_without_text='YES',
        evidence_standard='Local graphic identity requires function, connected/localized elements, scope and document binding; a short mark alone is insufficient. No score threshold or full-system identity was inferred.',
        rows=[r for r in rows if r['visibility']['raster_receipts']]))
    # A reference index explicitly describes V4. It is not a V5 package build.
    write(output / 'PACKAGE_INDEX.json', dict(status='NOT_REBUILT', referenced_iteration='V4',
        referenced_root=str(V4), package_count=len(index), packages=index,
        counts=dict(Counter(r['completeness'] for r in index)),
        correspondence_counts=dict(Counter(r['confidence'] for r in index))))
    reasons = Counter(reason for p in after.values() for b in p['boundary_completeness']['blockers']
                      for reason in b['reasons'])
    write(output / 'PARTIAL_REASON_INDEX.json', dict(status='V4_REFERENCE_ONLY_NOT_REBUILT', counts=dict(reasons)))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUT)
    args = parser.parse_args()
    output = args.output.resolve()
    if output == V4 or output.is_relative_to(V4) or output == V3 or output.is_relative_to(V3):
        raise PermissionError('Prior artifacts are immutable')
    # Fixed exact file set, no prior verdict/report files are admitted.
    allowed = [V4 / 'PACKAGE_INDEX.json', V4 / 'ROSTER.json']
    allowed += [p for root in (V3, V4) for p in root.glob('pair_*/packages/*.json')]
    guard = DiagnosticAccess(output, allowed)
    sys.addaudithook(guard.check)
    result = diagnose(output)
    inputs = sorted(p for p in guard.reads if Path(p) in guard.allowed or Path(p).name in {
        'REGRESSION_9_MANIFEST.json', 'SOURCE_IDENTITY_REVIEW.json'})
    write(output / 'DIAGNOSIS_ACCESS.json', dict(reads=inputs, denied=guard.denied,
        source_truth_read=False, historical_projectchanges_read=False, network_subprocesses_forbidden=True,
        scope='This deterministic diagnostic replay; earlier manifest extraction and raster inspection have separate source receipts.'))
    print(json.dumps({k: result[k] for k in ('status', 'regressions_analyzed', 'graphic_only_subject_losses',
          'delivery_with_budget_loss', 'continuation_displacement_packages', 'one_sided_identity_loss_packages')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
