"""Two-pair deterministic DEV replay. No model/provider imports or calls."""
import argparse
from collections import Counter
from pathlib import Path
import json

from experiments.project_change_272.inventory import ROOT, read, sha
from experiments.project_change_semantic_272.access import prepared_pairs
from experiments.project_change_semantic_272.history import document_history
from .evidence import EvidenceRequirement, comparison_readiness
from .packages import AdmittedPageLoader, package, write_package
from .delivery import semantic_packet, write_semantic_packet

FIXTURES = Path(__file__).with_name('two_pair_cases.json')


def run(output, source_checkout, phase='f1'):
    output = Path(output)
    fixtures = read(FIXTURES)
    indices = {c['pair_index'] for c in fixtures['cases']}
    pairs = {p['index']: p for p in prepared_pairs('DEV', indices=indices, source_repo=source_checkout)}
    documents, excluded = {}, {}
    for p in pairs.values():
        for side in ('old', 'new'):
            doc = p[side]
            key = (doc['document_code'], doc['document_version'], side.upper())
            documents[key] = doc
            excluded[key] = set(p['embargo_pages'][side]) | set(document_history(doc, p['embargo_pages'][side]))
    loader = AdmittedPageLoader(documents, excluded, output.parent / 'rasters')
    rows = []
    try:
        for case in fixtures['cases']:
            provenance = case['source_audit']
            if sha(provenance['path']) != provenance['sha256']:
                raise ValueError('Source-first truth drift')
            requirements = []
            for n, spec in enumerate(case['requirements']):
                doc = pairs.get(spec['pair_index'], {}).get(spec['side'].lower())
                # Linked source outside this two-pair run is explicitly MISSING.
                document = doc['document_code'] if doc else 'OUTSIDE_TWO_PAIR_SCOPE'
                version = doc['document_version'] if doc else 'NOT_ADMITTED'
                r = EvidenceRequirement(requirement_id=f"{case['pair_index']}_{case['case_id']}_{n}",
                    subject=case['title'], side=spec['side'], document=document, document_version=version,
                    page=spec['page'], evidence_type=spec['evidence_type'], evidence_role=spec['evidence_role'],
                    expected_semantic_content=spec['expected_semantic_content'], provenance=provenance,
                    scope_binding=f"source-audit:{provenance['sha256']}:{case['case_id']}:{spec['pair_index']}:{spec['side']}:{spec['page']}")
                requirements.append(r)
            body = package(requirements, loader)
            target = output / 'packages' / f"pair{case['pair_index']}_{case['case_id']}"
            write_package(target, body)
            delivery = semantic_packet(body, pairs[case['pair_index']])
            write_semantic_packet(target/'semantic', delivery)
            from experiments.project_change_semantic_272.run import view
            from experiments.project_change_semantic_272.prompts import PROPOSE
            from experiments.project_change_semantic_272.vision import image_messages
            # Build the exact local input representation, including image bytes;
            # no client/transport is constructed and nothing is sent.
            request_characters = len(PROPOSE) + len(json.dumps(view(delivery), ensure_ascii=False))
            if request_characters > 60000:
                raise ValueError('STOP: request text plus prompt exceeds existing input budget')
            image_blocks, image_receipts = image_messages(delivery)
            del image_blocks
            receipt = body['evidence_coverage']
            # Independent invariant: COMPLETE requires actual full raster delivery
            # bound to this requirement's exact source, page and subject.
            by_id = {e['evidence_id']: e for e in body['evidence']}
            false_complete = [r['requirement']['requirement_id'] for r in receipt['requirements']
                if r['completeness'] == 'COMPLETE' and not any(
                    by_id[i]['raster'] and by_id[i]['boundary_complete'] and
                    by_id[i]['scope_binding'] == r['requirement']['scope_binding']
                    for i in r['evidence_ids'])]
            statuses = Counter(r['completeness'] for r in receipt['requirements'])
            per_side = {s: dict(Counter(r['completeness'] for r in receipt['requirements']
                if r['requirement']['side'] == s)) for s in ('OLD', 'NEW')}
            row = dict(pair_index=case['pair_index'], case_id=case['case_id'], title=case['title'],
                source_truth=case['source_truth'], truth_origin=case['truth_origin'],
                source_audit=provenance, required_evidence=case['requirements'],
                old_package_state=dict(basis='SOURCE_FIRST_AUDIT_OF_HISTORICAL_PACKAGES',
                    assessment=case['before_assessment'], requirement_receipt_existed=False),
                new_package_state=per_side, f1_before=case['before_assessment'],
                f1_after=dict(status='FAIL' if false_complete else 'PASS', completeness=dict(statuses),
                    false_complete=false_complete, coverage_complete=receipt['complete']),
                collection_checks={
                    name: ('NOT_APPLICABLE' if not group else 'COMPLETE' if all(
                        r['completeness'] == 'COMPLETE' for r in group) else 'PARTIAL_OR_MISSING')
                    for name, group in {
                        'old_required': [r for r in receipt['requirements'] if r['requirement']['side'] == 'OLD'],
                        'new_required': [r for r in receipt['requirements'] if r['requirement']['side'] == 'NEW'],
                        'counter_evidence': [r for r in receipt['requirements'] if r['requirement']['evidence_role'] == 'COUNTER'],
                        'table_section': [r for r in receipt['requirements'] if r['requirement']['evidence_type'] in
                                          {'TEXT_SECTION','TABLE_COMPLETE','EQUIPMENT_SCHEDULE','NOTE'}],
                        'graphic_counterpart': [r for r in receipt['requirements'] if r['requirement']['evidence_type'] == 'GRAPHIC_REGION'],
                    }.items()},
                readiness=comparison_readiness(receipt, case['title'], novelty=any(
                    r.evidence_role == 'COUNTER' for r in requirements)),
                package=str(target / 'PACKAGE.json'), coverage_receipt=str(target / 'EVIDENCE_COVERAGE.json'),
                typed_state=case['source_typed_context'], applicable_conditions='PENDING_F2',
                comparability='PENDING_F2', materiality_rule='PENDING_F2',
                expected_semantic_disposition=case['source_truth'], new_inference_result=None)
            if phase == 'final':
                from .state_regression import evaluate_fixture
                row.update(evaluate_fixture(case))
            rows.append(row)
            print(f"pair {case['pair_index']} {case['case_id']}: {dict(statuses)}", flush=True)
    finally:
        loader.close()
    # Check original truth again after all work. Never write to source artifacts.
    for case in fixtures['cases']:
        if sha(case['source_audit']['path']) != case['source_audit']['sha256']:
            raise ValueError('Truth changed during regression')
    result = dict(schema='TWO_PAIR_F1_F4_F2_REGRESSION/1', phase=phase,
        f1_gate='PASS' if all(r['f1_after']['status'] == 'PASS' for r in rows) else 'FAIL',
        f2_gate=('PASS' if all(r.get('f2_regression_pass') for r in rows if r.get('f2_required')) else 'FAIL')
            if phase == 'final' else 'NOT_RUN',
        cases=rows, model_calls=0, validation_opened=False, final_holdout_opened=False,
        other_projects_used=False, production_changed=False, historically_blind=False,
        fixture_sha256=sha(FIXTURES), split_sha256=sha(ROOT/'SPLIT.json'),
        opened_pages=sorted({(r['document'], r['side'], r['page']) for r in loader.opened}),
        limitations=['Bounded source-audit retrieval, not autonomous subject discovery.',
            'PASS tests honest delivery status, not complete recall or inference accuracy.',
            'Linked evidence outside the two selected pairs remains MISSING.',
            'R25 was added after outputs; prior exposure is preserved.'])
    output.mkdir(parents=True, exist_ok=True)
    name = 'TWO_PAIR_F1_F4_F2_REGRESSION' if phase == 'final' else 'F1_REGRESSION'
    (output/(name+'.json')).write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)+'\n')
    lines = ['# '+name, '', f"F1: **{result['f1_gate']}**. F2: **{result['f2_gate']}**. Model calls: **0**.", '',
        'Each row links a complete machine-readable requirement receipt. COMPLETE means delivery of an audited region, not engineering truth.', '',
        '| Pair / case | Source truth | Before | After | OLD / NEW / counter / table-section / graphic |',
        '| --- | --- | --- | --- | --- |']
    for r in rows:
        before = r['f1_before'].replace('|', '/').replace('\n', ' ')
        lines.append(f"| {r['pair_index']} / {r['case_id']} | {r['source_truth']} | {before} | "
                     f"[{r['f1_after']['completeness']}]({r['coverage_receipt']}) | "
                     + ' / '.join(r['collection_checks'].values())+' |')
    lines += ['', '## Limits', ''] + ['- '+s for s in result['limitations']]
    (output/(name+'.md')).write_text('\n'.join(lines)+'\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--source-checkout', required=True, type=Path)
    parser.add_argument('--phase', choices=['f1', 'final'], default='f1')
    args = parser.parse_args()
    result = run(args.output, args.source_checkout, args.phase)
    if result['f1_gate'] != 'PASS' or result['f2_gate'] == 'FAIL':
        raise SystemExit('STOP: deterministic gate failed; no inference permitted')


if __name__ == '__main__':
    main()
