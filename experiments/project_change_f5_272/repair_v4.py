"""Offline F5 V4: frozen 12 first, audit, then the identical logic over 79."""
import argparse
from collections import Counter
from dataclasses import asdict
import json
from pathlib import Path
import sys

from experiments.project_change_272.inventory import ROOT, sha
from experiments.project_change_contracts_v3_272.evidence import coverage_receipt
from .common import OUT as V1, AccessAudit, admit, code_hashes, fingerprint, write
from .repair_v2 import OUT as V2, file_hashes
from .repair_v3 import OUT as V3

OUT = ROOT / 'fresh_dev_sample_f5_pipeline_v4'
DIAGNOSTIC = ROOT / 'fresh_dev_sample_f5_boundary_diagnostic'
PREVIOUS = (V1, V2, V3, DIAGNOSTIC)
STAGES = ['DOCUMENT_INVENTORY', 'SUBJECT_DISCOVERY', 'SUBJECT_CONFIDENCE',
          'OLD_NEW_CORRESPONDENCE', 'CONTINUATION_ANALYSIS', 'EVIDENCE_REQUIREMENTS',
          'BUDGET_ALLOCATION', 'DELIVERY', 'BOUNDARY_CERTIFICATE', 'F1', 'F4', 'F2_V4']


def read(path):
    return json.loads(Path(path).read_text())


def snapshot(output):
    if output.exists():
        raise FileExistsError('V4 requires a new output directory')
    write(output / 'PREVIOUS_FILE_HASHES.json', {str(p): file_hashes(p) for p in PREVIOUS})
    manifest = DIAGNOSTIC / 'BOUNDARY_DIAGNOSTIC_MANIFEST.json'
    value = read(manifest)
    write(output / 'ROSTER.json', dict(manifest_sha256=sha(manifest),
        selected=[{k: p[k] for k in ('package_id', 'package', 'pair', 'diagnostic_type')}
                  for p in value['selected_packages']],
        all=[{k: p[k] for k in ('package_id', 'package', 'pair')} for p in value['candidate_pool']],
        purpose='Tracking IDs only; no diagnostic judgements, source truth or historical requirements in build',
        prior_exposure='Previously audited DEV. Developer has seen the 12 diagnoses; not a blind test.'))


def finish_f1(body, requirements):
    receipt = coverage_receipt(requirements, body['evidence'])
    delivery = {r['requirement_id']: r for r in body['raster_allocation']['requirements']}
    for row in receipt['requirements']:
        r = delivery[row['requirement']['requirement_id']]
        row['delivery'] = r
        if r['omission_reason'] == 'BUDGET_LIMIT':
            row['completeness'] = row['requirement']['completeness'] = 'PARTIAL_BUDGET_LIMIT'
            row['missing_reason'] = 'Available raster omitted at fixed package budget'
    receipt.update(schema='EVIDENCE_COVERAGE/3', status='COMPLETE' if receipt['complete'] else 'PARTIAL')
    body.update(evidence_coverage=receipt, coverage_complete=receipt['complete'], scope_discovery='F5_NATIVE_SUBJECT_AND_CONTINUATION_V4')
    body['package_hash'] = fingerprint({k: v for k, v in body.items() if k != 'package_hash'})


def one_package(prepared, docs, canonical, guards, original, output):
    from .subject_v4 import refine_candidate
    from .requirements_v4 import build
    from .allocation_v4 import package as deliver
    from .contract_adapters import DiscoveryPageLoader, source_packet, graphic_bindings, typed_preparation
    from .boundary_v4 import observation
    from .boundary import certify_requirement, package_completeness
    from .end_to_end_package_builder import portable
    from .run import assert_answer_blind
    candidate = refine_candidate(original, canonical)
    reqs, sources, analyses, rows = build(candidate, canonical, prepared, docs, guards)
    pair, invs = prepared['pair'], prepared['inventories']
    guard_map = {(g['side'], g['page']): g for g in guards}
    loader = DiscoveryPageLoader(pair, invs, sources, output / 'rasters')
    def guarded_loader(req):
        if guard_map.get((req.side.lower(), req.page), {}).get('usable') == 'NO':
            return None
        return loader(req)
    try:
        body = deliver(reqs, guarded_loader, text_budget=28000, raster_budget=8)
    finally:
        loader.close()
    packet = source_packet(body, pair, candidate)
    evidence = [e for side in ('old', 'new') for e in packet['evidence'][side]]
    observations = {}
    for row in packet['evidence_coverage']['requirements']:
        req = row['requirement']
        observations[req['requirement_id']] = observation(req, row, docs[req['side'].lower()], evidence,
            candidate, analyses.get(req['requirement_id']), guard_map.get((req['side'].lower(), req['page'])))
    # Boundary decisions precede F1 evaluation; the final certificate then binds
    # this same observation to the finalized transport packet hash.
    from .boundary import decide
    initial_decisions = {r['requirement_id']: decide(r['required_type'], observations[r['requirement_id']])
                         for r in body['requirements']}
    finish_f1(body, reqs)
    packet = source_packet(body, pair, candidate)
    certs = []
    for row in packet['evidence_coverage']['requirements']:
        req = row['requirement']
        cert = certify_requirement(req, row['delivery']['mandatory'], observations[req['requirement_id']],
            dict(pair_index=pair['index'], pair_key=pair['pair_key'], candidate_id=candidate['candidate_id'],
                document=req['document'], document_version=req['document_version'], side=req['side'],
                source_pdf=invs[req['side'].lower()]['source']['pdf'], source_region=req['scope_binding'],
                packet_hash=fingerprint(packet), requirement_hash=fingerprint(req)))
        if (cert['status'], cert['reasons']) != initial_decisions[req['requirement_id']]:
            raise ValueError('F1 serialization changed an early boundary decision')
        certs.append(cert)
    completeness = package_completeness(certs)
    statuses = [r['completeness'] for r in body['evidence_coverage']['requirements']]
    legacy = 'COMPLETE' if all(s == 'COMPLETE' for s in statuses) else 'MISSING' if all(s == 'MISSING' for s in statuses) else 'PARTIAL'
    graphics = graphic_bindings(packet, candidate, output)
    typed = typed_preparation(packet, candidate)
    result = dict(schema='MODEL_READY_PACKAGE/5.4', package_status='MODEL_READY_PACKAGE',
        candidate_subject=candidate, canonical_subjects={sid: canonical[sid] for s in ('old', 'new') for sid in candidate[s]},
        evidence_packet=packet, f1_requirement_package=body,
        boundary_certificates=certs, boundary_completeness=completeness,
        completeness=completeness['completeness'], legacy_f1_completeness=legacy,
        completeness_authority='UNCHANGED_F5_BOUNDARY_PREDICATES_AFTER_V4_SUBJECT_CONTINUATION_OBSERVATION',
        typed_state_skeleton=typed, graphic_bindings=graphics,
        counter_evidence=body['counter_evidence'], inference_executed=False,
        requirement_plan=rows, continuation_analysis=analyses, pipeline_stages=STAGES,
        provenance=dict(pair_index=pair['index'], pair_key=pair['pair_key'], partition='DEV',
            document_versions={s: pair[s]['document_version'] for s in ('old', 'new')},
            subject_index_hash=fingerprint(canonical), legacy_tracking_roster_hash=fingerprint(prepared['correspondence'])))
    result = portable(result, output)
    result['package_hash'] = fingerprint(result)
    assert_answer_blind(result)
    return result


def source_build(output, all_packages=False):
    from .end_to_end_package_builder import prepare
    from .boundary_sources import Document
    from .subject_v4 import refine_subjects
    roster = read(output / 'ROSTER.json')
    if all_packages:
        if not (output / 'DIAGNOSTIC_12_RESULTS.json').exists():
            raise FileNotFoundError('Audit the frozen 12 before the 79-package regression')
        if read(output / 'ALGORITHM_FREEZE.json')['code'] != code_hashes():
            raise ValueError('Algorithm changed after diagnostic freeze')
    target = output if all_packages else output / 'diagnostic_12'
    if (target / 'PACKAGES_FREEZE.json').exists():
        raise FileExistsError('This build is already frozen')
    pairs = admit()
    access = AccessAudit(pairs, output)
    access.install()
    frozen_code = code_hashes()
    expected = {r['package_id'] for r in roster['all']}
    selected = {r['package_id'] for r in roster['selected']}
    inventory_rows, canonicals, subject_audit, guard_rows, packages, traces = [], {}, [], [], {}, []
    actual = set()
    from .subject_discovery import discover
    from .subject_correspondence import correspond
    first_prepared = []
    for pair in pairs:
        cache = output / '_inventory_cache' / f'pair_{pair["index"]}'
        paths = {s: cache / f'DOCUMENT_INVENTORY_{s.upper()}.json' for s in ('old', 'new')}
        if all(p.exists() for p in paths.values()):
            invs = {s: read(p) for s, p in paths.items()}
            for s, inv in invs.items():
                if inv['document_version'] != pair[s]['document_version'] or inv['source'] != pair[s]['artifacts']:
                    raise ValueError('Source inventory cache binding drift')
            indices = {s: discover(inv) for s, inv in invs.items()}
            prepared = dict(pair=pair, inventories=invs, indices=indices,
                subjects={x['subject_id']: x for index in indices.values() for x in index['subjects']},
                correspondence=correspond(indices['old']['subjects'], indices['new']['subjects']))
        else:
            prepared = prepare(pair)
            for s, inv in prepared['inventories'].items():
                write(paths[s], inv)
        first_prepared.append(prepared)
    write(output / 'SOURCE_INVENTORY_FREEZE.json', dict(
        inventories={str(p.relative_to(output)): sha(p) for p in sorted((output / '_inventory_cache').glob('pair_*/DOCUMENT_INVENTORY_*.json'))},
        producer='Fresh V4 source inventory; shared unchanged across diagnostic and all-package builds',
        inventory_code_sha256=sha(Path(__file__).with_name('document_inventory.py'))))
    total = sum(len(p['correspondence']['candidates']) for p in first_prepared)
    if total > 80:
        write(output / 'STOP.json', dict(status='PACKAGE_EXPLOSION', packages=total))
        raise SystemExit('PACKAGE_EXPLOSION')
    for prepared in first_prepared:
        pair = prepared['pair']
        docs = {s: Document(inv) for s, inv in prepared['inventories'].items()}
        canonical, audits, guards = refine_subjects(prepared, docs)
        canonicals.update(canonical); subject_audit.extend(audits); guard_rows.extend(guards)
        for side, inv in prepared['inventories'].items():
            write(target / f'pair_{pair["index"]}/DOCUMENT_INVENTORY_{side.upper()}.json', inv)
        # Discovery and subject confidence are frozen before correspondence is
        # re-evaluated; legacy IDs address the exact same research questions.
        actual.update(c['candidate_id'] for c in prepared['correspondence']['candidates'])
        for candidate in prepared['correspondence']['candidates']:
            if not all_packages and candidate['candidate_id'] not in selected:
                continue
            one = one_package(prepared, docs, canonical, guards, candidate, target)
            two = one_package(prepared, docs, canonical, guards, candidate, target)
            if one != two:
                raise ValueError('Nondeterministic package rebuild')
            rel = f'pair_{pair["index"]}/packages/{candidate["candidate_id"]}.json'
            write(target / rel, one)
            packages[rel] = one
            traces.append(dict(package=rel, stages=STAGES, requirements=len(one['requirement_plan']),
                fixed_budget=one['f1_requirement_package']['limits']))
            print(json.dumps(dict(built=candidate['candidate_id'], requirements=len(one['requirement_plan']),
                completeness=one['completeness']), ensure_ascii=False), flush=True)
    if actual != expected:
        raise ValueError('Frozen 79 tracking roster changed; no implicit re-selection allowed')
    if frozen_code != code_hashes():
        raise ValueError('Code changed during build')
    hashes = {rel: p['package_hash'] for rel, p in packages.items()}
    write(target / 'PACKAGES_FREEZE.json', dict(package_hashes=hashes, aggregate_hash=fingerprint(hashes),
        full_rerun_deterministic=True, passes=2))
    write(target / 'SUBJECT_INDEX_V4.json', canonicals)
    write(target / 'SUBJECT_AUDIT.json', dict(subjects=subject_audit,
        strong_downgrades=[dict(candidate_id=p['candidate_subject']['candidate_id'],
            reason=p['candidate_subject']['strong_downgrade_reason'], canonical_subjects=p['canonical_subjects'])
            for p in packages.values() if p['candidate_subject']['strong_downgrade_reason']],
        policy='No subject splitting implemented; unproven compound groups stay unresolved or possible. No manual merges.'))
    write(target / 'WRONG_DOCUMENT_AUDIT.json', dict(pages=guard_rows,
        mismatches=[g for g in guard_rows if g['usable'] == 'NO'], proven_cross_document_bindings=[]))
    write(target / 'CONTINUATION_AUDIT.json', [dict(package=rel, requirements=p['continuation_analysis']) for rel, p in packages.items()])
    write(target / 'NOTE_APPLICABILITY_V4.json', [dict(package=rel, requirement_id=qid, notes=a['notes'])
        for rel, p in packages.items() for qid, a in p['continuation_analysis'].items()])
    write(target / 'PACKAGE_INDEX.json', [dict(package=rel, candidate_id=p['candidate_subject']['candidate_id'],
        confidence=p['candidate_subject']['confidence'], subject_confidence=p['candidate_subject']['subject_confidence'],
        completeness=p['completeness'], package_hash=p['package_hash']) for rel, p in packages.items()])
    write(target / 'PACKAGE_COMPLETENESS.json', [dict(package=rel, **p['boundary_completeness']) for rel, p in packages.items()])
    write(target / 'PIPELINE_TRACE.json', traces)
    write(target / 'BUILD_ACCESS.json', dict(reads=sorted(access.reads), denied=access.denied,
        historical_truth_read=False, model_calls=0, network_and_subprocesses_forbidden=True))
    write(target / 'HASH_STABILITY.json', dict(status='PASS', package_passes=2, package_count=len(packages),
        aggregate_hash=fingerprint(hashes), inventory_passes=1,
        limitations='Two complete delivery/certificate rebuilds from one fresh inventory; not two independent extractions'))
    if not all_packages:
        write(output / 'ALGORITHM_FREEZE.json', dict(code=frozen_code, selected_package_hashes=hashes,
            selection_manifest_sha256=roster['manifest_sha256'], policy='No changes after frozen diagnostic rebuild'))
    else:
        frozen = read(output / 'ALGORITHM_FREEZE.json')['selected_package_hashes']
        if any(hashes.get(rel) != value for rel, value in frozen.items()):
            raise ValueError('The 12 packages changed during the 79-package regression')


def offline(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
        raise PermissionError('F5 V4 forbids model calls, network and subprocesses')
    if event == 'open' and isinstance(args[0], (str, bytes, Path)):
        path = Path(args[0]).resolve()
        mode, flags = args[1:3]
        writing = (isinstance(mode, str) and any(c in mode for c in 'wax+')) or bool(flags & 3)
        if writing and any(path.is_relative_to(p) for p in PREVIOUS):
            raise PermissionError('Previous artifact trees are immutable')


def main():
    sys.addaudithook(offline)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('snapshot', 'diagnostic-build', 'diagnostic-audit', 'all-build', 'audit'))
    parser.add_argument('--output', type=Path, default=OUT)
    args = parser.parse_args()
    output = args.output.resolve()
    if any(output == p or output.is_relative_to(p) for p in PREVIOUS):
        raise PermissionError('Previous artifact trees are immutable')
    if args.action == 'snapshot':
        snapshot(output)
    elif args.action in {'diagnostic-build', 'all-build'}:
        source_build(output, args.action == 'all-build')
    else:
        from .audit_v4 import diagnostic_audit, final_audit
        (diagnostic_audit if args.action == 'diagnostic-audit' else final_audit)(output)


if __name__ == '__main__':
    main()
