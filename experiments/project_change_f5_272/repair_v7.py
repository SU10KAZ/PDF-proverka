"""Offline V6 boundary replay. Diagnostic gate precedes the 79-package replay."""
import argparse
from collections import Counter
import json
from pathlib import Path
import sys

from .common import ROOT, REPO, fingerprint, write
from .boundary_v7 import certify, package_completeness
from .observations_v7 import evaluate
from experiments.project_change_272.inventory import sha

V6 = ROOT / 'fresh_dev_sample_f5_pipeline_v6'
DIAGNOSTIC = ROOT / 'fresh_dev_sample_f5_boundary_diagnostic'
OUT = ROOT / 'fresh_dev_sample_f5_pipeline_v7'


def read(path):
    return json.loads(Path(path).read_text())


def code_hashes():
    return {str(p.relative_to(REPO)): sha(p) for p in sorted(
        (REPO / 'experiments/project_change_f5_272').glob('*v7.py'))}


class OfflineAccess:
    def __init__(self, output):
        self.output = output.resolve()
        self.reads = set()
        self.evaluating = False
        self.evaluation_files = set()

    def check(self, event, args):
        if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
            raise PermissionError('V7 forbids all model/network/subprocess calls')
        if event in {'os.remove', 'os.rmdir', 'os.rename', 'os.link', 'os.symlink'}:
            raise PermissionError('V7 writes new artifacts only; deletion/rename/link forbidden')
        if event != 'open' or not isinstance(args[0], (str, bytes, Path)):
            return
        path = Path(args[0]).resolve()
        mode, flags = args[1:3]
        writing = isinstance(mode, str) and any(c in mode for c in 'wax+') or bool(flags & 3)
        if writing:
            if not path.is_relative_to(self.output):
                raise PermissionError('Only new V7 output is writable')
            return
        sensitive = path.is_relative_to(ROOT.parent) or path.is_relative_to(REPO / 'projects_v2')
        if sensitive:
            allowed = (path.is_relative_to(self.output) or path.is_relative_to(V6) or
                       path == DIAGNOSTIC / 'SOURCE_AUDIT_12_PACKAGES.json')
            if self.evaluating:
                allowed = path in self.evaluation_files
            if not allowed:
                raise PermissionError('V7 forbids source, reserve, other-project or diagnostic evidence reads: ' + str(path))
            self.reads.add(str(path))


def verify_inputs(output):
    expected = read(output / 'INPUT_FILE_HASHES.json')[str(V6)]
    actual = {str(p.relative_to(V6)): sha(p) for p in sorted(V6.rglob('*')) if p.is_file()}
    if actual != expected:
        raise ValueError('Immutable V6 artifact set or hashes changed')
    return True


def snapshot(output):
    if output.exists():
        raise FileExistsError('Snapshot requires a new output directory')
    manual_path = DIAGNOSTIC / 'SOURCE_AUDIT_12_PACKAGES.json'
    rows = []
    for audit in read(manual_path)['packages']:
        package = read(V6 / audit['package'])
        rows.append(dict(package=audit['package'], package_id=audit['package_id'], audit_id=audit['audit_id'],
            manual_verdict=audit['primary_verdict'], v6_status=package['completeness'],
            v6_package_hash=package['package_hash'], v6_file_sha256=sha(V6 / audit['package']),
            mandatory=[dict(requirement_id=c['requirement_id'], evidence_type=c['evidence_type'],
                            status=c['status'], reasons=c['reasons'])
                       for c in package['boundary_certificates'] if c['mandatory']]))
    write(output / 'DIAGNOSTIC_12_BEFORE.json', dict(baseline_before_replay=True, rows=rows))
    write(output / 'INPUT_FILE_HASHES.json', {str(V6): {str(p.relative_to(V6)): sha(p)
        for p in sorted(V6.rglob('*')) if p.is_file()},
        str(DIAGNOSTIC): {'SOURCE_AUDIT_12_PACKAGES.json': sha(manual_path)}})
    write(output / 'BASELINE_INDEX.json', read(V6 / 'PACKAGE_INDEX.json'))


def selected(output, full):
    if full:
        gate = read(output / 'DIAGNOSTIC_12_BEFORE_AFTER.json')
        if gate['status'] != 'PASS':
            raise PermissionError('Diagnostic-12 FAIL: full replay is forbidden')
        if read(output / 'ALGORITHM_FREEZE.json') != code_hashes():
            raise ValueError('Boundary code changed after diagnostic freeze')
        return [r['package'] for r in read(output / 'BASELINE_INDEX.json')]
    return [r['package'] for r in read(output / 'DIAGNOSTIC_12_BEFORE.json')['rows']]


def structural(package, certificates):
    issues = []
    if fingerprint({k: v for k, v in package.items() if k != 'package_hash'}) != package['package_hash']:
        issues.append('BASE_PACKAGE_SEAL_MISMATCH')
    requirements = {r['requirement_id']: r for r in package['f1_requirement_package']['requirements']}
    legacy = {c['requirement_id']: c for c in package['boundary_certificates']}
    if {c['evidence_requirement_id'] for c in certificates} != set(requirements) or len(certificates) != len(requirements):
        issues.append('REQUIREMENT_SET_CHANGED')
    for c in certificates:
        rid = c['evidence_requirement_id']
        if c['mandatory'] != legacy[rid]['mandatory'] or c['evidence_type'] != requirements[rid]['required_type']:
            issues.append('REQUIREMENT_ROLE_OR_TYPE_CHANGED')
        if certify(c['proof']['claim'], c['proof']['observation'], c['provenance']) != c:
            issues.append('CERTIFICATE_RECOMPUTATION_MISMATCH')
        if c['completeness'] == 'COMPLETE':
            if not c['delivered_parts'] or c['missing_parts'] or not c['provenance']['evidence_hashes']:
                issues.append('EMPTY_OR_MISSING_COMPLETE_PART')
            if c['note_status']['status'] == 'UNKNOWN_WITH_REASON':
                issues.append('UNRESOLVED_COMPLETE_NOTE')
            if any(p['status'] not in {'PART_VERIFIED', 'PART_NOT_REQUIRED_FOR_CLAIM'} for p in c['delivered_parts']):
                issues.append('UNVERIFIED_COMPLETE_PART')
            available = {e['evidence_id']: e for es in package['evidence_packet']['evidence'].values() for e in es}
            for part in c['delivered_parts']:
                if part['status'] == 'PART_NOT_REQUIRED_FOR_CLAIM':
                    continue
                for eid in part['evidence_ids']:
                    if eid not in available or part['source_locator']['document_version'] != available[eid]['document_version']:
                        issues.append('COMPLETE_PART_EVIDENCE_BINDING_MISMATCH')
    return sorted(set(issues))


def replay(output, guard, full=False):
    verify_inputs(output)
    paths = selected(output, full)
    target = output / ('all_79' if full else 'diagnostic_12')
    if target.exists():
        raise FileExistsError('Replay artifacts are immutable; use a new output for another run')
    # Corpus reads during evaluation are restricted to the selected immutable
    # package files and raster bytes they actually delivered. No source PDFs.
    guard.evaluation_files = {(V6 / p).resolve() for p in paths}
    guard.evaluating = True
    overlays, issues = {}, []
    for relative in paths:
        package = read(V6 / relative)
        before = fingerprint(package)
        for es in package['evidence_packet']['evidence'].values():
            for e in es:
                if e.get('raster'):
                    raster = (V6 / e['raster']['path']).resolve()
                    if not raster.is_relative_to(V6 / 'rasters'):
                        raise PermissionError('Raster outside immutable delivered set')
                    guard.evaluation_files.add(raster)
                    if sha(raster) != e['raster']['sha256']:
                        raise ValueError('Delivered raster hash mismatch')
        first = evaluate(package)
        second = evaluate(package)
        if first != second or before != fingerprint(package):
            raise ValueError('Non-deterministic replay or V6 data mutated in memory')
        issues.extend(dict(package=relative, issue=i) for i in structural(package, first))
        summary = package_completeness(first, baseline_status=package['completeness'])
        overlay = dict(schema='F5_BOUNDARY_OVERLAY/7', base_package=str(V6 / relative),
            base_package_hash=package['package_hash'], base_file_sha256=sha(V6 / relative),
            candidate_id=package['candidate_subject']['candidate_id'],
            correspondence=package['candidate_subject']['confidence'],
            claim_boundary_certificates=first, **summary)
        overlay['boundary_hash'] = fingerprint(overlay)
        overlays[relative] = overlay
    guard.evaluating = False
    if full:
        for relative in selected(output, False):
            if overlays[relative] != read(output / 'diagnostic_12' / relative):
                raise ValueError('Diagnostic certificate drift in all-79 replay')
    for relative, overlay in overlays.items():
        write(target / relative, overlay)
    write(target / 'INDEX.json', [dict(package=p, completeness=o['completeness'],
        correspondence=o['correspondence'], boundary_hash=o['boundary_hash']) for p, o in overlays.items()])
    write(target / 'STRUCTURAL_AUDIT.json', dict(status='PASS' if not issues else 'FAIL', issues=issues,
        evaluated_packages=len(overlays), base_seals_verified=True, raster_hashes_verified=True,
        mandatory_flags_unchanged=True, evidence_types_unchanged=True, two_identical_passes=True))
    if not full:
        write(output / 'ALGORITHM_FREEZE.json', code_hashes())
        diagnostic_audit(output, overlays)
    write(target / 'ACCESS_RECEIPT.json', dict(reads=sorted(guard.reads), model_calls=0,
        evaluated_package_files=paths, source_pdfs_opened=False, source_truth_other_67_opened=False,
        validation='NOT OPENED', final_holdout='NOT OPENED', other_projects='NO'))
    print(json.dumps(dict(stage='all_79' if full else 'diagnostic_12',
        completeness=dict(Counter(o['completeness'] for o in overlays.values())),
        structural='PASS' if not issues else 'FAIL')))


def diagnostic_audit(output, overlays):
    manual_file = DIAGNOSTIC / 'SOURCE_AUDIT_12_PACKAGES.json'
    expected = read(output / 'INPUT_FILE_HASHES.json')[str(DIAGNOSTIC)]['SOURCE_AUDIT_12_PACKAGES.json']
    if sha(manual_file) != expected:
        raise ValueError('Saved manual regression audit changed')
    manual = {r['package']: r for r in read(manual_file)['packages']}
    rows = []
    for before in read(output / 'DIAGNOSTIC_12_BEFORE.json')['rows']:
        after = overlays[before['package']]
        audit = manual[before['package']]
        remaining = []
        if before['manual_verdict'] == 'FALSE_PARTIAL' and after['completeness'] != 'COMPLETE':
            for c in after['claim_boundary_certificates']:
                if c['mandatory'] and c['completeness'] != 'COMPLETE':
                    remaining.append(dict(requirement_id=c['evidence_requirement_id'],
                        evidence_type=c['evidence_type'], reasons=c['explanation'],
                        missing_general_rule='Native/raster row-to-cell mapping and closed group proof for tabular '
                            'content delivered as TEXT/GRAPHIC. A title/whole raster is insufficient; '
                            'no prose heading or ventilation topology should be invented for a schedule.'))
        rows.append(dict(before, after=after['completeness'], boundary_hash=after['boundary_hash'],
            manual_scope=audit['package_decision_reason'], remaining_boundary_rules=remaining))
    false_before = [r for r in rows if r['manual_verdict'] == 'FALSE_PARTIAL']
    negatives = [r for r in rows if r['manual_verdict'] in {'CORRECT_PARTIAL', 'RETRIEVAL_GAP'}]
    false_complete = sum(r['after'] == 'COMPLETE' for r in negatives)
    masked = sum(r['after'] == 'COMPLETE' for r in rows if r['manual_verdict'] == 'RETRIEVAL_GAP')
    corrected = sum(r['after'] == 'COMPLETE' for r in false_before)
    gate = dict(exact_twelve=len(rows) == 12, false_complete_zero=false_complete == 0,
        retrieval_gaps_masked_zero=masked == 0,
        correct_partial_preserved=all(r['after'] == 'PARTIAL' for r in rows if r['manual_verdict'] == 'CORRECT_PARTIAL'),
        false_partial_reduced=corrected > 0,
        structural=read(output / 'diagnostic_12/STRUCTURAL_AUDIT.json')['status'] == 'PASS')
    result = dict(status='PASS' if all(gate.values()) else 'FAIL', gates=gate, rows=rows,
        false_partial_before=len(false_before), false_partial_after=len(false_before) - corrected,
        correct_partial_before=1, correct_partial_after=sum(r['after'] == 'PARTIAL' for r in rows if r['manual_verdict'] == 'CORRECT_PARTIAL'),
        retrieval_gap_before=6, retrieval_gap_after=sum(r['after'] != 'COMPLETE' for r in rows if r['manual_verdict'] == 'RETRIEVAL_GAP'),
        false_complete=false_complete, real_retrieval_gaps_masked=masked,
        audit_scope='Saved twelve DEV package verdicts only; no source truth for other 67')
    write(output / 'DIAGNOSTIC_12_BEFORE_AFTER.json', result)
    print(json.dumps({k: v for k, v in result.items() if k != 'rows'}))


def report(output, guard):
    verify_inputs(output)
    gate = read(output / 'DIAGNOSTIC_12_BEFORE_AFTER.json')
    full = (output / 'all_79/INDEX.json').exists()
    target = output / ('all_79' if full else 'diagnostic_12')
    index = read(target / 'INDEX.json')
    overlays = [read(target / r['package']) for r in index]
    certs = [dict(package=p['base_package'], **c) for p in overlays for c in p['claim_boundary_certificates']]
    for filename, types in [('TEXT_COMPLETENESS_AUDIT.json', {'TEXT_SECTION', 'NOTE'}),
                            ('TABLE_ROW_GROUP_AUDIT.json', {'TABLE_COMPLETE', 'EQUIPMENT_SCHEDULE'}),
                            ('GRAPHIC_FRAGMENT_AUDIT.json', {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'})]:
        write(output / filename, dict(rows=[c for c in certs if c['evidence_type'] in types]))
    for filename, field in [('NOTE_APPLICABILITY_AUDIT.json', 'note_status'), ('CONTINUATION_AUDIT.json', 'continuation_status')]:
        write(output / filename, dict(rows=[dict(package=c['package'], claim_id=c['claim_id'],
            requirement_id=c['evidence_requirement_id'], decision=c[field]) for c in certs]))
    write(output / 'PACKAGE_COMPLETENESS.json', dict(replayed=len(index), baseline_packages=79, rows=[
        {k: v for k, v in p.items() if k != 'claim_boundary_certificates'} for p in overlays]))
    complete = [p for p in overlays if p['completeness'] == 'COMPLETE']
    write(output / 'COMPLETE_PACKAGE_RECEIPTS.json', dict(packages=[dict(
        package=p['base_package'], base_package_hash=p['base_package_hash'], boundary_hash=p['boundary_hash'],
        mandatory_requirements=p['mandatory_requirements'], supporting_missing=p['supporting_missing'],
        claims=[c for c in p['claim_boundary_certificates'] if c['mandatory']]) for p in complete]))
    structural_audit = read(target / 'STRUCTURAL_AUDIT.json')
    write(output / 'STRUCTURAL_AUDIT.json', structural_audit)
    write(output / 'FALSE_COMPLETE_AUDIT.json', dict(status='PASS' if gate['false_complete'] == 0 and structural_audit['status'] == 'PASS' else 'FAIL',
        known_negative_false_complete=gate['false_complete'], retrieval_gaps_masked=gate['real_retrieval_gaps_masked'],
        known_twelve=gate['rows'], other_67='STRUCTURAL_ONLY; semantic false-complete rate not measured',
        complete_receipts=len(complete)))
    stable = read(output / 'ALGORITHM_FREEZE.json') == code_hashes()
    write(output / 'HASH_STABILITY.json', dict(status='PASS' if stable else 'FAIL',
        v6_all_artifact_bytes_unchanged=True, code_stable=stable, two_identical_passes=True,
        package_hash_meaning='Original V6 package seals unchanged; separate deterministic V7 boundary hashes',
        aggregate_boundary_hash=fingerprint({r['package']: r['boundary_hash'] for r in index})))
    rules = dict(schema='F5_BOUNDARY_RULES/7', claim_scope='Frozen V6 requirement and canonical scope',
        text='All accepted subject paragraphs; exact full native page hash; closed sentences; scoped dependencies',
        table='Identity, columns, all relevant/component rows, notes, verified mapping and group end',
        graphic='Verified local endpoints/edges/labels; off-page relation blocks; full page alone is insufficient',
        local_position='Existing LOCAL_POSITION_LABEL scope allows full native annotations with coordinates and full raster; '
                       'topology explicitly excluded by the frozen subject',
        notes='Applicability precedes delivery; UNKNOWN requires a scoped signal',
        parts=['PART_NOT_DELIVERED', 'PART_DELIVERED_NOT_VERIFIED', 'PART_NOT_REQUIRED_FOR_CLAIM'],
        package='Only frozen mandatory requirements block; supporting_missing retained',
        exclusions='No retrieval, subject, correspondence, budget, priority, prompt, F1/F2/F4/V4 or identity-only changes')
    write(output / 'BOUNDARY_RULES_V7.json', rules)
    counts = Counter(r['completeness'] for r in index)
    correspondence = Counter(r['correspondence'] for r in index)
    tests = read(output / 'TEST_RECEIPT.json')
    passed = (gate['status'] == 'PASS' and full and len(index) == 79 and counts['COMPLETE'] > 0
              and correspondence == {'STRONG': 23, 'POSSIBLE': 28, 'UNRESOLVED': 28}
              and counts['MISSING'] == 9 and structural_audit['status'] == 'PASS' and stable and tests['status'] == 'PASS')
    remaining = Counter(reason for c in certs if c['mandatory'] and c['completeness'] != 'COMPLETE' for reason in c['explanation'])
    result = dict(status='F5_V7_BOUNDARY_REPLAY_PASS' if passed else 'F5_V7_BOUNDARY_REPLAY_FAIL',
        diagnostic_12=gate['status'], false_partial_before=2, false_partial_after=gate['false_partial_after'],
        correct_partial_before=1, correct_partial_after=gate['correct_partial_after'], retrieval_gap_before=6,
        retrieval_gap_after=gate['retrieval_gap_after'], false_complete=gate['false_complete'],
        false_complete_scope='Known twelve only; other 67 structural-only',
        all_packages=79, evaluated_packages=len(index), complete_before=0, complete_after=counts['COMPLETE'],
        partial_before=70, partial_after=counts['PARTIAL'], missing=counts['MISSING'], correspondence=dict(correspondence),
        text_complete=sum(c['completeness'] == 'COMPLETE' and c['evidence_type'] in {'TEXT_SECTION','NOTE'} for c in certs),
        table_complete=sum(c['completeness'] == 'COMPLETE' and c['evidence_type'] in {'TABLE_COMPLETE','EQUIPMENT_SCHEDULE'} for c in certs),
        graphic_complete=sum(c['completeness'] == 'COMPLETE' and c['evidence_type'] in {'GRAPHIC_REGION','GRAPHIC_SCHEME'} for c in certs),
        route_count_unit='requirements', top_remaining_partial_reasons=remaining.most_common(8),
        boundary_logic='PASS' if passed else 'FAIL', continuation_logic=tests['status'], note_applicability=tests['status'],
        structural_audit=structural_audit['status'], local_tests=tests['passed'], hash_stability='PASS' if stable else 'FAIL',
        model_calls=0, validation='NOT OPENED', final_holdout='NOT OPENED', other_projects='NO', production='UNCHANGED',
        recommendation='READY_FOR_FRESH_INFERENCE' if passed else 'F5_STILL_NEEDS_REPAIR')
    write(output / 'F5_V7_RESULT.json', result)
    text = '# F5 V7 — claim-scoped boundary replay\n\n```json\n' + json.dumps(result, ensure_ascii=False, indent=2) + '\n```\n\n'
    text += ('V6 packages and every V6 artifact remain byte-identical. V7 stores boundary overlays, '
             'retaining original package hashes and separate boundary hashes. Subject discovery, correspondence, '
             'delivery, allocation, requirements, identity-only, F1/F2/F4/V4 and prompts were not rerun or changed.\n\n'
             'Diagnostic-12 ran before the full replay. Only the saved manual audit is used as semantic regression evidence. '
             'Other 67 packages receive structural checks, not new source-truth review. No model or network calls.\n\n'
             'A LOCAL_POSITION_LABEL certificate proves delivered native annotation text and its location on the raster. '
             'It does not prove a connected ventilation network; the existing V6 canonical scope explicitly excludes topology.\n\n'
             'Remaining known FALSE_PARTIAL: tabular content represented by TEXT/GRAPHIC still needs general verified '
             'row-to-cell and closed-row-group extraction from the delivered raster. Full-page delivery and a table title '
             'are insufficient. This limitation is retained per requirement in DIAGNOSTIC_12_BEFORE_AFTER.json.\n\n'
             'TEXT/TABLE/GRAPHIC COMPLETE counts are requirements, not packages. False COMPLETE = 0 is measured on '
             'the known diagnostic negatives only; it is not an unmeasured semantic guarantee for all 79.\n\n'
             'Stop after boundary replay. No Astra call or production deployment.\n')
    (output / 'F5_V7_REPORT.md').write_text(text)
    write(output / 'ACCESS_RECEIPT.json', dict(reads=sorted(guard.reads), model_calls=0,
        validation='NOT OPENED', final_holdout='NOT OPENED', other_projects='NO',
        source_pdfs_opened=False, source_truth_other_67_opened=False))
    print(json.dumps(result, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('snapshot', 'diagnostic', 'all', 'report'))
    parser.add_argument('--output', type=Path, default=OUT)
    args = parser.parse_args()
    output = args.output.resolve()
    if any(output == p or output.is_relative_to(p) for p in (V6, DIAGNOSTIC)):
        raise PermissionError('Previous iterations are immutable')
    guard = OfflineAccess(output)
    sys.addaudithook(guard.check)
    if args.action == 'snapshot':
        snapshot(output)
    elif args.action == 'report':
        report(output, guard)
    else:
        replay(output, guard, args.action == 'all')


if __name__ == '__main__':
    main()
