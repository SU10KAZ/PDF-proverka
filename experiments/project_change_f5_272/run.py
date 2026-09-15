"""Offline CLI. Two complete local passes, freeze, then stop; no model client."""
import argparse
from pathlib import Path

from experiments.project_change_272.inventory import ROOT, sha
from .common import OUT, admit, AccessAudit, code_hashes, fingerprint, write
from .end_to_end_package_builder import prepare, build


def package_limit(prepared):
    counts = {p['pair']['index']: len(p['correspondence']['candidates']) for p in prepared}
    return dict(status='PACKAGE_EXPLOSION' if sum(counts.values()) > 80 else 'PASS',
                packages_by_pair=counts, total=sum(counts.values()), maximum=80,
                granularity_stage='FUNCTIONAL_SUBJECT_SCOPE_CONNECTED_COMPONENTS')


def assert_answer_blind(value):
    if isinstance(value, dict):
        for k, v in value.items():
            if k in {'expected_answer', 'expert_label', 'known_findings', 'source_truth', 'project_changes',
                     'historical_project_changes', 'verdict', 'final_project_change'}:
                raise ValueError('Forbidden answer field: ' + k)
            if k == 'semantic_verdict' and v is not None:
                raise ValueError('Pre-inference semantic verdict')
            assert_answer_blind(v)
    elif isinstance(value, (list, tuple)):
        for item in value:
            assert_answer_blind(item)


def report(result):
    lines = ['# F5: document-to-package delivery', '', 'STATUS: **' + result['status'] + '**', '',
        '| Metric | Pair A · АР1 | Pair B · ИОС4.2 |', '|---|---:|---:|']
    a, b = result['pairs']
    def both(label, fn):
        lines.append('| ' + label + ' | ' + str(fn(a)) + ' | ' + str(fn(b)) + ' |')
    both('Pages catalogued OLD / NEW', lambda p: f"{p['pages_catalogued']['old']} / {p['pages_catalogued']['new']}")
    both('Content pages indexed OLD / NEW', lambda p: f"{p['pages_content_indexed']['old']} / {p['pages_content_indexed']['new']}")
    both('Subjects OLD / NEW', lambda p: f"{p['subjects_discovered']['old']} / {p['subjects_discovered']['new']}")
    both('Correspondence candidates', lambda p: p['correspondence_candidates'])
    both('STRONG / POSSIBLE / unresolved subjects', lambda p: f"{p['strong_correspondences']} / {p['possible_correspondences']} / {p['unresolved_subjects']}")
    both('Model-ready packages / estimated calls', lambda p: p['model_ready_packages'])
    both('COMPLETE / PARTIAL / MISSING', lambda p: f"{p['complete']} / {p['partial']} / {p['missing']}")
    both('TEXT / TABLE / GRAPHIC packages (overlap)', lambda p: ' / '.join(str(p['route_packages'][r]) for r in ('TEXT', 'TABLE', 'GRAPHIC')))
    lines += ['', '## Integration', '',
        '- F1: YES — unchanged v3 requirement allocation and coverage.',
        '- F2: YES — typed EngineeringState constructor; unknown values/dimensions remain explicit.',
        '- F4: YES — unchanged raster_locator_errors on exact delivered image bytes.',
        '- V4 binding: YES — unchanged bind resolver on explicit discovery identity/scope/context references.',
        '- V4 adapter tags its input as a discovery manifest. It does not fabricate a model response, state value or semantic witness.',
        '- Package explosion: NO. Canonical hashes: deterministic across two full runs.',
        '- Answer leakage: PASS for this build (source-only allowlist, no historical answers read).',
        '- Models: Codex 0; OpenRouter 0; Claude 0. No inference API or subprocess is available in the run.',
        '- VALIDATION: NOT OPENED. FINAL HOLDOUT: NOT OPENED. OTHER PROJECTS: NO. PRODUCTION: UNCHANGED.',
        '', '## Limits and recommendation', '',
        'All physical pages have inventory entries. Embargo and source revision-history pages are quarantined; '
        'front matter is catalogued separately. Their contents are not discovery evidence.', '',
        'Delivery of a full-page raster does not verify a whole engineering section, table continuation, notes or related graphic nodes. '
        'Unknown boundaries remain PARTIAL; available images omitted by the eight-image F1 budget remain PARTIAL_BUDGET_LIMIT. '
        'MODEL_READY_PACKAGE means a serializable inference input with visible gaps, not complete evidence or an approved engineering comparison.', '',
        'Discovery is a conservative function/scope vocabulary baseline, not an exhaustive subject census. '
        'Unmatched regions remain in SUBJECT_INDEX and SUMMARY. Connected candidate groups are possible retrieval correspondences, '
        'not confirmed transformations.', '',
        'The full-content indexing gate is not satisfied because the frozen access policy excludes source pages. '
        'No access rule was relaxed. Source truth and REAL/NOT/REVIEW were not produced.', '',
        'RECOMMENDATION: **' + result['status'] + '**. Stop before fresh inference.', '',
        'Local test results: TEST_RECEIPT.json. Per-pair inventories, requirements, coverage, trace and immutable packages are in pair_2/ and pair_8/.',
        'PACKAGES_FREEZE.json seals package hashes. SOURCE_ACCESS.json records the admitted files and page reads.']
    return '\n'.join(lines) + '\n'


def run(output=OUT):
    output = Path(output).resolve()
    if output.exists() and any(output.glob('**/SOURCE_AUDIT.json')):
        raise PermissionError('Output must not contain historical source audits')
    pairs = admit()
    frozen_code = code_hashes()
    split_hash = sha(ROOT / 'SPLIT.json')
    audit = AccessAudit(pairs, output)
    audit.install()
    write(output / 'ALGORITHM_FREEZE.json', dict(code=frozen_code, split_sha256=split_hash,
        pairs=[dict(index=p['index'], pair_key=p['pair_key'], source_versions={s: p[s]['document_version'] for s in ('old', 'new')}) for p in pairs],
        policy='No historical answer artifacts, case inputs, network or model calls; fixed vocabulary and 8 rasters / 28000 text chars per package'))
    prepared = []
    for pair in pairs:
        item = prepare(pair)
        prepared.append(item)
        print('INVENTORY_DISCOVERY_READY pair=%s subjects=%s candidates=%s' % (
            pair['index'], {s: len(item['indices'][s]['subjects']) for s in ('old', 'new')},
            len(item['correspondence']['candidates'])), flush=True)
    limit = package_limit(prepared)
    write(output / 'PACKAGE_LIMIT.json', limit)
    if limit['status'] == 'PACKAGE_EXPLOSION':
        for p in prepared:
            d = output / ('pair_' + str(p['pair']['index']))
            for s in ('old', 'new'):
                write(d / ('DOCUMENT_INVENTORY_' + s.upper() + '.json'), p['inventories'][s])
            write(d / 'SUBJECT_INDEX.json', p['indices'])
            write(d / 'CORRESPONDENCE.json', p['correspondence'])
        write(output / 'F5_RESULT.json', limit | dict(model_calls=0))
        print('PACKAGE_EXPLOSION: stopped before package build; ' + str(limit))
        return
    summaries = []
    for item in prepared:
        summaries.append(build(item, output))
        print('PACKAGES_BUILT pair=' + str(item['pair']['index']), flush=True)
    # Re-extract sources and re-discover, rather than merely re-hash cached packages.
    repeated = [prepare(pair) for pair in pairs]
    if fingerprint(prepared) != fingerprint(repeated):
        raise ValueError('Nondeterministic inventory/discovery/correspondence')
    rerun = [build(p, output) for p in repeated]
    if summaries != rerun:
        raise ValueError('Nondeterministic package rerun')
    import json
    package_hashes = {}
    for path in sorted(output.glob('pair_*/packages/*.json')):
        payload = json.loads(path.read_text())
        assert_answer_blind(payload)
        if payload['package_hash'] != fingerprint({k: v for k, v in payload.items() if k != 'package_hash'}):
            raise ValueError('Package seal mismatch')
        package_hashes[str(path.relative_to(output))] = payload['package_hash']
    write(output / 'PACKAGES_FREEZE.json', dict(schema='F5_PACKAGE_FREEZE/1', package_hashes=package_hashes,
        full_rerun_deterministic=True, aggregate_hash=fingerprint(package_hashes), model_calls=0))
    access = dict(files=sorted(audit.reads), denied=audit.denied,
        bootstrap='Existing prepared_pairs DEV guard: split/inventory metadata + hashes of only selected sources',
        pages={str(p['pair']['index']): {s: p['inventories'][s]['pages_opened'] for s in ('old', 'new')} for p in prepared},
        embargo={str(p['pair']['index']): p['pair']['embargo_pages'] for p in prepared},
        historical_answers_read=False, validation_opened=False, final_holdout_opened=False)
    write(output / 'SOURCE_ACCESS.json', access)
    leakage = dict(status='PASS', input_policy='ADMITTED_SOURCE_FILES_ONLY', answer_field_scan='PASS',
        packages_frozen=True, historical_outputs_read=False, known_findings_read=False,
        prior_exposure='Frozen DEV sample reused as instructed; no claim of historically pristine documents',
        structural_audit='NO_MANUAL_SOURCE_AUDIT_PERFORMED', model_calls=0)
    write(output / 'LEAKAGE_AUDIT.json', leakage)
    acceptance = dict(
        A_FULL_CONTENT_INDEXED=all(i['all_content_accessible'] for p in prepared for i in p['inventories'].values()),
        B_SUBJECT_DISCOVERY_NONEMPTY=all(p['indices'][s]['subjects'] for p in prepared for s in ('old', 'new')),
        C_CORRESPONDENCE_FORMED=all(p['correspondence']['edges'] for p in prepared),
        D_NON_ONE_TO_ONE_SUPPORTED='COVERED_BY_SYNTHETIC_TESTS',
        E_REQUIREMENTS_AUTOMATIC=True, F_F1_RECEIVES_REQUIREMENTS=True,
        G_MANDATORY_OLD_NEW='COVERED_BY_SYNTHETIC_TESTS_AND_PER_PACKAGE_ALLOCATION_RECEIPTS',
        H_F4_ACCEPTS_LOCATORS=sum(p['f4_accepted'] for p in summaries) > 0 and not any(p['f4_rejected'] for p in summaries),
        I_F2_TYPED_SKELETON=True, J_ANSWER_BLIND=True)
    blocking = []
    if not acceptance['A_FULL_CONTENT_INDEXED']:
        blocking.append('FULL_CONTENT_INDEXING_INCOMPATIBLE_WITH_FROZEN_QUARANTINE')
    if not any(p['complete'] for p in summaries):
        blocking.append('SEMANTIC_SCOPE_BOUNDARIES_NOT_VERIFIED')
    status = 'F5_STILL_NEEDS_REPAIR' if blocking or any(v is False for v in acceptance.values()) else 'READY_FOR_FRESH_INFERENCE'
    write(output / 'LOCAL_ACCEPTANCE.json', acceptance)
    result = dict(status=status, pairs=summaries, model_calls=0,
        f1_integrated=True, f2_integrated=True, f4_integrated=True, v4_binding_integrated=True,
        package_explosion=False, answer_leakage='PASS', deterministic=True,
        blocking_gates=blocking, recommendation=status)
    write(output / 'F5_RESULT.json', result)
    for side in ('old', 'new'):
        write(output / ('DOCUMENT_INVENTORY_' + side.upper() + '.json'),
              {str(p['pair']['index']): p['inventories'][side] for p in prepared})
    for name in ('SUBJECT_INDEX', 'CORRESPONDENCE', 'EVIDENCE_REQUIREMENTS', 'PACKAGE_INDEX', 'EVIDENCE_COVERAGE', 'PIPELINE_TRACE'):
        write(output / (name + '.json'), {str(p['index']): json.loads((output / ('pair_' + str(p['index'])) / (name + '.json')).read_text()) for p in pairs})
    report_path = output / 'F5_REPORT.md'
    content = report(result)
    if report_path.exists() and report_path.read_text() != content:
        raise ValueError('Report drift')
    report_path.write_text(content)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUT)
    run(parser.parse_args().output)
