"""Prepare precisely the 12 frozen DEV scopes, then replay V2 outputs offline."""
from dataclasses import replace
from pathlib import Path
import shutil
import sys

from .evidence import EvidenceRequirement, fingerprint
from experiments.project_change_contracts_272.packages import write_package
from .sources import TypedPageLoader
from .delivery import semantic_packet, coverage_view, write_semantic_packet
from experiments.project_change_contracts_272.witnesses import raster_locator_errors
from experiments.project_change_semantic_272.access import prepared_pairs
from experiments.project_change_semantic_272.history import document_history
from .common import CODE, ROOT, PREVIOUS, OUT, CASES, PROFILES, read, sha, ref, write, previous_hashes
from .allocation import package
from experiments.project_change_contracts_v2_272.negative import bounded_contract
from .normalization import normalize
from experiments.project_change_contracts_v2_272.sufficiency import PROFILES as PROFILE_CONTRACTS, VERSION as SUFFICIENCY_VERSION


def schema():
    output = read(PREVIOUS / 'OUTPUT_SCHEMA.json')
    output['properties']['materiality']['properties']['status']['enum'] = ['MATERIAL', 'NOT_MATERIAL', 'NOT_APPLICABLE', 'UNRESOLVED']
    s = {'type': 'string'}
    b = {'type': 'boolean'}
    strings = {'type': 'array', 'items': s}
    def obj(properties):
        return dict(type='object', properties=properties, required=list(properties), additionalProperties=False)
    def enum(*values):
        return dict(type='string', enum=list(values))
    for side in ('old', 'new'):
        state = output['properties'][side + '_state']
        state['properties'].update(subject_identity=obj(dict(functional_owner=s, system=s, subsystem=s, scope=s, evidence_ids=strings)),
            source_label={'type': ['string', 'null']}, functional_branch_identity={'type': ['string', 'null']},
            evidence_form=enum('DECLARATION', 'SELECTED_EQUIPMENT', 'TABLE_RESULT', 'GRAPHIC_STATE', 'NEGATIVE_STATE', 'UNKNOWN'))
        state['required'] = list(state['properties'])
    output['properties']['source_conflict'] = obj(dict(status=enum('NONE', 'PRESENT', 'UNKNOWN'),
        relevance=enum('RELEVANT', 'IRRELEVANT', 'UNKNOWN'), affected_claim_ids=strings, blocking=b,
        affected_subject=s, affected_state=s, explanation=s))
    output['properties']['old_absence'] = obj(dict(mode=enum('EXPLICIT_NEGATIVE', 'COMPLETE_BOUNDED_REPRESENTATION', 'NOT_FOUND', 'NOT_APPLICABLE'),
        inspected_evidence_ids=strings, scope_complete=b, completeness_basis=s,
        subject_matches=b, scope_matches=b, binding_reason=s, positive_evidence_ids=strings,
        literal_quote=s, negative_evidence_id=s, absence_verifiable=b, representation_basis=s))
    output['properties']['exists_change'] = obj(dict(status=enum('YES', 'NO', 'UNKNOWN'), evidence_ids=strings, explanation=s))
    output['required'] = list(output['properties'])
    return output


def prepare():
    if (OUT / 'ARCHITECTURE_FIX_FREEZE.json').exists():
        raise ValueError('V3 already frozen')
    OUT.mkdir(parents=True, exist_ok=True)
    baseline = OUT / 'PREVIOUS_RUN_HASHES.json'
    write(baseline, previous_hashes())
    previous = read(PREVIOUS / 'PACKAGE_INDEX.json')['packages']
    assert [(r['pair_index'], r['case_id']) for r in previous] == CASES
    pairs = {p['index']: p for p in prepared_pairs('DEV', indices={5, 7}, source_repo=CODE)}
    documents, excluded = {}, {}
    for pair in pairs.values():
        for side in ('old', 'new'):
            doc = pair[side]
            key = doc['document_code'], doc['document_version'], side.upper()
            documents[key] = doc
            excluded[key] = set(pair['embargo_pages'][side]) | set(document_history(doc, pair['embargo_pages'][side]))
    write(OUT / 'SOURCE_ACCESS.json', dict(partition='DEV', pair_indices=[5, 7], split=ref(ROOT / 'SPLIT.json'),
        documents=[dict(document=k[0], document_version=k[1], side=k[2], pdf=d['artifacts']['pdf'], excluded_pages=sorted(excluded[k])) for k, d in documents.items()]))
    loader = TypedPageLoader(documents, excluded, OUT / 'rasters')
    rows, replay = [], []
    try:
        for old_row in previous:
            case, key, token = old_row['case_id'], old_row['key'], old_row['case_token']
            old_body, old_packet = read(old_row['package']['path']), read(old_row['semantic_packet']['path'])
            assert sha(old_row['package']['path']) == old_row['package']['sha256']
            profile = PROFILES[case]
            requirements = [EvidenceRequirement(**(r | {'required_parts': tuple(r['required_parts'])})) for r in old_body['requirements']]
            body = package(requirements, loader, profile=profile)
            target = OUT / 'packages' / key
            write_package(target, body)
            packet = semantic_packet(body, pairs[old_row['pair_index']])
            write_semantic_packet(target / 'semantic', packet)
            contract = bounded_contract(packet)
            write(target / 'BOUNDED_OLD_SCOPE.json', contract)
            # Reuse only the previous frozen neutral query, never state seeds,
            # verdicts, analyst narratives, expected values or truth annotations.
            previous_input = read(old_row['model_input']['path'])
            coverage = coverage_view(packet)
            for view, full in zip(coverage['requirements'], packet['evidence_coverage']['requirements']):
                view['delivery'] = {k:v for k,v in full['delivery'].items() if k in
                    {'mandatory', 'purpose', 'required_type', 'selected_evidence_type', 'type_match', 'delivered', 'omission_reason'}}
                # One document/version/subject per side is already identified at
                # the packet root. Avoid repeating it on every requirement.
                view['requirement'] = {k: v for k, v in view['requirement'].items() if k in
                    {'requirement_id', 'side', 'page', 'evidence_role', 'evidence_type', 'required_type'}}
            data = dict(normalization_contract=dict(schema='F2_MODEL_NORMALIZATION/3',
                admission_order=['NORMALIZATION', 'CLAIM_TYPE', 'APPLICABILITY', 'COMPARABILITY', 'COUNTER_EVIDENCE', 'EXISTS_CHANGE', 'MATERIALITY', 'FINAL_ADMISSION']), case_token=token, claim_query=previous_input['claim_query'],
                engineering_subject=previous_input['engineering_subject'],
                authoritative_object=previous_input['authoritative_object'],
                comparison_direction=previous_input['comparison_direction'],
                source_identity=previous_input['source_identity'],
                evidence_sufficiency_profile=dict(schema=SUFFICIENCY_VERSION, name=profile, **PROFILE_CONTRACTS[profile]),
                bounded_old_scope=contract, evidence_coverage=coverage,
                raster_delivery={k: v for k, v in body['raster_allocation'].items() if k != 'requirements'},
                evidence={side: [{k: e[k] for k in ('evidence_id', 'side', 'document_version', 'source_kind', 'route', 'page', 'bbox', 'quote')}
                     | dict(raster_attached=bool(e.get('raster'))) for e in packet['evidence'][side]] for side in ('old', 'new')})
            write(target / 'MODEL_INPUT.json', data)
            locators = []
            for side in ('old', 'new'):
                for e in packet['evidence'][side]:
                    if e.get('raster'):
                        w = dict(evidence_id=e['evidence_id'], visual_locator='Полная страница', bbox_norm=[0, 0, 1, 1], binding_reason='Delivery check only')
                        errors = raster_locator_errors(w, e)
                        assert not errors, errors
                        locators.append(dict(evidence_id=e['evidence_id'], side=side, page=e['page'], raster=ref(e['raster']['path'])))
            write(target / 'RASTER_LOCATORS.json', locators)
            row = dict(case_id=case, pair_index=old_row['pair_index'], key=key, case_token=token, profile=profile,
                package=ref(target / 'PACKAGE.json'), package_hash=body['package_hash'],
                semantic_packet=ref(target / 'semantic/PACKET.json'), model_input=ref(target / 'MODEL_INPUT.json'),
                bounded_old_scope=ref(target / 'BOUNDED_OLD_SCOPE.json'), locators=ref(target / 'RASTER_LOCATORS.json'),
                coverage=ref(target / 'EVIDENCE_COVERAGE.json'), images=locators,
                completeness=body['evidence_coverage']['status'], raster_allocation=body['raster_allocation'])
            rows.append(row)
            success = read(PREVIOUS / 'calls' / token / 'SUCCESS.json')
            assert sha(success['normalized_path']) == success['normalized_sha256']
            raw = read(success['normalized_path'])
            normalized = normalize(raw, old_packet, profile)
            write(OUT / 'offline_replay' / (key + '.json'), normalized)
            replay.append(dict(key=key, previous_response=ref(success['normalized_path']),
                replay=ref(OUT / 'offline_replay' / (key + '.json')),
                case_id=case, raw_verdict=raw['verdict'], effective_verdict=normalized['effective_verdict'],
                exists_change=normalized['exists_change'], status=normalized['status'], issues=normalized['issues'],
                comparison=normalized.get('f2'), note='V2 raw response and its original delivered packet; new normalizer only, no inference'))
            print(key, row['completeness'], body['raster_allocation']['delivered_by_side'], flush=True)
    finally:
        loader.close()
    by_case = {r['case_id']: r for r in rows}
    from .gates import package_gates, replay_gates
    a2 = package_gates(rows)
    d2 = replay_gates(replay, OUT)
    gates = dict(A2=a2['status'] == 'PASS', D2=d2['status'] == 'PASS',
        frozen_cases=[(r['pair_index'], r['case_id']) for r in rows] == CASES,
        previous_runs_unchanged=read(baseline) == previous_hashes())
    write(OUT / 'A2_PACKAGE_REGRESSION.json', a2)
    write(OUT / 'D2_MECHANICAL_REPLAY.json', d2)
    write(OUT / 'LOCAL_REGRESSION.json', dict(status='PASS' if all(gates.values()) else 'FAIL', gates=gates,
        cases=replay, model_calls=0, selection='KNOWN_DEV_REGRESSION_NOT_BLIND'))
    write(OUT / 'PACKAGE_INDEX.json', dict(schema='PACKAGE_INDEX/3', packages=rows))
    write(OUT / 'EVIDENCE_COVERAGE.json', {r['key']: read(r['coverage']['path']) for r in rows})
    write(OUT / 'RASTER_ALLOCATION.json', {r['key']: r['raster_allocation'] for r in rows})
    write(OUT / 'OPENED_PAGES.json', dict(pages=loader.opened, partition='DEV', pair_indices=[5, 7]))
    write(OUT / 'OUTPUT_SCHEMA.json', schema())
    write(OUT / 'PROMPT.txt', (Path(__file__).with_name('prompt.txt')).read_text())
    write(OUT / 'TRUTH_REFERENCES_SEALED.json', read(PREVIOUS / 'TRUTH_REFERENCES_SEALED.json'))
    if not all(gates.values()):
        raise RuntimeError('STOP: local deterministic regression failed')
    print('LOCAL REGRESSION PASS; model calls 0; truth sealed', flush=True)


if __name__ == '__main__':
    from pathlib import Path
    prepare()
