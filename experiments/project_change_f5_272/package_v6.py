"""V6 transport through unchanged V4 boundary, F1, F4 and F2 adapters."""
from .common import fingerprint
from .repair_v4 import finish_f1, STAGES

def one_package(prepared, docs, canonical, guards, original, output):
    from .subject_v4 import refine_candidate
    from .requirements_v6 import build
    from .allocation_v6 import package as deliver
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
    result = dict(schema='MODEL_READY_PACKAGE/5.6', package_status='MODEL_READY_PACKAGE',
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
