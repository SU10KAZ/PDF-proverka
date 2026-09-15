"""PDF -> inventory -> subjects -> candidates -> F1/F4/F2/V4 -> frozen packages."""
from collections import Counter
from pathlib import Path

from experiments.project_change_contracts_v3_272.allocation import package
from .common import fingerprint, write
from .document_inventory import inventory
from .subject_discovery import discover
from .subject_correspondence import correspond
from .requirement_builder import build_requirements
from .contract_adapters import DiscoveryPageLoader, source_packet, graphic_bindings, typed_preparation


def prepare(pair):
    inventories = {s: inventory(pair[s], s.upper(), pair['embargo_pages'][s]) for s in ('old', 'new')}
    indices = {s: discover(inventories[s]) for s in ('old', 'new')}
    subjects = {s['subject_id']: s for index in indices.values() for s in index['subjects']}
    correspondence = correspond(indices['old']['subjects'], indices['new']['subjects'])
    return dict(pair=pair, inventories=inventories, indices=indices, subjects=subjects, correspondence=correspondence)


def portable(value, root):
    """Only generated paths are relativized; original source receipts stay pinned."""
    if isinstance(value, dict):
        return {k: portable(v, root) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [portable(v, root) for v in value]
    if isinstance(value, str) and value.startswith(str(root) + '/'):
        return value[len(str(root)) + 1:]
    return value


def build(prepared, root):
    root = Path(root).resolve()
    pair, invs = prepared['pair'], prepared['inventories']
    out = root / ('pair_' + str(pair['index']))
    for side in ('old', 'new'):
        write(out / ('DOCUMENT_INVENTORY_' + side.upper() + '.json'), invs[side])
    write(out / 'SUBJECT_INDEX.json', prepared['indices'])
    write(out / 'CORRESPONDENCE.json', prepared['correspondence'])
    packages, rows, all_coverage, trace = [], [], [], []
    for candidate in prepared['correspondence']['candidates']:
        reqs, regions, req_rows = build_requirements(candidate, prepared['subjects'], invs, pair)
        loader = DiscoveryPageLoader(pair, invs, regions, root / 'rasters')
        try:
            body = package(reqs, loader, profile='OTHER', text_budget=28000, raster_budget=8)
        finally:
            loader.close()
        body['scope_discovery'] = 'F5_DOCUMENT_DISCOVERY'
        body['package_hash'] = fingerprint({k: v for k, v in body.items() if k != 'package_hash'})
        packet = source_packet(body, pair, candidate)
        graphics = graphic_bindings(packet, candidate, root)
        typed = typed_preparation(packet, candidate)
        statuses = [r['completeness'] for r in body['evidence_coverage']['requirements']]
        completeness = ('COMPLETE' if statuses and all(s == 'COMPLETE' for s in statuses) else
                        'MISSING' if not statuses or all(s == 'MISSING' for s in statuses) else 'PARTIAL')
        result = dict(schema='MODEL_READY_PACKAGE/5', package_status='MODEL_READY_PACKAGE',
            candidate_subject=candidate, evidence_packet=packet, f1_requirement_package=body,
            completeness=completeness, typed_state_skeleton=typed, graphic_bindings=graphics,
            counter_evidence=body['counter_evidence'], inference_executed=False,
            readiness_meaning='Input serialization ready; incomplete context remains explicit, no semantic verdict',
            provenance=dict(pair_index=pair['index'], pair_key=pair['pair_key'], partition='DEV',
                document_versions={s: pair[s]['document_version'] for s in ('old', 'new')},
                subject_index_hash=fingerprint(prepared['indices']),
                correspondence_hash=fingerprint(prepared['correspondence'])))
        result = portable(result, root)
        result['hash_policy'] = 'Canonical hashes; generated raster paths relative to artifact root; original source receipts absolute'
        result['package_hash'] = fingerprint(result)
        filename = 'packages/' + candidate['candidate_id'] + '.json'
        write(out / filename, result)
        requested_routes = sorted({r['source_type'] for r in regions.values() if r})
        routes = sorted({r for side in ('old', 'new') for e in packet['evidence'][side]
                         for r in e['delivered_routes']})
        packages.append(dict(candidate_id=candidate['candidate_id'], path=filename,
            package_hash=result['package_hash'], completeness=completeness, routes=routes,
            requested_routes=requested_routes,
            cardinality=candidate['cardinality'], estimated_inference_calls=1,
            f4_accepted=sum(g['f4_status'] == 'ACCEPTED' for g in graphics),
            f4_rejected=sum(g['f4_status'] == 'REJECTED' for g in graphics)))
        rows.extend(req_rows)
        all_coverage.append(dict(candidate_id=candidate['candidate_id'], completeness=completeness,
                                coverage=portable(body['evidence_coverage'], root)))
        trace.append(dict(candidate_id=candidate['candidate_id'],
            stages=['DOCUMENT_INVENTORY', 'SUBJECT_DISCOVERY', 'CORRESPONDENCE', 'AUTOMATIC_REQUIREMENTS',
                    'F1_ALLOCATION_AND_COVERAGE_V3', 'F4_RASTER_LOCATOR', 'V4_EXPLICIT_BIND',
                    'F2_ENGINEERING_STATE_CONSTRUCTOR', 'MODEL_READY_PACKAGE'],
            requirement_count=len(reqs), allocated_by_side=body['raster_allocation']['delivered_by_side'],
            f2_states=len(typed['states']), v4_rejected=typed['binding_issues'],
            f4_accepted=sum(g['f4_status'] == 'ACCEPTED' for g in graphics)))
    write(out / 'EVIDENCE_REQUIREMENTS.json', rows)
    write(out / 'PACKAGE_INDEX.json', packages)
    write(out / 'EVIDENCE_COVERAGE.json', all_coverage)
    write(out / 'PIPELINE_TRACE.json', trace)
    confidence = Counter(c['confidence'] for c in prepared['correspondence']['candidates'])
    status = Counter(p['completeness'] for p in packages)
    summary = dict(pair_index=pair['index'], pair_key=pair['pair_key'],
        pages_catalogued={s: invs[s]['pages_catalogued'] for s in invs},
        pages_content_indexed={s: invs[s]['pages_content_indexed'] for s in invs},
        page_statuses={s: dict(Counter(p['status'] for p in invs[s]['pages'])) for s in invs},
        subjects_discovered={s: len(prepared['indices'][s]['subjects']) for s in invs},
        strong_correspondences=confidence['STRONG'], possible_correspondences=confidence['POSSIBLE'],
        unresolved_subjects=len(prepared['correspondence']['unresolved_subjects']),
        correspondence_candidates=len(packages), model_ready_packages=len(packages),
        complete=status['COMPLETE'], partial=status['PARTIAL'], missing=status['MISSING'],
        route_packages={r: sum(r in p['routes'] for p in packages) for r in ('TEXT', 'TABLE', 'GRAPHIC')},
        requested_route_packages={r: sum(r in p['requested_routes'] for p in packages) for r in ('TEXT', 'TABLE', 'GRAPHIC')},
        estimated_inference_calls=len(packages), package_index_hash=fingerprint(packages),
        f4_accepted=sum(p['f4_accepted'] for p in packages), f4_rejected=sum(p['f4_rejected'] for p in packages),
        unidentified_regions={s: len(prepared['indices'][s]['undiscovered_region_ids']) for s in invs})
    write(out / 'SUMMARY.json', summary)
    return summary
