"""Adapt offline requirement packages to the existing semantic input contract.

No model invocation: this produces a reviewable packet for a future separately
authorized run. Repeated requirement references share one delivered source image.
"""
from copy import deepcopy
from pathlib import Path
import json

from .evidence import fingerprint


def semantic_packet(body, pair):
    if pair['partition'] != 'DEV':
        raise PermissionError('This implementation program is DEV only')
    if body['package_hash'] != fingerprint({k:v for k,v in body.items() if k != 'package_hash'}):
        raise ValueError('Requirement package drift')
    sources, aliases = {}, {}
    for e in body['evidence']:
        side = e['side'].lower()
        if e['document_version'] != pair[side]['document_version'] or e['document'] != pair[side]['document_code']:
            raise PermissionError('Cross-document source in single-pair delivery')
        key = (side, e['document_version'], e['page'], e['raster']['sha256'] if e['raster'] else None)
        evidence_id = 'source_' + fingerprint(key)[:24]
        aliases[e['evidence_id']] = evidence_id
        if key in sources:
            # Complete source pages are collected once; later budget omissions
            # can only contain a subset. Preserve the larger whole-section text.
            if len(e['text']) > len(sources[key]['quote']):
                sources[key]['quote'] = e['text']
            continue
        types = {r['evidence_type'] for r in body['requirements'] if r['side'] == e['side'] and r['page'] == e['page']}
        route = 'GRAPHIC' if 'GRAPHIC_REGION' in types else 'TABLE' if types & {'TABLE_COMPLETE','EQUIPMENT_SCHEDULE'} else 'TEXT'
        raster = {k:v for k,v in e['raster'].items() if k != 'page'} if e['raster'] else None
        sources[key] = dict(evidence_id=evidence_id, side=side, document_version=e['document_version'],
            source_receipt=e['provenance']['pdf'], page=e['page'], bbox=e['bbox'], quote=e['text'],
            source_kind='PDF_RASTER_CROP' if raster else 'PDF_NATIVE_TEXT', route=route,
            raster=raster, visual_audit_required=bool(raster), subject=e['subject'])
    evidence = {s:[e for e in sources.values() if e['side'] == s] for s in ['old','new']}
    if sum(bool(e['raster']) for e in sources.values()) > body['limits']['raster_budget']:
        raise ValueError('Actual image delivery exceeds package budget')
    coverage = deepcopy(body['evidence_coverage'])
    for row in coverage['requirements']:
        row['evidence_ids'] = sorted({aliases[i] for i in row['evidence_ids']})
    coverage['subjects'] = [dict(subject=subject, **{
        side.lower()+'_evidence': [r for r in coverage['requirements']
            if r['requirement']['subject']==subject and r['requirement']['side']==side]
        for side in ('OLD','NEW')}) for subject in sorted({r['subject'] for r in body['requirements']})]
    coverage['delivery_hash'] = fingerprint(evidence)
    coverage['source_requirement_package_hash'] = body['package_hash']
    packet = dict(pair_index=pair['index'], pair_key=pair['pair_key'], partition='DEV',
        source_versions={s:pair[s]['document_version'] for s in ['old','new']},
        proposal_kind='REQUIREMENT_SCOPED_SOURCE_AUDIT',
        proposal_query='; '.join(sorted({r['subject'] for r in body['requirements']})),
        proposal_locator={}, evidence=evidence, evidence_coverage=coverage,
        coverage_complete=coverage['complete'], proposal_is_not_truth=True,
        model_instruction=body['model_instruction'], source_package_hash=body['package_hash'])
    packet['packet_id'] = fingerprint(packet)[:24]
    return packet


def coverage_view(packet):
    receipt = packet.get('evidence_coverage')
    if not receipt:
        return dict(schema='EVIDENCE_COVERAGE/1', complete=False, requirements=[],
                    missing_reason='SUBJECT_REQUIREMENTS_NOT_DECLARED', absence_proven=False)
    # Keep requirements and omissions visible without repeating file paths/hashes
    # for the same PDF on every row in the model's text budget.
    return dict(schema=receipt['schema'], complete=receipt['complete'], absence_proven=False,
        requirements=[dict(requirement={k:v for k,v in r['requirement'].items() if k in
            {'requirement_id','subject','side','page','document','document_version','evidence_role',
             'evidence_type','expected_semantic_content'}}, completeness=r['completeness'],
            missing_reason=r['missing_reason'], evidence_ids=r['evidence_ids']) for r in receipt['requirements']],
        completeness_meaning=receipt['completeness_meaning'])


def write_semantic_packet(directory, packet):
    directory=Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for name, value in [('PACKET.json',packet),('EVIDENCE_COVERAGE.json',packet['evidence_coverage'])]:
        path=directory/name
        payload=json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False)+'\n'
        if path.exists() and path.read_text()!=payload:
            raise ValueError('Delivery drift; use a fresh output directory')
        if not path.exists():
            path.write_text(payload)
