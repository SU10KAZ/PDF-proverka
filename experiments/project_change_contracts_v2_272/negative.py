"""A negative finding is valid only inside a fully inspected bounded OLD scope."""
from experiments.project_change_contracts_272.evidence import fingerprint

VERSION = 'BOUNDED_NEGATIVE_STATE/2'


def bounded_contract(packet):
    rows = [r for r in packet['evidence_coverage']['requirements'] if r['requirement']['side'] == 'OLD']
    return dict(schema=VERSION, subject=packet['proposal_query'],
        scope='Only the OLD subject regions on the explicitly enumerated admitted source pages',
        required_requirements=[r['requirement']['requirement_id'] for r in rows],
        required_evidence_ids=sorted({i for r in rows for i in r['evidence_ids']}),
        pages=sorted({r['requirement']['page'] for r in rows}),
        delivery_complete=bool(rows) and all(r['completeness'] == 'COMPLETE' for r in rows),
        completeness_contract='All listed OLD regions, their notes, legend and subject connections must be inspected. '
            'Explain why this representation is complete for the claimed engineering scope. '
            'Delivery alone is not semantic completeness or absence. Never extend absence to the entire project.',
        source_package_hash=packet['source_package_hash'])


def certify(contract, finding, evidence):
    """Validate source-bound model/auditor observations; never mine a no-hit into truth."""
    errors = []
    by_id = {e['evidence_id']: e for e in evidence if e['side'].upper() == 'OLD'}
    required = set(contract['required_evidence_ids'])
    inspected = set(finding.get('inspected_evidence_ids', []))
    if not contract.get('subject') or not contract.get('scope') or not contract.get('source_package_hash'):
        errors.append('BOUNDED_SCOPE_PROVENANCE_REQUIRED')
    if not contract.get('delivery_complete') or not required or not required <= inspected or not required <= by_id.keys():
        errors.append('REQUIRED_OLD_EVIDENCE_NOT_INSPECTED')
    if not finding.get('scope_complete') or not finding.get('completeness_basis'):
        errors.append('SCOPE_COMPLETENESS_NOT_ESTABLISHED')
    if finding.get('subject_matches') is not True or finding.get('scope_matches') is not True:
        errors.append('NEGATIVE_SUBJECT_OR_SCOPE_NOT_MATCHED')
    if not finding.get('binding_reason'):
        errors.append('NEGATIVE_BINDING_NOT_EXPLAINED')
    positives = set(finding.get('positive_evidence_ids', []))
    if positives:
        if positives <= by_id.keys():
            return dict(schema=VERSION, status='OLD_SOURCE_CONFLICT', absence_proven=False,
                        reasons=['SAME_VERSION_OLD_POSITIVE_EVIDENCE'], provenance=finding,
                        scope=contract['scope'], subject=contract['subject'])
        errors.append('POSITIVE_EVIDENCE_WRONG_VERSION_OR_UNDELIVERED')
    mode = finding.get('mode')
    if mode == 'EXPLICIT_NEGATIVE':
        quote, eid = finding.get('literal_quote', ''), finding.get('negative_evidence_id', '')
        text = by_id.get(eid, {}).get('quote', '')
        clean = lambda s: ' '.join(s.casefold().split())
        if not quote or clean(quote) not in clean(text) or not any(
                token in clean(quote) for token in ('не предусматрива', 'отсутств', 'не требу', 'not provided', 'absent')):
            errors.append('EXPLICIT_NEGATIVE_LITERAL_NOT_GROUNDED')
    elif mode == 'COMPLETE_BOUNDED_REPRESENTATION':
        if finding.get('absence_verifiable') is not True or not finding.get('representation_basis'):
            errors.append('ABSENCE_NOT_VERIFIABLE_IN_REPRESENTATION')
    else:
        errors.append('NO_HIT_IS_NOT_ABSENCE')
    return dict(schema=VERSION, status='NOT_FOUND' if errors else 'PROVEN_ABSENT_IN_BOUNDED_SCOPE',
        absence_proven=not errors, reasons=errors, scope=contract['scope'], subject=contract['subject'],
        provenance=dict(contract_hash=fingerprint(contract), observation=finding,
            evidence=[dict(evidence_id=i, document_version=by_id[i]['document_version'],
                page=by_id[i]['page'], source_receipt=by_id[i].get('source_receipt')) for i in sorted(required & by_id.keys())]),
        entire_project_absence=False)
