from copy import deepcopy

import pytest

from backend.app.pipeline.stages.findings_merge.publication_reconciliation import (
    InvalidPublication, build_packet, fingerprint, reconcile_publication,
)
from backend.app.pipeline.stages.findings_merge.recovery_addenda import render_description


def example():
    original = 'Quantity conflict. Graphic not checked.'
    addenda = [{'claim': 'Range conflict confirmed.', 'reason': 'Graphic and schedule differ.'}]
    finding = {'id': 'F-006', 'problem': 'Pressure switch',
               'description': render_description(original, addenda),
               'recovery_original_description': original, 'recovery_addenda': addenda}
    packet = build_packet(finding, {'t': 'Schedule range 50–500; quantity 3.'}, ['g'])
    part = {'id': 'current', 'statement': 'Расхождение количества и диапазона.',
            'outcome': 'supported', 'reason': 'Сверены перечень и схема.',
            'input_spans': [{'unit_id': u['id'], 'start': 0, 'end': len(u['text']), 'quote': u['text']}
                            for u in packet['input_units']],
            'evidence': [{'block_id': 't', 'type': 'text', 'quote': 'range 50–500'},
                         {'block_id': 'g', 'type': 'graphic', 'observation': 'Диапазон 20–200.'}],
            'required_context': []}
    return finding, packet, {'packet_sha256': fingerprint(packet), 'parts': [part]}


def test_current_publication_does_not_repeat_stale_caveat_or_destroy_history():
    finding, packet, response = example()
    before = deepcopy(finding)
    result = reconcile_publication(finding, packet, response)
    assert finding == before == result['archive']
    assert 'not checked' not in result['technical_findings'][0]['statement']
    assert not result['semantic_verified'] and not result['candidate_coverage_updated']
    result['archive']['description'] = 'Changed copy'
    assert finding == before


def test_mixed_claim_preserves_supported_part_and_routes_unresolved_part_separately():
    finding = {'id': 'F-060', 'problem': 'Legend', 'description': 'Label mismatch. Symbol equivalence unknown.'}
    packet = build_packet(finding, {'t': 'Legend label is TS.'})
    split = len('Label mismatch.')
    response = {'packet_sha256': fingerprint(packet), 'parts': [
        {'id': 'label', 'statement': 'Вопрос подписи легенды.', 'outcome': 'question', 'reason': 'Область применения неясна.',
         'input_spans': [{'unit_id': 'problem', 'start': 0, 'end': 6, 'quote': 'Legend'},
                         {'unit_id': 'description', 'start': 0, 'end': split, 'quote': finding['description'][:split]}],
         'evidence': [{'block_id': 't', 'type': 'text', 'quote': 'label is TS'}], 'required_context': []},
        {'id': 'symbol', 'statement': 'Эквивалентность символов не установлена.', 'outcome': 'needs_data',
         'reason': 'Изображение не передано.',
         'input_spans': [{'unit_id': 'description', 'start': split, 'end': len(finding['description']),
                          'quote': finding['description'][split:]}],
         'evidence': [], 'required_context': ['Изображение условного обозначения.']}]}
    result = reconcile_publication(finding, packet, response)
    assert len(result['questions']) == len(result['pending']) == 1
    assert result['technical_findings'] == result['withdrawn'] == []
    response['parts'][1]['outcome'] = 'refuted'
    with pytest.raises(InvalidPublication, match='missing_evidence'):
        reconcile_publication(finding, packet, response)


@pytest.mark.parametrize('mutation,reason', [
    (lambda r: r['parts'][0]['input_spans'].pop(), 'omitted_or_overlapping'),
    (lambda r: r['parts'][0]['input_spans'].append(deepcopy(r['parts'][0]['input_spans'][0])), 'omitted_or_overlapping'),
    (lambda r: r['parts'][0]['input_spans'][0].update(quote='invented'), 'invalid_input_span'),
    (lambda r: r['parts'][0]['evidence'][0].update(quote='range 100–900'), 'text_quote_not_in'),
    (lambda r: r['parts'][0]['evidence'][1].update(block_id='unattached'), 'graphic_not_supplied'),
    (lambda r: r['parts'].append(deepcopy(r['parts'][0])), 'duplicate_part_id'),
    (lambda r: r.update(packet_sha256='other packet'), 'response_packet_mismatch'),
])
def test_rejects_lost_claims_invented_sources_and_wrong_request(mutation, reason):
    finding, packet, response = example()
    mutation(response)
    with pytest.raises(InvalidPublication, match=reason):
        reconcile_publication(finding, packet, response)


def test_stale_packet_and_tampered_input_cannot_apply():
    finding, packet, response = example()
    modified = deepcopy(finding)
    modified['report_section'] = 'questions'
    with pytest.raises(InvalidPublication, match='finding_changed'):
        reconcile_publication(modified, packet, response)
    packet['input_units'].pop()
    response['packet_sha256'] = fingerprint(packet)
    with pytest.raises(InvalidPublication, match='packet_input_changed'):
        reconcile_publication(finding, packet, response)


def test_modified_description_cannot_be_silently_ignored():
    finding, _, _ = example()
    finding['description'] = 'New untracked conclusion'
    with pytest.raises(InvalidPublication, match='current_description_not_bound'):
        build_packet(finding, {'t': 'Text'})


def test_withdrawal_retains_explicit_reason_and_provenance_without_claiming_coverage():
    finding, packet, response = example()
    response['parts'][0].update(outcome='refuted', statement='Тезис опровергнут.', reason='Прямая ссылка присутствует.')
    result = reconcile_publication(finding, packet, response)
    assert len(result['withdrawn']) == 1
    assert not result['technical_findings']
    assert result['archive'] == finding
    assert not result['candidate_coverage_updated']


def test_misplaced_question_cannot_escape_complete_accounting():
    finding, packet, response = example()
    response['questions'] = [response['parts'].pop()]
    with pytest.raises(InvalidPublication, match='unexpected_response_fields'):
        reconcile_publication(finding, packet, response)


def test_superseded_limitation_is_history_not_engineer_question_or_false_defect():
    finding, packet, response = example()
    response['parts'][0]['outcome'] = 'superseded'
    result = reconcile_publication(finding, packet, response)
    assert len(result['history_only']) == 1
    assert result['questions'] == result['withdrawn'] == []
    assert result['part_counts_not_finding_counts']
    response['parts'][0]['evidence'] = []
    with pytest.raises(InvalidPublication, match='missing_evidence'):
        reconcile_publication(finding, packet, response)


def test_full_contract_includes_risk_solution_and_norms_without_silent_inheritance():
    finding, _, _ = example()
    finding.update(risk='Possible wrong equipment.', solution='Reconcile source documents.',
                   norm='Unverified clause', norm_quote='Reported quote',
                   candidate_norm_references=[{'document': 'Reported standard'}])
    packet = build_packet(finding, {'t': 'Source'})
    assert packet['contract_version'] == 2
    assert {u['id'] for u in packet['input_units']} >= {'risk', 'solution', 'norm', 'norm_quote', 'candidate_norm_references'}
    # Legacy pilots remain evaluable, but cannot masquerade as a full-field review.
    legacy = build_packet(finding, {'t': 'Source'}, contract_version=1)
    response = {'packet_sha256': fingerprint(legacy), 'parts': [{
        'id': 'q', 'statement': 'Needs clarification.', 'reason': 'Source limits.', 'outcome': 'question',
        'input_spans': [{'unit_id': u['id'], 'start': 0, 'end': len(u['text']), 'quote': u['text']}
                        for u in legacy['input_units']], 'evidence': [], 'required_context': []}]}
    result = reconcile_publication(finding, legacy, response)
    assert result['review_scope'] == 'description_only_legacy'
    assert result['unreviewed_claim_fields'] == ['solution', 'risk', 'norm', 'norm_quote', 'candidate_norm_references']
    # A proposal that accounts only for the description cannot pass the full contract.
    response['packet_sha256'] = fingerprint(packet)
    with pytest.raises(InvalidPublication, match='omitted_or_overlapping'):
        reconcile_publication(finding, packet, response)
