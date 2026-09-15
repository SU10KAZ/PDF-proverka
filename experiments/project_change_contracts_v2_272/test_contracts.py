import copy
from dataclasses import replace
import unittest
from unittest.mock import patch

from experiments.project_change_contracts_272.evidence import EvidenceRequirement
from experiments.project_change_contracts_272.states import EngineeringState, StateTransition
from experiments.project_change_contracts_272.comparability import materiality
from .allocation import package
from .negative import certify
from .normalization import conflict, phase, compare, normalize
from .sufficiency import evaluate


def requirement(name, side, page, kind='GRAPHIC_REGION', role='STATE'):
    return EvidenceRequirement(name, 'subject', side, role, 'doc', side + '_version', page,
                               'subject', kind, {'scope_map': 'fixture'}, scope_binding='bounded')


def loader(r):
    return dict(native='subject declaration', full_page=True, scope_binding=r.scope_binding,
                raster={'sha256': str(r.page)}, provenance={'pdf': {'sha256': 'fixture'}}, bbox=[0, 0, 100, 100])


def transition(role='TOPOLOGY', old='2 pipes', new='4 pipes', **kwargs):
    state = EngineeringState('subject', 'scope', role, old, stage_phase=None, functional_role='function',
        members=('one functional subject',), evidence_ids=('old_evidence',), provenance={'source': 'old'},
        local_or_global='LOCAL', component_or_total='COMPONENT', physical_quantity='heat', unit='W/m²',
        branch_identity='functional', **kwargs)
    return StateTransition(state, replace(state, value=new, evidence_ids=('new_evidence',)),
                           'source identity', 'source mapping', state_support='SOURCE_SUPPORTED')


class AllocationTests(unittest.TestCase):
    def test_old_and_new_mandatory_reserved(self):
        body = package([requirement('o', 'OLD', 1), requirement('n', 'NEW', 2)], loader, raster_budget=2)
        self.assertEqual(body['raster_allocation']['delivered_by_side'], {'OLD': 1, 'NEW': 1})
        self.assertTrue(body['coverage_complete'])

    def test_supporting_old_never_displaces_new_primary(self):
        reqs = [requirement('o' + str(i), 'OLD', i, 'COUNTER_EVIDENCE', 'COUNTER') for i in range(1, 9)]
        reqs += [requirement('primary_old', 'OLD', 9), requirement('primary_new', 'NEW', 10)]
        body = package(reqs, loader, raster_budget=2)
        selected = {r['requirement_id'] for r in body['raster_allocation']['requirements'] if r['delivered']}
        self.assertEqual(selected, {'primary_old', 'primary_new'})

    def test_budget_exhaustion_receipted(self):
        body = package([requirement('o', 'OLD', 1), requirement('n', 'NEW', 2)], loader, raster_budget=1)
        self.assertFalse(body['coverage_complete'])
        self.assertEqual(body['evidence_coverage']['status'], 'PARTIAL_BUDGET_LIMIT')
        omitted = [r for r in body['raster_allocation']['requirements'] if r['omitted']]
        self.assertTrue(omitted[0]['available'])
        self.assertEqual(omitted[0]['omission_reason'], 'BUDGET_LIMIT')

    def test_duplicate_requirement_page_uses_one_slot(self):
        body = package([requirement('o', 'OLD', 1), requirement('counter', 'OLD', 1, 'COUNTER_EVIDENCE', 'COUNTER'),
                        requirement('n', 'NEW', 2)], loader, raster_budget=2)
        self.assertTrue(body['coverage_complete'])
        self.assertEqual(body['raster_allocation']['used'], 2)


def absence_fixture():
    evidence = [dict(evidence_id='old', side='old', document_version='v1', page=1,
                     quote='Система не предусматривается.', source_receipt={'sha256': 'source'})]
    contract = dict(subject='система', scope='bounded', source_package_hash='packet',
                    required_evidence_ids=['old'], delivery_complete=True)
    finding = dict(mode='EXPLICIT_NEGATIVE', inspected_evidence_ids=['old'], scope_complete=True,
        completeness_basis='complete system description and drawing', subject_matches=True, scope_matches=True,
        binding_reason='system inside bounded description', positive_evidence_ids=[],
        literal_quote='Система не предусматривается.', negative_evidence_id='old')
    return contract, finding, evidence


class NegativeTests(unittest.TestCase):
    def test_explicit_negative_complete_scope(self):
        result = certify(*absence_fixture())
        self.assertEqual(result['status'], 'PROVEN_ABSENT_IN_BOUNDED_SCOPE')
        self.assertFalse(result['entire_project_absence'])

    def test_no_hit_is_not_absence(self):
        contract, finding, evidence = absence_fixture()
        finding['mode'] = 'NOT_FOUND'
        self.assertEqual(certify(contract, finding, evidence)['status'], 'NOT_FOUND')

    def test_negative_and_positive_conflict(self):
        contract, finding, evidence = absence_fixture()
        finding['positive_evidence_ids'] = ['old']
        self.assertEqual(certify(contract, finding, evidence)['status'], 'OLD_SOURCE_CONFLICT')

    def test_uninspected_or_foreign_old_cannot_certify(self):
        contract, finding, evidence = absence_fixture()
        for bad in ('inspection', 'scope', 'quote', 'side'):
            c, f, e = copy.deepcopy((contract, finding, evidence))
            if bad == 'inspection': f['inspected_evidence_ids'] = []
            if bad == 'scope': f['scope_matches'] = False
            if bad == 'quote': f['literal_quote'] = 'invented absent'
            if bad == 'side': e[0]['side'] = 'new'
            self.assertFalse(certify(c, f, e)['absence_proven'], bad)


class SufficiencyTests(unittest.TestCase):
    def test_topology_declarations_without_graphics(self):
        t = transition()
        result = evaluate('TOPOLOGY_DECLARATION', t, evidence_forms={'old': 'DECLARATION', 'new': 'DECLARATION'}, comparison=compare(t))
        self.assertTrue(result['sufficient'])
        self.assertFalse(result['graphics_complete_required'])

    def test_criterion_material_without_equipment(self):
        t = transition('INPUT_CRITERION', '60', '100')
        self.assertEqual(materiality(t, compare(t))['status'], 'MATERIAL')

    def test_aggregate_missing_basis_or_composition_is_review(self):
        t = transition('CALCULATED_RESULT', '800', '2585')
        result = compare(t)
        self.assertIn('consumer_composition', result['unknown'])
        self.assertIn('calculation_basis', result['unknown'])
        self.assertEqual(materiality(t, result)['status'], 'REVIEW')

    def test_novelty_requires_certificate(self):
        t = transition()
        self.assertFalse(evaluate('NOVEL_SYSTEM', t, evidence_forms={})['sufficient'])
        self.assertTrue(evaluate('NOVEL_SYSTEM', t, evidence_forms={}, negative=certify(*absence_fixture()))['sufficient'])


def raw_fixture():
    t = transition('SELECTED_EQUIPMENT', 'A', 'B')
    raw = dict(case_token='opaque', verdict='ACCEPT', identity_basis='both sources', mapping_basis='one function',
        source_conflict=dict(status='NONE', relevance='IRRELEVANT', affected_claims=[], explanation='no conflict'),
        old_state=t.old.to_dict(), new_state=t.new.to_dict(), witnesses=[])
    packet = dict(proposal_query='subject', source_package_hash='packet', evidence_coverage={'requirements': []}, evidence={})
    for side, label in [('old', 'П1ук'), ('new', 'П1.2д')]:
        s = raw[side + '_state']
        s.update(stage_phase=side.upper(), support='PROVEN', source_label=label,
            evidence_form='SELECTED_EQUIPMENT', branch_identity=label, functional_branch_identity='supply to dispatch room',
            subject_identity=dict(functional_owner='dispatch room', system='ventilation', subsystem='supply',
                                  scope='same room', evidence_ids=list(s['evidence_ids'])))
        packet['evidence'][side] = [dict(evidence_id=s['evidence_ids'][0], side=side, page=1,
                                    document_version=side, quote='Selected equipment', source_receipt={'sha256': side})]
        raw['witnesses'].append(dict(evidence_id=s['evidence_ids'][0], side=side, kind='TEXT_LITERAL', literal_quote='Selected equipment'))
    return raw, packet


class NormalizationTests(unittest.TestCase):
    def test_old_new_removed_from_phase(self):
        self.assertIsNone(phase('OLD stage_1; logical v002'))
        self.assertIsNone(phase('NEW stage_2 по направлению сравнения пакета'))
        self.assertEqual(phase('NEW stage_2; проектная'), 'проектная')

    def test_free_text_not_boolean(self):
        self.assertEqual(conflict('No conflicts found', 's', 'TOPOLOGY', 'k')['status'], 'UNKNOWN')
        self.assertFalse(conflict(dict(status='NONE', relevance='UNKNOWN', explanation='long explanation'), 's', 'TOPOLOGY', 'k')['blocking'])

    def test_conflict_only_blocks_related_claim(self):
        raw = dict(status='PRESENT', relevance='RELEVANT', affected_claims=['location'], explanation='location disagreement')
        self.assertTrue(conflict(raw, 's', 'ROUTING', 'location')['blocking'])
        self.assertFalse(conflict(raw, 's', 'SELECTED_EQUIPMENT', 'equipment')['blocking'])
        raw.update(relevance='IRRELEVANT')
        self.assertTrue(conflict(raw, 's', 'ROUTING', 'location')['blocking'])

    def test_marks_differ_same_functional_subject(self):
        raw, packet = raw_fixture()
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['effective_verdict'], 'ACCEPT', result)
        self.assertEqual(result['old_state']['engineering_subject'], result['new_state']['engineering_subject'])
        self.assertNotEqual(result['old_state']['provenance']['source_label'], result['new_state']['provenance']['source_label'])

    def test_cardinality_derived_from_members(self):
        raw, packet = raw_fixture()
        raw['new_state']['members'] = ['one', 'two']
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['old_state']['comparison_cardinality'], '1→N')

    def test_ambiguous_mapping_review(self):
        raw, packet = raw_fixture()
        raw['new_state']['subject_identity']['scope'] = 'UNKNOWN'
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['effective_verdict'], 'REVIEW')
        self.assertIn('new_AMBIGUOUS_SUBJECT_MAPPING', result['issues'])

    def test_unreferenced_identity_cannot_be_invented(self):
        raw, packet = raw_fixture()
        raw['new_state']['subject_identity']['evidence_ids'] = ['foreign']
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['effective_verdict'], 'REVIEW')
        self.assertIn('new_SUBJECT_MAPPING_NOT_GROUNDED', result['issues'])

    def test_invented_quote_cannot_support_accept(self):
        raw, packet = raw_fixture()
        raw['witnesses'][0]['literal_quote'] = 'not in source'
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['effective_verdict'], 'REVIEW')
        self.assertIn('UNGROUNDED_LITERAL_WITNESS', result['issues'])

    def test_aggregate_profile_cannot_be_bypassed_with_equipment_role(self):
        raw, packet = raw_fixture()
        result = normalize(raw, packet, 'AGGREGATE_CALCULATED_RESULT')
        self.assertEqual(result['effective_verdict'], 'REVIEW')
        self.assertIn('STATE_ROLE_DOES_NOT_MATCH_CLAIM_PROFILE', result['sufficiency']['reasons'])


class ExperimentGatesTests(unittest.TestCase):
    def test_truth_is_not_read_with_eleven_successes(self):
        from . import report
        with patch.object(report, 'verify', return_value=({}, [])), patch.object(report, 'collect',
                return_value={'completed_count': 11, 'model_calls': 12}), patch.object(report, 'read') as read:
            with self.assertRaises(PermissionError):
                report.report()
            read.assert_not_called()

    def test_truth_is_not_read_after_extra_call(self):
        from . import report
        with patch.object(report, 'verify', return_value=({}, [])), patch.object(report, 'collect',
                return_value={'completed_count': 12, 'model_calls': 13}), patch.object(report, 'read') as read:
            with self.assertRaises(PermissionError):
                report.report()
            read.assert_not_called()


if __name__ == '__main__':
    unittest.main()
