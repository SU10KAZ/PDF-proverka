import unittest
from .evaluation import evaluate


class QualityDenominators(unittest.TestCase):
    def test_empty_acceptance_cannot_claim_precision(self):
        r=evaluate(dict(project_changes=[]),dict(changes=[],partition='DEV',coverage_inventory_complete=True),[],[])
        self.assertIsNone(r['metrics']['project_change_precision'])
        self.assertIn('accepted',r['failed_gates'])

    def test_unresolved_accepted_stays_in_denominator(self):
        pred=dict(project_changes=[dict(project_change_id='x',status='ACCEPTED_CANDIDATE',pair_index=5)])
        j=dict(project_change_id='x',verdict='UNRESOLVED',truth_ids=[],source_checks=['Unresolved source ownership'])
        j.update({k:False for k in ['false_addition','false_removal','duplicate','over_grouped','under_grouped','parameter_explosion','traceable']})
        r=evaluate(pred,dict(changes=[],partition='DEV'),[j],[])
        self.assertEqual(r['metrics']['project_change_precision'],0)
        self.assertEqual(r['metrics']['unresolved_accepted'],1)

    def test_missing_source_judgement_is_not_silently_excluded(self):
        pred=dict(project_changes=[dict(project_change_id='x',status='ACCEPTED_CANDIDATE',pair_index=5)])
        with self.assertRaises(ValueError):evaluate(pred,dict(changes=[]),[],[])
