import copy
import unittest
from question_annex import build, render


class QuestionAnnexTests(unittest.TestCase):
    def question(self):
        return {'finding': 'Объект <node> требует расчётного подтверждения.',
                'claim_type': 'context_gap', 'affected_entity': '<node>',
                'evidence_quote': 'Наблюдаемая связь', 'evidence_kind': 'block_image',
                'context_status': 'needs_retrieval', 'confidence': 0.95,
                'counterevidence_checked': True, 'required_context': ['Расчёт узла'],
                'problem_class': 'load_path'}

    def test_preserves_separate_observations_without_promotion_or_mutation(self):
        a = self.question(); b = dict(a, finding='Иная оговорка об этом же объекте.')
        candidates = [a, b]; before = copy.deepcopy(candidates)
        result = build(candidates, {'block_id': 'a'})
        self.assertEqual(candidates, before)
        self.assertEqual([r['original_candidate'] for r in result['rows']], before)
        self.assertEqual(len(result['rows']), 2)
        self.assertTrue(all(r['route'] == 'question_pending_review' for r in result['rows']))
        self.assertTrue(all(not r['published_as_defect'] for r in result['rows']))
        self.assertEqual(result['published_source_indices'], [])
        self.assertIn('&lt;node&gt;', render(result))
        self.assertNotIn('<node>', render(result))

    def test_low_evidence_or_unspecified_request_stays_in_other_review(self):
        for patch in [{'confidence': 0.2}, {'required_context': []}, {'evidence_kind': 'none'}]:
            with self.subTest(patch=patch):
                result = build([dict(self.question(), **patch)], {})
                self.assertEqual(result['rows'][0]['route'], 'other_deferred_review')
                self.assertFalse(result['rows'][0]['technical_truth_verified'])

    def test_publishable_conflict_not_copied_to_question_section(self):
        candidate = dict(self.question(), claim_type='contradiction', context_status='sufficient',
                         finding='Два значения одного параметра не совпадают.', required_context=[])
        result = build([candidate], {})
        self.assertEqual(result['rows'], [])
        self.assertEqual(result['published_source_indices'], [0])


if __name__ == '__main__':
    unittest.main()
