from copy import deepcopy
import unittest
from .ownership import check_partition


class OwnershipPartition(unittest.TestCase):
    def setUp(self):
        self.members=[dict(member_id='a'),dict(member_id='b')]
        self.good=dict(groups=[dict(group_id='g',member_ids=['a','b'],engineering_subject='same circuit',
            summary_ru='State changed',identity_reason='explicit circuit identity',relation='LINKED_CONFIGURATION')],review=[])

    def test_linked_states_preserved(self):self.assertEqual(check_partition(self.members,self.good),[])

    def test_no_silent_loss(self):
        self.good['groups'][0]['member_ids']=['a']
        self.assertIn('LOST_DUPLICATED_OR_INVENTED_MEMBER',check_partition(self.members,self.good))

    def test_no_double_count(self):
        self.good['review']=[dict(member_id='b',reason='duplicate handling cannot count twice')]
        self.assertIn('LOST_DUPLICATED_OR_INVENTED_MEMBER',check_partition(self.members,self.good))

    def test_unknown_member_rejected(self):
        self.good['groups'][0]['member_ids'].append('foreign')
        self.assertTrue(check_partition(self.members,self.good))

    def test_explicit_review_counts_once(self):
        self.good['groups'][0].update(member_ids=['a'],relation='SINGLETON')
        self.good['review']=[dict(member_id='b',reason='unresolved engineering owner')]
        self.assertEqual(check_partition(self.members,self.good),[])
