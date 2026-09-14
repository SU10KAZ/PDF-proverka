import unittest
from .containers import system_field,admission


class ContainerTests(unittest.TestCase):
    def test_system_field_is_not_position_or_model(self):
        self.assertEqual(system_field(['Тел.:','','№ системы','ДУ8.2.3']),'ДУ8.2.3')
        self.assertIsNone(system_field(['Модель','ДУ8.2.3']))
        self.assertIsNone(system_field(['№ системы','ДУ8.2.3, ДУ8.2.4']))

    def test_unknown_owner_does_not_relabel_table(self):
        a=admission({'purity':'PROVEN'},{'source_type':'TABLE','status':'REVIEW','reasons':['OWNER_UNKNOWN']})
        self.assertEqual(a['source_purity'],'PROVEN');self.assertEqual(a['ownership'],'REVIEW')


if __name__=='__main__':unittest.main()
