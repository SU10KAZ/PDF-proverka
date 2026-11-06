from types import SimpleNamespace
import unittest
from md_operations import scan_operations


def scan(text,kind='TEXT'):
    return scan_operations([SimpleNamespace(page=1,blocks=[SimpleNamespace(block_id='t1',block_type=kind,text=text)])])

class OperationsTest(unittest.TestCase):
    def test_inside_diameter_and_display_rounding(self):
        pattern='G=61,12 м³/ч;\nВыбираем трубопровод: Ø219×6 мм, v={} м/с.'
        r=scan(pattern.format('0,66'))[0]
        self.assertAlmostEqual(float(r['computed_v_m_s']),.50448734,places=8)
        self.assertEqual(r['status'],'arithmetic_question')
        self.assertEqual(scan(pattern.format('0,50'))[0]['status'],'consistent_with_display_rounding')
    def test_units_and_regime_not_guessed(self):
        base='G=61,12 м³/ч;\nВыбираем трубопровод: Ø219×6 мм, v=0,66 м/с.'
        for text in [base.replace('м³/ч','т/ч'),base.replace('Ø219×6','DN200'),base.replace('\n','\nДругой режим:\n')]:
            self.assertEqual(scan(text)[0]['status'],'not_checked')
        self.assertEqual(scan(base,'IMAGE'),[])
    def test_empty_quantity_does_not_mean_zero(self):
        table='| Поз. | Наименование | Ед. измерения | Кол-во | Примечание |\n|---|---|---|---|---|\n| K1 | Ø100 | м | {} | {} |'
        self.assertEqual(scan(table.format('',''))[0]['status'],'quantity_question')
        self.assertEqual(scan(table.format('0','')),[])
        self.assertEqual(scan(table.format('','учтено в комплекте'))[0]['status'],'inclusion_note_not_verified')

if __name__=='__main__':unittest.main()
