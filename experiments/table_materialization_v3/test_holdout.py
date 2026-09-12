"""Source-only exclusion and numbering controls; never create holdout answers."""
import unittest

from .holdout import family, strict_numbers


class HoldoutSourceTests(unittest.TestCase):
    def test_document_family_excludes_revisions(self):
        self.assertEqual(family('Проект_ОВ_V1'), family('Проект ОВ V2'))
        self.assertNotEqual(family('Проект ОВ'), family('Проект ВК'))

    def test_reset_surface_requires_full_real_progression(self):
        self.assertEqual(strict_numbers([['1', 'A'], ['1', 'B']]), [])
        self.assertEqual(strict_numbers([['12.5', 'A'], ['13.5', 'B']]), [])
        self.assertEqual(strict_numbers([['1', '2', '3'], ['1.', 'A', 'x'], ['2.', 'B', 'y']]), [1, 2])
        self.assertEqual(strict_numbers([['3', 'A'], ['4', 'B']]), [3, 4])
        self.assertEqual(strict_numbers([['3', 'A'], ['2', 'B']]), [])


if __name__ == '__main__':
    unittest.main()
