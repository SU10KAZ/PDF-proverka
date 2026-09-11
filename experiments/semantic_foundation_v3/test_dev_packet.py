import unittest
from .dev_packet import adapt, page_hashes, select_cases, QUOTAS, AUTOMATIC


class DevPacketTests(unittest.TestCase):
    def test_adapter_preserves_all_source_line_numbers(self):
        text = "## СТРАНИЦА 1\n\n### BLOCK [text]: b1\n\n#### Заголовок\nТекст\n"
        adapted = adapt(text)
        self.assertEqual(len(text.splitlines()), len(adapted.splitlines()))
        self.assertEqual(adapted.splitlines()[4:], text.splitlines()[4:])

    def test_page_hash_ignores_export_envelope_not_content(self):
        a = "## СТРАНИЦА 1\n### BLOCK [text]: b1\nТекст\n"
        b = "## Page 8\n### BLOCK #99 [text]: other\nТекст\n"
        self.assertEqual(list(page_hashes(a).values()), list(page_hashes(b).values()))
        self.assertNotEqual(list(page_hashes(a).values()), list(page_hashes(b.replace("Текст", "Иной текст")).values()))

    def test_budget_and_human_controls_separate(self):
        self.assertEqual(sum(QUOTAS.values()), 126)
        self.assertEqual(sum(QUOTAS[v] for v in AUTOMATIC), 22)
        self.assertEqual(sum(QUOTAS[v] for v in QUOTAS if v not in AUTOMATIC), 104)

    def test_duplicate_page_content_across_versions_excluded(self):
        cases = [{"case_id": str(i), "document_version": str(i), "page_pair": [1, 2],
                  "page_content_hashes": ["same_left", "same_right"], "variant": "S1", "stratum": "S1"}
                 for i in range(5)]
        selected, _ = select_cases(cases)
        self.assertEqual(len(selected), 1)

    def test_document_cap_shared_across_variants(self):
        cases = [{"case_id": str(i), "document_version": "doc", "page_pair": [i, i+1],
                  "variant": "S2_CAPS" if i % 2 else "S2_NUMBER", "stratum": "S2"} for i in range(10)]
        selected, _ = select_cases(cases)
        self.assertEqual(len(selected), 3)


if __name__ == "__main__":
    unittest.main()
