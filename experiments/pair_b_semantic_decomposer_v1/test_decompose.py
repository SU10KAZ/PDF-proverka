import unittest

from .transport import encode, decode, request_bytes
from .decompose import output_schema


class DecompositionSafetyTests(unittest.TestCase):
    def test_unicode_lossless_substrings_and_empty_sides(self):
        text = 'Первый исходный текст\n' * 80
        source = dict(old=[dict(native_text=text, ocr_text=text[7:700], text=text)], new=[],
                      table={'cells': ['1,2', '3,4'], 'coordinates': [0, .1, .9, 1]})
        encoded = encode(source)
        self.assertEqual(decode(encoded), source)
        rows = [dict(identity='same_document', page=i, content=text) for i in range(5)]
        self.assertEqual(decode(encode(rows)), rows)

    def test_reserved_key_and_image_overflow_rejected(self):
        with self.assertRaises(ValueError):
            encode({'$source_text': 'ambiguous'})
        images = [dict(evidence_id=str(i), side='old', page=1, bbox=[0, 0, 1, 1]) for i in range(9)]
        with self.assertRaises(ValueError):
            request_bytes('prompt', {}, images)

    def test_schema_has_no_final_verdict_or_candidate_cap(self):
        schema = output_schema()
        candidates = schema['properties']['candidates']
        self.assertNotIn('maxItems', candidates)
        fields = candidates['items']['properties']
        self.assertNotIn('verdict', fields)
        self.assertEqual(set(fields['candidate_kind']['enum']),
                         {'POTENTIAL_CHANGE', 'POTENTIAL_NOT_CHANGE', 'INSUFFICIENT_FOR_COMPARISON'})


if __name__ == '__main__':
    unittest.main()
