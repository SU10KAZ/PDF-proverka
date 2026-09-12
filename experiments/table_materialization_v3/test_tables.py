"""Structural adversarial controls; no human case IDs or corpus-answer fixtures."""
import itertools
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.semantic_foundation_v3 import frozen_v1 as v1
from experiments.semantic_foundation_v3.materialize import materialize_document as foundation
from .model import (RULES, TableBoundaryEvidence, assemble, norm, ordinal, chain,
                    parse_cells, row_values, strict_run)
from .run import TableResolver, materialize_document


class TableTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def document(self, bodies, page_numbers=None):
        page_numbers = page_numbers or list(range(1, len(bodies) + 1))
        md, blocks = self.root / 'document.md', self.root / 'blocks.json'
        md.write_text('\n'.join(f'## Page {n}\n### BLOCK #1 [text]: b{n}\n{body}\n'
                                for n, body in zip(page_numbers, bodies)))
        blocks.write_text(json.dumps({'pages': [{'page_index': n - 1, 'width_px': 100, 'height_px': 140}
                                             for n in page_numbers],
                                     'blocks': [{'page_index': n - 1, 'block_id': f'b{n}', 'block_type': 'text'}
                                                for n in page_numbers]}))
        return {'document_version': 'fixed-version', 'document_code': 'synthetic', 'version_id': 'v1',
                'source_refs': {}, 'artifacts': {k: {'path': str(p), 'sha256': v1.file_sha(p)}
                for k, p in [('work_md', md), ('blocks', blocks)]}}

    def table(self, numbers, header=True):
        return ('| Позиция | Наименование |\n| -- | -- |\n' if header else '') + '\n'.join(
            f'| {n} | Изделие {n} |' for n in numbers)

    def result(self, bodies, promoted=RULES, page_numbers=None):
        return materialize_document(self.document(bodies, page_numbers), promoted)

    def test_evidence_permutation_conflict_and_gate(self):
        for j, s in itertools.product(itertools.permutations(['a', 'b', 'a']), itertools.permutations(['x', 'y'])):
            d = TableBoundaryEvidence.collect(1, 2, j, s, promoted=['a', 'b', 'x', 'y'])
            self.assertEqual((d.decision, d.basis, d.conflict), ('REVIEW', 'CONFLICT', True))
            self.assertEqual(d.join_evidence, ('a', 'b'))
        self.assertEqual(TableBoundaryEvidence.collect(1, 2).basis, 'NO_EVIDENCE')
        d = TableBoundaryEvidence.collect(1, 2, ['a'], promoted=[])
        self.assertEqual(d.decision, 'REVIEW')
        self.assertIn('UNPROMOTED:a', d.neutral_evidence)
        d = TableBoundaryEvidence.collect(1, 2, ['a'], ['x'], promoted=['a'])
        self.assertEqual((d.decision, d.basis), ('REVIEW', 'CONFLICT'))
        self.assertEqual(d.split_evidence, ('x',))

    def test_separator_is_not_a_header_and_all_cells_survive(self):
        body = '| 21 | Pump | 4.2 |\n| -- | -- | -- |\n| 22 | Fan | 0.5 |'
        r = self.result([body])
        t = r['tables']['tables'][0]
        self.assertEqual(t['rows']['role'][0], 'DATA')
        self.assertEqual(t['rows']['explicit_number'], [21, 22])
        raw = Path(r['ledger']['sources']['work_md']['path']).read_text().splitlines()
        self.assertEqual(row_values(t, 0, r['ledger'], raw)['cells'], ['21', 'Pump', '4.2'])
        self.assertEqual(parse_cells('|| a ||'), ['', 'a', ''])
        self.assertEqual(parse_cells(r'| a\|b | c |'), [r'a\|b', 'c'])
        self.assertEqual(ordinal(norm('4.2')), None)
        self.assertEqual(chain(norm('4.2')), None)

    def test_three_page_join_and_conservation(self):
        doc = self.document([self.table([1, 2]), self.table([3, 4]), self.table([5, 6])])
        base, r = foundation(doc), materialize_document(doc, ['EDGE_ORDINAL_FLOW'])
        self.assertEqual([d['decision'] for d in r['tables']['boundaries']], ['SAME', 'SAME'])
        self.assertEqual(r['tables']['tables'][0]['pages'], [1, 2, 3])
        for k in ('ledger', 'semantics', 'decisions'):
            self.assertEqual(v1.canonical_bytes(base[k]), v1.canonical_bytes(r[k]))
        lines = [i for t in r['tables']['tables'] for i in t['rows']['line']]
        expected = [i for i, kind in enumerate(r['ledger']['columns']['kind']) if kind == 'TABLE_ROW']
        self.assertEqual(lines, expected)
        self.assertEqual(len(lines), len(set(lines)))

    def test_width_repeated_header_and_co_location_do_not_prove_join(self):
        body = '| Heading | Other |\n| -- | -- |\n| Alpha | Beta |'
        r = self.result([body, body])
        self.assertEqual(r['tables']['boundaries'][0]['decision'], 'REVIEW')
        self.assertEqual(len(r['tables']['candidate_relations']), 1)
        r = self.result([body + '\n### BLOCK #2 [text]: second\n' + body])
        self.assertEqual(len(r['tables']['tables']), 2)

    def test_constant_ordinals_never_reset_or_join(self):
        self.assertFalse(strict_run([2, 2, 2]))
        r = self.result([self.table([3, 3]), self.table([1, 1])])
        self.assertNotIn('STRICT_NUMBERING_RESTART', r['tables']['boundaries'][0]['candidate_split'])
        r = self.result([self.table([1, 1]), self.table([2, 3])])
        self.assertNotIn('EDGE_ORDINAL_FLOW', r['tables']['boundaries'][0]['candidate_join'])

    def test_real_restart_and_specification_groups(self):
        r = self.result([self.table([2, 3]), self.table([1, 2])])
        self.assertEqual(r['tables']['boundaries'][0]['decision'], 'NEW')
        def spec(group, numbers):
            return '| Позиция | Наименование | Единица измерения | Количество |\n| -- | -- | -- | -- |\n' + \
                f'| | {group} | | |\n' + '\n'.join(f'| {n} | Item {group} {n} | шт. | 2 |' for n in numbers)
        r = self.result([spec('Насосы', [2, 3]), spec('Вентиляторы', [1, 2])])
        edge = r['tables']['boundaries'][0]
        self.assertIn('NUMBERING_RESTART_IN_SPECIFICATION', edge['neutral_evidence'])
        self.assertNotIn('STRICT_NUMBERING_RESTART', edge['candidate_split'])
        self.assertEqual(edge['decision'], 'SAME')

    def test_passport_distinct_same_page_contracts(self):
        left = '| Описание | Модуль | Количество |\n| -- | -- | -- |\n| Valve | DA | 1 |'
        right = '| Pressure (Pa) | Actual Flow (m³/h) | Motor Speed (RPM) |\n| -- | -- | -- |\n| 0 | 10 | 800 |'
        r = self.result([left + '\n### BLOCK #2 [text]: second\n' + right], ['SEMANTIC_CONTRACT_CHANGE'])
        self.assertEqual(r['tables']['boundaries'][0]['decision'], 'NEW')
        self.assertEqual(len(r['tables']['tables']), 2)

    def test_furniture_and_narrative_conflict(self):
        r = self.result([self.table([1, 2]) + '\n1', '2\n' + self.table([3, 4])])
        edge = r['tables']['boundaries'][0]
        self.assertEqual(edge['decision'], 'SAME')
        self.assertIn('FURNITURE_ONLY_GAP', edge['neutral_evidence'])
        r = self.result([self.table([1, 2]) + '\nNarrative', self.table([3, 4])], ['MEANINGFUL_NARRATIVE'])
        self.assertEqual(r['tables']['boundaries'][0]['decision'], 'NEW')

    def test_nonadjacent_explicit_identity_and_insufficient_identity(self):
        r = self.result(['#### 1. Schedule\nТаблица 7\n' + self.table([19, 20]),
                         'Таблица 7\n' + self.table([300, 301])], page_numbers=[1, 4])
        self.assertEqual(r['tables']['boundaries'][0]['decision'], 'SAME')
        r = self.result([self.table([19, 20]), self.table([300, 301])], page_numbers=[1, 4])
        self.assertEqual(r['tables']['boundaries'][0]['decision'], 'REVIEW')

    def test_keys_ignore_page_numbers_and_bounding_boxes(self):
        bodies = [self.table([1, 2]), self.table([3, 4])]
        a = self.result(bodies, ['EDGE_ORDINAL_FLOW'])
        b = self.result(bodies, ['EDGE_ORDINAL_FLOW'], page_numbers=[10, 11])
        self.assertEqual(a['tables']['tables'][0]['table_key'], b['tables']['tables'][0]['table_key'])
        self.assertEqual(a['tables']['tables'][0]['rows']['content_key'], b['tables']['tables'][0]['rows']['content_key'])

    def test_row_key_survives_insertion_and_repeats_are_traceable(self):
        a = self.result(['| A | 2 |\n| B | 3 |'])['tables']['tables'][0]
        b = self.result(['| Added | 4 |\n| A | 2 |\n| B | 3 |'])['tables']['tables'][0]
        self.assertEqual(a['rows']['content_key'][0], b['rows']['content_key'][1])
        r = self.result(['| A | 2 |\n| A | 2 |'])['tables']['tables'][0]
        self.assertNotEqual(*r['rows']['content_key'])

    def test_source_only_and_replay(self):
        doc = self.document([self.table([1, 2]), self.table([3, 4])])
        with patch.object(v1, 'materialize_document', side_effect=AssertionError('Legacy pipeline')):
            a, b = materialize_document(doc, ['EDGE_ORDINAL_FLOW']), materialize_document(doc, ['EDGE_ORDINAL_FLOW'])
        self.assertEqual(v1.canonical_bytes(a), v1.canonical_bytes(b))

    def test_repeated_note_is_transparent_and_preserved(self):
        note = '\nПримечания:\n1. Заполняется при монтаже.'
        r = self.result([self.table([1, 2]) + note, self.table([3, 4]) + note],
                        ['EDGE_ORDINAL_FLOW@CROSS_PAGE_HEADERED'])
        e = r['tables']['boundaries'][0]
        self.assertEqual(e['decision'], 'SAME')
        self.assertIn('REPEATED_TABLE_NOTE', e['neutral_evidence'])
        self.assertTrue(e['witnesses']['repeated_notes'])
        for i in e['witnesses']['repeated_notes']:
            self.assertNotEqual(r['ledger']['columns']['owner'][i], 'UNOWNED')

    def test_gate_does_not_transfer_to_headerless_or_same_page(self):
        policy = ['EDGE_ORDINAL_FLOW@CROSS_PAGE_HEADERED', 'SEMANTIC_CONTRACT_CHANGE@CROSS_PAGE']
        r = self.result([self.table([1, 2], False), self.table([3, 4], False)], policy)
        self.assertEqual(r['tables']['boundaries'][0]['decision'], 'REVIEW')
        self.assertEqual(len(r['tables']['tables']), 2)


if __name__ == '__main__':
    unittest.main()
