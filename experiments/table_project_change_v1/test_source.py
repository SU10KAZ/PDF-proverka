"""End-to-end adapter checks with explicit miniature Table V3 artifacts."""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from .common import digest, file_hash, write
from .engine import compare
from .source import load_document, typed_value, verify_evidence


def fixture(root, side, model, flow, review=False, reorder=False):
    folder = root / 'objects' / 'test-project' / side
    folder.mkdir(parents=True)
    header = ['Марка установки', 'Класс оборудования', 'Модель', 'Расход, м3/ч', 'Количество', 'Единица измерения']
    data = ['П1', 'установка', model, flow, '1', 'шт.']
    if reorder:
        header.reverse()
        data.reverse()
    cells = [header, data]
    md = folder / 'document.md'
    md.write_text('\n'.join('| ' + ' | '.join(row) + ' |' for row in cells) + '\n')
    # This source fixture is intentionally not claimed as a rendered source PDF.
    (folder / 'document.pdf').write_bytes(b'%PDF fixture used only for receipt tests\n')
    write(folder / 'blocks.json', {})
    receipts = {name: dict(path=str(folder / filename), sha256=file_hash(folder / filename)) for name, filename in
                [('pdf', 'document.pdf'), ('work_md', 'document.md'), ('blocks', 'blocks.json')]}
    version = 'fixture-' + side
    artifact_dir = root / 'artifacts' / version
    ledger = dict(schema='test-ledger', document_version=version, document_code='FIXTURE', sources=receipts,
                  columns=dict(kind=['TABLE_ROW'] * 2, owner=[0, 0], markdown_line=[1, 2], block_ref=[0, 0]),
                  blocks=[dict(block_id='block-1', page=1)])
    table = dict(table_key='table-' + side, segments=[0], pages=[1],
                 semantic_structure=dict(schema=[], type=None),
                 rows=dict(line=[0, 1], content_key=[digest(row) for row in cells], explicit_number=[None, None], role=['SCHEMA', 'DATA']))
    artifact = dict(schema='logical-tables.v3', document_version=version, tables=[table],
                    boundaries=[dict(basis='NO_EVIDENCE', decision='REVIEW')] if review else [],
                    candidate_relations=[dict(left_table=0, right_table=0)] if review else [])
    write(artifact_dir / 'ledger.json', ledger)
    write(artifact_dir / 'tables.json', artifact)
    return dict(document_version=version, document_code='FIXTURE', artifacts=receipts), root / 'artifacts'


class SourceTests(unittest.TestCase):
    def test_two_sources_to_one_replacement_with_column_reorder(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            old, directory = fixture(root, 'old', 'A', '1000')
            new, _ = fixture(root, 'new', 'B', '1500', reorder=True)
            a, b = load_document(old, directory), load_document(new, directory)
            result = compare(dict(comparison_scope='fixture-pair', old_records=a['records'], new_records=b['records']))
            changes = result['project_changes']
            self.assertEqual(1, len(changes))
            self.assertEqual('PROVEN', changes[0]['status'])
            self.assertEqual('EQUIPMENT_REPLACED', changes[0]['change_type'])
            self.assertEqual({'model', 'flow'}, {f['property'] for f in changes[0]['supporting_fact_changes']})
            evidence = changes[0]['evidence_old'] + changes[0]['evidence_new']
            self.assertEqual(10, verify_evidence(evidence))
            evidence[0]['quote'] = 'tampered'
            with self.assertRaises(ValueError):
                verify_evidence(evidence)

    def test_review_boundary_is_preserved(self):
        with TemporaryDirectory() as td:
            old, directory = fixture(Path(td), 'old', 'A', '1000', review=True)
            new, _ = fixture(Path(td), 'new', 'B', '1500')
            a, b = load_document(old, directory), load_document(new, directory)
            c = compare(dict(comparison_scope='fixture-pair', old_records=a['records'], new_records=b['records']))['project_changes'][0]
            self.assertEqual('REVIEW', c['status'])

    def test_source_tamper_fails_closed(self):
        with TemporaryDirectory() as td:
            doc, directory = fixture(Path(td), 'old', 'A', '1000')
            Path(doc['artifacts']['work_md']['path']).write_text('modified')
            with self.assertRaises(ValueError):
                load_document(doc, directory)

    def test_non_table_ownership_rejected(self):
        import json
        with TemporaryDirectory() as td:
            doc, directory = fixture(Path(td), 'old', 'A', '1000')
            p = directory / doc['document_version'] / 'ledger.json'
            ledger = json.loads(p.read_text())
            ledger['columns']['kind'][1] = 'TEXT'
            write(p, ledger)
            with self.assertRaises(ValueError):
                load_document(doc, directory)

    def test_standard_not_proven_model(self):
        self.assertEqual('REVIEW', typed_value('model', 'ГОСТ 123', 'Тип, марка')['status'])

    def test_unknown_numeric_reading_not_corrected(self):
        self.assertEqual('REVIEW', typed_value('power', '53.07 75', 'Мощность, кВт')['status'])
        self.assertIsNone(typed_value('power', '—', 'Мощность, кВт'))


if __name__ == '__main__':
    unittest.main()
