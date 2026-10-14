"""MD text is authoritative; PDF is used for graphics, never a competing transcript."""
import json

import pytest

from backend.app.services.project_change_v3 import source_prep, source_checks, supplemental, quality, verification
from backend.tests.project_change_v3.test_v3_runtime_generic import synthetic_pair  # noqa: F401


def _prepare(pair):
    return source_prep.prepare_comparison_sources(pair_id='MD', old_paths=pair['old'], new_paths=pair['new'],
                                                   work_dir=pair['work'], object_id='obj_test')


def test_preparation_never_extracts_pdf_text_and_preserves_md(synthetic_pair, monkeypatch):
    import fitz

    def forbidden(*args, **kwargs):
        raise AssertionError('PDF text extraction must not compete with authoritative MD')

    monkeypatch.setattr(fitz.Page, 'get_text', forbidden)
    prepared = _prepare(synthetic_pair)
    for row in prepared['structure']:
        page = source_prep.load_page_record(synthetic_pair['work'], row['side'], row['physical_page'])
        assert 'native_page_text' not in page
        assert page['blocks'][0]['text_source'] == 'STRUCTURED_MD'
        assert page['blocks'][0]['md_block_type'] == 'TEXT'
        assert page['blocks'][0]['structured_md'] == row['blocks'][0]['structured_md']


def test_md_text_annotation_wins_over_image_metadata(synthetic_pair):
    path = synthetic_pair['old']['blocks']
    data = json.loads(path.read_text())
    data['blocks'][0]['block_type'] = 'image'
    path.write_text(json.dumps(data))
    _prepare(synthetic_pair)
    block = source_prep.load_page_record(synthetic_pair['work'], 'OLD', 1)['blocks'][0]
    assert block['modality'] == 'TEXT' and block['graphic_crop_ref'] == ''
    assert block['md_block_type'] == 'TEXT'


def test_missing_authoritative_md_is_an_error_not_a_pdf_fallback(synthetic_pair):
    paths = synthetic_pair['old']
    (paths.get('markdown') or paths.get('md')).write_text('# No block\n')
    with pytest.raises(source_prep.SourcePreparationError, match='authoritative Markdown missing'):
        _prepare(synthetic_pair)


def test_legacy_raw_text_never_enters_request_or_deterministic_checks(synthetic_pair):
    _prepare(synthetic_pair)
    path = synthetic_pair['work'] / 'source/old/p001/page.json'
    page = json.loads(path.read_text())
    page['native_page_text'] = 'POISON_RAW_PDF_TEXT 999'
    path.write_text(json.dumps(page))
    data, images = source_prep.optimized_region_bundle(pair_id='MD', work_dir=synthetic_pair['work'],
                                                       region={'region_id':'R', 'old_pages':[1], 'new_pages':[1]})
    assert all('native_page_text' not in p for p in data['pages'])
    assert 'POISON_RAW_PDF_TEXT' not in '\n'.join(source_checks.page_lines(page))
    assert 'poison_raw_pdf_text' not in supplemental._tokens(page)
    assert not images  # TEXT does not receive a raster for OCR rechecking.


def test_md_authority_does_not_make_a_wrong_model_value_correct():
    block = {'block_id':'B', 'modality':'TEXT', 'md_block_type':'TEXT', 'structured_md':'Расход 100 м³/ч',
             'tables':[], 'bbox':[0,0,1,1]}
    pages = {('OLD',1): {'blocks':[block], 'native_page_text':'Расход 999 м³/ч'}}
    evidence = [{'side':'OLD', 'physical_page':1, 'block_id':'B'}]
    assert quality._source_literal_status('100', evidence, pages) == 'SOURCE_LITERAL_FOUND'
    assert quality._source_literal_status('999', evidence, pages) == 'SOURCE_VALUE_NOT_FOUND'
    batches = verification.build_verification_batches(
        work_items=[{'work_item_id':'W', 'projectchange_id':'C', 'parameter_ordinal':1,
                     'evidence':[['OLD',1,'B']]}],
        projectchanges=[{'projectchange_id':'C', 'changed_parameters':[{'old_value':'999'}]}],
        page_records=pages)
    assert batches[0]['images'] == []
    source = batches[0]['data']['items'][0]['sources'][0]
    assert source['structured_md'] == block['structured_md'] and source['md_block_type'] == 'TEXT'


def test_authoritative_text_is_not_discarded_as_recognition_metadata():
    text = 'Description: маркировка оборудования приведена в таблице.'
    page = {'blocks':[{'modality':'TEXT', 'structured_md':text}]}
    assert source_checks.page_lines(page) == [text]


def test_stamp_metadata_stays_on_its_page_and_preserves_conflicts(tmp_path):
    md = tmp_path / 'document.md'
    md.write_text('**Stamp:** Code: GLOBAL\n## Page 1 — Plan\n'
                  '> **Stamp:** Code: A | Sheet: 1\n> **Stamp:** Code: A | Sheet: 1\n'
                  '> **Stamp:** Code: B | Sheet: 1\n## Page 2\n'
                  '> **Stamp:** Code: C | Sheet: Не указано\n## Page 3\n')
    assert source_prep.parse_page_stamps(md) == {
        1: 'Code: A | Sheet: 1\nCode: B | Sheet: 1', 2: 'Code: C | Sheet: Не указано'}


@pytest.mark.parametrize('metadata,expected,origin', [
    ('> **Stamp:** Code: TEST | Sheet: 1', 'Code: TEST | Sheet: 1', 'MD_PAGE_STAMP'),
    ('', 'Не указано', 'MD_PAGE_STAMP_UNSPECIFIED'),
])
def test_stamp_without_separate_block_uses_md_metadata(synthetic_pair, monkeypatch, metadata, expected, origin):
    import fitz
    monkeypatch.setattr(fitz.Page, 'get_text', lambda *a, **k: pytest.fail('PDF text extraction'))
    paths = synthetic_pair['old']
    path = paths['blocks']
    data = json.loads(path.read_text())
    stamp = dict(data['blocks'][0], block_id='stamp-extra', block_type='stamp')
    data['blocks'].append(stamp)
    path.write_text(json.dumps(data))
    md = paths.get('markdown') or paths.get('md')
    md.write_text(md.read_text() + '\n## Page 1\n' + metadata + '\n')
    _prepare(synthetic_pair)
    blocks = source_prep.load_page_record(synthetic_pair['work'], 'OLD', 1)['blocks']
    record = next(b for b in blocks if b['block_id'] == 'stamp-extra')
    assert record['structured_md'] == expected
    assert record['text_source'] == origin
    assert record['graphic_crop_ref'] == ''
