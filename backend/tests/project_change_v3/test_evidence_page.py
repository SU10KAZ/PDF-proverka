"""Deterministic evidence highlighting on synthetic PDF pages; zero model calls."""
import fitz
import pytest

from backend.app.services.project_change_v3.evidence_page import locate, render_page
from backend.tests.project_change_v3.test_generic_production_boundary_e2e import generic_env, _run  # noqa: F401
from backend.tests.project_change_v3 import generic_fixture as gf


def pdf(lines, rotation=0):
    doc = fitz.open()
    page = doc.new_page(width=600, height=400)
    for index, text in enumerate(lines):
        page.insert_text((50, 50 + index * 40), text, fontsize=12)
    page.set_rotation(rotation)
    return doc


def evidence(quote, bbox=None, source_type='TEXT'):
    return {'quote': quote, 'bbox': bbox, 'source_type': source_type}


def test_exact_multiline_quote_highlights_only_its_lines():
    with pdf(['Unrelated heading', 'Pipe insulation thickness', 'must be at least 10 mm', 'Other requirements']) as doc:
        found = locate(doc[0], evidence('Pipe insulation thickness must be at least 10 mm'))
        assert found['kind'] == 'EXACT_QUOTE'
        assert len(found['highlights']) == 2
        assert all(.15 < b['y'] < .35 for b in found['highlights'])
        assert found['page_width'] == 600
        pix = fitz.Pixmap(render_page(doc[0]))
        assert pix.width / pix.height == pytest.approx(1.5)


def test_duplicate_phrase_is_ambiguous_unless_source_block_selects_one():
    with pdf(['Pipe insulation 10 mm', 'Pipe insulation 10 mm']) as doc:
        full = locate(doc[0], evidence('Pipe insulation 10 mm', [0, 0, 1, 1]))
        assert full['kind'] == 'SOURCE_BLOCK'
        assert 'несколько раз' in full['message']
        selected = locate(doc[0], evidence('Pipe insulation 10 mm', [0, .17, 1, .3]))
        assert selected['kind'] == 'EXACT_QUOTE'
        assert len(selected['highlights']) == 1 and selected['highlights'][0]['y'] > .17


def test_ellipsis_fragments_are_explicitly_partial():
    with pdf(['Pipe insulation thickness 10 mm', 'An intervening paragraph', 'Heating insulation thickness 20 mm']) as doc:
        found = locate(doc[0], evidence('Pipe insulation thickness 10 mm… words not present… Heating insulation thickness 20 mm'))
        assert found['kind'] == 'PARTIAL_QUOTE' and len(found['highlights']) == 2
        assert len(found['matched_fragments']) == 2
        assert 'Полное совпадение' in found['message']


@pytest.mark.parametrize('quote', ['Pipe insulation 20 mm', 'Insulation of pipe is 10 mm', 'Value -10 units', 'Value 1.0 units'])
def test_no_fuzzy_or_numeric_substitution(quote):
    with pdf(['Pipe insulation 10 mm', 'Value 10 units']) as doc:
        found = locate(doc[0], evidence(quote))
        assert found['kind'] == 'NOT_FOUND' and found['highlights'] == []


def test_graphic_uses_saved_block_even_when_caption_matches_text():
    with pdf(['Pipe insulation 10 mm']) as doc:
        found = locate(doc[0], evidence('Pipe insulation 10 mm', [.1, .2, .8, .6], 'GRAPHIC'))
        assert found['kind'] == 'SOURCE_BLOCK' and 'чертежа' in found['message']
        assert found['highlights'][0]['y'] == pytest.approx(.2)
        assert found['highlights'][0]['height'] == pytest.approx(.4)


def test_scan_and_missing_coordinates_have_honest_fallbacks():
    with pdf([]) as doc:
        block = locate(doc[0], evidence('No text', [.1, .2, .8, .6]))
        assert block['kind'] == 'SOURCE_BLOCK' and 'нет доступного текстового слоя' in block['message']
        for bbox in [None, [0, 0, 0, 0], [0, 0, float('nan'), 1], [-1, 0, 1, 1]]:
            found = locate(doc[0], evidence('No text', bbox))
            assert found['kind'] == 'NOT_FOUND' and found['highlights'] == []


@pytest.mark.parametrize('rotation', [0, 90, 180, 270])
def test_rotation_and_cropbox_keep_boxes_on_rendered_words(rotation):
    with pdf(['Pipe insulation 10 mm'], rotation) as doc:
        page = doc[0]
        page.set_cropbox(fitz.Rect(20, 20, 580, 380))
        word_rect = page.search_for('Pipe insulation 10 mm')[0] * page.rotation_matrix
        box = [word_rect.x0/page.rect.width, word_rect.y0/page.rect.height,
               word_rect.x1/page.rect.width, word_rect.y1/page.rect.height]
        found = locate(page, evidence('Pipe insulation 10 mm', box))
        assert found['kind'] == 'EXACT_QUOTE'
        marked = found['highlights'][0]
        assert marked['x'] == pytest.approx(box[0])
        assert marked['y'] == pytest.approx(box[1])
        assert marked['width'] == pytest.approx(box[2] - box[0])
        assert marked['height'] == pytest.approx(box[3] - box[1])
        pix = fitz.Pixmap(render_page(page))
        assert pix.width/pix.height == pytest.approx(page.rect.width/page.rect.height, abs=.002)


def test_page_endpoints_preserve_object_pair_run_and_stale_source_guards(generic_env, monkeypatch):  # noqa: F811
    env = generic_env; client = env['client']
    state = _run(env)
    url = f"/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes"
    items = client.get(url).json()['items']
    e = items[0]['evidence'][0]
    from backend.app.services.project_change_v3 import presentation
    def no_full_presentation(*args, **kwargs):
        raise AssertionError('Opening one image must not rebuild every card')
    monkeypatch.setattr(presentation, 'pair_presentation', no_full_presentation)
    found = client.get(e['page_view_url'])
    assert found.status_code == 200, found.text
    data = found.json()
    assert data['evidence_id'] == e['id'] and data['quote'] == e['quote']
    assert data['kind'] == 'SOURCE_BLOCK'  # fixture PDF labels differ from its synthetic OCR
    image = client.get(data['image_url'])
    assert image.status_code == 200 and image.content.startswith(b'\x89PNG')
    assert fitz.Pixmap(image.content).width / fitz.Pixmap(image.content).height == pytest.approx(1.5)
    assert client.get(e['image_url']).status_code == 200  # existing crop unchanged
    for bad in [e['page_view_url'].replace(gf.OBJECT_ID, 'other_object'),
                e['page_view_url'].replace(gf.PAIR_ID, 'other_pair'),
                e['page_view_url'].replace(state['run_id'], 'a'*32)]:
        assert client.get(bad).status_code == 404
    assert client.get(e['page_view_url'].split('?')[0]).status_code == 422
    # Altering the PDF makes both page endpoints unavailable, just like crops.
    from pathlib import Path
    p = Path(env['left']['pdf_path']); p.write_bytes(p.read_bytes() + b'\nchanged')
    assert client.get(e['page_view_url']).status_code == 404
    assert client.get(data['image_url']).status_code == 404
    assert [c['stage'] for c in env['fake'].calls] == ['MAPPING', 'MINING', 'MINING', 'DEDUPE']
