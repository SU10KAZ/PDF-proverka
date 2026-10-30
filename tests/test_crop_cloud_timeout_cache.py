"""Offline upload-cache identity, corruption and cloud-timeout fallback regression tests."""
import hashlib
import json

import pytest

from backend.app.pipeline.stages.crop_blocks import blocks
from backend.app.services.common.blocks_json import build_result_json
from backend.app.services.common.pdf_crop import extract_block_crop


@pytest.fixture
def cached_version(tmp_path, monkeypatch):
    def forbidden(*a, **kw):
        pytest.fail('tests must not contact external services')
    def deadline(*a, **kw):
        assert kw['budget_s'] == 5.0
        raise TimeoutError('simulated cloud download deadline')
    monkeypatch.setattr(blocks.urllib.request, 'urlopen', forbidden)
    monkeypatch.setattr(blocks, '_download_with_deadline', deadline)
    inp, work = tmp_path / '01_input', tmp_path / '02_work'
    inp.mkdir(); work.mkdir(); (inp / 'crops').mkdir()
    doc = blocks.fitz.open()
    page = doc.new_page(width=200, height=300)
    page.draw_rect(blocks.fitz.Rect(20, 30, 150, 200), color=(1, 0, 0), fill=(1, 0, 0))
    doc.save(work / 'document.pdf')
    block = {'block_id': 'image1', 'block_type': 'image', 'page_index': 0,
             'coords_norm': [0.1, 0.1, 0.8, 0.7], 'crop_url': 'https://crop.invalid/one'}
    data = {'document_id': 'document1', 'generated_at': 'generation1',
            'coordinate_space': 'normalized_page_top_left',
            'pages': [{'page_index': 0, 'width_px': 2000, 'height_px': 3000}], 'blocks': [block]}
    (inp / 'sample_blocks.json').write_text(json.dumps(data))
    (work / 'result.json').write_text(json.dumps(build_result_json(data, pdf_name='document.pdf')))
    crop = inp / 'crops/image1.pdf'
    extract_block_crop(doc, 0, block['coords_norm'], crop)
    doc.close()
    manifest = {'document_id': 'document1', 'blocks_json_generated_at': 'generation1',
                'entries': [{'block_id': 'image1', 'file': 'image1.pdf', 'status': 'ok',
                             'bytes': crop.stat().st_size, 'sha256': hashlib.sha256(crop.read_bytes()).hexdigest()}]}
    (inp / 'crops_manifest.json').write_text(json.dumps(manifest))
    return tmp_path


def run(version, **kwargs):
    return blocks.crop_blocks(str(version), skip_small=False, dpi=100,
                             output_dir_name=str(version / 'output'), **kwargs)


def test_verified_cache_after_cloud_timeout_avoids_pdf_render(cached_version, monkeypatch):
    def forbidden(*a, **kw): pytest.fail('verified cache should be used')
    monkeypatch.setattr(blocks, 'crop_from_pdf', forbidden)
    result = run(cached_version)
    assert result['errors'] == 0
    assert result['blocks'][0]['source'] == 'local_cache_pdf'
    assert result['crop_failover']['cloud_crop_attempts'] == 1
    first = (cached_version / 'output/block_image1.png').read_bytes()
    result = run(cached_version)
    assert result['skipped'] == 1
    assert (cached_version / 'output/block_image1.png').read_bytes() == first


@pytest.mark.parametrize('mismatch', ['generation', 'document', 'page', 'coords', 'sha', 'missing', 'file', 'corrupt', 'duplicate'])
def test_untrusted_cache_uses_exact_pdf(cached_version, mismatch):
    inp = cached_version / '01_input'
    manifest = json.loads((inp / 'crops_manifest.json').read_text())
    data = json.loads((inp / 'sample_blocks.json').read_text())
    if mismatch == 'generation': manifest['blocks_json_generated_at'] = 'old'
    if mismatch == 'document': manifest['document_id'] = 'other'
    if mismatch == 'page': data['blocks'][0]['page_index'] = 1
    if mismatch == 'coords': data['blocks'][0]['coords_norm'] = [0, 0, 1, 1]
    if mismatch == 'sha': manifest['entries'][0]['sha256'] = '0' * 64
    if mismatch == 'missing': (inp / 'crops/image1.pdf').unlink()
    if mismatch == 'file': manifest['entries'][0]['file'] = '../other.pdf'
    if mismatch == 'duplicate': manifest['entries'].append(manifest['entries'][0])
    if mismatch == 'corrupt':
        payload = b'not a PDF'
        (inp / 'crops/image1.pdf').write_bytes(payload)
        manifest['entries'][0].update(bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
    (inp / 'crops_manifest.json').write_text(json.dumps(manifest))
    (inp / 'sample_blocks.json').write_text(json.dumps(data))
    result = run(cached_version)
    assert result['errors'] == 0
    assert result['blocks'][0]['source'] == 'local_pdf'
    assert result['crop_failover']['cloud_crop_attempts'] == 1


@pytest.mark.parametrize('dpi,compact', [(100, False), (300, False), (None, True)])
def test_cache_preserves_render_policy_and_full_image(cached_version, dpi, compact):
    output = cached_version / 'policy_output'
    result = blocks.crop_blocks(str(cached_version), skip_small=False, dpi=dpi,
                                compact=compact, output_dir_name=str(output))
    assert result['errors'] == 0
    payload = (cached_version / '01_input/crops/image1.pdf').read_bytes()
    use_dpi = dpi if dpi is not None else blocks.TARGET_DPI_COMPACT
    min_side = blocks.MIN_LONG_SIDE_PX_COMPACT if compact and dpi is None else blocks.MIN_LONG_SIDE_PX
    expected = cached_version / 'expected.png'
    blocks._render_pdf_bytes_to_png(payload, expected, dpi=use_dpi, min_long_side=min_side)
    assert (output / 'block_image1.png').read_bytes() == expected.read_bytes()
    if compact:
        assert (output / 'block_image1_full.png').is_file()
        assert result['blocks'][0]['compact'] is True


