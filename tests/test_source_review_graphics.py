from hashlib import sha256

from PIL import Image

from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts


def test_graphic_files_are_bound_by_hash_but_not_marked_visually_verified(tmp_path):
    image = tmp_path/'block_g.png'
    Image.new('RGB', (3, 2)).save(image)
    queue = {'packets': [{'required_graphic_block_ids': ['g']}]}
    result = attach_graphic_receipts(queue, '### BLOCK #1 [IMAGE]: g\nSummary\n', tmp_path)
    packet = result['packets'][0]
    receipt = packet['graphic_attachments'][0]
    assert receipt['sha256'] == sha256(image.read_bytes()).hexdigest()
    assert (receipt['width'], receipt['height']) == (3, 2)
    assert not receipt['attached_to_model'] and not receipt['visually_reviewed']
    assert packet['graphic_context_status'] == 'ready_for_attachment'


def test_text_duplicate_and_outside_paths_are_never_attached(tmp_path):
    (tmp_path/'images').mkdir()
    Image.new('RGB', (1, 1)).save(tmp_path/'outside.png')
    (tmp_path/'images/block_g.png').symlink_to(tmp_path/'outside.png')
    md = '### BLOCK #1 [TEXT]: t\nText\n### BLOCK #2 [GRAPHIC]: g\nGraph\n### BLOCK #3 [IMAGE]: duplicate\nA\n### BLOCK #4 [IMAGE]: duplicate\nB\n'
    queue = {'packets': [{'required_graphic_block_ids': ['t', 'duplicate', 'g']}]}
    packet = attach_graphic_receipts(queue, md, tmp_path/'images')['packets'][0]
    assert packet['graphic_attachments'] == []
    assert [r['reason'] for r in packet['unavailable_graphics']] == ['not_unique_graphic_id', 'not_unique_graphic_id', 'outside_graphic_directory']


def test_missing_and_corrupt_graphics_remain_explicit(tmp_path):
    (tmp_path/'block_bad.png').write_text('Not an image')
    md = '### BLOCK #1 [IMAGE]: missing\nA\n### BLOCK #2 [IMAGE]: bad\nB\n'
    queue = {'packets': [{'required_graphic_block_ids': ['missing', 'bad']}]}
    packet = attach_graphic_receipts(queue, md, tmp_path)['packets'][0]
    assert packet['graphic_context_status'] == 'incomplete'
    assert packet['graphic_attachments'] == []
    assert len(packet['unavailable_graphics']) == 2
