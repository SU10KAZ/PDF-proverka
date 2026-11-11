"""Prepare graphic attachment receipts; no model calls or TEXT raster fallback."""
from collections import Counter
import hashlib
import json
from io import BytesIO
from pathlib import Path
import re

from PIL import Image


def attach_graphic_receipts(queue, md, graphic_dir):
    headers = re.findall(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I)
    counts = Counter(bid for _, bid in headers)
    allowed = {bid for kind, bid in headers if kind.upper() in ('IMAGE', 'GRAPHIC') and counts[bid] == 1}
    root = Path(graphic_dir).resolve()
    cache = {}
    for packet in queue.get('packets', []):
        receipts, unavailable = [], []
        for bid in packet.get('required_graphic_block_ids', []):
            if not isinstance(bid, str) or bid not in allowed or not re.fullmatch(r'[\w-]+', bid):
                unavailable.append({'block_id': bid, 'reason': 'not_unique_graphic_id'})
                continue
            if bid not in cache:
                path = root / f'block_{bid}.png'
                try:
                    if not path.resolve().is_relative_to(root):
                        cache[bid] = {'block_id': bid, 'reason': 'outside_graphic_directory'}
                    else:
                        raw = path.read_bytes()
                        with Image.open(BytesIO(raw)) as image:
                            size, fmt = image.size, image.format
                            image.verify()
                        if fmt != 'PNG':
                            raise ValueError('Expected PNG')
                        cache[bid] = {'block_id': bid, 'path': str(path),
                                      'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
                                      'width': size[0], 'height': size[1], 'mime_type': 'image/png',
                                      'attached_to_model': False, 'visually_reviewed': False}
                except (OSError, ValueError, SyntaxError) as exc:
                    cache[bid] = {'block_id': bid, 'reason': 'missing_or_unreadable_png', 'error_type': type(exc).__name__}
            item = dict(cache[bid])
            (unavailable if 'reason' in item else receipts).append(item)
        packet['graphic_attachments'] = receipts
        packet['unavailable_graphics'] = unavailable
        packet['graphic_context_status'] = ('incomplete' if unavailable else
                                             'ready_for_attachment' if receipts else 'not_required')
    _attach_detail_receipts(queue, md, root, allowed)
    # Executor-set directory only; never populated from a model response. Queue
    # copies preserve it across bounded source-review context expansion.
    operation_details = queue.get('_operation_graphic_detail_dir')
    if isinstance(operation_details, str):
        _attach_detail_receipts(queue, md, Path(operation_details).resolve(), allowed)
    return queue


def _attach_detail_receipts(queue, md, root, allowed):
    """Optional executor-authored detail manifest; never read paths from model output.

    Detail crops supplement the original graphic, not replace its source identity.
    Hashes establish byte/version provenance only, not visual or semantic validity.
    """
    manifest_path = root/'graphic_detail_manifest.json'
    if not manifest_path.exists():
        return
    try:
        manifest = json.loads(manifest_path.read_text())
        if manifest['source_md_sha256'] != hashlib.sha256(md.encode()).hexdigest():
            raise ValueError('detail_md_version_changed')
        rows = manifest['images']
        if not isinstance(rows, list):
            raise ValueError('invalid_detail_manifest')
    except (OSError, ValueError, KeyError, TypeError):
        for packet in queue.get('packets', []):
            packet['unavailable_graphics'].append({'reason':'detail_manifest_unusable'})
            packet['graphic_context_status'] = 'incomplete'
        return
    for packet in queue.get('packets', []):
        required = set(packet.get('required_graphic_block_ids', []))
        seen = {(r['block_id'], r['sha256']) for r in packet['graphic_attachments']}
        for row in rows:
            if not isinstance(row, dict) or row.get('block_id') not in required:
                continue
            bid = row['block_id']
            try:
                if bid not in allowed:
                    raise ValueError('not_unique_graphic_id')
                path = (root/row['file']).resolve()
                if not path.is_relative_to(root):
                    raise ValueError('outside_graphic_directory')
                raw = path.read_bytes()
                digest = hashlib.sha256(raw).hexdigest()
                if digest != row['sha256']:
                    raise ValueError('detail_image_version_changed')
                with Image.open(BytesIO(raw)) as image:
                    size, fmt = image.size, image.format
                    image.verify()
                if fmt != 'PNG':
                    raise ValueError('Expected PNG')
                if (bid, digest) in seen:
                    continue
                receipt = {'block_id':bid, 'path':str(path), 'sha256':digest,
                    'bytes':len(raw), 'width':size[0], 'height':size[1], 'mime_type':'image/png',
                    'view_kind':'detail_of_original_graphic',
                    'attached_to_model':False, 'visually_reviewed':False}
                for key in ('page_index', 'clip_rotated_page_points', 'source_pdf_sha256'):
                    if key in row:
                        receipt[key] = row[key]
                packet['graphic_attachments'].append(receipt)
                seen.add((bid, digest))
            except (OSError, ValueError, KeyError, TypeError, SyntaxError) as exc:
                packet['unavailable_graphics'].append({'block_id':bid,'reason':'detail_unavailable','error_type':type(exc).__name__})
        if packet['unavailable_graphics']:
            packet['graphic_context_status'] = 'incomplete'
        elif packet['graphic_attachments']:
            packet['graphic_context_status'] = 'ready_for_attachment'
