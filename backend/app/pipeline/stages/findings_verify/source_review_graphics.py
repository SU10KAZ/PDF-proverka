"""Prepare graphic attachment receipts; no model calls or TEXT raster fallback."""
from collections import Counter
import hashlib
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
    return queue
