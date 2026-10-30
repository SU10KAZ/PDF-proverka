"""Read upload-time PDF crops only when their document and geometry match OCR."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from backend.app.services.common.blocks_json import _scale_coords


class VerifiedCropCache:
    def __init__(self, version_dir: Path, ocr_path: Path, ocr: dict):
        self.root = version_dir / '01_input' / 'crops'
        self.entries = {}
        self.blocks = {}
        # The upload cache belongs to one canonical v2 document, never to a
        # similarly named legacy/multi-PDF OCR file or a different generation.
        if ocr_path.resolve() != (version_dir / '02_work' / 'result.json').resolve():
            return
        if ocr.get('source') != 'blocks_json_v1':
            return
        doc_id = ocr.get('document_id')
        generation = ocr.get('blocks_json_generated_at')
        if not doc_id or not generation:
            return
        try:
            manifest = json.loads((self.root.parent / 'crops_manifest.json').read_text())
            if (manifest.get('document_id'), manifest.get('blocks_json_generated_at')) != (doc_id, generation):
                return
            candidates = list(self.root.parent.glob('*_blocks.json'))
            candidates.append(version_dir / '02_work' / 'blocks.json')
            matches = []
            for path in candidates:
                if not path.is_file():
                    continue
                data = json.loads(path.read_text())
                if (data.get('document_id'), data.get('generated_at')) == (doc_id, generation):
                    matches.append(data)
            if len(matches) != 1:
                return
            data = matches[0]
            if data.get('coordinate_space') != 'normalized_page_top_left':
                return
            blocks = data['blocks']
            entries = manifest['entries']
            self.blocks = {b['block_id']: b for b in blocks}
            self.entries = {e['block_id']: e for e in entries}
            if len(self.blocks) != len(blocks) or len(self.entries) != len(entries):
                self.blocks, self.entries = {}, {}
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            self.blocks, self.entries = {}, {}

    def read(self, block: dict) -> bytes | None:
        """Invalid/old/mutated cache is a miss; the caller renders the source PDF."""
        try:
            bid = block['block_id']
            if not isinstance(bid, str) or Path(bid).name != bid or '\\' in bid:
                return None
            geometry = self.blocks.get(bid, {})
            entry = self.entries.get(bid, {})
            if geometry.get('block_type') != 'image':
                return None
            if geometry.get('page_index') != block['page_num'] - 1:
                return None
            width, height = block['page_dimensions']
            if not width or not height or _scale_coords(geometry.get('coords_norm'), width, height) != block['coords_px']:
                return None
            if entry.get('status') not in ('ok', 'cached') or entry.get('file') != f'{bid}.pdf':
                return None
            digest = entry.get('sha256')
            if not digest:
                return None
            path = self.root / entry['file']
            if path.resolve().parent != self.root.resolve():
                return None
            size = path.stat().st_size
            if size != entry.get('bytes') or not 0 < size <= 64 * 1024 * 1024:
                return None
            payload = path.read_bytes()
            if hashlib.sha256(payload).hexdigest() != digest:
                return None
            return payload
        except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError):
            return None
