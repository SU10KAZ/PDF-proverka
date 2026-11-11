"""Executor-bound, GRAPHIC-only detail recovery requested by unresolved operations."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import tempfile
import fitz

from .graphic_detail_render import render

GRAPHIC_DETAIL_INSTRUCTIONS = '''
If an attached GRAPHIC is unreadable at its current resolution, keep the operation
needs_data and add graphic_detail_requests: [{"block_id": "exact supplied ID",
"reason": "which marks or connections need a closer view"}] to that operation.
Request only supplied GRAPHIC/IMAGE block IDs; never paths, coordinates or TEXT.
The executor may supply additional views of the same block. Tiles are not separate
physical objects. Requesting a detail or receiving one proves no defect and does
not establish that the operation is checked. Preserve supported local findings.
'''


def freeze_graphic_source(pdf_path, blocks_path, output_dir):
    """Called only with executor-selected paths, not model response values."""
    result = {'output_dir': str(Path(output_dir).resolve())}
    for name, value in (('pdf', pdf_path), ('blocks', blocks_path)):
        path = Path(value).resolve()
        result[name + '_path'] = str(path)
        result[name + '_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def verify_graphic_source(source):
    for name in ('pdf', 'blocks'):
        if hashlib.sha256(Path(source[name + '_path']).read_bytes()).hexdigest() != source[name + '_sha256']:
            raise ValueError(name + '_version_changed')


def choose_detail_grid(source, bid, available):
    """Rendering budget heuristic only; never an engineering tolerance."""
    data = json.loads(Path(source['blocks_path']).read_text())
    matches = [b for b in data['blocks'] if b['block_id'] == bid]
    if data.get('coordinate_space') != 'normalized_page_top_left' or len(matches) != 1 or matches[0]['block_type'] != 'image':
        raise ValueError('Exactly one normalized GRAPHIC required')
    block = matches[0]
    n = block['coords_norm']
    if len(n) != 4 or not all(0 <= v <= 1 for v in n) or n[0] >= n[2] or n[1] >= n[3]:
        raise ValueError('Invalid block geometry')
    with fitz.open(source['pdf_path']) as doc:
        page = doc[block['page_index']]
        width = (n[2]-n[0])*page.rect.width
        height = (n[3]-n[1])*page.rect.height
    if available >= 3 and width / height >= 3:
        return 3, 1
    if available >= 3 and height / width >= 3:
        return 1, 3
    if available >= 9:
        return 3, 3
    if available >= 4:
        return 2, 2
    return 0, 0


def prepare_graphic_followup(request, payload, sources):
    """Return a bound pass or an explicit limit; never dispatch a model."""
    from .codex_targeted_findings import CodexTargetedPass
    from .object_operation_followup import verify_operation_source_binding
    packet = json.loads(request.messages[-1]['content'])
    operations = {op['operation'] for op in packet['operations']}
    requested = []
    for op in payload.get('object_operation_results', []):
        if (isinstance(op, dict) and op.get('object_id') == packet['object']['id']
                and op.get('operation') in operations and op.get('status') == 'needs_data'):
            entries = op.get('graphic_detail_requests', [])
            if not isinstance(entries, list):
                raise ValueError('Invalid graphic detail requests')
            for entry in entries:
                if not isinstance(entry, dict) or not isinstance(entry.get('block_id'), str) or not isinstance(entry.get('reason'), str) or not entry['reason'].strip():
                    raise ValueError('Graphic request requires block ID and reason')
                if entry['block_id'] not in requested:
                    requested.append(entry['block_id'])
    if not requested:
        return None, {'status': 'no_graphic_detail_request'}
    binding = request.continuation_source
    source = binding.get('graphic_source')
    if source is None:
        return None, {'status': 'graphic_source_unavailable', 'requested_block_ids': requested}
    error = verify_operation_source_binding(request)
    if error:
        raise ValueError(error)
    verify_graphic_source(source)
    for bid in requested:
        if (bid not in packet['attached_image_ids_in_order'] or bid not in sources
                or sources[bid]['kind'] not in ('GRAPHIC', 'IMAGE')):
            raise ValueError('Detail request must name a supplied GRAPHIC')
    done = {im['block_id'] for im in binding.get('detail_images', [])}
    pending = [bid for bid in requested if bid not in done]
    if not pending:
        return None, {'status': 'graphic_details_already_supplied', 'requested_block_ids': requested}
    remaining = 16 - len(request.image_paths)
    bid = pending[0]
    columns, rows = choose_detail_grid(source, bid, remaining)
    if not columns:
        return None, {'status': 'graphic_image_budget', 'requested_block_ids': pending}
    # One full block per revision; no silent replacement of earlier evidence.
    parent = Path(source['output_dir']); parent.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix='graphic_detail_', dir=parent))
    manifest = render(source['pdf_path'], source['blocks_path'], bid,
                      source['pdf_sha256'], run / 'tiles', columns=columns, rows=rows, scale=3)
    verify_graphic_source(source)
    tiles = manifest['tiles']
    paths = [str(p) for p in request.image_paths] + [t['path'] for t in tiles]
    if sum(Path(p).stat().st_size for p in paths) > 24_000_000:
        return None, {'status': 'graphic_bytes_budget', 'requested_block_ids': pending}
    next_packet = deepcopy(packet)
    next_packet['context_revision'] = packet.get('context_revision', 0) + 1
    next_packet['attached_image_ids_in_order'] += [bid] * len(tiles)
    next_packet['prior_operation_response_untrusted'] = payload
    next_packet.setdefault('graphic_detail_views', []).append({
        'block_id': bid, 'first_image_index_zero_based': len(request.image_paths),
        'tile_count': len(tiles), 'grid_columns': columns, 'grid_rows': rows,
        'same_source_block': True, 'semantic_verified': False})
    messages = deepcopy(request.messages)
    messages[-1]['content'] = json.dumps(next_packet, ensure_ascii=False)
    if sum(len(m['content']) for m in messages) > 120_000:
        return None, {'status': 'graphic_message_budget', 'requested_block_ids': pending}
    new_binding = deepcopy(binding)
    new_binding.setdefault('detail_images', []).extend(
        {k: tile[k] for k in ('block_id', 'path', 'sha256')} for tile in tiles)
    root = re.sub(r'_context_\d+$', '', request.stage)
    stage = f"{root}_context_{next_packet['context_revision']}"
    followup = CodexTargetedPass(stage, '03_' + stage + '.json', messages,
                                tuple(paths), continuation_source=new_binding)
    error = verify_operation_source_binding(followup)
    if error:
        raise ValueError(error)
    return followup, {'status': 'prepared', 'route': 'graphic_detail', 'next_stage': stage,
                      'block_id': bid, 'tiles_added': len(tiles),
                      'deferred_block_ids': pending[1:], 'manifest_path': str(run / 'tiles/manifest.json'),
                      'semantic_verified': False, 'absence_proven': False}
