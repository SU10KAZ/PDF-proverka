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
    attached_paths = {str(Path(p).resolve()) for p in request.image_paths}
    done = {im['block_id'] for im in binding.get('detail_images', [])
            if str(Path(im['path']).resolve()) in attached_paths}
    pending = [bid for bid in requested if bid not in done]
    if not pending:
        return None, {'status': 'graphic_details_already_supplied', 'requested_block_ids': requested}
    base_paths = [str(p) for p in request.image_paths]
    base_ids = list(packet['attached_image_ids_in_order'])
    retired = []
    bid = pending[0]
    columns, rows = choose_detail_grid(source, bid, 16-len(base_paths))
    if not columns:
        # Retire only supplemental views. Keep every original attachment, or
        # restore its hash-bound original if a prior window supplied tiles only.
        kept_paths, kept_ids = [], []
        for image_id,path in zip(base_ids,base_paths,strict=True):
            original = binding['images'][image_id]
            if Path(path).resolve() == Path(original['path']).resolve():
                kept_paths.append(path); kept_ids.append(image_id)
            else:
                retired.append({'block_id':image_id,
                    'sha256':hashlib.sha256(Path(path).read_bytes()).hexdigest()})
        for image_id in dict.fromkeys(base_ids):
            if image_id not in kept_ids:
                kept_ids.append(image_id); kept_paths.append(binding['images'][image_id]['path'])
        if retired:
            base_ids, base_paths = kept_ids, kept_paths
            columns, rows = choose_detail_grid(source, bid, 16-len(base_paths))
    if not columns:
        return None, {'status': 'graphic_image_budget', 'requested_block_ids': pending}
    # One full block per revision; no silent replacement of earlier evidence.
    parent = Path(source['output_dir']); parent.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix='graphic_detail_', dir=parent))
    manifest = render(source['pdf_path'], source['blocks_path'], bid,
                      source['pdf_sha256'], run / 'tiles', columns=columns, rows=rows, scale=3)
    verify_graphic_source(source)
    tiles = manifest['tiles']
    paths = base_paths + [t['path'] for t in tiles]
    if sum(Path(p).stat().st_size for p in paths) > 24_000_000:
        return None, {'status': 'graphic_bytes_budget', 'requested_block_ids': pending}
    next_packet = deepcopy(packet)
    next_packet['context_revision'] = packet.get('context_revision', 0) + 1
    next_packet['attached_image_ids_in_order'] = base_ids + [bid] * len(tiles)
    if retired:
        next_packet.setdefault('retired_graphic_detail_views', []).extend(next_packet.pop('graphic_detail_views', []))
        next_packet.setdefault('graphic_view_retirements', []).append({
            'context_revision':next_packet['context_revision'], 'retired_images':retired,
            'reason':'Supplemental detail budget reclaimed; all original block images retained or restored.',
            'boundary':'Retired details remain in the audit history but are not attached now. Prior observations are untrusted; cite only currently attached images for new graphic claims.'})
    next_packet['prior_operation_response_untrusted'] = payload
    next_packet.setdefault('graphic_detail_views', []).append({
        'block_id': bid, 'first_image_index_zero_based': len(base_paths),
        'tile_count': len(tiles), 'grid_columns': columns, 'grid_rows': rows,
        'same_source_block': True, 'semantic_verified': False})
    messages = deepcopy(request.messages)
    messages[-1]['content'] = json.dumps(next_packet, ensure_ascii=False)
    if sum(len(m['content']) for m in messages) > 120_000:
        return None, {'status': 'graphic_message_budget', 'requested_block_ids': pending}
    new_binding = deepcopy(binding)
    new_binding.setdefault('detail_images', []).extend(
        {k: tile[k] for k in ('block_id', 'path', 'sha256')} for tile in tiles)
    # A model may request both a close-up and another document in one response.
    # Satisfy both within one revision, rather than spending the entire revision
    # on graphics and losing the textual query when the next answer changes.
    from .object_operation_continuation import prepare_object_operation_continuation
    from ..findings_verify.source_review_evidence import evidence_digest
    from .object_operation_followup import _bound_attachment
    candidate_packet = deepcopy(next_packet)
    candidate_packet['context_revision'] = packet.get('context_revision', 0)
    candidate_messages = deepcopy(messages)
    candidate_messages[-1]['content'] = json.dumps(candidate_packet, ensure_ascii=False)
    candidate_wire = {'messages':candidate_messages, 'image_paths':paths}
    attachments = [_bound_attachment(new_binding, image_id, path)
                   for image_id,path in zip(candidate_packet['attached_image_ids_in_order'],paths,strict=True)]
    prepared = prepare_object_operation_continuation(
        candidate_wire, payload, sources, new_binding['images'],
        Path(new_binding['md_path']).read_text(),
        expected_request_sha256=evidence_digest(candidate_wire),
        expected_md_sha256=new_binding['md_sha256'],
        original_image_hashes={image_id:new_binding['images'][image_id]['sha256']
                               for image_id in candidate_packet['attached_image_ids_in_order']},
        original_image_bindings=attachments, graph=new_binding['graph'])
    if prepared['status'] == 'prepared':
        messages = prepared['request']['messages']
        paths = prepared['request']['image_paths']
        next_packet = json.loads(messages[-1]['content'])
    text_receipt = {k:v for k,v in prepared.items() if k not in ('request','images','searches')}
    root = re.sub(r'_context_\d+$', '', request.stage)
    stage = f"{root}_context_{next_packet['context_revision']}"
    followup = CodexTargetedPass(stage, '03_' + stage + '.json', messages,
                                tuple(paths), continuation_source=new_binding)
    error = verify_operation_source_binding(followup)
    if error:
        raise ValueError(error)
    from .graphic_detail_catalogue import publish_graphic_details
    catalogue = publish_graphic_details(new_binding, manifest)
    return followup, {'status': 'prepared', 'route': 'graphic_detail', 'next_stage': stage,
                      'block_id': bid, 'tiles_added': len(tiles), 'supplemental_images_retired': retired,
                      'deferred_block_ids': pending[1:], 'manifest_path': str(run / 'tiles/manifest.json'),
                      'semantic_verified': False, 'absence_proven': False,
                      'simultaneous_context_retrieval': text_receipt, 'review_detail_catalogue': catalogue}
