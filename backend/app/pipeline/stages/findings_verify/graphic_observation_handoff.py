"""Opt-in handoff of sealed source-only observations, never semantic verdicts."""
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path

SOURCE_ONLY_INSTRUCTIONS = 'Inspect only the attached engineering drawing. No tools or external sources. No audit finding or expected answer is supplied. Read each depicted heating section and the local legend independently. Describe the actual strokes inside its boundary, and trace strokes crossing the boundary to their visible endpoints. Distinguish device symbols from sensor connections or process piping; do not assume a shape is part of a symbol merely because it overlaps it. Compare each working symbol with every relevant legend example and any counterpart elsewhere on this image. Use observed shape and connectivity, not expectations from the equipment type. Preserve ambiguity where a line cannot be traced. Return JSON with sections (list of objects: system_literal, location_bbox_norm_xyxy, internal_strokes, connected_lines with endpoint_literal and visible_path, legend_comparison with similarities and differences, uncertainties), legend_entries (literal label, location_bbox_norm_xyxy, observed_shape), limitations (list). Coordinates in [0,1]. No defect verdict or correction prescription. Do not infer installation, compliance or universal symbol conventions. Russian descriptions; printed labels transcribed literally.'
SOURCE_ONLY_USER = 'Read the heating sections and legend in the supplied image.'
HANDOFF_INSTRUCTIONS = ('\nSource-only graphic observations are supplied separately from authoritative TEXT. '
    'They were generated without the candidate wording and are untrusted model observations, not prior '
    'reviewer verdicts or expected answers. Independently compare them with the attached image, trace '
    'connected lines and compare local legend symbols. Resolve disagreements explicitly; do not accept '
    'an observation merely because it was supplied. Unresolved geometry remains needs_data.\n')


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def bind_observation(request_path, response_path, block_id):
    request = json.loads(Path(request_path).read_text())
    response = json.loads(Path(response_path).read_text())
    if request.get('messages') != [
            {'role': 'system', 'content': SOURCE_ONLY_INSTRUCTIONS},
            {'role': 'user', 'content': SOURCE_ONLY_USER}]:
        raise ValueError('observation_request_not_source_only')
    if request.get('allowed_tools') != '' or request.get('model') != 'codex/gpt-6-astra':
        raise ValueError('observation_executor_mismatch')
    images = request.get('image_hashes')
    if not isinstance(images, dict) or len(images) != 1:
        raise ValueError('observation_image_ambiguous')
    path, digest = next(iter(images.items()))
    if _sha(path) != digest:
        raise ValueError('observation_image_changed')
    reading = response.get('json_data')
    if response.get('is_error') or not isinstance(reading, dict) or set(reading) != {
            'sections', 'legend_entries', 'limitations'}:
        raise ValueError('invalid_graphic_observation')
    if not isinstance(block_id, str) or not block_id:
        raise ValueError('missing_observation_block')
    for key in ('sections', 'legend_entries'):
        if not isinstance(reading[key], list):
            raise ValueError('invalid_graphic_observation')
        for row in reading[key]:
            box = row.get('location_bbox_norm_xyxy') if isinstance(row, dict) else None
            if (not isinstance(box, list) or len(box) != 4
                    or any(type(v) not in (float, int) or not math.isfinite(v) or not 0 <= v <= 1 for v in box)
                    or not box[0] < box[2] or not box[1] < box[3]):
                raise ValueError('invalid_observation_region')
    if not isinstance(reading['limitations'], list):
        raise ValueError('invalid_graphic_observation')
    return {'block_id': block_id, 'image_sha256': digest,
            'request_path': str(request_path), 'request_sha256': _sha(request_path),
            'response_path': str(response_path), 'response_sha256': _sha(response_path),
            'reading': deepcopy(reading), 'semantic_verified': False}


def checked_observations(records, attached_images):
    """Reject stale/different images and altered packets before a model call."""
    if not isinstance(records, list):
        raise ValueError('invalid_observation_records')
    supplied = {(im['block_id'], im['sha256']) for im in attached_images}
    seen, result = set(), []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError('invalid_observation_record')
        rebuilt = bind_observation(record['request_path'], record['response_path'], record['block_id'])
        if rebuilt != record:
            raise ValueError('observation_record_changed')
        key = (record['block_id'], record['image_sha256'])
        if key not in supplied or key in seen:
            raise ValueError('observation_image_not_supplied_or_duplicate')
        seen.add(key)
        result.append({k: deepcopy(record[k]) for k in
                       ('block_id', 'image_sha256', 'request_sha256', 'response_sha256', 'reading', 'semantic_verified')})
    return result
