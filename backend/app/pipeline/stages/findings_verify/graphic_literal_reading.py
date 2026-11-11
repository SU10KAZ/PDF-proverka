"""Source-only graphic readings, kept as untrusted observations, never TEXT."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from .source_review_evidence import evidence_digest


def prepare_literal_reading(source_request, image_sha256):
    images = [im for im in source_request['image_bindings'] if im['sha256'] == image_sha256]
    if len(images) != 1:
        raise ValueError('literal_image_missing_or_ambiguous')
    image = images[0]
    if image['path'] not in source_request['image_paths']:
        raise ValueError('literal_image_not_attached')
    if hashlib.sha256(Path(image['path']).read_bytes()).hexdigest() != image_sha256:
        raise ValueError('literal_image_changed')
    # Do not pass the candidate, its numbers, review history or expected answer.
    instruction = ('Read only the attached drawing image. No tools, files, web or engineering inference. '
        'Transcribe visible annotations literally, preserving printed digits, marks and units. '
        'Do not guess unreadable characters. Return JSON with annotations (list of objects, each '
        'with literal, bbox_norm_xyxy [left,top,right,bottom] in 0..1), and reading_uncertainties '
        '(list of nonempty strings). Empty annotations are allowed if nothing is legible. '
        'This is literal graphic reading, not a defect verdict.')
    return {'messages': [{'role': 'system', 'content': instruction},
                         {'role': 'user', 'content': 'Transcribe the supplied image.'}],
            'image_paths': [image['path']], 'image_bindings': [deepcopy(image)]}


def bind_literal_reading(request, response):
    image, = request['image_bindings']
    if hashlib.sha256(Path(image['path']).read_bytes()).hexdigest() != image['sha256']:
        raise ValueError('literal_image_changed')
    if not isinstance(response, dict) or set(response) != {'annotations', 'reading_uncertainties'}:
        raise ValueError('invalid_literal_reading')
    rows, uncertainties = response['annotations'], response['reading_uncertainties']
    if not isinstance(rows, list) or len(rows) > 200 or not isinstance(uncertainties, list):
        raise ValueError('invalid_literal_reading')
    if any(not isinstance(s, str) or not s.strip() for s in uncertainties):
        raise ValueError('invalid_literal_uncertainty')
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'literal', 'bbox_norm_xyxy'}:
            raise ValueError('invalid_literal_annotation')
        if not isinstance(row['literal'], str) or not row['literal'].strip():
            raise ValueError('invalid_literal_annotation')
        box = row['bbox_norm_xyxy']
        if (not isinstance(box, list) or len(box) != 4
            or any(type(v) not in (int, float) or not 0 <= v <= 1 for v in box)
            or not box[0] < box[2] or not box[1] < box[3]):
            raise ValueError('invalid_literal_region')
    return {'block_id': image['block_id'], 'image_sha256': image['sha256'],
            'request_sha256': evidence_digest(request), 'response_sha256': evidence_digest(response),
            'reading': deepcopy(response), 'semantic_verified': False,
            'boundary': 'Source-only model observation. Recheck the attached image; never authoritative TEXT.'}


def attach_literal_readings(source_request, readings):
    result = deepcopy(source_request)
    supplied = {(im['block_id'], im['sha256']) for im in result['image_bindings']}
    selected = [r for r in readings if (r['block_id'], r['image_sha256']) in supplied]
    if not selected:
        return result
    payload = json.loads(result['messages'][1]['content'])
    payload['independent_graphic_readings_untrusted'] = selected
    result['messages'][1]['content'] = json.dumps(payload, ensure_ascii=False)
    result['messages'][0]['content'] += ('\nIndependent graphic readings were made without the candidate wording. '
        'They are untrusted observations, not authoritative text. Compare their literal digits and marks '
        'against the attached image and the candidate. Explicitly resolve any disagreement using image '
        'evidence; recompute affected arithmetic. If the source remains ambiguous, preserve needs_data.\n')
    result['request_sha256'] = evidence_digest({'messages': result['messages'], 'images': result['image_bindings']})
    return result
