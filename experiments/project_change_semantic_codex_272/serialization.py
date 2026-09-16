"""One UTF-8 byte representation for both request freeze and transmission."""
import hashlib
import json

from experiments.project_change_272.inventory import sha

# Local transport guard, capacity-audited for frozen Pair B; not a model limit.
MAX_TEXT_CHARACTERS = 83000


def canonical_json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def request_sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def prepare_request_bytes(system, data, packet=None):
    # Retain the existing text/image budgets and image eligibility rules.
    if len(system) + len(json.dumps(data, ensure_ascii=False)) > MAX_TEXT_CHARACTERS:
        raise ValueError(f'Request exceeds {MAX_TEXT_CHARACTERS} text characters')
    images = []
    for side in ('old', 'new'):
        for evidence in (packet or {}).get('evidence', {}).get(side, []):
            if evidence.get('source_kind') == 'PDF_RASTER_CROP':
                raster = evidence['raster']
                if sha(raster['path']) != raster['sha256']:
                    raise ValueError('Raster drift')
                images.append(dict(evidence_id=evidence['evidence_id'], side=side,
                                   page=evidence['page'], bbox=evidence['bbox'], **raster))
    if len(images) > 8:
        raise ValueError('Too many images')
    labels = [dict(image_index=index + 1, file='image_%02d.png' % index,
                   evidence_id=image['evidence_id'], side=image['side'],
                   page=image['page'], bbox=image['bbox'])
              for index, image in enumerate(images)]
    payload = (system.encode('utf-8')
               + b'\n\nProcess only the supplied evidence. Return the final JSON directly.\n'
               + b'Attached images are ordered as this source manifest:\n'
               + canonical_json_bytes(labels)
               + b'\n\nUNTRUSTED PACKET DATA:\n'
               + canonical_json_bytes(data))
    return payload, images
