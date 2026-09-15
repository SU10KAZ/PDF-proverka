"""Mechanical locator binding. Passing is not a semantic event verdict."""
import math
from pathlib import Path

import fitz

from experiments.project_change_272.inventory import sha


def normalized_box_valid(box):
    return (isinstance(box, (list, tuple)) and len(box) == 4
            and all(isinstance(x, (int, float)) and not isinstance(x, bool)
                    and math.isfinite(x) and 0 <= x <= 1 for x in box)
            and box[0] < box[2] and box[1] < box[3])


def raster_locator_errors(witness, evidence):
    """bbox_norm is relative to the exact raster attached to evidence_id."""
    errors = []
    if witness.get('evidence_id') != evidence.get('evidence_id'):
        errors.append('RASTER_BINDING_MISMATCH')
    for field in ('visual_locator', 'binding_reason'):
        if not isinstance(witness.get(field), str) or not witness[field].strip():
            errors.append('INVALID_' + field.upper())
    if witness.get('literal_quote') or witness.get('quote'):
        errors.append('MIXED_LITERAL_AND_VISUAL_WITNESS')
    if not normalized_box_valid(witness.get('bbox_norm')):
        errors.append('INVALID_RASTER_BBOX')
    raster = evidence.get('raster')
    try:
        if not isinstance(raster, dict) or not raster.get('path') or not raster.get('sha256'):
            raise ValueError('Missing raster receipt')
        path = Path(raster['path'])
        if not path.is_file() or sha(path) != raster['sha256']:
            raise ValueError('Raster missing or changed')
        pixmap = fitz.Pixmap(str(path))
        if pixmap.width <= 0 or pixmap.height <= 0:
            raise ValueError('Empty image')
        for dimension in ('width', 'height'):
            if dimension in raster and raster[dimension] != getattr(pixmap, dimension):
                raise ValueError('Raster dimension drift')
    except (ValueError, TypeError, OSError, RuntimeError):
        errors.append('MISSING_OR_INVALID_RASTER')
    return sorted(set(errors))
