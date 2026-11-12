"""Validate review receipts, never equate matching quotes with semantic truth."""
from .source_review_metadata import graphic_stamp_metadata, text_stamp_metadata
import hashlib
import json
import math
import re
from collections import Counter
from copy import deepcopy
from pathlib import Path

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def validate_source_review(response, queue, findings, md):
    """Bind an untrusted review to current inputs; do not mutate findings.

    Acceptance means provenance/shape checks passed, not that claim coverage,
    reasoning or physical interpretation are correct. No automatic suppression.
    """
    result = {'status': 'unusable', 'errors': [], 'raw_response': deepcopy(response),
              'semantic_verified': False, 'claim_coverage_verified': False,
              'automatic_application': False}
    errors = result['errors']
    if not isinstance(response, dict) or not isinstance(queue, dict):
        errors.append('invalid_review_or_queue')
        return result
    from .source_review_bundle import verify_source_review_bundle
    try:
        verify_source_review_bundle(queue)
    except (ValueError, OSError, KeyError, TypeError):
        errors.append('stale_or_invalid_source_bundle')
        return result
    fid = response.get('finding_id')
    if not isinstance(fid, str) or not fid.strip():
        errors.append('invalid_finding_id')
        return result
    candidates = [f for f in findings if isinstance(f, dict) and f.get('id') == fid] if isinstance(findings, list) else []
    packets = queue.get('packets')
    matches = [p for p in packets if isinstance(p, dict) and p.get('finding_id') == fid] if isinstance(packets, list) else []
    if len(candidates) != 1 or len(matches) != 1:
        errors.append('missing_or_ambiguous_finding_or_packet')
        return result
    packet = matches[0]
    if packet.get('context_sha256') and response.get('context_sha256') != packet['context_sha256']:
        errors.append('stale_review_context')
    digest = hashlib.sha256(json.dumps(candidates[0], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    if response.get('finding_sha256') != digest or packet.get('finding_sha256') != digest:
        errors.append('stale_finding')
    md_digest = hashlib.sha256(md.encode()).hexdigest()
    if response.get('source_md_sha256') != md_digest or queue.get('source_md_sha256') != md_digest:
        errors.append('stale_source')
    if packet.get('status') != 'pending' or candidates[0].get('source_evidence_review_status') != 'pending':
        errors.append('review_not_pending')
    blocks = authoritative_text_blocks(md)
    headers = re.findall(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I)
    header_counts = Counter(bid for _, bid in headers)
    graphic_ids = {bid for kind, bid in headers if kind.upper() in ('IMAGE', 'GRAPHIC') and header_counts[bid] == 1}
    graphics = packet.get('graphic_attachments', [])
    graphics = graphics if isinstance(graphics, list) else []
    sources = packet.get('sources')
    allowed = set()
    if not isinstance(sources, list):
        errors.append('invalid_packet_sources')
    else:
        for source in sources:
            bid = source.get('block_id') if isinstance(source, dict) else None
            if not isinstance(bid, str) or bid not in blocks or source.get('authoritative_text') != blocks[bid]:
                errors.append('packet_source_mismatch')
            else:
                allowed.add(bid)
    metadata = packet.get('graphic_metadata', [])
    expected_metadata = graphic_stamp_metadata(md, packet.get('required_graphic_block_ids', []))
    if metadata != expected_metadata:
        errors.append('packet_graphic_metadata_mismatch')
    metadata_allowed = {r['block_id']: r['stamp'] for r in expected_metadata if r in metadata}
    # TEXT packets already include their literal Stamp lines. Accept only those
    # exact stamps in verified, supplied text blocks, never arbitrary MD content.
    metadata_allowed.update({r['block_id']: r['stamp'] for r in text_stamp_metadata(md, allowed)})
    claims = response.get('claims')
    if not isinstance(claims, list) or not claims:
        errors.append('missing_claims')
        return result
    ids = set()
    for index, claim in enumerate(claims):
        prefix = f'claim_{index}:'
        if not isinstance(claim, dict):
            errors.append(prefix+'invalid_claim')
            continue
        cid = claim.get('claim_id')
        if not isinstance(cid, str) or not cid.strip() or cid in ids:
            errors.append(prefix+'missing_or_duplicate_claim_id')
        else:
            ids.add(cid)
        for key in ('claim', 'reason'):
            if not isinstance(claim.get(key), str) or not claim[key].strip():
                errors.append(prefix+'missing_'+key)
        verdict = claim.get('status')
        if verdict not in ('supported', 'refuted', 'needs_data'):
            errors.append(prefix+'invalid_status')
        evidence = claim.get('evidence', [])
        if not isinstance(evidence, list):
            errors.append(prefix+'invalid_evidence')
            continue
        if verdict in ('supported', 'refuted') and not evidence:
            errors.append(prefix+'missing_evidence')
        if verdict == 'needs_data':
            missing = claim.get('missing_data')
            if not isinstance(missing, list) or not missing or any(not isinstance(v, str) or not v.strip() for v in missing):
                errors.append(prefix+'missing_data_not_specified')
        for item in evidence:
            bid = item.get('block_id') if isinstance(item, dict) else None
            quote = item.get('quote') if isinstance(item, dict) else None
            kind = item.get('type', 'text') if isinstance(item, dict) else None
            if kind == 'metadata':
                if not isinstance(bid, str) or bid not in metadata_allowed:
                    errors.append(prefix+'metadata_not_in_packet')
                elif not isinstance(quote, str) or not quote.strip() or quote not in metadata_allowed[bid]:
                    errors.append(prefix+'quote_not_in_metadata')
                continue
            if kind == 'image':
                refs = [g for g in graphics if isinstance(g, dict) and g.get('block_id') == bid]
                if len(refs) > 1:
                    refs = [g for g in refs if g.get('sha256') == item.get('image_sha256')]
                if not isinstance(bid, str) or bid not in graphic_ids or len(refs) != 1 or refs[0].get('attached_to_model') is not True:
                    errors.append(prefix+'graphic_not_supplied')
                    continue
                ref = refs[0]
                try:
                    actual_hash = hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()
                except (OSError, TypeError, KeyError, ValueError):
                    actual_hash = None
                if actual_hash is None or actual_hash != ref.get('sha256') or item.get('image_sha256') != actual_hash:
                    errors.append(prefix+'graphic_version_mismatch')
                bbox = item.get('bbox_norm_xyxy')
                valid_box = (isinstance(bbox, list) and len(bbox) == 4
                             and all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and 0 <= v <= 1 for v in bbox))
                if not valid_box or not (bbox[0] < bbox[2] and bbox[1] < bbox[3]):
                    errors.append(prefix+'invalid_graphic_region')
                if not isinstance(item.get('observation'), str) or not item['observation'].strip():
                    errors.append(prefix+'missing_graphic_observation')
                continue
            if kind != 'text':
                errors.append(prefix+'unsupported_evidence_type')
                continue
            if not isinstance(bid, str) or bid not in allowed:
                errors.append(prefix+'source_not_in_packet')
            elif not isinstance(quote, str) or not quote.strip() or quote not in blocks[bid]:
                errors.append(prefix+'quote_not_in_source')
    if not errors:
        result['status'] = 'recorded_pending_semantic_acceptance'
        result['finding_id'] = fid
        result['claims'] = deepcopy(claims)
    return result
