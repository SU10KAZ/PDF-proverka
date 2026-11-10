"""Build one review request locally; no provider dispatch or automatic retries."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def prepare_source_review_request(queue, finding, md, *, max_images=8):
    md_hash = hashlib.sha256(md.encode()).hexdigest()
    if md_hash != queue.get('source_md_sha256'):
        raise ValueError('source_version_changed')
    matches = [p for p in queue.get('packets', []) if p.get('finding_id') == finding.get('id')]
    if len(matches) != 1 or matches[0].get('status') != 'pending':
        raise ValueError('missing_or_ambiguous_pending_packet')
    packet = matches[0]
    finding_hash = hashlib.sha256(json.dumps(finding, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    if finding_hash != packet.get('finding_sha256'):
        raise ValueError('finding_version_changed')
    blocks = authoritative_text_blocks(md)
    for source in packet['sources']:
        if source.get('block_id') not in blocks or source.get('authoritative_text') != blocks[source['block_id']]:
            raise ValueError('packet_text_changed')
    headers = re.findall(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I)
    counts = Counter(bid for _, bid in headers)
    graphic_ids = {bid for kind, bid in headers if kind.upper() in ('IMAGE', 'GRAPHIC') and counts[bid] == 1}
    images, omitted = [], []
    receipts = packet.get('graphic_attachments', [])
    for bid in packet.get('required_graphic_block_ids', []):
        selected = [r for r in receipts if r.get('block_id') == bid]
        if bid not in graphic_ids or len(selected) != 1:
            omitted.append({'block_id': bid, 'reason': 'graphic_receipt_unavailable'})
            continue
        if len(images) >= max_images:
            omitted.append({'block_id': bid, 'reason': 'image_limit'})
            continue
        receipt = selected[0]
        try:
            actual = hashlib.sha256(Path(receipt['path']).read_bytes()).hexdigest()
        except (OSError, KeyError, TypeError):
            actual = None
        if actual is None or actual != receipt.get('sha256'):
            raise ValueError('graphic_version_changed')
        images.append({k: receipt[k] for k in ('block_id', 'path', 'sha256', 'width', 'height')})
    # Explicit allow-list: no historical decisions, detector reviews or control answers.
    claims = [{k: r.get(k) for k in ('source_finding_id', 'source_claim', 'block_ids')}
              for r in finding.get('source_review_requests', []) if isinstance(r, dict)]
    payload = {'finding_id': finding['id'], 'finding_sha256': finding_hash, 'source_md_sha256': md_hash,
               'context_sha256': packet.get('context_sha256'),
               'final_claim': {k: finding.get(k) for k in ('problem', 'description', 'solution')},
               'display_fields': {k: finding.get(k) for k in ('risk', 'consequence', 'severity', 'norm', 'norm_quote') if finding.get(k) not in (None, '')},
               'original_claims': claims, 'authoritative_text': packet['sources'],
               'images_in_attachment_order': [{k: v for k, v in im.items() if k != 'path'} for im in images],
               'unavailable_images': omitted,
               'missing_block_ids': packet.get('missing_or_ambiguous_block_ids', []),
               'budgeted_text_block_ids': packet.get('budgeted_block_ids', [])}
    instruction = '''Review only the supplied claims, authoritative TEXT and attached images.
Do not access files, tools, web, historical reviews or evaluation answers.
Separate each substantive claim and preserve uncertainty. Include substantive
assertions from display_fields (risk, consequence, severity and normative text)
in your review; these fields are not established facts or verified requirements. A source address or
matching quote does not prove the claim. Do not correct TEXT using raster.
For arithmetic enumerate source rows and reproduce the operation. Distinguish
annotation conflicts from physical defects and operating-mode assumptions.
Return JSON: finding_id, finding_sha256, source_md_sha256, context_sha256, claims.
Each claim: claim_id, claim, status (supported/refuted/needs_data), reason, evidence.
TEXT evidence: {type:"text", block_id, quote}, an exact quote from supplied text.
IMAGE evidence: {type:"image", block_id, image_sha256, bbox_norm_xyxy:[left,top,right,bottom], observation}.
Regions use normalized coordinates 0..1. Cite only images actually attached.
For needs_data include missing_data (nonempty list) and optionally context_queries
(specific object labels or literal phrases to search in prepared MD). Missing or
bounded context never proves whole-project absence. Do not rewrite the finding.
'''
    messages = [{'role': 'system', 'content': instruction},
                {'role': 'user', 'content': json.dumps(payload, ensure_ascii=False)}]
    fingerprint = hashlib.sha256(json.dumps({'messages': messages, 'images': images}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    return {'messages': messages, 'image_paths': [im['path'] for im in images], 'image_bindings': images,
            'request_sha256': fingerprint, 'sent': False, 'automatic_dispatch': False,
            'boundary': 'Prepared input only; no model request or isolation guarantee.'}
