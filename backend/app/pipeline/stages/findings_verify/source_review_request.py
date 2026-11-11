"""Build one review request locally; no provider dispatch or automatic retries."""
from .source_review_metadata import graphic_stamp_metadata
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def prepare_source_review_request(queue, finding, md, *, max_images=8):
    from .source_review_bundle import verify_source_review_bundle
    verify_source_review_bundle(queue)
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
    metadata = packet.get('graphic_metadata', [])
    expected_metadata = graphic_stamp_metadata(md, packet.get('required_graphic_block_ids', []))
    if metadata and metadata != expected_metadata:
        raise ValueError('packet_graphic_metadata_changed')
    images, omitted = [], []
    receipts = packet.get('graphic_attachments', [])
    for bid in packet.get('required_graphic_block_ids', []):
        selected = [r for r in receipts if r.get('block_id') == bid]
        if bid not in graphic_ids or not selected:
            omitted.append({'block_id': bid, 'reason': 'graphic_receipt_unavailable'})
            continue
        seen_hashes = set()
        for receipt in selected:
            if receipt.get('sha256') in seen_hashes:
                raise ValueError('ambiguous_graphic_receipt')
            seen_hashes.add(receipt.get('sha256'))
            if len(images) >= max_images:
                omitted.append({'block_id':bid,'sha256':receipt.get('sha256'),'reason':'image_limit'}
                               if len(selected)>1 else {'block_id':bid,'reason':'image_limit'})
                continue
            try:
                actual = hashlib.sha256(Path(receipt['path']).read_bytes()).hexdigest()
            except (OSError, KeyError, TypeError):
                actual = None
            if actual is None or actual != receipt.get('sha256'):
                raise ValueError('graphic_version_changed')
            image = {k:receipt[k] for k in ('block_id','path','sha256','width','height')}
            image.update({k:receipt[k] for k in ('view_kind','page_index','clip_rotated_page_points','source_pdf_sha256') if k in receipt})
            images.append(image)
    # Explicit allow-list: no historical decisions, detector reviews or control answers.
    claims = [{k: r.get(k) for k in ('source_finding_id', 'source_claim', 'block_ids')}
              for r in finding.get('source_review_requests', []) if isinstance(r, dict)]
    payload = {'finding_id': finding['id'], 'finding_sha256': finding_hash, 'source_md_sha256': md_hash,
               'context_sha256': packet.get('context_sha256'),
               'final_claim': {k: finding.get(k) for k in ('problem', 'description', 'solution')},
               'display_fields': {k: finding.get(k) for k in ('risk', 'consequence', 'severity', 'norm', 'norm_quote') if finding.get(k) not in (None, '')},
               'finding_purpose': {k:finding.get(k) for k in ('report_section','question_basis','profile_reason','engineer_action_required')},
               'upstream_operation_contexts': packet.get('upstream_operation_contexts', []),
               'source_catalogue': queue.get('source_catalogue', []),
               'source_origins': {bid: origin for bid, origin in queue.get('source_origins', {}).items()
                                  if bid in {s['block_id'] for s in packet['sources']} | {im['block_id'] for im in images}},
               'original_claims': claims, 'authoritative_text': packet['sources'],
               'graphic_metadata': metadata,
               'images_in_attachment_order': [{k: v for k, v in im.items() if k != 'path'} for im in images],
               'unavailable_images': omitted,
               'context_search_boundary': {k: packet.get('context_search_receipt', {}).get(k) for k in ('query_hit_counts', 'deferred_queries', 'backlog', 'absence_proven')},
               'missing_block_ids': packet.get('missing_or_ambiguous_block_ids', []),
               'budgeted_text_block_ids': packet.get('budgeted_block_ids', [])}
    instruction = '''Review only the supplied claims, authoritative TEXT and attached images.
Do not access files, tools, web, historical reviews or evaluation answers.
Separate each substantive claim and preserve uncertainty.
When source_catalogue is present, source_origins maps each scoped block ID to its
original document and version. Cite the scoped IDs. Keep document versions and
physical executions separate; shared names or marks do not prove applicability.
Preserve the finding's purpose and modality. A question about an actionable source
ambiguity is not an assertion of a proven physical defect. Check the factual
premises, conditional calculation, decision affected, and remaining uncertainty.
Upstream context records show supplied sources and reported inspections, not
verified conclusions. Recheck sources independently. Before proposing internal
retrieval again, identify a concrete new source or operation, not merely repeat
work recorded upstream. Missing bounded context alone never justifies a question. Include substantive
assertions from display_fields (risk, consequence, severity and normative text)
in your review; these fields are not established facts or verified requirements. A source address or
matching quote does not prove the claim. Do not correct TEXT using raster.
For tables preserve the header-to-cell mapping, including blank cells. A number
in another column is not a literal quantity. Distinguish directly stated counts
from derived counts: a derived count needs an explicit source-backed relationship
applicable to the same object, execution and scope. A repeated ratio in other
assemblies is not by itself that relationship. If a quantity cell is blank and
that relationship is unverified, keep the count and dependent totals conditional
or needs_data; never silently move the number, substitute zero or certify a total.
For arithmetic enumerate source rows and reproduce the operation. Attach exact
evidence quotes for the operand rows AND the headers, object membership and
multiplicities needed to connect those operands to the stated scope. A component
row alone does not establish how many assemblies contain it. For conditional
counts cite the blank quantity cells with their column context; do not cite only
an unrelated material row from the same block. Preserve these dependency links
in claim evidence so final report citations can be independently followed.
Distinguish annotation conflicts from physical defects and operating-mode assumptions.
Return JSON: finding_id, finding_sha256, source_md_sha256, context_sha256, claims.
Each claim: claim_id, claim, status (supported/refuted/needs_data), reason, evidence.
TEXT evidence: {type:"text", block_id, quote}, an exact quote from supplied text.
STAMP evidence: {type:"metadata", block_id, quote}, an exact quote from supplied graphic_metadata.
These are literal prepared-MD stamps for document/sheet identity only, not proof of drawing contents.
IMAGE evidence: {type:"image", block_id, image_sha256, bbox_norm_xyxy:[left,top,right,bottom], observation}.
Regions use normalized coordinates 0..1 of the cited attached image, including detail crops.
Several images can share a source block: distinguish them by image_sha256. Detail
crops supplement one original graphic and are not additional physical objects.
Cite only images actually attached.
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
