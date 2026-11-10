"""Prepare bounded semantic-review inputs without treating provenance as proof."""
import hashlib
import json
import re
from .source_review_metadata import graphic_stamp_metadata
from collections import Counter
from copy import deepcopy

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks
from backend.app.pipeline.stages.findings_review.deterministic_critic import _referenced_block_ids


def attach_pending_source_reviews(findings, known_block_ids):
    """Queue lineage relevance independently of direct-reference structural checks.

    Called after the corrector, so page repairs and phantom-reference cleanup
    still run. This adds review metadata only, never rewrites a claim or severity.
    """
    changed = 0
    for finding in findings if isinstance(findings, list) else []:
        if not isinstance(finding, dict):
            continue
        lineage = finding.get('source_candidate_lineage', [])
        operation_lineage = finding.get('operation_candidate_lineage', [])
        if isinstance(lineage,list) and isinstance(operation_lineage,list):
            lineage = lineage + operation_lineage
        source_ids = finding.get('source_finding_ids', [])
        if not isinstance(lineage, list) or not isinstance(source_ids, list):
            continue
        requests = []
        for receipt in lineage:
            if not isinstance(receipt, dict) or receipt.get('status') != 'requires_semantic_review':
                continue
            sid = receipt.get('source_finding_id')
            if not isinstance(sid, str) or sid not in source_ids:
                continue
            refs = []
            for key in ('existing_block_ids', 'unresolved_block_ids'):
                values = receipt.get(key, [])
                if isinstance(values, list):
                    refs.extend(b for b in values if isinstance(b, str) and b)
            refs = list(dict.fromkeys(refs))
            if refs:
                request = {'source_finding_id': sid, 'source_claim': receipt.get('source_claim'), 'block_ids': refs}
                missing = [bid for bid in refs if bid not in known_block_ids]
                if missing:
                    request['unresolved_index_block_ids'] = missing
                requests.append(request)
        if not requests:
            continue
        updates = {'requires_human_review': True, 'source_evidence_review_status': 'pending',
                   'source_review_requests': requests}
        if any(finding.get(k) != v for k, v in updates.items()):
            finding.update(updates)
            changed += 1
    return changed


def build_source_review_queue(findings, md, *, max_chars=100000):
    findings = findings if isinstance(findings, list) else []
    counts = Counter(f.get('id') for f in findings if isinstance(f, dict) and isinstance(f.get('id'), str))
    blocks = authoritative_text_blocks(md)
    headers = re.findall(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I)
    header_counts = Counter(bid for _, bid in headers)
    graphic_ids = {bid for kind, bid in headers if kind.upper() in ('IMAGE', 'GRAPHIC') and header_counts[bid] == 1}
    packets = []
    for finding in findings:
        if not isinstance(finding, dict) or finding.get('source_evidence_review_status') != 'pending':
            continue
        fid = finding.get('id')
        packet = {'finding_id': fid, 'status': 'pending', 'semantic_verified': False,
                  'finding_snapshot': deepcopy(finding),
                  'finding_sha256': hashlib.sha256(json.dumps(finding,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),
                  'sources': [], 'unresolved_block_ids': [], 'budgeted_block_ids': [],
                  'required_graphic_block_ids': [], 'missing_or_ambiguous_block_ids': []}
        packets.append(packet)
        if not isinstance(fid, str) or not fid.strip() or counts[fid] != 1:
            packet.update(status='not_ready',reason='missing_or_ambiguous_finding_id')
            continue
        requests = finding.get('source_review_requests', [])
        refs = []
        for request in requests if isinstance(requests, list) else []:
            if isinstance(request, dict) and isinstance(request.get('block_ids'), list):
                refs.extend(r for r in request['block_ids'] if isinstance(r, str) and r)
        candidate_refs = set(refs)
        image_refs, text_refs, _ = _referenced_block_ids(finding)
        final_refs = set(image_refs + text_refs)
        refs.extend(image_refs + text_refs)
        packet['reference_origins'] = {
            ref: [label for label, group in (('source_candidate', candidate_refs), ('final_finding', final_refs)) if ref in group]
            for ref in dict.fromkeys(refs)
        }
        used = 0
        for ref in dict.fromkeys(refs):
            if ref not in blocks:
                packet['unresolved_block_ids'].append(ref)
                packet['required_graphic_block_ids' if ref in graphic_ids else 'missing_or_ambiguous_block_ids'].append(ref)
            elif used + len(blocks[ref]) > max_chars:
                packet['budgeted_block_ids'].append(ref)
            else:
                packet['sources'].append({'block_id':ref,'authoritative_text':blocks[ref]})
                used += len(blocks[ref])
        packet['graphic_metadata'] = graphic_stamp_metadata(md, packet['required_graphic_block_ids'])
        packet['source_context_status'] = ('available_bounded_text' if refs and not packet['unresolved_block_ids']
                                           and not packet['budgeted_block_ids'] else 'needs_data')
        packet['review_instruction'] = (
            'Compare original source claims in source_review_requests with the final finding and authoritative TEXT. '
            'Return supported/refuted/needs_data separately for each substantive claim, with exact evidence and reasoning. '
            'A valid source ID is not proof. For arithmetic enumerate source rows and reproduce the operation. '
            'Do not infer absence from bounded context or correct TEXT from raster. '
            'Graphics and missing linked documents require additional evidence; preserve uncertainty. '
            'For a graphic actually attached to this request, cite type=image, block_id, image_sha256, '
            'bbox_norm_xyxy=[left,top,right,bottom] in [0,1], and a concrete observation. '
            'A file listed as available but not attached is not visual evidence. '
            'Do not rewrite or remove the final finding in this review response.')
    return {'packets':packets,'pending_count':len(packets),'completed_count':0,
            'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),
            'automatic_dispatch':False,'semantic_verified':False}
