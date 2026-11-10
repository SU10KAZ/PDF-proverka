"""Prepare bounded semantic-review inputs without treating provenance as proof."""
import hashlib
import json
from collections import Counter
from copy import deepcopy

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


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
            refs = receipt.get('existing_block_ids', [])
            existing = list(dict.fromkeys(b for b in refs if isinstance(b, str) and b in known_block_ids)) if isinstance(refs, list) else []
            if existing:
                requests.append({'source_finding_id': sid, 'source_claim': receipt.get('source_claim'),
                                 'block_ids': existing})
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
    packets = []
    for finding in findings:
        if not isinstance(finding, dict) or finding.get('source_evidence_review_status') != 'pending':
            continue
        fid = finding.get('id')
        packet = {'finding_id': fid, 'status': 'pending', 'semantic_verified': False,
                  'finding_snapshot': deepcopy(finding),
                  'finding_sha256': hashlib.sha256(json.dumps(finding,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),
                  'sources': [], 'unresolved_block_ids': [], 'budgeted_block_ids': []}
        packets.append(packet)
        if not isinstance(fid, str) or not fid.strip() or counts[fid] != 1:
            packet.update(status='not_ready',reason='missing_or_ambiguous_finding_id')
            continue
        requests = finding.get('source_review_requests', [])
        refs = []
        for request in requests if isinstance(requests, list) else []:
            if isinstance(request, dict) and isinstance(request.get('block_ids'), list):
                refs.extend(r for r in request['block_ids'] if isinstance(r, str) and r)
        used = 0
        for ref in dict.fromkeys(refs):
            if ref not in blocks:
                packet['unresolved_block_ids'].append(ref)
            elif used + len(blocks[ref]) > max_chars:
                packet['budgeted_block_ids'].append(ref)
            else:
                packet['sources'].append({'block_id':ref,'authoritative_text':blocks[ref]})
                used += len(blocks[ref])
        packet['source_context_status'] = ('available_bounded_text' if refs and not packet['unresolved_block_ids']
                                           and not packet['budgeted_block_ids'] else 'needs_data')
        packet['review_instruction'] = (
            'Compare original source claims in source_review_requests with the final finding and authoritative TEXT. '
            'Return supported/refuted/needs_data separately for each substantive claim, with exact evidence and reasoning. '
            'A valid source ID is not proof. For arithmetic enumerate source rows and reproduce the operation. '
            'Do not infer absence from bounded context or correct TEXT from raster. '
            'Graphics and missing linked documents require additional evidence; preserve uncertainty. '
            'Do not rewrite or remove the final finding in this review response.')
    return {'packets':packets,'pending_count':len(packets),'completed_count':0,
            'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),
            'automatic_dispatch':False,'semantic_verified':False}
