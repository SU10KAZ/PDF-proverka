"""Prepare and validate a separate, source-bound check of a proposed report edit.

No dispatch or automatic inbox writes. A passing model receipt is an admission
candidate under an explicit execution policy, not certified engineering truth.
"""
from copy import deepcopy
import hashlib
import json
from .source_review_admission import apply_review_admissions
from .source_review_evidence import evidence_digest
from .source_review_request import prepare_source_review_request
from .source_review_results import validate_source_review
from .source_review_profile import OUTPUT_PROFILE

CHECKS = ('source_support', 'quantities_and_scope', 'original_clause_preservation',
          'uncertainty_and_consequences', 'profile_and_norms', 'peer_overlap', 'final_citations')
FIELDS = ('id','problem','description','solution','risk','consequence','severity','norm','norm_quote',
          'report_section','question_basis','profile_reason','sheet','page','evidence')


def prepare_final_check(entry, document, md):
    fid=entry['response']['finding_id']
    matches=[f for f in document['findings'] if f.get('id')==fid]
    if len(matches)!=1:raise ValueError('ambiguous_final_check_finding')
    original=matches[0]
    preview,receipts=apply_review_admissions(document,[entry],md)
    if receipts[0]['status']!='applied':
        raise ValueError('invalid_proposed_edit: '+receipts[0]['reason'])
    source=prepare_source_review_request(entry['queue'],original,md,max_images=16)
    packet=json.loads(source['messages'][1]['content'])
    proposed=next((f for f in preview['findings'] if f.get('id')==fid),None)
    binding={'entry_sha256':evidence_digest(entry),'document_sha256':evidence_digest(document),
             'source_md_sha256':hashlib.sha256(md.encode()).hexdigest()}
    # Prior reviewer conclusions and semantic verdicts are deliberately absent.
    visible=lambda f:{k:deepcopy(f[k]) for k in FIELDS if k in f}
    payload={'finding_id':fid,'binding_sha256':evidence_digest(binding),'action':entry['decision']['action'],
             'original':visible(original),'proposed':visible(proposed) if proposed else None,
             'current_report_peers':[visible(f) for f in document['findings'] if f.get('id')!=fid],
             'sources':{k:packet[k] for k in ('authoritative_text','images_in_attachment_order',
                 'graphic_metadata','unavailable_images','missing_block_ids','budgeted_text_block_ids',
                 'context_search_boundary','source_catalogue','source_origins')},
             'required_checks':list(CHECKS)}
    instruction='''Independently check this proposed audit-report edit against the authoritative TEXT and attached GRAPHIC images. No tools, web, history, engineer answers or prior reviewer verdicts are supplied. Peer findings are untrusted presentation context, not source evidence. Never correct TEXT from raster.
For each required check return check_id, status (pass/fail/needs_data), reason and evidence (source-bound text quotes or image observations with block_id, image_sha256 and bbox_norm_xyxy). Use type=text with exact quote, or type=image with observation and bbox_norm_xyxy. Every factual pass needs direct source evidence; pure presentation checks may have an empty evidence list with an explicit rationale. A matching quote is not proof of object identity or applicability.
Check all final visible fields, each original substantive clause and each source-dependent premise. Unknown totals must remain conditional, blank table cells cannot inherit adjacent-column numbers, and project-specific values cannot become universal rules. Preserve confirmed useful cores and actionable ambiguity. A suppression requires refutation of every original defect basis; deferral cannot remove a supported core. Source uncertainty does not itself warrant an engineer question. Unverified normative references must remain unverified.
For peer_overlap distinguish a cross-reference or distinct aggregate from a duplicate finding. Do not borrow a peer's independent defect to make the target seem valid. Verify final citations actually support the final retained clauses and identify stale or omitted location references. For final_citations, check the proposed evidence field, not just availability somewhere in the packet. If an edit removes all visible text, check whether removal is justified instead.
Return one JSON object: finding_id, binding_sha256 (copy), decision (release/hold), checks (one entry for each required check). Release only if every check passes; otherwise hold with specific reasons. Do not rewrite the finding or treat the proposed edit as an expected answer. This is a quality check, not a claim of whole-project completeness.'''
    instruction += '\n'+OUTPUT_PROFILE+'\n'
    request={'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],
             'image_paths':source['image_paths'],'image_bindings':source['image_bindings']}
    return {'request':request,'binding':binding,'finding_id':fid}


def validate_final_check(entry, document, md, result):
    prepared=prepare_final_check(entry,document,md)
    if (not isinstance(result,dict) or result.get('finding_id')!=prepared['finding_id']
            or result.get('binding_sha256')!=evidence_digest(prepared['binding'])):
        raise ValueError('stale_final_check')
    checks=result.get('checks')
    if not isinstance(checks,list) or len(checks)!=len(CHECKS):raise ValueError('incomplete_final_checks')
    seen=set();all_evidence=[]
    for check in checks:
        cid=check.get('check_id') if isinstance(check,dict) else None
        if cid not in CHECKS or cid in seen:raise ValueError('ambiguous_final_checks')
        seen.add(cid)
        if (check.get('status') not in ('pass','fail','needs_data')
                or not isinstance(check.get('reason'),str) or not check['reason'].strip()
                or not isinstance(check.get('evidence'),list)):
            raise ValueError('invalid_final_check_reason_or_evidence')
        if check['status']=='pass' and cid in ('source_support','quantities_and_scope') and not check['evidence']:
            raise ValueError('factual_final_check_without_evidence')
        all_evidence.extend(check['evidence'])
    supplied_images={(image['block_id'],image['sha256']) for image in prepared['request']['image_bindings']}
    for evidence in all_evidence:
        if isinstance(evidence,dict) and evidence.get('type')=='image':
            if (evidence.get('block_id'),evidence.get('image_sha256')) not in supplied_images:
                raise ValueError('final_check_image_not_supplied')
    if all_evidence:
        response={k:entry['response'][k] for k in ('finding_id','finding_sha256','source_md_sha256')}
        if 'context_sha256' in entry['response']:response['context_sha256']=entry['response']['context_sha256']
        response['claims']=[{'claim_id':'final-check-evidence','claim':'Sources cited by final checker',
            'status':'supported','reason':'Validate evidence binding, not factual conclusion','evidence':all_evidence}]
        check=validate_source_review(response,entry['queue'],document['findings'],md)
        if check['errors']:raise ValueError('invalid_final_check_sources: '+','.join(check['errors']))
    decision=result.get('decision')
    if decision not in ('release','hold'):raise ValueError('invalid_final_check_decision')
    passed=all(c['status']=='pass' for c in checks)
    if decision=='release' and not passed:raise ValueError('release_with_unresolved_final_check')
    return {'status':'release_candidate' if decision=='release' else 'held',
            'binding':prepared['binding'],'review':deepcopy(result),
            'semantic_certified':False,'original_document_mutated':False}
