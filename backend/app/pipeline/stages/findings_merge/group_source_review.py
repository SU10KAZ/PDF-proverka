"""Source-bound independent review of a presentation group; no implicit dispatch."""
from copy import deepcopy
import hashlib
import json
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_final_check import FIELDS
from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review
from backend.app.services.findings.report_routing import report_section_key

CHECKS=('shared_issue','corrective_action','individual_scopes','uncertainty_and_quantities','source_scope_and_identity')


def prepare_group_review(document,proposal,md,graphic_dir,*,max_images=12,extra_text_block_ids=()):
    findings=document['findings'];ids=[f['id'] for f in findings]
    requested=proposal.get('member_ids')
    if len(set(ids))!=len(ids) or not isinstance(requested,list) or len(requested)<2 or len(set(requested))!=len(requested) or any(i not in ids for i in requested):
        raise ValueError('invalid_or_ambiguous_group_members')
    members=[f for f in findings if f['id'] in requested]
    sections={report_section_key(f) for f in members}
    if len({f.get('severity') for f in members})!=1 or len(sections)!=1 or 'unclassified' in sections:
        raise ValueError('group_section_or_severity_not_compatible')
    refs=proposal.get('source_refs')
    evidenced={e.get('block_id') for f in members for e in f.get('evidence',[]) if isinstance(e,dict)}
    if not isinstance(refs,list) or not refs or any(not isinstance(r,str) or r not in evidenced for r in refs):
        raise ValueError('group_source_refs_not_member_evidence')
    if any(not isinstance(proposal.get(k),str) or not proposal[k].strip() for k in ('shared_issue','common_corrective_action')):
        raise ValueError('group_wording_required')
    from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks
    texts=authoritative_text_blocks(md)
    if not isinstance(extra_text_block_ids,(list,tuple)) or any(not isinstance(b,str) or b not in texts for b in extra_text_block_ids):
        raise ValueError('extra_group_context_requires_authoritative_text')
    extra_text_block_ids=list(dict.fromkeys(extra_text_block_ids))
    # Temporary packet identity only; never insert this aggregate into the report.
    synthetic={'id':'group-source-review','problem':proposal['shared_issue'],
        'description':'Proposed presentation grouping; individual propositions are supplied separately.',
        'solution':proposal['common_corrective_action'],'evidence':[deepcopy(e) for f in members for e in f.get('evidence',[])],
        'source_evidence_review_status':'pending','source_review_requests':[{'block_ids':list(dict.fromkeys(refs+extra_text_block_ids))}]}
    queue=build_source_review_queue([synthetic],md)
    attach_graphic_receipts(queue,md,graphic_dir)
    source=prepare_source_review_request(queue,synthetic,md,max_images=max_images)
    data=json.loads(source['messages'][1]['content'])
    binding={'proposal_sha256':evidence_digest(proposal),'members_sha256':evidence_digest(members),
        'document_sha256':evidence_digest(document),'source_md_sha256':hashlib.sha256(md.encode()).hexdigest()}
    if extra_text_block_ids:binding['extra_text_block_ids']=extra_text_block_ids
    payload={'binding':binding,'required_checks':list(CHECKS),
        'proposal':{k:proposal[k] for k in ('member_ids','shared_issue','common_corrective_action','source_refs')},
        'members':[{k:deepcopy(f[k]) for k in FIELDS if k in f} for f in members],
        'sources':{k:data[k] for k in ('authoritative_text','images_in_attachment_order','graphic_metadata',
            'unavailable_images','missing_block_ids','budgeted_text_block_ids','context_search_boundary','source_catalogue','source_origins')},
        'render_contract':'All individual original descriptions, problems, solutions, risks, norms and addresses remain visibly quoted in the group. Only the group heading and common action come from the proposal; all other findings remain unchanged.'}
    instruction='''Independently review this proposed presentation group against authoritative TEXT and attached GRAPHIC. No tools, web, historical answers or engineer decisions. The proposal and member findings are untrusted. Do not correct TEXT from raster. Review only grouping: it must not imply that every member assertion has become verified.
Determine whether these are manifestations of the same specific issue with one justified common corrective action, not merely matching model names, dimensions or generic categories. Preserve separate physical addresses, scopes, quantities and uncertainty. A group heading must not strengthen an uncertain member into a fact, erase source/document identity doubts, infer whole-document absence from an excerpt, sum cable lengths into shortage, or expand a local discrepancy into a confirmed delivery defect. Exact TEXT spellings and stamped identities remain distinct unless source evidence establishes the relation. Repeated drawings are not automatically additional objects. Check the proposed common heading and action, not just whether child text will be preserved.
Return one JSON object with binding (copy), action (group/keep_separate/needs_context), reason, checks. Each required check exactly once: check_id, status (pass/fail/needs_data), reason, evidence. Cite direct supporting sources for shared_issue and source_scope_and_identity passes. TEXT: {type:"text",block_id,quote} exact quote. GRAPHIC stamps: {type:"metadata",block_id,quote}; metadata is identity only, not drawing proof. IMAGE: {type:"image",block_id,image_sha256,bbox_norm_xyxy,observation}. Cite only attached images. Other presentation checks may use empty evidence with a reason.
Use group only if every check passes. Otherwise preserve separate findings and state the exact problem or needed source. Do not rewrite the proposal or resolve questionable technical facts to force a group. No expected groups or row count are supplied. Model approval is a candidate under an explicit experimental policy, not engineering certification.'''
    request={'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],
        'image_paths':source['image_paths'],'image_bindings':source['image_bindings']}
    return {'request':request,'binding':binding,'queue':queue,'synthetic':synthetic}


def validate_group_review(document,proposal,md,graphic_dir,result,*,max_images=12,extra_text_block_ids=()):
    prepared=prepare_group_review(document,proposal,md,graphic_dir,max_images=max_images,extra_text_block_ids=extra_text_block_ids)
    if not isinstance(result,dict) or result.get('binding')!=prepared['binding']:
        raise ValueError('stale_group_source_review')
    checks=result.get('checks');seen=set();evidence=[]
    if not isinstance(checks,list) or len(checks)!=len(CHECKS):raise ValueError('incomplete_group_checks')
    for check in checks:
        cid=check.get('check_id') if isinstance(check,dict) else None
        if cid not in CHECKS or cid in seen:raise ValueError('ambiguous_group_check')
        seen.add(cid)
        if check.get('status') not in ('pass','fail','needs_data') or not isinstance(check.get('reason'),str) or not check['reason'].strip() or not isinstance(check.get('evidence'),list):
            raise ValueError('invalid_group_check')
        if cid in ('shared_issue','source_scope_and_identity') and check['status']=='pass' and not check['evidence']:
            raise ValueError('group_factual_pass_without_evidence')
        evidence.extend(check['evidence'])
    images={(i['block_id'],i['sha256']) for i in prepared['request']['image_bindings']}
    for e in evidence:
        if isinstance(e,dict) and e.get('type')=='image' and (e.get('block_id'),e.get('image_sha256')) not in images:
            raise ValueError('group_image_not_attached')
    if evidence:
        packet=prepared['queue']['packets'][0]
        response={'finding_id':prepared['synthetic']['id'],'finding_sha256':packet['finding_sha256'],
            'source_md_sha256':prepared['binding']['source_md_sha256'],'claims':[{'claim_id':'group-check','claim':'Cited group sources',
                'status':'supported','reason':'Source binding only','evidence':evidence}]}
        supplied_queue=deepcopy(prepared['queue'])
        for queued in supplied_queue['packets']:
            for receipt in queued.get('graphic_attachments',[]):
                receipt['attached_to_model']=(receipt['block_id'],receipt['sha256']) in images
        verified=validate_source_review(response,supplied_queue,[prepared['synthetic']],md)
        if verified['errors']:raise ValueError('invalid_group_source_evidence: '+','.join(verified['errors']))
    action=result.get('action')
    if action not in ('group','keep_separate','needs_context') or not isinstance(result.get('reason'),str) or not result['reason'].strip():
        raise ValueError('invalid_group_decision')
    if action=='group' and any(c['status']!='pass' for c in checks):raise ValueError('group_with_unresolved_check')
    decision={'action':action,'reviewer':'independent_source_group_review','reason':result['reason'],
        **{k:prepared['binding'][k] for k in ('proposal_sha256','members_sha256','source_md_sha256')},
        'same_issue_verified':action=='group','common_corrective_action_verified':action=='group','individual_scopes_preserved':action=='group',
        'source_review':deepcopy(result),'semantic_certified':False}
    return decision
