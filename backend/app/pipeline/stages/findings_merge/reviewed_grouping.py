"""Explicit, source-bound presentation grouping; never infer approval from similarity."""
from copy import deepcopy
import hashlib
import re
from ..findings_verify.source_review_evidence import evidence_digest
from backend.app.services.findings.report_routing import report_section_key


def apply_reviewed_group(document, proposal, decision, source_md):
    """Apply one approved group or preserve the entire document with a refusal.

    This is an opt-in local admission function. Reviewer assertions are retained
    for inspection, not certified as semantic truth. No provider call occurs.
    """
    original=deepcopy(document)
    try:
        if not isinstance(document,dict) or not isinstance(document.get('findings'),list):
            raise ValueError('invalid_document')
        if not isinstance(proposal,dict) or not isinstance(decision,dict):
            raise ValueError('explicit_group_review_required')
        items=document['findings']
        ids=[f.get('id') for f in items if isinstance(f,dict)]
        if len(ids)!=len(items) or any(not isinstance(i,str) or not i for i in ids) or len(set(ids))!=len(ids):
            raise ValueError('ambiguous_finding_ids')
        member_ids=proposal.get('member_ids')
        if (not isinstance(member_ids,list) or len(member_ids)<2
            or any(not isinstance(i,str) or i not in ids for i in member_ids)
            or len(set(member_ids))!=len(member_ids)):
            raise ValueError('invalid_members')
        members=[f for f in items if f['id'] in member_ids]
        if (decision.get('proposal_sha256')!=evidence_digest(proposal)
            or decision.get('members_sha256')!=evidence_digest(members)
            or decision.get('source_md_sha256')!=hashlib.sha256(source_md.encode()).hexdigest()):
            raise ValueError('stale_group_review')
        if (decision.get('action')!='group' or any(decision.get(k) is not True for k in
            ('same_issue_verified','common_corrective_action_verified','individual_scopes_preserved'))
            or any(not isinstance(decision.get(k),str) or not decision[k].strip() for k in ('reviewer','reason'))):
            raise ValueError('group_scope_not_reviewed')
        if any(not isinstance(proposal.get(k),str) or not proposal[k].strip()
               for k in ('shared_issue','common_corrective_action')):
            raise ValueError('group_wording_required')
        if len({f.get('severity') for f in members})!=1 or len({report_section_key(f) for f in members})!=1:
            raise ValueError('incompatible_severity_or_section')
        if report_section_key(members[0])=='unclassified':
            raise ValueError('explicit_report_section_required')
        refs=proposal.get('source_refs')
        known=set(re.findall(r'^### BLOCK[^\n]*?:\s*(\S+)',source_md,re.M))
        evidenced={e['block_id'] for f in members for e in f.get('evidence',[])
                    if isinstance(e,dict) and isinstance(e.get('block_id'),str)}
        if not isinstance(refs,list) or not refs or any(not isinstance(r,str) or r not in known or r not in evidenced for r in refs):
            raise ValueError('unbound_group_sources')
        from backend.app.services.findings.findings_service import aggregate_merged_fields
        merged=deepcopy(members[0]);merged.update(aggregate_merged_fields(members,merged))
        merged['problem']=proposal['shared_issue']
        merged['solution']=proposal['common_corrective_action']
        # Explicit member-level text remains visible, not only in a hidden archive.
        lines=[]
        for f in members:
            lines.append(f"[{f['id']}]")
            for key in ('problem','description','solution','risk','consequence','norm','norm_quote','sheet','page','location'):
                if f.get(key) not in (None,''):
                    lines.append(f'{key}: {f[key]}')
        merged['description']='\n'.join(lines)
        if any(len(str(merged.get(k,'')))>32767 for k in ('problem','description','solution','risk','norm','norm_quote')):
            raise ValueError('group_exceeds_excel_cell_limit')
        merged['grouping_original_findings']=deepcopy(members)
        merged['merged_original_finding_ids']=[f['id'] for f in members]
        merged['grouping_provenance']={'proposal':deepcopy(proposal),'decision':deepcopy(decision),'semantic_certified':False}
        result=deepcopy(document)
        result['findings']=[merged if f['id']==members[0]['id'] else f for f in result['findings']
                            if f['id'] not in member_ids or f['id']==members[0]['id']]
        result.setdefault('meta',{})['total_findings']=len(result['findings'])
        return result,{'status':'applied','member_ids':member_ids,'surviving_id':members[0]['id'],'semantic_certified':False}
    except (ValueError,TypeError,KeyError,AttributeError) as exc:
        return original,{'status':'not_applied','reason':str(exc)}
