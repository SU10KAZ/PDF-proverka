"""Explicit, source-bound presentation grouping; never infer approval from similarity."""
from copy import deepcopy
import hashlib
import re
from ..findings_verify.source_review_evidence import evidence_digest
from backend.app.services.findings.report_routing import report_section_key



def _inherited_scope_lines(decision, member_ids):
    """Expose reviewed limitations without resolving or endorsing member claims."""
    review = decision.get('source_review')
    if review is None:
        return []
    if not isinstance(review, dict):
        raise ValueError('invalid_group_source_review')
    issues = review.get('inherited_member_issues', [])
    if not isinstance(issues, list):
        raise ValueError('invalid_inherited_group_issues')
    if not issues:
        return []
    binding = review.get('binding', {})
    if (not isinstance(binding, dict) or review.get('action') != 'group'
            or any(binding.get(k) != decision.get(k) for k in
                   ('proposal_sha256', 'members_sha256', 'source_md_sha256'))):
        raise ValueError('stale_group_source_review')
    lines = ['Ограничения проверки отдельных замечаний:']
    for issue in issues:
        if (not isinstance(issue, dict) or not isinstance(issue.get('member_ids'), list)
                or not issue['member_ids']
                or any(not isinstance(i, str) or i not in member_ids for i in issue['member_ids'])
                or len(set(issue['member_ids'])) != len(issue['member_ids'])
                or not isinstance(issue.get('issue'), str) or not issue['issue'].strip()
                or not isinstance(issue.get('evidence'), list) or not issue['evidence']):
            raise ValueError('invalid_inherited_group_issue')
        lines.append('[' + ', '.join(issue['member_ids']) + '] ' + issue['issue'])
    return lines + ['']


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
        labels=dict(problem='Проблема',description='Описание',solution='Решение',risk='Риск',consequence='Последствия',norm='Норма',norm_quote='Цитата нормы',sheet='Лист',page='Страница',location='Место')
        lines=_inherited_scope_lines(decision, member_ids)
        for f in members:
            lines.append(f"[{f['id']}]")
            for key in ('problem','description','solution','risk','consequence','norm','norm_quote','sheet','page','location'):
                if f.get(key) not in (None,''):
                    lines.append(f'{labels[key]}: {f[key]}')
        merged['description']='\n'.join(lines)
        if any(len(str(merged.get(k,'')))>32767 for k in ('problem','description','solution','risk','norm','norm_quote')):
            raise ValueError('group_exceeds_excel_cell_limit')
        if len(merged['description'])+len(str(merged.get('norm','')))+8>32767:
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


def run_group_admission_phase(output_dir, source_md):
    """Apply an explicit review inbox after source reviews; absent inbox is a no-op."""
    import json
    from pathlib import Path
    from collections import Counter
    root=Path(output_dir);inbox=root/'03_group_review_admissions.json'
    if not inbox.exists():return {'skipped':True,'applied':0,'not_applied':0,'already_applied':0}
    entries=json.loads(inbox.read_text())
    if not isinstance(entries,list) or any(not isinstance(e,dict) for e in entries):
        raise ValueError('invalid_group_review_inbox')
    counts=Counter()
    for entry in entries:
        proposal=entry.get('proposal')
        ids=proposal.get('member_ids',[]) if isinstance(proposal,dict) else []
        if isinstance(ids,list):counts.update(i for i in ids if isinstance(i,str))
    path=root/'03_findings.json';before=json.loads(path.read_text());result=deepcopy(before);receipts=[]
    for entry in entries:
        proposal,decision=entry.get('proposal'),entry.get('decision')
        ids=proposal.get('member_ids',[]) if isinstance(proposal,dict) else []
        if isinstance(ids,list) and any(isinstance(i,str) and counts[i]>1 for i in ids):
            receipts.append({'status':'not_applied','reason':'overlapping_group_proposals'});continue
        already=False
        for finding in result.get('findings',[]):
            provenance=finding.get('grouping_provenance',{})
            if (isinstance(decision,dict) and decision.get('source_md_sha256')==hashlib.sha256(source_md.encode()).hexdigest()
                and provenance.get('proposal')==proposal and provenance.get('decision')==decision
                and finding.get('grouping_original_findings')
                and evidence_digest(finding['grouping_original_findings'])==decision.get('members_sha256')):
                already=True;break
        if already:
            receipts.append({'status':'already_applied','member_ids':ids});continue
        result,receipt=apply_reviewed_group(result,proposal,decision,source_md);receipts.append(receipt)
    if result!=before:
        temporary=path.with_suffix('.group_review.tmp')
        temporary.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');temporary.replace(path)
    report={'skipped':False,**{k:sum(r['status']==k for r in receipts) for k in ('applied','not_applied','already_applied')},'receipts':receipts}
    (root/'03_group_review_admission_results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    return report
