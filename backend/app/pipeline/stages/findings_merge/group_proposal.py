"""Presentation grouping discovery over current output, without evaluation answers."""
from copy import deepcopy
import json
from backend.app.pipeline.stages.findings_verify.source_review_final_check import FIELDS


def prepare_group_proposal(document,*,max_findings=256,max_chars=250000):
    if any(type(v) is not int or v<1 for v in (max_findings,max_chars)):raise ValueError('positive_group_proposal_budget_required')
    findings=document.get('findings')
    if not isinstance(findings,list):raise ValueError('invalid_group_document')
    ids=[f.get('id') for f in findings]
    if any(not isinstance(i,str) or not i for i in ids) or len(set(ids))!=len(ids):raise ValueError('ambiguous_group_finding_ids')
    if len(findings)>max_findings:raise ValueError('group_proposal_finding_budget')
    payload={'findings':[{k:deepcopy(f[k]) for k in FIELDS if k in f} for f in findings]}
    instruction='''Propose presentation groups for this current audit output. This does not verify truth or authorize removing findings. No tools, web, historical findings or engineer decisions.
Group only manifestations of the same specific issue with a common corrective action and compatible severity and explicit report_section. Similar model names, dimensions, source pages or generic categories are insufficient. An unclassified report_section stays unclassified; do not assign a section to force grouping. Preserve separate physical addresses, each source and each uncertainty. Do not sum quantities into shortage, resolve ambiguous identifiers, change severity or turn conditional claims into facts. A common heading must not be stronger than the member statements. When uncertain keep separate.
Return JSON: groups, keep_separate_ids. Each group: member_ids (at least two), reason, shared_issue, common_corrective_action, source_refs (existing block_id citations from its members), distinctions_to_preserve (nonempty list of strings). Every supplied finding ID occurs exactly once, in one group or keep_separate_ids. Do not invent a target row count. All proposals require an independent source review before application.'''
    request={'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],'image_paths':[]}
    if sum(len(m['content']) for m in request['messages'])>max_chars:raise ValueError('group_proposal_character_budget')
    return request


def validate_group_proposal(document,result):
    if not isinstance(result,dict) or not isinstance(result.get('groups'),list) or not isinstance(result.get('keep_separate_ids'),list):
        raise ValueError('invalid_group_proposal')
    byid={f['id']:f for f in document['findings']}
    if len(byid)!=len(document['findings']):raise ValueError('ambiguous_group_finding_ids')
    assigned=list(result['keep_separate_ids'])
    for group in result['groups']:
        if not isinstance(group,dict):raise ValueError('invalid_group')
        members=group.get('member_ids')
        if not isinstance(members,list) or len(members)<2 or any(not isinstance(i,str) or i not in byid for i in members):raise ValueError('invalid_group_members')
        if any(not isinstance(group.get(k),str) or not group[k].strip() for k in ('reason','shared_issue','common_corrective_action')):raise ValueError('missing_group_rationale')
        distinctions=group.get('distinctions_to_preserve')
        if not isinstance(distinctions,list) or not distinctions or any(not isinstance(s,str) or not s.strip() for s in distinctions):raise ValueError('missing_member_distinctions')
        refs={e.get('block_id') for i in members for e in byid[i].get('evidence',[]) if isinstance(e,dict)}
        proposed=group.get('source_refs')
        if not isinstance(proposed,list) or not proposed or any(not isinstance(r,str) or r not in refs for r in proposed):raise ValueError('group_borrows_unrelated_sources')
        assigned.extend(members)
    if any(not isinstance(i,str) for i in assigned) or len(assigned)!=len(set(assigned)) or set(assigned)!=set(byid):
        raise ValueError('group_partition_loses_or_duplicates_findings')
    return deepcopy(result)
