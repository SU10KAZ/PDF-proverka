"""Trace requirement applicability to authoritative TEXT, not model recollection."""
from collections import Counter, defaultdict
import re


def authoritative_text_blocks(md):
    headers=list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$',md,re.M|re.I))
    counts=Counter(h.group(2) for h in headers)
    result={}
    for i,h in enumerate(headers):
        if h.group(1).upper() not in ('TEXT','TABLE') or counts[h.group(2)]!=1:
            continue
        content=md[h.end():headers[i+1].start() if i+1<len(headers) else len(md)]
        content=re.split(r'^##[ \t]+(?:Page|СТРАНИЦА)\b',content,maxsplit=1,flags=re.M|re.I)[0]
        result[h.group(2)]=content
    return result


def reconcile_requirement_checks(data, text_blocks):
    requirements=data.get('requirement_registry',[])
    requirements=requirements if isinstance(requirements,list) else []
    index=defaultdict(list)
    for requirement in requirements:
        if isinstance(requirement,dict) and isinstance(requirement.get('id'),str):
            index[requirement['id']].append(requirement)
    objects=data.get('object_registry',[])
    object_ids=Counter(o.get('id') for o in objects if isinstance(o,dict) and isinstance(o.get('id'),str)) if isinstance(objects,list) else Counter()
    checks=data.get('requirement_applicability_checks',[])
    checks=checks if isinstance(checks,list) else []
    def quoted(evidence):
        return isinstance(evidence,list) and bool(evidence) and all(
            isinstance(e,dict) and isinstance(e.get('block_id'),str) and e['block_id'] in text_blocks
            and isinstance(e.get('quote'),str) and bool(e['quote'].strip()) and e['quote'] in text_blocks[e['block_id']]
            for e in evidence)
    rows=[]
    for check in checks:
        row={'status':'not_checked','semantic_verified':False,'receipt':check};rows.append(row)
        if not isinstance(check,dict) or not isinstance(check.get('requirement_id'),str) or len(index[check['requirement_id']])!=1:
            row['reason']='unknown_or_ambiguous_requirement';continue
        requirement=index[check['requirement_id']][0]
        oid=check.get('object_id')
        if not isinstance(oid,str) or object_ids[oid]!=1:
            row['reason']='unknown_or_ambiguous_object';continue
        row.update(requirement_id=check['requirement_id'],object_id=oid)
        applicability=check.get('applicability')
        if applicability not in ('applies','does_not_apply','unknown') or not isinstance(check.get('reason'),str) or not check['reason'].strip():
            row['reason']='missing_applicability_reason';continue
        if applicability=='unknown':
            row['status']='needs_data';continue
        if requirement.get('source_kind') not in ('project_requirement','catalogue','normative_candidate'):
            row['reason']='unknown_requirement_source_kind';continue
        if requirement['source_kind']=='normative_candidate':
            row.update(status='needs_data',reason='norm_resolver_verification_required');continue
        fields=('document','revision','requirement_text','scope','stage','execution','exceptions')
        unknown={'unknown','неизвестно','не указано','not specified','?'}
        if any(not isinstance(requirement.get(k),str) or not requirement[k].strip() or requirement[k].strip().lower() in unknown for k in fields):
            row.update(status='needs_data',reason='incomplete_requirement_identity_or_scope');continue
        if not quoted(requirement.get('source_evidence')) or not quoted(check.get('scope_evidence')):
            row['reason']='requirement_or_scope_quote_not_in_authoritative_text';continue
        row.update(status='reported_'+applicability,requirement=requirement,
                   comparison=check.get('comparison','unknown'),
                   boundary='Source quotations exist; applicability reasoning and comparison are not independently verified.')
    return {'checks':rows,'unresolved':sum(r['status'] in ('not_checked','needs_data') for r in rows),
            'requirements_without_checks':sorted(set(index)-{r.get('requirement_id') for r in rows}),
            'coverage_complete':False,'semantic_verified':False}
