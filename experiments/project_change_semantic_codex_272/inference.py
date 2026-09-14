"""Provider-neutral copy of the frozen propose/verify/repair state contract.

Kept in the separate Codex route to leave the disabled legacy executor intact.
No engineering rules, thresholds or prompts are changed.
"""
from experiments.project_change_semantic_272.run import view, check_event
from experiments.project_change_semantic_272.prompts import VERIFY, REPAIR


async def infer_packet(p, call, primary_prompt):
    proposed=await call(p,'propose',primary_prompt,view(p))
    events=proposed.get('events',[]) if isinstance(proposed,dict) else []
    if not isinstance(events,list):events=[]
    mechanical=[dict(event=e,errors=check_event(p,e,False)) for e in events]
    event_ids=[e.get('event_id') for e in events if isinstance(e,dict) and isinstance(e.get('event_id'),str)]
    for row in mechanical:
        if isinstance(row['event'],dict) and event_ids.count(row['event'].get('event_id'))>1:
            row['errors'].append('DUPLICATE_EVENT_ID')
    clean=[r['event'] for r in mechanical if not r['errors']]
    reviewed=await call(p,'verify',VERIFY,dict(packet=view(p),proposals=clean)) if clean else {}
    decisions=reviewed.get('decisions',[]) if isinstance(reviewed,dict) else []
    if not isinstance(decisions,list):decisions=[]
    # One bounded repair may remove unsupported detail. It never
    # bypasses a fresh exact-witness and semantic audit.
    repairable=[e for e in clean if any(isinstance(d,dict) and d.get('event_id')==e['event_id'] and d.get('verdict')=='REVIEW' and d.get('scope_correct') is True and d.get('material_change') is True for d in decisions)]
    if repairable:
        repair=await call(p,'repair',REPAIR,dict(packet=view(p),proposals=repairable,audit=decisions))
        repaired=repair.get('events',[]) if isinstance(repair,dict) else []
        if not isinstance(repaired,list):repaired=[]
        repair_ids={e['event_id'] for e in repairable}
        repaired=[e for e in repaired if isinstance(e,dict) and e.get('event_id') in repair_ids and not check_event(p,e,False)]
        if repaired:
            second=await call(p,'verify_repair',VERIFY,dict(packet=view(p),proposals=repaired))
            ds=second.get('decisions',[]) if isinstance(second,dict) else []
            if isinstance(ds,list):
                replacements={e['event_id']:e for e in repaired}
                for row in mechanical:
                    if isinstance(row['event'],dict) and row['event'].get('event_id') in replacements:
                        row['original_event']=row['event'];row['event']=replacements[row['event']['event_id']]
                        row['errors']=[]
                decisions=[d for d in decisions if isinstance(d,dict) and d.get('event_id') not in replacements]+ds
    for row in mechanical:
        event=row['event'];matches=[d for d in decisions if isinstance(event,dict) and isinstance(d,dict) and d.get('event_id')==event.get('event_id')]
        d=matches[0] if len(matches)==1 else {}
        accepted=not row['errors'] and d.get('verdict')=='ACCEPT' and all(d.get(k) is True for k in ['scope_correct','states_entailed','material_change','grouping_correct'])
        row.update(status='ACCEPTED_CANDIDATE' if accepted else 'REVIEW',semantic_audit=d,
            evidence_scope=dict(packet_id=p['packet_id'],pair_key=p['pair_key'],coverage_complete=False))
    result=dict(packet_id=p['packet_id'],pair_index=p['pair_index'],events=mechanical,
                unknowns=proposed.get('unknowns',[]) if isinstance(proposed,dict) else [])
    return result
