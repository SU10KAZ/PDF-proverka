"""Bounded citation-only repair candidate; never changes or releases finding prose."""
from copy import deepcopy
import json
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check, validate_final_check
from backend.app.pipeline.stages.findings_verify.source_review_results import validate_source_review


def prepare(entry, document, md, check):
    validated=validate_final_check(entry,document,md,check)
    failed=[c for c in check['checks'] if c['status']!='pass']
    if validated['status']!='held' or len(failed)!=1 or failed[0]['check_id']!='final_citations':
        raise ValueError('citation_only_failure_required')
    request=deepcopy(prepare_final_check(entry,document,md)['request'])
    packet=json.loads(request['messages'][1]['content'])
    supported={c['claim_id'] for c in entry['decision']['claim_reviews'] if c['verdict']=='supported'}
    packet['existing_supported_claims']=[c for c in entry['response']['claims'] if c['claim_id'] in supported]
    packet['citation_check']=failed[0]
    packet['repair_binding']=evidence_digest({'entry':entry,'document':document,'md':md,'check':check})
    request['messages']=[{'role':'system','content':'''Complete only missing TEXT citations for the proposed finding using authoritative supplied TEXT. Do not alter wording, quantities, scope, claim verdicts, report routing or images. Do not correct TEXT from raster. Existing supported claims are untrusted candidate conclusions: check that each added quotation directly supports the specific existing claim. Include table headers, object membership, multiplicities and operand rows wherever necessary to make the retained calculations traceable. A source block's presence is not a substitute for the exact required quoted rows. Do not add unrelated blocks or duplicate existing citations. If the prose needs correction, return cannot_complete with a reason instead of papering over it with citations.
Return exactly repair_binding (copy), status (candidate or cannot_complete), reason (nonempty), additions: an array of {claim_id,evidence:{type:"text",block_id,quote},reason}. Each addition must cite an existing supported claim, an exact source quotation, and explain its relevance. No history, engineer answers or expected verdict are supplied. This proposal does not authorize release; an independent check will recheck the complete edit.'''} ,{'role':'user','content':json.dumps(packet,ensure_ascii=False)}]
    return request,packet['repair_binding']


def apply_candidate(entry,document,md,check,result):
    _,binding=prepare(entry,document,md,check)
    if not isinstance(result,dict) or set(result)!={'repair_binding','status','reason','additions'} or result['repair_binding']!=binding:
        raise ValueError('invalid_citation_repair_binding_or_fields')
    if result['status']!='candidate' or not isinstance(result['reason'],str) or not result['reason'].strip():
        raise ValueError('citation_completion_not_proposed')
    additions=result['additions']
    if not isinstance(additions,list) or not 0<len(additions)<=128:raise ValueError('invalid_citation_additions')
    revised=deepcopy(entry)
    claims={c['claim_id']:c for c in revised['response']['claims']}
    supported={c['claim_id'] for c in revised['decision']['claim_reviews'] if c['verdict']=='supported'}
    refs=revised['decision'].setdefault('final_evidence_refs',[])
    for addition in additions:
        if not isinstance(addition,dict) or set(addition)!={'claim_id','evidence','reason'}:raise ValueError('invalid_citation_addition')
        cid=addition['claim_id'];ev=addition['evidence']
        if cid not in supported or not isinstance(addition['reason'],str) or not addition['reason'].strip():raise ValueError('unsupported_citation_claim')
        if not isinstance(ev,dict) or set(ev)!={'type','block_id','quote'} or ev['type']!='text':raise ValueError('text_citation_only')
        evidence=claims[cid].setdefault('evidence',[])
        if ev not in evidence:evidence.append(deepcopy(ev))
        ref={'claim_id':cid,'evidence_index':evidence.index(ev)}
        if ref not in refs:refs.append(ref)
    checked=validate_source_review(revised['response'],revised['queue'],document['findings'],md)
    if checked['errors']:raise ValueError('invalid_citation_sources: '+','.join(checked['errors']))
    revised['decision']['response_sha256']=evidence_digest(revised['response'])
    # Structural admission and source binding, not semantic endorsement.
    before=json.loads(prepare_final_check(entry,document,md)['request']['messages'][1]['content'])['proposed']
    after=json.loads(prepare_final_check(revised,document,md)['request']['messages'][1]['content'])['proposed']
    ignored={'evidence','sheet','page'}
    if {k:v for k,v in before.items() if k not in ignored}!={k:v for k,v in after.items() if k not in ignored}:
        raise ValueError('citation_repair_changed_visible_prose')
    return revised
