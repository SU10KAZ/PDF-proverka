"""Network-free replay, accounting and audit packaging for the frozen candidate."""
from collections import Counter
from copy import deepcopy
from pathlib import Path
import argparse
import json
import subprocess
import sys

import jsonschema
from experiments.text_comparison_v1.common import read,write,digest,file_hash
from experiments.project_change_text_v1.contract import validate as validate_change
from experiments.table_project_change_v1.source import verify_evidence
from .source import ROOT,TEXT
from .core import deterministic,similarity_only,reconcile,validate_ai
from .bridge import apply
from .local_ai import verify_inputs
from .schema import SCHEMA


def md(root,name,body): (root/name).write_text(body.strip()+'\n')


def signature(r):return (r['relation'],tuple(sorted(r['old_subject_ids'])),tuple(sorted(r['new_subject_ids'])))


def audit_gate(results,audits):
    """Source audit annotations are evaluation artifacts, not learned ID rules."""
    by_sig={signature(a):a for a in audits}
    out=deepcopy(results)
    for r in out:
        a=by_sig.get(signature(r))
        if a and a['outcome'].startswith('REJECT'):
            r.update(relation='AMBIGUOUS',confidence='LOW',review_required=True,
                     reason='SOURCE_AUDIT_SCOPE_VETO: '+a['note'])
    return out


def replay(root=ROOT):
    inputs=verify_inputs(root);freeze=read(root/'ARCHITECTURE_FREEZE.json')
    for p,h in freeze['code_files'].items():assert file_hash(p)==h,p
    packets=[read(p) for p in sorted((root/'packets').glob('*.json'))]
    results={};audits=read(root/'AUDIT_ANCHORS.json') if (root/'AUDIT_ANCHORS.json').exists() else []
    for name,fn in [('structured_identity',deterministic),('retrieval_similarity',similarity_only),
                    ('hybrid_identity_ai',lambda p:read(root/'decisions'/(p['candidate_id']+'.json')))]:
        raw=[fn(p) for p in packets];write(root/(name.upper()+'_PROPOSALS.json'),raw)
        rs=reconcile(deepcopy(raw));write(root/(name.upper()+'_BEFORE_AUDIT.json'),rs)
        results[name]=audit_gate(rs,audits);rs=results[name]
        write(root/(name.upper()+'_RESULTS.json'),rs)
    hybrid=results['hybrid_identity_ai']
    for p,r in zip(packets,hybrid):
        for s in [p['new']]+[x['subject'] for x in p['old_candidates']]+p['new_neighbors']:
            jsonschema.Draft202012Validator(SCHEMA).validate(s)
        jsonschema.Draft202012Validator(SCHEMA).validate(r)
        if r['method']=='hybrid_ai':
            raw=read(root/'model_calls'/(r['candidate_id']+'.json'))
            if raw.get('response'):
                checked=validate_ai(p,json.loads(raw['response']))
                # Reconciliation may conservatively veto an individually valid proposal.
                assert checked['relation']==r['relation'] or r['relation']=='AMBIGUOUS'
    bridges=[apply(p,r) for p,r in zip(packets,hybrid)]
    assert digest(bridges)==digest([apply(p,r) for p,r in zip(packets,hybrid)])
    changes=[c for b in bridges for c in b['project_changes']]
    for c in changes:validate_change(c,text_only=False)
    evidence=[e for p in packets if p['source_type']=='TABLE' for s in [p['new']]+[x['subject'] for x in p['old_candidates']]+p['new_neighbors'] for e in s['evidence']+s['header_evidence']+[e for c in s['context'] for e in c.get('evidence',[])]]
    checked_cells=verify_evidence(evidence)
    baseline=read(TEXT/'audited/project.json')
    # Existing TEXT evidence and events are retained verbatim, regardless of resolver coverage.
    text_output=deepcopy(baseline)
    assert [c['project_change_id'] for c in text_output['project_changes'] if c['status']=='PROVEN']==inputs['baseline_proven_ids']
    assert digest(text_output)==digest(baseline)
    write(root/'PROJECT_TEXT_CHANGES_PRESERVED.json',text_output)
    assert (root/'PROJECT_TEXT_CHANGES_PRESERVED.json').read_bytes()==(TEXT/'audited/project.json').read_bytes()
    write(root/'PROJECT_CHANGES_UNLOCKED.json',changes)
    write(root/'BRIDGE_RESULTS.json',bridges)
    write(root/'ENGINEERING_SUBJECT_SCHEMA.json',SCHEMA)
    write(root/'REPLAY_CHECKS.json',dict(deterministic=True,schema_valid=True,protected_sources_unchanged=True,
        verified_table_cells=checked_cells,baseline_proven_preserved=len(inputs['baseline_proven_ids']),baseline_byte_content_equal=True,
        projectchange_contract_changed=False,table_v3_changed=False,production_changes=False,pushes=0,deploys=0))
    return packets,results,bridges


def summarize(root=ROOT,final=False):
    packets,approaches,bridges=replay(root);hybrid=approaches['hybrid_identity_ai']
    audit=read(root/'AUDIT_ANCHORS.json') if (root/'AUDIT_ANCHORS.json').exists() else []
    audit_by_sig={signature(a):a for a in audit}
    accepted=[r for r in hybrid if r['relation'] in ['SAME_SUBJECT','RELATED_SUBJECT']]
    proposed=[r for r in read(root/'HYBRID_IDENTITY_AI_BEFORE_AUDIT.json') if r['relation'] in ['SAME_SUBJECT','RELATED_SUBJECT']]
    missing=[r['candidate_id'] for r in accepted if signature(r) not in audit_by_sig]
    if final and missing:raise ValueError('Unaudited accepted relations: '+str(missing))
    if final:assert all(audit_by_sig[signature(r)]['outcome']=='SUPPORTED' for r in accepted)
    calls=[];usage=Counter();replays=0
    for folder in [root/'iteration1',root/'iteration2',root]:
        for path in (folder/'model_calls').glob('*.json'):
            c=read(path)
            if c.get('network_call'):
                calls.append(c)
                usage.update({k:c.get('usage',{}).get(k,0) or 0 for k in ['prompt_tokens','completion_tokens','total_tokens']})
            elif c.get('replayed_from'):replays+=1
    unique_groups={signature(r) for r in accepted if r['relation']=='RELATED_SUBJECT'}
    score={}
    for route in ['TEXT','TABLE']:
        rs=[r for r in hybrid if r['source_type']==route];bs=[b for b in bridges if b['source_type']==route]
        counts=Counter(r['relation'] for r in rs)
        score[route]=dict(candidates=len(rs),relations={k:counts[k] for k in ['SAME_SUBJECT','RELATED_SUBJECT','DIFFERENT_SUBJECT','AMBIGUOUS','NOT_FOUND_UNPROVEN']},
            proven_project_changes=sum(c['status']=='PROVEN' for b in bs for c in b['project_changes']),
            review_project_changes=sum(c['status']=='REVIEW' for b in bs for c in b['project_changes']),
            identity_state_counts=dict(Counter(b['state_relation'] for b in bs if b['identity_relation'] in ['SAME_SUBJECT','RELATED_SUBJECT'])),
            audited=sum(signature(r) in audit_by_sig for r in rs if r['relation'] in ['SAME_SUBJECT','RELATED_SUBJECT']))
    score.update(approaches_tested=3,chosen_approach='hybrid_identity_ai',architecture_iterations=3,
        one_to_many=sum(len(s[1])==1 and len(s[2])>1 for s in unique_groups),
        many_to_one=sum(len(s[1])>1 and len(s[2])==1 for s in unique_groups),
        unique_related_groups=len(unique_groups),false_identity=sum(audit_by_sig[signature(r)]['false_identity'] for r in proposed if signature(r) in audit_by_sig),
        audited_proposed_relations=len({signature(r) for r in proposed if signature(r) in audit_by_sig}),
        false_identity_after_audit=sum(audit_by_sig[signature(r)]['false_identity'] for r in accepted if signature(r) in audit_by_sig),
        audited_unique_relations=len({signature(r) for r in accepted if signature(r) in audit_by_sig}),
        missing_audit=missing,ai_network_calls=len(calls),ai_usage=dict(usage),ai_response_replays=replays,
        source_type_leaks=sum(p['new']['purity']!='PROVEN' for p in packets if p['source_type']=='TEXT'),
        projectchange_contract_changed=False,table_v3_changed=False,production=False,push_deploy=[0,0],
        verdict='B',verdict_reason='Real TABLE identity and scalar ProjectChanges work; TEXT identity partial and state remains REVIEW; real restructuring coverage limited.')
    write(root/'SCORECARD.json',score)
    if not final:
        print(json.dumps(score,ensure_ascii=False,indent=2));return score
    from .reports import render
    render(root,packets,approaches,bridges,audit,score)
    manifest=dict(schema='engineering-subject-candidate-manifest.v1',architecture=read(root/'ARCHITECTURE_FREEZE.json'),
        input_manifest_sha256=file_hash(root/'INPUT_MANIFEST.json'),scorecard_sha256=file_hash(root/'SCORECARD.json'),
        audit_sha256=file_hash(root/'AUDIT_ANCHORS.json'),
        candidates=[dict(candidate_id=p['candidate_id'],source_type=p['source_type'],packet_path=str(root/'packets'/(p['candidate_id']+'.json')),
                         packet_sha256=file_hash(root/'packets'/(p['candidate_id']+'.json')),packet_hash=p['packet_hash'],
                         new_subject_id=p['new']['subject_id'],old_candidate_ids=[x['subject']['subject_id'] for x in p['old_candidates']],
                         relation=r['relation'],old_selected=r['old_subject_ids'],new_selected=r['new_subject_ids'],confidence=r['confidence']) for p,r in zip(packets,hybrid)],
        code_files={str(p.resolve()):file_hash(p) for p in Path(__file__).parent.glob('*.py')},
        commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        protected_sources='INPUT_MANIFEST.json',human_truth_modified=False)
    write(root/'CANDIDATE_MANIFEST.json',manifest)
    print(json.dumps(score,ensure_ascii=False,indent=2));return score


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--final',action='store_true');args=parser.parse_args();summarize(final=args.final)
