"""Trace saved changes to source candidates and frozen adjudications; no inference calls."""
import hashlib
import json
from pathlib import Path
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CONTROL = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')

def read(p): return json.loads(p.read_text())
def ref(p): return {'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}

def main():
    decisions = {}
    for path in (ROOT/'experiments/auditmanager_stage1_20261109/decisions').glob('*.json'):
        d = read(path)
        for cid in d['candidate_ids']: decisions[cid] = (path, d)
    traces = []
    for run in read(HERE/'saved_runtime_inventory.json')['runs']:
        disc = run['discipline']
        out = Path(run['final_check']['source']['path']).parent
        tp = out/'02_text_analysis.json'
        text = {f['id']: f for f in read(tp)['text_findings']}
        mp = CONTROL/'snapshots'/disc/'findings_merge.json'
        merged = {f['id']: f for f in read(mp)['findings']}
        for change in run['changed_findings']:
            f = merged[change['finding_id']]
            candidates = [text[x] for x in f.get('source_finding_ids', []) if x in text]
            source_refs = sorted({b for c in candidates for b in c.get('related_block_ids', [])})
            merged_refs = set(f.get('related_block_ids', [])) | set(f.get('source_block_ids', []))
            merged_refs |= {e['block_id'] for e in f.get('evidence', []) if isinstance(e, dict) and e.get('block_id')}
            cid = 'B-'+disc+'-'+f['id']
            dp, decision = decisions[cid]
            traces.append({'candidate_id': cid, 'finding_id': f['id'], 'discipline': disc,
                'sources': [ref(tp), ref(mp), ref(dp)],
                'text_candidates': candidates, 'merged_finding': f,
                'source_related_block_ids': source_refs,
                'merged_explicit_block_ids': sorted(merged_refs),
                'source_refs_absent_from_explicit_merge_fields': sorted(set(source_refs)-merged_refs),
                'observed_verifier_change': change,
                'adjudicated_output_section': decision['output_section'],
                'adjudicated_wording': decision.get('wording', decision.get('required_behavior')),
                'causal_boundary': 'Saved candidate→merge comparison locates disappearance of explicit reference fields. It does not yet identify the internal merge operation, nor establish semantic validity of every original assertion.'})
    candidates = [t for t in traces if t['source_related_block_ids'] and not t['merged_explicit_block_ids']
                  and 'no_evidence' in json.dumps(t['observed_verifier_change'], ensure_ascii=False)]
    output = {'summary': {'changed_findings': len(traces), 'no_evidence_changes_with_upstream_refs_and_empty_merge_refs':len(candidates)},
              'code_mechanisms': [
                  {**ref(ROOT/'backend/app/pipeline/stages/findings_review/deterministic_critic.py'), 'symbol':'deterministic_verdict', 'line':337, 'behavior':'Empty referenced image/text sets produce no_evidence; not a semantic refutation.'},
                  {**ref(ROOT/'backend/app/pipeline/stages/findings_review/deterministic_corrector.py'), 'symbol':'_downgrade_or_flag', 'line':173, 'behavior':'Protected severities get human-review flag; others move to cross-check severity.'}],
              'traces':traces,
              'recommended_hypothesis':'Preserve source-to-evidence references through merge, validate their existence and object relevance before verification; do not bypass no_evidence or restore severity blindly.',
              'limits':['An adjudicated question is useful but not automatically a technical defect.','Reference presence does not prove semantic relevance.','Saved snapshots prove field-level transitions; internal cause requires merge implementation/output tracing.','No production changes or new model runs performed.']}
    (HERE/'verifier_change_traces.json').write_text(json.dumps(output,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(output['summary'],ensure_ascii=False))

if __name__ == '__main__': main()
