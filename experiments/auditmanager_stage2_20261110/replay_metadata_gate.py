"""Offline causal probe of the unchanged production gate; variants are synthetic."""
import copy
import hashlib
import json
import runpy
from pathlib import Path
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]

def main():
    code = ROOT/'backend/app/pipeline/stages/block_analysis/finding_evidence_gate.py'
    gate = runpy.run_path(str(code))
    inventory = HERE/'merge_coverage_inventory.json'
    project = next(p for p in json.loads(inventory.read_text())['projects'] if p['discipline']=='EOM')
    assert len(project['deferred'])==1
    original = project['deferred'][0]['finding']
    matches = {}
    for field in ('finding','evidence_quote','recommendation'):
        matches[field] = [m.group(0) for m in gate['_INTERNAL_METADATA_RE'].finditer(original.get(field,''))]
    neutral = copy.deepcopy(original)
    phrase = 'оба обозначения также повторяются в переданном контексте другого листа.'
    assert phrase in neutral['evidence_quote']
    neutral['evidence_quote'] = neutral['evidence_quote'].replace(phrase,'оба обозначения также повторяются на другом листе.')
    metadata = copy.deepcopy(original)
    metadata['finding'] = 'Переданный контекст блока определяет иной тип фрагмента. Исправить тип фрагмента в разметке.'
    metadata['evidence_quote'] = 'Переданная разметка: тип фрагмента не совпадает с контекстом блока.'
    metadata['recommendation'] = 'Исправить тип фрагмента в разметке.'
    rows=[]
    for name,item in [('historical_exact_candidate',original),('synthetic_neutral_source_wording',neutral),('synthetic_actual_metadata_complaint',metadata)]:
        published,deferred,stats=gate['gate_findings']([copy.deepcopy(item)])
        rows.append({'case':name,'synthetic':name!='historical_exact_candidate','input':item,
                     'published_count':len(published),'deferred_count':len(deferred),
                     'reasons':[r for x in deferred for r in x['_evidence_gate']['reasons']], 'stats':stats})
    assert rows[0]['reasons']==['internal_metadata_comparison']
    assert rows[1]['published_count']==1 and not rows[1]['reasons']
    assert 'internal_metadata_comparison' in rows[2]['reasons']
    result={'mechanism':'Broad phrase match in evidence_quote defers a project-level room-number finding.',
            'code':{'path':str(code.relative_to(ROOT)),'sha256':hashlib.sha256(code.read_bytes()).hexdigest(),'symbol':'gate_findings / _INTERNAL_METADATA_RE'},
            'source':{'path':str(inventory.relative_to(ROOT)),'sha256':hashlib.sha256(inventory.read_bytes()).hexdigest()},
            'original_regex_matches_by_field':matches,'cases':rows,
            'conclusion':'Changing only the source-context phrase changes gate outcome. This reproduces a wording-sensitive false metadata classification, not an engineering validity check.',
            'preserved_final_finding':'B-EOM-F-024 survives through another detector candidate; no final loss in this run is claimed.',
            'recommended_behavior':'Classify whether the assertion complains about internal representation. Mere mention of provided context as a source must not suppress a project-document discrepancy.',
            'limits':['Neutral and metadata variants are synthetic; not independent historical engineer decisions.','Passing the gate does not establish technical validity or appropriate output severity.','Do not remove the metadata guard wholesale; real complaints about internal extraction must remain out of project findings.'],
            'production_changed':False}
    (HERE/'metadata_gate_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'matches':matches,'outcomes':[{k:r[k] for k in ('case','published_count','reasons')} for r in rows]},ensure_ascii=False))

if __name__=='__main__':main()
