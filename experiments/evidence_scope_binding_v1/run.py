"""Reproducible offline DEV experiment, with human truth kept read-only."""
from collections import Counter
from copy import deepcopy
from pathlib import Path
import argparse
import hashlib
import json
import re

from .core import APPROACHES, bind, digest
from .source import clean, pdf, read, receipt, source_data, table_nodes, text_nodes

ROOT = Path('/home/coder/auditmanager/corpus-audits/20260914_evidence_scope_binding_v1')
PREVIOUS = ROOT.parent / '20260913_engineering_subject_multiproject_validation'
REPO = Path(__file__).resolve().parents[2]


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2)+'\n')


def protect():
    for path, sha in read(ROOT/'BASELINE_MANIFEST.json')['protected_code'].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != sha:
            raise ValueError('Frozen code changed: ' + path)


def historical_cases():
    result=[]
    diagnostic=ROOT.parent/'20260913_project_change_text_v1/reports/HISTORICAL_DIAGNOSTIC_EVIDENCE.json'
    for record in read(diagnostic):
        version=record['source_refs'][0]['document_version']
        source=ROOT.parent/'20260913_text_comparison_goal_driven/run2/documents'/version/'sections.json'
        ledger=read(source)['ledger']; lp=ROOT/'derived_ledgers'/(version+'.json');write(lp,ledger)
        d=dict(document_version=version,document_code=ledger['document_code'],project='272_Sadovnicheskaya_76_Balchug_Esteyt',
               discipline='OV',artifacts={k:v for k,v in record['source_receipts'].items() if k in ('pdf','blocks','work_md')})
        line=ledger['columns']['markdown_line'].index(record['source_refs'][0]['markdown_line'])
        fragment,nodes=text_nodes(d,lp,line)
        result.append(dict(case_id='text_historical_'+record['side'].upper(),route='TEXT',split='DEV_HISTORICAL_DIAGNOSTIC',
                           fragment=fragment,nodes=nodes,document=d,side=record['side'].upper(),ledger_path=str(lp)))
        if record['side']=='new':
            raw=Path(d['artifacts']['work_md']['path']).read_text().splitlines()
            # First list entry on the next page is a real continuation control.
            hits=[i for i,n in enumerate(ledger['columns']['markdown_line'])
                  if ledger['blocks'][ledger['columns']['block_ref'][i]]['page']==record['source_refs'][0]['page']+1
                  and re.match(r'^\s*[-–•]\s',raw[n-1])]
            if hits:
                fragment,nodes=text_nodes(d,lp,hits[0])
                result.append(dict(case_id='text_historical_continuation',route='TEXT',split='DEV_HISTORICAL_DIAGNOSTIC',
                                   fragment=fragment,nodes=nodes,document=d,side='NEW',ledger_path=str(lp)))
    return result


def refresh():
    """Rebuild derived evidence for fixed DEV selectors; preserves prior run."""
    if (ROOT/'reports/CANDIDATE_MANIFEST.json').exists():
        raise ValueError('Candidate frozen; start a separate experiment to change inputs')
    protect()
    cases=read(ROOT/'cases.json'); manifest=read(ROOT/'INPUT_MANIFEST.json')
    revision=ROOT/'iterations'/('before_'+digest(cases)[:16]);write(revision/'cases.json',cases);write(revision/'INPUT_MANIFEST.json',manifest)
    for c in cases:
        if c['route']=='TABLE':
            c['fragment'],c['nodes']=table_nodes(c['subject'])
        else:
            lp=c.get('ledger_path') or PREVIOUS/'v3'/c['document']['document_version']/'ledger.json'
            c['fragment'],c['nodes']=text_nodes(c['document'],lp,c['fragment']['interval'][0])
    if not any(c['case_id']=='text_historical_OLD' for c in cases):
        cases.extend(historical_cases())
    for c in cases:
        for item in c['fragment']['source_evidence'][0]['source_receipts'].values():
            if isinstance(item,dict) and 'path' in item and 'sha256' in item:
                manifest['sources'][item['path']]=item['sha256']
    manifest.update(cases_sha256=digest(cases),case_ids=[c['case_id'] for c in cases],
                    revision_reason='DEV raster hardening, same three approaches; historical list diagnostic admitted explicitly as DEV')
    write(ROOT/'cases.json',cases);write(ROOT/'INPUT_MANIFEST.json',manifest)
    print('Refreshed fixed DEV cases',dict(Counter(c['route'] for c in cases)),flush=True)


def prepare():
    if (ROOT/'cases.json').exists():
        raise ValueError('Cases already pinned; replay instead')
    protect()
    documents = read(PREVIOUS/'DOCUMENTS.json')
    cases = []; missing = []
    for mapping in read(PREVIOUS/'private/BLIND_MAPPING.json'):
        p = read(PREVIOUS/'packets'/(mapping['candidate_id']+'.json'))
        targets = [('NEW', p['new'])]
        if mapping['case_id'] == 'case_023':
            targets += [('OLD_2', next(r['subject'] for r in p['old_candidates']
                                      if r['subject']['subject_id'] == mapping['labels']['OLD_2']))]
        for label, subject in targets:
            fragment, nodes = table_nodes(subject)
            cases.append(dict(case_id=mapping['case_id']+'_'+label, route='TABLE',
                              split='DEV_PREVIOUSLY_INSPECTED', fragment=fragment, nodes=nodes,
                              previous_mapping=mapping, subject=subject))
    probes = read(PREVIOUS/'reports/EXCLUDED_NARRATIVE_PROBES.json')
    for pidx, probe in enumerate(probes, 1):
        ds = [d for d in documents if d['document_code'] == probe['document']]
        for d in ds:
            side = 'NEW' if d['version_id'] == max(x['version_id'] for x in ds) else 'OLD'
            lp = PREVIOUS/'v3'/d['document_version']/'ledger.json'
            ledger, raw = source_data(str(lp))
            for idx, note in enumerate(probe['notes'], 1):
                prefix = re.sub(r'\W+', '', note['excerpt'].casefold())[:45]
                hits = [i for i,n in enumerate(ledger['columns']['markdown_line'])
                        if prefix in re.sub(r'\W+', '',raw[n-1].casefold())]
                if len(hits) != 1:
                    missing.append(dict(document=d['document_code'], side=side, probe=idx,
                                        reason='NO_UNIQUE_EXACT_PREFIX', occurrences=len(hits)))
                    continue
                fragment, nodes = text_nodes(d, lp, hits[0])
                cases.append(dict(case_id=f'text_{pidx:02d}_{idx}_{side}', route='TEXT',
                                  split='DEV_PREVIOUSLY_INSPECTED', fragment=fragment, nodes=nodes,
                                  document=d, side=side))
                print(cases[-1]['case_id'], fragment['source_type'], flush=True)
    # Natural list items from the diagnosed general-notes page. Selection is by
    # source syntax, not by output acceptance or expected numeric change.
    d = next(d for d in documents if d['document_code'] == probes[0]['document'] and d['version_id'] == 'v002')
    lp = PREVIOUS/'v3'/d['document_version']/'ledger.json'; ledger, raw = source_data(str(lp))
    candidates = [i for i,n in enumerate(ledger['columns']['markdown_line'])
                  if ledger['blocks'][ledger['columns']['block_ref'][i]]['page'] == probes[0]['notes'][0]['page']
                  and re.match(r'^-\s+(?:температура|скорость|относительная|жилая|кухня|санузел)',raw[n-1])]
    for k,i in enumerate(candidates[:6],1):
        fragment,nodes = text_nodes(d,lp,i)
        cases.append(dict(case_id=f'text_list_{k:02d}',route='TEXT',split='DEV_DIAGNOSTIC',
                          fragment=fragment,nodes=nodes,document=d,side='NEW'))
    sources = {}
    for d in documents:
        for item in d['artifacts'].values():
            sources[item['path']] = item['sha256']
    for path,sha in sources.items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != sha:
            raise ValueError('Source receipt mismatch: '+path)
    write(ROOT/'INPUT_MANIFEST.json', dict(split='DEV', sources=sources, missing_probes=missing,
                                          cases_sha256=digest(cases), case_ids=[c['case_id'] for c in cases]))
    write(ROOT/'cases.json', cases)
    print('PINNED', dict(Counter(c['route'] for c in cases)), 'missing', len(missing), flush=True)


def enrich(subject, binding):
    """Only existing clue/context fields receive certified source scope.

    No state/header/cell/unit text rewriting. A group mark is never inherited
    as an individual indoor member's own mark.
    """
    s = deepcopy(subject)
    if binding['status'] != 'PROVEN':
        s['purity'] = 'UNPROVEN'
        return s
    dims = binding['dimensions']
    for dimension, field in [('floor','floor'), ('room','room'), ('system','system'),
                             ('apartment_or_unit','served_zone'), ('heading','local_heading')]:
        if dims[dimension]:
            s['clues'][field] = list(dims[dimension])
    if s['source_type'] == 'TABLE':
        from .source import NUMBERED_EQUIPMENT
        match = NUMBERED_EQUIPMENT.match(s['cells'][0])
        if match and dims['equipment_group']:
            s['clues']['mark'] = dims['equipment_group']
            s['clues']['equipment_class'] = [clean(match[1]).casefold()]
    # A data group from the first row is not a column header of all later rows.
    # Context is replaced by actual ancestors; raw column headers/state remain
    # intact for the frozen ProjectChange consumer.
    s['context'] = [dict(kind='source_scope', text=n['dimension']+': '+n['value'],
                         scope_evidence=n['scope_evidence']) for n in binding['scope_nodes']]
    return s


def text_subject(case, binding):
    from experiments.engineering_subject_resolver_v1.core import make_subject, features
    from experiments.project_change_text_v1.engine import evidence
    f=case['fragment']; d=case['document']
    refs=[dict(document_version=r['document_version'],line_id=r['ledger_line'],page=r['page'],
               block_id=r['block_id'],markdown_line=r['markdown_line'],within_block_line=r['within_block_line'],
               line_sha256=r['line_sha256'],edge='LINE') for r in f['source_evidence']]
    headings=binding['dimensions']['heading'] or []
    contexts=[dict(instance_id=n['scope_id'],section_key=n['value'],title=n['value'],heading_path=[n['value']],status=n['status'])
              for n in binding['scope_nodes'] if n['dimension']=='heading']
    unit=dict(unit_id=f['fragment_id'],document_version=d['document_version'],document_code=d['document_code'],
              text=f['text'],source_refs=refs,
              source_receipts={k:{z:v[z] for z in ('path','sha256')} for k,v in d['artifacts'].items()
                               if k in ('pdf','blocks','work_md')},section_context=contexts,
              nearest_heading=headings[-1] if headings else None,section_ownership_proven=binding['status']=='PROVEN',
              eligibility='ELIGIBLE',review_reasons=[],source_route='TEXT',
              text_purity_basis=[f['admission']['basis']],page_span=f['pages'],block_ids=[refs[0]['block_id']])
    comparison=('scope-dev/historical-cooling' if case['case_id'].startswith('text_historical')
                else 'scope-dev/'+d['project']+'/'+d['document_code'])
    return make_subject(source_type='TEXT',side=case['side'],version=d['document_version'],scope=comparison,
                        text=f['text'],evidence=[evidence(unit)],clues=features(f['text'],' / '.join(headings),d['discipline']),
                        unit=unit,purity='PROVEN' if binding['status']=='PROVEN' else 'UNPROVEN',local_id=f['fragment_id'])


def downstream():
    from experiments.engineering_subject_resolver_v1.core import deterministic, rank
    from experiments.engineering_subject_resolver_v1.source import packet
    from experiments.engineering_subject_resolver_v1.bridge import apply
    from experiments.engineering_subject_resolver_v1.schema import SUBJECT, RELATION
    import jsonschema
    protect()
    cases=read(ROOT/'cases.json')
    bindings={r['case_id']:r['binding'] for r in read(ROOT/'scope_results.json') if r['approach']=='certified_hierarchy'}
    cache={}; outcomes=[]
    def scoped(s):
        if s['subject_id'] not in cache:
            f,n=table_nodes(s);b=bind(f,n);cache[s['subject_id']]=dict(subject=enrich(s,b),binding=b)
        return deepcopy(cache[s['subject_id']]['subject'])
    def literal_only(s):
        from .source import NUMBERED_EQUIPMENT
        s=deepcopy(s);m=NUMBERED_EQUIPMENT.match(s['cells'][0])
        if m:
            s['clues']['mark']=['№'+clean(m[2])];s['clues']['equipment_class']=[clean(m[1]).casefold()]
        return s
    for c in cases:
        if c['route']!='TABLE' or not c['case_id'].endswith('_NEW'):
            continue
        original=read(PREVIOUS/'packets'/(c['previous_mapping']['candidate_id']+'.json'))
        q=enrich(original['new'],bindings[c['case_id']]);old=[scoped(r['subject']) for r in original['old_candidates']]
        after=packet(original['candidate_id'],q,rank(q,old))
        lq=literal_only(original['new']);lo=[literal_only(r['subject']) for r in original['old_candidates']]
        literal=packet(original['candidate_id'],lq,rank(lq,lo))
        result=deterministic(after);before=deterministic(original);literal_result=deterministic(literal)
        for s in [q]+old:
            jsonschema.Draft202012Validator(SUBJECT).validate(s)
        jsonschema.Draft202012Validator(RELATION).validate(result)
        previous=read(PREVIOUS/'decisions'/(original['candidate_id']+'.json'))
        bridge=apply(after,result)
        row=dict(case_id=c['case_id'],route='TABLE',baseline=before,literal_only=literal_result,
                 previous_hybrid=previous,scoped=result,bridge=bridge,
                 scope_delta=result['relation']=='SAME_SUBJECT' and literal_result['relation']!='SAME_SUBJECT',
                 previous_hybrid_delta=result['relation']=='SAME_SUBJECT' and previous['relation']!='SAME_SUBJECT')
        outcomes.append(row);write(ROOT/'downstream_packets'/(c['case_id']+'.json'),after)
        print(c['case_id'],before['relation'],literal_result['relation'],result['relation'],flush=True)
    subjects=[]
    for c in cases:
        if c['route']=='TEXT':
            s=text_subject(c,bindings[c['case_id']]);s=enrich(s,bindings[c['case_id']]);subjects.append((c,s))
            jsonschema.Draft202012Validator(SUBJECT).validate(s)
    for c,q in subjects:
        if c['side']!='NEW':continue
        olds=[s for _,s in subjects if s['side']=='OLD'];retrieved=rank(q,olds) if q['purity']=='PROVEN' else []
        p=packet(c['case_id'],q,retrieved);result=deterministic(p);bridge=apply(p,result)
        outcomes.append(dict(case_id=c['case_id'],route='TEXT',scoped=result,bridge=bridge,
                             retrieved_candidates=len(retrieved),scope_delta=result['relation']=='SAME_SUBJECT',
                             previous_hybrid_delta=result['relation']=='SAME_SUBJECT'))
        write(ROOT/'downstream_packets'/(c['case_id']+'.json'),p)
    write(ROOT/'downstream_scope_cache.json',cache);write(ROOT/'DOWNSTREAM_RESULTS.json',outcomes)
    funnel={}
    for side in ('OLD','NEW'):
        subset=[c for c in cases if c['route']=='TEXT' and c['side']==side]
        selected=[o for o in outcomes if o['route']=='TEXT'] if side=='NEW' else []
        funnel[side]=dict(source_fragments=len(subset),eligible=sum(c['fragment']['source_type']=='NARRATIVE_TEXT' for c in subset),
                          scoped=sum(bindings[c['case_id']]['status']=='PROVEN' for c in subset),
                          queries_with_identity_candidates=sum(o['retrieved_candidates']>0 for o in selected),
                          identity_candidate_pairs=sum(o['retrieved_candidates'] for o in selected),
                          matched=sum(o['scoped']['relation']=='SAME_SUBJECT' for o in selected))
    write(ROOT/'TEXT_FUNNEL.json',funnel)
    print('FUNNEL',funnel,flush=True);protect()


def replay():
    protect()
    cases = read(ROOT/'cases.json'); truth=read(ROOT/'audit/expected.json')
    if digest(cases) != read(ROOT/'INPUT_MANIFEST.json')['cases_sha256']:
        raise ValueError('Pinned cases changed')
    outputs=[]
    for approach in APPROACHES:
        for case in cases:
            result=bind(case['fragment'],case['nodes'],approach)
            expected=truth[case['case_id']]
            actual=result['dimensions']
            wrong=[]; unassessed=[]
            for dim,values in actual.items():
                if dim in ('table','discipline') or not values:
                    continue
                if dim not in expected['dimensions']:
                    unassessed.append(dim)
                elif not set(values) <= set(expected['dimensions'][dim]):
                    wrong.append(dim)
            missing=[dim for dim in expected.get('required_dimensions',[]) if not actual[dim]]
            audit='false' if wrong else 'unsure' if unassessed or missing or expected.get('unsure') else 'correct'
            outputs.append(dict(case_id=case['case_id'],route=case['route'],approach=approach,
                                binding=result,audit=audit,wrong_dimensions=wrong,
                                unassessed_dimensions=unassessed,missing_dimensions=missing))
    write(ROOT/'scope_results.json',outputs)
    score={}
    for approach in APPROACHES:
        score[approach]={}
        for route in ('TEXT','TABLE'):
            rows=[r for r in outputs if r['approach']==approach and r['route']==route]
            proven=[r for r in rows if r['binding']['status']=='PROVEN']
            counts=Counter(r['audit'] for r in proven)
            score[approach][route]=dict(cases=len(rows),proven=len(proven),
                correct=counts['correct'],false=counts['false'],unsure=counts['unsure'],
                unresolved=len(rows)-len(proven),precision=counts['correct']/len(proven) if proven else None,
                wrong_parent_bindings=counts['false'],
                source_contamination=sum(r['binding']['source_type'] != {'TEXT':'NARRATIVE_TEXT','TABLE':'TABLE'}[route] for r in proven))
    write(ROOT/'SCORECARD.json',score)
    print(json.dumps(score,ensure_ascii=False,indent=2))
    protect()


if __name__ == '__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('action',choices=['prepare','refresh','replay','downstream'])
    args=parser.parse_args()
    globals()[args.action]()
