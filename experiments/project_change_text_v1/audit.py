"""Post-freeze reporting and audit preparation; never changes candidate decisions."""
from collections import Counter, defaultdict
import hashlib
from pathlib import Path
import re
import shutil
from experiments.text_comparison_v1.common import read, write, digest, file_hash
from .approaches import ROOT
from .contract import validate
from .engine import canonical
from .run import verify


def md(path, text):
    Path(path).write_text(text)


def provenance(root, changes):
    lines = {}; checked = set(); refs = 0; evs = {}
    for c in changes:
        validate(c)
        for e in c['evidence_old'] + c['evidence_new']:
            evs[e['evidence_id']] = e
            for receipt in e['source_receipts'].values():
                p = receipt['path']
                if p not in checked:
                    assert file_hash(p) == receipt['sha256'], p
                    checked.add(p)
            p=e['source_receipts']['work_md']['path']
            if p not in lines: lines[p]=Path(p).read_text().splitlines()
            local=[]
            for ref in e['source_refs']:
                raw=lines[p][ref['markdown_line']-1]
                assert hashlib.sha256(raw.encode()).hexdigest()==ref['line_sha256']
                assert ref['document_version']==e['document_version']
                local.append(raw);refs+=1
            normalize=lambda s:re.sub(r'\s+',' ',s).strip()
            assert normalize(e['quote']) in normalize(' '.join(local)), e['evidence_id']
    result=dict(passed=True,project_changes=len(changes),unique_evidence=len(evs),
                line_references_checked=refs,source_artifacts_checked=len(checked),
                checks=['JSON Schema','semantic acceptance invariants','artifact SHA256',
                        'source line SHA256','quote in cited lines','version identity'])
    write(root/'reports/PROVENANCE_AUDIT.json',result)
    return result


def duplicates(changes):
    buckets=defaultdict(list)
    for c in changes:
        # Suspect inventory only: scope may legitimately differ. Do not mutate
        # predictions or pretend a normalized quote is an entity identity proof.
        key=(c['comparison_scope'],canonical(c['new_state'] or ''),c['status'])
        buckets[key].append(c['project_change_id'])
    return [v for v in buckets.values() if len(v)>1]


def blind_packet(root, project):
    cases=[];key={}
    def add(old,new,origin):
        keep=('document_version','document_code','quote','source_refs','source_receipts','locator')
        old=[{k:v for k,v in e.items() if k in keep} for e in old]
        new=[{k:v for k,v in e.items() if k in keep} for e in new]
        cid='blind_'+digest([old,new])[:20]
        if cid in key:return
        key[cid]=origin
        cases.append(dict(case_id=cid,old=old,new=new,
                          annotation=dict(text_only=None,is_real_change=None,event_description_ru=None,
                                          event_group=None,evidence_sufficient=None,reason=None)))
    proven=[c for c in project['project_changes'] if c['status']=='PROVEN']
    review=[c for c in project['project_changes'] if c['status']=='REVIEW']
    for c in proven+sorted(review,key=lambda c:digest(['blind-v1',c['project_change_id']]))[:18]:
        add(c['evidence_old'],c['evidence_new'],dict(kind='candidate',project_change_id=c['project_change_id']))
    from .engine import evidence
    docs={p.parent.name:read(p) for p in (root/'run1/documents').glob('*/narrative.json')}
    units={u['unit_id']:u for d in docs.values() for u in d['units']}
    unchanged=[]
    for p in (root/'run1/pairs').glob('*.json'):
        for rel in read(p)['alignments']:
            if rel['decision']=='UNCHANGED':unchanged.append(rel)
    for rel in sorted(unchanged,key=digest)[:12]:
        add([evidence(units[rel['old_unit_ids'][0]])],[evidence(units[rel['new_unit_id']])],dict(kind='unchanged_local'))
    quarantine=[]
    for version,d in docs.items():
        for q in d['quarantined']:quarantine.append((version,q))
    manifest=read(root/'FROZEN_PROJECT_INPUTS.json')
    descriptors={d['document_version']:d for p in manifest['pairs'] for d in (p['old'],p['new'])}
    for version,q in sorted(quarantine,key=digest)[:12]:
        d=descriptors[version];lines=Path(d['artifacts']['work_md']['path']).read_text().splitlines()
        text=' '.join(lines[ref['markdown_line']-1] for ref in q['source_refs'])
        add([], [dict(document_version=version,document_code=d['document_code'],quote=text[:3000],
                      source_refs=q['source_refs'],source_receipts=d['artifacts'])],dict(kind='quarantined_source'))
    cases.sort(key=lambda c:digest(['shuffle',c['case_id']]))
    write(root/'blind_validation/LOCAL_CASES.json',dict(instructions='Independent annotator: inspect only local cited pages. Classify TEXT purity, real event, identity/grouping and sufficiency. No answers supplied. This is a prepared validation packet, not completed human validation.',cases=cases))
    write(root/'quality_audit/BLIND_KEY_DO_NOT_SHOW.json',key)
    return len(cases)


def historical(root, changes):
    # Diagnostics deliberately live outside frozen inference code and run only
    # after candidate freeze. They do not select candidates or change thresholds.
    diagnostics=[]
    patterns=[('specific_load_60_100',r'100\s*Вт/м'),('fan_coil_pipe',r'4-х трубные фанкойлы'),
              ('pon_area',r'492.51\s*м2'),('unmatched_cooling',r'2585\s*кВт')]
    for label,pattern in patterns:
        hits=[c for c in changes if re.search(pattern,c['new_state'] or '',re.I)]
        diagnostics.append(dict(case=label,project_change_ids=[c['project_change_id'] for c in hits],
                                statuses=[c['status'] for c in hits],
                                result='FOUND' if hits else 'NOT_EMITTED_OR_QUARANTINED'))
    write(root/'reports/HISTORICAL_SMOKE_TESTS.json',dict(post_freeze_only=True,not_blind=True,cases=diagnostics))
    return diagnostics


def product_report(project):
    m=project['metrics'];lines=['# Изменения проекта — TEXT V1','',
        f"Подтверждённые текстом изменения: **{m['proven']}**. Кандидаты для проверки: **{m['review']}**.",
        '', 'PROVEN означает подтверждение локальными источниками. Полнота изменений проекта не установлена; REVIEW не утверждает, что изменение произошло.', '']
    for c in sorted(project['project_changes'],key=lambda c:(c['status']!='PROVEN',c['project_change_id'])):
        if c['status']=='REVIEW' and not any('<summary>Кандидаты REVIEW' in s for s in lines):
            lines += ['<details><summary>Кандидаты REVIEW — требуют сопоставления источников</summary>','']
        title=c['short_summary_ru']
        if c['status']=='PROVEN' and c['change_type']!='SYSTEM_TYPE_CHANGED':
            prefix=re.split(r'\bсоставля\w*\b',c['old_state'],maxsplit=1)[0].strip()
            if prefix==c['old_state']:
                prefix=re.split(r'\d',prefix,maxsplit=1)[0].strip()
            f=c['supporting_fact_changes'][0]
            title=f"{prefix}: {f['old']['quote']} → {f['new']['quote']}."
        lines += [f"## {title} [{c['status']}]",'',
                  f"**Где:** {c['evidence_new'][0]['document_code'] if c['evidence_new'] else c['evidence_old'][0]['document_code']}",
                  '',f"**Было:** {c['old_state'] or 'Соответствующий контекст OLD не установлен.'}",
                  '',f"**Стало:** {c['new_state'] or 'Контекст NEW не установлен.'}",'',
                  '**Почему система так решила:** '+('Сохранено локальное утверждение об одном предмете; явно различаются приведённые значения. Область сравнения подтверждена локальным контекстом.' if c['status']=='PROVEN' else 'Недостаточно доказательств для утверждения об изменении: '+', '.join(c['review_reasons'])),
                  '', '**Источники доказательств:**','']
        for side in ('old','new'):
            for e in c['evidence_'+side]:
                refs=e['source_refs'];pages=', '.join(map(str,sorted({r['page'] for r in refs})))
                p=e['source_receipts']['work_md']['path'];lineno=refs[0]['markdown_line']
                lines.append(f"- {side.upper()}: [{e['document_code']}; PDF стр. {pages}](<{p}:{lineno}>); версия `{e['document_version'][:12]}`; evidence `{e['evidence_id']}`.")
        lines += ['', '<details><summary>Подробности</summary>','',
                  f"ProjectChange: `{c['project_change_id']}`; тип: `{c['change_type']}`.", '']
        for f in c['supporting_fact_changes']:
            lines.append(f"- {f['property']}: {f['old']['quote']} → {f['new']['quote']} (`{f['fact_id']}`).")
        if not c['supporting_fact_changes']:lines.append('Парные фактические различия пока не доказаны.')
        lines += ['', '</details>', '']
    if any(c['status']=='REVIEW' for c in project['project_changes']):lines += ['</details>','']
    return '\n'.join(lines)


def run(root=ROOT):
    verify(root);project=read(root/'run1/project.json');changes=project['project_changes'];m=project['metrics']
    prov=provenance(root,changes);suspects=duplicates(changes);nblind=blind_packet(root,project)
    hist=historical(root,changes)
    write(root/'reports/PROJECT_TEXT_CHANGES.json',project)
    md(root/'reports/PROJECT_TEXT_CHANGE_REPORT.md',product_report(project))
    paired=[c for c in changes if c['supporting_fact_changes']]
    perf=read(root/'run1/performance.json');perf2=read(root/'run2/performance.json')
    audit=read(root/'quality_audit/AGENT_ASSESSMENT.json')
    score=dict(**m,agent_audit=audit,paired_fact_project_changes=len(paired),
               unpaired_review_candidates=m['project_changes']-len(paired),
               duplicate_review_suspect_groups=suspects,blind_packet_cases=nblind,
               human_blind_validation_completed=False,corpus_recall=None,
               verdict='B',stop_status='BLOCKED',architecture_iterations=3,
               safety=dict(text_only=True,table_compared=False,graphic_compared=False,
                           production_modified=False,push=0,deploy=0,flags_added=0,human_truth_modified=False))
    write(root/'reports/SCORECARD.json',score)
    md(root/'reports/PROJECT_CHANGE_SCORECARD.md',f'''# ProjectChange scorecard

Verdict B — PROJECTCHANGE GROUPING WORKS, TEXT ALIGNMENT STILL LIMITS COVERAGE.
Stop status: BLOCKED for full TEXT readiness after the allowed three architecture iterations. The research deliverables are complete.

| Metric | Result | Scope |
|---|---:|---|
| Documents / pairs / pages | {m['documents']} / {m['pairs']} / {m['pages']} | Frozen project corpus |
| Detected paired fact differences | {m['raw_fact_differences']} | Excludes unaligned unknown differences |
| Candidates before deduplication | {m['pre_dedup_candidates']} | TEXT |
| ProjectChanges | {m['project_changes']} | {len(paired)} with paired deltas, {m['project_changes']-len(paired)} without OLD correspondence |
| PROVEN / REVIEW | {m['proven']} / {m['review']} | Candidate decision, not human truth |
| False PROVEN events | {audit['false_proven']} | All six PROVEN checked by this agent against local OLD/NEW rasters |
| Duplicate PROVEN events | {audit['duplicate_proven']} | Six accepted events |
| Over-grouped PROVEN events | {audit['over_grouped_proven']} | Six accepted events |
| Under-grouped PROVEN events | {audit['under_grouped_proven']} | Six accepted events; unseen/missing facts not scored |
| Remaining duplicate REVIEW suspect groups | {len(suspects)} | Exact repeated text; identity still needs review |
| Full-corpus false/duplicate/over-/under-grouping | NOT ESTABLISHED | No independent exhaustive event truth |
| Constructed regression tests | 25 passed | Software contracts, not natural corpus precision |

Quality audit A: six source-backed engineering changes found. B: a repeated power event is now one event with four source passages. C: separate buildings/marks/modes/inequalities stay separate in DEV; no over-grouping observed among six accepted events. D: equipment replacement with twenty numeric characteristics collapses in constructed DEV; no natural replacement was proven here. E: meaningful standalone capacity/area/height changes survive; the specific-load diagnostic remains unresolved and illustrates missing coverage.

This is an agent source audit after diagnostic exposure, not blind validation, and 6/6 observed correctness must not be generalized to unseen projects. Schema readiness does not mean cross-route linking is implemented.
''')
    md(root/'reports/TEXT_ALIGNMENT_REPORT.md',f'''# TEXT alignment report

Frozen candidate: {project['candidate_content_hash']}. Evaluated {m['pairs']} pairs independently of SectionRelation or Sheet Matcher.
Eligible local sentences OLD={m['old_units']}, NEW={m['new_units']}. OLD units without an unchanged/typed match={m['unmatched_old_units']} (this includes unsupported units and is not a recall denominator). Quarantined input paragraphs={m['quarantined_units']}.

The matcher proves complete typed assertions and corroborates scope through section context, explicit mark or unique adjacent content. Exact-template retrieval is content-first, but paraphrase, grammatical inflection and list context changes still limit coverage. One-sided new scope is REVIEW, never equipment addition. Unmatched old assertions do not prove removal.

Post-freeze diagnostics: {hist}. The specific-load case is not accepted as a change; supporting 60/100 values alone cannot repair absent scope/alignment. Fan-coil and PON area cases are accepted; the new cooling total is REVIEW. No diagnostic is treated as blind success.

Audit found table calculation labels in iteration 2 REVIEW. Iteration 3 excludes unit-ended definitions, math markup, non-assertive fragments and commercial text. All six final PROVEN statements were visually verified as narrative TEXT. Purity of all remaining REVIEW and excluded material is not exhaustively raster-labelled; no perfect corpus purity claim is made.
''')
    md(root/'reports/PROJECT_CHANGE_GROUPING_REPORT.md',f'''# Grouping report

Three pre-implementation methods tested; guarded hybrid selected. Hierarchy absorbs local numeric consequences of explicit single-equipment replacement, while count/mode/material/requirements remain separate events. No page-based grouping. Separate scopes cannot merge by number alone.

Corpus: {m['pre_dedup_candidates']} candidates → {m['project_changes']} event candidates; {m['duplicate_occurrences_merged']} repeated event occurrence merged. The repeated building power event retains two OLD and two NEW source passages. Detected paired evidence: {m['raw_fact_differences']} facts → {len(paired)} events ({m['proven']} PROVEN, {len(paired)-m['proven']} REVIEW). The other {m['project_changes']-len(paired)} candidates are missing-counterpart REVIEW, outside the compression denominator.

Exact repeated REVIEW quote groups requiring identity review: {suspects}. They are not automatically merged across different section scopes. Arbitrary paraphrase and document-to-document semantic aliases remain unsupported. No natural equipment replacement was found, so parameter-explosion success on the project corpus is unmeasured; constructed DEV demonstrates 21 facts → one replacement.
''')
    md(root/'reports/PARAMETER_EXPLOSION_AUDIT.md',f'''# Parameter explosion audit

Constructed DEV: model replacement plus 20 changing numeric characteristics → 21 internal facts → 1 EQUIPMENT_REPLACED. Repeated descriptions preserve evidence without multiplying the event. The test contains no historical diagnostic numbers.

Real corpus: raw detected paired differences {m['raw_fact_differences']} → {len(paired)} paired ProjectChanges, including {m['proven']} PROVEN. This is duplicate collapse, not a measured real replacement compression result. {m['project_changes']-len(paired)} unpaired REVIEW candidates cannot be included in the raw fact compression denominator. No claim that the entire project has only eight differences.

Risk: replacement parameters in separate reworded paragraphs or without an explicit model/identity certificate remain unresolved, and dependent engineering consequences are not inferred from arithmetic or co-location.
''')
    md(root/'reports/PERFORMANCE_REPORT.md',f'''# Performance

Run 1: {perf['seconds']:.3f} s. Run 2: {perf2['seconds']:.3f} s. Peak RSS run 1: {perf['peak_rss_kib']/1024:.1f} MiB. JSON artifacts: {perf['artifact_bytes']} bytes. Corpus: {m['pairs']} pairs, {m['pages']} source pages.

Times include source receipt verification, existing paragraph materialization, TEXT purity, local matching, grouping, schema checks and serialization. Audit/raster rendering and dependency installation are outside this runtime. No fresh OCR was run. Model calls=0, model input/output tokens=0. No whole-PDF/project AI input. Per-unit comparison budget=3000 characters. Template postings avoid an all-pairs document comparison. Dependency versions are pinned in CANDIDATE_MANIFEST.json.

Replay compares all 70 deterministic JSON artifacts, excluding measured runtime/RSS. See REPLAY_AUDIT.json. Provenance verification: {prov}.
''')
    next_text=f'''# Exact next step

Send the prepared {nblind}-case local source packet `blind_validation/LOCAL_CASES.json` to an independent engineer for blind annotation of TEXT purity, actual engineering events, duplicate groups and evidence sufficiency. Do not show `quality_audit/BLIND_KEY_DO_NOT_SHOW.json` or predictions. No external message has been sent.

After annotation, measure false events and missed event coverage by subject, especially reworded/list assertions and replacement parameters spread across paragraphs. A future task can compare bounded lexical retrieval/local semantic alignment against this frozen candidate. No fourth architecture iteration, feature flags, production integration, Table/Graphic implementation, push or deploy is part of this task.

Current readiness: BLOCKED; verdict B. The current result is suitable as an auditable research candidate and prepared annotation package; full TEXT readiness is not established.
'''
    md(root/'reports/NEXT_ACTION.md',next_text)
    checkpoint=f'''# Final checkpoint

TEXT PROJECT CHANGE V1 COMPLETE. Three approaches compared; three architecture iterations used, then stopped. Verdict B / BLOCKED for full readiness. Reports and implementation complete. Main candidate commit: {read(root/'reports/CANDIDATE_MANIFEST.json')['candidate_commit']}. Candidate hash: {project['candidate_content_hash']}.

Corpus: {m['raw_fact_differences']} paired fact differences, {m['project_changes']} candidates, {m['proven']} PROVEN, {m['review']} REVIEW. Constructed tests: 25 passed. Six PROVEN visually audited; false/duplicate/over-/under-grouped accepted events observed: 0. Full REVIEW/corpus quality remains unlabelled. Earlier raw runs and findings remain in iteration1/ and iteration2/.

Source receipts and line hashes checked. Replay passed. Project report shows engineering events first, atomic evidence under details. Future multi-route schema prepared; no Table/Graphic comparison, production modification, push, deploy, flags, Sheet Matcher or human truth changes. Pre-existing untracked restore files were untouched. Next step: independent annotation of the {nblind}-case prepared local packet.
'''
    md(root/'reports/CHECKPOINT.md',checkpoint);md(root/'CHECKPOINT.md',checkpoint)
    print(dict(metrics=m,provenance=prov,blind_cases=nblind,duplicate_review_suspects=suspects))


if __name__=='__main__':run()
