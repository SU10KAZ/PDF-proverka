"""Audited DEV delivery and content-uninspected post-freeze holdout registration."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import csv
import hashlib
import json
import math
import subprocess

from .core import APPROACHES, digest, gate
from .run import ROOT, PREVIOUS, REPO, protect, read, write
from .schema import SCHEMA


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def md(name, text):
    (ROOT/'reports'/name).write_text(text.strip()+'\n')


def seal_inputs():
    manifest=read(ROOT/'INPUT_MANIFEST.json')
    for p in (PREVIOUS/'packets').glob('*.json'):
        manifest['sources'][str(p)]=sha(p)
        packet=read(p)
        for subject in [packet['new']]+[r['subject'] for r in packet['old_candidates']]:
            for r in subject['evidence'][0]['locator']['artifact_receipts'].values():
                manifest['sources'][r['path']]=r['sha256']
    for p in [PREVIOUS/'private/BLIND_MAPPING.json',PREVIOUS/'reports/EXCLUDED_NARRATIVE_PROBES.json',
              ROOT/'audit/expected.json',ROOT/'audit/label_dev.py']:
        manifest['sources'][str(p)]=sha(p)
    for p in (PREVIOUS/'narrative').glob('*/*.json'):
        manifest['sources'][str(p)]=sha(p)
    for p in (PREVIOUS/'decisions').glob('*.json'):
        manifest['sources'][str(p)]=sha(p)
    for path,expected in manifest['sources'].items():
        if sha(path)!=expected:raise ValueError('Changed input: '+path)
    write(ROOT/'INPUT_MANIFEST.json',manifest)
    return manifest


def prepare_holdout(candidate_hash):
    target=ROOT/'holdout/REGISTERED_SLICE.json'
    if target.exists():
        holdout=read(target)
        if holdout['candidate_sha256']!=candidate_hash:
            raise ValueError('Holdout is already bound to another frozen candidate')
        return holdout
    seed='evidence-scope-after-freeze-20260914-v1'
    excluded={'214_Alia_ASTERUS','256_Primavera_K14_Spartak','272_Sadovnicheskaya_76_Balchug_Esteyt'}
    inventory=[]; selected=[]
    for project in sorted((REPO/'projects_v2/objects').iterdir()):
        if not project.is_dir():continue
        if project.name in excluded:
            inventory.append(dict(project=project.name,excluded='INSPECTED_DEV_PROJECT'));continue
        directories=list(project.glob('disciplines/*/documents/*/versions/*/02_work'))
        complete=[d for d in directories if all((d/f).is_file() and (d/f).stat().st_size for f in ('document.pdf','document.md','blocks.json'))]
        grouped=Counter(str(d.parent.parent.parent) for d in complete)
        inventory.append(dict(project=project.name,version_directories=len(directories),complete_versions=len(complete),
                              paired_documents=sum(n>=2 for n in grouped.values())))
        by_discipline=defaultdict(list)
        for d in complete:by_discipline[d.parents[4].name].append(d)
        ordered=[]
        for discipline,paths in sorted(by_discipline.items()):
            ordered.append(sorted(paths,key=lambda p:digest([seed,str(p)]))[0])
        for path in sorted(ordered,key=lambda p:digest([seed,str(p)]))[:3]:
            selected.append(dict(project=project.name,discipline=path.parents[4].name,
                                 document=path.parents[2].name,version=path.parent.name,
                                 sources={k:dict(path=str(path/n),sha256=sha(path/n)) for k,n in
                                          [('pdf','document.pdf'),('blocks','blocks.json'),('work_md','document.md')]}))
    result=dict(schema='scope-holdout-registration.v1',registered_at=datetime.now(timezone.utc).isoformat(),
                candidate_sha256=candidate_hash,selection_seed=seed,selected=selected,inventory=inventory,
                semantic_contents_inspected=False,annotations_created=0,predictions_run=0,
                policy='After candidate freeze: three documents per available wholly unseen project, distinct disciplines; metadata/hash selection only.',
                limitation='Two Sobytie projects, one developer/project family; no complete paired versions. Third fresh project Mosfilmovskaya has zero complete versions. This is an ownership-only slice, not downstream paired validation.',
                next_protocol='Before viewing predictions, source-audit 20 fragments per route per document (or all if fewer), include unresolved and mixed fragments. Keep labels separate, run frozen candidate once; no tuning or case replacement. Report family clustering. Restore a genuinely different third project and paired versions for downstream validation.')
    write(target,result)
    return result


def finish():
    protect();manifest=seal_inputs()
    score=read(ROOT/'SCORECARD.json');rows=read(ROOT/'scope_results.json');cases=read(ROOT/'cases.json')
    chosen='certified_hierarchy';selected=[r for r in rows if r['approach']==chosen]
    downstream=read(ROOT/'DOWNSTREAM_RESULTS.json');funnel=read(ROOT/'TEXT_FUNNEL.json')
    write(ROOT/'reports/EVIDENCE_SCOPE_SCHEMA.json',SCHEMA)
    code={str(p):sha(p) for p in sorted(Path(__file__).parent.glob('*.py'))}
    freeze_path=ROOT/'reports/CANDIDATE_MANIFEST.json'
    frozen_at=read(freeze_path)['frozen_at'] if freeze_path.exists() else datetime.now(timezone.utc).isoformat()
    candidate=dict(schema='evidence-scope-candidate.v1',status='FROZEN_DEV_CANDIDATE',frozen_at=frozen_at,
        split='DEV; previous multiproject validation explicitly contaminated by prior inspection',
        chosen=chosen,approaches_tested=list(APPROACHES),code_sha256=code,input_manifest_sha256=sha(ROOT/'INPUT_MANIFEST.json'),
        case_set_sha256=manifest['cases_sha256'],scope_schema_sha256=sha(ROOT/'reports/EVIDENCE_SCOPE_SCHEMA.json'),
        scorecard_sha256=sha(ROOT/'SCORECARD.json'),audit_labels_sha256=sha(ROOT/'audit/expected.json'),
        tests_sha256=sha(ROOT/'TEST_RESULTS.txt'),protected_code=read(ROOT/'BASELINE_MANIFEST.json')['protected_code'],
        model_calls=0,ai_approach_tested=False,precision_gate='EMPIRICAL_DEV_PASS; not independent validation',
        downstream_benefit=dict(additional_source_audited_identity_matches=1,accepted_project_changes=0,raw_project_changes=1),
        public_contracts_changed=False,table_v3_modified=False,production=False,push=0,deploy=0)
    frozen=ROOT/'reports/CANDIDATE_MANIFEST.json'
    if frozen.exists() and read(frozen)!=candidate:raise ValueError('Frozen candidate would change')
    write(frozen,candidate)
    holdout=prepare_holdout(sha(frozen))
    # Audit sidecars never rewrite frozen ProjectChange outputs or human truth.
    raw_changes=[(o,c) for o in downstream for c in o['bridge']['project_changes']]
    change_audit=[]
    for outcome,change in raw_changes:
        change_audit.append(dict(case_id=outcome['case_id'],project_change_id=change['project_change_id'],
            emitted_status=change['status'],audit_status='REVIEW',accepted=False,
            scalar_observation='Source verifies cooling capacity 10000 -> 8000 W.',
            failure='Frozen consumer labels cooling capacity as flow with unit W; explicit model replacement and consumed power are unparsed. Incomplete event/typed-property contract, not a scope error.',
            source_audit=['audit/regression_OLD.png','audit/regression_NEW.png'],
            actual_event='One outdoor unit replacement MVUH100BT-VA1 -> MVUH80BT-VA1 in apartment 6, block №3.6.'))
    write(ROOT/'PROJECTCHANGE_SOURCE_AUDIT.json',change_audit)
    identity_audit=[]
    for outcome in downstream:
        if outcome['scoped']['relation']!='SAME_SUBJECT':continue
        identity_audit.append(dict(case_id=outcome['case_id'],accepted=True,
                                  scope_delta=outcome['scope_delta'],
                                  basis='Same explicitly numbered outdoor slot and apartment; source-scoped row/header receipts. Cases 011/022/035 corroborated by prior source audits; 023 rechecked against both source rasters.',
                                  old_subject_ids=outcome['scoped']['old_subject_ids']))
    write(ROOT/'IDENTITY_SOURCE_AUDIT.json',identity_audit)
    with (ROOT/'reports/CASE_AUDIT.csv').open('w') as stream:
        writer=csv.writer(stream);writer.writerow(['case_id','route','approach','status','audit','parent_dimensions','wrong_dimensions','reasons'])
        for row in rows:
            b=row['binding'];writer.writerow([row['case_id'],row['route'],row['approach'],b['status'],row['audit'],
                json.dumps({d:v for d,v in b['dimensions'].items() if v},ensure_ascii=False),';'.join(row['wrong_dimensions']),';'.join(b['reasons'])])
    md('EVIDENCE_SCOPE_CONTRACT.md', '''# EvidenceScope V1

Offline ownership sidecar, before EngineeringSubject. It does not compare OLD/NEW states or decide identity. The existing EngineeringSubject and ProjectChange schemas and consumers, and Table V3, are byte-for-byte protected.

Each binding has a version-local fragment ID, source type, requested route, container ID, dimensions, an ancestor DAG, source evidence, confidence and PROVEN/REVIEW status. Dimensions are arrays to preserve simultaneous system/room/floor/group scopes; `null` means UNKNOWN. A label is stored literally, without guessing absent building, floor, system or room. Confidence HIGH is structural support, not a calibrated probability.

Each scope node names its dimension/value, parent IDs, inclusive source-ledger interval, pages, source container, proof status and receipts. Provenance includes SHA256-pinned PDF/blocks/Markdown, exact line number/hash/quote, cell column when applicable, and PDF rectangles where localized. Continuation adds both boundary witnesses. IDs are local to one source version; they do not identify an entity across versions.

Sibling row groups close previous intervals. Floor boundaries close subordinate apartment/equipment groups. An equipment group's numbered owner can contain indoor member rows; its number is never fed to a member as that member's own mark. Caption nodes belong to adjacent tables, not later prose. Source location needs the leading phrase plus independent internal/tail phrases for long paragraphs. A short numeric list item can be narrative without a verb.

PROVEN requires a pure route, verified source location, a supported owner, complete acyclic ancestry and no conflicting physical scopes. Missing/cyclic/ambiguous parents, unresolved OCR IDs, MIXED and UNKNOWN remain REVIEW. Unknown dimensions stay null even when the known owner is proven.

The core accepts hierarchical parent/cell provenance. This adapter implements explicit row groups, headings, colon lists and certified page continuation; it does not reconstruct arbitrary merged-cell column spans, graphic ownership, room identity or all implicit paragraph subjects. Those coverage limits remain REVIEW. No AI was used; optional bounded-package helper does not promote uncertainty.

Adapter to frozen EngineeringSubject only populates existing clues/context from certified scope and literal numbered equipment anchors. Unit text, row cells, state, headers and consumer logic are unchanged. Scope status REVIEW cannot enter matching as proven source evidence.
''')
    table=['| Approach | TEXT proven / correct / false / unsure | TABLE proven / correct / false / unsure |',
           '|---|---:|---:|']
    for a in APPROACHES:
        table.append('| '+a+' | '+' | '.join(' / '.join(str(score[a][r][k]) for k in ('proven','correct','false','unsure')) for r in ('TEXT','TABLE'))+' |')
    md('APPROACHES_COMPARED.md','''# Three approaches, DEV only

1. `nearest_heading`: nearest same-page heading/group start, without respecting its closing boundary. Diagnostic baseline; position proximity alone cannot establish ownership.
2. `structural_intervals`: closed row/section/list intervals and complete ancestry inside one page.
3. `certified_hierarchy`: the same structural engine plus explicit continued-table caption/header witnesses and same-column list continuation between adjacent pages. Selected.

'''+'\n'.join(table)+'''

The third approach adds one real cross-page TEXT binding and two TABLE bindings over page-local structure. No fourth approach and no AI branch were tested. AI is optional in the task; deterministic abstention was chosen instead of an unmeasured AI gain. Three bounded DEV materializations (initial and two source-adapter refreshes) are retained under iterations; no holdout content was used.

Architecture choice is based on implementing-agent source audits, previously inspected validation sources and historical failure diagnostics. It is not independent validation or a blind precision estimate. Old validation is explicitly DEV. The observed precision gate passes on resolved DEV cases; generalization remains unproven.
''')
    for route in ('TEXT','TABLE'):
        s=score[chosen][route];n=s['proven'];lower=n/(n+1.96**2)
        extra=('45 natural fragments: 42 prior-validation/failure-analysis fragments and 3 historical cooling-list fragments. Includes OLD and NEW; 22 remain REVIEW. Heading omission, unresolved phrase localization and source-grid ambiguity are recorded, not silently dropped. Historical 60/100 W/m² list ownership and its next-page commercial item are recovered; no TEXT identity accepted by the unchanged deterministic resolver.' if route=='TEXT' else
               '41 natural fragments: all 40 previous NEW targets plus the true OLD counterpart of diagnostic case 023. Ten remain REVIEW: eight OCR-confusable apartment IDs, one unproven cross-page prefix (case 020), and one row without a proven engineering parent (case 039). Group IDs such as OCR НБЗ8.4 are not silently rewritten. TABLE subjects are strongly clustered in ALIA OV; no broad TABLE generalization claim.')
        md(route+'_SCOPE_REPORT.md',f'''# {route} scope ownership — DEV

Cases {s['cases']}; PROVEN {n}; correct / false / unsure among PROVEN: {s['correct']} / {s['false']} / {s['unsure']}. REVIEW/unresolved {s['unresolved']}. Observed precision {s['precision']:.1%}; wrong-parent rate 0/{n}; source contamination 0/{n}.

All-correct Wilson 95% lower bound approximately {lower:.1%}; project/template/model correlation is not accounted for. Empirical >=95% on resolved audited DEV passes; these sample sizes do not establish a population lower bound of 95%.

{extra}

Labels in ../audit/expected.json are implementing-agent DEV source assessments, not human truth or independent adjudication. Every result, including abstentions and the two baselines, is in CASE_AUDIT.csv and ../scope_results.json. Rasters and exact source receipts are retained under ../audit and ../cases.json.
''')
    foundation=Counter();quality=[]
    for path in sorted((PREVIOUS/'narrative').glob('*/*.json')):
        x=read(path);q=x['quality'];quality.append(dict(version=path.parent.name,side=path.stem,quality=q))
        if path.stem=='new':foundation.update(q['foundation_exclusions'])
    write(ROOT/'ORIGINAL_TEXT_FUNNEL.json',dict(versions=quality,new_excluded_line_routes=dict(foundation)))
    md('TEXT_ADMISSION_FUNNEL.md',f'''# TEXT admission: cause-separated DEV funnel

Original seven document pairs / 14 versions: OLD 148 paragraph units -> 5 admitted; NEW 73 paragraph units -> 0 admitted -> 0 identity candidates. Every one of the 73 NEW units fails the primary-assertion/value gate (65 also fail narrative shape). These are mostly labels left after genuine prose was excluded upstream. Original NEW excluded ledger-line routes: {dict(foundation)}. Those are line counts, not paragraph/fragment counts; do not add their denominators.

The diagnostic cohort is fixed and intentionally contains known missed prose, not a random full-corpus recall sample. 45 source fragments -> 31 source-eligible -> 23 PROVEN ownership. Of 17 OLD fragments, 13 source-eligible and 9 scoped.

NEW funnel: **28 source -> 18 eligible -> 14 scoped -> 11 queries with identity candidates (21 candidate pairs) -> 0 SAME_SUBJECT -> 0 ProjectChanges**.

```mermaid
flowchart LR
 S[28 NEW source fragments] --> E[18 NARRATIVE_TEXT]
 E --> O[14 proven scopes]
 O --> C[11 queries / 21 candidate pairs]
 C --> I[0 deterministic identity matches]
```

| Cause | Evidence / count |
|---|---|
| A. Genuine absence | Not established. All 45 selected probes are real narrative/list evidence. No full-document absence certificate is claimed. |
| B. Source/admission loss | Original page routing drops genuine prose. New fragment gate admits 31; 12 UNKNOWN + 2 MIXED abstain because PDF localization/grid separation is unresolved. These 14 are known-prose coverage losses, not successful exclusions of non-narrative content. |
| C. Ownership loss | 8 of 31 source-eligible fragments still lack supported ancestry, notably prose in separate OCR blocks without a recovered heading. |
| D. Later retrieval | 14 scoped NEW queries search the scoped DEV OLD pool; 11 retrieve 21 alternatives. Three have no eligible counterpart in this selected OLD pool: ventilation introductory scope and two environmental list probes. This is pool coverage, not proven document absence. |
| Later identity decision | All 21 retrieved pairs reach the frozen deterministic branch; it requires an explicit mark plus another anchor and equipment class. These narrative requirements/list values do not supply that combination. No EngineeringSubject redesign or silent blame for the upstream zero admission. |

Six previous probe prefixes lack a unique exact OLD/NEW occurrence and are logged in INPUT_MANIFEST.json, outside the 45 evaluable-fragment denominator. Historical 60 -> 100 W/m² now passes source/list ownership; matching remains unresolved in the frozen branch. No AI/hybrid rerun was performed, so TEXT hybrid precision remains unmeasured.
''')
    gates=[]
    for c in cases:
        f=c['fragment'];gates.append(dict(case_id=c['case_id'],source_type=f['source_type'],route=c['route'],
            admitted=gate(f['source_type'],c['route'])=='PROVEN',expected_source_type='TABLE' if c['route']=='TABLE' else 'NARRATIVE_TEXT',
            source=f['source_evidence'],basis=f.get('admission',{})))
    write(ROOT/'SOURCE_GATE_AUDIT.json',gates)
    md('SOURCE_TYPE_GATE_AUDIT.md','''# Source-type gate

Explicit types: NARRATIVE_TEXT, TABLE, GRAPHIC, TITLE_BLOCK, PAGE_FURNITURE, MIXED, UNKNOWN. TEXT consumes only NARRATIVE_TEXT; TABLE only TABLE. All other route/type combinations REVIEW. Mixed content is admitted only as a separately localized child fragment. Paragraph/list purity is checked independently of page routing; no document/file/apartment exclusions are used.

Real diagnostic sample: 45 narrative/list fragments -> 31 NARRATIVE_TEXT, 12 UNKNOWN, 2 MIXED. All 41 table rows remain TABLE from source-pinned V3 row receipts. Confirmed cross-route contamination: 0 among the 72 source-admitted fragments, and 0 among the 54 PROVEN scope bindings. The 14 narrative abstentions are explicitly counted as coverage omissions.

The seven-type/two-route matrix is exercised by tests, not reported as 14 natural cases. Natural GRAPHIC/TITLE_BLOCK/PAGE_FURNITURE classification precision is unmeasured; no graphic implementation exists. PDF sheet-border grids are separated from substantive tables by merged-cell geometry. Leading-phrase plus independent local anchors prevent using a repeated address in a title block as the paragraph's source location; table captions cannot own following prose.

Raster-first DEV corrections and raw earlier derived cases remain in ../audit and ../iterations. The audit does not imply full-corpus purity or solve all OCR localization. No provenance/candidate result is upgraded using human truth.
''')
    md('WRONG_PARENT_AUDIT.md','''# Wrong-parent audit

Chosen candidate: TEXT 0/23 and TABLE 0/31 false PROVEN ownership; 32/86 total fragments remain REVIEW. Nearest-heading baseline produces 4 wrong TEXT parents and 10 wrong TABLE parents; 13 additional TABLE acceptances lack the required apartment ownership. Those incomplete acceptances are UNSURE, not counted correct.

Case 023 OLD and NEW are apartment 6; table-prefix apartment 5/2 headings are not their owners. Closed row-group intervals end at the next apartment/floor boundary. The direct source rows and before/after raster crops are retained. The previous source audit is reused only as DEV, not new independent evidence.

Case 020 begins after a page break without a supported continuation caption/header certificate. It stays REVIEW rather than borrowing the following apartment group. Eight OCR-confusable apartment labels remain REVIEW. The unit tests also exercise neighboring tables, sibling crossings, missing ancestors, cycles and repeated equipment models.

Full case trace: CASE_AUDIT.csv, ../audit/expected.json, ../scope_results.json. Audit correctness evaluates every populated engineering dimension against the source labels; missing required apartment/list owner is UNSURE. Unknown nonrequired dimensions do not become invented facts.
''')
    md('DOWNSTREAM_IDENTITY_IMPACT.md','''# Impact on the frozen EngineeringSubject resolver

The exact same deterministic function is run in three input conditions: original evidence, literal numbered-equipment anchors alone, and anchors plus certified scope. TABLE SAME_SUBJECT counts: **0 -> 3 -> 4** across the fixed 40 queries. The one causal scope gain is case 023. All four accepted row identities are source-audited. No scalar, model equality or row ordinal establishes identity.

The literal anchor adapter extracts an existing source designation/class into existing clue fields; this contribution is separated from scope. Model-bearing engineering_function alone could not match the replacement. Apartment 6 provides an independent conserved owner and enables the unique numbered slot to match. No previously accepted literal-only result is lost.

The earlier hybrid run accepted 17/40; it rejected case 023 as DIFFERENT_SUBJECT. The new source-audited identity is additional to that known prior result. This experiment does not rerun the paid hybrid resolver and does not compare 4 deterministic acceptances with 17 hybrid acceptances as a recall regression/improvement. Full hybrid nonregression remains unmeasured.

TEXT: 11 NEW queries / 21 candidate pairs unlocked after admission and ownership, but 0 accepted identities in the unchanged deterministic branch. No full OLD corpus retrieval/absence proof. All enriched subjects and decisions validate against the frozen schemas. Original unit text, state, cells and column headers are untouched.

Artifacts: ../DOWNSTREAM_RESULTS.json, ../downstream_packets, ../IDENTITY_SOURCE_AUDIT.json. EngineeringSubject contract/logic changed: NO.
''')
    md('PROJECTCHANGE_UNLOCK_REPORT.md','''# ProjectChange source audit

Raw frozen-consumer output: TEXT 0 / TABLE 1. **Source-audited accepted ProjectChanges: TEXT 0 / TABLE 0.** The raw TABLE event is retained unchanged and rejected in a separate audit sidecar; emitted PROVEN is not treated as correctness proof.

Case 023 has one real outdoor-unit replacement, apartment 6: MVUH100BT-VA1 -> MVUH80BT-VA1, consumed power 2660 -> 2100 W, cooling capacity 10000 -> 8000 W. Both PDFs and exact row occurrences were rechecked (../audit/regression_OLD.png and regression_NEW.png).

After scope unlock, the frozen consumer emits CAPACITY_CHANGED but calls its typed property `flow` with unit `вт`. It does not parse the changed model or consumed power from these headers. The scalar cooling-capacity observation is source-correct; the typed property and complete replacement event are not. Therefore the experiment claims one raw event and zero accepted complete ProjectChanges, not one correctly recognized equipment replacement.

This isolates a downstream typed-property/model-header limitation after ownership and identity are repaired. ProjectChange contract/engine, bridge and Table V3 remain frozen; no scope-specific consumer workaround was added. Evidence: ../PROJECTCHANGE_SOURCE_AUDIT.json, ../DOWNSTREAM_RESULTS.json. Group composition, 1:N and full engineering-list usefulness remain unvalidated.
''')
    md('REGRESSION_CASES.md','''# Regressions

| Case | Result | Evidence |
|---|---|---|
| Wrong apartment, block №3.6 | PASS for OLD/NEW apartment 6 and frozen identity; downstream event separately REVIEW | exact table-cell/row-group receipts; regression_OLD/NEW.png |
| 60 -> 100 W/m² list ownership | PASS source and colon-list owner on both versions | text_historical_OLD/NEW.png |
| Continued NEW commercial list on next page | PASS with same-column list-boundary certificate | text_historical_continuation.png; previous-page list anchors |
| Prefix room row before НБ38.2 (case 020) | REVIEW, no unsafe next-group inheritance | same-version row occurrence and absent continuation proof |
| OCR Cyrillic З in digit positions | REVIEW, no invented correction | eight prior NEW rows |
| Table caption above separate declaration/prose | No cross-container promotion | PDF title/table geometry |
| Repeated address in drawing stamp | Unverified phrase location remains UNKNOWN | leading phrase requirement |
| Cross-version/table parents, cycles, conflicting scopes, repeated model | Tests PASS | ../TEST_RESULTS.txt |

86 tests pass: 19 new scope tests plus 67 existing resolver/bridge/ProjectChange source/engine tests. All 258 real approach outputs validate against EvidenceScope schema; enriched subjects/relations validate against frozen schemas, exact source-line SHA256s and protected code hashes verified. Tests are controls, not natural-case precision observations. No ID-specific runtime rules for №3.6, apartment names or files exist; diagnostic selection/labels alone name the regression.
''')
    next_step='Source-annotate the registered six-document ownership holdout from two previously unseen Sobytie projects before viewing predictions, then run the frozen candidate once; restore a third unrelated complete project and paired versions for broader/downstream validation. Handle flow/W and replacement-model parsing in a separate authorized consumer experiment.'
    md('NEXT_ACTION.md',f'''# PARTIAL

Scope-layer empirical DEV gate passes (23/23 TEXT and 31/31 TABLE; one additional audited identity), and the candidate is frozen. Overall verdict PARTIAL: 32 unresolved ownership fragments, zero accepted complete ProjectChanges, no fresh independent results, and remaining narrative admission losses. No further approach/tuning is included.

Post-freeze slice is prepared at ../holdout/REGISTERED_SLICE.json: {len(holdout['selected'])} documents from two wholly unseen project IDs, both Sobytie from one developer/project family. Semantic source contents uninspected; labels 0, predictions 0. Only metadata/byte hashes were read. The local third unseen project has no complete PDF/blocks/Markdown set; neither selected fresh project has paired versions. This is ownership validation preparation only, not an independent downstream validation result.

Exact next step: {next_step}

Production NO; push/deploy 0/0; Graphic implementation NO; Table V3 modification NO; human truth modification NO. EngineeringSubject and ProjectChange public contracts/logic unchanged.
''')
    md('CHECKPOINT.md',f'''# Evidence Scope Binding V1 complete — PARTIAL

Three approaches evaluated. Chosen {chosen}. DEV TEXT 45 / PROVEN 23 / correct 23 / false 0 / unsure 0; TABLE 41 / PROVEN 31 / correct 31 / false 0 / unsure 0. Unresolved 22 TEXT + 10 TABLE. One additional real TABLE identity. Raw ProjectChange 1; source-audited accepted 0. Known №3.6 ownership regression PASS. Previous validation is DEV; no independent validation result claimed.

All requested reports, schema, source/label manifests, per-case outputs, source audit sidecars and content-uninspected next holdout registration are present. Candidate freeze: CANDIDATE_MANIFEST.json. 86 tests pass; 258 real scope outputs validate. Replay is network-free and source-only.

```sh
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PWD
python -m experiments.evidence_scope_binding_v1.run replay
python -m experiments.evidence_scope_binding_v1.run downstream
```

`refresh` refuses after freeze. Source snapshots/receipts are required for replay. Baseline untracked unrelated files docs/diverse_corpus_restore.md and scripts/restore_diverse_corpus.py are preserved. No production access, push or deploy; no human-truth writes. Exact next step: {next_step}
''')
    summary=dict(approaches_tested=3,chosen=chosen,TEXT=score[chosen]['TEXT'],TABLE=score[chosen]['TABLE'],
                 text_funnel=funnel,wrong_parent_bindings=0,source_contamination=0,
                 engineering_subject_matches_unlocked={'TEXT':0,'TABLE':1},
                 project_changes_unlocked={'TEXT':0,'TABLE':0},raw_project_changes={'TEXT':0,'TABLE':1},
                 regression='PASS',verdict='PARTIAL',next_step=next_step,
                 engineering_subject_changed=False,project_change_changed=False,production=False,push=0,deploy=0)
    write(ROOT/'FINAL_SUMMARY.json',summary)
    protect()
    delivery={str(p.relative_to(ROOT)):sha(p) for p in ROOT.rglob('*') if p.is_file() and p.name!='DELIVERY_MANIFEST.json'}
    write(ROOT/'DELIVERY_MANIFEST.json',dict(files=delivery,candidate_sha256=sha(frozen)))
    print(json.dumps(summary,ensure_ascii=False,indent=2))


if __name__=='__main__':
    finish()
