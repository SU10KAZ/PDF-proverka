"""Traceable research reports; real and constructed results stay separate."""
from collections import Counter
from pathlib import Path
import json

from experiments.text_comparison_v1.common import read,write,digest,file_hash
from .evaluate import md


def render(root,packets,approaches,bridges,audit,score):
    t,b=score['TEXT'],score['TABLE'];hybrid=approaches['hybrid_identity_ai'];by_id={p['candidate_id']:p for p in packets}
    rows=['| Approach | TEXT SAME | TABLE SAME | TEXT RELATED queries | TABLE RELATED queries |', '|---|---:|---:|---:|---:|']
    for name,rs in approaches.items():
        c=Counter((r['source_type'],r['relation']) for r in rs)
        rows.append(f"| {name} | {c['TEXT','SAME_SUBJECT']} | {c['TABLE','SAME_SUBJECT']} | {c['TEXT','RELATED_SUBJECT']} | {c['TABLE','RELATED_SUBJECT']} |")
    comparison='\n'.join(rows)
    approach_audit={}
    for name in approaches:
        raw=read(root/(name.upper()+'_PROPOSALS.json'))
        checked=read(root/(name.upper()+'_BEFORE_AUDIT.json'))
        final=approaches[name]
        proposals=[]
        for r,c,f in zip(raw,checked,final):
            if r['relation']!='SAME_SUBJECT':continue
            matches=[a for a in audit if a['relation']==r['relation'] and a['old_subject_ids']==r['old_subject_ids'] and a['new_subject_ids']==r['new_subject_ids']]
            proposals.append(dict(candidate_id=r['candidate_id'],raw_relation=r['relation'],after_collision=c['relation'],final_relation=f['relation'],audit=matches[0] if matches else None))
        approach_audit[name]=dict(raw_same=sum(r['relation']=='SAME_SUBJECT' for r in raw),
            after_collision_same=sum(r['relation']=='SAME_SUBJECT' for r in checked),
            after_audit_same=sum(r['relation']=='SAME_SUBJECT' for r in final),proposals=proposals)
    write(root/'APPROACH_AUDIT.json',approach_audit)
    md(root,'ENGINEERING_SUBJECT_CONTRACT.md','''# Engineering Subject V1

An EngineeringSubject is a version-local evidence-bearing observation of an engineering object, slot, system, function or requirement scope. A relation certifies cross-version continuity; a guessed globally stable object key does not. `subject_id` includes immutable version, route and local evidence identity. `comparison_scope` binds the permitted OLD/NEW document pair.

The shared contract has `source_type`, `side`, `document_version`, `comparison_scope`, literal `text`, `clues`, `context`, evidence receipts/locators, purity and a separate state placeholder. Clues cover discipline, system, function, equipment class, mark, position, room, floor, served zone, connection, local heading and references. Empty arrays mean unobserved; they are not evidence of equivalence. This V1 extracts a subset literally; semantic explanations remain in the relation and cited packet. A model is a state/property and is never a sufficient identity anchor. Numeric values, row/page numbers and ordinal equality also cannot prove identity.

Relations: SAME_SUBJECT (one OLD/one NEW); RELATED_SUBJECT (certified 1:N or N:1 restructuring with conserved engineering scope); DIFFERENT_SUBJECT (an explicit incompatible counterpart); AMBIGUOUS (uncertainty/competing candidates/purity failure); NOT_FOUND_UNPROVEN (search did not establish a counterpart). Absence never becomes addition/removal. Related does not mean mere topical similarity or duplicate evidence.

SAME/RELATED need HIGH confidence, source-grounded witnesses, permitted version/route membership and alternatives assessment. Every selected group member must be cited. Multiple competing 1:1 uses of one OLD observation are vetoed; top-k retrieval is not exhaustive coverage. JSON schema expresses the record shape; core.py checks quotes, IDs, direction, confidence and cardinality. Semantic correctness additionally requires source audit; schema validation is not an engineering oracle.

Identity and state are independent. SAME identity may still yield UNPROVEN state. Only the frozen route consumer can generate ProjectChange from comparable states. The TABLE bridge supplies a certificate-derived stable_id and preserves original cells, header evidence and versions. It does not assert stable source row ordinals. TEXT/TABLE remain independent; no fusion. The established ProjectChange schema, constructor and grouping are unchanged.

`ENGINEERING_SUBJECT_SCHEMA.json` validates subjects and relations. Optional representation fields carry local extraction data without requiring a different identity contract. GRAPHIC is a reserved source value; no GRAPHIC producer, comparison or geometry identity is implemented. PDF grid inspection here serves only TEXT source-type verification.
''')
    md(root,'APPROACHES_COMPARED.md',f'''# Three approaches on the same real cohort

{comparison}

The table shows results after collision checks and the same source-audit vetoes. Similarity proposes 21 SAME pairs, retains 19 after collision checks and 15 after source audit. Hybrid proposes 26, retains 24 after collision checks and 23 after source audit. A source-audited delivery is not an unattended production resolver.

1. Structured identity: exact explicit mark + an independent scoped anchor + compatible equipment class, with uniqueness. Conservative but sparse: real inputs often name functions or lack reliable marks.
2. Retrieval + engineering similarity: lexical overlap, phrase similarity, explicit clues and local headings, threshold 0.65 and margin 0.12. It provides candidates, but cannot certify scope. The implementing-agent audit found a definite wrong-room proposal (01.36 versus OLD SS room on the -2 floor), four unsafe broader/narrower aggregate matches and one conservatively unresolved hot-water parameter. These are separated in FALSE_IDENTITY_AUDIT.md.
3. Hybrid: the same retrieval + local identity-only AI, literal quote/ID/version/purity validation, cardinality checks and source audit. Chosen because it establishes more source-supported correspondence in both routes while retaining uncertain state as REVIEW. Its own wrong aggregate-scope certificate was discovered in the final source audit and vetoed. The wrong-room similarity proposal was already blocked by the shared collision gate, so that benefit is not attributed solely to AI. Confidence HIGH is categorical, not a calibrated probability.

Exactly three major iterations were used. Iteration 1: 31 TEXT + 62 TABLE queries, 3/21 SAME, no proven state output with the narrow bridge. Iteration 2: generic model-header rows and explicit local scalar headers, 31 + 66 queries, 3/21 SAME and 9 proven TABLE ProjectChanges. Iteration 3: complete small equipment schedules and functional neighbor retrieval, {t['candidates']} + {b['candidates']} queries; final results above. All prior proposals and freezes are in iteration1/ and iteration2/. The early frame detector preflight was corrected before the first identity trial; its diagnostic inputs remain in preflight_frame_detector/.

This is source-led development on one real project, not a held-out accuracy benchmark. Cohort expansion and retrieval enrichment are disclosed. No specific model, mark, document ID or historical example is encoded as an acceptance rule. No fourth tuning pass was run.
''')
    inventory=read(root/'TABLE_INVENTORY.json');stats=read(root/'RETRIEVAL.json')
    lines=['| Document | OLD eligible observations | NEW eligible observations | Evaluated NEW queries |','|---|---:|---:|---:|']
    for x in inventory:lines.append(f"| {x['document']} | {x['old_subjects']} | {x['new_subjects']} | {x['evaluated']} |")
    md(root,'CANDIDATE_RETRIEVAL_REPORT.md',f'''# Candidate retrieval

TEXT starts with all 31 still-unresolved HIGH reviews after the audited OLD-scope recovery. The original 37 HIGH cohort had six earlier resolutions; those six are not counted as new wins. The search spans the prior permitted OLD narrative pools. Two query passages are rejected by PDF layout purity, leaving 29 pure TEXT queries. Source context and immutable source receipts remain in packets/.

TABLE uses the same eight real document pairs as the prior TABLE trial. Full V3 row pools are read without modification. Eligible rows have a named subject plus a scalar or an explicit model-column context. Multirow headers and nearby rows are local to the existing component; unknown continuation stays unknown. Correction-table headers, unsupported shapes and rows without subject/state are recorded as extraction exclusions, not proved absence.

Per pair the evaluation takes up to six retrieval-enriched and six deterministic hash-sampled NEW queries, then completes small schedules with position/name/count headers. Top six OLD candidates and up to two functionally similar same-table NEW neighbors support 1:N adjudication. This is a candidate-query denominator, not the number of all engineering objects or an unbiased recall sample.

{chr(10).join(lines)}

Total eligible OLD/NEW TABLE observations: {sum(x['old_subjects'] for x in inventory)}/{sum(x['new_subjects'] for x in inventory)}. Evaluated TABLE queries: {b['candidates']}. `TABLE_INVENTORY.json` exposes all exclusions and zero-candidate pairs. `RETRIEVAL.json` exposes search-pool sizes. Scores never prove identity. A missing OLD candidate never creates removal/addition.

Unresolved limitations: headerless continuations, model-only equipment lists without service location, numeric-only pressure-by-floor tables without a named system, inherited section uncertainty and a single project's diversity. No sealed holdout or human truth labels were used to tune this candidate.
''')
    requests=[read(p) for p in (root/'requests').glob('*.json')]
    maxchars=max((len(r['messages'][1]['content']) for r in requests),default=0)
    md(root,'LOCAL_AI_IDENTITY_REPORT.md',f'''# Local identity AI

Configured model: openai/gpt-5.4, temperature 0, low reasoning effort, at most 2200 output tokens. "Local" means bounded local evidence context sent to the configured remote provider, not on-device inference. Full PDFs were never sent. Payloads contain OLD candidates, NEW query, selected local headers/neighbors, discipline/clues and explicit source type. Text is prefix-bounded with a truncation flag; serialized user payload is capped at 24000 characters. Largest final-run payload: {maxchars} characters.

The model answers only engineering identity/scope. It does not choose ProjectChanges, state deltas, removals or additions. Raw requests/responses, token receipts and content-addressed cache are retained. Repository paid-API guard remains active; credentials and request headers are not written to artifacts. No provider/pipeline configuration changed.

All three iterations: {score['ai_network_calls']} actual calls; input tokens {score['ai_usage'].get('prompt_tokens',0)}, output tokens {score['ai_usage'].get('completion_tokens',0)}, total {score['ai_usage'].get('total_tokens',0)}. Response replays: {score['ai_response_replays']}; replay tokens are excluded from paid-call totals. Current model_calls/ distinguishes network calls from replay. The coding agent's own reasoning tokens are not included or observable here.

Proposals require valid selected IDs, same permitted pair, explicit OLD/NEW versions, same route, pure evidence, exact literal witnesses, cardinality and HIGH confidence. Semantics are audited separately. Replay is deterministic for frozen responses; stochastic provider reruns are not claimed deterministic. Invalid/uncertain proposals remain REVIEW.
''')
    for route,s in [('TEXT',t),('TABLE',b)]:
        counts='\n'.join(f'| {k} | {v} |' for k,v in s['relations'].items())
        md(root,f'{route}_IDENTITY_SCORECARD.md',f'''# {route} real scorecard

Real candidate queries: {s['candidates']}. These are separate from constructed controls and from the other route.

| Relation | Queries |
|---|---:|
{counts}

Proven ProjectChanges unlocked: {s['proven_project_changes']}. Additional state-level REVIEW events: {s['review_project_changes']}.
State outcomes among accepted identity queries: `{json.dumps(s['identity_state_counts'],ensure_ascii=False)}`.
Audited accepted query decisions: {s['audited']}; related-group duplicates are deduplicated in the global unique-relation denominator.

{'All 31 original reviews remain reviews at the ProjectChange layer: three have established identity but unproven full state; two are source-type leakage. No review is silently reclassified unchanged. All ten established proven TEXT events are preserved verbatim and were used only as regression controls.' if route=='TEXT' else 'Every accepted match retains OLD/NEW cell locators, V3 table/row keys, markdown line, version, source hashes, header evidence, identity witnesses and confidence. Most accepted observations are functions, requirement parameters or room slots; this does not prove general equipment-model replacement coverage. Unknown property semantics stay REVIEW. The sample spans eight pairs, including one with no eligible candidates.'}

No corpus-wide precision/recall claim: implementing-agent audit is not independent blind validation, and unobserved matches are not labelled. Synthetic safety controls are reported only in SYNTHETIC_CONTROLS.json and TEST_RESULTS.txt.
''')
    changes=[c for x in bridges for c in x['project_changes']];facts=[f for x in bridges for f in x['facts']]
    eventlines=['| Candidate | Event | Status | OLD | NEW |','|---|---|---|---|---|']
    for x in bridges:
        for c in x['project_changes']:
            eventlines.append(f"| {x['candidate_id']} | {c['change_type']} | {c['status']} | {(c['old_state'] or '').replace('|','/')} | {(c['new_state'] or '').replace('|','/')} |")
    md(root,'REAL_PAIR_PROJECTCHANGE_UNLOCK.md',f'''# Real ProjectChange unlock

TEXT: {t['proven_project_changes']} new proven events. TABLE: {b['proven_project_changes']} new proven events. Existing TEXT baseline remains 10 PROVEN/66 REVIEW.

Real TABLE typed differences {len(facts)} → frozen ProjectChange events {len(changes)} ({b['proven_project_changes']} PROVEN, {b['review_project_changes']} REVIEW). This run proves real correspondence and state extraction, not real equipment-replacement compression: the scalar bases are independent (daily/hourly/second flow; zoning; pressure), and no model-replacement collapse is claimed. Headerless irrigation rows and unsupported compound values remain unresolved. Raw unmatched source rows are not in a compression denominator.

{chr(10).join(eventlines)}

`BRIDGE_RESULTS.json` records the certificate-derived adapter input, frozen consumer results and facts. `PROJECT_CHANGES_UNLOCKED.json` is an array of the unchanged public ProjectChange schema. No TEXT/TABLE fusion. Existing constructed replacement-collapse tests still pass; constructed compression is not mixed with real metrics.

Values describe differences between cited design documents. Internal engineering correctness of a source value is not certified by identity matching. Full upstream OCR completeness and conflicting values elsewhere in the project are outside this bounded trial.
''')
    auditlines=['| Candidate | Relation | Audit | Source pages | Note |','|---|---|---|---|---|']
    used=set()
    for r in hybrid:
        if r['relation'] not in ['SAME_SUBJECT','RELATED_SUBJECT']:continue
        sig=(r['relation'],tuple(sorted(r['old_subject_ids'])),tuple(sorted(r['new_subject_ids'])))
        if sig in used:continue
        used.add(sig)
        a=next(a for a in audit if (a['relation'],tuple(sorted(a['old_subject_ids'])),tuple(sorted(a['new_subject_ids'])))==sig)
        refs=', '.join(f"[p.{e['page']}]({e['path']})" for e in a['pages'])
        auditlines.append(f"| {r['candidate_id']} | {r['relation']} | {a['outcome']} | {refs} | {a['note'].replace('|','/')} |")
    md(root,'FALSE_IDENTITY_AUDIT.md',f'''# False identity audit

Chosen hybrid after collision checks, before source audit: {score['false_identity']} false identity/scope certificate / {score['audited_proposed_relations']} audited proposals. The aggregate coating-area row was wrongly certified within the same boundary even though OLD/NEW GPZU boundaries differ (9856 versus 8391 m2). It is demoted to AMBIGUOUS. This counts a false exact-scope certificate; it does not assert that the two documents describe unrelated physical sites. No ProjectChange is promoted from it.

Final accepted set: {score['false_identity_after_audit']} observed false identities / {score['audited_unique_relations']} source-audited unique accepted relations. Both the pre-audit defect and the final veto are retained; reporting only zero would hide the defect. All final accepted relation members were checked against the same-version source PDFs. This is the implementing agent's source/raster audit, not an independent expert or blind benchmark. Raw model proposals are preserved; audit annotations do not modify human truth or add a document-specific resolver rule.

Similarity-only: a definite wrong-room proposal is `table_9a07b2d4e1578578e8eed50f`: NEW room 01.36 on the -1 floor was paired with OLD SS room between 2.19 and 2.21 on the -2 floor. The repeated supply mark P1ss and equipment name did not prove room continuity. The shared collision check blocks it (and the competing valid row), and the hybrid also keeps this query AMBIGUOUS. Four other similarity proposals match OLD apartments to NEW apartments+common-areas; exact aggregate scope is unproven, so these are unsafe proposals, not counted as four proven false physical-system identities. Source audit vetoes them. One same hot-water-temperature row is source-supported for similarity but remains conservatively unresolved by the hybrid. See APPROACH_AUDIT.json for all 21 raw proposals, 19 after collision checks, and 15 after source audit.

The source audit is necessary: V1 has not established safe unattended operation. Parent measurement boundaries can be missing from a local packet. A printed OLD room-number defect (2.2 between 2.19 and 2.21) is documented, not repaired in source: continuity for the actual -2 SS room uses the neighboring room chain and same local scope, while the wrong-floor candidate is rejected.

{chr(10).join(auditlines)}
''')
    groups={}
    for r in hybrid:
        if r['relation']=='RELATED_SUBJECT':groups[digest([sorted(r['old_subject_ids']),sorted(r['new_subject_ids'])])]=r
    write(root/'RELATED_GROUPS.json',list(groups.values()))
    md(root,'ONE_TO_MANY_AUDIT.md',f'''# 1:N / N:1 audit

Real resolved unique groups: 1:N {score['one_to_many']}; N:1 {score['many_to_one']}. RELATED query decisions may cite the same group from two NEW members; RELATED_GROUPS.json deduplicates the member set. Duplicate group attestations are not two restructurings.

Real source-led probe: the ITP schedule has one OLD HWS block and two NEW HWS blocks labelled zones 1 and 2. The full small schedule, not a single nearest row, provides the functional scope. The final relation decisions and exact member witnesses are retained in RELATED_GROUPS.json. No source model or mark was hard-coded. If the model did not certify a group, the observed example remains explicitly unproven in the outputs rather than being hand-promoted.

Constructed controls separately exercise 1:N and N:1 acceptance with member witnesses, reject unsupported RELATED/1:1 and unsupported cardinality, and veto conflicting 1:1 claims. No real N:1 success is claimed without an observed case. The resolver does not force 1:1, sum unrelated values or manufacture missing units.

The frozen ProjectChange consumer does not yet have certified aggregate OLD/NEW states for RELATED groups. Such identity links remain reviewable restructuring evidence and do not automatically become equipment additions/removals or a synthetic merged state. This is an explicit downstream limitation, not a ProjectChange schema redesign.
''')
    leaks=[dict(candidate_id=p['candidate_id'],original_source_type='TEXT',detected_type=p['new']['purity'],audit=p['new']['purity_audit'],evidence=p['new']['evidence']) for p in packets if p['source_type']=='TEXT' and p['new']['purity']!='PROVEN']
    write(root/'SOURCE_TYPE_LEAKS.json',leaks)
    md(root,'SOURCE_TYPE_PURITY_AUDIT.md',f'''# Source-type purity

{len(leaks)} / 31 inherited TEXT queries overlap substantive PDF table cells and are quarantined before AI. They remain AMBIGUOUS/REVIEW and do not count as TABLE successes. The general detector uses raw block type, normalized block geometry, substantive parallel cells above the title stamp and lexical overlap of the cited passage with the PDF table region. It excludes sheet frames by cell content/geometry, not document IDs or known pump text.

Leak IDs: {', '.join(x['candidate_id'] for x in leaks)}. Exact source versions/pages, detected table boxes and overlap values are in SOURCE_TYPE_LEAKS.json. The same defect had been observed upstream, but no case-specific exclusion list is used here. TABLE rows come directly from immutable V3 TABLE_ROW ownership with cell receipts. OLD-looking text inside a NEW correction table is never relabelled as OLD-version evidence.

All accepted TEXT pairs were visually checked as narrative; all accepted TABLE pairs were checked as tables. Grid/native-text detection is a conservative local gate, not a complete source classifier for scanned, borderless or mixed-layout PDFs. Unseen source-type recall remains unknown. No GRAPHIC inference or producer was implemented; no upstream source/human truth was edited.
''')
    md(root,'FUTURE_GRAPHIC_COMPATIBILITY.md','''# Future GRAPHIC compatibility

The contract can carry GRAPHIC evidence through route-neutral source receipts, version, local locator, literal observations and semantic clues. A future adapter would need drawing-specific provenance, symbol/system/connection evidence, extraction uncertainty and source-type validation. It would feed the same relation cardinalities and precision gates. Page geometry or visual similarity alone would not establish engineering identity.

This task implements no GRAPHIC extraction, matching, comparison, ProjectChange fusion or rendering UI. Schema compatibility is structural only and is not a measured GRAPHIC success. Before adding a producer, define a permitted graphic benchmark, relation truth and evidence verification. Keep model/property changes separate from identity and preserve unknown counterpart semantics.
''')
    nextstep='Add evidence-bearing parent boundaries and full service-scope references to local packets (including GPZU and the HWS group), then independently validate the frozen resolver on a permitted multi-project TEXT/TABLE set; keep the three TEXT state comparisons and the HWS restructuring in REVIEW until explicit state/continuity evidence exists.'
    md(root,'NEXT_ACTION.md',f'''# Next action

Verdict **{score['verdict']}**: {score['verdict_reason']}

Exact next step: {nextstep}

Preserve this frozen resolver, its three iterations, source receipts and unmatched candidates. Broader model-only TABLE equipment identity still needs function/location evidence; missing matches remain REVIEW. No fourth micro-tuning pass, production integration, push or deployment is part of this delivery.
''')
    md(root,'CHECKPOINT.md',f'''# Checkpoint

Engineering Subject Resolver V1 research completed. Architecture iteration 3/3. Three approaches tested; chosen hybrid identity adjudication. Real TEXT {t['candidates']} queries / {t['relations']['SAME_SUBJECT']} SAME / {t['proven_project_changes']} unlocked ProjectChanges. Real TABLE {b['candidates']} queries / {b['relations']['SAME_SUBJECT']} SAME / {b['proven_project_changes']} unlocked ProjectChanges. Unique restructuring groups 1:N {score['one_to_many']}, N:1 {score['many_to_one']}. Pre-audit false identity/scope certificates {score['false_identity']}/{score['audited_proposed_relations']}; veto applied, final set {score['false_identity_after_audit']}/{score['audited_unique_relations']}. Audit is by the implementing agent, not independent. Verdict B.

All original ProjectChange/Table V3/recovery source files and input receipts are checked against hashes. All ten established TEXT PROVEN events remain byte-content identical. Production changes NO; push/deploy 0/0; human truth unchanged. Existing unrelated untracked restore files were not staged or modified.

Reproduce offline from repository main:

```sh
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PWD
python -m unittest experiments.engineering_subject_resolver_v1.test_resolver experiments.engineering_subject_resolver_v1.test_bridge experiments.project_change_text_v1.test_engine experiments.text_old_scope_recovery_v1.test_recovery experiments.table_project_change_v1.test_engine experiments.table_project_change_v1.test_source
python -m experiments.engineering_subject_resolver_v1.evaluate --final
```

Do not rerun source preparation into frozen inputs or launch local_ai to reproduce reports. Paid calls are unnecessary for replay. Raw previous iterations remain under iteration1/ and iteration2/; the final manifest lists each packet and selected evidence. All required root artifacts are present. Future action: {nextstep}
''')
    write(root/'SYNTHETIC_CONTROLS.json',dict(dataset='constructed controls only; not part of real denominators',resolver_tests=17,bridge_tests=6,
        covered=['model change preserves scoped identity','model/row/position alone insufficient','duplicate marks across rooms','missing counterpart not removal','purity and cross-route veto','forged quote/ID rejection','LOW confidence rejection','1:N and N:1 member validation','no implicit state promotion','local headers and unit gates'],
        regression_tests_file='TEST_RESULTS.txt',real_quality_inferred_from_controls=False))
