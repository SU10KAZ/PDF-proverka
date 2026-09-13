"""Post-freeze audit packaging and deterministic, network-free closeout.

Audit annotations are external evidence, not new inference rules. The original
candidate, decisions and raw run are preserved. This module never calls AI.
"""
from collections import Counter
from copy import deepcopy
import json
from pathlib import Path
import statistics

from experiments.text_comparison_v1.common import read, write, digest, file_hash
from experiments.project_change_text_v1.audit import provenance
from experiments.project_change_text_v1.contract import validate
from .decisions import RESULTS, validate_decision, SYSTEM_PROMPT
from .integrate import run as integrate
from .local_ai import payload
from .run import ROOT, BASE, verify


def md(root, name, body):
    (root / 'reports' / name).write_text(body.strip() + '\n')


def costs(root):
    batches = []
    for name, directory in [('iteration1', root/'iteration1/model_calls'),
                            ('iteration2', root/'model_calls')]:
        records = [read(p) for p in sorted(directory.glob('*.json'))]
        actual = [r for r in records if r.get('network_call', True) and 'response' in r]
        batches.append(dict(iteration=name, network_calls=len(actual),
                            cached_responses=sum(r.get('network_call') is False for r in records),
                            prompt_tokens=sum(r['usage']['prompt_tokens'] for r in actual),
                            completion_tokens=sum(r['usage']['completion_tokens'] for r in actual),
                            cached_prompt_tokens=sum(r['usage'].get('prompt_tokens_details', {}).get('cached_tokens', 0) or 0 for r in actual),
                            provider_reported_cost_usd=round(sum(r['usage'].get('cost', 0) for r in actual), 9),
                            failures=sum('response' not in r for r in records)))
    totals = {k: sum(b[k] for b in batches) for k in batches[0] if k != 'iteration'}
    totals['provider_reported_cost_usd'] = round(totals['provider_reported_cost_usd'], 9)
    totals['total_tokens'] = totals['prompt_tokens'] + totals['completion_tokens']
    return dict(batches=batches, totals=totals, model='openai/gpt-5.4',
                provider='Configured OpenRouter endpoint', paid_api_guard_enabled=True,
                reasoning_effort='low', max_output_tokens=2400, concurrency=3,
                cache_usage_counted_once=True, human_review_cost_not_included=True)


def replay(root):
    before = file_hash(root/'audited/project.json')
    integrate(root, 'audited')
    after = file_hash(root/'audited/project.json')
    assert before == after, 'Integration replay drift'
    checked = 0
    for path in sorted((root/'model_calls').glob('*.json')):
        record = read(path)
        packet = read(root/'packets'/path.name)
        messages = [dict(role='system', content=SYSTEM_PROMPT),
                    dict(role='user', content=json.dumps(payload(packet), ensure_ascii=False))]
        assert digest(messages) == record['request_hash'], path
        decision = validate_decision(packet, json.loads(record['response']))
        saved = read(root/'decisions'/path.name)
        saved.pop('method', None)
        assert decision == saved, path
        checked += 1
    result = dict(passed=True, response_validation_replays=checked,
                  integration_sha256=after, network_calls=0,
                  note='Saved responses + deterministic validation/integration; not a fresh stochastic model run.')
    write(root/'reports/REPLAY_REPORT.json', result)
    return result


def run(root=ROOT):
    manifest = verify(root)
    assessment = read(root/'quality_audit/AGENT_ASSESSMENT.json')
    overrides = read(root/'quality_audit/ACCEPTANCE_OVERRIDES.json')
    rep = replay(root)
    project = read(root/'audited/project.json')
    raw = read(root/'raw/project.json')
    base = read(BASE/'reports/PROJECT_TEXT_CHANGES.json')
    originals = {c['project_change_id']: c for c in base['project_changes']}
    changes = {c['project_change_id']: c for c in project['project_changes']}
    inventory = read(root/'reports/PRIORITY_INVENTORY.json')
    prep = read(root/'reports/PREPARE_METRICS.json')
    packets = [read(p) for p in sorted((root/'packets').glob('*.json'))]
    decisions = {p.stem: read(p) for p in sorted((root/'decisions').glob('*.json'))}
    assert len(packets) == len(decisions) == 37
    # Preserve unresolved base records. Purity-invalid inherited records remain
    # REVIEW with an explicit reason, never as accepted TEXT evidence.
    for cid, reason in overrides.items():
        c = changes[cid]
        assert c['status'] == 'REVIEW'
        c['review_reasons'] = sorted(set(c['review_reasons'] + ['SOURCE_TABLE_CONFIRMED_BY_RASTER']))
        c['decision_reasons'].append(reason)
    for row in project['resolutions']:
        row['decision_before_source_audit'] = row['decision']
        if row['project_change_id'] in overrides:
            row['decision'] = 'NOT_FOUND_UNPROVEN'
            row['confidence'] = 'LOW'
    m = project['metrics']
    m['raw_decisions'] = deepcopy(m['decisions'])
    m['decisions'] = {k: sum(r['decision'] == k for r in project['resolutions']) for k in RESULTS}
    m['source_purity_blocked_reviews'] = len(overrides)
    m['high_review_remaining'] = 37 - m['high_value_review_resolved']
    m['false_new_project_changes'] = assessment['false_new_unique_events']
    m['new_project_changes_audited'] = assessment['new_unique_events_audited']
    assert m['proven'] == 10 and m['review'] == 66
    assert m['high_value_review_resolved'] == 6 and m['new_proven_events'] == 4
    assert sum(m['decisions'].values()) == 37
    for row in inventory:
        if row['priority'] != 'HIGH':
            assert changes[row['project_change_id']] == originals[row['project_change_id']]
    # Base PROVEN events survive. Only the fan-coil event gains evidence.
    for c in base['project_changes']:
        if c['status'] == 'PROVEN':
            after = changes[c['project_change_id']]
            assert after['status'] == 'PROVEN' and after['event_key'] == c['event_key']
            for e in c['evidence_old'] + c['evidence_new']:
                assert e in after['evidence_old'] + after['evidence_new']
    rejected_refs = {(e['document_version'], r['line_id']) for cid in overrides
                     for e in originals[cid]['evidence_new'] for r in e['source_refs']}
    for c in changes.values():
        validate(c)
        if c['status'] == 'PROVEN':
            assert not rejected_refs & {(e['document_version'], r['line_id'])
                                        for e in c['evidence_old'] + c['evidence_new'] for r in e['source_refs']}
    project['source_purity_audit'] = dict(
        raw_run_table_leak_detected=True, known_table_new_statements=len(overrides),
        blocked_review_ids=sorted(overrides), accepted_recovery_table_evidence=0,
        inherited_review_route_labels_preserved=True,
        note='Original producer labeled two table cells TEXT. Flagged REVIEW records are retained for accounting only; their evidence is prohibited in accepted events. Raw packet/manifest route flags were assumptions disproved by raster audit.')
    project['table_compared_scope'] = 'Accepted recovery evidence only; raw run table contamination is recorded in source_purity_audit.'
    project['quality_audit'] = assessment
    project['verdict'] = 'PARTIAL'
    project['metrics']['architecture_iterations'] = 2
    write(root/'reports/PROJECT_TEXT_CHANGES_RECOVERED.json', project)
    prov = provenance(root, project['project_changes'])
    cost = costs(root)
    write(root/'reports/CONTEXT_COST.json', cost)
    tally = {k: m['decisions'][k] for k in RESULTS}
    score = dict(metrics=m, costs=cost['totals'], replay=rep, provenance=prov,
                 verdict='PARTIAL', human_blind_validation_completed=False,
                 safety=dict(production_changed=False, push=0, deploy=0,
                             human_truth_changed=False, schema_changed=False, grouping_changed=False,
                             raw_table_leak_detected=True, accepted_table_evidence=0, graphic_compared=False))
    write(root/'reports/SCORECARD.json', score)
    md(root, 'OLD_SCOPE_RECOVERY_ARCHITECTURE.md', f'''
# TEXT OLD scope recovery V1

Frozen baseline: `{manifest['base_candidate_hash']}`. Recovery candidate: `{manifest['candidate_hash']}`.
The existing ProjectChange schema, constructor, event-type mapping and grouping implementation are byte-for-byte protected by the baseline manifest. All code changes live in a separate experiment directory.

1. Rank the original 72 REVIEW by NEW product value before searching OLD; process the 37 HIGH only.
2. Build a local OLD narrative pool with source hashes. A descriptive heading containing “scheme” is not itself a drawing. Keep genuine drawings, structured tables, specifications and formula labels excluded.
3. Retrieve up to six distinct local OLD assertions by token/stem BM25-like score, phrase overlap, marks/models and heading prior. Numbers/units and neighboring concepts contribute lexical features. Source sequence bounds adjacent context. SectionRelation is not a gate; scores do not establish identity.
4. Exact full assertions may resolve unchanged cases; otherwise send only bounded NEW + OLD packets to local AI. In this cohort every packet required AI.
5. Validate source IDs, verbatim quotes, differing grounded facts, units, confidence and existing entity rules. OLD_SAME covers every NEW claim. OLD_DIFFERENT proves a contradictory state of the same subject; additional unmatched details do not become separate proved changes.
6. Use the frozen constructor and event type. Link repeated evidence only within the same comparison/type and compatible explicit identity, with the same cited OLD assertion and identical before/after states. No general regrouping or cross-document clustering is introduced.
7. Post-freeze source/raster audit may veto a contribution and retain REVIEW. Audit annotations remain separate from inference rules. No human truth is changed.

OLD_ABSENT_PROVEN is supported as an outcome category but never emitted here: top-k retrieval cannot prove exhaustive absence. No EQUIPMENT_ADDED/REMOVED is inferred from missing text.

Two major iterations were used. Iteration 1 over-constrained changed events by demanding counterparts for all extra NEW details and admitted scalar calculation labels. Iteration 2 separates event proof from detail coverage and excludes generic symbol/value labels. Identical AI requests replay their responses. No document ID, equipment mark or target number is an inference exception.

The final audit found two inherited NEW table cells mislabeled TEXT. One had produced a raw accepted contribution. It was vetoed; the same pump event survives through an independent narrative occurrence. The frozen manifest's table flags record producer assumptions, not a successful raster purity audit. See QUALITY_AUDIT.md for this limitation. Production suitability is PARTIAL.
''')
    ranks = []
    for row in project['resolutions']:
        if row['action'] in ('PROMOTE_NEW_EVENT', 'ATTACH_TO_EXISTING_EVENT', 'REMOVE_UNCHANGED_REVIEW'):
            p = next(p for p in packets if p['project_change_id'] == row['project_change_id'])
            selected = decisions[row['project_change_id']]['selected_old_unit_ids']
            ranks.extend(i+1 for i, c in enumerate(p['old_candidates']) if c['unit_id'] in selected)
    pools = [read(p) for p in sorted((root/'documents').glob('*/scope_pool.json'))]
    md(root, 'LOCAL_RETRIEVAL_REPORT.md', f'''
# Local retrieval

72 REVIEW ranked: HIGH 37, MEDIUM 30, LOW 5. Only HIGH was executed. There are {len(pools)} version-specific local pools (OLD plus NEW for contextual anchors), containing {sum(len(p['units']) for p in pools)} eligible paragraphs in total.

OLD candidates per packet: {prep['candidates']}. Local engineering text total {prep['local_text_characters']} characters, median {statistics.median(p['local_text_characters'] for p in packets):.0f}, maximum {prep['max_local_text_characters']}. Cap 24,000 characters; adjacent context cap 400 characters per fragment, same source page and at most five Markdown lines apart. The containing NEW paragraph is also supplied.

Selected candidate ranks for the six accepted resolutions: {ranks}. This is observed hit rank, not measured recall. Every accepted source was among the locally retrieved candidates. Search failure never proves OLD absence; 29 final results remain NOT_FOUND_UNPROVEN, including one audit-vetoed raw change.

The relaxed narrative-heading adapter recovered the drainage pump paragraphs wrongly excluded by the baseline because their broad enclosing heading mentioned a scheme. The independent raster audit also found the opposite error: two NEW table cells falsely labeled narrative upstream. Both are blocked in the final result. Candidate pools and packet hashes remain frozen for reproduction.

The 800 kW / 2585 kW cooling pair remains a promising REVIEW: model confidence did not establish identical aggregation scope. This is a coverage limitation, not proof of no change.
''')
    ct = cost['totals']
    md(root, 'LOCAL_AI_REPORT.md', f'''
# Local AI

Configured remote model: `openai/gpt-5.4` through OpenRouter, reasoning low, temperature 0, JSON output, maximum 2,400 output tokens, concurrency 3, timeout 90 seconds, no automatic retries. “Local” describes the bounded evidence context; inference was not on-device.

Requests contain NEW, 3–6 OLD candidates, local headings, neighboring text and engineering entities. No full PDF, project, source filesystem path or API credential is sent in the model payload. Existing paid API guard was enabled, with reservations retained for the batch; no production configuration was changed.

Iteration 1: 37 calls. Iteration 2: 8 calls and 29 exact request-hash response replays. Total {ct['network_calls']} actual calls, {ct['failures']} failed calls. No provider traffic during audit/replay.

Raw validated outcomes: {json.dumps(raw['metrics']['decisions'], ensure_ascii=False)}.
After source audit: {json.dumps(tally, ensure_ascii=False)}.
One OLD_DIFFERENT is downgraded to NOT_FOUND_UNPROVEN because its NEW citation is a table cell. A second table-derived packet already remained NOT_FOUND_UNPROVEN. Source decisions and provider responses are preserved, never relabeled in place.

All 37 saved responses were revalidated against exact packet/request hashes. HIGH is required for changed or unchanged acceptance; model claims remain proposals until quote, schema and source checks pass. Six final HIGH reviews resolve: four new events, one evidence attachment and one unchanged removal.
''')
    rows = ['| Outcome | Raw iteration 2 | After source audit |', '|---|---:|---:|']
    rows += [f"| {k} | {raw['metrics']['decisions'].get(k,0)} | {m['decisions'][k]} |" for k in RESULTS]
    md(root, 'OLD_SCOPE_SCORECARD.md', f'''
# OLD scope scorecard

Verdict: **PARTIAL**. Recovery works for a subset; remaining scope gaps and source classification prevent claiming general readiness.

Initial REVIEW: 72. HIGH: 37. HIGH resolved: 6/37 (16.2%); remaining HIGH: 31. MEDIUM 30 and LOW 5 are untouched.

{chr(10).join(rows)}

Before PROVEN/REVIEW: **6/72**. After source audit: **10/66**. New unique high-value events: **4**. Accepted duplicate evidence collapsed: **1**. Unchanged review removed: **1**.

False accepted new ProjectChanges: **0 / 4 audited**. Changed event groups including the enhanced baseline fan-coil event: 0/5. These are implementing-agent source audits, not independent blind validation. Raw source-purity failure: 1 of 7 resolved proposals; two NEW packets had table contamination. No invalid table evidence remains in PROVEN.

AI: {ct['network_calls']} actual calls; {ct['prompt_tokens']} input + {ct['completion_tokens']} output = {ct['total_tokens']} tokens. Provider-reported cost USD {ct['provider_reported_cost_usd']:.6f}.

Schema and grouping unchanged. Production changed NO. Push/deploy 0/0. Human truth unchanged. Two major iterations, no third tuning pass.
''')
    event_rows = ['| New ProjectChange | Engineering event | OLD / NEW PDF pages |', '|---|---|---|']
    for row in project['resolutions']:
        if row['action'] != 'PROMOTE_NEW_EVENT':
            continue
        c = changes[row['target_project_change_id']]
        pages = [[str(r['page']) for e in c['evidence_'+side] for r in e['source_refs']] for side in ('old', 'new')]
        event_rows.append(f"| {c['project_change_id']} | {c['short_summary_ru']} | {','.join(sorted(set(pages[0])))} / {','.join(sorted(set(pages[1])))} |")
    md(root, 'PROJECTCHANGE_BEFORE_AFTER.md', f'''
# ProjectChanges before / after

| Metric | Before | Raw iteration 2 | Audited final |
|---|---:|---:|---:|
| PROVEN | 6 | 10 | 10 |
| REVIEW | 72 | 65 | 66 |
| HIGH REVIEW resolved | 0 | 7 | 6 |
| New unique PROVEN events | 0 | 4 | 4 |
| Duplicate contributions collapsed | 0 | 2 | 1 |
| Unchanged reviews removed | 0 | 1 | 1 |

{chr(10).join(event_rows)}

Accounting: 72 − 4 promotions − 1 duplicate attachment − 1 unchanged = 66 REVIEW. Final collection has 76 ProjectChanges. Two flagged source-purity reviews are included in REVIEW, with no accepted engineering comparison. The unchanged statement is preserved in the unchanged ledger, not emitted as a project change.

Every original PROVEN event keeps its event key and all original evidence. All 35 MEDIUM/LOW reviews are unchanged. Fan-coil 2→4 pipes gains another narrative witness without adding another event.

The recovered EOM electrical-power assertion is 1175.4→1935.4 kW; the baseline EE assertion is 1181.7→1935.4 kW. Separate version/document scopes and differing OLD states are retained. No cross-document identity or inconsistency resolution is claimed.
''')
    dup = project['duplicate_links'][0]
    md(root, 'DUPLICATE_COLLAPSE_AUDIT.md', f'''
# Duplicate collapse audit

Accepted collapse: `{dup['source_review_id']}` → `{dup['target']}`. OLD same conditioning-system assertion, 2-pipe fan-coils; NEW narrative repeats 4-pipe fan-coils. The prior event type/key remains; source evidence is unioned. One engineering event survives.

Raw run also collapsed two vent-chamber pump descriptions. One was on NEW PDF page 7 inside a correction table. Its source is prohibited and its inherited candidate remains flagged REVIEW. Only page 13 narrative is accepted; therefore the final accepted collapse count is **1**, not 2. The pump event is constructed from the admissible page-13 review.

Parking-perimeter and ventilation-chamber pumps are separate events: their explicitly named pits and OLD/new models differ. ITP and KNS candidates remain REVIEW. Repeated model strings at different locations are not merged. EOM and EE electricity assertions retain distinct comparison scopes and OLD states. The frozen general grouping implementation is unchanged.
''')
    audit_rows = ['| Source review | Audit result | Evidence / reason |', '|---|---|---|']
    audit_rows += [f"| {a['source_review_id']} | {a['verdict']} | {a['reason']} Images: {', '.join(a['raster_images'])} |" for a in assessment['assessments']]
    control_rows = '\n'.join(f"- `{a['source_review_id']}`: {a['reason']}" for a in assessment['retained_review_controls'])
    md(root, 'QUALITY_AUDIT.md', f'''
# Quality audit

Post-freeze implementing-agent audit, not human or blind assessment. All four accepted new events, the existing fan-coil evidence attachment, unchanged removal and rejected table contribution were checked against local text and PDF raster. All 37 HIGH NEW statements were checked for layout/source purity, across 24 unique NEW pages; four additional OLD pages were viewed. One OLD scalar-calculation page was inspected separately during iteration 2.

**False accepted new ProjectChanges: 0/4.** Raw event factual contradictions: none found in this small sample. Raw TEXT-source acceptance violations: **1/7 resolved proposals**. Two inherited NEW packets were actually table cells mislabeled TEXT; both are vetoed. This exposes a real source-routing limitation and prevents READY. Flags in frozen packets are not retrospective proof of purity.

The audit veto only removes acceptance. It does not fabricate OLD evidence or alter human truth. The vent-chamber replacement is still independently proven by narrative OLD page 10 and NEW page 13. Both table-derived original rows remain REVIEW with explicit source-purity reasons; no PROVEN event cites their lines. Table-cell comparisons in the raw experiment are a recorded deviation from the requested TEXT-only scope; accepted results exclude them. No graphic comparison was performed.

{chr(10).join(audit_rows)}

Eight retained-review controls were inspected for conservative scope treatment; these are not confirmed negatives and do not estimate recall:

{control_rows}

Provenance: {prov['project_changes']} schema-valid ProjectChanges, {prov['unique_evidence']} unique evidence records, {prov['line_references_checked']} line references, {prov['source_artifacts_checked']} artifact hashes. Quotes match cited Markdown and versions; decisive accepted model names, counts and powers were checked on raster. Non-decisive OCR spelling differences are preserved as source text.

Reproducibility: 37 response-validation replays and deterministic integration pass, without new model calls. Baseline source files and frozen recovery code retain their hashes. No estimate of whole-project recall, independent precision or human agreement is made.
''')
    md(root, 'CONTEXT_COST.md', f'''
# Context and cost

| Batch | Actual calls | Disk replays | Input tokens | Output tokens | Provider USD |
|---|---:|---:|---:|---:|---:|
''' + '\n'.join(f"| {b['iteration']} | {b['network_calls']} | {b['cached_responses']} | {b['prompt_tokens']} | {b['completion_tokens']} | {b['provider_reported_cost_usd']:.6f} |" for b in cost['batches']) + f'''

Total: **{ct['network_calls']} actual calls**, **{ct['total_tokens']} tokens** ({ct['prompt_tokens']} input + {ct['completion_tokens']} output), **USD {ct['provider_reported_cost_usd']:.6f}** reported by the provider. Cached input tokens within real provider calls: {ct['cached_prompt_tokens']}. Disk-replayed usage is excluded from billable totals. Output token count includes reasoning tokens reported by the provider.

Final cohort: 37 packets, {prep['local_text_characters']} local text characters, maximum {prep['max_local_text_characters']} per packet. Counts of prompt tokens include JSON fields and instructions. The request contains no whole PDF or project; only bounded engineering fragments. PDF raster inspection was local. The existing paid API guard was used on every new call.
''')
    detail = ['| Review | Final decision | Action | OLD selected IDs | Reason |', '|---|---|---|---|---|']
    for row in project['resolutions']:
        d = decisions[row['project_change_id']]
        reason = row.get('audit_reason') or d['state_reason_ru']
        detail.append(f"| {row['project_change_id']} | {row['decision']} | {row['action']} | {', '.join(d['selected_old_unit_ids'])} | {reason.replace('|', '/')} |")
    md(root, 'OLD_SCOPE_DECISIONS.md', '# Per-review decisions\n\n' + '\n'.join(detail))
    md(root, 'NEXT_ACTION.md', '''
# Next action

PARTIAL. The bounded recovery experiment is complete; production readiness is not established.

The next separately scoped development task should repair upstream source classification using PDF layout, including correction tables flattened into prose and formula labels. Require a regression corpus spanning several documents, then freeze inputs again. Do not add document-specific exceptions to this recovery candidate.

After source purity is reliable, run independent blind validation of accepted changes and a source-led recall audit of the 31 remaining HIGH reviews. Prioritize cooling aggregation, room/pit identity, scope of fire requirements and internally inconsistent equipment labels. MEDIUM/LOW expansion is secondary. Keep missing OLD context as REVIEW; top-k absence is never proof.

No third tuning iteration or extra provider run was needed for this delivery. No deployment, push, production flag or human truth change is included.
''')
    md(root, 'CHECKPOINT.md', f'''
# Checkpoint

Completed research delivery. Verdict PARTIAL. Candidate iteration 2 of maximum 3; hash `{manifest['candidate_hash']}`. Protected baseline `{manifest['base_candidate_hash']}`.

Final PROVEN/REVIEW 10/66, HIGH resolved 6/37, new unique events 4, unchanged removed 1, accepted duplicate collapse 1. False accepted new events 0/4 agent-audited. Two inherited source-purity reviews remain flagged. All required reports are present.

Reproduce without network from repository main:

```sh
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PWD
python -m unittest experiments.project_change_text_v1.test_engine experiments.text_old_scope_recovery_v1.test_recovery
python -m experiments.text_old_scope_recovery_v1.run verify
python -m experiments.text_old_scope_recovery_v1.integrate --name audited
python -m experiments.text_old_scope_recovery_v1.report
```

The pinned baseline dependency directory is required; the system Python alone lacks the frozen jsonschema dependency versions. Do not rerun prepare/freeze into an existing frozen candidate. Raw responses, iteration-1 artifacts, source packets and audit annotations are preserved. Packaging adds explicit audit outcomes without rewriting provider proposals or frozen inference.

Production changed NO. Push/deploy 0/0. Human truth unchanged. ProjectChange schema/grouping source unchanged. Delivery hashes are recorded separately in DELIVERY_MANIFEST.json; frozen CANDIDATE_MANIFEST.json is not rewritten after the audit.
''')
    print(json.dumps(score, ensure_ascii=False))


if __name__ == '__main__':
    run()
