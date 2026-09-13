"""Human reports with explicit separation of constructed and corpus evidence."""
from collections import Counter
from pathlib import Path
import statistics
import subprocess

from .common import digest, file_hash, read, write


def md(root, name, text):
    (root / 'reports' / name).write_text(text.rstrip() + '\n')


def render(root, score, experiment, dev, input_docs, outputs, performance, frozen):
    chosen = experiment['approaches'][-1]
    changes = [c for o in outputs for c in o['project_changes']]
    reasons = Counter(r for c in changes for r in c['review_reasons'])
    s = score
    verdict = 'B' if s['entity_matches'] and s['project_changes'] else 'C'
    verdict_text = 'GROUPING WORKS, TABLE IDENTITY LIMITS COVERAGE' if verdict == 'B' else 'TABLE EVIDENCE TOO WEAK'
    md(root, 'TABLE_PROJECTCHANGE_ARCHITECTURE.md', f'''# TABLE → ProjectChange V1

Offline consumer of immutable Table V3 artifacts. No backend integration, production, TEXT/GRAPHIC inference, push or deploy.

Pipeline: pinned TABLE rows/headers → explicit scoped entity observations → typed differences → entity/event grouping → existing ProjectChange V1. Source loader calls only the public V3 row reader; it neither changes nor reruns V3 decisions. Unresolved candidate relations remain REVIEW. Captions, narrative text, section titles, graphic objects and older route verdicts do not decide table truth.

Chosen approach: hybrid_entity_event_collapse. A model is a state, never the equipment identity. Identity uses project, document comparison, explicit mark/stable ID/function, system, building/room/floor, class and explicit table group. Position alone never proves continuity. Different modes and quantity bases stay separate. Duplicate owners and contradictory values abstain.

The adapter supports explicit wide headers and vertical parameter tables with an in-table subject. Unsupported multi-level columns, anonymous passports, cable connections, arbitrary formulas, component expansion and cross-document aliases retain coverage gaps. It never fills identity from TEXT or forces table continuation. Repeated header/row/column placement is not an engineering change.

Addition/removal requires complete matching old AND new scope inventories, all known table components, no unknown rows, PROVEN boundaries, explicit equipment identity and an evidence-bearing absence certificate. Corpus inputs do not provide such certificates: unmatched rows remain REVIEW. Absence is not represented as zero or a fabricated cell quote.

Contract: urn:pdf-proverka:project-change:1, byte-equivalent exported JSON schema, imported unchanged validator with text_only=False plus TABLE receipt verification. No second ProjectChange schema. TABLE internal observations and diff records are evidence intermediates, never replacement product objects.

Result: {verdict} — {verdict_text}. This does not certify accuracy on a blind event benchmark.
''')
    rows = ''.join(f"| {x['approach']} | {x['project_changes']} | {x['exact_events']}/{x['project_changes']} | {x['false_project_changes']} | {x['duplicate_project_changes']} | {x['over_grouped']} | {x['under_grouped']} |\n" for x in experiment['approaches'])
    md(root, 'APPROACHES_COMPARED.md', f'''# Executed approaches

18 explicitly constructed DEV scenarios, 40 fact/presence differences, 17 expected events. This is executable event-partition validation, not human corpus truth or an OCR benchmark. Expected partitions are authored independently of grouping. All alternatives consume exactly the same extracted observations.

| Approach | Events | Exact events | False/type errors | Duplicate fragments | Over-grouped | Under-grouped |
|---|---:|---:|---:|---:|---:|---:|
{rows}
Entity-first collapses independent count and material decisions. Row/event clustering fragments a vertical replacement across source rows. Hybrid groups the same entity, compatible mode/basis and one event; an explicit changed model absorbs selected-product characteristics. It retains separate count, material, mode, requirement and composition decisions.

Choose hybrid: {chosen['exact_events']}/{chosen['project_changes']} exact constructed partitions, zero duplicates and grouping errors, deterministic auditable Python, no model calls. Twenty changed selected characteristics plus the model are retained in one replacement (21 → 1). A local LLM was not tested and is not included in the approaches count.

Raw fact partitions and all alternative outputs: APPROACH_EXPERIMENT.json. Independent accuracy for real ProjectChanges remains unmeasured. Source-only replay was loaded after CANDIDATE_FREEZE.json, with no changes to the algorithm based on its results.
''')
    md(root, 'TABLE_ENTITY_RESOLUTION_REPORT.md', f'''# Entity resolution

Entity key excludes model, changed parameters, source page, row index and version-local TableKey. Comparison scope includes both immutable DocumentVersions. Explicit system/class/function/location qualifiers prevent matching repeated marks across buildings or unrelated systems. Anonymous observations remain unresolved. Stable positions require a continuity certificate; ordinal equality cannot provide one.

Real source-only comparison: {s['document_pairs']} pairs, {s['entity_matches']} entity candidates, {s['entity_matches_proven']} with all implemented identity gates satisfied. {s['unresolved_observations']} unresolved entity/property observations. These are deterministic certificates, not measured entity matching accuracy.

Existing V3 human DEV: 44 boundary cases, 13 PROVEN correct / 31 REVIEW; 29.55% coverage. Those cases concern table boundaries inside one version. They do not label cross-version equipment identity or ProjectChanges. V3 SAME/NEW is never interpreted as equipment unchanged/added.

Source-only compared components: {s['table_components']}; boundary decisions PROVEN {s['table_boundary_proven']}, REVIEW {s['table_boundary_review']}. Counts are component-adjacent boundary decisions, not independent event truth. {len(dev)} DEV documents also passed identical-version zero-change replay.

Boundary and row coverage inventory: DEV_SOURCE_INVENTORY.json and SOURCE_ONLY_INPUT_VIEWS.json. Entity matches, alternatives and unobserved facts: TABLE_DIFFS.json. Unsupported rows remain visible and prohibit complete-scope absence.
''')
    md(root, 'TABLE_DIFF_CONTRACT.md', '''# Route-internal TABLE difference contract

Public output objects are the existing ProjectChange V1 schema. This document describes internal inputs only.

Record: project_scope, explicit subject qualifiers, record_id/table_key (provenance only), values, TABLE evidence and review_reasons. Each value contains property, value, unit, original quote, mode, basis, product_characteristic, semantic status, and cell evidence. `product_characteristic` is an explicit observation role; raw shared numbers do not prove dependence.

Match: unique scoped entity key, old/new record references, status and reasons. Model is excluded. Conflicting states for an entity/property are retained as unresolved observations and veto associated PROVEN events. Multiple unresolved owners are never resolved by row order.

Fact: immutable fact_id; same property/mode/basis old/new states; dimensional comparison; old/new evidence; all review reasons; entity key. Typed numeric values normalize only unambiguous decimals and a closed unit-conversion list. Unknown readings, incompatible dimensions and unsupported numeric headers remain REVIEW. Absence of a parameter produces an unresolved observation, not zero.

Coverage: raw_table_differences counts compared typed facts plus one-sided entity observations. paired_cell_difference_occurrences counts changed compared cells before duplicate representation collapse. unmatched_entity_observations is reported separately. These metrics are not a count of every cell in unsupported tables; table rows and coverage gaps are separate. Compression must never conceal excluded rows.

Each fact has one primary event in fact_ownership. Details retain all changed facts, their states and all cell references. Independent decisions remain separate. Reordering equivalent columns/rows and unit normalization produce no engineering events.

Evidence: TABLE route, immutable source receipts for PDF/MD/blocks, table/ledger artifact receipts, DocumentVersion, table_key, row_key, page, block, ledger line, Markdown line, zero-based column index and header row refs. TABLE quote is a cell, not narrative TEXT. text_purity_basis=[] is explicitly not applicable. A scope absence certificate is a machine witness with quote=null, not an invented missing-row quotation.

Persisted verification rehashes sources and artifacts, resolves cells through Table V3, verifies TABLE ownership for cell and header, checks source values and version/page/row locators. Extraction evidence is not raster validation: OCR ambiguity is not corrected automatically.
''')
    md(root, 'PARAMETER_COLLAPSE_REPORT.md', f'''# Parameter collapse

Constructed DEV: {chosen['raw_differences']} → {chosen['project_changes']} events, ratio {chosen['parameter_compression_ratio']:.4f}. Replacement stress case: model + 20 characteristics → one ProjectChange retaining all 21 facts. Replacement plus independent quantity change → two ProjectChanges. Different room/system/mode or conflicting values block unjustified collapse.

Real source-only: {s['raw_table_differences']} typed fact/presence observations → {s['project_changes']} candidates. Paired changed cell occurrences: {s['paired_cell_difference_occurrences']}; unmatched entity observations: {s['unmatched_entity_observations']}. Overall ratio: {s['parameter_compression_ratio']}. This includes REVIEW presence candidates; it is not a pure parameter compression statistic. Missing/unsupported table rows are not part of this denominator.

Every normalized fact has exactly one primary owner in TABLE_DIFFS.json. Public supporting_fact_changes retains bilateral changed facts; one-sided REVIEW has no fake supporting fact. All original observation details, units, mode/basis, alternatives and cell evidence remain in internal diff records. No cap at N parameters is used. Count, mode, material, requirements and composition are not swallowed by a replacement.

No causal inference from correlated values, no automatic formula assumptions, no event per formatting operation, no table-wide collapse. SPECIFICATION_RESTRUCTURED produces zero events for proven representation-only differences; meaningful explicit membership change maps to SYSTEM_CONFIGURATION_CHANGED. Unresolved restructuring is a coverage/review observation.
''')
    md(root, 'TABLE_PROJECTCHANGE_SCORECARD.md', f'''# Scorecard

| Metric | Existing source-only pairs | Constructed DEV controls |
|---|---:|---:|
| Raw typed fact/presence differences | {s['raw_table_differences']} | {chosen['raw_differences']} |
| Entity matches | {s['entity_matches']} | {chosen['entity_matches']} |
| ProjectChange candidates | {s['project_changes']} | {chosen['project_changes']} |
| PROVEN | {s['proven']} | {chosen['proven']} |
| REVIEW | {s['review']} | {chosen['review']} |
| False ProjectChanges | UNASSESSED | {chosen['false_project_changes']} |
| Duplicate IDs / duplicate truth events | {s['duplicate_project_changes']} IDs; semantic duplicates UNASSESSED | {chosen['duplicate_project_changes']} |
| Over-grouped | UNASSESSED | {chosen['over_grouped']} |
| Under-grouped | UNASSESSED | {chosen['under_grouped']} |
| Compression | {s['raw_table_differences']} → {s['project_changes']} | {chosen['raw_differences']} → {chosen['project_changes']} |

Corpus event truth does not exist in the permitted boundary DEV slice. UNASSESSED is JSON null, never a reported zero. PROVEN means implemented evidence gates pass; it is not a human quality label. REVIEW candidates are questions for an engineer. Real candidate precision/recall and semantic grouping accuracy cannot be inferred from source receipts or constructed tests.

V3 human DEV remains 13/44 PROVEN (13/13 correct), 31 REVIEW. No truth files were changed and no fresh holdout/EVAL answers were opened. Comparison membership is derived from the pre-existing conservation manifest using unique project + syntax-normalized document code and old/new stage membership; aliases beyond punctuation are intentionally unresolved.

Verdict: **{verdict} — {verdict_text}**.
''')
    engineer = ['# Изменения проекта по TABLE', '',
                f'Реальные исходные пары: {s["document_pairs"]}. Кандидатов: {len(changes)}. PROVEN: {s["proven"]}; REVIEW: {s["review"]}.',
                'REVIEW — предложение проверить возможное изменение. Неполнота таблицы не доказывает добавление или удаление.', '']
    for n, c in enumerate(changes, 1):
        subject = c['engineering_subject']
        engineer += [f'## {n}. {c["status"]} — {c["short_summary_ru"]}', '',
                     f'- Что изменилось: {c["change_type"]}.',
                     f'- Где: {c["comparison_scope"]}; система: {subject["system"] or "не установлена"}; помещение: {subject["room"] or "не установлено"}.',
                     f'- Было: {c["old_state"] or "не установлено; отсутствие не доказано"}.',
                     f'- Стало: {c["new_state"] or "не установлено; отсутствие не доказано"}.',
                     f'- Основная сущность: {subject["semantic_subject"]} ({subject["entity_id"]}).',
                     '- Почему это одно изменение проекта: один сопоставленный инженерный слот и одна группа события; характеристики сохраняются в деталях. Для REVIEW это основание кандидата, а не подтверждённое событие.',
                     f'- Причины REVIEW: {", ".join(c["review_reasons"]) or "нет"}.', '', '<details>',
                     '<summary>Подробности и evidence rows/cells</summary>', '',
                     '| Параметр | Было | Стало |', '|---|---|---|']
        for f in c['supporting_fact_changes']:
            engineer.append(f"| {f['property']} | {f['old']['value']} {f['old']['unit'] or ''} | {f['new']['value']} {f['new']['unit'] or ''} |")
        engineer += ['', 'Доказательства исходных ячеек:', '']
        for side in ('old', 'new'):
            for e in c['evidence_' + side]:
                loc = e['locator']
                path = e['source_receipts']['work_md']['path']
                quote = (e['quote'] or '').replace('\n', ' ')
                engineer.append(f"- {side.upper()}: [{e['document_code']}, PDF стр. {loc['page']}, MD {loc['markdown_line']}, столбец {loc['column_index'] + 1}](<{path}:{loc['markdown_line']}>) — {quote}; table={loc['table_key']}; row={loc['row_key']}; evidence={e['evidence_id']}.")
        engineer += ['', '</details>', '']
    if not changes:
        engineer += ['В разрешённых парах не получено адресуемых межверсионных ProjectChange. См. покрытие и неразрешённые наблюдения в TABLE_DIFFS.json.', '']
    engineer += ['## Отдельные сконструированные примеры', '',
                 'Эти примеры не являются найденными изменениями реального проекта:', '',
                 '- Приточная установка П1: модель A заменена на B; 20 связанных характеристик остаются деталями одного события.',
                 '- П1: количество изменено с 1 до 2 шт.; событие отдельно от замены.',
                 '- П1: производительность изменена с 1000 до 1500 м³/ч, напор — со 100 до 150 Па.', '',
                 'Полный набор: CONSTRUCTED_DEV_CHANGES.json.']
    md(root, 'ENGINEER_FACING_REPORT.md', '\n'.join(engineer))
    links = [dict(project_change_id=c['project_change_id'], engineering_entity=c['engineering_subject'],
                  system=c['engineering_subject']['system'], mark=c['engineering_subject']['mark'],
                  function=c['engineering_subject']['engineering_function'], old_state=c['old_state'], new_state=c['new_state'],
                  evidence_source_type='TABLE', comparison_scope=c['comparison_scope'],
                  global_entity_id=None, event_key=c['event_key']) for c in changes]
    write(root / 'reports/FUSION_LINKS.json', links)
    md(root, 'FUTURE_FUSION_COMPATIBILITY.md', '''# Future route linking

READY at the existing ProjectChange schema and linking-field level. Fusion implementation and global entity resolution are NOT implemented or validated.

| Required concept | Existing contract field |
|---|---|
| engineering_entity | engineering_subject |
| system | engineering_subject.system |
| mark | engineering_subject.mark |
| function | engineering_subject.engineering_function |
| old_state / new_state | old_state / new_state |
| evidence_source_type | routes=['TABLE']; evidence_*.route='TABLE' |
| Local scope | comparison_scope, engineering_subject.scope_key |
| Stable event link | event_key, project_change_id |

Core objects validate unchanged against the TEXT project's shared ProjectChange V1 schema. FUSION_LINKS.json is an optional index pointing to core IDs, not a second ProjectChange schema. Global entity IDs stay null; entity IDs are comparison-scoped. Old/new model choices are separate from entity identity. Unknown system/mark/function stays null rather than borrowing TEXT/GRAPHIC.

Requested TABLE taxonomy maps to existing vocabulary: SYSTEM_COMPOSITION_CHANGED → SYSTEM_CONFIGURATION_CHANGED; IMPORTANT_PARAMETER_CHANGED → OTHER_ENGINEERING_CHANGE with typed supporting properties. EQUIPMENT_REPLACED/ADDED/REMOVED/COUNT_CHANGED and CAPACITY_CHANGED already exist. Meaningful restructuring with explicit membership change uses configuration; pure formatting/restructuring stays outside ProjectChange. No enum was added.

Future fusion must resolve cross-document/route aliases and conflicts, preserve source-specific evidence, distinguish overlapping claims from independent count/mode decisions, and re-evaluate claim-level REVIEW. Current local IDs must not be treated as global physical equipment IDs. TEXT V1 behavior is unchanged.
''')
    md(root, 'QUALITY_AUDIT.md', f'''# Quality audit

Source-only candidate extraction is separated from constructed DEV quality scores. Current permitted human TABLE truth labels 44 within-version boundaries, not events between revisions. No new labels are written to truth, no holdout/EVAL sources/answers are used, and no result is promoted solely because a source-only replay looks good.

Checks: shared schema equality; semantic contract validator; TABLE route purity; receipt and persisted cell/header ownership verification ({s['verified_evidence_cells']} evidence cells); same-version zero-event replay for {len(dev)} DEV documents; deterministic comparison replay; one primary owner per fact; no duplicate public IDs ({s['duplicate_project_changes']}); explicit unknown absence. Tests include 20-parameter vertical replacement, independent count, distinct buildings, source tampering, non-TABLE ownership, unit conversion, conflicting/duplicate values and boundary REVIEW.

REVIEW reason counts: {dict(reasons)}.

Limitations: automatic extraction covers explicit header vocabulary, not all equipment tables. No raster adjudication, human event precision, semantic duplicate/over/under grouping labels, full scope completeness or blind generalization result. Source receipts do not prove OCR accuracy. Unknown real false/over/under counts remain null. A passing constructed suite does not warrant blind-validation readiness by itself.

No relevant findings are hidden in compression metrics. TABLE_DIFFS.json preserves unmatched properties and conflicting observations; input views preserve coverage gaps. Boundary REVIEW is not upgraded by grouping. Source-only examples are never added to rules after the candidate freeze.

Safety snapshot: INITIAL_SNAPSHOT.json (when supplied by the session); final protected-file comparison is recorded in SAFETY_AUDIT.json. Table V3 algorithm: unchanged. TEXT algorithm: unchanged. Truth: unchanged. GRAPHIC: absent. Production: no. Push/deploy: 0/0.
''')
    md(root, 'PERFORMANCE_REPORT.md', f'''# Performance

Measured local wall time: {performance['wall_seconds']:.3f} s. Source-only loading, receipt checks and comparison: {performance['source_load_compare_verify_seconds']:.3f} s. Peak process RSS: {performance['max_rss_kib']} KiB. Python {performance['python']}.

Five warm identical-version comparison passes over selected source-only views: {performance['warm_same_version_replay_seconds']}; median {statistics.median(performance['warm_same_version_replay_seconds']):.4f} s. Repeated output hashes agree. These timings are not Table V3/OCR extraction timings or production service latency.

LLM calls 0; network calls 0; new OCR calls 0. Source files are local. Diff uses scoped hash maps and sorted deterministic outputs. Full source receipt hashing is included in run wall time. Full source views preserve evidence for replay at the cost of larger output artifacts.
''')
    manifest = dict(**frozen, schema_id='urn:pdf-proverka:project-change:1', approaches_tested=3,
                    constructed_dev=chosen, source_only_score=s, verdict=verdict, verdict_text=verdict_text,
                    future_fusion='READY: schema/linking only; fusion not implemented',
                    table_v3_modified=False, text_modified=False, truth_modified=False,
                    production=False, push=0, deploy=0, candidate_commit=None,
                    files_at_freeze=frozen['code_files'])
    write(root / 'reports/CANDIDATE_MANIFEST.json', manifest)
    md(root, 'NEXT_ACTION.md', f'''# Next action

Verdict {verdict}: {verdict_text}.

Retain this candidate and its explicit coverage limitations. Before deciding blind-validation readiness, obtain a permitted independent development set of actual table revision pairs with entity correspondence, engineering event partitions and scope-completeness labels. Current boundary truth cannot supply those labels. Additions/removals additionally need evidence-bearing complete TABLE scopes; extend extraction for multi-level headers and anonymous passports only in a separately authorized task using DEV data.

Use blind sources only after candidate freeze and evaluation protocol approval; do not tune against fresh holdout/EVAL. Future TEXT/TABLE/GRAPHIC linking fields are ready, but route fusion/global identity remains a separate task. No production action is proposed or executed here.
''')
    md(root, 'CHECKPOINT.md', f'''# Checkpoint

TABLE PROJECT CHANGE V1 COMPLETE. Offline implementation, three executed approaches, constructed DEV controls, immutable DEV source replay and existing source-only comparison replay complete. Reports and raw evidence are in this directory.

Chosen hybrid; source-only differences {s['raw_table_differences']} → {s['project_changes']} candidates ({s['proven']} PROVEN / {s['review']} REVIEW). Constructed controls {chosen['raw_differences']} → {chosen['project_changes']}; false/duplicates/over/under = 0/0/0/0. Source-only false/semantic grouping quality remains unassessed.

No algorithm changes after source-only replay. Table V3/TEXT/truth unchanged. Production no; push/deploy 0/0. Final commit and protected-file receipts are recorded in CANDIDATE_MANIFEST.json and SAFETY_AUDIT.json after session closeout. Verdict {verdict} — {verdict_text}.
''')
