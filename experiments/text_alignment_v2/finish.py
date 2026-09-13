"""Traceable post-freeze reports; never relabel or repair frozen predictions."""
import argparse
from collections import Counter,defaultdict
from pathlib import Path

from experiments.text_comparison_v1.common import read,write,file_hash,digest
from .run import verify


def finish(root):
    root=Path(root);reports=root/"reports";freeze=verify(root)
    project=read(root/"run1/project.json");audit=read(root/"quality_audit/decisions.json")
    ctx=read(reports/"CONTEXT_AND_SCOPE.json");cs=ctx["summary"]
    dev=read(root/"dev_experiment/scorecard.json");devperf=read(root/"dev_experiment/performance.json")
    alt=read(root/"postfreeze_alternatives/metrics.json")["metrics"];altperf=read(root/"postfreeze_alternatives/performance.json")["seconds"]
    perf=read(root/"run1/performance.json");perf2=read(root/"run2/performance.json")
    baseline=read(root.parent/"20260913_text_comparison_goal_driven/reports/FINAL_METRICS.json")
    holdout=read(root/"fresh_holdout/selection.json")
    replay=read(reports/"REPLAY_AUDIT.json")
    pairs=project["pairs"];changes=Counter(c["category"] for c in project["changes"])
    total={k:sum(p[k] for p in pairs) for k in ("old_units","new_units","eligible_old_units","eligible_new_units","aligned_old_units","aligned_new_units","aligned_relations","review_units","pages")}
    shapes=sum((Counter(p["shapes"]) for p in pairs),Counter())
    scope={k:{s:sum(p[k][s] for p in ctx["pairs"]) for s in ("old","new")} for k in ("narrative_lines","aligned_lines","baseline_matched_lines_on_same_denominator","aligned_lines_outside_baseline_matched_scope")}
    units=total["old_units"]+total["new_units"];aligned=total["aligned_old_units"]+total["aligned_new_units"]
    line_den=sum(scope["narrative_lines"].values());line_b=sum(scope["baseline_matched_lines_on_same_denominator"].values());line_c=sum(scope["aligned_lines"].values())
    results={"verdict":"C","readiness":"BLOCKED_REJECTED_FROZEN_CANDIDATE","research_complete":True,"product_improvement_demonstrated":False,
             "candidate_commit":freeze["candidate_commit"],"candidate_content_hash":freeze["candidate_content_hash"],
             "approaches_tested":3,"selected_architecture":"Hybrid content retrieval with deterministic local identity certificates; exact typed fact comparison",
             "baseline":{"engineering":0,"editorial":2,"review_changes":1198,"matched_sections":171,"review_section_relations":1067},
             "project":{**total,"shapes":dict(shapes),"changes":dict(changes),"raw_unit_coverage":aligned/units,"raw_line_coverage":line_c/line_den,"baseline_matched_line_coverage":line_b/line_den},
             "quality_audit":{"engineering":audit["engineering_counts"],"alignment":audit["alignment_counts"],"human_truth":False},
             "context":cs,"runtime_seconds":perf["seconds"],"replay_runtime_seconds":perf2["seconds"],"fresh_holdout_n":holdout["N"],
             "model_calls":0,"input_tokens":0,"output_tokens":0,"production_modified":False,"push":0,"deploy":0,
             "routed_table_rows_compared":False,"table_legend_leakage_found":True,"absolute_no_table_content_claim_valid":False,
             "routed_graphic_content_compared":False,"replay_pass":replay["pass"],"scope_metrics":scope,
             "next_step":"Keep candidate rejected and holdout blind. On DEV-only sources, repair complete-unit parsing and narrative-role eligibility, then evaluate local semantic adjudication for rewrites/splits and freeze a new candidate before any holdout annotation."}
    write(reports/"FINAL_METRICS.json",results)

    def md(name,text):
        (reports/name).write_text(text.strip()+"\n")

    # Freeze outputs stay unchanged; audit annotations are an explicitly separate
    # view. Grouping is navigation by document and available heading, not proof of
    # section identity or cross-layer AtomicChange fusion.
    groups=defaultdict(lambda:defaultdict(list))
    for c in project["changes"]:
        titles=[s["title"] for s in c["section_context"]]
        groups[c["pair_key"]][titles[0] if titles else "No reliable heading"].append(c["change_id"])
    write(reports/"PROJECT_TEXT_CHANGES.json",{"schema":"project-text-changes.v2-audited-view","candidate_content_hash":freeze["candidate_content_hash"],
                                             "release_readiness":"REJECTED","frozen_predictions_unmodified":True,"changes":project["changes"],
                                             "navigation_groups":{p:dict(x) for p,x in groups.items()},
                                             "post_freeze_agent_audit":audit,"human_truth":False})

    md("TEXT_ALIGNMENT_ARCHITECTURE.md",f"""# TEXT Alignment V2 architecture

**Frozen candidate rejected for readiness (verdict C).** It demonstrates that local text comparison can bypass SectionRelation, but does not safely deliver material engineering coverage.

Source Markdown/blocks/PDF receipts → existing LineLedger/PageModel/FurnitureModel/CaptionModel/HeadingModel and TextSection ownership → local paragraph/list units (including uncertain/orphan section ownership) → cheap inverted retrieval → unique local identity certificate → separate typed fact comparison → document/heading navigation and exact source evidence.

Selected before project inspection: hybrid lexical postings plus canonical text and typed assertion-template indices. For each OLD unit or adjacent 2–3-unit variant, shortlist at most 3 NEW candidates. Many-to-many is not forced. Move tolerance comes from content indices; order is only used for adjacent grouping and exact neighbor witnesses. There are no LLM calls or whole-project prompts.

TextSection is reused as context, heading ranking prior, scope corroboration and output grouping. No SectionRelation object is read by the aligner. Section ownership REVIEW is retained in metadata and does not automatically bar comparison. {sum(scope['aligned_lines_outside_baseline_matched_scope'].values())} aligned source lines lie outside the baseline matched-section scope. This is raw scope expansion, not a verified engineering benefit.

Alignment and fact comparison are separate. Exact canonical prose or a complete typed template plus reciprocal uniqueness can certify SAME_ENGINEERING_SUBJECT. Fuzzy lexical retrieval alone never certifies it. The fact stage additionally requires local heading/neighbor/mark scope evidence; unexplained wording, range/formula ambiguity and unmatched content stay REVIEW.

Observed blockers: reused quantity lexicon parses a prefix of м³/сут.; source roles leak headings, TOC, letterhead and a table legend into narrative; exact paragraph templates fail substantive rewrites and most small engineering facts; duplicated statements inflate event counts. The candidate remains frozen without post-project repairs.

`FACT_ADDED` / `FACT_REMOVED` are reserved output types; this candidate never emits them because it lacks positive absence/scope certificates. This is safe abstention and a coverage limitation. Local LLM alignment/adjudication is an untested future extension, not a claimed result.

Code: `{freeze['candidate_commit']}`. Configuration and all reused source-module hashes: CANDIDATE_MANIFEST.json. There are no production imports, flags, table producers, Sheet changes or cross-layer fusion.
""")
    md("LOCAL_UNIT_CONTRACT.md","""# Local narrative unit contract

A unit is a source paragraph or list item. A paragraph larger than 1800 characters is split only at natural sentence punctuation; an unsplittable unit over 6000 characters remains REVIEW. No arbitrary fixed token windows are used. Adjacent 2–3 units may form a candidate variant if ledger lines are contiguous, every unit is eligible and total text is at most 6000 characters. Groups cannot cross a routed exclusion.

Each unit preserves `unit_id`, document version/code, source page span, block ids, original ledger line ids and SHA-256, Markdown line and within-block line, paragraph id and character span, complete source text, section contexts/status, nearest heading, ownership certainty, eligibility reasons, and TABLE_REF/GRAPHIC_REF markers. Multiple sentence units may reference the same original line; paragraph spans distinguish their text. Every eligible TEXT non-heading line is represented; references never silently disappear.

Normalization removes Markdown emphasis, initial list/heading markup, repeated whitespace, case and ё/е distinction. It does not remove engineering numbers, unit spelling, numeric punctuation, inequalities, signs or equipment identifiers. Terminal prose punctuation has a separate editorial certificate. General word reordering and stylistic rewrites remain REVIEW rather than being declared engineering changes.

Foundation TABLE/GRAPHIC/FURNITURE routes and proven HEADING lines are excluded. Units with fewer than five alphabetic words remain REVIEW. Source invariants pass on all 18,772 units and 20,939 narrative line ids, but they verify inherited routing, not its factual correctness. The post-freeze raster audit found 5/24 narrative-role errors, including a table legend and letterhead. Therefore this extraction contract is not validated for production; strengthening role eligibility is required.

TextSection ownership is metadata. A REVIEW owner cannot block exact local alignment, and local identity cannot upgrade the section's ownership certainty.
""")
    md("CANDIDATE_RETRIEVAL_REPORT.md",f"""# Candidate retrieval report

Hybrid builds lexical word postings, exact canonical text indices and typed-template indices over 1–3 adjacent narrative units. Posting-score pools are capped at 256, with complete exact canonical/template hits added independently of the cap. At most 3 candidates are retained for each OLD variant. Exact global duplicate checks include the full index, so a truncated posting list cannot manufacture uniqueness.

Identity uses complete local assertions, not bag-of-words overlap, section number, page proximity or an ordinal pairing of quantities. A reciprocal unique certificate is required. Singleton exact matches support neighbor evidence; shared nontrivial heading text or explicit marks provide additional scope support for facts. Lexical score and heading priors rank candidates but cannot alone prove identity.

Project: {cs['retrieval_packages']:,} retrieval packages, {cs['retrieval_packages_with_candidates']:,} with candidates. Among eligible unaligned OLD singletons, {sum(p['retrieval_unproven_old_singletons'] for p in ctx['pairs']):,} have unproven candidates and {sum(p['retrieval_no_candidate_old_singletons'] for p in ctx['pairs']):,} have no retrieved candidate. Ineligible short/oversize units are separate abstentions. No missing counterpart is called an added/removed fact.

Project output uses `kind=NO_MATCH, decision=UNSURE` for each unmatched unit: this means no *certified* match, even where the shortlist contains candidates. All {total['review_units']:,} such items are REVIEW. It is not a proven semantic absence. `REVIEW` shape is reserved for unresolved paired groups, which this version does not emit; presenting its zero count as zero review would be misleading.

Stored per package: OLD ids, top NEW ids, score, certificate, global uniqueness, scope evidence, pool size, local character count and input hash. No model output/cache records exist because no model was called.

Observed weakness: complete-template matching resolves little reworded engineering text. The post-freeze diagnostics for specific load, fan-coil property, area and added cooling demand all remain without automatic engineering output. No thresholds or lexicons were changed from those diagnostics.
""")
    md("AI_ALIGNMENT_CONTRACT.md","""# AI alignment contract and actual use

Pipeline AI use: **NONE**. Model, prompt, temperature, provider settings and active input/output schemas are null in CANDIDATE_MANIFEST.json. Model calls / billed input / billed output: **0 / 0 / 0**. No provider account was accessed and no background queries were made. The coding assistant's research reasoning and manual source audit are separate from pipeline telemetry; their token cost is not available and is not claimed to be zero.

Actual deterministic stages use separate outputs: alignment decision (`SAME_ENGINEERING_SUBJECT` or `UNSURE` in this implementation), alignment shape, source ids, certificate/scope evidence; then change category, complete OLD/NEW quotes, source refs and a typed field delta. The broader intended alignment vocabulary includes `RELATED_BUT_DIFFERENT` and `NO_MATCH`; these are available in the blind annotation schema, not fabricated model outputs.

A future local model extension must freeze a new candidate, model/version, prompt, schemas, generation settings and evidence requirements before use. Allowed input is one OLD local unit/group, at most three NEW local candidates, nearest heading/marks and minimal neighbor context. Return alignment only: SAME_ENGINEERING_SUBJECT / RELATED_BUT_DIFFERENT / NO_MATCH / UNSURE, candidate ids and supporting source quotes. Compare engineering meaning in a separate stage only after alignment. Neither stage may infer removal from missing retrieval.

Per-call cache must retain input hash, candidate ids, model and prompt version, output plus output hash, provider token usage, errors and any abstention. Replay uses cached I/O only and must never silently query the model. This extension is **not implemented or tested here**; no semantic-model benefit is claimed.
""")
    md("ALIGNMENT_SCORECARD.md",f"""# Alignment scorecard

No human TextAlignment labels exist. SECTION/OWNER DEV truth is unchanged and cannot score this layer. The pinned truth SHA-256 remains `71ed12f693442665a64c073a56f3d13df321bfdd0303f4a378286437616906d7`.

DEV source-derived software contracts, not human truth: hybrid {dev['summary'][2]['correct']}/{dev['summary'][2]['accepted']} accepted correspondences agree with construction, coverage 1811/1881 = 96.28%. Sequence 1166/1881 = 61.99%; engineering-anchor retrieval 375/1881 = 19.94%. The corpus is intentionally transformed; these are not estimates of natural rewrite accuracy or project precision.

Project units OLD/NEW: {total['old_units']:,} / {total['new_units']:,}; eligible: {total['eligible_old_units']:,} / {total['eligible_new_units']:,}. Accepted relations {total['aligned_relations']:,}: 1→1 {shapes['ONE_TO_ONE']:,}; 1→N {shapes['ONE_TO_N']}; N→1 {shapes['N_TO_ONE']}. Aligned local units OLD/NEW {total['aligned_old_units']:,} / {total['aligned_new_units']:,}. Unmatched/review units {total['review_units']:,}, or {100*total['review_units']/units:.2f}%. Raw unit coverage {100*aligned/units:.2f}% (all units); {100*aligned/(total['eligible_old_units']+total['eligible_new_units']):.2f}% among eligible units. Both denominators include upstream routing uncertainty.

Predeclared diverse agent audit: 19/24 accepted local correspondences pass narrative-role review; 5/24 fail (heading, TOC/body role, header, footer, table legend). One of the 19 is administrative context only. Observed acceptance 79.17%, not a human or representative population precision estimate. Content-string identity alone is insufficient. The engineering-proposal alignments themselves are all locally corroborated; one subsequent fact representation is wrong.

Source line scope, same denominator: baseline {line_b}/{line_den} = {100*line_b/line_den:.2f}%; content-first {line_c}/{line_den} = {100*line_c/line_den:.2f}%. The new route adds {sum(scope['aligned_lines_outside_baseline_matched_scope'].values())} aligned lines outside matched baseline sections, but misses some baseline lines and includes audited non-narrative material. This is not validated useful engineering coverage.

General semantic 1→N and N→1 rewrites are not proven by exact split/merge successes, many of which are pagination or formatting splits. No-match never authorizes FACT_REMOVED/FACT_ADDED. Human precision and recall remain null.
""")
    md("TEXT_CHANGE_SCORECARD.md",f"""# Text change scorecard

Frozen proposals: engineering **{changes['ENGINEERING_CHANGE']}**, editorial **{changes['EDITORIAL_CHANGE']}**, NO_SEMANTIC_CHANGE **{changes['NO_SEMANTIC_CHANGE']}**, REVIEW **{changes['REVIEW']:,}**. Engineering types: VALUE_CHANGED 3; PROPERTY_CHANGED / SYSTEM_CHANGED / REQUIREMENT_CHANGED / FACT_ADDED / FACT_REMOVED 0.

Agent audit of all engineering proposals: **confirmed 2; false 1; unresolved 0**. The two confirmed rows repeat the same active calculated building power change, 1181.7 → 1935.4 kW, in two locations of the energy-efficiency document. Thus only **one distinct corroborated engineering event** is demonstrated. No whole-project human precision or recall is available.

The false row has a real source change, water demand 117.85 → 121.33 m³/day, but emits 424260 → 436788 m³/h after parsing the prefix `м3/с` from `м3/сут.`. It is counted false because its structured engineering representation is wrong; counting only the existence of changed digits would conceal the defect. The frozen prediction is retained unmodified and explicitly rejected in the audited report.

All three retrieval approaches share this comparator and emit the same three engineering rows, including the same defect. They are all rejected for safe product readiness. None of the four historical diagnostic classes produces an automatic engineering change after freeze. Removal/addition, semantic rephrasing and system/property rewrites remain coverage blockers.

Editorial and unchanged results were not exhaustively audited. Alignment audit shows that these counts contain heading/furniture/table-legend artifacts. They must not be presented as independently useful engineering decisions.
""")
    rows=["| Pair | Units OLD/NEW | 1:1 / 1:N / N:1 | Aligned | Engineering / editorial / REVIEW |","|---|---:|---:|---:|---:|"]
    for p in pairs:
        sh=p["shapes"];ch=p["changes"]
        rows.append(f"| {p['source_pair_id']} | {p['old_units']}/{p['new_units']} | {sh.get('ONE_TO_ONE',0)}/{sh.get('ONE_TO_N',0)}/{sh.get('N_TO_ONE',0)} | {p['aligned_relations']} | {ch.get('ENGINEERING_CHANGE',0)}/{ch.get('EDITORIAL_CHANGE',0)}/{ch.get('REVIEW',0)} |")
    md("PROJECT_TEXT_ALIGNMENT_REPORT.md",f"""# Project text alignment report

**TEXT ALIGNMENT V2 COMPLETE — verdict C; frozen candidate rejected, product goal blocked.**

Same pinned project: 23 pairs, 46 documents, {total['pages']} pages. Candidate `{freeze['candidate_commit']}`. All source Markdown/blocks/PDF hashes were checked. TABLE/GRAPHIC routed blocks were not imported; post-freeze source audit nevertheless found a table legend routed as TEXT, so an absolute no-table-content safety claim is not supported.

{chr(10).join(rows)}

Totals: {total['old_units']:,} OLD / {total['new_units']:,} NEW units; {total['aligned_relations']:,} accepted relations; {total['aligned_old_units']:,} OLD and {total['aligned_new_units']:,} NEW units aligned; {total['review_units']:,} review units. Candidate changes: engineering 3, editorial 76, unchanged 1298, REVIEW 16004. Each unmatched unit is a review work item; these counts are not comparable in granularity to baseline section review counts.

## Audited engineering findings by document and heading

Document: АА_БЭ–03-ДС3–ЭЭ → АА_БЭ–ДС3–ЭЭ_20260301.

- **Сведения о потребителях электроэнергии:** active calculated building power 1181.7 → 1935.4 kW. OLD PDF p9 / NEW p8. Confirmed from local source text and rasters.
- **Корпус 4 (inherited heading context):** the same power statement appears again, OLD p12 / NEW p11. The source actually introduces it with “Потребность в электроэнергии … ИОС1.1”; the inherited heading is imperfect. This is a duplicate witness to the event, not another changed system.
- **Water-demand row, inherited electricity heading:** real source text changes 117.85 → 121.33 m³/day, but the candidate's structured unit conversion is false. Rejected proposal; do not use its numerical output.

PROJECT_TEXT_CHANGES.json preserves every frozen prediction, full provenance and navigation groups, with separate agent-audit decisions. CANDIDATE_RETRIEVAL_REPORT.md and QUALITY_AUDIT.md explain uncertainty. The candidate is not ready for acceptance scoring or deployment. The untouched blind packet remains unannotated for a future frozen candidate.
""")
    md("SECTION_GATED_VS_CONTENT_FIRST.md",f"""# Section-gated versus content-first

| Metric | Frozen section-gated baseline | Content-first candidate |
|---|---:|---:|
| Project | 23 pairs / 46 docs / 2663 pages | same sources |
| Matched scope objects | 171 section relations | 1377 local relations |
| Raw matched narrative lines on common denominator | {line_b}/{line_den} ({100*line_b/line_den:.2f}%) | {line_c}/{line_den} ({100*line_c/line_den:.2f}%) |
| Engineering proposals | 0 | 3 |
| Audit-confirmed engineering rows | 0 proposals to audit | 2, one distinct event |
| False structured engineering changes | no engineering proposals | 1/3 audited |
| Editorial | 2 | 76 |
| NO_SEMANTIC_CHANGE | 98 | 1298 |
| REVIEW change records | 1198 section/fact-level records | 16004 unit-level records |
| Relation REVIEW | 1067 section relations | 16004 unmatched units, all UNSURE |
| End-to-end runtime | 28.110 s (previous observation) | {perf['seconds']:.3f} s; replay {perf2['seconds']:.3f} s |
| Pipeline model calls / input / output tokens | 0 / 0 / 0 | 0 / 0 / 0 |
| Human TextAlignment/TextChange precision | unavailable | unavailable |

Coverage comparisons use the intersection of baseline section source lines with this candidate's narrative line denominator. Matched lines are raw structural scope, not a guarantee of engineering meaning. Unit REVIEW counts and section REVIEW counts cannot support a percentage 'noise reduction' claim.

The architecture removes the mandatory SectionRelation gate and increases raw local scope, but safe material engineering benefit is not demonstrated: one distinct corroborated event, one false structured fact and narrative-role leakage. Verdict C applies to this frozen implementation, not a proof that all content-first approaches fail.
""")
    auditrows=["| Sample | Verdict | Issue / interpretation |","|---|---|---|"]
    for x in audit["alignment"]:
        auditrows.append(f"| {x['sample_number']} | {x['verdict']} | {x['failure_code'] or x['engineering_usefulness']} |")
    md("QUALITY_AUDIT.md",f"""# Post-freeze quality audit — agent review, not human truth

The coding assistant audited source evidence. This is **not an independent human audit**, annotation or a replacement for blind labels. Selection policy was declared before project execution and frozen in CANDIDATE_MANIFEST.json: all engineering proposals if ≤30, plus up to 24 diverse accepted relations across shape/certificate/ownership and document pairs, deterministic hash selection independent of correctness. Exact selection and decisions: ../quality_audit/selection.json and ../quality_audit/decisions.json.

Engineering population/sample **3/3**: **2 confirmed / 1 false / 0 unresolved**. Both power rows are verified against old p9/p12 and new p8/p11 native text and rendered PDFs; they describe one repeated event. The water row's source delta exists, but daily units are incorrectly parsed/converted as seconds. The strict structured-fact audit counts this false. Selection did not omit the failed row.

Alignment sample **24**: **19 confirmed / 5 false / 0 unresolved** under end-to-end narrative eligibility. One confirmed relation is administrative metadata, not an engineering benefit. All sampled content strings are locally similar/equal, illustrating why surface equivalence alone is insufficient. The failures are upstream role errors, not swapped numerical source paragraphs.

{chr(10).join(auditrows)}

PDF source-role evidence is saved for cases 1, 5, 9, 20, 21 and 24. Case 5 is a table-specific legend on the emission-source parameter table; adjacent column numerals contaminate OCR prose. Case 9 is a heading. Case 20 is letterhead footer text. Case 21 aligns TOC title to body heading. Case 24 is letterhead header text. Other confirmations use complete local Markdown quotes plus verified source hashes; they are not all separately checked against page rasters.

Source invariant audit passes all 20,939 line witnesses and 18,772 units, with no duplicates or routed exclusion reuse. This does **not** establish correct original routing. No detailed table data rows or graphic internals were observed in the inspected accepted units, but the table legend is table content and violates the intended TEXT-only boundary. `table_content_compared=false` in frozen raw outputs describes route labels only; this audited finding supersedes an absolute safety claim.

Observed proportions (2/3 structured engineering; 19/24 narrative alignment) are descriptive, clustered and deliberately stratified. No population precision interval or human-truth claim is warranted. Candidate unchanged after findings; readiness rejected.
""")
    md("MODEL_CONTEXT_BUDGET.md",f"""# Model and local context budget

Exact tokenizer: tiktoken {ctx['version']}, o200k_base; existing local dependency/cache reused. Full raw Markdown OLD+NEW count: {cs['raw_markdown_once_tokens_count_only']:,} tokens (count only, includes excluded content). Full routed narrative OLD+NEW once: **{cs['full_narrative_once_tokens']:,}** tokens. The narrative denominator still includes audited role leakage.

Retrieval: {cs['retrieval_packages']:,} OLD local-variant packages, {cs['retrieval_packages_with_candidates']:,} with candidates, at most 3 NEW candidates each. Summed candidate prose: **{cs['retrieval_content_tokens']:,}** tokens. Summed compact potential local packages with headings/ids/provenance: **{cs['retrieval_package_tokens']:,}** tokens. Overlapping variants are counted separately; these totals exceed a single full-project narrative pass ({cs['retrieval_package_tokens']/cs['full_narrative_once_tokens']:.2f}× for packages). They were not sent to a model.

Median potential package: **{cs['median_local_package_tokens']:.0f}** tokens; maximum **{cs['max_local_package_tokens']}**. Median reduction versus that pair's full narrative, among packages with candidates: prose **{100*cs['median_content_reduction_per_nonempty_package']:.2f}%**, package with context **{100*cs['median_package_reduction_per_nonempty_package']:.2f}%**. Aligned local-unit prose totals {cs['aligned_local_unit_tokens']:,} tokens. Report coverage alongside reduction: raw aligned units {100*aligned/units:.2f}%, with quality failures.

Actual model calls/input/output: **0 / 0 / 0**; average actual model context **N/A**. No billed cost saving is claimed. The reduction describes local potential package size, not aggregate cost improvement or the coding assistant's own context/token usage. Per-package exact counts and scope denominators: CONTEXT_AND_SCOPE.json.
""")
    md("PERFORMANCE_REPORT.md",f"""# Performance report

End-to-end selected candidate, same frozen 23-pair / 46-document / 2663-page project: run1 **{perf['seconds']:.3f} s**, run2 **{perf2['seconds']:.3f} s**. Previous section-gated observation 28.110 s. This is observational timing on a shared environment, not a controlled speed benchmark.

Peak RSS: {perf['peak_rss_kib']:,} KiB ({perf['peak_rss_kib']/1024:.1f} MiB). Run1 JSON artifact bytes: {perf['artifact_bytes']:,}. Materialization/source checking {sum(p['materialize_seconds'] for p in perf['pairs']):.3f} s; local alignment {sum(p['alignment_seconds'] for p in perf['pairs']):.3f} s; fact comparison {sum(p['comparison_seconds'] for p in perf['pairs']):.3f} s. Remaining elapsed time includes output serialization.

DEV source-derived experiments: sequence {devperf['sequence']:.3f} s; engineering-anchor {devperf['engineering_anchor']:.3f} s; hybrid {devperf['hybrid']:.3f} s. Post-freeze alternative project runs over already materialized units: sequence {altperf['sequence']:.3f} s; engineering-anchor {altperf['engineering_anchor']:.3f} s. Those exclude source materialization and are not directly comparable to full candidate runs.

Pipeline model calls/input/output tokens: 0/0/0, provider latency zero. Exact potential context is measured separately; no cost claim includes research-assistant overhead.

Replay: {replay['files_compared']} deterministic JSON files byte-identical; wall time/RSS excluded in performance.json. Candidate code/source hashes verified. New contract tests 16/16; Foundation/V1/safe-coverage 64/64. Holdout and audit-selection replay receipts are recorded separately. A passing replay/test suite does not fix the observed engineering-unit and narrative-role defects.
""")
    md("NEXT_ACTION.md","""# Exact next action

**Verdict C — CONTENT-FIRST DOES NOT SAFELY IMPROVE USEFUL COVERAGE for this frozen candidate. Product-readiness status: BLOCKED. Research artifacts and negative findings are complete.**

Keep this candidate rejected and the 24-case packet blind. Start a new DEV-only revision with complete engineering-unit token boundaries and source-role eligibility for heading/TOC/letterhead/table legends. Add meaningful generic DEV contracts for full unit names and narrative roles, using these findings as defect classes rather than tuning to project values. Assess retrieval/adjudication on independent DEV paired text for semantic rephrasing and split/merge; a local model may help but has not yet been tested.

Then freeze a new candidate, including any model/prompt/schemas/settings, before running validation. Obtain independent blind TextAlignment/TextChange annotations only after that freeze, with explicit class and split/merge coverage review. Do not consume this packet as tuning data. True added/removed facts need positive scope/absence evidence and cannot be supplied as labels by the assistant. No production deployment or push is authorized by this research result.

Unresolved scope: full semantic rewrite coverage, robust removal/addition certificates, event deduplication, source-role correctness and absent human alignment/change truth. The current 24 blind cases provide structurally diverse source opportunities, not guaranteed coverage of every requested semantic class.
""")
    # Add project evidence to the already registered comparison, preserving its
    # before-implementation plan and DEV selection rationale.
    approach_path=reports/"APPROACHES_COMPARED.md"
    prior=approach_path.read_text().split("\n## Post-freeze project comparison")[0]
    text="\n## Post-freeze project comparison\n\nAll approaches were frozen before these project observations; none were tuned afterwards.\n\n| Approach | Aligned relations | Engineering proposals | False structured changes (agent audit) | REVIEW units | Post-freeze runtime | Model calls / tokens | Complexity |\n|---|---:|---:|---:|---:|---|---|---|\n"
    for x in alt:
        # Exact source/field tuples establish that the same audited defect is
        # emitted by each alternative; no unseen proposal is assigned a label.
        canonical_props=lambda cs:{digest([c['pair_key'],c['old_source_refs'],c['new_source_refs'],c['fact_delta']]) for c in cs}
        selected_eng=[c for c in project['changes'] if c['category']=='ENGINEERING_CHANGE']
        if canonical_props(x['engineering_proposals'])!=canonical_props(selected_eng):raise ValueError('Alternative proposals need a separate source audit')
        text+=f"| {x['approach']} | {x['aligned_relations']} | 3 (one distinct confirmed event) | 1/3 | {x['review_units']} | {altperf[x['approach']]:.3f} s; no materialization | 0 / 0 | {'low; monotone sequence' if x['approach']=='sequence' else 'medium; broad property postings'} |\n"
    text+=f"| hybrid | 1377 | 3 (one distinct confirmed event) | 1/3 | 16004 | {perf['seconds']:.3f} s end-to-end | 0 / 0 | medium; lexical/template indices and 1–3-unit variants |\n"
    text+="\nAll three miss automatic engineering output for all four post-freeze historical diagnostic classes (specific load, fan-coil pipe property, area, new cooling demand). DEV construction precision was 100% for accepted cases, but natural project human precision is unknown; all 3 actual engineering proposals are the same source/field tuples across approaches, independently traceable to the shared audit. Their observed structured-fact precision is 2/3, with duplicate confirmed power statements.\n\nHybrid won the DEV alignment comparison; its project advantage over sequence is small and does not improve distinct engineering-event usefulness. All approaches are rejected for readiness because their shared fact layer emits a false structured change, and source-role safety fails. The experiment does not establish that a local semantic model would solve these issues.\n"
    approach_path.write_text(prior+text)
    write(reports/"FINAL_STATE.json",{"verdict":"C","product_readiness":"BLOCKED","research_complete":True,
                                      "candidate_code_unchanged":True,"production_modified":False,"push":0,"deploy":0,
                                      "all_required_reports_written":True,"human_truth_modified":False,"fresh_holdout_annotated":False})
    print({"verdict":"C","reports":len(list(reports.iterdir())),"engineering":audit['engineering_counts'],"alignment":audit['alignment_counts']},flush=True)

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",required=True);finish(p.parse_args().root)
