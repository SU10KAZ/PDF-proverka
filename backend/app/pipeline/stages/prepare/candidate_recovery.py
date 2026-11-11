"""Build source-backed recovery requests from this run, without historical answers."""
import hashlib
import json
import re
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage
from backend.app.pipeline.stages.findings_merge.subclaim_coverage import pending_subclaims
from backend.app.pipeline.stages.block_context.contract import resolve_blocks_dir
from backend.app.pipeline.stages.prepare.graph_builder import build_block_page_index


def build_recovery_passes(output_dir, md_path):
    from .codex_targeted_findings import CodexTargetedPass
    output_dir, md_path = Path(output_dir), Path(md_path)
    names = ('02_text_analysis.json', '01_blocks_analysis.json', '03_findings.json')
    if not all((output_dir / name).is_file() for name in names):
        return []
    payloads = [json.loads((output_dir / name).read_text()) for name in names]
    coverage = build_candidate_coverage(*payloads)
    graph_path = output_dir / 'document_graph.json'
    graph = json.loads(graph_path.read_text()) if graph_path.exists() else {}
    block_pages = build_block_page_index(graph)
    text_ids = {b.get('id') or b.get('block_id') for page in graph.get('pages', [])
                for b in page.get('text_blocks', []) if isinstance(b, dict)}
    blocks_dir = resolve_blocks_dir(output_dir)
    index_path = blocks_dir / 'index.json'
    index = json.loads(index_path.read_text()) if index_path.exists() else {}
    files = {b.get('block_id'): b.get('file') for b in index.get('blocks', []) if isinstance(b, dict)}
    md = md_path.read_text(encoding='utf-8')
    headers = list(re.finditer(r'^##[ \t]+(?:Page|СТРАНИЦА)[ \t]+(\d+)\b[^\n]*$', md, re.M | re.I))
    page_text = {int(h.group(1)): md[h.start():headers[i+1].start() if i+1 < len(headers) else len(md)]
                 for i, h in enumerate(headers)}
    # The prepared MD owns TEXT and its block addresses. Historical graph
    # snapshots can contain a different subset of IDs; never silently discard
    # existing MD blocks merely because that graph does not contain them.
    md_block_pages, md_text_ids = {}, set()
    for page, content in page_text.items():
        for match in re.finditer(r'^###\s+BLOCK[^\n]*?\[([^]]+)\]:\s*(\S+)', content, re.M | re.I):
            kind, bid = match.groups()
            md_block_pages[bid] = page
            if kind.upper() in {'TEXT', 'TABLE'}:
                md_text_ids.add(bid)
    block_pages.update(md_block_pages)
    text_ids = md_text_ids
    obligations = pending_subclaims(payloads[2], set(block_pages) | set(files), coverage)
    candidates = []
    for candidate in coverage['candidates']:
        pending = obligations.get((candidate['origin'], candidate['pointer']), [])
        if candidate['status'] == 'unlinked_requires_trace' or candidate.get('deferred') or pending:
            candidates.append({**candidate, 'pending_subclaims': pending})
    if not candidates:
        return []
    # Review candidates from the same graphic together so the model can
    # distinguish duplicate claims from independent defects before publication.
    groups = {}
    for c in candidates:
        key = ('graphic', c['block_id']) if c['origin'] == 'graphic' and c.get('block_id') else (c['origin'], c['pointer'])
        groups.setdefault(key, []).append(c)
    passes, manifest = [], []
    for group in groups.values():
        candidate = group[0]
        item = candidate['candidate']
        refs = [candidate.get('block_id')]
        for member in group:
            for obligation in member.get('pending_subclaims', []):
                claim = obligation.get('claim')
                values = claim.get('required_source_refs') if isinstance(claim, dict) else None
                if isinstance(values, list):
                    refs.extend(values)
            for field in ('related_block_ids', 'source_block_ids', 'source_block_hints'):
                values = member['candidate'].get(field)
                if isinstance(values, list):
                    refs.extend(values)
        refs = list(dict.fromkeys(r for r in refs if isinstance(r, str) and r))
        pages = sorted({block_pages[r] for r in refs if r in block_pages})
        selected_pages, skipped_pages, chunks = [], [], []
        size = 0
        for page in pages:
            chunk = page_text.get(page, '')
            if not chunk or size + len(chunk) > 100000:
                skipped_pages.append(page)
                continue
            chunks.append(chunk)
            selected_pages.append(page)
            size += len(chunk)
        if not pages and len(md) <= 100000:
            chunks = [md]
        images, image_refs, unavailable = [], [], []
        for ref in refs:
            name = files.get(ref)
            if not isinstance(name, str):
                continue
            path = (blocks_dir / name).resolve()
            if path.is_relative_to(blocks_dir.resolve()) and path.is_file() and len(images) < 8:
                images.append(path)
                image_refs.append(ref)
            else:
                unavailable.append(ref)
        key = hashlib.sha256((candidate['origin'] + candidate['pointer']).encode()).hexdigest()[:16]
        packet = {'candidate': candidate, 'candidate_group': group, 'existing_findings': payloads[2].get('findings', []),
                  'selected_md_pages': selected_pages, 'missing_or_budgeted_md_pages': skipped_pages,
                  'attached_image_block_ids_in_order': image_refs, 'unavailable_image_block_ids': unavailable,
                  'authoritative_md': '\n'.join(chunks), 'context_complete': False,
                  'crop_source': {'directory': str(blocks_dir.resolve()),
                                  'index_sha256': hashlib.sha256(index_path.read_bytes()).hexdigest() if index_path.exists() else None},
                  'md_source': {'path': str(md_path.resolve()), 'sha256': hashlib.sha256(md_path.read_bytes()).hexdigest()},
                  'allowed_source_refs': sorted(set(image_refs) | {r for r, page in block_pages.items() if page in selected_pages and r in text_ids})}
        instructions = '''Review the candidate_group against the supplied original sources.
Some candidates already have a linked finding but still have pending_subclaims.
Review those unresolved parts without repeating already supported content. A link
or retained decision for the parent does not close its unresolved parts.
Every decision must include subclaims, one per independently checkable claim, with
id (stable within that candidate), claim, decision, reason, source_refs,
required_source_refs for missing sources, and target_finding_ids for retained or
duplicate claims. Reuse pending claim IDs exactly. Omitted claims remain pending.
An outcome is only a model report, not independent certification of truth.
Candidates sharing a graphic are reviewed together: emit one finding per distinct
problem, preserving every supporting source_finding_id. Different defects in one
graphic must remain distinct. Return one decision for each candidate in the group.
Deferred candidates (deferred=true, stable D-ID or P-ID) were withheld for insufficient
context. Preserve their gate reasons and required_context; they are not confirmed
findings. Inspect the requested scope before deciding; if unavailable, return
needs_data with literal context_queries, not a defect or refutation.
Prescan candidates (origin=prescan, stable P-ID) are lexical triggers, not defects.
A designation change requires an evidenced identity conflict or applicable requirement;
co-occurring words or marks alone do not establish a need to rename equipment.
Earlier detector claims, summaries and ID links are not independent evidence.
MD TEXT is authoritative: never correct its recognition from images. Inspect attached
images for GRAPHIC claims. The context is bounded; missing context means needs_data,
not refuted. Compare with existing findings by meaning and location before adding.
Return JSON with findings (only supported, genuinely missing findings in Russian,
standard final finding fields and source_finding_ids, plus report_section set to
technical_findings for confirmed technical conflicts or questions for conditional,
minor editorial or out-of-profile clarifications), and candidate_decisions
(origin, pointer, decision retained/duplicate/refuted/needs_data/out_of_profile,
reason in Russian, source_refs exact inspected block IDs). For graphic claims without
adequate attached images return needs_data, not a confirmed finding. Include evidence
and exact locations in any finding. Every findings[] item MUST contain its own
nonempty evidence ARRAY of objects, each with block_id from allowed_source_refs
and an exact quote for TEXT or a concrete observation for an attached GRAPHIC.
Example shape only: "evidence": [{"block_id": "<inspected permitted block ID>",
"type": "text", "quote": "<exact source quote>", "location": "<source location>"}].
Replace placeholders with inspected source content; never invent evidence to meet
this shape. A plain evidence string, related_block_ids, or source_refs solely in
candidate_decisions does not satisfy finding-level evidence. For split findings,
assign only the evidence supporting that individual claim; do not copy the whole
candidate's reference set to every child. If support is unavailable, use needs_data.
Use only these severity values: КРИТИЧЕСКОЕ, ЭКОНОМИЧЕСКОЕ, ЭКСПЛУАТАЦИОННОЕ,
РЕКОМЕНДАТЕЛЬНОЕ, ПРОВЕРИТЬ ПО СМЕЖНЫМ. Questions use report_section=questions;
do not invent severity values such as РЕДАКЦИОННОЕ or УТОЧНЕНИЕ.
Do not invent norms. Preserve useful questions as questions; do not convert them
into technical defects. Do not omit a decision.
For needs_data, include context_queries: up to six literal object marks or short
phrases to search in this project's prepared MD. No regex or historical answer IDs.
Search may return TEXT/TABLE evidence or GRAPHIC navigation summaries; summaries
cannot replace an image. If the retrieved text remains insufficient, keep needs_data.
For duplicate decisions, provide target_finding_ids: the exact IDs of ALL existing
findings preserving the candidate atoms, and explain the mapping in reason. Never
add a duplicate finding. Do not infer a target ID from its numbering.
'''
        passes.append(CodexTargetedPass(stage=f'candidate_recovery_{key}',
            output_filename=f'03_candidate_recovery_{key}.json',
            messages=[{'role': 'system', 'content': instructions},
                      {'role': 'user', 'content': json.dumps(packet, ensure_ascii=False)}],
            image_paths=tuple(images)))
        manifest.append({k: v for k, v in packet.items() if k not in ('authoritative_md', 'existing_findings')})
    (output_dir / '03_candidate_recovery_plan.json').write_text(json.dumps(
        {'requests': manifest, 'count': len(passes), 'history_supplied': False}, ensure_ascii=False, indent=2))
    return passes


def validate_recovery_payload(targeted_pass, payload):
    """Require addressed evidence before admitting recovery output to ordinary review."""
    packet = json.loads(targeted_pass.messages[-1]['content'])
    candidate = packet['candidate']
    source_ids = {c['source_id'] for c in packet.get('candidate_group', [candidate])}
    accepted, pending = [], []
    items = payload.get('findings', [])
    for item in items if isinstance(items, list) else []:
        # Some providers return the explicit source_refs contract instead of
        # evidence. Adapt only supplied addresses; never synthesize quotations
        # or interpret address validity as factual verification.
        if isinstance(item, dict) and (
                not item.get('evidence') or isinstance(item.get('evidence'), str)):
            refs = item.get('source_refs')
            if (isinstance(refs, list) and refs
                    and all(isinstance(ref, str) and ref in packet['allowed_source_refs'] for ref in refs)):
                image_ids = set(packet['attached_image_block_ids_in_order'])
                original_evidence = item.get('evidence')
                item = {**item, 'evidence': [
                    {'block_id': ref, 'type': 'graphic' if ref in image_ids else 'text',
                     'origin': 'model_source_refs'} for ref in dict.fromkeys(refs)],
                    'recovery_evidence_adapted_from': 'source_refs'}
                if isinstance(original_evidence, str):
                    # Retain the model's explanation without inventing a
                    # per-source quotation or treating it as verified evidence.
                    item['recovery_original_evidence_text'] = original_evidence
        reason = None
        if not isinstance(item, dict):
            reason = 'invalid_finding'
        elif not isinstance(item.get('source_finding_ids'), list) or not any(sid in source_ids for sid in item['source_finding_ids'] if isinstance(sid, str)):
            reason = 'missing_candidate_link'
        elif candidate['origin'] == 'graphic' and not packet['attached_image_block_ids_in_order']:
            reason = 'graphic_source_unavailable'
        elif not isinstance(item.get('evidence'), list) or not any(
                isinstance(e, dict) and e.get('block_id') in packet['allowed_source_refs']
                for e in item['evidence']):
            reason = 'missing_inspected_source_reference'
        if reason:
            pending.append({'finding': item, 'reason': reason})
        else:
            accepted.append({**item, 'recovery_semantic_verified': False})
    from backend.app.pipeline.stages.findings_merge.recovery_links import prepare_duplicate_links
    links, unresolved_links = prepare_duplicate_links(packet, payload.get('candidate_decisions'))
    return {**payload, 'findings': accepted, 'recovery_pending': pending + unresolved_links,
            'recovery_link_updates': links}
