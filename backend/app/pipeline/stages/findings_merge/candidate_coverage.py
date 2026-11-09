"""Account for merge input candidates without equating ID links with preservation."""
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME, TEXT_ANALYSIS_FILENAME, resolve_existing,
)


def build_candidate_coverage(text, blocks, final):
    candidates = []
    for i, item in enumerate(text.get('text_findings', [])):
        candidates.append({'origin': 'text', 'pointer': f'/text_findings/{i}', 'candidate': item})
    for i, block in enumerate(blocks.get('block_analyses', [])):
        for field in ('findings', 'deferred_findings'):
            for j, item in enumerate(block.get(field, [])):
                candidates.append({'origin': 'graphic', 'block_id': block.get('block_id'),
                                   'pointer': f'/block_analyses/{i}/{field}/{j}',
                                   'deferred': field == 'deferred_findings', 'candidate': item})
    counts = Counter(c['candidate'].get('id') for c in candidates
                     if isinstance(c['candidate'], dict) and isinstance(c['candidate'].get('id'), str))
    links = defaultdict(list)
    for i, finding in enumerate(final.get('findings', [])):
        refs = finding.get('source_finding_ids', [])
        for sid in set(r for r in refs if isinstance(r, str)) if isinstance(refs, list) else []:
            links[sid].append({'id': finding.get('id'), 'pointer': f'/findings/{i}'})
    for row in candidates:
        item = row['candidate']
        sid = item.get('id') if isinstance(item, dict) else None
        valid = isinstance(sid, str) and bool(sid.strip())
        row['source_id'] = sid
        row['linked_findings'] = links.get(sid, []) if valid else []
        row['semantic_preservation_verified'] = False
        if not valid or counts[sid] != 1:
            row['status'] = 'ambiguous_source_identity'
        elif row.get('deferred'):
            row['status'] = 'deferred_requires_context_review'
        elif row['linked_findings']:
            row['status'] = 'linked_requires_semantic_review'
        else:
            row['status'] = 'unlinked_requires_trace'
    return {'candidates': candidates, 'counts': dict(Counter(r['status'] for r in candidates)),
            'candidate_count': len(candidates),
            'unresolved_final_source_ids': sorted(sid for sid in links if sid not in counts),
            'preservation_verified': False,
            'boundary': 'Missing links are not proof of loss; links are not proof of preserved meaning. '
                        'Deferred claims require context review, not automatic publication.'}


def write_candidate_coverage(output_dir):
    """Write a hash-bound post-merge snapshot; never mutate input findings."""
    output_dir = Path(output_dir)
    paths = [resolve_existing(output_dir, TEXT_ANALYSIS_FILENAME),
             resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME),
             output_dir / '03_findings.json']
    payloads, sources, missing = [], [], []
    for path in paths:
        if path is None or not path.exists():
            missing.append(str(path))
            payloads.append({})
            continue
        raw = path.read_bytes()
        payloads.append(json.loads(raw))
        sources.append({'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()})
    report = build_candidate_coverage(*payloads)
    report.update(sources=sources, missing_inputs=missing, input_inventory_complete=not missing)
    (output_dir / '03_candidate_coverage.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return report
