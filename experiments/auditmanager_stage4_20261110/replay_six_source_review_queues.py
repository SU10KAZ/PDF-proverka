"""Measure pending source reviews across saved classic outputs, locally only."""
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile

from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.prepare.graph_builder import build_block_page_index


def main():
    root = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')
    projects = json.loads((root/'input_manifest.json').read_text())
    reports = []
    for project in projects:
        source = Path(project['output_dir'])
        md_path = Path(project['version_dir'])/'02_work/document.md'
        md = md_path.read_bytes()
        assert hashlib.sha256(md).hexdigest() == project['source_md_sha256']
        original = json.loads((source/'03_findings.json').read_text())
        candidate = deepcopy(original)
        graph = json.loads((source/'document_graph.json').read_text())
        block_analysis = json.loads((source/'01_blocks_analysis.json').read_text())
        known = set(build_block_page_index(graph)) | {b['block_id'] for b in block_analysis['block_analyses'] if b.get('block_id')}
        text_candidate_lineage(candidate['findings'], json.loads((source/'02_text_analysis.json').read_text()), known)
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out/'02_work').mkdir()
            (out/'02_work/document.md').write_bytes(md)
            for name in ('document_graph.json', '01_blocks_analysis.json', '02_text_analysis.json'):
                (out/name).write_bytes((source/name).read_bytes())
            (out/'03_findings.json').write_text(json.dumps(candidate, ensure_ascii=False))
            stats = _run_deterministic_phase(out, project['discipline']+'-source-queue')
            assert not any(stats[k] for k in ('critic_error', 'corrector_error', 'source_review_queue_error'))
            after = json.loads((out/'03_findings.json').read_text())
            queue = json.loads((out/'03_source_review_queue.json').read_text())
        before = {f['id']: f for f in original['findings']}
        current = {f['id']: f for f in after['findings']}
        assert len(before) == len(original['findings']) and len(current) == len(after['findings'])
        assert set(before) == set(current)
        changes = [{'finding_id': fid, 'field': key, 'before': before[fid].get(key), 'after': current[fid].get(key)}
                   for fid in before for key in ('problem', 'description', 'solution', 'severity')
                   if before[fid].get(key) != current[fid].get(key)]
        packets = [{'finding_id': p['finding_id'], 'finding_sha256': p['finding_sha256'],
                    'status': p['status'], 'source_context_status': p.get('source_context_status'),
                    'supplied_text_block_ids': [s['block_id'] for s in p['sources']],
                    'unresolved_block_ids': p['unresolved_block_ids'], 'budgeted_block_ids': p['budgeted_block_ids']}
                   for p in queue['packets']]
        pending = {f['id'] for f in current.values() if f.get('source_evidence_review_status') == 'pending'}
        assert pending == {p['finding_id'] for p in packets}
        lineage_requires_review = {f['id'] for f in current.values()
                                   if any(r.get('status') == 'requires_semantic_review'
                                          for r in f.get('source_candidate_lineage', []))}
        reports.append({'discipline': project['discipline'], 'findings': len(before), 'pending': len(packets),
                        'context_counts': dict(Counter(p.get('source_context_status', 'not_ready') for p in packets)),
                        'core_field_changes': changes, 'packets': packets, 'stats': stats,
                        'lineage_requires_review_count': len(lineage_requires_review),
                        'lineage_not_queued_ids': sorted(lineage_requires_review - pending),
                        'source_md_sha256': queue['source_md_sha256'],
                        'saved_findings_sha256': hashlib.sha256((source/'03_findings.json').read_bytes()).hexdigest()})
        print(project['discipline'], 'findings', len(before), 'pending', len(packets), 'core changes', len(changes))
    result = {'projects': reports, 'total_findings': sum(r['findings'] for r in reports),
              'pending_total': sum(r['pending'] for r in reports),
              'lineage_not_queued_total': sum(len(r['lineage_not_queued_ids']) for r in reports),
              'new_model_calls': 0, 'semantic_reviews_completed': 0,
              'boundary': 'Saved-output replay after provenance recovery, not new audit. Queue covers only findings marked pending by the deterministic critic; other findings are not thereby semantically verified.'}
    Path(__file__).with_name('six_source_review_queue_replay.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')


if __name__ == '__main__':
    main()
