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
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request


def main():
    root = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')
    projects = json.loads((root/'input_manifest.json').read_text())
    reports = []
    preparation = []
    prepared_root = Path(tempfile.mkdtemp(prefix='auditmanager_known_source_reviews_'))
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
        attach_graphic_receipts(queue, md.decode('utf-8'), source/'blocks_stage02_100')
        project_requests = []
        destination = prepared_root/project['discipline']
        destination.mkdir()
        for packet in queue['packets']:
            request = prepare_source_review_request(queue, current[packet['finding_id']], md.decode('utf-8'))
            target = destination/(packet['finding_id']+'.json')
            target.write_text(json.dumps(request, ensure_ascii=False, indent=2)+'\n')
            payload = json.loads(request['messages'][1]['content'])
            project_requests.append({'finding_id': packet['finding_id'], 'path': str(target),
                                     'request_sha256': request['request_sha256'],
                                     'message_chars': sum(len(m['content']) for m in request['messages']),
                                     'images': len(request['image_paths']),
                                     'omitted_images': payload['unavailable_images'],
                                     'budgeted_text_blocks': payload['budgeted_text_block_ids'],
                                     'missing_block_ids': payload['missing_block_ids'], 'sent': False})
        preparation.append({'discipline': project['discipline'], 'requests': project_requests,
                            'total_message_chars': sum(q['message_chars'] for q in project_requests),
                            'largest_request_chars': max((q['message_chars'] for q in project_requests), default=0),
                            'image_attachments': sum(q['images'] for q in project_requests)})
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
        for packet, summary in zip(queue['packets'], packets):
            summary['reference_origins'] = packet.get('reference_origins', {})
            summary['required_graphic_block_ids'] = packet['required_graphic_block_ids']
            summary['missing_or_ambiguous_block_ids'] = packet['missing_or_ambiguous_block_ids']
            summary['graphic_files'] = []
            for bid in packet['required_graphic_block_ids']:
                path = source/'blocks_stage02_100'/f'block_{bid}.png'
                summary['graphic_files'].append({'block_id': bid, 'path': str(path), 'exists': path.is_file(),
                                                'sha256': hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None,
                                                'attached_to_model': False, 'visually_reviewed_in_this_replay': False})
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
              'boundary': 'Saved-output replay after provenance recovery, not new audit. Queue covers pending explicit candidate lineage; other findings are not thereby semantically verified.'}
    Path(__file__).with_name('six_source_review_queue_replay.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    preflight = {'prepared_root': str(prepared_root), 'projects': preparation,
                 'total_requests': sum(len(p['requests']) for p in preparation),
                 'total_message_chars': sum(p['total_message_chars'] for p in preparation),
                 'total_image_attachments': sum(p['image_attachments'] for p in preparation),
                 'requests_with_omitted_inputs': sum(bool(q['omitted_images'] or q['budgeted_text_blocks'] or q['missing_block_ids'])
                                                     for p in preparation for q in p['requests']),
                 'automatic_dispatch': False, 'new_model_calls': 0,
                 'cost_and_duration': 'Unmeasured; character counts exclude image tokenization, output and reasoning. Pilot timing and quota measurement needed.',
                 'scope': 'Known saved-claim verification requests; not independent discovery or new whole-project audit.'}
    Path(__file__).with_name('six_source_review_requests_preflight.json').write_text(json.dumps(preflight, ensure_ascii=False, indent=2)+'\n')
    print('Prepared', preflight['total_requests'], 'requests, chars', preflight['total_message_chars'], 'images', preflight['total_image_attachments'])


if __name__ == '__main__':
    main()
