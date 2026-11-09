"""Read-only source selection replay; model calls are deliberately absent."""
import hashlib
import json
import tempfile
from pathlib import Path
from backend.app.pipeline.stages.prepare.candidate_recovery import build_recovery_passes
from backend.app.pipeline.stages.block_context.contract import resolve_blocks_dir

BASE = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs')

def main():
    results = []
    for disc in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
        version = next((BASE / disc).glob('versions/*'))
        source = version / '03_analysis/runs/control'
        md = version / '02_work/document.md'
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp)
            files = [source / n for n in ('01_blocks_analysis.json', '02_text_analysis.json', '03_findings.json', 'document_graph.json')]
            for path in files:
                (out / path.name).symlink_to(path)
            blocks = resolve_blocks_dir(source)
            (out / blocks.name).symlink_to(blocks)
            passes = build_recovery_passes(out, md)
            requests = []
            for request in passes:
                packet = json.loads(request.messages[-1]['content'])
                requests.append({'stage': request.stage, 'source_id': packet['candidate']['source_id'],
                    'selected_md_pages': packet['selected_md_pages'], 'md_chars': len(packet['authoritative_md']),
                    'missing_md_pages': packet['missing_or_budgeted_md_pages'],
                    'image_block_ids': packet['attached_image_block_ids_in_order'],
                    'image_count': len(request.image_paths),
                    'allowed_source_refs': packet['allowed_source_refs'],
                    'images': [{'path': str(p.resolve()), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in request.image_paths]})
            result = {'discipline': disc, 'requests': requests, 'request_count': len(requests),
                      'sources': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in [*files, md]]}
            results.append(result)
            print(disc, [(r['source_id'], r['selected_md_pages'], r['image_count']) for r in requests])
    target = Path(__file__).with_name('recovery_source_replay.json')
    target.write_text(json.dumps({'kind': 'local_source_selection_no_inference', 'projects': results,
                                 'quality_improvement_verified': False}, ensure_ascii=False, indent=2) + '\n')

if __name__ == '__main__':
    main()
