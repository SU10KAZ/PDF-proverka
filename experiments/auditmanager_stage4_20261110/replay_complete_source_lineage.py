"""Replay lineage with runtime's complete graphic-plus-document block index."""
import copy
import hashlib
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.prepare.graph_builder import build_block_page_index


if __name__ == '__main__':
    root = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')
    projects = json.loads((root/'input_manifest.json').read_text())
    results = []
    for project in projects:
        out = Path(project['output_dir'])
        files = {name: out/name for name in ('03_findings.json','02_text_analysis.json','01_blocks_analysis.json','document_graph.json')}
        data = {name: json.loads(path.read_text()) for name,path in files.items()}
        original = data['03_findings.json']['findings']
        findings = copy.deepcopy(original)
        known = set(build_block_page_index(data['document_graph.json'])) | {
            block['block_id'] for block in data['01_blocks_analysis.json']['block_analyses'] if block.get('block_id')}
        changed = text_candidate_lineage(findings,data['02_text_analysis.json'],known)
        assert text_candidate_lineage(findings,data['02_text_analysis.json'],known) == 0
        for before, after in zip(original,findings):
            assert {k:v for k,v in before.items() if k!='source_candidate_lineage'} == {
                k:v for k,v in after.items() if k!='source_candidate_lineage'}
        receipts = [{'finding_id': f['id'], 'lineage': f['source_candidate_lineage']}
                    for f in findings if f.get('source_candidate_lineage')]
        existing = sum(len(r.get('existing_block_ids',[])) for f in receipts for r in f['lineage'])
        unresolved = sum(len(r.get('unresolved_block_ids',[])) for f in receipts for r in f['lineage'])
        results.append({'discipline':project['discipline'],'findings_updated':changed,
                        'existing_reference_occurrences':existing,'unresolved_reference_occurrences':unresolved,
                        'receipts':receipts,'sources':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in files.values()]})
        print(project['discipline'],changed,'findings;',existing,'existing;',unresolved,'unresolved')
    result = {'supersedes_for_reference_existence':'source_lineage_replay.json',
              'reason':'Use runtime-equivalent full graph index plus block analyses. Earlier replay classified valid TEXT references as unresolved.',
              'projects':results,'original_finding_fields_unchanged':True,'idempotence_passed':True,
              'semantic_verification':False,'new_model_calls':0,
              'boundary':'A resolvable block address is provenance, not proof of the merged claim. Keep source_review_required until semantic review.'}
    Path(__file__).with_name('complete_source_lineage_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
