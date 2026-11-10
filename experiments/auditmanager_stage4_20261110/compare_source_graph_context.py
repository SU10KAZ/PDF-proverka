"""Check comparable graphic context, keeping excluded stamp nodes explicit."""
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def pairs(graph, allowed):
    return {(image, item['text_block_id']) for page in graph['pages']
            for image, values in page.get('local_text_links', {}).items()
            if image in allowed for item in values}


if __name__ == '__main__':
    here = Path(__file__).resolve().parent
    current = json.loads((here / 'six_project_sheet_metadata.json').read_text())
    baseline_root = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')
    baseline = json.loads((baseline_root / 'input_manifest.json').read_text())
    rows = []
    for entry in current['graphs']:
        original = next(p for p in baseline if p['discipline'] == entry['discipline'])
        before_path = Path(original['output_dir']) / 'document_graph.json'
        after_path = Path(entry['path'])
        if sha(after_path) != entry['output_sha256']:
            raise ValueError('Candidate graph changed')
        before, after = json.loads(before_path.read_text()), json.loads(after_path.read_text())
        graphic_ids = {b['id'] for p in after['pages'] for b in p['image_blocks']}
        baseline_image_ids = {b['id'] for p in before['pages'] for b in p['image_blocks']}
        excluded = baseline_image_ids - graphic_ids
        if excluded != set(after['geometry_only_stamp_ids']):
            raise ValueError('Excluded image nodes are not exactly the source stamp nodes')
        old_pairs, new_pairs = pairs(before, graphic_ids), pairs(after, graphic_ids)
        rows.append({'discipline': entry['discipline'], 'graphic_blocks': len(graphic_ids),
                     'unchanged_links': len(old_pairs & new_pairs),
                     'lost_links': sorted(old_pairs-new_pairs), 'added_links': sorted(new_pairs-old_pairs),
                     'excluded_geometry_stamp_nodes': len(excluded),
                     'sources': [{'path': str(p), 'sha256': sha(p)} for p in (before_path, after_path)]})
    result = {'projects': rows, 'all_graphic_context_links_preserved': all(not r['lost_links'] and not r['added_links'] for r in rows),
              'new_model_calls': 0, 'boundary': 'Compares geometric image-to-text link identities only. Does not certify semantic relevance, full prompt equivalence, output quality or model discovery.'}
    (here / 'six_project_graph_context_comparison.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(result['all_graphic_context_links_preserved'], sum(r['unchanged_links'] for r in rows), 'links')
