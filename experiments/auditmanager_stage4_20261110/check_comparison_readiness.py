"""Revalidate staged six-project sources; separate old status from current proof."""
import hashlib
import json
from pathlib import Path

OUT=Path(__file__).parent


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    package=json.loads((OUT/'six_project_input_package.json').read_text())
    rows=[]
    for project in package['projects']:
        root=Path(project['root']);failures=[]
        for name,digest in project['files'].items():
            path=root/name
            if not path.is_file() or path.is_symlink() or sha(path)!=digest:failures.append(name)
        graph_path=root/'03_analysis/runs/candidate/document_graph.json'
        graph=json.loads(graph_path.read_text())
        md_sha=sha(root/'02_work/document.md')
        if graph.get('source_md_sha256')!=md_sha:failures.append('document_graph_source_hash')
        actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
        extras=sorted(actual-set(project['files'])-{'03_analysis/runs/candidate/document_graph.json'})
        rows.append({'discipline':project['discipline'],'root':str(root),
            'frozen_source_files_checked':len(project['files']),'source_failures':failures,
            'additional_files_requiring_inspection':extras,
            'graph_sha256':sha(graph_path),'source_md_sha256':md_sha,
            'text_blocks':graph.get('total_text_blocks'),'graphic_blocks':graph.get('total_image_blocks'),
            'runner_project_metadata_present':(root/'01_input/project_info.json').exists()})
    report={'scope':'Current local input readiness, not candidate freeze or authorization certificate.',
        'projects':rows,'source_files_verified':all(not r['source_failures'] for r in rows),
        'additional_files_absent':all(not r['additional_files_requiring_inspection'] for r in rows),
        'document_graphs_present_and_bound':all('document_graph_source_hash' not in r['source_failures'] for r in rows),
        'authorization_state':'Standing direct Codex CLI authorization is recorded in AGENTS.md. OpenRouter remains per-request only. Old package execution_authorized=false is historical packaging state, not a pending user question.',
        'remaining_execution_gates':['Finish original stage4 controls and freeze selected implementation and prompts.',
            'Prepare actual paired classic/candidate runners with identical Astra topology, effort, inputs and scopes; historical mixed-model control cannot establish method effect.',
            'Prepare source-only runner metadata and verify provider restrictions and answer isolation.',
            'Freeze evaluator with documented errata holds before scoring; no answers provided to search executors.',
            'Record processes, request provenance, retries, usage and output hashes for all six projects; evaluate new-project transfer separately.'],
        'full_six_project_run_started':False,'new_model_calls':0,'stage4_complete':False}
    (OUT/'comparison_readiness_current.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'projects':len(rows),'files':sum(r['frozen_source_files_checked'] for r in rows),
        'source_files_verified':report['source_files_verified'],'extra_files':sum(len(r['additional_files_requiring_inspection']) for r in rows)},ensure_ascii=False))


if __name__=='__main__':main()
