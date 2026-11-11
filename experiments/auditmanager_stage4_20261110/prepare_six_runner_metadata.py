"""Prepare minimal source-only runner metadata; no findings or evaluator inputs."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    package = json.loads((HERE/'six_project_input_package.json').read_text())
    records = []
    for project in package['projects']:
        root = Path(project['root'])
        for name, digest in project['files'].items():
            assert sha(root/name) == digest, (project['discipline'], name)
        discipline = project['discipline']
        data = {'project_id': 'method-comparison-'+discipline, 'name': discipline,
                'section': discipline, 'description': '', 'md_file': '02_work/document.md',
                'pdf_file': '02_work/document.pdf', 'blocks_json_file': '02_work/blocks.json'}
        for key in ('md_file','pdf_file','blocks_json_file'):
            assert data[key] in project['files'] and (root/data[key]).is_file()
        path = root/'01_input/project_info.json';path.parent.mkdir(exist_ok=True)
        if path.exists():
            assert json.loads(path.read_text()) == data, 'Refusing to overwrite different metadata'
        else:
            with path.open('x') as f:
                f.write(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
        records.append({'discipline':discipline,'path':str(path),'sha256':sha(path),'metadata':data})
    report = {'scope':'Runner plumbing only; no comparison freeze or execution.', 'projects':records,
              'metadata_source':'Fixed discipline and manifest-verified source paths only.',
              'engineer_answers_or_prior_findings_used':False,'new_model_calls':0}
    (HERE/'six_runner_metadata_manifest.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print('Prepared source-only metadata for',len(records),'projects')

if __name__=='__main__':
    main()
