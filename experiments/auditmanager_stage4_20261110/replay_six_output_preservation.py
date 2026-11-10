"""Saved-output verifier-to-Excel regression; no model discovery/acceptance claim."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage
from backend.app.pipeline.stages.findings_review.deterministic_critic import build_index
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS


def main():
    root=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')
    reports=[]
    for project in json.loads((root/'input_manifest.json').read_text()):
        source=Path(project['output_dir']); md=Path(project['version_dir'])/'02_work/document.md'
        assert hashlib.sha256(md.read_bytes()).hexdigest()==project['source_md_sha256']
        original=json.loads((source/'03_findings.json').read_text()); candidate=deepcopy(original)
        index=build_index(json.loads((source/'01_blocks_analysis.json').read_text()),json.loads((source/'document_graph.json').read_text()))
        text_candidate_lineage(candidate['findings'],json.loads((source/'02_text_analysis.json').read_text()),index.image_block_ids | index.text_block_ids)
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory); (out/'02_work').mkdir(); (out/'02_work/document.md').write_bytes(md.read_bytes())
            for name in ('01_blocks_analysis.json','document_graph.json','02_text_analysis.json'):
                (out/name).write_bytes((source/name).read_bytes())
            (out/'03_findings.json').write_text(json.dumps(candidate,ensure_ascii=False))
            stats=_run_deterministic_phase(out,project['discipline']+'-output-preservation')
            assert not any(stats[k] for k in ('critic_error','corrector_error','source_review_queue_error'))
            after=json.loads((out/'03_findings.json').read_text())
            before={f['id']:f for f in original['findings']}; current={f['id']:f for f in after['findings']}
            assert len(before)==len(original['findings']) and set(before)==set(current)
            for fid in before:
                for key in ('problem','description','solution','severity'):
                    assert before[fid].get(key)==current[fid].get(key),(project['discipline'],fid,key)
            wb=Workbook(); build_project_sheet(wb,{'project_id':project['discipline'],'sheet_name':project['discipline'],'findings_json':after})
            path=out/'report.xlsx'; wb.save(path); wb.close()
            saved=load_workbook(path); rows=list(saved[project['discipline']].values)
            col=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
            ids=[row[col] for row in rows if row[col] in current]
            assert len(ids)==len(current) and set(ids)==set(current)
            question_positions=[i for i,row in enumerate(rows) if 'Вопросы и уточнения' in row]
            question_ids={f['id'] for f in current.values() if f.get('report_section')=='questions'}
            if question_ids:
                assert question_positions
                assert all(i>question_positions[0] for i,row in enumerate(rows) if row[col] in question_ids)
            saved.close()
            reports.append({'discipline':project['discipline'],'findings_preserved':len(current),
                'excel_ids_unique_and_complete':True,'questions_exported_separately':len(question_ids),
                'pending_source_reviews':stats['source_reviews_pending'], 'core_fields_preserved':True,
                'source_md_sha256':project['source_md_sha256']})
            print(project['discipline'],len(current),'preserved through Excel; pending',stats['source_reviews_pending'])
    report={'projects':reports,'findings_preserved':sum(p['findings_preserved'] for p in reports),
        'pending_source_reviews':sum(p['pending_source_reviews'] for p in reports),'new_model_calls':0,
        'independent_discovery':False,'semantic_acceptance':False,
        'scope':'All saved findings including useful and potentially false ones preserved; this tests transport, not semantic quality. No absence-model phase or new whole-project run.'}
    Path(__file__).with_name('six_project_output_preservation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')


if __name__=='__main__':main()
