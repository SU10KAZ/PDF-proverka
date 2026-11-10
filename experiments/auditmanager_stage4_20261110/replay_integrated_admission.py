"""Real saved responses + explicit semantic decisions through final admission/Excel.

Known-case replay, no new discovery. Absence-model phase is replaced with a
no-op in this local experiment; no external calls. Structural verifier is real.
"""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch,AsyncMock
from openpyxl import Workbook,load_workbook
from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet,PROJ_COLUMNS

HERE=Path(__file__).parent
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')


def entries():
    queue=json.loads((HERE/'EOM_source_review_queue.json').read_text())
    reviews=json.loads((HERE/'EOM_astra_pilot_result.json').read_text())['json_data']['reviews']
    result=[]
    for response in reviews:
        fid=response['finding_id'];is_false=fid=='F-040';rows=[]
        for c in response['claims']:
            basis=(c['claim_id'] in ('C1','C3','C5','C7')) if is_false else c['claim_id'] in ('C1','C2','C3','C4','C5')
            rows.append({'claim_id':c['claim_id'],'role':'defect_basis' if basis else 'context',
                'verdict':c['status'],'same_scope_verified':True,'evidence_sufficiency_verified':True,
                'reason':'Explicit source review and complete-row arithmetic recorded in EOM_astra_pilot_assessment.json; hypotheses remain needs_data.'})
        d={'response_sha256':evidence_digest(response),'origin':'explicit_source_review','reviewer':'known_case_manual_source_review',
           'reason':'EOM_astra_pilot_assessment.json; original tables and full row arithmetic checked independently.',
           'action':'suppress' if is_false else 'revise','claim_reviews':rows}
        if not is_false:
            d.update(final_wording_verified=True,approved_fields={
                'problem':'Сумма опубликованных мощностей РП5 отличается от итогов таблицы',
                'description':'По 24 строкам потребителей сумма Py составляет 94,5 кВт при итоге 94,10 кВт. Сумма Pp составляет 89,9 кВт при указанном значении 89,50 кВт. Оба расхождения — 0,4 кВт. Причина расхождения и одновременность работы в пожарном режиме по этому фрагменту не установлены.',
                'solution':'Проверить формулы итогов РП5 и принятый состав нагрузок для каждого режима; согласовать таблицу с расчётом.'})
        result.append({'queue':queue,'response':response,'decision':d})
    return result


async def main():
    destination=Path(tempfile.mkdtemp(prefix='auditmanager_integrated_admission_'));reports=[]
    inbox=entries()
    for p in json.loads((BASE/'input_manifest.json').read_text()):
        disc=p['discipline'];source=Path(p['output_dir']);out=destination/disc;out.mkdir()
        md=Path(p['version_dir'])/'02_work/document.md';(out/'02_work').mkdir();(out/'02_work/document.md').write_bytes(md.read_bytes())
        for name in ('01_blocks_analysis.json','document_graph.json','02_text_analysis.json'):(out/name).write_bytes((source/name).read_bytes())
        data=json.loads((source/'03_findings.json').read_text());baseline=deepcopy(data)
        if disc=='EOM':
            snapshots={p['finding_id']:p['finding_snapshot'] for p in inbox[0]['queue']['packets']}
            data['findings']=[deepcopy(snapshots[f['id']]) if f['id'] in ('F-003','F-040') else f for f in data['findings']]
            (out/'03_source_review_admissions.json').write_text(json.dumps(inbox,ensure_ascii=False))
        if disc=='ITP':
            initial=Path('/tmp/auditmanager_itp_initial_context_astra_20261110')
            queue=json.loads((initial/'queue.json').read_text())
            finding=json.loads((initial/'finding.json').read_text())
            request=json.loads((initial/'request.json').read_text())
            sent={im['block_id']:im['sha256'] for im in request['image_bindings']}
            for receipt in queue['packets'][0]['graphic_attachments']:
                receipt['attached_to_model']=sent.get(receipt['block_id'])==receipt['sha256']
            response=json.loads((HERE/'ITP_initial_context_result.json').read_text())['json_data']
            data['findings']=[finding if f['id']=='F-042' else f for f in data['findings']]
            (out/'blocks_stage02_100').symlink_to(source/'blocks_stage02_100',target_is_directory=True)
            (out/'03_source_review_responses.json').write_text(json.dumps([{'queue':queue,'response':response}],ensure_ascii=False))
        (out/'03_findings.json').write_text(json.dumps(data,ensure_ascii=False))
        ctx=SimpleNamespace(project_id=disc,output_dir=out,project_info={},is_cancelled=None,log=AsyncMock(),update_pipeline_log=lambda *a,**kw:None)
        with patch('backend.app.pipeline.stages.findings_verify.runner._run_absence_phase',return_value={'candidates':0,'verified':False}):
            result=await run_findings_verify(ctx)
        final=json.loads((out/'03_findings.json').read_text());current={f['id']:f for f in final['findings']}
        if disc=='EOM':
            print('ADMISSION', (out/'03_source_review_admission_results.json').read_text())
            assert result.source_review_applied==2 and result.source_review_not_applied==0
            assert 'F-003' in current and 'F-040' not in current
            assert final['suppressed_findings'][0]['id']=='F-040' and len(final['source_review_history'])==2
        else:assert result.source_review_applied==0
        if disc=='ITP':
            assert result.source_followups_prepared==1, result.error
            followups=json.loads((out/'03_source_review_followups.json').read_text())
            assert followups[0]['request']['sent'] is False
        for f in baseline['findings']:
            if disc=='EOM' and f['id'] in ('F-003','F-040'):continue
            for k in ('problem','description','solution','severity'):assert current[f['id']].get(k)==f.get(k),(disc,f['id'],k)
        wb=Workbook();build_project_sheet(wb,{'project_id':disc,'sheet_name':disc,'findings_json':final});file=out/'report.xlsx';wb.save(file);wb.close()
        saved=load_workbook(file);col=next(i for i,(k,*_) in enumerate(PROJ_COLUMNS) if k=='finding_id');ids=[r[col] for r in saved[disc].values if r[col] in current];saved.close()
        assert len(ids)==len(current) and set(ids)==set(current)
        reports.append({'discipline':disc,'before':len(baseline['findings']),'after':len(current),'admissions_applied':result.source_review_applied,'followups_prepared':result.source_followups_prepared,'other_core_fields_preserved':True,'excel_path':str(file)})
        print(disc,len(current),'exported')
    report={'projects':reports,'known_case_replay':True,'automatic_semantic_decisions':False,'new_model_calls':0,
        'absence_model_phase':'Explicit no-op for local replay; structural and final admission phases real.',
        'result':'One false finding archived, one supported core revised, remaining242 findings core fields preserved through integrated verifier and Excel.'}
    (HERE/'integrated_admission_replay.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')


if __name__=='__main__':asyncio.run(main())
