"""Apply explicitly reviewed known-case decisions to experiment copies only.

This is an output-path control, not an automatic semantic filter, tuning label
for blind runs, or proof of independent discovery. Baseline files are read-only.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from openpyxl import Workbook, load_workbook
from backend.app.pipeline.stages.findings_verify.runner import _run_deterministic_phase
from backend.app.pipeline.stages.report.generate_excel_report import build_project_sheet, PROJ_COLUMNS

HERE=Path(__file__).parent
BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control')


def main():
    output=Path(tempfile.mkdtemp(prefix='auditmanager_known_semantic_output_'))
    eom=json.loads((HERE/'EOM_astra_pilot_assessment.json').read_text())
    itp=json.loads((HERE/'ITP_astra_graphic_pilot_assessment.json').read_text())
    assert eom['control_pair_passed'] and itp['semantic_outcome']=='partial_pass'
    reports=[]
    for project in json.loads((BASE/'input_manifest.json').read_text()):
        disc=project['discipline']; source=Path(project['output_dir']); out=output/disc;out.mkdir()
        md=Path(project['version_dir'])/'02_work/document.md'
        assert hashlib.sha256(md.read_bytes()).hexdigest()==project['source_md_sha256']
        original=json.loads((source/'03_findings.json').read_text()); data=deepcopy(original)
        before={f['id']:f for f in original['findings']}; decisions=[]
        if disc=='EOM':
            removed=next(f for f in data['findings'] if f['id']=='F-040')
            data['findings']=[f for f in data['findings'] if f['id']!='F-040']
            decisions.append({'finding_id':'F-040','action':'suppress_in_experiment_output','original_finding':removed,
                'reason':'Complete published 89-row inventories independently sum to 1636.5 in both tables; alleged 1622.7 sum is wrong.',
                'control':'EOM_astra_pilot_assessment.json','decision_origin':'explicit known-case manual source review'})
            f=next(f for f in data['findings'] if f['id']=='F-003')
            f.update(problem='Сумма опубликованных мощностей РП5 отличается от итогов таблицы',
                description='По 24 строкам потребителей сумма Py составляет 94,5 кВт при итоге 94,10 кВт. Сумма Pp составляет 89,9 кВт при указанном значении 89,50 кВт. Оба расхождения — 0,4 кВт. Совпадение с мощностью клапанов не доказывает их пропуск; одновременность работы в пожарном режиме по этому фрагменту не установлена.',
                solution='Проверить формулы итогов РП5 и принятый состав нагрузок для каждого режима; согласовать таблицу с расчётом.')
            decisions.append({'finding_id':'F-003','action':'revise_in_experiment_output','control':'EOM_astra_pilot_assessment.json',
                'reason':'Preserve verified arithmetic core; separate unproven cause and operating-mode conclusion.',
                'decision_origin':'explicit known-case manual source review'})
        if disc=='ITP':
            f=next(f for f in data['findings'] if f['id']=='F-042')
            f.update(description='Подписи на чертежах указывают К22 — 1000 л и К23 — 100 л. Экспликация, спецификация и документы подбора задают К22 — два бака по 1500 л, К23 — один бак 200 л. На плане также присутствуют марки RM 1500 PN10 и RM 200 PN10. Подтверждено противоречие подписей; неправильный подбор или фактическая поставка оборудования этим не доказаны.',
                solution='Согласовать подписи объёмов К22 и К23 на чертежах со спецификацией и документами подбора. Не назначать замену оборудования только на основании этих подписей.')
            decisions.append({'finding_id':'F-042','action':'revise_in_experiment_output','control':'ITP_astra_graphic_pilot_assessment.json',
                'reason':'Keep supported volume-annotation conflicts; do not infer physical undersizing or quantity mismatch.',
                'decision_origin':'explicit known-case manual source review'})
        changed={d['finding_id'] for d in decisions}
        for d in decisions:
            d['original_finding_sha256']=hashlib.sha256(json.dumps(before[d['finding_id']],ensure_ascii=False,sort_keys=True).encode()).hexdigest()
            d['source_md_sha256']=project['source_md_sha256']
        (out/'decisions.json').write_text(json.dumps(decisions,ensure_ascii=False,indent=2)+'\n')
        (out/'02_work').mkdir(); (out/'02_work/document.md').write_bytes(md.read_bytes())
        for name in ('01_blocks_analysis.json','document_graph.json','02_text_analysis.json'):
            (out/name).write_bytes((source/name).read_bytes())
        (out/'03_findings.json').write_text(json.dumps(data,ensure_ascii=False,indent=2))
        stats=_run_deterministic_phase(out,disc+'-known-semantic-output')
        assert not any(stats[k] for k in ('critic_error','corrector_error','source_review_queue_error'))
        final=json.loads((out/'03_findings.json').read_text()); current={f['id']:f for f in final['findings']}
        expected={f['id']:f for f in data['findings']}
        assert set(current)==set(expected)
        for fid in current:
            for key in ('problem','description','solution','severity'):
                assert current[fid].get(key)==expected[fid].get(key),(disc,fid,key)
                if fid not in changed: assert current[fid].get(key)==before[fid].get(key)
        wb=Workbook();build_project_sheet(wb,{'project_id':disc,'sheet_name':disc,'findings_json':final})
        workbook=out/'report.xlsx';wb.save(workbook);wb.close();saved=load_workbook(workbook)
        col=next(i for i,(key,*_) in enumerate(PROJ_COLUMNS) if key=='finding_id')
        rows=list(saved[disc].values); ids=[row[col] for row in rows if row[col] in before]
        assert len(ids)==len(current) and set(ids)==set(current)
        if disc=='EOM': assert 'F-003' in ids and 'F-040' not in ids
        if disc=='ITP': assert 'F-042' in ids
        saved.close()
        reports.append({'discipline':disc,'baseline_findings':len(before),'output_findings':len(current),
            'decision_ids':sorted(changed),'all_other_core_fields_preserved':True,'excel_ids_verified':True,
            'workbook_path':str(workbook),'decisions_path':str(out/'decisions.json'),
            'findings_sha256':hashlib.sha256((out/'03_findings.json').read_bytes()).hexdigest(),
            'decision_log_sha256':hashlib.sha256((out/'decisions.json').read_bytes()).hexdigest()})
        print(disc,len(before),'->',len(current),'Excel verified')
    report={'scope':'Known-case manually reviewed decisions through actual deterministic verification and Excel; no automatic semantic decision proof.',
        'new_model_calls':0,'production_changed':False,'independent_discovery':False,
        'baseline_findings':sum(p['baseline_findings'] for p in reports),
        'output_findings':sum(p['output_findings'] for p in reports),'projects':reports,
        'remaining':'Generic semantic acceptance/application is not integrated; use this as expected downstream control, never as hidden history for a blind candidate audit.'}
    (HERE/'known_semantic_output_replay.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')


if __name__=='__main__':main()
