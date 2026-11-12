"""Prepare independently reviewed minor-symbol wording; no automatic admission."""
import argparse
import json
from pathlib import Path
from .prepare_publication_f040_admission import prepare as prepare_base
from .run_primary_text_runtime_probe import sha, write
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check


def prepare(root):
    manifest=prepare_base(root,
        Path('/tmp/auditmanager_heater_source_reading_20261112/request.json'),
        Path('/tmp/auditmanager_heater_source_reading_20261112_run/heater_source_reading_attempt_1.json'))
    entry=json.loads((root/'entry.json').read_text())
    document=json.loads((root/'input_document.json').read_text())
    md=Path(manifest['source_md_path']).read_text()
    original=next(f for f in document['findings'] if f['id']=='F-040')
    response=entry['response']; decision=entry['decision']
    water=next(c for c in response['claims'] if c['claim_id']=='water_context')['evidence']
    image=water[0]
    evidence=[dict(image,bbox_norm_xyxy=[.67,.165,.722,.24],observation='В центре ромба секции Пм4.1 показаны закрашенные треугольники на вертикали. Тонкая линия справа продолжается к TS1.'),
              dict(image,bbox_norm_xyxy=[.773,.875,.905,.925],observation='В обозначении водяной нагревательной секции местной легенды центральный элемент выполнен тонкими пересекающимися штрихами.')]
    reason='Повторный непосредственный просмотр исходного GRAPHIC: различие центрального начертания видно, но водяное назначение однозначно раскрыто подключениями. Это локальное оформление, не ошибка оборудования.'
    response['claims'].append({'claim_id':'limited_symbol_difference','claim':'Центральное начертание секции Пм4.1 отличается от водяного символа в легенде того же листа.','status':'supported','reason':reason,'evidence':evidence})
    decision['claim_reviews'].append({'claim_id':'limited_symbol_difference','role':'defect_basis','verdict':'supported','same_scope_verified':True,'evidence_sufficiency_verified':True,'reason':reason})
    decision.update(action='question',response_sha256=evidence_digest(response),reason=reason,
        question_basis='minor_confirmed',profile_reason=reason,final_wording_verified=True,
        approved_fields={
          'problem':'Начертание нагревательной секции Пм4.1 отличается от водяного символа в легенде.',
          'description':'На листе 10 в центре ромба секции Пм4.1 показаны закрашенные треугольники, а в водяном символе легенды — тонкие пересекающиеся штрихи. Водяное назначение секции раскрыто подключениями 85/60 °C, насосом M3 и клапаном Y3. Тонкая линия справа связана с TS1; её нельзя считать отдельной молнией электрического нагрева. Замечание относится к согласованности оформления, а не к выбору или работоспособности оборудования.',
          'solution':'При корректировке листа согласовать центральное начертание секции Пм4.1 с водяным символом легенды.'},
        display_field_reviews={
          'risk':{'original_value_sha256':evidence_digest(original['risk']),'consistent_with_final_claim':True,'value':'','reason':'Назначение раскрыто водяным узлом; прежнее утверждение о неоднозначном типе не сохраняется.'},
          'severity':{'original_value_sha256':evidence_digest(original['severity']),'consistent_with_final_claim':True,'value':'РЕКОМЕНДАТЕЛЬНОЕ','reason':reason}},
        final_evidence_refs=[{'claim_id':'limited_symbol_difference','evidence_index':i} for i in range(2)]+[{'claim_id':'water_context','evidence_index':0}])
    # Update old descriptions of the coordinator's uncertainty, retaining original claims.
    for c in response['claims']:
        if c['claim_id'] in ('problem','description','solution','risk'):
            c['reason']='Полное отождествление с электрическим символом не подтверждено; ограниченное отличие от водяной легенды рассмотрено отдельно.'
    decision['response_sha256']=evidence_digest(response)
    preview,receipts=apply_review_admissions(document,[entry],md)
    assert receipts==[{'finding_id':'F-040','status':'applied','action':'question'}],receipts
    assert [f for f in preview['findings'] if f['id']!='F-040']==[f for f in document['findings'] if f['id']!='F-040']
    revised=next(f for f in preview['findings'] if f['id']=='F-040')
    assert revised['report_section']=='questions' and revised['risk']==''
    write(root/'entry.json',entry);write(root/'local_preview.json',preview)
    write(root/'request.json',prepare_final_check(entry,document,md)['request'])
    manifest.update(entry_sha256=sha(root/'entry.json'),request_sha256=sha(root/'request.json'),
        action='question',question_basis='minor_confirmed',target_retained_in_pending_archive=False,
        target_retained_as_question_in_preview=True,production_acceptance=False)
    write(root/'preparation.json',manifest)
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('root',type=Path)
    print(json.dumps(prepare(parser.parse_args().root.resolve()),ensure_ascii=False))
