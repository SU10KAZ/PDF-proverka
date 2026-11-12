"""Prepare a bounded, explicit F040 deferral; never change the sealed report."""
import argparse
from copy import deepcopy
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
from backend.app.pipeline.stages.findings_verify.source_review_graphics import attach_graphic_receipts
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_final_check import prepare_final_check
from backend.app.pipeline.stages.findings_verify.source_review_admission import apply_review_admissions
from backend.app.pipeline.stages.findings_verify.graphic_observation_handoff import bind_observation
from .run_primary_text_runtime_probe import sha, write


def prepare(root, observation_request=None, observation_response=None):
    report = Path('/tmp/auditmanager_runtime_recovered_findings_20261112.json')
    source = Path('/tmp/auditmanager_corrected_source_inputs_20261111/AK/02_work/document.md')
    graphics = Path('/tmp/auditmanager_primary_text_runtime_recovery_bound_20261112/work/blocks_stage02_100')
    sealed = json.loads(report.read_text())
    document = deepcopy(sealed)
    md = source.read_text()
    finding = next(f for f in document['findings'] if f['id'] == 'F-040')
    bid = 'blk_e3e39f8820d7474caa10ab626d2a2c91'
    finding['source_evidence_review_status'] = 'pending'
    finding['source_review_requests'] = [{'source_finding_id': 'G-013',
        'source_claim': finding['description'], 'block_ids': [bid]}]
    queue = attach_graphic_receipts(build_source_review_queue([finding], md), md, graphics)
    packet = queue['packets'][0]
    image, = packet['graphic_attachments']
    # This exact image was supplied to the completed publication and source-only
    # runs; the coordinator also viewed it directly. This is not a new model receipt.
    image['attached_to_model'] = True
    image['visually_reviewed'] = True
    if (observation_request is None) != (observation_response is None):
        raise ValueError('both_observation_files_required')
    if observation_request is not None:
        queue['source_only_graphic_observations'] = [
            bind_observation(observation_request, observation_response, bid)]
    def evidence(box, observation):
        return {'type': 'image', 'block_id': bid, 'image_sha256': image['sha256'],
                'bbox_norm_xyxy': box, 'observation': observation}
    shape = [evidence([.66, .10, .76, .27],
        'Тонкая линия от TS1 продолжается внутрь секции Пм4.1; её нельзя автоматически считать самостоятельным знаком электрического нагрева.'),
        evidence([.77, .87, .99, .99],
        'У электрического образца легенды имеется отдельный ломаный знак справа внизу ромба.')]
    water = [evidence([.66, .17, .80, .42],
        'Под секцией Пм4.1 показан водяной узел с M3, Y3 и линиями 85/60 °C.')]
    claims = []
    reviews = []
    units = [('problem', finding['problem']), ('description', finding['description']),
             ('solution', finding['solution']), ('risk', finding['risk'])]
    for cid, text in units:
        role = 'defect_basis' if cid in ('problem', 'description') else (
            'recommendation' if cid == 'solution' else 'context')
        reason = ('Водяной узел виден, но тождество рабочего символа электрическому '
                  'образцу не установлено. Из этого не следует доказанная необходимость '
                  'замены символа или самостоятельного вопроса инженеру.')
        claims.append({'claim_id': cid, 'claim': text, 'status': 'needs_data',
                       'reason': reason, 'evidence': shape + water,
                       'missing_data': ['Достаточное основание идентификации символа и значимости предполагаемого расхождения.']})
        reviews.append({'claim_id': cid, 'role': role, 'verdict': 'needs_data', 'reason': reason})
    claims.append({'claim_id': 'water_context', 'claim': 'Показан водяной узел 85/60 °C с M3 и Y3.',
                   'status': 'supported', 'reason': 'Наблюдение контекста, не самостоятельный дефект.', 'evidence': water})
    reviews.append({'claim_id': 'water_context', 'role': 'context', 'verdict': 'supported',
                    'same_scope_verified': True, 'evidence_sufficiency_verified': True,
                    'reason': 'Непосредственно просмотрен узел той же секции.'})
    response = {'finding_id': finding['id'], 'finding_sha256': packet['finding_sha256'],
                'source_md_sha256': queue['source_md_sha256'], 'claims': claims}
    decision = {'response_sha256': evidence_digest(response), 'origin': 'explicit_source_review',
        'reviewer': 'coordinator_source_review_not_engineer', 'action': 'defer',
        'reason': 'Неподтверждённое основание оставить во внутреннем архиве, не считать технически опровергнутым и не заменять вопросом с новым основанием.',
        'claim_reviews': reviews}
    entry = {'queue': queue, 'response': response, 'decision': decision}
    preview, receipts = apply_review_admissions(document, [entry], md)
    assert receipts == [{'finding_id': 'F-040', 'status': 'applied', 'action': 'defer'}], receipts
    assert preview['findings'] == [f for f in sealed['findings'] if f['id'] != 'F-040']
    assert preview['pending_source_findings'][-1] == finding
    request = prepare_final_check(entry, document, md)['request']
    root.mkdir(parents=True, exist_ok=False)
    write(root / 'entry.json', entry)
    write(root / 'input_document.json', document)
    write(root / 'local_preview.json', preview)
    write(root / 'request.json', request)
    manifest = {'source_report_path': str(report), 'source_report_sha256': sha(report),
        'source_md_path': str(source), 'source_md_sha256': sha(source),
        'entry_sha256': sha(root / 'entry.json'), 'request_sha256': sha(root / 'request.json'),
        'decision_origin': 'explicit_coordinator_review_not_model_response',
        'original_85_findings_preserved_in_sealed_report': True,
        'other_84_findings_identical_in_local_preview': True,
        'target_retained_in_pending_archive': True,
        'source_only_observation_attached': observation_request is not None,
        'final_model_check_completed': False, 'production_acceptance': False}
    write(root / 'preparation.json', manifest)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--observation-request', type=Path)
    parser.add_argument('--observation-response', type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.root.resolve(), args.observation_request, args.observation_response), ensure_ascii=False))
