"""Fresh F5 repair build and post-freeze structural audit. No model calls.

Run snapshot, build and audit as separate processes so the source-only build's
access hook never needs access to the previous packages. Baseline artifacts are
read only for mechanical BEFORE/AFTER measurements, never for discovery.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

import fitz

from experiments.project_change_272.inventory import ROOT, sha
from experiments.project_change_contracts_272.witnesses import raster_locator_errors, normalized_box_valid
from .common import OUT as BASELINE, fingerprint, write
from .contract_adapters import has_source_text
from .run import run, assert_answer_blind

OUT = ROOT / 'fresh_dev_sample_f5_pipeline_v2'
ROUTES = ('TEXT', 'TABLE', 'GRAPHIC')


def read(path):
    return json.loads(Path(path).read_text())


def file_hashes(root):
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob('*')) if p.is_file()}


def snapshot(output):
    if output.exists():
        raise FileExistsError('V2 output must be new: ' + str(output))
    freeze = read(BASELINE / 'PACKAGES_FREEZE.json')
    if len(freeze['package_hashes']) != 53:
        raise ValueError('Expected the original 53 frozen packages')
    write(output / 'BASELINE_FILE_HASHES.json', dict(root=str(BASELINE), files=file_hashes(BASELINE)))


def route(kind):
    return ('GRAPHIC' if kind in {'GRAPHIC_REGION', 'GRAPHIC_SCHEME'} else
            'TABLE' if kind in {'TABLE_COMPLETE', 'EQUIPMENT_SCHEDULE'} else 'TEXT')


def mechanical_payload(evidence, kind):
    # Audit delivered bytes/text, not inventory route labels or coverage flags.
    if evidence.get('raster'):
        return True
    if route(kind) == 'GRAPHIC':
        return False
    if route(kind) == 'TABLE':
        rows = [s for s in evidence.get('quote', '').splitlines()
                if s.strip().startswith('|') and not re.fullmatch(r'[\s|:\-]+', s)]
        return len(rows) >= 3
    return has_source_text(evidence.get('quote'))


def metrics(root):
    counts, rows, empty_rows = Counter(), [], []
    empty_links = 0
    for path in sorted(root.glob('pair_*/packages/*.json')):
        p = read(path)
        counts['packages'] += 1
        counts[p['completeness']] += 1
        counts[p['candidate_subject']['confidence']] += 1
        evidence = {e['evidence_id']: e for s in ('old', 'new') for e in p['evidence_packet']['evidence'][s]}
        bindings = [b for state in p['typed_state_skeleton']['states'].values() for b in state['evidence_bindings']]
        empty = [b for b in bindings if b['evidence_id'] not in evidence or
                 not (evidence[b['evidence_id']].get('raster') or has_source_text(evidence[b['evidence_id']].get('quote')))]
        if empty:
            empty_links += len(empty)
            empty_rows.append(dict(package=str(path.relative_to(root)),
                evidence_ids=sorted({b['evidence_id'] for b in empty}), binding_count=len(empty)))
        rows.append(dict(package=str(path.relative_to(root)), candidate_id=p['candidate_subject']['candidate_id'],
                         completeness=p['completeness'], confidence=p['candidate_subject']['confidence']))
    return dict(packages=counts['packages'], empty_v4_bindings=len(empty_rows),
        empty_v4_binding_references=empty_links,
        **{k: counts[k] for k in ('COMPLETE', 'PARTIAL', 'MISSING', 'STRONG', 'POSSIBLE', 'UNRESOLVED')},
        empty_packages=empty_rows, package_rows=rows)


def audit_packages(output):
    issues, packages, delivery = [], [], []
    checked_rasters = {}
    freeze = read(output / 'PACKAGES_FREEZE.json')
    inventories = {i: {s: read(output / f'pair_{i}/DOCUMENT_INVENTORY_{s.upper()}.json')
                       for s in ('old', 'new')} for i in (2, 8)}
    for path in sorted(output.glob('pair_*/packages/*.json')):
        p = read(path)
        rel = str(path.relative_to(output))
        local = []

        def check(condition, code):
            if not condition:
                local.append(code)

        packet, body = p['evidence_packet'], p['f1_requirement_package']
        index = p['provenance']['pair_index']
        invs = inventories[index]
        regions = {r['region_id']: r for inv in invs.values() for r in inv['regions']}
        check(p['package_hash'] == freeze['package_hashes'].get(rel) ==
              fingerprint({k: v for k, v in p.items() if k != 'package_hash'}), 'PACKAGE_HASH_DRIFT')
        check(body['package_hash'] == fingerprint({k: v for k, v in body.items() if k != 'package_hash'}), 'F1_HASH_DRIFT')
        check(packet['packet_id'] == fingerprint({k: v for k, v in packet.items() if k != 'packet_id'})[:24], 'PACKET_HASH_DRIFT')
        check(packet['source_package_hash'] == body['package_hash'], 'SOURCE_PACKAGE_HASH_DRIFT')
        check(packet['evidence_coverage']['delivery_hash'] == fingerprint(packet['evidence']), 'DELIVERY_HASH_DRIFT')
        check(not p['inference_executed'] and p['typed_state_skeleton']['semantic_verdict'] is None, 'INFERENCE_OR_VERDICT')
        assert_answer_blind(p)
        evidence, delivered_routes = {}, set()
        for side in ('old', 'new'):
            for e in packet['evidence'][side]:
                eid = e['evidence_id']
                check(eid not in evidence, 'DUPLICATE_EVIDENCE_ID')
                evidence[eid] = e
                check(e['side'] == side and e['document_version'] == invs[side]['document_version'], 'EVIDENCE_SIDE_VERSION')
                check(e['source_receipt'] == invs[side]['source']['pdf'], 'SOURCE_RECEIPT_DRIFT')
                check(invs[side]['pages'][e['page'] - 1]['status'] == 'INDEXED', 'EXCLUDED_PAGE_DELIVERED')
                check(bool(e['raster']) or has_source_text(e['quote']), 'EMPTY_PACKET_EVIDENCE')
                units = [u for u in body['evidence'] if u['side'].lower() == side and u['page'] == e['page']
                         and u['document_version'] == e['document_version']
                         and (u['raster']['sha256'] if u['raster'] else None) == (e['raster']['sha256'] if e['raster'] else None)]
                check(any(e['quote'] == u['text'] for u in units), 'UNDELIVERED_QUOTE')
                actual = set()
                if e['raster']:
                    raster = e['raster']
                    raster_path = (output / raster['path']).resolve()
                    check(raster_path.is_relative_to(output / 'rasters'), 'RASTER_OUTSIDE_OUTPUT')
                    if str(raster_path) not in checked_rasters:
                        pix = fitz.Pixmap(str(raster_path))
                        checked_rasters[str(raster_path)] = (sha(raster_path), pix.width, pix.height)
                    check(checked_rasters[str(raster_path)] ==
                          (raster['sha256'], raster['width'], raster['height']), 'RASTER_BYTES_OR_DIMENSIONS')
                    check(raster['source_pdf_sha256'] == e['source_receipt']['sha256'], 'RASTER_SOURCE_MISMATCH')
                for link in e['region_bindings']:
                    r = regions[link['region_id']]
                    check(r['side'].lower() == side and r['page'] == e['page'] and
                          r['document_version'] == e['document_version'], 'REGION_SIDE_PAGE_VERSION')
                    check(link['bbox_norm'] == r['bbox_norm'] and normalized_box_valid(link['bbox_norm']), 'REGION_LOCATOR')
                    check(any(u['requirement_id'] == link['requirement_id'] and
                              u['provenance']['region_id'] == link['region_id'] for u in units), 'REGION_UNIT_LINK')
                    real = mechanical_payload(e, link['evidence_type'])
                    check(real == link['payload_delivered'], 'ROUTE_PAYLOAD_FLAG')
                    check(link['boundary_verified'] == r['boundary_verified'], 'BOUNDARY_PROMOTED')
                    if real:
                        actual.add(route(link['evidence_type']))
                check(actual == set(e['delivered_routes']), 'DELIVERED_ROUTE_COUNT')
                delivered_routes.update(actual)
        check(sum(bool(e['raster']) for e in evidence.values()) <= 8, 'RASTER_BUDGET')
        check(sum(len(e['quote']) for e in evidence.values()) <= 28000, 'TEXT_BUDGET')
        for side, state in p['typed_state_skeleton']['states'].items():
            check(state['value'] == 'UNKNOWN' and not state['state_value_evidence_ids'], 'INVENTED_STATE_VALUE')
            for b in state['evidence_bindings']:
                e = evidence.get(b['evidence_id'])
                check(e is not None and bool(e.get('raster') or has_source_text(e.get('quote'))), 'V4_REFERENCE_TO_EMPTY_DELIVERY')
                if e:
                    check(e['side'] == side and b['side'] == side, 'V4_SIDE')
                    check(b['provenance']['document_version'] == e['document_version'], 'V4_VERSION')
                    check(b['provenance']['packet_hash'] == fingerprint(packet), 'V4_PACKET_HASH')
                check(not b['witness_validated'], 'PREINFERENCE_SEMANTIC_WITNESS')
        reqs = packet['evidence_coverage']['requirements']
        check({r['requirement']['requirement_id'] for r in reqs} ==
              {r['requirement_id'] for r in body['requirements']}, 'REQUIREMENT_DROPPED')
        check(p['completeness'] == ('COMPLETE' if reqs and all(r['completeness'] == 'COMPLETE' for r in reqs)
              else 'MISSING' if not reqs or all(r['completeness'] == 'MISSING' for r in reqs) else 'PARTIAL'), 'COMPLETENESS_PROMOTED')
        for row in reqs:
            q = row['requirement']
            expected = sorted(e['evidence_id'] for e in evidence.values() if any(
                r['requirement_id'] == q['requirement_id'] and mechanical_payload(e, q['required_type'])
                for r in e['region_bindings']))
            check(expected == row['evidence_ids'], 'REQUIREMENT_ROUTE_BINDING')
            delivery.append(dict(package=rel, pair_index=index, requirement_id=q['requirement_id'],
                side=q['side'], page=q['page'], route=route(q['required_type']),
                evidence_role=q['evidence_role'], evidence_ids=row['evidence_ids'],
                payload_delivered=bool(expected), completeness=row['completeness'],
                omission_reason=row['delivery']['omission_reason'],
                raster_ids=[eid for eid in expected if evidence[eid]['raster']],
                text_ids=[eid for eid in expected if has_source_text(evidence[eid]['quote'])]))
            if row['completeness'] == 'COMPLETE':
                check(bool(expected) and all(regions[r['region_id']]['boundary_verified']
                    for eid in expected for r in evidence[eid]['region_bindings']
                    if r['requirement_id'] == q['requirement_id']), 'COMPLETE_WITHOUT_BOUNDARY')
        for g in p['graphic_bindings']:
            e = evidence[g['evidence_id']]
            actual = dict(e)
            if e['raster']:
                actual['raster'] = e['raster'] | dict(path=str(output / e['raster']['path']))
            errors = raster_locator_errors(g, actual)
            check(errors == g['errors'], 'F4_ERRORS_DRIFT')
            check(g['f4_status'] == ('ACCEPTED' if not errors else 'UNAVAILABLE' if not e['raster'] else 'REJECTED'), 'F4_STATUS')
        c = p['candidate_subject']
        if c['confidence'] == 'STRONG':
            check(bool(c['edges']) and all(e['confidence'] == 'STRONG' and
                  e['basis']['scope_relation'] == 'EXACT' and e['basis']['source_form_overlap'] and
                  e['basis']['old_scope'] == e['basis']['new_scope'] and 'UNKNOWN' not in e['basis']['old_scope']
                  for e in c['edges']), 'STRONG_RULE_RELAXED')
        packages.append(dict(package=rel, pair_index=index, delivered_routes=sorted(delivered_routes),
                             status='FAIL' if local else 'PASS'))
        issues.extend(dict(package=rel, issue=code) for code in sorted(set(local)))
    check_paths = {p['package'] for p in packages}
    if check_paths != set(freeze['package_hashes']) or freeze['aggregate_hash'] != fingerprint(freeze['package_hashes']):
        issues.append(dict(issue='PACKAGE_FREEZE_SET_OR_AGGREGATE'))
    route_summary = {}
    for r in ROUTES:
        group = [x for x in delivery if x['route'] == r]
        route_summary[r] = dict(requested_requirements=len(group),
            delivered_requirements=sum(x['payload_delivered'] for x in group),
            undelivered_requirements=sum(not x['payload_delivered'] for x in group),
            complete_requirements=sum(x['completeness'] == 'COMPLETE' for x in group),
            delivered_packages=sum(r in p['delivered_routes'] for p in packages),
            requested_packages=len({x['package'] for x in group}),
            with_raster_requirements=sum(bool(x['raster_ids']) for x in group),
            with_text_requirements=sum(bool(x['text_ids']) for x in group))
    return dict(schema='F5_REPAIR_V2_STRUCTURAL_AUDIT/1', status='FAIL' if issues else 'PASS',
        issues=issues, issue_counts=dict(Counter(i['issue'] for i in issues)),
        package_count=len(packages), packages=packages, unique_rasters_verified=len(checked_rasters),
        route_summary=route_summary, model_calls=0), delivery


def report(result, structural, diagnosis):
    before, after = result['before'], result['after']
    lines = ['# F5 REPAIR V2 — новый результат', '',
        f"Дата формирования: {result['generated_at']}. Каталог: `{result['output']}`.", '',
        f"Structural audit: **{structural['status']}**. Pipeline readiness: **{result['status']}**.", '',
        '## BEFORE → AFTER', '', '| Метрика | BEFORE | AFTER |', '|---|---:|---:|']
    for key in ('empty_v4_bindings', 'COMPLETE', 'PARTIAL', 'MISSING', 'STRONG', 'POSSIBLE', 'UNRESOLVED', 'packages'):
        lines.append(f"| {key} | {before[key]} | {after[key]} |")
    lines += [f"| structural audit | FAIL | {structural['status']} |", '| model calls | 0 | 0 |', '',
        '`empty_v4_bindings` сохраняет единицу исходного аудита: число пакетов с пустыми V4-ссылками. '
        f"Отдельных ссылок: {before['empty_v4_binding_references']} → {after['empty_v4_binding_references']}.", '',
        '## Исправления и причины', '',
        '1. F1 сохранял записи без текста и растра после исчерпания бюджета. Адаптер создавал для них '
        'source ID, а V4 проверял принадлежность ID пакету, сторону и версию, но не содержимое. '
        'V2 исключает пустые записи из evidence, сохраняет их в delivery_omissions и оставляет требования '
        'и бюджетные пропуски в coverage. Identity и counter bindings допускают только доставленные данные.', '',
        '2. Coverage связывал requirement со всеми типами регионов на странице. Теперь связь проверяется '
        'по requirement_id и фактически доставленному региону. Объявленные маршруты и реальные доставки '
        'считаются отдельно. Для GRAPHIC нужен проверенный растр; заголовок таблицы не заменяет её строки.', '',
        f"3. В исходном графе уже было {diagnosis['before_strong_edges']} STRONG edges. UNKNOWN-связи объединяли "
        'их с широким контекстом, после чего confidence всей компоненты становился POSSIBLE. V2 сохраняет '
        'эти группы и отдельно выпускает компоненты из STRONG edges с точным scope. Правило edge не менялось: '
        'одинаковая функция, известный одинаковый scope и пересечение source forms. UNKNOWN остаётся POSSIBLE.', '',
        f"Добавлено {after['packages'] - before['packages']} пакетов точного scope; широкие кандидаты сохранены. "
        'Пакеты пересекаются по контексту. STRONG означает качество retrieval-соответствия; '
        'это не число подтверждённых инженерных изменений. Лимит 80 не изменён.', '',
        '## Реальная доставка TEXT / TABLE / GRAPHIC', '',
        '| Route | Требований | С данными | Без данных | COMPLETE | Пакетов с данными |',
        '|---|---:|---:|---:|---:|---:|']
    for r in ROUTES:
        m = structural['route_summary'][r]
        lines.append(f"| {r} | {m['requested_requirements']} | {m['delivered_requirements']} | "
                     f"{m['undelivered_requirements']} | {m['complete_requirements']} | {m['delivered_packages']} |")
    lines += ['', 'Доставка здесь означает наличие исходного содержимого, пригодного для чтения. '
        'Она не подтверждает полные границы раздела, продолжение таблицы, все примечания или связанные узлы. '
        'Текст/OCR остаётся непроверенной транскрипцией; проверка растра подтверждает байты, размеры, версию и locator.', '',
        '## Почему COMPLETE остаётся 0', '',
        'Source discovery пока не сертифицирует семантические границы TEXT/TABLE/GRAPHIC. '
        'В V2 boundary_verified не повышался, бюджеты остались 8 растров / 28000 символов. '
        'Поэтому PARTIAL сохранён там, где не доказана полнота. Кроме того, часть страниц закрыта frozen '
        'embargo/history policy. Успешный structural audit подтверждает исправление структуры, '
        'но не готовность полного корпуса к inference.', '',
        '## Сохранность, воспроизводимость и ограничения', '',
        f"- Старые frozen packages: {result['preservation']['package_count']} из 53 без изменений SHA-256.",
        f"- Все исходные файлы: {result['preservation']['unchanged_files']} из {result['preservation']['total_files']} без изменений.",
        '- Те же DEV пары 2 / 8, OLD=stage_1, NEW=stage_2; версии и split проверены существующим guard.',
        '- Две полные локальные сборки дали идентичные пакеты; PACKAGES_FREEZE.json запечатан.',
        '- Structural audit проверил каждый пакет после freeze; пакеты при аудите не редактировались.',
        '- Model calls: 0. В сборке и аудите запрещены network/subprocess. OpenRouter запросов нет.',
        '- VALIDATION / FINAL HOLDOUT не открывались. Production не менялся.',
        '- Метрики BEFORE прочитаны из предыдущих пакетов только для механического сравнения; '
        'сборка читала только четыре разрешённых источника. Исторические ответы не использовались.', '',
        '## Артефакты', '',
        '- F5_REPAIR_V2_RESULT.json — новый результат BEFORE/AFTER и причины блокировки.',
        '- STRUCTURAL_AUDIT.json — проверки каждого пакета и подтверждение сохранности.',
        '- ROUTE_DELIVERY_AUDIT.json — доставка каждого requirement и связанные evidence IDs.',
        '- STRONG_DIAGNOSIS.json — исходные STRONG edges, новые кандидаты и связи с контекстом.',
        '- BASELINE_FILE_HASHES.json — исходные SHA-256; EMPTY_BINDING_REPAIR.json — все 17 случаев.',
        '- TEST_RESULTS.xml / TEST_RECEIPT.json — новые тесты текущей итерации.', '',
        f"Итог: **{result['status']}**; structural repair **{structural['status']}**; остановка до inference."]
    return '\n'.join(lines) + '\n'


def audit(output):
    # This process can read the two artifact trees and Python, but not any other
    # corpus evidence. The build runs in a different, stricter source-only hook.
    def guard(event, args):
        if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
            raise PermissionError('F5 repair audit forbids network and subprocesses')
        if event == 'open' and isinstance(args[0], (str, bytes, Path)):
            path = Path(args[0]).resolve()
            if path.is_relative_to(ROOT.parent) and not path.is_relative_to(output):
                allowed = (path.is_relative_to(BASELINE) and
                           str(path.relative_to(BASELINE)) in baseline_files)
                mode, flags = args[1:3]
                writing = (isinstance(mode, str) and any(c in mode for c in 'wax+')) or bool(flags & 3)
                if not allowed or writing:
                    raise PermissionError('Audit baseline is read-only; other corpus reads denied')
    baseline = read(output / 'BASELINE_FILE_HASHES.json')
    baseline_files = baseline['files']
    sys.addaudithook(guard)
    current = file_hashes(BASELINE)
    changed = sorted(p for p in set(current) | set(baseline_files) if current.get(p) != baseline_files.get(p))
    before, after = metrics(BASELINE), metrics(output)
    if before['packages'] != 53 or before['empty_v4_bindings'] != 17:
        raise ValueError('Baseline does not match the requested F5 iteration')
    frozen_before = {str(p.relative_to(output)): sha(p) for p in output.glob('pair_*/packages/*.json')}
    structural, delivery = audit_packages(output)
    if changed:
        structural['issues'].append(dict(issue='BASELINE_MODIFIED', paths=changed))
    preservation = dict(status='FAIL' if changed else 'PASS', changed_files=changed,
        total_files=len(baseline_files), unchanged_files=sum(current.get(p) == h for p, h in baseline_files.items()),
        package_count=sum('/packages/' in p and current.get(p) == h for p, h in baseline_files.items()))
    diagnosis = dict(before_strong_edges=0, before_strong_candidates=before['STRONG'],
        after_strong_candidates=after['STRONG'], strong_rule_changed=False, pairs={})
    empty_repairs = []
    for index in (2, 8):
        old = read(BASELINE / f'pair_{index}/CORRESPONDENCE.json')
        new = read(output / f'pair_{index}/CORRESPONDENCE.json')
        diagnosis['before_strong_edges'] += sum(e['confidence'] == 'STRONG' for e in old['edges'])
        by_id = {c['candidate_id']: c for c in new['candidates']}
        retained = all(c == by_id.get(c['candidate_id']) for c in old['candidates'])
        edges_same = old['edges'] == new['edges']
        if not retained or not edges_same:
            structural['issues'].append(dict(issue='BROAD_CONTEXT_OR_EDGE_RULE_CHANGED', pair_index=index))
        diagnosis['pairs'][str(index)] = dict(before_edge_counts=dict(Counter(e['confidence'] for e in old['edges'])),
            before_candidate_counts=dict(Counter(c['confidence'] for c in old['candidates'])),
            after_candidate_counts=dict(Counter(c['confidence'] for c in new['candidates'])),
            all_original_candidates_retained=retained, all_original_edges_identical=edges_same,
            added_candidates=[c for c in new['candidates'] if c.get('parent_context_candidate_id')])
    for item in before['empty_packages']:
        p = read(output / item['package'])
        evidence = {e['evidence_id']: e for s in ('old', 'new') for e in p['evidence_packet']['evidence'][s]}
        omitted = {r['evidence_id'] for r in p['evidence_packet']['delivery_omissions']}
        remaining = [eid for eid in item['evidence_ids'] if eid in evidence and
                     not (evidence[eid]['raster'] or has_source_text(evidence[eid]['quote']))]
        empty_repairs.append(item | dict(after_empty_evidence_ids=remaining,
            recorded_as_omitted=all(eid in omitted for eid in item['evidence_ids']),
            status='FAIL' if remaining or not all(eid in omitted for eid in item['evidence_ids']) else 'PASS'))
    if any(r['status'] != 'PASS' for r in empty_repairs):
        structural['issues'].append(dict(issue='INCOMPLETE_EMPTY_BINDING_REPAIR'))
    frozen_after = {str(p.relative_to(output)): sha(p) for p in output.glob('pair_*/packages/*.json')}
    if frozen_before != frozen_after:
        structural['issues'].append(dict(issue='AUDIT_MUTATED_PACKAGES'))
    structural.update(status='FAIL' if structural['issues'] else 'PASS', preservation=preservation,
        frozen_packages_changed=frozen_before != frozen_after, package_files_sha256=frozen_after,
        issue_counts=dict(Counter(i['issue'] for i in structural['issues'])))
    build_result = read(output / 'F5_RESULT.json')
    tests = read(output / 'TEST_RECEIPT.json')
    result = dict(schema='F5_REPAIR_V2_RESULT/1', generated_at=datetime.now(timezone.utc).isoformat(),
        output=str(output), baseline=str(BASELINE),
        before={k: v for k, v in before.items() if k not in {'empty_packages', 'package_rows'}},
        after={k: v for k, v in after.items() if k not in {'empty_packages', 'package_rows'}},
        structural_audit=structural['status'], preservation=preservation, model_calls=0,
        status=build_result['status'] if structural['status'] == 'PASS' else 'F5_STILL_NEEDS_REPAIR',
        blocking_gates=build_result['blocking_gates'], tests=tests,
        full_rerun_deterministic=read(output / 'PACKAGES_FREEZE.json')['full_rerun_deterministic'])
    write(output / 'STRUCTURAL_AUDIT.json', structural)
    write(output / 'ROUTE_DELIVERY_AUDIT.json', dict(summary=structural['route_summary'], requirements=delivery))
    write(output / 'STRONG_DIAGNOSIS.json', diagnosis)
    write(output / 'EMPTY_BINDING_REPAIR.json', empty_repairs)
    write(output / 'F5_REPAIR_V2_RESULT.json', result)
    (output / 'F5_REPAIR_V2_REPORT.md').write_text(report(result, structural, diagnosis))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if structural['status'] != 'PASS':
        raise SystemExit('Structural audit failed: ' + str(structural['issue_counts']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('snapshot', 'build', 'audit'))
    parser.add_argument('--output', type=Path, default=OUT)
    args = parser.parse_args()
    output = args.output.resolve()
    if output == BASELINE or output.is_relative_to(BASELINE):
        raise PermissionError('The original F5 artifacts are immutable')
    if args.action == 'snapshot':
        snapshot(output)
    elif args.action == 'build':
        if not (output / 'BASELINE_FILE_HASHES.json').is_file():
            raise FileNotFoundError('Take the baseline snapshot first')
        if (output / 'PACKAGES_FREEZE.json').exists():
            raise FileExistsError('V2 packages are already frozen')
        run(output)
    else:
        audit(output)
