"""Aggregate source reviews, without converting them into engineer acceptance."""
import hashlib
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build():
    ledger_path = HERE / 'runtime_AK_output_quality_review_in_progress.json'
    ledger = json.loads(ledger_path.read_text())
    rows = ledger['rows']
    by_id = {r['finding_id']: r for r in rows}
    if len(rows) != 85 or len(by_id) != 85 or ledger['unchecked'] != 0:
        raise ValueError('Incomplete or ambiguous source review')
    for row in rows:
        ev = row['evidence']
        if digest(ROOT / ev['path']) != ev['sha256']:
            raise ValueError(f"Evidence changed: {row['finding_id']}")
    if digest(Path(ledger['findings_path'])) != ledger['findings_sha256']:
        raise ValueError('Sealed output changed')

    def action(code, stage, behavior, affected, controls, test, limit):
        ids = lambda ns: [f'F-{n:03d}' for n in ns]
        selected = ids(affected)
        return dict(id=code, stage=stage, proposed_behavior=behavior,
                    affected_findings=selected, preserve_controls=ids(controls),
                    evidence=[dict(finding_id=k, **by_id[k]['evidence']) for k in selected],
                    verification=test, evidence_limit=limit,
                    implementation_status='proposed_not_implemented')

    actions = [
        action('AK-Q01', 'context retrieval / filtering',
               'Перед выводом об отсутствии связи проверить явные отсылки к другому комплекту. Сохранять подтверждённые конфликты одного объекта между документами.',
               [13], [65, 85], 'F-013 не остаётся техническим дефектом; F-065 и F-085 сохраняют конфликт и доказательства.',
               'Контроли проверены по источникам, но не являются подтверждённой инженером парой одинакового типа.'),
        action('AK-Q02', 'graphic interpretation / filtering',
               'Не приравнивать разные условные обозначения. Проверять локальную легенду и соединения; в составном замечании отклонять только опровергнутую часть.',
               [40, 60], [74, 76], 'Снять неверный тип нагревателя F-040; у F-060 оставить вопрос легенды; сохранить конфликты подключения F-074/F-076.',
               'Нет инженерно подтверждённой пары на одинаковые символы; контролируется сохранение соседних полезных графических находок.'),
        action('AK-Q03', 'specialist review / final routing',
               'Отделять команду управления от сигнала обратной связи. Без подтверждения функции канала направлять неоднозначность в вопросы, не объявлять отсутствие DO нарушением.',
               [37, 45, 50, 52, 57], [54, 58, 59], 'Пять неоднозначных трактовок не публикуются как дефекты; явные конфликты номеров DI/DO остаются.',
               'Контроль номера канала не доказывает наличие положительного случая отсутствующей команды; такая пара ещё нужна.'),
        action('AK-Q04', 'merge / final publication',
               'Формировать актуальный вывод по каждой части замечания. Исходный текст и история остаются в provenance; устаревшие оговорки не выдаются как действующие. Каждая часть должна иметь явный исход: сохранена, уточнена, вопрос, опровергнута или не проверена.',
               [3, 5, 6, 7, 17, 27, 32, 33, 51, 53], [1, 4], 'После переработки остаются все подтверждённые конфликты; непроверенные части не получают статус проверенных; неизменные замечания не теряются.',
               'Наличие addendum и совпадение строки не доказывают семантическую правильность финального вывода.'),
        action('AK-Q05', 'merge / final publication',
               'Ссылки на замечания связывать со стабильной идентичностью и текущим ID; внутренние parent-идентификаторы хранить в истории, а не в пользовательском тексте.',
               [82, 84], [85], 'После перенумерации ссылки не ведут к чужому объекту; источник и содержание замечания сохраняются.',
               'Нельзя удалять любой текст F-xxx регулярным выражением: возможны полезные корректные ссылки.'),
        action('AK-Q06', 'merge / duplicate handling',
               'Объединять повтор одного конфликта с сохранением всех мест. Вопрос, уже разрешённый найденными доказательствами, присоединять к подтверждённому выводу.',
               [9, 63, 64, 78, 79, 81], [42, 47], 'F-078/079/081 дают один конфликт с тремя местами; F-009 не повторяет разрешённый вопрос; разные установки F-042/047 не склеиваются.',
               'Совпадение текста или марки само по себе не устанавливает идентичность объекта.'),
        action('AK-Q07', 'specialist review / final routing',
               'Разделять факт повторной марки, область уникальности и доказанный конфликт. Нормативную ссылку без проверки применимости не повышать до установленного нарушения.',
               [20, 21, 22, 68, 69, 77, 82, 83], [65, 85], 'Неопределённая область/норма остаётся вопросом; подтверждённая идентичность объекта позволяет сохранить технический конфликт.',
               'Текущий юридический статус норм не проверен; инженерная полезность вопросов не измерена.'),
        action('AK-Q08', 'evidence reconciliation / final publication',
               'Сохранять буквальные значения источников и точно указывать лист. При расхождении трасс не подменять исходную строку предполагаемым исправлением.',
               [16, 19, 34], [14, 66, 67, 75], 'Исправить ошибочный идентификатор/место в выводе; сохранить исходные MD и графические значения отдельно; не потерять подтверждённую арифметику.',
               'Арифметическое расхождение не доказывает фактическую недопоставку или отсутствие допустимого запаса.'),
    ]
    return dict(status='source_quality_assessment_complete_remediation_pending',
                ledger={'path': str(ledger_path.relative_to(ROOT)), 'sha256': digest(ledger_path)},
                sealed_findings={'path': ledger['findings_path'], 'sha256': ledger['findings_sha256']},
                total_assessed=len(rows), unchecked=0,
                status_counts=dict(sorted(Counter(r['status'] for r in rows).items())),
                clear_refuted_findings=['F-013', 'F-040'],
                unsupported_technical_promotions=['F-037', 'F-045', 'F-050', 'F-052', 'F-057'],
                mixed_supported_and_unsupported=['F-060'],
                actions=actions, production_acceptance=False,
                precision=None, engineer_acceptance=None,
                limits=['85 assessed is not 85 accepted or true.',
                        'Actions overlap; their counts must not be summed as defects.',
                        'Known AK sources are development regression, not blind validation.',
                        'This assessment does not close stage 4 or the six-discipline plan.',
                        'No filter effectiveness or engineer workload improvement measured.'],
                next_step='Implement evidence-bound per-subclaim publication reconciliation, then replay the sealed AK output and measure retained coverage, false claims, routing and duplicates before further full trials.')


if __name__ == '__main__':
    result = build()
    destination = HERE / 'runtime_AK_quality_summary_20261112.json'
    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'assessed': result['total_assessed'], 'actions': len(result['actions']), 'output': str(destination)}, ensure_ascii=False))
