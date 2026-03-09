#!/usr/bin/env python3
"""Update 01_text_analysis.json: add block prioritization and new findings from MD analysis."""
import json
from datetime import datetime

PROJECT_DIR = r"projects/133-23-ГК-ОВ2.2 (7)"
STAGE01_PATH = f"{PROJECT_DIR}/_output/01_text_analysis.json"
INDEX_PATH = f"{PROJECT_DIR}/_output/blocks/index.json"

HIGH_KEYWORDS = [
    'ДВ2', 'ДВ3', 'ДВ4', 'ДВ5',
    'ДП2', 'ДП3', 'ДП8', 'ДП10', 'ДП11', 'ДП12', 'ДП13', 'ДП14', 'ДП17', 'ДП19',
    'ПЕ1', 'ПЕ2', 'ПЕ3', 'ПЕ4', 'ПЕ5', 'ПЕ6', 'ПЕ7', 'ПЕ8', 'ПЕ9',
    'ПЕ10', 'ПЕ11', 'ПЕ12', 'ПЕ13', 'ПЕ14', 'ПЕ15',
    'изоляции', 'огнестойк',
    'В7', 'противодымной', 'дымоудаления',
    'П1к', 'В1к', 'В1ж',
    'П2тех', 'В2тех', 'П1тех',
    'дымос', 'КЛОП',
]
SKIP_KEYWORDS = [
    'аэродинамических характеристик', 'График', 'зависимости статического давления',
    'Аэродинамическая характеристика',
]
LOW_KEYWORDS = [
    'взрыв-схема', 'Трехмерная', '3D-рендер',
    'Типовой узел', 'узла крепления', 'узла монтажа', 'узла прохода', 'узла проходки',
    'Чертеж узла', 'Чертеж общего вида', 'вид снизу',
    'расположения зданий',
    'таблицей характеристик', 'таблицей технических', 'таблицей парам',
    'таблицей размеров', 'таблицей габарит',
]


def get_priority(block):
    label = block.get('ocr_label', '')
    # SKIP first
    for kw in SKIP_KEYWORDS:
        if kw in label:
            return 'SKIP', 'Нет аудиторской ценности (график характеристик)'
    # HIGH
    for kw in HIGH_KEYWORDS:
        if kw in label:
            return 'HIGH', f'Система/оборудование "{kw}" — приоритетная проверка'
    # LOW
    for kw in LOW_KEYWORDS:
        if kw in label:
            return 'LOW', 'Справочная/узловая информация'
    # JSON labels (broken OCR on complex drawings)
    if label.strip().startswith('{'):
        return 'HIGH', 'OCR вернул JSON вместо текста — сложная схема, требует визуального анализа'
    # Plans/sections → MEDIUM
    if any(kw in label for kw in ['План ', 'план ', 'Разрез', 'разрез', 'Аксонометр', 'аксонометр',
                                    'Фрагмент плана', 'венткамер', 'Схема расположения']):
        return 'MEDIUM', 'Чертёж/план/разрез — стандартная визуальная проверка'
    return 'LOW', 'Не идентифицирован как приоритетный объект аудита'


def main():
    # Load existing
    with open(STAGE01_PATH, encoding='utf-8') as f:
        data = json.load(f)

    with open(INDEX_PATH, encoding='utf-8') as f:
        idx = json.load(f)

    # Build block lists
    blocks_for_review = []
    blocks_skipped = []

    for b in idx['blocks']:
        priority, reason = get_priority(b)
        entry = {
            'block_id': b['block_id'],
            'page': b['page'],
            'file': b['file'],
            'size_kb': b['size_kb'],
            'priority': priority,
            'ocr_label_short': b.get('ocr_label', '')[:80].replace('\n', ' '),
            'check_reason': reason,
        }
        if priority in ('HIGH', 'MEDIUM'):
            blocks_for_review.append(entry)
        else:
            blocks_skipped.append(entry)

    high_count = sum(1 for b in blocks_for_review if b['priority'] == 'HIGH')
    medium_count = sum(1 for b in blocks_for_review if b['priority'] == 'MEDIUM')
    print(f"HIGH: {high_count}, MEDIUM: {medium_count}, LOW+SKIP: {len(blocks_skipped)}")

    # New findings from MD analysis (add after existing T-001..T-011)
    new_findings = [
        {
            "id": "T-012",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "fire_protection",
            "source": "Аксонометрические схемы ПЕ1–ПЕ15, листы 29–30 (PDF стр. ~62–67)",
            "finding": (
                "На схемах обходных каналов ПЕ (байпасы системы В1ж) выявлено несоответствие "
                "класса огнезащиты: часть каналов обозначена как EI150 δ=30 мм, "
                "часть — EI90, а ряд каналов на уровнях +35–49 м имеет только 'тепловую изоляцию "
                "δ=25 мм' без указания класса EI. Обходные каналы ПЕ пересекают противопожарные "
                "преграды или примыкают к дымоудаляющим шахтам, что требует единообразного класса "
                "огнестойкости в соответствии с СП 7.13130.2013 п.7.14. "
                "Требуется унификация обозначений и подтверждение классов EI на блоках ПЕ8–ПЕ15."
            ),
            "norm": (
                "СП 7.13130.2013 п.7.14: предел огнестойкости воздуховодов и шахт систем "
                "общеобменной вентиляции при пересечении противопожарных преград — не менее EI30; "
                "для транзитных каналов вне пределов обслуживаемого этажа — EI90 или EI150 "
                "в зависимости от расстояния пересечения"
            ),
            "needs_visual_check": True,
            "related_block_ids": [
                "DGYP-64U3-AAP", "97DQ-T7Q3-UYK", "9WFC-WQ9K-VCE", "996V-FFUN-F9V",
                "4PDY-AMQP-4MU", "4U43-9CGD-XNJ", "J3QH-FP6U-JUH",
                "7FGQ-Q74W-DTG", "96VR-7H3V-A4Y", "9VT6-LKLQ-7QN",
                "6F7N-FKLK-49V", "94X4-WDRW-XCT", "XMLA-MDHE-U33",
                "P9DR-NCLH-NLG", "66FH-YL7M-7PU"
            ]
        },
        {
            "id": "T-013",
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "smoke_exhaust",
            "source": "Аксонометрическая схема ДП8, лист 27 (PDF стр. ~60), блок 6AJQ-CGMH-4M4",
            "finding": (
                "По тексту MD-файла схема ДП8 охватывает только 8 из 15 этажей надземной части. "
                "Для 15-этажного МКД с одной лестничной клеткой типа Н2 или Л2 покрытие дымоудаления "
                "должно распространяться на все коридоры, где нет естественного проветривания. "
                "Требуется визуальная проверка схемы ДП8 и подтверждение, что оставшиеся этажи "
                "обслуживаются другими системами (ДП или иными)."
            ),
            "norm": (
                "СП 7.13130.2013 п.7.2: обязательное дымоудаление из коридоров без естественного "
                "проветривания длиной более 15 м жилых и общественных зданий"
            ),
            "needs_visual_check": True,
            "related_block_ids": ["6AJQ-CGMH-4M4"]
        },
        {
            "id": "T-014",
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "equipment",
            "source": "Аксонометрическая схема В2тех, лист 24 (PDF стр. ~55), блок TRA7-D6V9-9LJ",
            "finding": (
                "На схеме системы В2тех (вытяжная общеобменная вентиляция технических помещений) "
                "обнаружен клапан типа КОМ-ДД (дымо-дымовой), который относится к оборудованию "
                "противодымной защиты. Применение противодымного клапана в системе общеобменной "
                "вентиляции нетипично и требует обоснования или исправления маркировки. "
                "Возможна ошибка в обозначении (вместо КОМ должен стоять КДМ или РДК)."
            ),
            "norm": (
                "СП 7.13130.2013 (классификация клапанов); ГОСТ 21.602-2016 (условные обозначения "
                "элементов систем ОВ)"
            ),
            "needs_visual_check": True,
            "related_block_ids": ["TRA7-D6V9-9LJ", "7TU4-CLMY-4H7"]
        },
        {
            "id": "T-015",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "documentation",
            "source": "Штамп изменений, лист 1 Общих данных (тексты ревизий)",
            "finding": (
                "В таблице изменений (штамп) присутствуют изменения №1, №3, №4, но изменение №2 "
                "отсутствует. Пропуск номера в последовательности изменений может свидетельствовать "
                "о технической ошибке в нумерации или о неучтённом изменении. Рекомендуется проверить "
                "и восстановить целостность журнала изменений."
            ),
            "norm": "ГОСТ 21.101-2020 п.8 (порядок внесения изменений в проектную документацию)",
            "needs_visual_check": False,
            "related_block_ids": []
        },
        {
            "id": "T-016",
            "severity": "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
            "category": "airflow",
            "source": "Аксонометрические схемы ПЕ1–ПЕ15 с указанием расходов воздуха",
            "finding": (
                "Расходы воздуха через обходные каналы ПЕ указаны на схемах, однако их "
                "соответствие суммарному расходу системы В1ж (28 540 м³/ч) требует арифметической "
                "проверки: сумма расходов по всем ПЕ-каналам должна соответствовать расходу В1ж "
                "с учётом нормы не менее 1-кратного воздухообмена от объёма квартир. "
                "Проверить: Σ(ПЕ1..ПЕ15) ≈ 28 540 м³/ч и соответствие количеству квартир."
            ),
            "norm": (
                "СП 54.13330.2022, приложение В (нормы воздухообмена жилых зданий); "
                "СП 60.13330.2020 п.7 (расчёт воздухообмена)"
            ),
            "needs_visual_check": True,
            "related_block_ids": [
                "DGYP-64U3-AAP", "996V-FFUN-F9V", "9WFC-WQ9K-VCE",
                "97DQ-T7Q3-UYK", "J3QH-FP6U-JUH", "4PDY-AMQP-4MU",
                "4U43-9CGD-XNJ", "7FGQ-Q74W-DTG"
            ]
        }
    ]

    # Update the data
    data['text_source'] = 'md+extracted_text'
    data['timestamp'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+03:00')
    data['note'] = (
        "Этап 01 выполнен по двум источникам: (1) extracted_text — прямое извлечение из PDF "
        "(144 стр.), (2) MD-файл (Chandra OCR) с анализом [IMAGE]-блоков. "
        "Основные находки T-001..T-011 получены из extracted_text; "
        "T-012..T-016 добавлены по MD-анализу аксонометрических схем. "
        "Блоки: 232 image-блока доступны в _output/blocks/."
    )

    # Add new findings
    existing_ids = {f['id'] for f in data.get('text_findings', [])}
    for nf in new_findings:
        if nf['id'] not in existing_ids:
            data['text_findings'].append(nf)

    # Set block lists
    data['blocks_for_review'] = blocks_for_review
    data['blocks_skipped'] = blocks_skipped
    data['blocks_note'] = (
        f"Блоки сформированы crop_blocks.py: {idx['total_blocks']} image-блоков. "
        f"Приоритет HIGH: {high_count} (системы ДВ/ДП/ПЕ, противодымная вентиляция), "
        f"MEDIUM: {medium_count} (планы, разрезы), "
        f"LOW+SKIP: {len(blocks_skipped)} (узлы крепления, графики, 3D-чертежи). "
        "Критичные группы: ПЕ1–ПЕ15 (огнезащита T-012), ДВ3/ДВ5 (дымостойкость T-004), "
        "ДП8 (охват этажей T-013)."
    )

    # Update pipeline status
    data['pipeline_status'] = {
        'stage_01_complete': True,
        'blocks_available': True,
        'total_blocks': idx['total_blocks'],
        'high_priority_blocks': high_count,
        'medium_priority_blocks': medium_count,
        'stage_02_ready': True,
        'blocker': None
    }

    # Save
    with open(STAGE01_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Done. Total findings: {len(data['text_findings'])}")
    print(f"Blocks for review: {len(blocks_for_review)}, Skipped: {len(blocks_skipped)}")


if __name__ == '__main__':
    main()
