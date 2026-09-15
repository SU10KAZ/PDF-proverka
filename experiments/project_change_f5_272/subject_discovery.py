"""Deterministic, answer-blind functional groups, not paragraph/number proposals.

The vocabulary describes engineering functions; it contains no document codes,
page maps, expected values or historical findings. A group is a retrieval subject,
not a claim that its equipment members or states are equivalent.
"""
import re
from collections import defaultdict

from .common import fingerprint

FUNCTIONS = {
    'supply_air': (r'приточ\w*', 'Приточная вентиляция', 'ENGINEERING_FUNCTION'),
    'extract_air': (r'вытяж\w*', 'Вытяжная вентиляция', 'ENGINEERING_FUNCTION'),
    'smoke_extract': (r'дымоудален\w*', 'Дымоудаление', 'SYSTEM'),
    'air_pressurization': (r'подпор\w*', 'Подпор воздуха', 'ENGINEERING_FUNCTION'),
    'heating': (r'отоплен\w*', 'Отопление', 'SYSTEM'),
    'cooling': (r'кондиционир\w*|холодоснабжен\w*', 'Кондиционирование и холодоснабжение', 'SYSTEM'),
    'heat_supply': (r'теплоснабжен\w*', 'Теплоснабжение', 'SYSTEM'),
    'air_exchange': (r'воздухообмен\w*', 'Воздухообмен', 'CALCULATION_CRITERION'),
    'automation': (r'автоматизац\w*|автоматическ\w* управлен\w*', 'Автоматизация', 'ENGINEERING_FUNCTION'),
    'acoustic': (r'шум\w*|шумоглуш\w*', 'Акустические требования', 'REQUIREMENT'),
    'fire_protection': (r'огнезащит\w*|огнестойк\w*|противопожарн\w*', 'Противопожарные решения', 'REQUIREMENT'),
    'floor_layout': (r'план(?:ы|а)?\s+(?:\d+[-\w ]*этаж|этаж|на\s+отм|подвал|кровл)|планиров\w*', 'Планировка', 'ROOM_ZONE'),
    'facade': (r'фасад\w*', 'Фасады', 'ENGINEERING_FUNCTION'),
    'section': (r'разрез\w*', 'Разрезы', 'NODE'),
    'roof': (r'кровл\w*|крыш\w*', 'Кровля', 'ENGINEERING_FUNCTION'),
    'walls': (r'перегород\w*|стен\w*', 'Стены и перегородки', 'EQUIPMENT_GROUP'),
    'openings': (r'двер\w*|окон\w*|витраж\w*|проем\w*|проём\w*', 'Заполнение проёмов', 'EQUIPMENT_GROUP'),
    'stairs': (r'лестниц\w*', 'Лестницы', 'NODE'),
    'evacuation': (r'эвакуац\w*', 'Эвакуация', 'ENGINEERING_FUNCTION'),
    'room_schedule': (r'экспликац\w*', 'Экспликация помещений', 'TABLE_ROW_GROUP'),
    'finishes': (r'отделк\w*|отделоч\w*', 'Отделка', 'ENGINEERING_FUNCTION'),
    'accessibility': (r'маломобильн\w*|\bмгн\b', 'Доступность', 'REQUIREMENT'),
    'water_supply': (r'водоснабжен\w*', 'Водоснабжение', 'SYSTEM'),
    'drainage': (r'водоотведен\w*|канализац\w*', 'Водоотведение', 'SYSTEM'),
    'power_supply': (r'электроснабжен\w*', 'Электроснабжение', 'SYSTEM'),
}


def scope_of(headings):
    text = ' '.join(headings).casefold()
    floors = sorted(set(re.findall(r'(?<!\d)(\d{1,2})\s*(?:[-–]\s*(?:го|й|ого))?\s*этаж', text)))
    zones = [name for term, name in [('подвал', 'BASEMENT'), ('кровл', 'ROOF'), ('чердак', 'ATTIC')]
             if term in text]
    return sorted(set(['FLOOR:' + n for n in floors] + zones)) or ['UNKNOWN']


def discover(inv):
    groups, unmatched = defaultdict(list), []
    for region in inv['regions']:
        headings = region['headings']
        text = (region['text'] + '\n' + '\n'.join(headings)).casefold()
        # Named heading scope is retained; a number in a table is never a scope.
        scope = tuple(scope_of(headings))
        found = [key for key, (pattern, _, _) in FUNCTIONS.items() if re.search(pattern, text)]
        if not found:
            unmatched.append(region['region_id'])
        for key in found:
            groups[(key, scope)].append(region)
    subjects = []
    for (key, scope), regions in sorted(groups.items()):
        _, description, kind = FUNCTIONS[key]
        sid = 's_' + fingerprint([inv['document_version'], key, scope])[:24]
        subjects.append(dict(subject_id=sid, document=inv['document'],
            document_version=inv['document_version'], side=inv['side'], subject_type=kind,
            functional_key=key, functional_description=description, scope=list(scope),
            pages=sorted({r['page'] for r in regions}), source_types=sorted({r['source_type'] for r in regions}),
            labels_marks=sorted({label for r in regions for label in r['labels']}),
            document_sections=sorted({h for r in regions for h in r['headings']}),
            supporting_evidence_refs=sorted({r['region_id'] for r in regions}),
            boundary_status='CANDIDATE_FUNCTIONAL_GROUP', identity_status='UNVERIFIED',
            provenance=dict(method='DOCUMENT_FUNCTION_AND_NAMED_SCOPE/5',
                            inventory_hash=fingerprint(inv))))
    return dict(schema='SUBJECT_INDEX/5', subjects=subjects,
                undiscovered_region_ids=unmatched,
                granularity='Functional subject + explicit named scope; marks remain group members',
                exhaustive=False)
