"""Local content alignment → typed evidence → guarded engineering events.

No page/section-relation gate, model provider, table comparator or graphic route.
Unexplained residual wording and uncertain identity fail closed to REVIEW.
"""
from collections import Counter, defaultdict
from decimal import Decimal
import re
from experiments.text_comparison_v1.common import digest
from experiments.text_comparison_v1.facts import MATERIAL, MODE, FIRE
from experiments.text_alignment_v2.facts import MODAL
from .contract import validate

MARK = re.compile(r'\b(?:ПВ|П|В|ДВ|ДУ|ПД|ВРУ|ГРЩ|ФК|Н|FCU|AHU|CH)[-]?\d+(?:[.-]\d+)*\b', re.I)
MODEL = re.compile(r'\bмодел[ьи]\s+[«"\']?(?P<value>[a-zа-я][a-zа-я\d_./-]*\d[a-zа-я\d_./-]*|[a-z][a-z\d_./-]*)', re.I)
PIPE = re.compile(r'(?P<n>\d+|двух|трех|четырех)(?:\s*[-–]?\s*[хx])?\s*[-–]?\s*трубн\w*', re.I)
CLASSES = {'фанкойл': 'фанкойл', 'насос': 'насос', 'вентилятор': 'вентилятор',
           'холодильн': 'холодильная машина', 'чиллер': 'чиллер', 'драйкул': 'драйкулер',
           'котел': 'котел', 'котл': 'котел', 'светильник': 'светильник', 'трансформатор': 'трансформатор'}
COUNT = re.compile(r'(?P<n>\d+)\s+(?P<entity>холодильн\w*\s+машин\w*|чиллер\w*|драйкул\w*|фанкойл\w*|насос\w*|вентилятор\w*|котл\w*|светильник\w*|трансформатор\w*)', re.I)
NUMBER = r'[+−-]?(?:\d{1,3}(?:\s\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?)'
UNIT = r'(?:квт|kw|вт|w)\s*/\s*[мm][²2]|[мm][³3]\s*/\s*(?:сут(?:ки)?|day|ч|h|с|s)|мпа|mpa|кпа|kpa|па|pa|квт|kw|вт|w|[мm][²2]|[мm][³3]|мм|mm|кг|kg|°\s*[cс]|%|м|m'
QUANTITY = re.compile(rf'(?<![\w.,])(?P<n>{NUMBER})\s*(?P<u>{UNIT})(?![\w/])', re.I)
UNITS = {'квт': ('W', 1000), 'kw': ('W', 1000), 'вт': ('W', 1), 'w': ('W', 1),
         'вт/м2': ('W/m2', 1), 'w/m2': ('W/m2', 1), 'квт/м2': ('W/m2', 1000), 'kw/m2': ('W/m2', 1000),
         'м3/ч': ('m3/h', 1), 'm3/h': ('m3/h', 1), 'м3/с': ('m3/h', 3600), 'm3/s': ('m3/h', 3600),
         'м3/сут': ('m3/day', 1), 'м3/сутки': ('m3/day', 1), 'm3/day': ('m3/day', 1),
         'па': ('Pa', 1), 'pa': ('Pa', 1), 'кпа': ('Pa', 1000), 'kpa': ('Pa', 1000),
         'мпа': ('Pa', 1000000), 'mpa': ('Pa', 1000000), 'м2': ('m2', 1), 'm2': ('m2', 1),
         'м3': ('m3', 1), 'm3': ('m3', 1), 'мм': ('mm', 1), 'mm': ('mm', 1),
         'м': ('m', 1), 'm': ('m', 1), 'кг': ('kg', 1), 'kg': ('kg', 1),
         '°с': ('C', 1), '°c': ('C', 1), '%': ('%', 1)}
PROPERTY = re.compile(r'мощност\w*|расход\w*|нагрузк\w*|потребност\w*|напор\w*|давлен\w*|площад\w*|масс\w*|ширин\w*|высот\w*|длин\w*|температур\w*|производительност\w*', re.I)
LABELS = {'model': 'модель', 'pipe_type': 'трубная конфигурация', 'count': 'количество',
          'mode': 'режим', 'material': 'материал', 'requirement': 'требование', 'fire': 'огнестойкость'}


def canonical(text):
    text = re.sub(r'\*{1,2}([^*]+)\*{1,2}', r'\1', text)
    text = re.sub(r'^\s*[-•]\s+', '', text)
    return re.sub(r'\s+', ' ', text.casefold().replace('ё', 'е')).strip().rstrip('.;')


def analyze(text):
    raw = canonical(text)
    slots = []

    def add(a, b, prop, value, unit=None):
        if any(x['span'][0] < b and x['span'][1] > a for x in slots):
            return
        slots.append(dict(span=[a, b], property=prop, value=value, unit=unit, quote=raw[a:b]))

    for m in MODEL.finditer(raw):
        add(*m.span('value'), 'model', m['value'])
    for m in PIPE.finditer(raw):
        n = {'двух': '2', 'трех': '3', 'четырех': '4'}.get(m['n'], m['n'])
        add(*m.span(), 'pipe_type', n, 'pipe')
    for m in COUNT.finditer(raw):
        add(*m.span('n'), 'count', m['n'], 'count')
    for m in QUANTITY.finditer(raw):
        u = re.sub(r'\s+', '', m['u']).replace('²', '2').replace('³', '3')
        if u not in UNITS:
            continue
        # Reject partial unit parsing, ranges and ratios (including daily-flow
        # suffixes). Signed scalars are accepted only outside those contexts.
        before, after = raw[:m.start()], raw[m.end():]
        if re.search(r'\d\s*[/–−-]\s*$', before) or re.match(r'\s*/', after):
            continue
        # A malformed thousands group must not become a tail scalar. Adjacent
        # standalone numerals without a delimiter also need interpretation.
        if re.search(r'(?<![\w])\d+(?:[.,]\d+)?\s+$', before):
            continue
        unit, factor = UNITS[u]
        v = format((Decimal(re.sub(r'\s+', '', m['n']).replace(',', '.').replace('−', '-')) * factor).normalize(), 'f')
        labels = list(PROPERTY.finditer(before[max(0, len(before)-65):]))
        prop = labels[-1].group() if labels else 'quantity'
        add(*m.span(), prop + ':' + unit, v, unit)
    for regex, prop in ((MATERIAL, 'material'), (MODE, 'mode'), (FIRE, 'fire'), (MODAL, 'requirement')):
        for m in regex.finditer(raw):
            add(*m.span(), prop, m.group())
    slots.sort(key=lambda s: s['span'])
    template = raw
    for s in reversed(slots):
        a, b = s['span']
        template = template[:a] + '<' + s['property'] + '>' + template[b:]
    classes = sorted({value for key, value in CLASSES.items() if re.search(r'\b' + key, raw)})
    marks = sorted(set(MARK.findall(raw)))
    room = re.search(r'помещени\w*\s+([\w.-]+)', raw)
    floor = re.search(r'(?:этаж\w*\s+([\d-]+)|([\d-]+)\s*[-]?\s*(?:м|й)?\s+этаж)', raw)
    system = re.search(r'\b(?:систем[аы]\s+)?(кондиционирован\w*|вентиляци\w*|холодоснабжен\w*|теплоснабжен\w*|отоплен\w*|водоснабжен\w*)', raw)
    return dict(raw=raw, template=template, slots=slots, marks=marks, classes=classes,
                room=room[1] if room and re.search(r'\d', room[1]) else None,
                floor=next((g for g in floor.groups() if g), None) if floor else None,
                system=system[1] if system else None,
                ambiguous=len(classes) > 1 or len(marks) > 1 or bool(re.search(r'�|\$|\\frac', raw)))


def titles(unit):
    return sorted({canonical(s['title']) for s in unit.get('section_context', [])})


def evidence(unit):
    return dict(evidence_id='ev_' + digest([unit['document_version'], unit['unit_id']])[:24],
                route='TEXT', document_version=unit['document_version'], document_code=unit['document_code'],
                quote=unit['text'], source_refs=unit['source_refs'], source_receipts=unit['source_receipts'],
                local_unit_id=unit['unit_id'], section_titles=titles(unit), locator=None,
                text_purity_basis=unit['text_purity_basis'])


def entity(a, b, scope, context):
    common = b or a
    explicit = len(common['marks']) == 1 and len(common['classes']) <= 1
    ambiguous = common['ambiguous'] or bool(a and a['ambiguous'])
    # Full assertion preserves engineering function, qualifications and location
    # when no explicit mark exists. A class or a page alone never resolves identity.
    key = [scope, context, common['marks'], common['classes'], common['room'], common['floor'],
           common['system'], None if explicit else common['template']]
    # A singular, explicitly named building property can repeat in several
    # sections with a different explanatory tail. Preserve operating conditions
    # and parenthetical qualifiers, but omit symbolic variable spelling and
    # downstream commentary. Never apply this to generic equipment classes,
    # lists, rooms, plural objects, or ambiguous/numeric multi-property sentences.
    subject_core = object_property_core(common)
    if subject_core:
        key = [scope, 'EXPLICIT_OBJECT_PROPERTY', subject_core]
    models_a = [s['value'] for s in (a or {}).get('slots', []) if s['property'] == 'model']
    models_b = [s['value'] for s in (b or {}).get('slots', []) if s['property'] == 'model']
    return dict(entity_id='entity_' + digest(key)[:24], system=common['system'],
                equipment_class=common['classes'][0] if len(common['classes']) == 1 else None,
                mark=common['marks'][0] if explicit else None,
                equipment_model_old=models_a[0] if len(models_a) == 1 else None,
                equipment_model_new=models_b[0] if len(models_b) == 1 else None,
                room=common['room'], floor=common['floor'], engineering_function=None,
                semantic_subject=subject_core or common['template'],
                scope_key=digest([scope, 'EXPLICIT_OBJECT_PROPERTY'] if subject_core else [scope, context]),
                identity_basis=['EXPLICIT_OBJECT_PROPERTY' if subject_core else 'EXPLICIT_MARK_AND_LOCAL_ASSERTION' if explicit else 'COMPLETE_LOCAL_ASSERTION',
                                'SECTION_CONTEXT' if context else 'LOCAL_CONTENT_CONTEXT'],
                resolution='AMBIGUOUS' if ambiguous else 'EXPLICIT' if explicit else 'LOCAL')


def object_property_core(a):
    quantities = [s for s in a['slots'] if ':' in s['property']]
    if len(quantities) != 1 or len(a['slots']) != 1 or a['marks'] or a['ambiguous']:
        return None
    split = re.search(r'\b(?:составляет|составит|равна|равен|равно)\b', a['raw'])
    if not split or split.start() >= quantities[0]['span'][0]:
        return None
    core = a['raw'][:split.start()].strip()
    if not PROPERTY.search(core) or not re.search(r'\b(?:здания|жилого дома|сооружения|корпуса)\b', core):
        return None
    if re.search(r'\b(?:и|или)\b|[,;]', core):
        return None
    prefix = a['raw'][split.end():quantities[0]['span'][0]].strip()
    if re.fullmatch(r'[a-zа-я][a-zа-я0-9_]{0,5}\s*[=–-]', prefix):
        prefix = ''
    tail = a['raw'][quantities[0]['span'][1]:]
    conditions = re.findall(r'\([^)]*\)', tail)
    # Unparenthesized conditions cannot be discarded as explanatory commentary.
    residual = re.sub(r'\([^)]*\)', '', tail)
    if re.search(r'\b(?:при|для|в режиме|летн\w*|зимн\w*)\b', residual.split(', что')[0]):
        return None
    return core + (' ' + prefix if prefix else '') + (' ' + ' '.join(conditions) if conditions else '')


def event_type(prop, slot):
    if prop == 'model': return 'EQUIPMENT_REPLACED'
    if prop == 'pipe_type': return 'SYSTEM_TYPE_CHANGED'
    if prop == 'count': return 'EQUIPMENT_COUNT_CHANGED'
    if prop == 'mode': return 'SYSTEM_MODE_CHANGED'
    if prop in ('requirement', 'fire'): return 'REQUIREMENT_CHANGED'
    if prop == 'material': return 'ENGINEERING_SOLUTION_CHANGED'
    if slot['unit'] in ('W', 'W/m2', 'm3/h', 'm3/day'): return 'CAPACITY_CHANGED'
    if slot['unit'] == 'm2': return 'LAYOUT_CHANGED'
    return 'OTHER_ENGINEERING_CHANGE'


def summary(kind, ent, facts, proven):
    subject = ent['mark'] or ent['equipment_class'] or ent['system']
    if not proven:
        return 'Проверить изменение: ' + (subject or 'инженерное утверждение') + '.'
    f = next((x for x in facts if x['property'] == 'model'), facts[0])
    old, new = f['old']['quote'], f['new']['quote']
    if kind == 'EQUIPMENT_REPLACED':
        return f'Заменена модель оборудования {subject or "локального узла"}: {old} → {new}.'
    label = LABELS.get(f['property'], f['property'].split(':')[0])
    if kind == 'SYSTEM_TYPE_CHANGED':
        return f'Изменена трубная конфигурация ({subject or "система"}): {old} → {new}.'
    if kind == 'EQUIPMENT_COUNT_CHANGED':
        return f'Изменено количество оборудования ({subject or "локальный узел"}): {old} → {new}.'
    return f'Изменение «{label}» ({subject or "локальное требование"}): {old} → {new}.'


def make_change(scope, old, new, a, b, facts, reasons, context, kind=None):
    ent = entity(a, b, scope, context)
    review = list(reasons)
    if ent['resolution'] == 'AMBIGUOUS': review.append('AMBIGUOUS_ENTITY')
    if any(f['property'] == 'model' for f in facts) and not ent['equipment_class']:
        review.append('MODEL_WITHOUT_EQUIPMENT_CLASS')
    proven = not review and bool(facts) and bool(old and new)
    kind = kind or (event_type(facts[0]['property'], facts[0]['new']) if facts else 'OTHER_ENGINEERING_CHANGE')
    semantic_states = sorted([(f['property'], f['old']['value'], f['new']['value'], f['new']['unit']) for f in facts])
    if kind == 'EQUIPMENT_REPLACED':
        semantic_states = [s for s in semantic_states if s[0] == 'model']
    key = digest([scope, ent['entity_id'], kind, semantic_states,
                  None if proven else [(a or {}).get('raw'), (b or {}).get('raw')]])
    return dict(project_change_id='pc_' + digest(key)[:24], event_key=key, comparison_scope=scope,
                change_type=kind, engineering_subject=ent,
                old_state=old['text'] if old else None, new_state=new['text'] if new else None,
                short_summary_ru=summary(kind, ent, facts, proven),
                importance='MATERIAL' if proven and kind != 'OTHER_ENGINEERING_CHANGE' else 'UNASSESSED',
                scope='EQUIPMENT' if ent['mark'] else 'SYSTEM' if ent['system'] else 'LOCAL_ASSERTION',
                status='PROVEN' if proven else 'REVIEW', confidence='HIGH' if proven else 'LOW',
                evidence_old=[evidence(old)] if old else [], evidence_new=[evidence(new)] if new else [],
                supporting_fact_changes=facts, decision_reasons=['EXACT_TYPED_LOCAL_ASSERTION'] if facts else [],
                review_reasons=sorted(set(review)), routes=['TEXT'], conflicts=[])


def compare(scope, old_units, new_units):
    aa = [(u, analyze(u['text'])) for u in old_units]
    bb = [(u, analyze(u['text'])) for u in new_units]
    by_template = defaultdict(list)
    new_templates = defaultdict(list)
    exact = defaultdict(list)
    for i, (u, a) in enumerate(aa):
        exact[a['raw']].append(i)
        if a['slots']: by_template[a['template']].append(i)
    for j, (u, b) in enumerate(bb):
        if b['slots']: new_templates[b['template']].append(j)
    changes, relations, raw_count, used = [], [], 0, set()
    for j, (new, b) in enumerate(bb):
        if not b['slots']: continue
        candidates = by_template[b['template']]
        # Prefer contextual identity without requiring a SectionRelation object.
        contexts = [i for i in candidates if set(titles(aa[i][0])) & set(titles(new))]
        if contexts: candidates = contexts
        variants = {aa[i][1]['raw'] for i in candidates}
        new_peers = [k for k in new_templates[b['template']] if titles(bb[k][0]) == titles(new)]
        new_ambiguous = len({bb[k][1]['raw'] for k in new_peers}) > 1
        if b['raw'] in exact and len(variants) <= 1 and not new_ambiguous:
            matches = [i for i in exact[b['raw']] if not titles(new) or titles(aa[i][0]) == titles(new)]
            if matches:
                used.update(matches)
                relations.append(dict(new_unit_id=new['unit_id'], old_unit_ids=[aa[i][0]['unit_id'] for i in matches], decision='UNCHANGED'))
                continue
        if len(variants) != 1:
            if not (b['classes'] or b['system'] or PROPERTY.search(b['raw'])):
                continue
            reason = 'AMBIGUOUS_OLD_STATES' if variants else 'OLD_SCOPE_NOT_ESTABLISHED'
            c = make_change(scope, None, new, None, b, [], [reason], titles(new))
            changes.append(c)
            relations.append(dict(new_unit_id=new['unit_id'], old_unit_ids=[], decision=reason))
            continue
        i = candidates[0]
        old, a = aa[i]
        context = sorted(set(titles(old)) & set(titles(new)))
        review = []
        if new_ambiguous: review.append('AMBIGUOUS_NEW_STATES')
        if len({tuple(titles(aa[k][0])) for k in candidates}) > 1:
            review.append('AMBIGUOUS_SCOPE')
        # Exact typed subject plus stable marks, or a unique unchanged adjacent
        # sentence, permits local alignment even without a section match.
        neighbor = False
        for offset in (-1, 1):
            x, y = i + offset, j + offset
            if 0 <= x < len(aa) and 0 <= y < len(bb):
                if aa[x][1]['raw'] == bb[y][1]['raw'] and len(exact[aa[x][1]['raw']]) == 1:
                    neighbor = True
        mark_scope = a['marks'] == b['marks'] and len(a['marks']) == 1
        if not (context or mark_scope or neighbor): review.append('LOCAL_SCOPE_UNCORROBORATED')
        if a['ambiguous'] or b['ambiguous']: review.append('AMBIGUOUS_ENTITY')
        deltas = []
        for index, (x, y) in enumerate(zip(a['slots'], b['slots'])):
            if (x['property'], x['unit']) != (y['property'], y['unit']):
                review.append('TYPED_SLOT_MISMATCH')
                continue
            if x['value'] != y['value']:
                data = dict(property=x['property'], old={k:x[k] for k in ('value', 'quote', 'unit')},
                            new={k:y[k] for k in ('value', 'quote', 'unit')},
                            evidence_old=[evidence(old)['evidence_id']], evidence_new=[evidence(new)['evidence_id']])
                data['fact_id'] = 'fact_' + digest([scope, old['unit_id'], new['unit_id'], index, data])[:24]
                deltas.append(data)
        raw_count += len(deltas)
        used.update(candidates)
        relations.append(dict(new_unit_id=new['unit_id'], old_unit_ids=[aa[k][0]['unit_id'] for k in candidates],
                              decision='TYPED_ALIGNMENT', scope_basis=context or ['EXPLICIT_MARK' if mark_scope else 'ADJACENT_CONTENT' if neighbor else 'UNRESOLVED']))
        replacement = [f for f in deltas if f['property'] == 'model']
        # Only parameters of a locally single equipment subject are replacement
        # consequences. Independent mode/count/material/requirements stay events.
        consumed = set()
        if len(replacement) == 1:
            attached = [f for f in deltas if f['property'] == 'model' or ':' in f['property']]
            changes.append(make_change(scope, old, new, a, b, attached, review, context, 'EQUIPMENT_REPLACED'))
            consumed.update(f['fact_id'] for f in attached)
        for f in deltas:
            if f['fact_id'] not in consumed:
                changes.append(make_change(scope, old, new, a, b, [f], review, context))
    # Preserve all old occurrences used as same-state witnesses, even when the
    # fact delta uses a single local old quote as its immediate support.
    old_lookup = {u['unit_id']:u for u in old_units}
    witnesses = {r['new_unit_id']:r['old_unit_ids'] for r in relations if r['decision']=='TYPED_ALIGNMENT'}
    for c in changes:
        for ev in c['evidence_new']:
            for uid in witnesses.get(ev['local_unit_id'], []):
                e = evidence(old_lookup[uid])
                if e['evidence_id'] not in {x['evidence_id'] for x in c['evidence_old']}:
                    c['evidence_old'].append(e)
    grouped = {}
    merges = 0
    for c in changes:
        key = c['event_key']
        if key not in grouped:
            grouped[key] = c
            continue
        merges += 1
        existing = grouped[key]
        for field, id_field in (('evidence_old', 'evidence_id'), ('evidence_new', 'evidence_id'), ('supporting_fact_changes', 'fact_id')):
            values = {x[id_field]: x for x in existing[field] + c[field]}
            existing[field] = [values[k] for k in sorted(values)]
        existing['decision_reasons'] = sorted(set(existing['decision_reasons'] + ['SAME_ENTITY_EVENT_AND_STATES']))
    result = sorted(grouped.values(), key=lambda c:c['project_change_id'])
    for c in result: validate(c)
    return dict(changes=result, alignments=relations,
                metrics=dict(raw_fact_differences=raw_count, pre_dedup_candidates=len(changes),
                             project_changes=len(result), proven=sum(c['status']=='PROVEN' for c in result),
                             review=sum(c['status']=='REVIEW' for c in result), duplicate_occurrences_merged=merges,
                             unmatched_old_units=len(aa)-len(used), old_units=len(aa), new_units=len(bb),
                             alignment_decisions=dict(Counter(r['decision'] for r in relations))))
