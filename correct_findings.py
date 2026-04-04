import json, shutil, os

base = r'D:\Отдел Системного Анализа\1. Audit Manager\projects\EOM\133_23-ГК-ГРЩ\_output'
src = os.path.join(base, '03_findings.json')
bak = os.path.join(base, '03_findings_pre_review.json')

shutil.copy2(src, bak)
print('Backup created:', os.path.exists(bak))

with open(src, 'r', encoding='utf-8') as f:
    data = json.load(f)

findings = data['findings']
fmap = {f['id']: f for f in findings}

# F-005: weak_evidence, narrow_evidence
f005 = fmap['F-005']
f005['related_block_ids'] = ['WXXU-CGL3-QFM']
f005['description'] = (
    'В спецификации оборудования (стр. 15) для позиции «ЩУ-2/Т» указан тип ШУ-12/Т '
    '(шкаф на 12 счетчиков, 2000\u00d71000\u00d7220), а для позиции «ЩУ-12/Т» указан тип ШУ-2Т '
    '(шкаф на два счетчика, 600\u00d7600\u00d7220). Числовое обозначение в наименовании позиции '
    'не совпадает с типоразмером шкафа: позиция «ЩУ-2/Т» описывает шкаф на 12 счётчиков, '
    'а позиция «ЩУ-12/Т» — шкаф на 2 счётчика. Подтверждено по спецификации в текстовом блоке.'
)

# F-006: weak_evidence, narrow_evidence
f006 = fmap['F-006']
f006['description'] = (
    'Для ВРУ-ИТП ввод №2 (зима) на схеме РП2 (стр. 7) указан коэффициент спроса '
    'Кс = 0.128172588832487 — необработанное значение из расчётной таблицы (15 знаков после запятой). '
    'Такая точность нехарактерна для проектной документации и свидетельствует о переносе значения '
    'из расчёта без округления. Подтверждено на чертеже блока A99X-NV3N-KRM.'
)
f006['problem'] = 'Необработанный коэффициент спроса Кс для ВРУ-ИТП (зима) — 0.128172588832487 (15 знаков после запятой)'

# F-007: weak_evidence, narrow_evidence
f007 = fmap['F-007']
f007['evidence_text_refs'] = []
f007['description'] = (
    'На схеме вводов ГРЩ (блок 96HR-4TP4-WYD, стр. 7) указана расчётная полная мощность '
    'Sр = 1027,80 кВА и два трансформатора по 1000 кВА. Расчётная мощность превышает номинальную '
    'мощность одного трансформатора на 2,78%. В проекте отсутствует расчёт допустимой аварийной '
    'перегрузки трансформатора по ГОСТ 14209-97.'
)
f007['related_block_ids'] = ['96HR-4TP4-WYD']

# F-008: weak_evidence, downgrade_severity
f008 = fmap['F-008']
f008['severity'] = 'РЕКОМЕНДАТЕЛЬНОЕ'
f008['description'] = (
    '[Critic: слабое evidence] Вводные автоматические выключатели 1QF1 и 2QF1 — ВА-731 на 2000А '
    '(блок 96HR-4TP4-WYD, стр. 7). Номинальный ток трансформатора 1000 кВА при 0,4 кВ составляет '
    '~1443А. Коэффициент превышения 2000/1443 = 1,39. Для подтверждения вывода о недостаточной '
    'защите трансформатора необходимы данные об уставках расцепителей и кривых срабатывания, '
    'которых нет в доступных блоках. Рекомендуется проверить координацию защит.'
)

# F-017: weak_evidence, narrow_evidence
f017 = fmap['F-017']
f017['evidence'] = [e for e in f017['evidence'] if e['block_id'] != 'EPND-MTHD-WXT']
f017['evidence_text_refs'] = [r for r in f017['evidence_text_refs'] if r['text_block_id'] != 'EPND-MTHD-WXT']
f017['related_block_ids'] = [b for b in f017['related_block_ids'] if b != 'EPND-MTHD-WXT']
f017['description'] = (
    'На однолинейной схеме (блок 4XPY-EUKK-QYR, стр. 7) присутствует обозначение ВРУ-П1 (ПЭСПЗ), '
    'которое не встречается в остальных частях документа, где данная линия обозначена как ВРУ-4 (Паркинг). '
    'Необходимо уточнить: является ли ВРУ-П1 (ПЭСПЗ) тем же ВРУ-4, или это отдельное ВРУ для систем '
    'противопожарной защиты автостоянки. Если это отдельное ВРУ — оно отсутствует в таблице нагрузок.'
)

# Recount severities
severities = {}
for f in findings:
    s = f['severity']
    severities[s] = severities.get(s, 0) + 1
print('Recounted severities:', severities)

data['meta']['by_severity'] = severities
data['meta']['total_findings'] = len(findings)
data['meta']['review_applied'] = True
data['meta']['review_stats'] = {
    'total_reviewed': 17,
    'passed': 12,
    'fixed': 4,
    'removed': 0,
    'downgraded': 1
}

with open(src, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print('Done. Total findings:', len(findings))
