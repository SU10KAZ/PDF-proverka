"""Direct authoritative TEXT checks; no PDF extraction or graphic-summary evidence."""
import hashlib
import json
import math
import re
from pathlib import Path

WORK = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation')
OUT = Path(__file__).resolve().parent

def blocks(disc):
    path = WORK / 'sources' / disc / 'document.md'
    text = path.read_text()
    matches = list(re.finditer(r'^### BLOCK[^\n]*\[(\w+)\]:\s*(\S+)', text, re.M))
    result = []
    for i, m in enumerate(matches):
        end = matches[i+1].start() if i+1 < len(matches) else len(text)
        if m.group(1) == 'TEXT':
            result.append({'block_id': m.group(2), 'text': text[m.start():end], 'offset': m.start()})
    return path, hashlib.sha256(path.read_bytes()).hexdigest(), result

def save(name, disc, token, quote, data):
    path, sha, bb = blocks(disc)
    hits = [b for b in bb if token in b['text'] and quote in b['text']]
    assert len(hits) == 1, (disc, token, len(hits))
    b = hits[0]
    result = {'review_date':'2026-11-09', 'route':'authoritative_TEXT', 'source_md':str(path),
              'source_md_sha256':sha, 'block_id':b['block_id'], 'exact_quote':quote,
              'quote_offset':b['offset']+b['text'].index(quote),
              'fresh_model_calls':0, **data}
    (OUT/name).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')

quote = 'Нагреваемая сторона, трубопровод вентиляции.\n\nGвен макс = (Qвен макс × 1000)/(τ₁ - τ₂) = (1,527 × 1000)/(85 - 60) = 61,12 м³ / ч;\n\nВыбираем трубопровод: Ø219 × 6,0 мм. Δh=2,49 кгс/м² м, v=0,66 м/сек.'
v = (61.12/3600)/(math.pi*(.219-2*.006)**2/4)
assert abs(v-0.504487)<.000001
save('ITP72b_primary_TEXT.json','ITP','Нагреваемая сторона, трубопровод вентиляции.',quote,
     {'control_id':'ITP:72b','technical_status':'confirmed_internal_arithmetic_difference',
      'inputs':{'flow_m3h':61.12,'outer_diameter_mm':219,'wall_mm':6,'stated_velocity_ms':.66},
      'calculation':{'formula':'(Q/3600)/(pi*((D-2*t)/1000)^2/4)','internal_diameter_mm':207,'velocity_ms':v},
      'limits':'Проверка согласованности указанного объёмного расхода и диаметра. Не доказывает реальный режим, нарушение допустимой скорости или непригодность системы.',
      'engineer_decision':None,'historical_parent':'ITP:72; решение по производному тезису отдельно не зафиксировано'})
quote = '| 1 | Труба канализационная полипропиленовая бесшумная д50x0 | ГОСТ 32414-2013 |  | Pro Aqua | м | 5,2 |  |  |'
save('VK14_primary_TEXT.json','VK','д50x0',quote,
     {'control_id':'VK:14','technical_status':'confirmed_ambiguous_product_designation',
      'confirmed':'В СО буквально задано д50x0, количество 5,2 м, изготовитель Pro Aqua; код изделия отсутствует.',
      'required_semantics':'Уточнить типоразмер и толщину стенки именно этой позиции СО.',
      'limits':'Не утверждать изготовление физической трубы с нулевой стенкой, отсутствие труб или соответствие всем обозначениям на плане. Графическая часть исторического тезиса здесь не проверена.',
      'engineer_decision':None})
print('Two direct TEXT checks passed; graphics and historical engineer decisions unchanged.')
