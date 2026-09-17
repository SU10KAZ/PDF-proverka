"""Render frozen experiment/evaluation artifacts; never invoke a model."""
from collections import Counter
import json
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from .run import OUT, read, write, sha, now


def workbook(tabs):
    book = Workbook()
    book.remove(book.active)
    for name, rows in tabs.items():
        sheet = book.create_sheet(name[:31])
        if not rows:
            rows = [{'status':'NO_ROWS'}]
        keys = list(dict.fromkeys(k for r in rows for k in r))
        sheet.append(keys)
        for row in rows:
            vals = []
            for key in keys:
                v = row.get(key)
                if isinstance(v, (dict,list)):
                    v = json.dumps(v,ensure_ascii=False)
                if isinstance(v,str):
                    v = v[:32760]
                    if v.startswith(('=','+','-','@')):
                        v = "'" + v
                vals.append(v)
            sheet.append(vals)
        sheet.freeze_panes='A2'
        sheet.auto_filter.ref=sheet.dimensions
        for cell in sheet[1]:
            cell.font=Font(bold=True,color='FFFFFF')
            cell.fill=PatternFill('solid',fgColor='24476A')
        for col in sheet.columns:
            sheet.column_dimensions[col[0].column_letter].width=min(70,max(18,len(str(col[0].value))+2))
            for cell in col:
                cell.alignment=Alignment(vertical='top',wrap_text=True)
    target=OUT/'RESULTS.xlsx'
    if target.exists():
        raise FileExistsError(target)
    book.save(target)


def stopped():
    stops=[read(p) for p in sorted(OUT.glob('STOP_*.json'))]
    assert stops
    calls=list((OUT/'raw').glob('*/INVOCATION.json'))
    receipts=[read(p) for p in sorted((OUT/'raw').glob('*/RECEIPT.json'))]
    reason=stops[-1]['error']
    status=dict(status='NOT_COMPLETED', reason=reason, model='gpt-6-astra', reasoning='xhigh',
        model_calls=len(calls), completed_calls=sum(bool(r['usage']) for r in receipts),
        openrouter=0, claude=0, evaluation='NOT_RUN', proven10='NOT_OPENED', f13='NOT_OPENED',
        production='UNCHANGED', validation='NOT OPENED', final_holdout='NOT OPENED',
        retries=0, repairs=0)
    write(OUT/'RUN_STATUS.json', status)
    for name in ['DOCUMENT_MAP.json','DOCUMENT_MAP_FREEZE.json','CHANGE_MINER_RESULTS.json',
                 'CHANGE_MINER_FREEZE.json','PROVEN10_EVALUATION.json','F13_CHECK.json',
                 'FALSE_POSITIVE_AUDIT.json']:
        if not (OUT/name).exists():
            write(OUT/name,dict(status='NOT_CREATED' if 'FREEZE' in name else 'NOT_RUN',
                               reason=reason, artifact_is_placeholder=True))
    write(OUT/'BASELINE_VS_AI_MAPPING.json',dict(baseline_final_accept=0, baseline_proven_total=10,
        baseline_source='user-supplied; previous truth not opened', ai_found=None,
        hypothesis='NOT_EVALUATED', reason=reason))
    text=f'''STATUS: NOT_COMPLETED

MODEL: gpt-6-astra / xhigh
MODEL CALLS: {len(calls)} (completed: {status['completed_calls']})
OpenRouter: 0; Claude: 0

Причина остановки: {reason}
Повторных запусков и исправления результата не было.

MAPPING GROUPS: NOT_AVAILABLE
OLD PAGES MAPPED: NOT_AVAILABLE / 108
NEW PAGES MAPPED: NOT_AVAILABLE / 188
CONCRETE CHANGES: NOT_AVAILABLE
UNRESOLVED HINTS: NOT_AVAILABLE
PROVEN10: NOT_EVALUATED (NOT_OPENED)
F13: NOT_EVALUATED (NOT_OPENED)
FALSE CHANGES: NOT_EVALUATED
CURRENT PIPELINE PROVEN FOUND: 0/10 (из задания пользователя)
AI-MAPPING PIPELINE FOUND: NOT_EVALUATED
HYPOTHESIS: NOT_EVALUATED

ARCHITECTURE CONCLUSION:
Эксперимент не завершён, поэтому вывод о гипотезе сделать нельзя.
Источник остановки сохранён в raw и STOP; неподтверждённые результаты не посчитаны нулями.

PRODUCTION: UNCHANGED
VALIDATION: NOT OPENED
FINAL HOLDOUT: NOT OPENED

Подготовлено 294 разрешённых страницы. OLD p4 и NEW p8 исключены по frozen split.
Это исторически DEV-known пара; опыт не является исторически слепым тестом.
Файлы с artifact_is_placeholder=true обозначают невыполненные этапы, а не реальные freeze.
'''
    write(OUT/'FINAL_REPORT.md',text)
    workbook({'Status':[status], 'Baseline':[read(OUT/'BASELINE_VS_AI_MAPPING.json')],
              'Calls':[dict(path=str(p),**read(p)) for p in calls], 'Stop':stops})
    write(OUT/'DELIVERY_MANIFEST.json',dict(at=now(),hashes={p.name:sha(p) for p in OUT.iterdir() if p.is_file()}))


if __name__=='__main__':
    stopped()
