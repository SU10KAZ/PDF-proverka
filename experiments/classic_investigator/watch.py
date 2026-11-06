"""Watch already-started pilot jobs; recover only documented transient JSON failures."""
import argparse
import html
import json
from pathlib import Path
import subprocess
import sys
import time

from evaluate import queue


def publish(work):
    states=[]
    for p in sorted((work/'runs').glob('*/status.json')):
        s=json.loads(p.read_text());states.append((p,s))
    rows=''.join('<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in (
        p.parent.name,s['status'],s['steps'],len(s['findings']),round(time.time()-s['started']) if s['status']=='running' else round(s['elapsed_s']),s.get('error') or ''))+'</tr>' for p,s in states)
    checked=work/'evaluation/primary_checks.json'
    primary=json.loads(checked.read_text()) if checked.exists() else []
    checks=''.join('<li>'+html.escape(c['atom']+': '+c['technical_status'])+'</li>' for c in primary)
    report='''<!doctype html><html lang="ru"><meta charset="utf-8"><meta http-equiv="refresh" content="30"><title>Пилот исследовательского прохода</title><style>body{font:16px system-ui;max-width:1200px;margin:40px auto}table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:8px}p{max-width:950px}</style><h1>Пилот ИТП и ЭОМ</h1><p>Кандидаты ещё требуют проверки. Их число не равно числу подтверждённых дефектов. Все исходники, запросы и результаты сохранены в этой папке. Продвижения в production нет.</p><table><tr><th>Проход</th><th>Состояние</th><th>Шаги / 24</th><th>Кандидаты</th><th>Секунды</th><th>Ошибка</th></tr>'''+rows+'</table><h2>Начатая независимая проверка</h2><ul>'+checks+'''</ul><p>Первые две подтверждённые арифметические темы ЭОМ уже были в A/B: это сохранение находок, а не прирост. По ГВС ИТП сумма зон требует учёта явно указанного коэффициента неравномерности.</p><p>Далее: проверка всех кандидатов, сравнение атомов с историей, сохранение полезных находок классики, свежий контрольный прогон, выбор варианта и регрессия на шести проектах. Переносимость проверяется отдельно на новых проектах.</p></html>'''
    target=work/'Статус_пилота.html';temp=target.with_suffix('.tmp');temp.write_text(report);temp.replace(target)
    return states


def main(work):
    recovering={}
    while True:
        states=publish(work)
        queue(work)
        for p,s in states:
            key=p.parent.name
            proc=recovering.get(key)
            if proc and proc.poll() is None:continue
            if s['status']=='failed' and 'claude_json_not_found' in (s.get('error') or '') and not (work/'STOP_REQUESTED.json').exists():
                # Failed request/response and completed steps are retained by --resume.
                discipline,provider=key.split('_',1)
                log=(p.parent/'recovery_supervisor.log').open('a')
                recovering[key]=subprocess.Popen([sys.executable,str(Path(__file__).with_name('investigate.py')),'run','--work',str(work),'--discipline',discipline,'--provider',provider,'--resume'],stdout=log,stderr=subprocess.STDOUT)
                log.close()
        if states and all(s['status']!='running' for _,s in states) and all(p.poll() is not None for p in recovering.values()):
            break
        time.sleep(30)
    publish(work);queue(work)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--work',type=Path,required=True);main(p.parse_args().work)
