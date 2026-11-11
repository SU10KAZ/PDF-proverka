"""Export existing unresolved questions for engineering review; do not fill answers."""
import hashlib
import html
import json
import os
from pathlib import Path
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill

HERE=Path(__file__).parent


def main():
    source=HERE/'open_control_engineering_handoff.json'
    data=json.loads(source.read_text());rows=data['rows']
    wb=Workbook();ws=wb.active;ws.title='Вопросы инженеру'
    headers=['Контроль','Исходный контроль','Что требуется установить','Ответ инженера','Документ / редакция / лист / фрагмент','Обоснование','Имя проверяющего','Дата']
    ws.append(headers)
    evidence=wb.create_sheet('Доказательства');evidence.append(['Контроль','Путь','SHA256'])
    sections=[]
    for row in rows:
        assert row['engineering_answer'] is None
        ws.append([row['control_id'],row['original_control'],row['required_resolution'],None,None,None,None,None])
        links=[]
        for item in row['evidence']:
            p=Path(item['path']);actual=hashlib.sha256(p.read_bytes()).hexdigest()
            if actual!=item['sha256']:raise ValueError('Evidence changed: '+str(p))
            evidence.append([row['control_id'],str(p),actual])
            relative=os.path.relpath(p.resolve(),HERE.resolve())
            links.append('<li><a href="'+html.escape(relative,quote=True)+'">'+html.escape(p.name)+'</a></li>')
        sections.append('<article><h2>'+html.escape(row['control_id'])+'</h2><p><b>Исходный контроль:</b> '+html.escape(row['original_control'])+'</p><p><b>Нужное уточнение:</b> '+html.escape(row['required_resolution'])+'</p><details><summary>Доказательства</summary><ul>'+''.join(links)+'</ul></details><p class="pending">Ответ инженера не получен.</p></article>')
    guide=wb.create_sheet('Границы проверки')
    for text in ['Шесть незакрытых вопросов этапа 4. Это пакет разрешения споров, не слепое сравнительное испытание.',
                 'Заполните ответ, точный документ/редакцию/лист/фрагмент, обоснование, имя и дату. Если данных недостаточно — укажите, каких именно.',
                 'Не исправляйте исходный контроль. Разделяйте ошибку формулировки и фактическую верность её самостоятельных тезисов.',
                 'Не считать отсутствие подтверждения опровержением. Не считать число выносок числом физических экземпляров без связи.',
                 'Выбор раздела выдачи не меняет техническую истинность и не доказывает физические последствия.',
                 'Все поля ответа оставлены пустыми. Книга не содержит вымышленных инженерных решений.',
                 'Не передавать эту книгу независимым исполнителям поиска до фиксации их результатов.']:
        guide.append([text])
    for sheet in wb:
        sheet.freeze_panes='A2'
        for cell in sheet[1]:cell.font=Font(bold=True,color='FFFFFF');cell.fill=PatternFill('solid',fgColor='27445C')
        for row in sheet:
            for cell in row:cell.alignment=Alignment(vertical='top',wrap_text=True)
        sheet.auto_filter.ref=sheet.dimensions
    for col,width in zip('ABCDEFGH',[16,65,90,60,65,65,26,20]):ws.column_dimensions[col].width=width
    for i in range(2,ws.max_row+1):ws.row_dimensions[i].height=125
    evidence.column_dimensions['A'].width=16;evidence.column_dimensions['B'].width=115;evidence.column_dimensions['C'].width=70
    guide.column_dimensions['A'].width=125
    xlsx=HERE/'Незакрытые_вопросы_инженеру.xlsx';wb.save(xlsx)
    check=load_workbook(xlsx);assert check['Вопросы инженеру'].max_row==7
    for row in check['Вопросы инженеру'].iter_rows(min_row=2,min_col=4,max_col=8):assert all(c.value is None for c in row)
    page='''<!doctype html><html lang="ru"><meta charset="utf-8"><title>Незакрытые инженерные вопросы</title><style>body{max-width:1000px;margin:40px auto;padding:0 20px;font:17px/1.55 system-ui;color:#213547;background:#f6f8fa}article{background:white;padding:22px;margin:20px 0;border:1px solid #dce2e7;border-radius:10px}h2{margin-top:0}.pending{color:#805d12}a{color:#155c9d}li{overflow-wrap:anywhere}</style><h1>Шесть незакрытых вопросов этапа 4</h1><p>Нужны инженерные привязки и решения о профиле выдачи. Спорные случаи не засчитаны как успешные контроли. Полные сравнительные аудиты ещё не запущены.</p><p><a href="Незакрытые_вопросы_инженеру.xlsx">Скачать таблицу для ответа</a></p><p>Для ответа укажите документ, редакцию, лист или фрагмент и обоснование. Этот пакет предназначен для разрешения споров; не передавайте его исполнителям независимого поиска до фиксации их результатов.</p>'''+''.join(sections)+'</html>'
    output=HERE/'Незакрытые_вопросы_инженеру.html';output.write_text(page)
    receipt={'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'questions':len(rows),'evidence_refs_checked':evidence.max_row-1,'blank_engineer_answers_verified':True,'files':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [xlsx,output]},'model_calls':0,'stage4_complete':False}
    (HERE/'engineering_handoff_export.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(receipt,ensure_ascii=False))

if __name__=='__main__':main()
