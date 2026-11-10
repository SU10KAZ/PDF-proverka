"""Loopback-only real Vue/table fixture; synthetic data, no production/API writes."""
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path

REPO=Path(__file__).resolve().parents[2]
FRONT=REPO/'frontend'
index=(FRONT/'index.html').read_text()
start=index.index('<table class="findings-table"')
table=index[start:index.index('</table>',start)+len('</table>')]
app=(FRONT/'static/js/app.js').read_text()
logic=app[app.index('        const findingsHaveQuestions ='):app.index('        const findingsTotalPages =')]
setup=r'''
const {createApp,ref,computed}=Vue;
createApp({setup(){
const mode=ref('mixed'),expertReviewMode=ref(false),decisions=ref({}),reasons=ref({});
const source=[
{id:'F-Q2',report_section:'questions',severity:'РЕКОМЕНДАТЕЛЬНОЕ',sheet:'4',description:'Уточнить ссылку на лист схемы',solution:'Согласовать ссылку'},
{id:'F-T2',report_section:'technical_findings',severity:'СУЩЕСТВЕННОЕ',sheet:'7',description:'Подтверждённое расхождение отметок',solution:'Согласовать отметки'},
{id:'F-Q1',report_section:'questions',severity:'РЕКОМЕНДАТЕЛЬНОЕ',sheet:'1',description:'Уточнить обозначение в ведомости',solution:'Уточнить обозначение'},
{id:'F-L1',severity:'СУЩЕСТВЕННОЕ',sheet:'2',description:'Сохранённое классическое замечание',solution:'Устранить расхождение'}];
const sortedFindings=computed(()=>mode.value==='empty'?[]:mode.value==='questions'?source.filter(f=>f.report_section==='questions'):mode.value==='legacy'?source.filter(f=>!f.report_section):source);
const findingsData=computed(()=>({total:sortedFindings.value.length}));
const getExpertDecision=id=>decisions.value[id]||'';
const setExpertDecision=(id,kind,value)=>{decisions.value[id]=value};
const getExpertReason=id=>reasons.value[id]||'';
const setExpertReason=(id,value)=>{reasons.value[id]=value};
'''+logic+r'''
return {mode,expertReviewMode,findingsData,paginatedFindings,findingSectionHeading,
getExpertDecision,setExpertDecision,getExpertReason,setExpertReason,
expandedFindingId:null,findingBlockMap:{},findingTextEvidence:{},canOperate:true,
sevClass:s=>s==='СУЩЕСТВЕННОЕ'?'major':'recommendation',findingDetectorBadge:()=>null,
findingExtRegBadge:()=>null,findingMigratedBadge:()=>null,findingNormReferences:()=>[],
findingNormBadge:()=>null,
isCarriedOver:()=>false,toggleFindingBlocks:()=>{},cleanSubProblem:s=>s};
}}).mount('#app');
'''
html=('''<!doctype html><html lang="ru"><meta charset="utf-8"><link rel="icon" href="data:,"><title>Проверка разделов AuditManager</title>
<link rel="stylesheet" href="/styles.css"><style>body{display:block;padding:24px}#app{display:block}.controls{display:flex;gap:10px;margin:16px 0}.controls button{padding:10px}</style>
<script src="/vue.js"></script><body><div id="app"><h1>Проверка разделов AuditManager</h1>
<p>Локальный стенд: настоящий шаблон таблицы, синтетические данные. Решения хранятся только в этой вкладке.</p>
<div class="controls"><button @click="mode='mixed'">Смешанный список</button><button @click="mode='questions'">Только вопросы</button><button @click="mode='legacy'">Без вопросов</button><button @click="mode='empty'">Пусто</button><button @click="expertReviewMode=!expertReviewMode">Экспертный режим</button></div>
<div class="table-container">'''+table+'''</div><p v-if="!findingsData.total">Пустой список</p></div><script>'''+setup+'''</script></body></html>''').encode()
assets={'/':('text/html; charset=utf-8',html),'/vue.js':('text/javascript',(FRONT/'static/js/vue.global.prod.js').read_bytes()),'/styles.css':('text/css',(FRONT/'static/css/styles.css').read_bytes())}
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in assets:
            self.send_error(404);return
        kind,body=assets[self.path];self.send_response(200);self.send_header('Content-Type',kind);self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass

if __name__=='__main__':
    server=HTTPServer(('127.0.0.1',0),Handler)
    manifest={'url':f'http://127.0.0.1:{server.server_port}/','source_hashes':{str(p.relative_to(REPO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [FRONT/'index.html',FRONT/'static/js/app.js',FRONT/'static/css/styles.css',FRONT/'static/js/vue.global.prod.js']},
        'table_sha256':hashlib.sha256(table.encode()).hexdigest(),'logic_sha256':hashlib.sha256(logic.encode()).hexdigest(),
        'scope':'Real table template, section computed logic, Vue and CSS; synthetic rows; ancillary badges/evidence and persistence stubbed.',
        'production_accessed':False,'project_data_sent':False}
    Path('/tmp/auditmanager_question_browser_fixture_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    print(json.dumps({'url':manifest['url']}),flush=True)
    server.serve_forever()
