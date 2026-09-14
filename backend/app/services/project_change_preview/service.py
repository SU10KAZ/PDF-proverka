"""ProjectChangeView adapter, version-bound evidence rasterization and preview API service."""
from __future__ import annotations

from collections import Counter
import copy
import math
from pathlib import Path
import threading

from .sources import FrozenSources, OBJECT, REPO, SourceUnavailable, canonical, event_identity, signature
from .decisions import ACTIONS, DecisionConflict, PreviewDecisions

BASE = '/api/project-change-preview/objects/' + OBJECT
TYPES = {'EQUIPMENT_REPLACED':'EQUIPMENT', 'EQUIPMENT_ADDED':'EQUIPMENT', 'EQUIPMENT_REMOVED':'EQUIPMENT',
    'EQUIPMENT_COUNT_CHANGED':'QUANTITY', 'SYSTEM_TYPE_CHANGED':'SYSTEM', 'SYSTEM_CONFIGURATION_CHANGED':'SYSTEM',
    'SYSTEM_MODE_CHANGED':'PARAMETERS', 'CAPACITY_CHANGED':'PARAMETERS', 'LAYOUT_CHANGED':'LAYOUT',
    'ROUTING_CHANGED':'LAYOUT', 'REQUIREMENT_CHANGED':'REQUIREMENT', 'ENGINEERING_SOLUTION_CHANGED':'SYSTEM'}
DISCIPLINES = {2:('АР','Архитектура'),5:('ИОС2.1','Водоснабжение'),6:('ИОС3.1','Водоотведение'),
    7:('ИОС4.1','Отопление'),8:('ИОС4.2','Вентиляция'),9:('ИОС4.3','Теплоснабжение'),
    10:('ИОС1.1','Электроснабжение'),13:('ОДИ','Доступность'),14:('ООС1','Охрана окружающей среды'),
    15:('ПЗ','Пояснительная записка'),16:('ПЗУ','Генеральный план'),19:('ИОС5.2','Связь'),21:('ИОС5.4','Связь')}
PROPERTIES = {'flow':'Расход','power':'Мощность','pressure':'Давление','temperature':'Температура',
    'flow_return_temperature':'Температурный график','diameter':'Диаметр','heat_load':'Тепловая нагрузка',
    'model':'Модель','quantity':'Количество','area':'Площадь','presence':'Наличие','count':'Количество'}
REASONS = {'OLD_SCOPE_NOT_ESTABLISHED':'Надёжная привязка к OLD не установлена. Отсутствие объекта пока не доказано.',
    'AMBIGUOUS_ENTITY':'Возможны несколько объектов. Не установлено, что OLD и NEW относятся к одной установке.',
    'NOT_FOUND_UNPROVEN':'На другой стороне не найдено надёжного соответствия.',
    'SAME_SUBJECT':'Проверьте, относятся ли фрагменты к одному инженерному объекту.'}


class PreviewService:
    def __init__(self, state_dir, sources=None):
        self.sources = sources or FrozenSources()
        self.state_dir = Path(state_dir).expanduser().resolve()
        forbidden = [REPO, self.sources.root, Path('/home/coder/auditmanager').resolve()]
        if any(self.state_dir.is_relative_to(p) for p in forbidden):
            raise ValueError('Preview state must be isolated from checkout, research and deployment roots')
        self.decisions = PreviewDecisions(self.state_dir / 'decisions.sqlite3')
        self._lock = threading.RLock()
        self._pages = {}
        self.evidence = {}
        self.items = []
        self._pairs = {}
        self._prepare()

    def _page(self, pair_id, side, number):
        import fitz
        p = self.sources.pairs.get(pair_id)
        if not p or side not in {'old','new'} or type(number) is not int or number < 1 or number in p['embargo_pages'][side]:
            raise SourceUnavailable('Страница не допущена для исследовательского просмотра')
        key = (pair_id,side,number)
        with self._lock:
            if key not in self._pages:
                with fitz.open(self.sources.documents[(pair_id,side)]) as doc:
                    if number > len(doc):
                        raise SourceUnavailable('Evidence page outside PDF')
                    page = doc[number-1]
                    self._pages[key] = {'width':page.rect.width,'height':page.rect.height,
                        'rotation':page.rotation,'page_count':len(doc)}
            return self._pages[key]

    def _evidence(self, raw, pair, side):
        import fitz
        path, pages = self.sources.evidence_binding(pair, side, raw)
        document = pair[side]
        loc = raw.get('locator') or {}
        output, keys = [], []
        for page in pages:
            dims = self._page(pair['pair_key'],side,page)
            box = loc.get('bbox_pdf_points') if loc.get('page') == page else None
            region = None
            if box:
                if not isinstance(box,list) or len(box)!=4 or any(type(x) not in {int,float} or not math.isfinite(x) for x in box):
                    raise SourceUnavailable('Invalid source region')
                rect = fitz.Rect(box)
                # Source coordinates are unrotated PDF points, raster coordinates
                # follow page rotation. There is no text-search guessed bbox.
                if dims['rotation']:
                    with fitz.open(path) as doc:
                        rect = rect * doc[page-1].rotation_matrix
                if (rect.is_empty or rect.is_infinite or rect.x0 < 0 or rect.y0 < 0
                        or rect.x1 > dims['width'] or rect.y1 > dims['height']):
                    raise SourceUnavailable('Source region outside PDF page')
                box = list(rect)
                region = {'units':'normalized','x':rect.x0/dims['width'],'y':rect.y0/dims['height'],
                    'width':rect.width/dims['width'],'height':rect.height/dims['height']}
            stable = {'side':side,'document':document['document_code'],'version':document['version_id'],
                'pdf_sha256':document['artifacts']['pdf']['sha256'],'page':page,'route':raw['route'],
                'quote_sha256':signature(raw.get('quote')),
                'source_refs':sorted([{k:r.get(k) for k in ['page','line_sha256','markdown_line','within_block_line']}
                    for r in raw.get('source_refs',[]) if r['page']==page],key=canonical),
                'locator':{k:loc.get(k) for k in ['kind','page','bbox_pdf_points','row_key','column_index','table_key','ledger_line','markdown_line'] if k in loc},
                'source_artifacts':{k:v['sha256'] for k,v in raw['source_receipts'].items()}}
            evidence_id = 'pev_' + signature(stable)
            e = {'id':evidence_id,'source_type':raw['route'],'side':side.upper(),
                'document':{'id':document['document_code'],'label':document['document_code'],
                    'version':document['version_id'],'pdf_path':str(path)},
                'pair_id':pair['pair_key'],'page':page,'region':region,
                'crop_precision':'EXACT_REGION' if region else 'PAGE_LEVEL',
                'image_url':f'{BASE}/evidence/{evidence_id}/crop',
                'short_explanation_ru':('Исходная редакция. ' if side=='old' else 'Новая редакция. ')
                    + ('Указанная в источнике область: проверьте значение и назначение объекта.' if region else
                       'Показана вся страница: точная область в provenance отсутствует. Сопоставьте описание объекта.'),
                'quote':raw.get('quote') or '', 'source_evidence_id':raw['evidence_id']}
            self.evidence[evidence_id] = {'view':e, 'path':path, 'box':box, 'sha256':stable['pdf_sha256']}
            output.append(e);keys.append(stable)
        return output, keys

    def _prepare(self):
        for raw,pair in self.sources.events:
            evidence,keys=[],[]
            for side in ['old','new']:
                for e in raw['evidence_'+side]:
                    views, stable = self._evidence(e,pair,side); evidence.extend(views); keys.extend(stable)
            subject=raw['engineering_subject'];m=self.sources.manifest
            identity=event_identity(raw,pair,keys,m['candidate_version'])
            cipher,discipline=DISCIPLINES[pair['index']]
            reasons=[REASONS[r] for r in raw.get('review_reasons',[]) if r in REASONS]
            details=[{'label':PROPERTIES.get(f['property'], 'Характеристика'),
                'old':f['old'].get('quote') or f['old'].get('value') or '',
                'new':f['new'].get('quote') or f['new'].get('value') or ''} for f in raw.get('supporting_fact_changes',[])]
            item={'id':raw['project_change_id'],'summary_ru':raw['short_summary_ru'],
                'change_type':TYPES.get(raw['change_type'],'OTHER'),'status':'REVIEW',
                'importance':'HIGH' if raw['importance']=='MATERIAL' else 'NORMAL',
                'cipher':cipher,'discipline':discipline,'engineering_system':subject.get('system') or '',
                'engineering_subject':subject.get('mark') or subject.get('equipment_class') or 'Объект требует уточнения',
                'old_state':raw.get('old_state') or '', 'new_state':raw.get('new_state') or '',
                'evidence':evidence,'details':details,'conflicts':[{'explanation_ru':'Источники противоречат друг другу.',
                    'resolved':c.get('status')=='RESOLVED','values':c.get('values',[])} for c in raw.get('conflicts',[])],
                'review_question':'Подтверждается ли это изменение по исходным документам?',
                'review_explanation_ru':' '.join(reasons) or 'Исследовательский вывод требует решения инженера. Проверьте один и тот же объект в OLD и NEW.',
                'technical_provenance':[f"run: {m['source_run_id']}",f"candidate: {m['candidate_version']} ({m['candidate_status']})",
                    f"research status: {raw['status']}",f"subject: {subject.get('entity_id')}",f"event: {raw['event_key']}",
                    *raw.get('review_reasons',[])],
                'source_run_id':m['source_run_id'],'candidate_version':m['candidate_version'],'research_status':raw['status'],
                **{k:v for k,v in identity.items() if k not in {'identity_payload','evidence_snapshot'}},
                '_evidence_snapshot':identity['evidence_snapshot']}
            self.items.append(item)
        counts=Counter(c['decision_key'] for c in self.items)
        for c in self.items:
            if counts[c['decision_key']]>1:
                c['identity_reusable']=False
        for key,p in self.sources.pairs.items():
            docs={}
            for side, name in [('old','left'),('new','right')]:
                doc=p[side]
                docs[name]={'pdf_path':str(self.sources.documents[(key,side)]),'filename':doc['document_code']+'.pdf',
                    'version_id':doc['version_id'],'document_code':doc['document_code']}
            self._pairs[key]={'id':key,**docs}

    def envelope(self, report_only=False):
        self.sources.assert_current()
        history,revision=self.decisions.read()
        items=[]
        for base in self.items:
            item={k:copy.deepcopy(v) for k,v in base.items() if not k.startswith('_')}
            effective=self.decisions.effective(base,history)
            item['decision_state']=effective['state']; item['effective_decision']=effective['record']
            item['status']=ACTIONS[effective['record']['decision']] if effective['record'] else 'REVIEW'
            if item['decision_state']=='STALE_DECISION':
                item['review_explanation_ru']='Предыдущее решение относится к другим или неоднозначным данным. Проверьте изменение заново.'
            if any(not c['resolved'] for c in item['conflicts']) and item['status']=='CONFIRMED':
                item['status']='CONFLICT'; item['effective_decision']=None
            elif any(not c['resolved'] for c in item['conflicts']) and item['status']=='REVIEW':
                item['status']='CONFLICT'
            if not report_only or item['status']=='CONFIRMED':
                items.append(item)
        pairs=list(self._pairs.values())
        return {'schema_version':'project-change-view/1','object_id':OBJECT,'origin':'RESEARCH',
            'mode':'BACKEND_PREVIEW','revision':self.sources.source_revision,'decision_revision':revision,
            'source_run_id':self.sources.manifest['source_run_id'],'candidate_version':self.sources.manifest['candidate_version'],
            'candidate_status':self.sources.manifest['candidate_status'],
            'capabilities':{'decisions':True,'history':True,'exports':False,'project_launch':False},
            'items':items,'summary':self.statistics(),
            'viewer_session':{'id':'pc-preview-'+self.sources.source_revision[:20],
                'documents':{'stage_1':[p['left'] for p in pairs],'stage_2':[p['right'] for p in pairs]},'pairs':pairs,
                'document_pairing':{'version':1,'left_order':[p['left']['pdf_path'] for p in pairs],
                    'right_order':[p['right']['pdf_path'] for p in pairs],
                    'confirmed_pairs':[{'left_pdf':p['left']['pdf_path'],'right_pdf':p['right']['pdf_path']} for p in pairs]}}}

    def statistics(self):
        ev=[e for item in self.items for e in item['evidence']]
        crops = {canonical([e['sha256'], e['view']['page'], e['box']]):e for e in self.evidence.values()}
        return {'project_changes':len(self.items),'research_statuses':dict(Counter(c['research_status'] for c in self.items)),
            'evidence_items':len(ev),'evidence_sources':dict(Counter(e['source_type'] for e in ev)),
            'unique_evidence':len(self.evidence),'unique_crops':len(crops),
            'crop_precision':dict(Counter(e['view']['crop_precision'] for e in crops.values()))}

    def decide(self, change_id, decision_key, binding_signature, action, actor, comment,
               expected_source_revision, expected_decision_revision):
        self.sources.assert_current()
        item=next((c for c in self.items if c['id']==change_id),None)
        if (not item or item['decision_key']!=decision_key or item['binding_signature']!=binding_signature
                or expected_source_revision!=self.sources.source_revision):
            raise DecisionConflict('Изменение или версия источников устарели. Обновите данные.')
        if action=='CONFIRM' and any(not c['resolved'] for c in item['conflicts']):
            raise DecisionConflict('Нельзя подтвердить изменение с открытым конфликтом источников.')
        self.decisions.append(item,action,actor,comment,expected_decision_revision)
        return self.envelope()

    def history(self, decision_key):
        self.sources.assert_current()
        item = next((c for c in self.items if c['decision_key']==decision_key), None)
        if not item:
            raise KeyError(decision_key)
        rows,_=self.decisions.read()
        return [{**r, 'same_decision_key': r['decision_key']==decision_key} for r in rows
                if r['decision_key']==decision_key or r['lineage_key']==item['lineage_key']]

    def pair(self, pair_id):
        self.sources.assert_current()
        if pair_id not in self._pairs: raise KeyError(pair_id)
        indexes={}
        for side in ['old','new']:
            pages=sorted({e['page'] for c in self.items for e in c['evidence']
                if e['pair_id']==pair_id and e['side']==side.upper()})
            indexes[side]=[{'pdf_page':p,'title':'Страница источника','sheet_number':str(p)} for p in pages]
        return {'pair':self._pairs[pair_id],
            'left_page_count':self._page(pair_id,'old',1)['page_count'],
            'right_page_count':self._page(pair_id,'new',1)['page_count'],
            # This is an evidence page inventory, NOT a new matching result.
            'sheet_matching':{'suggestions':{'status':'preview_evidence_inventory','suggestions':[],
                'left_sheet_index':indexes['old'],'right_sheet_index':indexes['new']},
                'links':{'links':[],'unlinked_left_pages':[]}}}

    def page_info(self,pair_id,side,page):
        self.sources.assert_current()
        d=self._page(pair_id,side,page)
        return {**d,'signature':self.sources.pairs[pair_id][side]['artifacts']['pdf']['sha256']}

    def crop(self,evidence_id):
        self.sources.assert_current()
        item=self.evidence.get(evidence_id)
        if not item: raise KeyError(evidence_id)
        return self._raster(item['path'],item['view']['page'],item['box'],1200,item['sha256'])

    def page_raster(self,pair_id,side,page,width=1400,tile=None):
        self.page_info(pair_id,side,page)
        path=self.sources.documents[(pair_id,side)]
        digest=self.sources.pairs[pair_id][side]['artifacts']['pdf']['sha256']
        return self._raster(path,page,None,width,digest,tile)

    def _raster(self,path,page,box,width,digest,tile=None):
        import fitz
        with self._lock:
            cache=self.state_dir/'crops'/f"{signature([digest,page,box,width,tile,'raster-v1'])}.png"
            if cache.exists(): return cache.read_bytes()
            with fitz.open(path) as doc:
                pdf_page=doc[page-1];clip=fitz.Rect(box) if box else pdf_page.rect
                scale=min(3,width/clip.width)
                if tile is not None:
                    level,x,y=tile;scale=2**level
                    clip=fitz.Rect(x*512/scale,y*512/scale,(x+1)*512/scale,(y+1)*512/scale)&pdf_page.rect
                    if clip.is_empty: raise SourceUnavailable('Tile outside page')
                data=pdf_page.get_pixmap(matrix=fitz.Matrix(scale,scale),clip=clip,alpha=False).tobytes('png')
            cache.parent.mkdir(parents=True,exist_ok=True)
            temporary=cache.with_suffix('.tmp');temporary.write_bytes(data);temporary.replace(cache)
            return data
