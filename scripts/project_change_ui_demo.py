"""Local-only ProjectChange UI preview using admitted 272 DEV evidence.

python scripts/project_change_ui_demo.py --port 8973
No production service, data, decisions or algorithms are written or invoked.
"""
import argparse
import hashlib
import json
import mimetypes
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from experiments.project_change_272.policy import admitted_pairs  # noqa: E402
from experiments.project_change_272.inventory import ROOT, OBJECT  # noqa: E402

SELECTED = {
    9: ['pc_860ec69a2f5aa936d16f0419'],
    6: ['pc_469b116d59e345a236d57ec8', 'pc_09301812f46f5f83391485b3'],
    13: ['pc_7988dac35db9055438e508f2'],
}
META = {
    9: ('ИОС4.3', 'Теплоснабжение'), 6: ('ИОС3.1', 'Водоотведение'), 13: ('ОДИ', 'Доступность'),
}


def build_fixture(directory):
    import fitz
    directory.mkdir(parents=True, exist_ok=True)
    pairs = {p['index']: p for p in admitted_pairs('DEV') if p['index'] in SELECTED}
    bindings, changes, metadata, sources, pair_data, receipts = {}, [], {}, {}, {}, []
    for index, ids in SELECTED.items():
        pair = pairs[index]
        artifact = ROOT / f'runs/dev_union_v1/pairs/{index}.json'
        raw = json.loads(artifact.read_text())
        receipts.append({'artifact': str(artifact), 'sha256': hashlib.sha256(artifact.read_bytes()).hexdigest()})
        selected = [next(c for c in raw['project_changes'] if c['project_change_id'] == cid) for cid in ids]
        changes.extend(selected)
        docs, handles, allowed_pages = {}, {}, {'left': set(), 'right': set()}
        for side, key in [('old', 'left'), ('new', 'right')]:
            doc = pair[side]
            pdf = doc['artifacts']['pdf']
            handles[side] = fitz.open(pdf['path'])
            docs[key] = {'pdf_path': pdf['path'], 'filename': doc['document_code'] + '.pdf',
                         'version_id': doc['version_id'], 'document_code': doc['document_code']}
            sources[(pair['pair_key'], key)] = {'path': pdf['path'], 'embargo': pair['embargo_pages'][side]}
        for c in selected:
            metadata[c['project_change_id']] = {'cipher': META[index][0], 'discipline': META[index][1]}
            for side, key in [('old', 'left'), ('new', 'right')]:
                for e in c['evidence_' + side]:
                    if e['source_receipts']['pdf']['sha256'] != pair[side]['artifacts']['pdf']['sha256']:
                        raise ValueError('Evidence is not from the admitted source version')
                    locator = e.get('locator') or {}
                    pages = sorted({r['page'] for r in e['source_refs']} | ({locator['page']} if locator.get('page') else set()))
                    if not pages:
                        raise ValueError('Fixture evidence must have a page')
                    bindings[e['evidence_id']] = []
                    for page_number in pages:
                        if page_number in pair['embargo_pages'][side]:
                            raise PermissionError('Embargoed evidence')
                        allowed_pages[key].add(page_number)
                        page = handles[side][page_number - 1]
                        box = locator.get('bbox_pdf_points')
                        if box:
                            clip = fitz.Rect(box) + (-12, -12, 12, 12)
                        else:
                            quote = ' '.join((e.get('quote') or '').split())
                            hits = page.search_for(quote) if quote else []
                            if not hits and quote:
                                # Research OCR uses Latin T/C; PDF has Cyrillic Т/С.
                                hits = page.search_for(quote.translate(str.maketrans({'T': 'Т', 'C': 'С'})))
                            if hits:
                                clip = fitz.Rect(hits[0])
                                for hit in hits[1:]:
                                    clip |= hit
                                clip += (-20, -15, 20, 15)
                            else:
                                clip = page.rect
                        clip &= page.rect
                        name = f"{e['evidence_id']}-{page_number}.png"
                        scale = min(3, 900 / max(clip.width, 1))
                        page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False).save(directory / name)
                        region = None if clip == page.rect else {'units': 'normalized',
                            'x': clip.x0 / page.rect.width, 'y': clip.y0 / page.rect.height,
                            'width': clip.width / page.rect.width, 'height': clip.height / page.rect.height}
                        explanation = ('Исходное состояние: ' if side == 'old' else 'Новая редакция: ') + (
                            'аннотация контура, проверьте параметры.' if e['route'] == 'GRAPHIC' else
                            'ячейка таблицы расходов; единицы и заголовок важны для сравнения.' if e['route'] == 'TABLE' else
                            'текстовое описание объекта; сопоставьте назначение и значение.')
                        if c['project_change_id'] == SELECTED[6][0]:
                            explanation = 'Описание насоса в NEW. Соответствующий источник OLD не установлен; проверьте, является ли это заменой.'
                        bindings[e['evidence_id']].append({'document': {'id': pair[side]['document_code'],
                            'label': META[index][0], 'version': pair[side]['version_id'], 'pdf_path': docs[key]['pdf_path']},
                            'pair_id': pair['pair_key'], 'page': page_number, 'region': region,
                            'image_url': '/static/project-change-demo/' + name, 'short_explanation_ru': explanation})
        # Only source-backed page references are fixture rows. Confidence is
        # explicitly demo UI state, never a new matcher/truth result.
        left_pages, right_pages = sorted(allowed_pages['left']), sorted(allowed_pages['right'])
        links = [{'left_pages': [a], 'right_pages': [b], 'source': 'manual', 'confidence': 'manual'}
                 for a, b in zip(left_pages, right_pages)]
        sheet_index = lambda pages: [{'pdf_page': p, 'title': f'Фрагмент источника · стр. {p}', 'sheet_number': str(p)} for p in pages]
        pair_data[pair['pair_key']] = {'pair': {'id': pair['pair_key'], **docs},
            'left_page_count': len(handles['old']), 'right_page_count': len(handles['new']),
            'sheet_matching': {'suggestions': {'status': 'ready', 'suggestions': [],
                'left_sheet_index': sheet_index(left_pages), 'right_sheet_index': sheet_index(right_pages)},
                'links': {'links': links, 'unlinked_left_pages': []}}}
        for handle in handles.values():
            handle.close()
    metadata[SELECTED[9][0]].update({
        'summary_ru': 'Изменены параметры контура теплоснабжения T12/T22',
        'old_state': 'Контур T12/T22: температурный график 95/70 °C; расход 87,49 м³/ч.',
        'new_state': 'Контур T12/T22: температурный график 90/65 °C; расход 44,7 м³/ч.',
        'review_question': 'Подтверждается ли изменение параметров контура T12/T22?',
        'review_explanation_ru': 'Сверьте обозначение контура и его назначение. Текст и аннотации схем показаны отдельно; каждый фрагмент относится к исходному PDF.'})
    metadata[SELECTED[6][0]].update({'summary_ru': 'Возможно изменён дренажный насос',
        'engineering_system': 'Дренажная канализация', 'engineering_subject': 'Дренажный насос TMT 32M113/7,5Ci',
        'review_question': 'Это замена существующего насоса или уточнение документации?',
        'review_explanation_ru': 'В NEW найдены два описания насоса. Надёжная привязка к OLD отсутствует, поэтому утверждать замену пока нельзя.'})
    revision = hashlib.sha256(json.dumps(receipts, sort_keys=True).encode()).hexdigest()[:16]
    context = {'object_id': OBJECT, 'partition': 'DEV', 'revision': revision, 'metadata': metadata, 'bindings': bindings,
        'property_labels': {'flow': 'Расход', 'flow_return_temperature': 'Температурный график',
            'temperature': 'Температура', 'pressure': 'Давление', 'diameter': 'Диаметр',
            'heat_load': 'Тепловая нагрузка', 'area': 'Площадь'}}
    input_path = directory / 'research-adapter-input.json'
    input_path.write_text(json.dumps({'changes': changes, 'context': context}, ensure_ascii=False))
    adapter = REPO / 'frontend/static/js/project-change-view.js'
    code = "const fs=require('fs');const a=require(process.argv[1]);const d=JSON.parse(fs.readFileSync(process.argv[2]));process.stdout.write(JSON.stringify(a.fromResearch(d.changes,d.context)));"
    envelope = json.loads(subprocess.check_output(['node', '-e', code, str(adapter), str(input_path)]))
    documents = {'stage_1': [v['pair']['left'] for v in pair_data.values()], 'stage_2': [v['pair']['right'] for v in pair_data.values()]}
    session = {'id': 'project-change-ui-local-demo-272', 'documents': documents,
        'pairs': [v['pair'] for v in pair_data.values()], 'project_change_presentation': envelope,
        'document_pairing': {'version': 1, 'left_order': [d['pdf_path'] for d in documents['stage_1']],
            'right_order': [d['pdf_path'] for d in documents['stage_2']],
            'confirmed_pairs': [{'left_pdf': v['pair']['left']['pdf_path'], 'right_pdf': v['pair']['right']['pdf_path']} for v in pair_data.values()]}}
    fixture = {'session': session, 'pairs': pair_data, 'sources': sources}
    (directory / 'source-receipts.json').write_text(json.dumps({'origin': 'RESEARCH', 'partition': 'DEV',
        'baseline': 'logical v002', 'receipts': receipts, 'note': 'UI fixture; no truth or matcher output created.'}, ensure_ascii=False, indent=2))
    (directory / 'presentation.json').write_text(json.dumps(envelope, ensure_ascii=False, indent=2))
    return fixture


def serve(port, directory):
    import fitz
    fixture = build_fixture(directory)
    lock = threading.Lock()
    raster_cache = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def send(self, value, status=200, kind='application/json'):
            data = value if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header('Content-Type', kind)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass  # Browser cancelled an obsolete page/tile request.

        def do_POST(self):
            path = unquote(urlsplit(self.path).path)
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))) or '{}')
            if path == '/api/stage-comparison/sessions':
                return self.send(fixture['session'])
            if path.endswith('/pairs'):
                pair = next((p for p in fixture['pairs'].values() if p['pair']['left']['pdf_path'] == body.get('left_pdf')
                    and p['pair']['right']['pdf_path'] == body.get('right_pdf')), None)
                return self.send(pair or {'detail': 'Fixture pair not found'}, 200 if pair else 404)
            self.send({'detail': 'Local evidence preview is read-only'}, 405)

        def do_PUT(self):
            self.send({'detail': 'Local evidence preview is read-only'}, 405)

        def do_GET(self):
            parsed = urlsplit(self.path)
            path, query = unquote(parsed.path), parse_qs(parsed.query)
            if path in ['/', '/index.html']:
                return self.send((REPO / 'frontend/index.html').read_bytes(), kind='text/html; charset=utf-8')
            if path.startswith('/static/'):
                base = directory if path.startswith('/static/project-change-demo/') else REPO / 'frontend/static'
                relative = path.removeprefix('/static/project-change-demo/') if base == directory else path.removeprefix('/static/')
                file = (base / relative).resolve()
                if file.is_relative_to(base.resolve()) and file.is_file():
                    return self.send(file.read_bytes(), kind=mimetypes.guess_type(file.name)[0] or 'application/octet-stream')
                return self.send({}, 404)
            if path == '/api/objects':
                return self.send({'objects': [{'id': OBJECT, 'name': '272. Садовническая 76 / Балчуг Эстейт'}], 'current_id': OBJECT})
            if path.startswith('/api/project-change-preview/'):
                return self.send({'detail': 'Backend bridge is not this legacy demo server'}, 404)
            if path == '/api/stage-comparison/objects':
                return self.send({'items': [{'id': OBJECT, 'name': 'Садовническая 76 / Балчуг Эстейт', 'stages': [
                    {'name': 'stage_1', 'path': 'demo/272/stage_1', 'pdf_count': 3},
                    {'name': 'stage_2', 'path': 'demo/272/stage_2', 'pdf_count': 3}]}]})
            if '/pairs/' in path:
                pair_id, _, action = path.split('/pairs/', 1)[1].partition('/')
                if pair_id not in fixture['pairs']:
                    return self.send({}, 404)
                if not action:
                    return self.send(fixture['pairs'][pair_id])
                if action in ['page-info', 'page-preview', 'page-thumb', 'page-tile']:
                    side = query.get('side', ['left'])[0]
                    source = fixture['sources'].get((pair_id, side))
                    page_number = int(query.get('page', ['1'])[0])
                    if not source or page_number in source['embargo']:
                        return self.send({'detail': 'Source page is not admitted'}, 403)
                    with lock, fitz.open(source['path']) as doc:
                        if not 1 <= page_number <= len(doc):
                            return self.send({}, 404)
                        page = doc[page_number - 1]
                        if action == 'page-info':
                            return self.send({'width': page.rect.width, 'height': page.rect.height, 'signature': 'v002-demo'})
                        cache_key = self.path
                        if cache_key not in raster_cache:
                            if action == 'page-tile':
                                level = min(6, max(0, int(query.get('level', ['0'])[0])))
                                scale = 2 ** level
                                x, y = int(query.get('x', ['0'])[0]), int(query.get('y', ['0'])[0])
                                clip = fitz.Rect(x * 512 / scale, y * 512 / scale, (x+1)*512/scale, (y+1)*512/scale) & page.rect
                            else:
                                scale = min(2000, int(query.get('width', ['1400'])[0])) / page.rect.width
                                clip = page.rect
                            if clip.is_empty:
                                return self.send({}, 404)
                            raster_cache[cache_key] = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False).tobytes('png')
                        return self.send(raster_cache[cache_key], kind='image/png')
                if action == 'text-search':
                    return self.send({'pages': [], 'results': [], 'total_matches': 0, 'has_text_layer': False})
                return self.send({})
            if path.startswith('/api/'):
                return self.send({'items': [], 'projects': [], 'users': [], 'disciplines': [], 'auth_enabled': False})
            self.send({}, 404)

    print(f'Local UI preview: http://127.0.0.1:{port}/?projectChangeUi=1#/stage-comparison', flush=True)
    print(f'Fixture sources and receipts: {directory}', flush=True)
    ThreadingHTTPServer(('127.0.0.1', port), Handler).serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8973)
    parser.add_argument('--directory', type=Path, default=Path('/tmp/project-change-ui-272'))
    args = parser.parse_args()
    try:
        serve(args.port, args.directory)
    except KeyboardInterrupt:
        pass
