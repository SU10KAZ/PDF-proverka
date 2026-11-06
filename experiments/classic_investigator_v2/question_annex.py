"""Reviewable deferred questions; never promote them into confirmed findings."""
import argparse
import copy
import hashlib
import html
import json
from pathlib import Path

from backend.app.pipeline.stages.block_analysis.finding_evidence_gate import gate_findings


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def build(candidates, source):
    if not isinstance(candidates, list) or any(not isinstance(c, dict) for c in candidates):
        raise ValueError('Expected candidate objects')
    tagged = []
    for index, candidate in enumerate(candidates):
        item = copy.deepcopy(candidate)
        if '_annex_source_index' in item:
            raise ValueError('Reserved provenance field in source')
        item['_annex_source_index'] = index
        tagged.append(item)
    published, deferred, gate_report = gate_findings(tagged, max_published=None)
    rows = []
    for item in deferred:
        index = item.pop('_annex_source_index')
        original = candidates[index]
        reasons = set(item['_evidence_gate']['reasons'])
        required = original.get('required_context')
        specific_question = (
            original.get('claim_type') == 'context_gap'
            and isinstance(required, list) and bool(required)
            and all(isinstance(s, str) and s.strip() for s in required)
            and bool(str(original.get('affected_entity') or '').strip())
            and reasons <= {'non_publishable_claim_type', 'insufficient_context', 'speculative_wording'}
        )
        rows.append({'source_index': index, 'original_candidate': copy.deepcopy(original),
                     'original_sha256': digest(original), 'gate_candidate': item,
                     'route': 'question_pending_review' if specific_question else 'other_deferred_review',
                     'technical_truth_verified': False, 'engineering_value_verified': False,
                     'published_as_defect': False})
    return {'source': copy.deepcopy(source), 'source_candidates_sha256': digest(candidates),
            'gate_report': gate_report, 'rows': rows,
            'published_source_indices': [c['_annex_source_index'] for c in published],
            'scope': 'Experimental review appendix. Exact candidate preservation and actual gate routing; '
                     'not a technical verdict, automatic customer publication or full pipeline replay.',
            'production_changed': False}


def render(result):
    parts = ['<!doctype html><meta charset="utf-8"><title>Отложенные вопросы</title>',
             '<style>body{font:17px/1.5 system-ui;max-width:1000px;margin:40px auto;padding:20px}'
             'pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f5f5f5;padding:16px}</style>',
             '<h1>Вопросы и другие отложенные кандидаты</h1>',
             '<p>Это приложение для проверки. Вопросы не являются доказанными дефектами; '
             'их техническая верность и ценность ещё не подтверждены автоматически. '
             'Цитаты ниже — цитаты, заявленные моделью, а не результат независимой проверки.</p>']
    for route, title in [('question_pending_review', 'Вопросы по недостающему контексту'),
                         ('other_deferred_review', 'Другие кандидаты: требуется отдельное решение')]:
        parts.append('<h2>' + title + '</h2>')
        rows = [r for r in result['rows'] if r['route'] == route]
        if not rows:
            parts.append('<p>Нет записей.</p>')
        for row in rows:
            parts.append('<h3>Исходная запись ' + str(row['source_index'] + 1) + '</h3>')
            parts.append('<pre>' + html.escape(json.dumps(row, ensure_ascii=False, indent=2)) + '</pre>')
    parts.append('<h2>Источник и результат фильтра</h2><pre>' + html.escape(json.dumps(
        {'source': result['source'], 'gate_report': result['gate_report']}, ensure_ascii=False, indent=2)) + '</pre>')
    return ''.join(parts)


def run(response, output, block_id, page, sheet):
    if output.exists() or output.with_suffix('.html').exists():
        raise ValueError('Appendix exists; use a new version path')
    raw_bytes = response.read_bytes()
    raw = json.loads(raw_bytes)
    if raw.get('error'):
        raise ValueError('Failed model response')
    source = {'response': str(response), 'response_sha256': hashlib.sha256(raw_bytes).hexdigest(),
              'block_id': block_id, 'page': page, 'sheet': sheet,
              'model': raw.get('usage', {}).get('model'),
              'gate_implementation_sha256': hashlib.sha256(Path(
                  gate_findings.__code__.co_filename).read_bytes()).hexdigest()}
    result = build(raw['json']['findings'], source)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    output.with_suffix('.html').write_text(render(result))
    print(result['gate_report'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--response', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--block-id', required=True)
    parser.add_argument('--page', type=int, required=True)
    parser.add_argument('--sheet', required=True)
    args = parser.parse_args()
    run(args.response, args.output, args.block_id, args.page, args.sheet)
