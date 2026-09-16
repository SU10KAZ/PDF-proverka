"""Source-only broad-context inventory and the user's pre-inference stop rule.

One existing broad retrieval candidate supplies one context seed, never a final
engineering subject. Whole indexed pages expand its context, without modality
matching or source-truth access. No inference capability is imported here.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

from experiments.project_change_272.policy import admitted_pairs

REPO = Path(__file__).resolve().parents[2]
ROOT = Path('/home/coder/auditmanager/corpus-audits/20260914_project_change_272')
BASE = ROOT / 'fresh_dev_sample_f5_pipeline_v6'
OUT = ROOT / 'pair_b_semantic_decomposer_v1'
PAIR_KEY = 'caea6d2810c334ec0368de8e'
LIMIT = 12
POLICY = {
    'grouping': 'ONE_EXISTING_BROAD_RETRIEVAL_CANDIDATE_PER_CONTEXT',
    'expansion': 'ALL_INDEXED_REGIONS_ON_REFERENCED_PAGES',
    'seed_subject_authority': 'COARSE_CONTEXT_ONLY_NOT_ENGINEERING_IDENTITY',
    'empty_contexts': 'RETAIN_AND_REPORT',
    'modality_filter': None,
    'merge_to_fit_budget': False,
    'truth_used': False,
}


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    data = value if isinstance(value, str) else json.dumps(
        value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + '\n'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as stream:
        stream.write(data)


def budget_status(count):
    return 'STOP_BROAD_CONTEXT_LIMIT' if count > LIMIT else 'INVENTORY_WITHIN_LIMIT'


class SourceGuard:
    """Deny non-allowlisted corpus reads, baseline writes and all network calls."""
    def __init__(self, allowed, output):
        self.allowed = {Path(p).resolve() for p in allowed}
        self.output = Path(output).resolve()
        self.reads = set()
        self.denied = []

    def check(self, event, args):
        if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
            self.denied.append(event)
            raise PermissionError('Source-only preflight prohibits external calls')
        if event != 'open' or not isinstance(args[0], (str, bytes, Path)):
            return
        path = Path(os.fsdecode(args[0])).resolve()
        if path.is_relative_to(self.output):
            return
        sensitive = path.is_relative_to(ROOT.parent) or path.is_relative_to(REPO / 'projects_v2')
        if not sensitive:
            return
        flags = args[2] if len(args) > 2 and isinstance(args[2], int) else 0
        writing = bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
        if writing or path not in self.allowed:
            self.denied.append(str(path))
            raise PermissionError('Preflight source allowlist rejected: ' + str(path))
        self.reads.add(str(path))


def bundle(package, inventories, source_file):
    """Produce locators, not semantic conclusions, model prompts or verdicts."""
    candidate = package['candidate_subject']
    sides = {}
    for side in ('old', 'new'):
        inv = inventories[side]
        delivered = package['evidence_packet']['evidence'][side]
        pages = sorted({e['page'] for e in delivered})
        page_rows = {p['page']: p for p in inv['pages']}
        if any(page_rows[p]['status'] != 'INDEXED' for p in pages):
            raise ValueError('Context references excluded page')
        regions = [r for r in inv['regions'] if r['page'] in pages]
        sides[side] = {
            'document_version': inv['document_version'],
            'pages': pages,
            'page_context': [page_rows[p] for p in pages],
            'region_refs': [dict(region_id=r['region_id'], page=r['page'],
                                 source_type=r['source_type'], bbox_norm=r['bbox_norm'])
                            for r in regions],
            'evidence_refs': [dict(evidence_id=e['evidence_id'], page=e['page'],
                                   route=e['route'], bbox=e.get('bbox'), raster=e.get('raster'))
                              for e in delivered],
            'raw_pdf_pages': pages,
            'raw_pdf_delivered_to_model': False,
            'missing_side': not pages,
        }
    return {
        'bundle_id': 'b_' + candidate['candidate_id'].removeprefix('c_'),
        'source_candidate_id': candidate['candidate_id'],
        'coarse_heading': candidate['subject'],
        'subject_identity_confirmed': False,
        'source_file': str(source_file),
        'source_sha256': sha(source_file),
        'old': sides['old'], 'new': sides['new'],
        'empty': not any(sides[s]['pages'] for s in sides),
        'candidate_cardinality': '0..N_AFTER_AI_ONLY',
        'page_correspondence': 'NOT_ASSERTED_BY_INVENTORY',
    }


def main():
    if OUT.exists():
        raise FileExistsError('Immutable experiment already exists: ' + str(OUT))
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--short'], cwd=REPO, text=True).splitlines()
    pair = admitted_pairs('DEV', indices=[8])[0]
    if (pair['pair_key'], pair['old']['structure']['pages'], pair['new']['structure']['pages']) != (PAIR_KEY, 108, 188):
        raise ValueError('Pair B identity drift')
    paths = sorted((BASE / 'pair_8/packages').glob('*.json'))
    inventory_paths = {s: BASE / f'pair_8/DOCUMENT_INVENTORY_{s.upper()}.json' for s in ('old', 'new')}
    v4_receipt = ROOT / 'fresh_dev_pair_a_post_inference_repair_v4/REPAIR_CODE_FREEZE.json'
    sources = [Path(a['path']) for s in ('old', 'new') for a in pair[s]['artifacts'].values()]
    metadata = [ROOT / name for name in ('SPLIT.json', 'SPLIT.sha256', 'INVENTORY.json', 'OVERLAP_AUDIT.json')]
    guard = SourceGuard([*paths, *inventory_paths.values(), v4_receipt, *sources, *metadata], OUT)
    sys.addaudithook(guard.check)
    inventories = {s: read(p) for s, p in inventory_paths.items()}
    inputs = {str(p): sha(p) for p in [*paths, *inventory_paths.values(), v4_receipt, *sources, *metadata]}
    v4_codes = read(v4_receipt)['code']
    for relative, digest in v4_codes.items():
        if sha(REPO / relative) != digest:
            raise ValueError('Frozen post-inference V4 code drift: ' + relative)
    bundles = []
    for path in paths:
        package = read(path)
        if package['provenance']['pair_index'] != 8 or package['provenance']['pair_key'] != PAIR_KEY:
            raise ValueError('Foreign package')
        for side in ('old', 'new'):
            for evidence in package['evidence_packet']['evidence'][side]:
                if evidence['document_version'] != pair[side]['document_version'] or evidence['side'] != side:
                    raise ValueError('Evidence version/side mismatch')
                if evidence['source_receipt'] != pair[side]['artifacts']['pdf']:
                    raise ValueError('Evidence PDF mismatch')
                if evidence.get('raster'):
                    raster = (BASE / evidence['raster']['path']).resolve()
                    if not raster.is_relative_to(BASE / 'rasters'):
                        raise ValueError('Foreign raster path')
                    guard.allowed.add(raster)
                    digest = sha(raster)
                    if digest != evidence['raster']['sha256']:
                        raise ValueError('Raster drift')
                    inputs[str(raster)] = digest
        bundles.append(bundle(package, inventories, path))
    ids = [b['bundle_id'] for b in bundles]
    if not bundles or len(set(ids)) != len(ids):
        raise ValueError('Empty inventory or duplicate context IDs')
    status = budget_status(len(bundles))
    if status != 'STOP_BROAD_CONTEXT_LIMIT':
        raise RuntimeError('This source-only entrypoint has no inference implementation; do not claim experiment completion')
    coverage = {}
    for side in ('old', 'new'):
        indexed = {p['page'] for p in inventories[side]['pages'] if p['status'] == 'INDEXED'}
        covered = {p for b in bundles for p in b[side]['pages']}
        coverage[side] = dict(indexed_pages=len(indexed), covered_pages=len(covered),
                              uncovered_indexed_pages=sorted(indexed - covered),
                              inventory_sha256=inputs[str(inventory_paths[side])],
                              source_pdf=pair[side]['artifacts']['pdf'])
    index = dict(status=status, policy=POLICY, count=len(bundles), limit=LIMIT,
                 nonempty_count=sum(not b['empty'] for b in bundles),
                 contexts=bundles, coverage=coverage, all_sources_delivered=False)
    write(OUT / 'BROAD_CONTEXT_INDEX.json', index)
    code = {str(p.relative_to(REPO)): sha(p) for p in sorted(Path(__file__).parent.glob('*.py'))}
    freeze = dict(status=status, frozen_at=datetime.now(timezone.utc).isoformat(),
                  repo_head=head, working_tree_at_start=dirty, code=code,
                  pair_index=8, pair_key=PAIR_KEY, partition='DEV', pages={'old': 108, 'new': 188},
                  input_hashes=inputs, frozen_v4_code_hashes=v4_codes,
                  policy=POLICY, broad_contexts=len(bundles), broad_context_limit=LIMIT,
                  planned_model='gpt-6-astra', planned_reasoning='xhigh', model_calls=0,
                  broad_context_index_sha256=sha(OUT / 'BROAD_CONTEXT_INDEX.json'),
                  result_frozen=False, evaluation_authorized=False,
                  validation='NOT OPENED', final_holdout='NOT OPENED', production='UNCHANGED')
    write(OUT / 'EXPERIMENT_FREEZE.json', freeze)
    for path, digest in inputs.items():
        if sha(path) != digest:
            raise ValueError('Input mutated during inventory: ' + path)
    receipt = dict(status='PASS', scope='THIS_PREFLIGHT_ONLY_NOT_A_BLIND_CORPUS_CLAIM',
                   source_truth_opened=False, evaluation_started=False,
                   inherited_exposure='User-supplied aggregate baseline counts/root causes; historical DEV-known corpus',
                   forbidden=['SOURCE_VERIFICATION.json', 'F01-F13 traces', 'LOSS_FUNNEL.json',
                              'independent ChatGPT findings', 'Pair B source truth', 'expected changes'],
                   read_paths=sorted(guard.reads), denied_accesses=guard.denied,
                   access_policy='Existing DEV pair guard, then explicit source-only allowlist; network disabled',
                   baseline_inputs_unchanged=True, model_prompts_created=False,
                   pair_a_sources_opened=False, frozen_v4_receipt_read_as_code_metadata_only=True,
                   validation='NOT OPENED', final_holdout='NOT OPENED')
    write(OUT / 'NO_TRUTH_LEAKAGE_RECEIPT.json', receipt)
    usage = {stage: dict(calls=0, input_tokens=0, output_tokens=0, failures=0)
             for stage in ('DECOMPOSITION', 'COMPARISON', 'TOTAL')}
    usage.update(OpenRouter=0, Claude=0)
    write(OUT / 'MODEL_USAGE.json', usage)
    report = f'''# Pair B / ИОС4.2 — semantic decomposer V1

STATUS: {status}

Инвентарь: {len(bundles)} broad contexts; {index['nonempty_count']} непустых.
Лимит: {LIMIT}. Применён STOP из пункта 15 задания до первого model call.

Каждый существующий broad candidate использован только как seed контекста.
Контекст включает ссылки на все индексированные регионы его исходных страниц,
TEXT/TABLE/GRAPHIC без фильтра одинаковой модальности, координаты и исходные PDF.
Растры проверены по SHA-256; ссылки на PDF/растры не считаются доставкой модели.
Широкий заголовок не утверждается как один инженерный объект или один claim.
Два пустых seed сохранены явно; их исключение тоже не позволяет пройти лимит.
Объединение разных тем ради прохождения лимита не выполнялось.
Охват страниц и пропуски представлены в BROAD_CONTEXT_INDEX.json; полнота не заявляется.

DECOMPOSITION CALLS: 0
COMPARISON CALLS: 0
TOTAL MODEL CALLS: 0
LOCAL CANDIDATES: N/A — decomposition не запускалась
MODEL-READY LOCAL PACKAGES: N/A
OpenRouter: 0; Claude: 0

Исходный baseline из задания: PROVEN 10; sufficient evidence 4/10;
raw found 0/10; final ACCEPT 0/10; SUBJECT_TOO_BROAD 4; PACKAGE_SCOPE_WRONG 4.

Все метрики нового эксперимента, CORRECT/FALSE ACCEPT, REVIEW, NOT_CHANGE,
negative control F13 и сравнение cross-modality: NOT EVALUATED.
Это не нулевой recall и не свидетельство провала AI decomposition.

NO TRUTH LEAKAGE: PASS для текущей подготовки. Исторический DEV и переданные
пользователем агрегаты не объявляются новым слепым набором.
Source truth не открыта; ни decomposition freeze, ни result freeze не созданы.
Post-inference V4 проверен по frozen code hashes, но не запускался.

ARCHITECTURE CONCLUSION: гипотеза не проверена из-за лимита broad contexts.
Вопросы A–F: NOT EVALUATED; YES/NO без inference и evaluation были бы выдумкой.
RECOMMENDATION: MORE_CONTROLLED_TESTING_REQUIRED

Для следующего отдельно определённого опыта потребуется согласовать группировку
широкого контекста или лимит. Эта версия остановлена, её inventory не сокращён.

PRODUCTION: UNCHANGED
PAIR A: NOT RERUN
BASELINE / UI: UNCHANGED
VALIDATION: NOT OPENED
FINAL HOLDOUT: NOT OPENED

Repo HEAD на старте: `{head}`.
EXPERIMENT_FREEZE.json содержит все input hashes и frozen V4 code hashes.
Артефакты decomposition/comparison, ProjectChanges, системный XLSX и evaluation
не созданы: STOP наступил до этих стадий, фиктивные результаты не публикуются.
'''
    write(OUT / 'FINAL_REPORT.md', report)
    write(OUT / 'ARCHITECTURE_DECISION_REPORT.md',
          'STATUS: NOT EVALUATED\n\n' +
          'Лимит broad contexts превышен до inference. Вопросы A–F не оценены.\n'
          'Оснований переносить subject/claim discovery на AI пока не получено.\n'
          'RECOMMENDATION: MORE_CONTROLLED_TESTING_REQUIRED\n')
    write(OUT / 'PREFLIGHT_ARTIFACT_MANIFEST.json', {
        p.name: sha(p) for p in sorted(OUT.iterdir()) if p.is_file()})
    print(json.dumps(dict(status=status, broad_contexts=len(bundles), nonempty=index['nonempty_count'],
                          model_calls=0, artifacts=str(OUT)), ensure_ascii=False))


if __name__ == '__main__':
    main()
