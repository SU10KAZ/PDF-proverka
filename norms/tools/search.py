"""Семантический поиск по пунктам норм (двухстадийный: bi-encoder + reranker).

Архитектура:
  Стадия 1: bi-encoder (e5-large) → cosine similarity → top-N кандидатов
  Стадия 2: cross-encoder (bge-reranker-v2-m3) → переоценивает пары (запрос, кандидат)
            и возвращает финальный top-K

Использование:
    python3 search.py "требования к заземлению в ванных"
    python3 search.py "огнестойкость перекрытий" --top 10
    python3 search.py "класс бетона" --code СП                # только СП
    python3 search.py "..." --no-rerank                       # отключить reranker
    python3 search.py "..." --candidates 50                   # сколько кандидатов в reranker
    python3 search.py "..." --json                            # JSON вывод

Требует paragraphs_embeddings.npz (запустить embed_paragraphs.py).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

EMBEDDINGS_NPZ = Path(__file__).parent / "paragraphs_embeddings.npz"
MODEL_NAME_DEFAULT = "intfloat/multilingual-e5-base"
RERANKER_MODEL_NAME = "BAAI/bge-reranker-v2-m3"
DEFAULT_CANDIDATES = 20  # сколько кандидатов передавать в reranker
RERANKER_MAX_LENGTH = 256  # max токенов на пару (query+text); 256 покрывает p75 пунктов
RERANKER_BATCH_SIZE = 16  # на CPU больше = лучше за счёт SIMD

# Module-level cache: загружаем один раз, держим в памяти процесса
_index_cache = None
_model_cache = None
_reranker_cache = None
_reranker_failed = False  # если reranker не загрузился — больше не пытаемся


def load_index():
    """Грузит и МАТЕРИАЛИЗУЕТ .npz в обычный dict.

    np.load для .npz возвращает ленивый NpzFile: каждое обращение data["embeddings"]
    повторно распаковывает массив из ZIP (~1.6 ГБ для e5-large). Поэтому делаем
    распаковку один раз и кладём numpy-массивы в dict.
    """
    global _index_cache
    if _index_cache is None:
        if not EMBEDDINGS_NPZ.exists():
            print(f"ERROR: нет {EMBEDDINGS_NPZ}. Запустите embed_paragraphs.py", file=sys.stderr)
            sys.exit(1)
        with np.load(EMBEDDINGS_NPZ, allow_pickle=True) as npz:
            _index_cache = {key: npz[key] for key in npz.files}
    return _index_cache


def _get_model(model_name: str):
    global _model_cache
    if _model_cache is None:
        from sentence_transformers import SentenceTransformer
        _model_cache = SentenceTransformer(model_name)
    return _model_cache


def _get_reranker():
    """Lazy-load cross-encoder reranker. Возвращает None при ошибке."""
    global _reranker_cache, _reranker_failed
    if _reranker_failed:
        return None
    if _reranker_cache is None:
        try:
            from sentence_transformers import CrossEncoder
            _reranker_cache = CrossEncoder(RERANKER_MODEL_NAME, max_length=RERANKER_MAX_LENGTH)
        except Exception as e:
            print(f"WARN: reranker не загрузился ({e}), fallback на dense-only", file=sys.stderr)
            _reranker_failed = True
            return None
    return _reranker_cache


def search(
    query: str,
    top: int,
    code_filter: str | None,
    *,
    rerank: bool = True,
    candidates: int = DEFAULT_CANDIDATES,
) -> list[dict]:
    """Двухстадийный поиск: bi-encoder → cross-encoder rerank.

    Args:
        query: запрос на естественном языке
        top: финальное число результатов
        code_filter: подстрока для фильтра по коду нормы
        rerank: применять ли reranker (False → классический dense-only)
        candidates: сколько кандидатов передать в reranker (используется только если rerank=True)
    """
    data = load_index()
    model_name = str(data["model"][0])
    model = _get_model(model_name)
    q_input = "query: " + query
    q_emb = model.encode([q_input], normalize_embeddings=True, convert_to_numpy=True)[0]

    sims = data["embeddings"] @ q_emb  # cosine, т.к. уже нормализованы

    codes = data["codes"]
    if code_filter:
        mask = np.array([code_filter.lower() in c.lower() for c in codes])
        sims = np.where(mask, sims, -1)

    # Стадия 1: сколько кандидатов забрать от bi-encoder
    stage1_top = max(candidates, top) if rerank else top
    idx = np.argsort(-sims)[:stage1_top]
    # Отбрасываем «отфильтрованные» (sims=-1)
    idx = [i for i in idx if sims[i] >= 0]

    if not idx:
        return []

    # Собираем кандидатов
    candidates_list = []
    for i in idx:
        candidates_list.append(
            {
                "_idx": int(i),
                "dense_score": float(sims[i]),
                "code": str(codes[i]),
                "paragraph": str(data["paragraphs"][i]),
                "file": str(data["files"][i]),
                "line": int(data["line_nums"][i]),
                "text": str(data["texts"][i]),
            }
        )

    # Стадия 2: rerank
    if rerank and len(candidates_list) > 1:
        reranker = _get_reranker()
        if reranker is not None:
            pairs = [(query, c["text"]) for c in candidates_list]
            try:
                rerank_scores = reranker.predict(
                    pairs,
                    batch_size=RERANKER_BATCH_SIZE,
                    show_progress_bar=False,
                )
                for c, s in zip(candidates_list, rerank_scores):
                    c["score"] = float(s)
                candidates_list.sort(key=lambda x: -x["score"])
            except Exception as e:
                print(f"WARN: reranker.predict упал ({e}), fallback на dense", file=sys.stderr)
                for c in candidates_list:
                    c["score"] = c["dense_score"]
        else:
            for c in candidates_list:
                c["score"] = c["dense_score"]
    else:
        for c in candidates_list:
            c["score"] = c["dense_score"]

    # Финальная подрезка до top
    results = []
    for c in candidates_list[:top]:
        c.pop("_idx", None)
        results.append(c)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", help="запрос на естественном языке")
    ap.add_argument("--top", type=int, default=5, help="кол-во результатов")
    ap.add_argument("--code", help="фильтр по подстроке кода нормы (напр. 'СП')")
    ap.add_argument("--no-rerank", action="store_true", help="отключить cross-encoder reranker")
    ap.add_argument("--candidates", type=int, default=DEFAULT_CANDIDATES,
                    help=f"сколько кандидатов передать в reranker (default {DEFAULT_CANDIDATES})")
    ap.add_argument("--json", action="store_true", help="вывод в JSON")
    ap.add_argument("--snippet", type=int, default=300, help="длина сниппета текста")
    args = ap.parse_args()

    results = search(
        args.query,
        args.top,
        args.code,
        rerank=not args.no_rerank,
        candidates=args.candidates,
    )

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return

    if not results:
        print("Ничего не найдено", file=sys.stderr)
        return

    mode = "dense+rerank" if not args.no_rerank else "dense-only"
    print(f"Запрос: {args.query}  [{mode}]\n")
    for i, r in enumerate(results, 1):
        snippet = r["text"][: args.snippet]
        if len(r["text"]) > args.snippet:
            snippet += "…"
        score_str = f"{r['score']:.3f}"
        if "dense_score" in r and not args.no_rerank:
            score_str += f" (dense {r['dense_score']:.3f})"
        print(f"[{i}] {score_str}  {r['code']} п. {r['paragraph']}  (стр. {r['line']})")
        print(f"    {snippet}")
        print()


if __name__ == "__main__":
    main()
