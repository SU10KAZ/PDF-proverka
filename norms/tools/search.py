"""Семантический поиск по пунктам норм.

Использование:
    python3 search.py "требования к заземлению в ванных"
    python3 search.py "огнестойкость перекрытий" --top 10
    python3 search.py "класс бетона" --code СП         # только СП
    python3 search.py "..." --json                     # JSON вывод

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

# Module-level cache: загружаем один раз, держим в памяти процесса
_index_cache = None
_model_cache = None


def load_index():
    global _index_cache
    if _index_cache is None:
        if not EMBEDDINGS_NPZ.exists():
            print(f"ERROR: нет {EMBEDDINGS_NPZ}. Запустите embed_paragraphs.py", file=sys.stderr)
            sys.exit(1)
        _index_cache = np.load(EMBEDDINGS_NPZ, allow_pickle=True)
    return _index_cache


def _get_model(model_name: str):
    global _model_cache
    if _model_cache is None:
        from sentence_transformers import SentenceTransformer
        _model_cache = SentenceTransformer(model_name)
    return _model_cache


def search(query: str, top: int, code_filter: str | None) -> list[dict]:
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

    idx = np.argsort(-sims)[:top]
    results = []
    for i in idx:
        if sims[i] < 0:
            break
        results.append(
            {
                "score": float(sims[i]),
                "code": str(codes[i]),
                "paragraph": str(data["paragraphs"][i]),
                "file": str(data["files"][i]),
                "line": int(data["line_nums"][i]),
                "text": str(data["texts"][i]),
            }
        )
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", help="запрос на естественном языке")
    ap.add_argument("--top", type=int, default=5, help="кол-во результатов")
    ap.add_argument("--code", help="фильтр по подстроке кода нормы (напр. 'СП')")
    ap.add_argument("--json", action="store_true", help="вывод в JSON")
    ap.add_argument("--snippet", type=int, default=300, help="длина сниппета текста")
    args = ap.parse_args()

    results = search(args.query, args.top, args.code)

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return

    if not results:
        print("Ничего не найдено", file=sys.stderr)
        return

    print(f"Запрос: {args.query}\n")
    for i, r in enumerate(results, 1):
        snippet = r["text"][: args.snippet]
        if len(r["text"]) > args.snippet:
            snippet += "…"
        print(f"[{i}] {r['score']:.3f}  {r['code']} п. {r['paragraph']}  (стр. {r['line']})")
        print(f"    {snippet}")
        print()


if __name__ == "__main__":
    main()
