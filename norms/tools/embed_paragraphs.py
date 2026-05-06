"""Эмбеддит каждый пункт из paragraphs.jsonl и сохраняет векторы.

Вход: paragraphs.jsonl (результат build_paragraph_index.py)
Выход: paragraphs_embeddings.npz  (embeddings + метадата)

Использование:
    python3 embed_paragraphs.py                # полный прогон
    python3 embed_paragraphs.py --limit 100    # первые 100 пунктов (debug)
    python3 embed_paragraphs.py --rebuild      # игнорировать кеш

Требует venv: source venv/bin/activate
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np

PARAGRAPHS_JSONL = Path(__file__).parent / "paragraphs.jsonl"
OUTPUT_NPZ = Path(__file__).parent / "paragraphs_embeddings.npz"
MODEL_NAME = "intfloat/multilingual-e5-base"
MAX_CHARS = 2000  # ~512 токенов русского


def load_paragraphs() -> list[dict]:
    with PARAGRAPHS_JSONL.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def build_input(p: dict) -> str:
    """e5 требует префикс 'passage: ' для корпуса."""
    # Добавляем код нормы в начало — помогает эмбеддингу понимать контекст
    text = f"{p['code']}, п. {p['paragraph']}: {p['text']}"
    return "passage: " + text[:MAX_CHARS]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="только N пунктов (debug)")
    ap.add_argument("--rebuild", action="store_true", help="игнорировать кеш")
    ap.add_argument("--batch-size", type=int, default=32)
    args = ap.parse_args()

    paragraphs = load_paragraphs()
    if args.limit:
        paragraphs = paragraphs[: args.limit]
    print(f"Пунктов: {len(paragraphs)}", file=sys.stderr)

    # Кеш
    if OUTPUT_NPZ.exists() and not args.rebuild:
        cached = np.load(OUTPUT_NPZ, allow_pickle=True)
        if len(cached["ids"]) == len(paragraphs):
            print(f"Кеш актуален: {OUTPUT_NPZ}", file=sys.stderr)
            return
        print("Кеш устарел → пересчёт", file=sys.stderr)

    from sentence_transformers import SentenceTransformer

    print(f"Загружаю {MODEL_NAME}...", file=sys.stderr)
    model = SentenceTransformer(MODEL_NAME)

    inputs = [build_input(p) for p in paragraphs]
    t0 = time.time()
    print(f"Эмбеддинг {len(inputs)} пунктов (batch={args.batch_size})...", file=sys.stderr)
    embeddings = model.encode(
        inputs,
        batch_size=args.batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    elapsed = time.time() - t0
    print(f"Готово за {elapsed:.1f}с ({len(inputs)/elapsed:.1f} пунктов/сек)", file=sys.stderr)

    # Сохраняем как структуру массивов, без pickle (быстрее загрузка)
    ids = np.arange(len(paragraphs))
    codes = np.array([p["code"] for p in paragraphs])
    para_nums = np.array([p["paragraph"] for p in paragraphs])
    files = np.array([p["file"] for p in paragraphs])
    lines_arr = np.array([p["line"] for p in paragraphs])
    texts = np.array([p["text"] for p in paragraphs])

    np.savez(
        OUTPUT_NPZ,
        ids=ids,
        embeddings=embeddings,
        codes=codes,
        paragraphs=para_nums,
        files=files,
        line_nums=lines_arr,
        texts=texts,
        model=np.array([MODEL_NAME]),
    )
    print(f"Сохранено: {OUTPUT_NPZ} ({OUTPUT_NPZ.stat().st_size / 1e6:.1f} MB)", file=sys.stderr)


if __name__ == "__main__":
    main()
