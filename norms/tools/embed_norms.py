"""Считает эмбеддинги норм и находит семантически близких соседей.

Процесс:
1. Для каждой нормы из active_norms.json извлекает заголовок + текст секции
   'Область применения' (fallback: title).
2. Считает эмбеддинги моделью intfloat/multilingual-e5-large.
3. Cosine similarity → top-K соседей.
4. Сохраняет embeddings.npz и semantic_neighbors.json.
5. Опционально: инъектирует секцию '## Похожие по смыслу' в каждый .md.

Использование:
    python3 embed_norms.py                     # полный прогон с инъекцией
    python3 embed_norms.py --no-inject         # только посчитать, в файлы не писать
    python3 embed_norms.py --limit 10          # первые 10 норм
    python3 embed_norms.py --top-k 5           # сколько соседей писать (default 5)
    python3 embed_norms.py --rebuild           # пересчитать эмбеддинги (не юзать кеш)

Требует venv: source venv/bin/activate
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np

VAULT = Path(__file__).resolve().parent.parent / "vault"
ACTIVE_JSON = Path(__file__).parent / "active_norms.json"
EMBEDDINGS_NPZ = Path(__file__).parent / "embeddings.npz"
NEIGHBORS_JSON = Path(__file__).parent / "semantic_neighbors.json"

MODEL_NAME = "intfloat/multilingual-e5-large"
MAX_CHARS = 2000  # e5-large: 512 токенов ≈ ~2000 символов русского текста

NEIGHBORS_SECTION = "## Похожие по смыслу"

# Секция "Область применения" — стандартная для норм РФ.
# Варианты: "# Область применения", "## 1 Область применения", "##### 1 Область применения"
AREA_HEADER_RE = re.compile(
    r"^#+\s*\d*\.?\s*Область\s+применения\s*$",
    re.MULTILINE | re.IGNORECASE,
)

FRONTMATTER_TITLE_RE = re.compile(r'^title:\s*"(.+?)"', re.MULTILINE)
FRONTMATTER_CODE_RE = re.compile(r'^code:\s*"(.+?)"', re.MULTILINE)


def extract_scope_text(content: str) -> str:
    """Ищет секцию 'Область применения', возвращает её текст (до следующего заголовка)."""
    m = AREA_HEADER_RE.search(content)
    if not m:
        return ""
    start = m.end()
    rest = content[start:]
    # До следующего заголовка того же или большего уровня
    next_header = re.search(r"^#+\s", rest, re.MULTILINE)
    scope = rest[: next_header.start()] if next_header else rest
    scope = scope.strip()
    # Чистим markdown-шум
    scope = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", scope)  # [text](url) → text
    scope = re.sub(r"\n{3,}", "\n\n", scope)
    return scope[:MAX_CHARS]


def build_embedding_input(title: str, scope: str) -> str:
    """Готовит текст для e5-large. Префикс 'passage: ' обязателен для этой модели."""
    combined = title.strip()
    if scope:
        combined += "\n" + scope
    return "passage: " + combined[:MAX_CHARS]


def load_norms() -> list[dict]:
    data = json.loads(ACTIVE_JSON.read_text(encoding="utf-8"))
    return data["norms"]


def prepare_corpus(norms: list[dict]) -> tuple[list[str], list[str], list[str]]:
    """Возвращает (коды, тексты_для_эмбеддинга, сырые_scope_для_отладки)."""
    codes: list[str] = []
    inputs: list[str] = []
    scopes: list[str] = []
    for n in norms:
        md_path = VAULT / n["file"]
        if not md_path.exists():
            print(f"WARN: файл не найден: {n['file']}", file=sys.stderr)
            continue
        try:
            content = md_path.read_text(encoding="utf-8")
        except Exception as e:
            print(f"WARN: чтение {n['file']}: {e}", file=sys.stderr)
            continue
        title = n.get("title", "") or n["code"]
        scope = extract_scope_text(content)
        codes.append(n["code"])
        inputs.append(build_embedding_input(title, scope))
        scopes.append(scope[:200])
    return codes, inputs, scopes


def compute_embeddings(inputs: list[str], model_name: str) -> np.ndarray:
    from sentence_transformers import SentenceTransformer

    print(f"Загружаю модель {model_name} (первый раз — скачает ~2GB)...", file=sys.stderr)
    model = SentenceTransformer(model_name)
    print(f"Считаю эмбеддинги для {len(inputs)} норм...", file=sys.stderr)
    emb = model.encode(
        inputs,
        batch_size=8,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,  # для cosine → dot product
    )
    return emb


def find_neighbors(
    codes: list[str], embeddings: np.ndarray, top_k: int
) -> dict[str, list[dict]]:
    """Для каждой нормы — top_k ближайших по cosine similarity."""
    # Уже нормализованы → cosine = dot product
    sim = embeddings @ embeddings.T
    np.fill_diagonal(sim, -1)  # исключаем самоссылки

    result: dict[str, list[dict]] = {}
    for i, code in enumerate(codes):
        idx = np.argsort(-sim[i])
        nbrs = []
        for j in idx:
            if sim[i, j] < 0:
                break  # исчерпали реальных соседей (остались только самоссылки)
            nbrs.append({"code": codes[j], "score": float(sim[i, j])})
            if len(nbrs) >= top_k:
                break
        result[code] = nbrs
    return result


def strip_section(body: str, header: str) -> str:
    """Удаляет '## {header}' секцию (до следующей ## или EOF)."""
    pattern = re.compile(
        rf"\n## {re.escape(header)}\s*\n.*?(?=\n## |\Z)", re.DOTALL
    )
    return pattern.sub("\n", body).rstrip() + "\n"


def build_neighbors_section(
    neighbors: list[dict], code_to_file: dict[str, str]
) -> str:
    lines = ["", NEIGHBORS_SECTION, "", "Близкие по тематике нормы (по эмбеддингам):", ""]
    for n in neighbors:
        code = n["code"]
        fname = code_to_file.get(code, "")
        stem = fname[:-3] if fname.endswith(".md") else fname
        score = n["score"]
        if stem:
            lines.append(f"- [[{stem}|{code}]] — {score:.2f}")
        else:
            lines.append(f"- {code} — {score:.2f}")
    lines.append("")
    return "\n".join(lines)


def inject_sections(neighbors: dict[str, list[dict]], norms: list[dict]) -> int:
    """Добавляет/обновляет секцию '## Похожие по смыслу' в каждом .md. Возвращает кол-во обновлённых."""
    code_to_file = {n["code"]: n["file"] for n in norms}
    updated = 0
    for code, nbrs in neighbors.items():
        fname = code_to_file.get(code)
        if not fname:
            continue
        path = VAULT / fname
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8")
            content = strip_section(content, "Похожие по смыслу")
            content = content.rstrip() + "\n" + build_neighbors_section(nbrs, code_to_file)
            path.write_text(content, encoding="utf-8")
            updated += 1
        except Exception as e:
            print(f"WARN: запись {fname}: {e}", file=sys.stderr)
    return updated


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-inject", action="store_true", help="не писать в .md")
    ap.add_argument("--limit", type=int, help="обработать только N норм")
    ap.add_argument("--top-k", type=int, default=5, help="сколько соседей записать")
    ap.add_argument("--rebuild", action="store_true", help="пересчитать эмбеддинги (игнорировать кеш)")
    args = ap.parse_args()

    norms = load_norms()
    if args.limit:
        norms = norms[: args.limit]
    print(f"Норм к обработке: {len(norms)}", file=sys.stderr)

    codes, inputs, scopes = prepare_corpus(norms)
    print(f"Подготовлено корпусов: {len(codes)}", file=sys.stderr)
    empty_scope = sum(1 for s in scopes if not s)
    print(f"Без 'Область применения' (используется только title): {empty_scope}", file=sys.stderr)

    # Кеш эмбеддингов
    if EMBEDDINGS_NPZ.exists() and not args.rebuild:
        cached = np.load(EMBEDDINGS_NPZ, allow_pickle=True)
        cached_codes = list(cached["codes"])
        if cached_codes == codes:
            print(f"Использую кеш: {EMBEDDINGS_NPZ}", file=sys.stderr)
            embeddings = cached["embeddings"]
        else:
            print("Кеш устарел (изменился набор норм) → пересчёт", file=sys.stderr)
            embeddings = compute_embeddings(inputs, MODEL_NAME)
            np.savez(EMBEDDINGS_NPZ, codes=np.array(codes), embeddings=embeddings)
    else:
        embeddings = compute_embeddings(inputs, MODEL_NAME)
        np.savez(EMBEDDINGS_NPZ, codes=np.array(codes), embeddings=embeddings)
        print(f"Эмбеддинги сохранены: {EMBEDDINGS_NPZ}", file=sys.stderr)

    neighbors = find_neighbors(codes, embeddings, args.top_k)
    NEIGHBORS_JSON.write_text(
        json.dumps(
            {
                "meta": {
                    "model": MODEL_NAME,
                    "total": len(codes),
                    "top_k": args.top_k,
                },
                "neighbors": neighbors,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Соседи сохранены: {NEIGHBORS_JSON}", file=sys.stderr)

    if args.no_inject:
        print("--no-inject: файлы не изменены", file=sys.stderr)
        return

    updated = inject_sections(neighbors, norms)
    print(f"Обновлено файлов: {updated}", file=sys.stderr)


if __name__ == "__main__":
    main()
