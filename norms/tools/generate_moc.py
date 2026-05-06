"""Генерирует MOC-заметки (Maps of Content) по тематическим кластерам.

Использует эмбеддинги норм (embeddings.npz) + KMeans кластеризацию.
Для каждого кластера создаёт .md файл в vault'е со списком [[ссылок]].

Использование:
    python3 generate_moc.py                    # авто-кластеризация
    python3 generate_moc.py --clusters 15      # количество кластеров
    python3 generate_moc.py --dry-run          # только показать кластеры
    python3 generate_moc.py --clean            # удалить старые MOC-файлы

Требует venv: source venv/bin/activate
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

import numpy as np
from sklearn.cluster import KMeans

VAULT = Path(__file__).resolve().parent.parent / "vault"
EMBEDDINGS_NPZ = Path(__file__).parent / "embeddings.npz"
ACTIVE_JSON = Path(__file__).parent / "active_norms.json"
REFS_GRAPH = Path(__file__).parent / "refs_graph.json"
NEIGHBORS_JSON = Path(__file__).parent / "semantic_neighbors.json"
OUTPUT_JSON = Path(__file__).parent / "clusters.json"

MOC_PREFIX = "MOC - "

# Стоп-слова для автоименования кластеров
STOP_WORDS = {
    "межгосударственный", "стандарт", "национальный", "свод", "правил",
    "российской", "федерации", "государственный", "союза", "сср",
    "система", "общие", "технические", "условия", "метод", "методы",
    "определения", "требования", "правила", "средства", "изделия",
    "документация", "стандартов", "стандарте", "нормы", "нормативные",
    "строительные", "ведомственные", "проектирования", "устройства",
    "утв", "ред", "приказом", "приказ", "минстроя", "россии",
    "введен", "действие", "кодекс", "часть", "основные", "положения",
    "постановление", "правительства", "решение", "комиссии", "письмо",
    "справочная", "информация", "утверждении", "принятии", "распоряжение",
    "госстроя", "минрегиона", "федеральный", "закон",
    "федерац", "федера", "единая", "систем", "систему",
    "сибирского", "округа", "восточно", "западно", "арбитражного",
    "верховного", "судебной", "коллегии", "гражданским", "делам",
    "таможенного", "листов", "регламент",
}


def load_data():
    emb_data = np.load(EMBEDDINGS_NPZ, allow_pickle=True)
    codes = list(emb_data["codes"])
    embeddings = emb_data["embeddings"]

    norms_data = json.loads(ACTIVE_JSON.read_text(encoding="utf-8"))
    code_to_norm = {n["code"]: n for n in norms_data["norms"]}

    return codes, embeddings, code_to_norm


def cluster_norms(embeddings: np.ndarray, n_clusters: int) -> np.ndarray:
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    return km.fit_predict(embeddings)


def extract_cluster_name(
    norms_in_cluster: list[dict],
    embeddings: np.ndarray,
    indices: list[int],
    all_codes: list[str],
    top_n: int = 3,
) -> str:
    """Автоименование: частотные слова + тип ближайшей к центру нормы."""
    # Частотные слова из заголовков
    words: Counter = Counter()
    for n in norms_in_cluster:
        title = n.get("title", "").replace("_", " ")
        for w in re.findall(r"[а-яёА-ЯЁ]{5,}", title):
            w_lower = w.lower()
            if w_lower not in STOP_WORDS:
                words[w_lower] += 1

    # Доминирующий тип
    types = Counter(n.get("type", "?") for n in norms_in_cluster)
    dominant_type = types.most_common(1)[0][0]

    # Ближайшая к центроиду норма (наиболее репрезентативная)
    if indices:
        cluster_emb = embeddings[indices]
        centroid = cluster_emb.mean(axis=0)
        centroid /= np.linalg.norm(centroid)
        dists = cluster_emb @ centroid
        central_idx = indices[int(np.argmax(dists))]
        central_code = all_codes[central_idx]
    else:
        central_code = norms_in_cluster[0]["code"] if norms_in_cluster else "?"

    if words:
        top = [w for w, _ in words.most_common(top_n)]
        topic = ", ".join(w.capitalize() for w in top)
    else:
        topic = dominant_type

    return f"{topic} ({central_code})"


def build_moc_content(
    cluster_name: str,
    cluster_id: int,
    norms: list[dict],
    all_clusters: dict[int, str],
) -> str:
    """Генерирует содержимое MOC-файла."""
    lines = [
        "---",
        f'type: "MOC"',
        f'cluster_id: {cluster_id}',
        f'norms_count: {len(norms)}',
        "---",
        "",
        f"# {cluster_name}",
        "",
        f"Тематический индекс: **{len(norms)} норм** в этом кластере.",
        "",
        "## Нормы",
        "",
    ]

    # Группируем по типу внутри кластера
    by_type: dict[str, list[dict]] = {}
    for n in norms:
        t = n.get("type", "other")
        by_type.setdefault(t, []).append(n)

    for norm_type in sorted(by_type.keys()):
        type_norms = sorted(by_type[norm_type], key=lambda n: n["code"])
        lines.append(f"### {norm_type} ({len(type_norms)})")
        lines.append("")
        for n in type_norms:
            fname = n["file"]
            stem = fname[:-3] if fname.endswith(".md") else fname
            code = n["code"]
            title = n.get("title", "")[:60]
            year = n.get("year", "")
            lines.append(f"- [[{stem}|{code}]] ({year}) — {title}")
        lines.append("")

    # Навигация к другим MOC
    lines.append("## Другие тематические индексы")
    lines.append("")
    for cid, cname in sorted(all_clusters.items()):
        if cid != cluster_id:
            lines.append(f"- [[{MOC_PREFIX}{cname}|{cname}]]")
    lines.append("")

    return "\n".join(lines)


def clean_old_mocs():
    """Удаляет старые MOC-файлы из vault'а."""
    removed = 0
    for f in VAULT.glob(f"{MOC_PREFIX}*.md"):
        f.unlink()
        removed += 1
    return removed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clusters", type=int, default=12, help="количество кластеров")
    ap.add_argument("--dry-run", action="store_true", help="только показать кластеры")
    ap.add_argument("--clean", action="store_true", help="удалить старые MOC-файлы")
    args = ap.parse_args()

    if args.clean and not args.dry_run:
        n = clean_old_mocs()
        print(f"Удалено MOC-файлов: {n}", file=sys.stderr)

    codes, embeddings, code_to_norm = load_data()
    print(f"Норм: {len(codes)}, кластеров: {args.clusters}", file=sys.stderr)

    labels = cluster_norms(embeddings, args.clusters)

    # Группируем нормы по кластерам
    clusters: dict[int, list[dict]] = {}
    for i, code in enumerate(codes):
        cid = int(labels[i])
        norm = code_to_norm.get(code, {"code": code, "file": "", "type": "?", "title": ""})
        clusters.setdefault(cid, []).append(norm)

    # Автоименование
    cluster_names: dict[int, str] = {}
    for cid, norms in sorted(clusters.items()):
        indices = [i for i, c in enumerate(codes) if c in {n["code"] for n in norms}]
        name = extract_cluster_name(norms, embeddings, indices, codes)
        cluster_names[cid] = name

    # Вывод
    print(f"\n{'ID':>3}  {'Норм':>5}  Название", file=sys.stderr)
    print("-" * 60, file=sys.stderr)
    for cid in sorted(clusters.keys()):
        print(f"{cid:>3}  {len(clusters[cid]):>5}  {cluster_names[cid]}", file=sys.stderr)

    if args.dry_run:
        print("\nDRY-RUN: MOC-файлы не создавались", file=sys.stderr)
        # Сохраняем clusters.json для инспекции
        OUTPUT_JSON.write_text(
            json.dumps(
                {cid: {"name": cluster_names[cid], "codes": [n["code"] for n in norms]}
                 for cid, norms in clusters.items()},
                ensure_ascii=False, indent=2,
            ),
            encoding="utf-8",
        )
        print(f"Кластеры: {OUTPUT_JSON}", file=sys.stderr)
        return

    # Удаляем старые MOC-файлы
    clean_old_mocs()

    # Генерируем MOC
    for cid, norms in clusters.items():
        name = cluster_names[cid]
        content = build_moc_content(name, cid, norms, cluster_names)
        moc_path = VAULT / f"{MOC_PREFIX}{name}.md"
        moc_path.write_text(content, encoding="utf-8")
        print(f"  ✓ {moc_path.name} ({len(norms)} норм)", file=sys.stderr)

    print(f"\nСоздано MOC-файлов: {len(clusters)}", file=sys.stderr)


if __name__ == "__main__":
    main()
