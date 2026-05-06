"""MCP-сервер для работы с нормативной базой.

Text tools (человекочитаемый вывод, совместимы с прежним поведением):
  - list_norms: список действующих норм (с фильтром по типу/коду)
  - find_paragraph: найти пункт по коду нормы и номеру пункта
  - search: семантический поиск по пунктам нормативных документов
  - norm_info: информация о конкретной норме (связи, соседи)

JSON tools (машинный вывод, для использования из других проектов):
  - get_norm_status: статус нормы по «грязному» вводу (active/replaced/cancelled/unknown)
  - get_paragraph_json: структурированный payload пункта
  - semantic_search_json: список результатов семантического поиска

Запуск:
    cd norms_search && source venv/bin/activate
    python3 mcp_server.py                    # stdio транспорт

Подключение к Claude Code:
    claude mcp add norms -- /path/to/norms_search/venv/bin/python3 /path/to/norms_search/mcp_server.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

sys.path.insert(0, str(Path(__file__).parent))
from parse_filename import normalize_user_code  # noqa: E402

VAULT = Path(__file__).resolve().parent.parent / "vault"
DATA_DIR = Path(__file__).parent
ACTIVE_JSON = DATA_DIR / "active_norms.json"
REFS_GRAPH = DATA_DIR / "refs_graph.json"
NEIGHBORS_JSON = DATA_DIR / "semantic_neighbors.json"

mcp = FastMCP("norms")

_active_cache: list[dict] | None = None
_refs_cache: dict | None = None
_neighbors_cache: dict | None = None


def _load_active() -> list[dict]:
    global _active_cache
    if _active_cache is None:
        data = json.loads(ACTIVE_JSON.read_text(encoding="utf-8"))
        _active_cache = data["norms"]
    return _active_cache


def _load_refs() -> dict:
    global _refs_cache
    if _refs_cache is None and REFS_GRAPH.exists():
        _refs_cache = json.loads(REFS_GRAPH.read_text(encoding="utf-8"))
    return _refs_cache or {}


def _load_neighbors() -> dict:
    global _neighbors_cache
    if _neighbors_cache is None and NEIGHBORS_JSON.exists():
        _neighbors_cache = json.loads(NEIGHBORS_JSON.read_text(encoding="utf-8"))
    return _neighbors_cache or {}


@mcp.tool()
def list_norms(
    type_filter: str = "",
    code_filter: str = "",
    limit: int = 50,
) -> str:
    """Список действующих норм из vault'а.

    Args:
        type_filter: фильтр по типу (ГОСТ, СП, СНиП, ВСН, ФЗ, МДС, РД, ПУЭ)
        code_filter: подстрока в коде нормы (например "256" или "10180")
        limit: максимум записей (default 50)
    """
    norms = _load_active()
    result = []
    for n in norms:
        if type_filter and n.get("type", "").lower() != type_filter.lower():
            continue
        if code_filter and code_filter.lower() not in n.get("code", "").lower():
            continue
        result.append(f"{n['code']}  ({n.get('type', '?')}, {n.get('year', '?')})  {n.get('title', '')[:60]}")
        if len(result) >= limit:
            break
    header = f"Найдено: {len(result)} (из {len(norms)} всего)"
    if type_filter:
        header += f", тип={type_filter}"
    if code_filter:
        header += f", код содержит '{code_filter}'"
    return header + "\n\n" + "\n".join(result) if result else header + "\n\nНичего не найдено."


@mcp.tool()
def find_paragraph(code: str, paragraph: str, max_lines: int = 50) -> str:
    """Найти текст конкретного пункта в нормативном документе по коду и номеру.

    Args:
        code: код нормы, например "СП 256.1325800.2016" или "ГОСТ 10180-2012"
        paragraph: номер пункта, например "15.30", "1.1", "3"
        max_lines: максимум строк в ответе
    """
    from find_paragraph import find_file, find_paragraph as _find

    file = find_file(code)
    if file is None:
        key = normalize_user_code(code)
        return f"Файл не найден для кода '{code}' (искали '{key}*' в vault'е)"

    result = _find(file, paragraph)
    if result is None:
        return f"Пункт '{paragraph}' не найден в {file.name}"

    line_num, lines = result
    if max_lines and len(lines) > max_lines:
        lines = lines[:max_lines]
        lines.append(f"\n... (обрезано)")

    header = f"📄 {code} п. {paragraph}  (файл: {file.name}, строка: {line_num})\n"
    return header + "\n".join(lines)


@mcp.tool()
def search(query: str, top: int = 5, code_filter: str = "") -> str:
    """Семантический поиск по пунктам всех нормативных документов.

    Найдёт релевантные пункты по смыслу запроса на естественном языке.
    Например: "требования к заземлению в ванных комнатах",
    "минимальная толщина защитного слоя бетона", "огнестойкость перекрытий".

    Args:
        query: запрос на естественном языке
        top: количество результатов (default 5)
        code_filter: фильтр по подстроке кода (напр. "СП" — только своды правил)
    """
    from search import search as _search

    results = _search(query, top, code_filter or None)
    if not results:
        return "Ничего не найдено."

    lines = [f"Запрос: {query}\n"]
    for i, r in enumerate(results, 1):
        snippet = r["text"][:400]
        if len(r["text"]) > 400:
            snippet += "…"
        lines.append(f"[{i}] score={r['score']:.3f}  {r['code']} п. {r['paragraph']}")
        lines.append(f"    {snippet}\n")
    return "\n".join(lines)


@mcp.tool()
def norm_info(code: str) -> str:
    """Подробная информация о норме: метаданные, явные ссылки, семантические соседи.

    Args:
        code: код нормы, например "СП 256.1325800.2016"
    """
    norms = _load_active()
    norm = next((n for n in norms if n["code"].lower() == code.lower()), None)

    # Попробуем loose match
    if not norm:
        norm = next((n for n in norms if code.lower() in n["code"].lower()), None)

    if not norm:
        return f"Норма '{code}' не найдена в vault'е."

    lines = [
        f"Код: {norm['code']}",
        f"Тип: {norm.get('type', '?')}",
        f"Год: {norm.get('year', '?')}",
        f"Название: {norm.get('title', '?')}",
        f"Файл: {norm.get('file', '?')}",
    ]

    refs_data = _load_refs()
    graph = refs_data.get("graph", {})
    node = graph.get(norm["code"])
    if node:
        cited = node.get("cited_in_vault", [])
        unknown = node.get("cited_unknown", [])
        fz = node.get("fz_refs", [])
        lines.append(f"\nЯвные ссылки в тексте (в vault'е): {len(cited)}")
        for c in cited[:15]:
            lines.append(f"  - {c}")
        if len(cited) > 15:
            lines.append(f"  ... ещё {len(cited) - 15}")
        if unknown:
            lines.append(f"\nУпоминает, но нет в vault'е: {len(unknown)}")
            for c in unknown[:10]:
                lines.append(f"  - {c}")
        if fz:
            lines.append(f"\nФедеральные законы: {', '.join(fz)}")

    nbrs_data = _load_neighbors()
    nbrs = nbrs_data.get("neighbors", {}).get(norm["code"])
    if nbrs:
        lines.append(f"\nСемантические соседи (по эмбеддингам):")
        for n in nbrs:
            lines.append(f"  - {n['code']}  (score={n['score']:.3f})")

    # Входящие ссылки (кто ссылается НА эту норму)
    incoming = []
    for other_code, other_node in graph.items():
        if norm["code"] in other_node.get("cited_in_vault", []):
            incoming.append(other_code)
    if incoming:
        lines.append(f"\nНа эту норму ссылаются ({len(incoming)}):")
        for c in incoming[:15]:
            lines.append(f"  - {c}")
        if len(incoming) > 15:
            lines.append(f"  ... ещё {len(incoming) - 15}")

    return "\n".join(lines)


@mcp.tool()
def get_norm_status(code: str) -> dict:
    """Authoritative статус нормы по «грязному» вводу.

    Источник истины — status_index.json (build_status_index.py собирает его
    из vault/ + status_overrides.yaml). Никакого WebSearch.

    Args:
        code: код нормы в любом написании.

    Returns:
        dict. Ключевые поля: query, normalized_query, found, matched_code,
        status (active|outdated_edition|replaced|cancelled|unknown),
        doc_status, edition_status, authoritative, resolution_reason
        (exact|alias|manual_override|not_in_index|unsupported_family|not_found),
        detected_family, supported_family, needs_manual_addition,
        replacement_doc, current_version, title, file, type, year,
        details, source_url, last_verified, parse_confidence, source.
    """
    from norms_api import get_norm_status as _get_norm_status

    return _get_norm_status(code)


@mcp.tool()
def get_paragraph_json(code: str, paragraph: str, max_lines: int = 50) -> dict:
    """JSON-поиск пункта. Никогда не бросает исключение.

    Args:
        code: код нормы
        paragraph: номер пункта ("15.30", "1.1", "3")
        max_lines: лимит строк текста в ответе

    Returns:
        dict. Ключевые поля: query_code, matched_code, paragraph, found,
        text, file, line, status, doc_status, edition_status, authoritative,
        has_text, resolution_reason (exact|alias|manual_override|
        no_document_text|paragraph_not_found|not_in_index|unsupported_family|
        not_found), replacement_doc, truncated.
    """
    from norms_api import get_paragraph as _get_paragraph

    return _get_paragraph(code, paragraph, max_lines)


@mcp.tool()
def semantic_search_json(query: str, top: int = 5, code_filter: str = "") -> list[dict]:
    """JSON-версия семантического поиска. Пустой запрос/ошибка → [].

    Args:
        query: запрос на естественном языке
        top: количество результатов
        code_filter: подстрока для фильтра по коду (напр. "СП")

    Returns:
        list[dict] с полями score, code, paragraph, file, line, text.
    """
    from norms_api import semantic_search as _semantic_search

    return _semantic_search(query, top, code_filter or None)


if __name__ == "__main__":
    mcp.run(transport="stdio")
