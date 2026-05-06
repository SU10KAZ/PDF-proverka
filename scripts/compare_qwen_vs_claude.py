"""
Сравнение качества OCR: Qwen (blocks_qwen_300) vs Claude (claude -p subprocess).

Берёт выборку блоков из проекта, отправляет каждый блок Claude с тем же
промптом что и Qwen, сравнивает результаты по полям JSON.

Использование:
    python scripts/compare_qwen_vs_claude.py <project_path> [--n 20] [--block-ids ID1 ID2 ...]
"""
import argparse
import shutil

CLAUDE_BIN = shutil.which("claude") or "/home/coder/.vscode-server/extensions/anthropic.claude-code-2.1.126-linux-x64/resources/native-binary/claude"
import base64
import json
import re
import subprocess
import sys
import time
from pathlib import Path

# Тот же промпт что в qwen_enrich.py
SYSTEM_PROMPT = """Ты — инженер. Извлеки факты из блока чертежа: что нарисовано, марки, размеры, ссылки.
Отвечай строго JSON по схеме, без преамбулы и markdown. Не ищи ошибок. Не выдумывай.
Если поле не видно — [] или null.
"""

USER_INSTRUCTION = """JSON по схеме:
{{
"block_type":"план|план_армирования|план_опалубки|разрез|сечение|ведомость|схема|таблица|узел|другое",
"subject":"до 120 симв — что изображено",
"marks":["марки/позиции"],
"rebar_specs":["параметры арматуры"],
"dimensions":["размеры"],
"references_on_block":["ссылки на листы/разрезы"],
"axes":["оси"],
"level_marks":["отметки"],
"concrete_class":"класс бетона|null",
"notes":"1-2 предл — контекст без картинки"
}}

block_id={block_id} page={page} sheet={sheet_no} ocr="{ocr_label}"

Текст со страницы (подсказка, не копируй):
---
{page_text}
---
"""

SCHEMA_FIELDS = [
    "block_type", "subject", "marks", "rebar_specs", "dimensions",
    "references_on_block", "axes", "level_marks", "concrete_class", "notes",
]

BLOCK_RE = re.compile(r"^### BLOCK \[(?:[A-Z]+)\]: (?P<block_id>[A-Z0-9-]+)\s*$", re.M)
ENRICHED_RE = re.compile(r"^\*\*\[ENRICHED .+?\]\*\*\s*$", re.M)


def load_graph_page_text(graph: dict, page: int) -> str:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            parts = []
            sheet = p.get("sheet_name") or ""
            if sheet:
                parts.append(f"[SHEET] {sheet}")
            for tb in p.get("text_blocks", [])[:10]:
                txt = (tb.get("text") or "").strip()
                if txt:
                    parts.append(txt[:250])
            return "\n".join(parts)[:1500]
    return ""


def load_graph_sheet_no(graph: dict, page: int) -> str:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            return str(p.get("sheet_no_normalized") or p.get("sheet_no_raw") or "")
    return ""


def parse_qwen_enrichment_from_md(md_path: Path) -> dict[str, dict]:
    """Извлекает ENRICHED-блоки из MD. Парсит JSON из bullet-списка."""
    text = md_path.read_text(encoding="utf-8")
    sections = BLOCK_RE.split(text)
    result = {}
    block_ids = BLOCK_RE.findall(text)

    # sections[0] = до первого блока, затем чередуются: block_id, содержимое
    for i, block_id in enumerate(block_ids):
        section = sections[i + 1]
        enriched_m = ENRICHED_RE.search(section)
        if not enriched_m:
            result[block_id] = None
            continue
        enriched_text = section[enriched_m.end():].strip()
        # Попытка распарсить bullet-список в dict
        parsed = {}
        field_map = {
            "Тип блока": "block_type",
            "Содержание": "subject",
            "Марки": "marks",
            "Арматура": "rebar_specs",
            "Размеры": "dimensions",
            "Оси": "axes",
            "Отметки": "level_marks",
            "Бетон": "concrete_class",
            "Ссылки": "references_on_block",
            "Заметки": "notes",
        }
        for line in enriched_text.splitlines():
            line = line.strip()
            if not line.startswith("- **"):
                break
            m = re.match(r"- \*\*(.+?):\*\* (.+)", line)
            if m:
                ru_key = m.group(1)
                val = m.group(2).strip()
                en_key = field_map.get(ru_key)
                if en_key:
                    parsed[en_key] = val
        result[block_id] = parsed if parsed else None
    return result


def call_claude(png_path: Path, user_text: str) -> tuple[dict | None, float, str]:
    """Запускает claude -p subprocess, возвращает (parsed_json, elapsed_s, raw)."""
    img_data = base64.b64encode(png_path.read_bytes()).decode()
    prompt = f"{SYSTEM_PROMPT}\n\n{user_text}\n\n[IMAGE: data:image/png;base64,{img_data}]"
    t0 = time.monotonic()
    try:
        # Передаём через stdin чтобы не упереться в лимит аргументов (~2MB)
        proc = subprocess.run(
            [CLAUDE_BIN, "-p", "-"],
            input=prompt,
            capture_output=True, text=True, timeout=120,
        )
        elapsed = time.monotonic() - t0
        raw = proc.stdout.strip()
    except subprocess.TimeoutExpired:
        return None, time.monotonic() - t0, "TIMEOUT"
    except Exception as e:
        return None, time.monotonic() - t0, f"ERROR: {e}"

    # Извлекаем JSON из ответа
    json_m = re.search(r"\{.*\}", raw, re.DOTALL)
    if json_m:
        try:
            return json.loads(json_m.group()), elapsed, raw
        except json.JSONDecodeError:
            pass
    return None, elapsed, raw


def field_score(qwen_val, claude_val) -> tuple[str, str]:
    """Возвращает (статус, пояснение) для одного поля."""
    def normalize(v):
        if v is None:
            return None
        if isinstance(v, list):
            return [str(x).strip().lower() for x in v if x]
        return str(v).strip().lower()

    q = normalize(qwen_val)
    c = normalize(claude_val)

    if q == c:
        return "=", ""
    if q in (None, [], "", "null") and c not in (None, [], "", "null"):
        return "QWEN_MISS", f"Claude: {claude_val}"
    if c in (None, [], "", "null") and q not in (None, [], "", "null"):
        return "CLAUDE_MISS", f"Qwen: {qwen_val}"
    return "DIFF", f"Q={qwen_val} | C={claude_val}"


def is_qwen_template(qwen_data: dict | None) -> bool:
    """True если Qwen вернул шаблон вместо реального ответа."""
    if not qwen_data:
        return True
    # Шаблонные значения из USER_INSTRUCTION
    template_markers = {
        "block_type": "план|план_армирования|план_опалубки|разрез|сечение|ведомость|схема|таблица|узел|другое",
        "subject": "до 120 симв — что изображено",
        "marks": "марки/позиции",
    }
    for field, marker in template_markers.items():
        val = qwen_data.get(field, "")
        if isinstance(val, str) and marker.lower() in val.lower():
            return True
    return False


def compare_blocks(project_path: Path, n: int, block_ids_filter: list[str] | None):
    out_dir = project_path / "_output"
    summary_path = out_dir / "qwen_enrichment_summary.json"
    graph_path = out_dir / "document_graph.json"
    blocks_dir = out_dir / "blocks_qwen_300"
    blocks_100_dir = out_dir / "blocks_qwen_100"

    # Ищем MD файл
    md_files = list(project_path.glob("*_document.md"))
    if not md_files:
        print(f"[ERROR] MD файл не найден в {project_path}")
        sys.exit(1)
    md_path = md_files[0]

    summary = json.loads(summary_path.read_text())
    graph = json.loads(graph_path.read_text())
    _idx300 = json.loads((blocks_dir / "index.json").read_text())
    index_300 = {b["block_id"]: b for b in (_idx300.get("blocks") or _idx300 if isinstance(_idx300, dict) else _idx300)}
    _idx100 = json.loads((blocks_100_dir / "index.json").read_text())
    index_100 = {b["block_id"]: b for b in (_idx100.get("blocks") or _idx100 if isinstance(_idx100, dict) else _idx100)}
    qwen_md = parse_qwen_enrichment_from_md(md_path)

    # Выборка блоков
    all_blocks = summary.get("blocks", [])
    if block_ids_filter:
        all_blocks = [b for b in all_blocks if b["block_id"] in block_ids_filter]
    else:
        # Берём разнообразную выборку: сначала проблемные (template/missing), потом OK
        template_ids = [b["block_id"] for b in all_blocks if is_qwen_template(qwen_md.get(b["block_id"]))]
        ok_ids = [b["block_id"] for b in all_blocks if b["block_id"] not in template_ids]
        # До половины выборки — проблемные, остальное — случайные OK
        import random
        random.seed(42)
        pick_template = template_ids[:min(len(template_ids), n // 2)]
        pick_ok = random.sample(ok_ids, min(len(ok_ids), n - len(pick_template)))
        selected_ids = set(pick_template + pick_ok)
        all_blocks = [b for b in all_blocks if b["block_id"] in selected_ids]

    all_blocks = all_blocks[:n]

    print(f"\n{'='*70}")
    print(f"Проект: {project_path.name}")
    print(f"MD: {md_path.name}")
    print(f"Блоков для сравнения: {len(all_blocks)}")
    print(f"{'='*70}\n")

    stats = {"total": 0, "qwen_template": 0, "claude_fail": 0, "fields_eq": 0, "fields_total": 0}

    for i, block_meta in enumerate(all_blocks, 1):
        block_id = block_meta["block_id"]
        # page хранится в index, а не в summary.blocks
        idx_item = index_300.get(block_id) or index_100.get(block_id) or {}
        page = idx_item.get("page") or block_meta.get("page", 0)

        # PNG: предпочитаем 300 DPI, fallback на 100
        png_300 = blocks_dir / f"block_{block_id}.png"
        png_100 = blocks_100_dir / f"block_{block_id}.png"
        png_path = png_300 if png_300.exists() else png_100
        if not png_path.exists():
            print(f"[{i}/{len(all_blocks)}] {block_id} — PNG не найден, пропуск")
            continue

        # Метаданные блока (idx_item уже определён выше)
        ocr_label = idx_item.get("ocr_label", "")
        page_text = load_graph_page_text(graph, page)
        sheet_no = load_graph_sheet_no(graph, page)

        user_text = USER_INSTRUCTION.format(
            block_id=block_id, page=page, sheet_no=sheet_no,
            ocr_label=ocr_label, page_text=page_text,
        )

        # Qwen результат из MD
        qwen_data = qwen_md.get(block_id)
        qwen_is_template = is_qwen_template(qwen_data)

        print(f"[{i}/{len(all_blocks)}] {block_id} (стр.{page}) — отправляю Claude...", flush=True)

        claude_data, elapsed, raw = call_claude(png_path, user_text)

        stats["total"] += 1
        if qwen_is_template:
            stats["qwen_template"] += 1
        if claude_data is None:
            stats["claude_fail"] += 1

        # Вывод
        print(f"  PNG: {png_path.name} ({idx_item.get('size_kb', '?')} KB)")
        print(f"  OCR label: {ocr_label[:80]}")
        print(f"  Claude: {elapsed:.1f}s | {'OK' if claude_data else 'FAIL'}")
        if qwen_is_template:
            print(f"  Qwen:   ШАБЛОН (не распознал)")
        print()

        # Сравнение по полям
        print(f"  {'ПОЛЕ':<22} {'СТАТУС':<14} ДЕТАЛИ")
        print(f"  {'-'*60}")
        for field in SCHEMA_FIELDS:
            q_val = qwen_data.get(field) if qwen_data and not qwen_is_template else "—"
            c_val = claude_data.get(field) if claude_data else "—"
            status, detail = field_score(q_val, c_val)
            stats["fields_total"] += 1
            if status == "=":
                stats["fields_eq"] += 1
            marker = "  " if status == "=" else "! " if "MISS" in status else "? "
            print(f"  {marker}{field:<20} {status:<14} {detail[:60]}")

        print()
        if claude_data is None:
            print(f"  [RAW Claude output]: {raw[:300]}")
        print()

    # Итог
    print(f"{'='*70}")
    print(f"ИТОГО: {stats['total']} блоков")
    print(f"  Qwen шаблон (не распознал): {stats['qwen_template']}")
    print(f"  Claude fail:                {stats['claude_fail']}")
    if stats["fields_total"]:
        pct = 100 * stats["fields_eq"] / stats["fields_total"]
        print(f"  Совпадение полей:           {stats['fields_eq']}/{stats['fields_total']} ({pct:.0f}%)")
    print()


def main():
    parser = argparse.ArgumentParser(description="Сравнение Qwen vs Claude по блокам")
    parser.add_argument("project_path", help="Путь к папке проекта")
    parser.add_argument("--n", type=int, default=20, help="Кол-во блоков (default: 20)")
    parser.add_argument("--block-ids", nargs="*", help="Конкретные block_id для сравнения")
    args = parser.parse_args()

    project_path = Path(args.project_path)
    if not project_path.exists():
        print(f"[ERROR] Путь не найден: {project_path}")
        sys.exit(1)

    compare_blocks(project_path, args.n, args.block_ids)


if __name__ == "__main__":
    main()
