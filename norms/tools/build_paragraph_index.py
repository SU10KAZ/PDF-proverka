"""Парсит все пункты из всех норм vault'а в единый индекс.

Вход: active_norms.json + .md-файлы в vault'е.
Выход: paragraphs.jsonl — по строке на пункт:
    {"code", "paragraph", "text", "file", "line"}

Пункт — строка, начинающаяся с номера вида "N", "N.N", "N.N.N" (возможно
в markdown-заголовке или в **bold**). Текст пункта — от найденной строки
до следующего пункта / страницы / блока / заголовка.

При повторах одного и того же номера оставляем запись с самым длинным текстом
(обычно это "настоящая" версия, а не упоминание в оглавлении).

Использование:
    python3 build_paragraph_index.py                # полный прогон
    python3 build_paragraph_index.py --limit 5      # первые 5 норм (debug)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from parse_filename import parse_filename  # noqa: E402
from find_paragraph import is_boundary, _strip_md_decoration, _HEADING_RE  # noqa: E402

VAULT = Path(__file__).resolve().parent.parent / "vault"
ACTIVE_JSON = Path(__file__).parent / "active_norms.json"
OUTPUT_JSONL = Path(__file__).parent / "paragraphs.jsonl"

NUMBER_RE = re.compile(r"^(\d+(?:\.\d+)*)\b")
DATE_RE = re.compile(r"^\d{1,2}\.\d{1,2}\.(19|20)\d{2}$")  # дд.мм.гггг
DATE_PARTIAL_RE = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{2}$")  # дд.мм.гг
# После одноцифрового номера — месяц/год значит это дата, а не пункт
MONTH_WORDS_RE = re.compile(
    r"^\s*(январ|феврал|март|апрел|мая?|июн|июл|август|сентябр|октябр|ноябр|декабр)",
    re.IGNORECASE,
)
YEAR_WORDS_RE = re.compile(r"^\s*(г\.|года|год\b)", re.IGNORECASE)

MIN_TEXT_LEN = 30  # очень короткие "пункты" (только номер) отбрасываем
MAX_PARAGRAPH_DEPTH = 6  # 1.2.3.4.5.6 — дальше уже абсурд


def is_valid_paragraph_number(num: str, rest_of_line: str, is_heading: bool) -> bool:
    """Фильтрует ложные срабатывания (даты, номера годов, '1 января'...)."""
    # Дата целиком (дд.мм.гггг или дд.мм.гг)
    if DATE_RE.match(num) or DATE_PARTIAL_RE.match(num):
        return False
    # Слишком глубокая вложенность (обычно фальш-позитив)
    if num.count(".") >= MAX_PARAGRAPH_DEPTH:
        return False
    # Одноцифровый номер: строгие требования
    if "." not in num:
        # Следом месяц или "года" → это дата ("1 января 1986 года")
        if MONTH_WORDS_RE.match(rest_of_line) or YEAR_WORDS_RE.match(rest_of_line):
            return False
        try:
            if int(num) > 50:  # разделов с номером >50 не бывает
                return False
        except ValueError:
            return False
        # Не-heading принимается только если текст продолжается "." или ")" после номера
        # Пример: "1. Настоящий стандарт..." → ок; "1 января" → не ок (уже отбросили)
        if not is_heading and not rest_of_line.startswith((".", ")")):
            return False
    return True


def find_paragraph_starts(lines: list[str]) -> list[tuple[int, str, bool]]:
    """Находит все строки, которые выглядят как начало пункта."""
    result: list[tuple[int, str, bool]] = []
    for i, line in enumerate(lines):
        h = _HEADING_RE.match(line)
        if h:
            body = _strip_md_decoration(line[h.end():])
            m = NUMBER_RE.match(body)
            if m:
                num = m.group(1)
                rest = body[m.end():]
                if is_valid_paragraph_number(num, rest, is_heading=True):
                    result.append((i, num, True))
                continue
        stripped = _strip_md_decoration(line)
        m = NUMBER_RE.match(stripped)
        if m:
            num = m.group(1)
            rest = stripped[m.end():]
            if is_valid_paragraph_number(num, rest, is_heading=False):
                result.append((i, num, False))
    return result


def collect_paragraph_text(lines: list[str], start_idx: int, paragraph: str) -> str:
    """Собирает текст пункта от start_idx до ближайшей границы."""
    collected = [lines[start_idx]]
    for j in range(start_idx + 1, len(lines)):
        if is_boundary(lines[j], paragraph):
            break
        collected.append(lines[j])
    # Удаляем пустые trailing строки
    while collected and not collected[-1].strip():
        collected.pop()
    return "\n".join(collected).strip()


def extract_paragraphs(content: str, code: str, file: str) -> list[dict]:
    """Извлекает все пункты из одной нормы. Дедуп по (code, paragraph) → самый длинный."""
    lines = content.splitlines()
    starts = find_paragraph_starts(lines)

    best: dict[str, dict] = {}  # paragraph_num → best record
    for line_idx, para_num, is_head in starts:
        text = collect_paragraph_text(lines, line_idx, para_num)
        if len(text) < MIN_TEXT_LEN:
            continue
        prev = best.get(para_num)
        if prev is None or len(text) > len(prev["text"]):
            best[para_num] = {
                "code": code,
                "paragraph": para_num,
                "text": text,
                "file": file,
                "line": line_idx + 1,
            }
    return list(best.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="обработать только N норм (для отладки)")
    args = ap.parse_args()

    data = json.loads(ACTIVE_JSON.read_text(encoding="utf-8"))
    norms = data["norms"]
    if args.limit:
        norms = norms[: args.limit]

    total_paragraphs = 0
    skipped_files = 0
    per_norm_counts: list[tuple[str, int]] = []

    with OUTPUT_JSONL.open("w", encoding="utf-8") as out:
        for n in norms:
            # Пропускаем low_confidence (приказы/постановления — у них нет пунктов в стандартном формате)
            if n.get("parse_confidence") == "low":
                skipped_files += 1
                continue
            path = VAULT / n["file"]
            if not path.exists():
                skipped_files += 1
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except Exception as e:
                print(f"WARN: {n['file']}: {e}", file=sys.stderr)
                skipped_files += 1
                continue
            paragraphs = extract_paragraphs(content, n["code"], n["file"])
            per_norm_counts.append((n["code"], len(paragraphs)))
            for p in paragraphs:
                out.write(json.dumps(p, ensure_ascii=False) + "\n")
                total_paragraphs += 1

    per_norm_counts.sort(key=lambda x: -x[1])
    print(f"Норм обработано: {len(norms) - skipped_files}", file=sys.stderr)
    print(f"Пропущено: {skipped_files}", file=sys.stderr)
    print(f"Пунктов всего: {total_paragraphs}", file=sys.stderr)
    print(f"Индекс: {OUTPUT_JSONL}", file=sys.stderr)
    print(f"\nТоп-5 норм по кол-ву пунктов:", file=sys.stderr)
    for code, cnt in per_norm_counts[:5]:
        print(f"  {cnt:5d}  {code}", file=sys.stderr)
    print(f"\nТоп-5 самых маленьких:", file=sys.stderr)
    for code, cnt in per_norm_counts[-5:]:
        print(f"  {cnt:5d}  {code}", file=sys.stderr)


if __name__ == "__main__":
    main()
