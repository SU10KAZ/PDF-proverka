#!/usr/bin/env python3
"""reserc.md #102/#106 — реестр env-флагов: «флаг → читатель → дефолт → статус».

Находки #102/#106: в проде ~190 STAGE_COMPARISON_-флагов + десятки прочих, среди
них orphan (объявлены в .env/.env.example, но никто не читает) и вводящие в
заблуждение хардкоды; **нет реестра** «флаг→читатель→дефолт». Этот скрипт его
строит.

READ-ONLY. Ничего не мутирует. `.env` — живой прод-конфиг (gitignore), он не
редактируется; скрипт лишь сверяет с ним код и сообщает:

  - orphan_in_env     — раскомментирован в .env/.env.example, но НЕТ читателя в
                        коде (кандидат на удаление — вручную, осознанно);
  - read_only_default — читается в коде, но нет ни в .env, ни в .env.example
                        (живёт на дефолте из кода — это нормально, но полезно
                        видеть для документации);
  - documented        — есть и в коде, и в env-файле.

Использование:
    python scripts/audit_env_flags.py                 # человекочитаемый отчёт
    python scripts/audit_env_flags.py --json out.json # + машинный реестр
    python scripts/audit_env_flags.py --orphans-only  # только orphan_in_env
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS = ("backend", "scripts")

# Чтения env в коде. Кроме прямых os.getenv/os.environ.get, флаги в этом проекте
# читаются через хелперы-обёртки: _env_bool / _env_int / _env_str /
# _env_bool_runtime / env_flag / get_env_* и т.п. Их тоже надо учесть, иначе
# живые флаги ложно попадут в orphan.
_READ_CALL = re.compile(
    r"""(?:(?:os\.)?(?:getenv|environ\.get)|_?env_[a-z_]+|get_env_[a-z_]+)\(\s*["']([A-Z][A-Z0-9_]{2,})["']\s*(?:,\s*([^)\n]*?))?\s*\)"""
)
_READ_SUBSCRIPT = re.compile(r"""os\.environ\[\s*["']([A-Z][A-Z0-9_]{2,})["']\s*\]""")
# Любое появление имени флага строковым литералом (страховка от косвенных чтений:
# settings.X, pydantic, membership-проверки, конструкции по константе-имени).
_NAME_LITERAL = re.compile(r"""["']([A-Z][A-Z0-9_]{2,})["']""")

# Строка .env: NAME=value  (активная) или  # NAME=value  (закомментированный пример)
_ENV_ACTIVE = re.compile(r"^([A-Z][A-Z0-9_]{2,})=(.*)$")
_ENV_COMMENTED = re.compile(r"^#\s*([A-Z][A-Z0-9_]{2,})=(.*)$")


def extract_readers(text: str) -> dict[str, list[int]]:
    """{flag: [line_no, ...]} для всех env-чтений в тексте файла."""
    out: dict[str, list[int]] = {}
    for i, line in enumerate(text.splitlines(), start=1):
        for m in _READ_CALL.finditer(line):
            out.setdefault(m.group(1), []).append(i)
        for m in _READ_SUBSCRIPT.finditer(line):
            out.setdefault(m.group(1), []).append(i)
    return out


def extract_defaults(text: str) -> dict[str, str]:
    """{flag: default_expr} — лучший best-effort дефолт из os.getenv(name, default)."""
    out: dict[str, str] = {}
    for m in _READ_CALL.finditer(text):
        name, default = m.group(1), (m.group(2) or "").strip()
        if default and name not in out:
            out[name] = default
    return out


def parse_env_file(text: str) -> tuple[set[str], set[str]]:
    """(active, commented) множества имён флагов из .env-подобного файла."""
    active: set[str] = set()
    commented: set[str] = set()
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        m = _ENV_ACTIVE.match(s)
        if m:
            active.add(m.group(1))
            continue
        mc = _ENV_COMMENTED.match(s)
        if mc:
            commented.add(mc.group(1))
    return active, commented


def classify(
    readers: dict[str, set[str]],
    referenced: set[str],
    defaults: dict[str, str],
    env_active: set[str],
    env_commented: set[str],
    example_active: set[str],
    example_commented: set[str],
) -> dict:
    """Свести всё в реестр. Чистая функция (тестируемая).

    `readers` — точные читатели (файл→строки) из getenv/хелперов.
    `referenced` — флаг встречается строковым литералом где-либо в коде (широкая
    страховка от косвенных чтений). Orphan-гейт строится на `referenced`, чтобы
    живой флаг не попал в orphan из-за нестандартного чтения.
    """
    all_in_env = env_active | env_commented | example_active | example_commented
    all_names = set(readers) | referenced | all_in_env

    registry = {}
    orphans, read_only_default, documented = [], [], []
    for name in sorted(all_names):
        rd = sorted(readers.get(name, []))
        is_referenced = name in referenced or bool(rd)
        in_env_active = name in env_active
        in_any_env = name in all_in_env
        rec = {
            "flag": name,
            "readers": rd,
            "reader_count": len(rd),
            "referenced_in_code": is_referenced,
            "default": defaults.get(name),
            "in_env_active": in_env_active,
            "in_env_commented": name in env_commented,
            "in_example": name in (example_active | example_commented),
        }
        if not is_referenced and in_any_env:
            rec["status"] = "orphan_in_env"
            orphans.append(name)
        elif is_referenced and not in_any_env:
            rec["status"] = "read_only_default"
            read_only_default.append(name)
        else:
            rec["status"] = "documented"
            documented.append(name)
        registry[name] = rec

    return {
        "summary": {
            "total_flags": len(all_names),
            "with_readers": sum(1 for r in registry.values() if r["readers"]),
            "orphan_in_env": len(orphans),
            "read_only_default": len(read_only_default),
            "documented": len(documented),
        },
        "orphan_in_env": orphans,
        "read_only_default": read_only_default,
        "registry": registry,
    }


def build_registry(root: Path = ROOT) -> dict:
    readers: dict[str, set[str]] = {}
    defaults: dict[str, str] = {}
    code_literals: set[str] = set()
    for d in SCAN_DIRS:
        base = root / d
        if not base.exists():
            continue
        for py in base.rglob("*.py"):
            try:
                text = py.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            rel = str(py.relative_to(root))
            for flag in extract_readers(text):
                readers.setdefault(flag, set()).add(rel)
            for flag, dflt in extract_defaults(text).items():
                defaults.setdefault(flag, dflt)
            code_literals.update(_NAME_LITERAL.findall(text))

    def _read(p: Path) -> str:
        try:
            return p.read_text(encoding="utf-8")
        except OSError:
            return ""

    env_active, env_commented = parse_env_file(_read(root / ".env"))
    ex_active, ex_commented = parse_env_file(_read(root / ".env.example"))

    # Кандидаты = env-флаги ∪ точные читатели. referenced — те из кандидатов,
    # чьё имя встречается строковым литералом в коде (широкая страховка).
    candidates = set(readers) | env_active | env_commented | ex_active | ex_commented
    referenced = {n for n in candidates if n in code_literals} | set(readers)

    return classify(
        readers, referenced, defaults,
        env_active, env_commented, ex_active, ex_commented,
    )


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Реестр env-флагов (reserc.md #102/#106)")
    ap.add_argument("--json", metavar="PATH", help="записать полный реестр в JSON")
    ap.add_argument("--orphans-only", action="store_true", help="печатать только orphan_in_env")
    args = ap.parse_args(argv)

    reg = build_registry()
    s = reg["summary"]

    if args.json:
        Path(args.json).write_text(
            json.dumps(reg, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"Реестр записан: {args.json}")

    if args.orphans_only:
        for name in reg["orphan_in_env"]:
            print(name)
        return 0

    print("=== Реестр env-флагов (reserc.md #102/#106) ===")
    print(f"  всего флагов:        {s['total_flags']}")
    print(f"  с читателями в коде: {s['with_readers']}")
    print(f"  orphan_in_env:       {s['orphan_in_env']}  (в env, но никто не читает)")
    print(f"  read_only_default:   {s['read_only_default']}  (читается, но нет в env-файлах)")
    print(f"  documented:          {s['documented']}")
    if reg["orphan_in_env"]:
        print("\n--- ORPHAN (объявлены в .env/.env.example, нет читателя в коде) ---")
        for name in reg["orphan_in_env"]:
            rec = reg["registry"][name]
            where = "active" if rec["in_env_active"] else ("commented" if rec["in_env_commented"] else "example")
            print(f"  {name}  [{where}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
