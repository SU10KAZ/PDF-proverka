"""
build_canonical_memory.py
--------------------------
Строит canonical project memory из block_extraction_*.json + 01_text_analysis.json.

Canonical memory = нормализованные таблицы сущностей с отслеживанием всех упоминаний
и атрибутов из разных источников. Это основа для candidate generators.

Использование:
    python prototype_v4/build_canonical_memory.py projects/EOM/133_23-ГК-ГРЩ
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


def normalize_line_id(raw: str) -> str:
    """Нормализовать идентификатор кабельной линии.

    Примеры:
      "М-1.1" → "М-1.1"
      "M-1.1" (латиница) → "М-1.1"
      " м-1.1 " → "М-1.1"
      "М1.1" → "М-1.1"
    """
    if not raw:
        return ""
    s = raw.strip().upper()
    # Латиница M → кириллица М
    s = s.replace("M", "М")
    # Добавить дефис если отсутствует между буквой и цифрой
    s = re.sub(r"^(М)(\d)", r"\1-\2", s)
    return s


def normalize_panel_id(raw: str) -> str:
    """Нормализовать идентификатор щита.

    Примеры:
      "ЩУ-2/Т" → "ЩУ-2/Т"
      "щу-2/т" → "ЩУ-2/Т"
      "ВРУ-1 " → "ВРУ-1"
      "RU-1" → "ВРУ-1"  (не делаем — слишком агрессивно)
    """
    if not raw:
        return ""
    s = raw.strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_breaker_id(raw: str) -> str:
    """Нормализовать обозначение автомата.

    Примеры:
      "QF1.1" → "QF1.1"
      "qf1.1" → "QF1.1"
      "QF 1.1" → "QF1.1"
    """
    if not raw:
        return ""
    s = raw.strip().upper().replace(" ", "")
    return s


class CanonicalMemory:
    """Canonical project memory — структурированные сущности проекта."""

    def __init__(self):
        # По каждой сущности храним словарь:
        # canonical_id → {
        #   "mentions": [...],        # все упоминания с источниками
        #   "attributes": {           # атрибуты по полям
        #       "pe_count": [...],    # все значения с evidence
        #       ...
        #   }
        # }
        self.cables = defaultdict(lambda: {"mentions": [], "attributes": defaultdict(list)})
        self.breakers = defaultdict(lambda: {"mentions": [], "attributes": defaultdict(list)})
        self.panels = defaultdict(lambda: {"mentions": [], "attributes": defaultdict(list)})
        self.current_transformers = defaultdict(lambda: {"mentions": [], "attributes": defaultdict(list)})
        self.notes = []  # notes — это список, не dict (нет канонического ключа)

    def add_cable_mention(self, cable_data: dict, source: dict):
        """Добавить упоминание кабеля из extraction.

        cable_data — словарь из extraction (entities.cables[i])
        source — {"block_id", "page", "sheet", "sheet_type"}
        """
        raw_line_id = cable_data.get("line_id") or ""
        line_id = normalize_line_id(raw_line_id)
        if not line_id:
            return  # кабель без идентификатора — пропускаем

        evidence = cable_data.get("evidence") or {}
        mention = {
            "source": source,
            "raw_line_id": raw_line_id,
            "raw_quote": evidence.get("raw_quote"),
            "confidence": evidence.get("confidence"),
            "visual_location": evidence.get("visual_location"),
        }
        self.cables[line_id]["mentions"].append(mention)

        # Атрибуты — каждое значение с привязкой к источнику
        attrs = self.cables[line_id]["attributes"]
        for field in ["mark", "phase_count", "phase_section_mm2",
                      "pe_or_n_count", "pe_or_n_section_mm2", "pe_or_n_type",
                      "source_panel", "destination", "length_m"]:
            value = cable_data.get(field)
            if value is not None:
                attrs[field].append({
                    "value": value,
                    "source": source,
                    "raw_quote": evidence.get("raw_quote"),
                    "confidence": evidence.get("confidence"),
                })

        # Alternate readings — тоже добавляем как отдельные значения
        for alt in cable_data.get("alternate_readings") or []:
            for field, value in alt.items():
                if field in ("confidence", "reason"):
                    continue
                attrs[field].append({
                    "value": value,
                    "source": source,
                    "raw_quote": evidence.get("raw_quote"),
                    "confidence": alt.get("confidence", 0.3),
                    "is_alternate": True,
                    "alternate_reason": alt.get("reason"),
                })

    def add_breaker_mention(self, breaker_data: dict, source: dict):
        raw_designation = breaker_data.get("designation") or ""
        breaker_id = normalize_breaker_id(raw_designation)
        if not breaker_id:
            return

        evidence = breaker_data.get("evidence") or {}
        mention = {
            "source": source,
            "raw_designation": raw_designation,
            "raw_quote": evidence.get("raw_quote"),
            "confidence": evidence.get("confidence"),
        }
        self.breakers[breaker_id]["mentions"].append(mention)

        attrs = self.breakers[breaker_id]["attributes"]
        for field in ["model", "current_rating_a", "trip_type",
                      "location", "protects_line"]:
            value = breaker_data.get(field)
            if value is not None:
                attrs[field].append({
                    "value": value,
                    "source": source,
                    "raw_quote": evidence.get("raw_quote"),
                })

    def add_panel_mention(self, panel_data: dict, source: dict):
        raw_id = panel_data.get("id") or ""
        panel_id = normalize_panel_id(raw_id)
        if not panel_id:
            return

        evidence = panel_data.get("evidence") or {}
        context = evidence.get("context", "unknown")  # spec_row | schema_label | ...

        mention = {
            "source": source,
            "raw_id": raw_id,
            "raw_quote": evidence.get("raw_quote"),
            "context": context,
            "confidence": evidence.get("confidence"),
        }
        self.panels[panel_id]["mentions"].append(mention)

        attrs = self.panels[panel_id]["attributes"]
        for field in ["type", "description", "source_feed", "count_in_spec"]:
            value = panel_data.get(field)
            if value is not None:
                attrs[field].append({
                    "value": value,
                    "source": {**source, "context": context},
                    "raw_quote": evidence.get("raw_quote"),
                })

    def add_note(self, note_data: dict, source: dict):
        """Примечания — просто список, без нормализации ID."""
        self.notes.append({
            "text": note_data.get("text", ""),
            "scope_hint": note_data.get("scope_hint"),
            "category": note_data.get("category", "general"),
            "source": source,
            "raw_quote": (note_data.get("evidence") or {}).get("raw_quote"),
        })

    def add_ct_mention(self, ct_data: dict, source: dict):
        raw_designation = ct_data.get("designation") or ""
        ct_id = normalize_breaker_id(raw_designation)  # TA1.1 → TA1.1
        if not ct_id:
            return

        self.current_transformers[ct_id]["mentions"].append({
            "source": source,
            "raw_quote": (ct_data.get("evidence") or {}).get("raw_quote"),
        })
        attrs = self.current_transformers[ct_id]["attributes"]
        for field in ["model", "primary_current_a", "secondary_current_a",
                      "accuracy_class", "associated_line"]:
            value = ct_data.get(field)
            if value is not None:
                attrs[field].append({"value": value, "source": source})

    def to_dict(self) -> dict:
        """Сериализовать в dict для JSON."""
        return {
            "cables": {k: {
                "mentions": v["mentions"],
                "attributes": {f: vals for f, vals in v["attributes"].items()},
            } for k, v in self.cables.items()},
            "breakers": {k: {
                "mentions": v["mentions"],
                "attributes": {f: vals for f, vals in v["attributes"].items()},
            } for k, v in self.breakers.items()},
            "panels": {k: {
                "mentions": v["mentions"],
                "attributes": {f: vals for f, vals in v["attributes"].items()},
            } for k, v in self.panels.items()},
            "current_transformers": {k: {
                "mentions": v["mentions"],
                "attributes": {f: vals for f, vals in v["attributes"].items()},
            } for k, v in self.current_transformers.items()},
            "notes": self.notes,
        }

    def stats(self) -> dict:
        return {
            "cables": len(self.cables),
            "breakers": len(self.breakers),
            "panels": len(self.panels),
            "current_transformers": len(self.current_transformers),
            "notes": len(self.notes),
            "cables_with_conflicts": sum(
                1 for c in self.cables.values()
                if any(len({a["value"] for a in attrs}) > 1
                       for attrs in c["attributes"].values())
            ),
        }


def build_from_extractions(project_dir: Path) -> CanonicalMemory:
    """Построить canonical memory из block_extraction_*.json.

    Если нет extraction файлов — fallback на block_batch_*.json (старый формат).
    """
    out = project_dir / "_output"
    memory = CanonicalMemory()

    # Новый формат — block_extraction_*.json
    extraction_files = sorted(out.glob("block_extraction_*.json"))

    if not extraction_files:
        print(f"[INFO] Нет block_extraction_*.json, fallback на block_batch_*.json", file=sys.stderr)
        extraction_files = sorted(out.glob("block_batch_*.json"))

    if not extraction_files:
        print(f"[ERROR] Нет extraction/batch файлов в {out}", file=sys.stderr)
        return memory

    for f in extraction_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Не могу прочитать {f.name}: {e}", file=sys.stderr)
            continue

        # Новый формат: block_extractions[]
        blocks_key = "block_extractions" if "block_extractions" in data else "block_analyses"
        blocks = data.get(blocks_key, [])

        for block in blocks:
            source = {
                "block_id": block.get("block_id", "?"),
                "page": block.get("page"),
                "sheet": block.get("sheet"),
                "sheet_type": block.get("sheet_type"),
            }
            entities = block.get("entities", {})

            for cable in entities.get("cables", []):
                memory.add_cable_mention(cable, source)
            for breaker in entities.get("breakers", []):
                memory.add_breaker_mention(breaker, source)
            for panel in entities.get("panels", []):
                memory.add_panel_mention(panel, source)
            for note in entities.get("notes", []):
                memory.add_note(note, source)
            for ct in entities.get("current_transformers", []):
                memory.add_ct_mention(ct, source)

    return memory


def main():
    parser = argparse.ArgumentParser(description="Build canonical project memory")
    parser.add_argument("project", help="Путь к проекту")
    parser.add_argument("--output", help="Куда сохранить (default: _output/canonical_memory.json)")
    args = parser.parse_args()

    project_dir = Path(args.project)
    if not project_dir.exists():
        print(f"ERROR: project {project_dir} не найден", file=sys.stderr)
        sys.exit(1)

    memory = build_from_extractions(project_dir)
    stats = memory.stats()

    print(f"\n=== Canonical Memory Stats ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # Save
    output_path = Path(args.output) if args.output else project_dir / "_output" / "canonical_memory.json"
    output_path.write_text(
        json.dumps(memory.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\nSaved: {output_path}")


if __name__ == "__main__":
    main()
