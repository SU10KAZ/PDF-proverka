"""
build_canonical_memory_v2.py
-----------------------------
V2: строит canonical memory из typed_facts формата (entity_mentions + relation_mentions).

Отличие от v1:
- Читает typed_facts_batch_*.json (новый формат)
- Использует entity_mentions с exact_keys + attributes[] + source_context
- Поддерживает relation_mentions (особенно duplicate_identifier_with)
- Использует uncertainty_events для downgrade confidence
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


def normalize_line_id(raw: str) -> str:
    """Нормализовать идентификатор кабельной линии."""
    if not raw:
        return ""
    s = raw.strip().upper()
    # Латиница M/M → кириллица М (частая OCR ошибка)
    s = s.replace("M", "М")
    # Добавить дефис если отсутствует
    s = re.sub(r"^(М)(\d)", r"\1-\2", s)
    return s


def normalize_panel_id(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_breaker_id(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip().upper().replace(" ", "")
    return s


class CanonicalMemory:
    """
    Canonical project memory — группировка упоминаний по каноническим ключам.

    Структура:
      mentions_by_key[entity_type][canonical_key] = [mention1, mention2, ...]
      relations = [relation1, relation2, ...]
      uncertainties = [uncertainty1, ...]
    """

    def __init__(self):
        self.mentions_by_key = defaultdict(lambda: defaultdict(list))
        self.relations = []
        self.uncertainties = []
        self._all_mentions_by_id = {}  # mention_id → mention (для relation lookup)

    def _get_attr(self, mention: dict, attr_name: str) -> tuple[object, dict | None]:
        """Вернуть (value_norm, attribute_obj) для атрибута по имени."""
        for attr in mention.get("attributes", []):
            if attr.get("name") == attr_name:
                return attr.get("value_norm"), attr
        return None, None

    def _canonical_key(self, mention: dict) -> str:
        """Определить канонический ключ для упоминания."""
        entity_type = mention.get("entity_type")
        exact = mention.get("exact_keys", {}) or {}

        if entity_type == "line":
            line_id = exact.get("line_id")
            if line_id:
                return normalize_line_id(line_id)
        elif entity_type == "panel":
            panel_id = exact.get("panel_id")
            if panel_id:
                return normalize_panel_id(panel_id)
        elif entity_type == "breaker":
            breaker_id = exact.get("breaker_id")
            if breaker_id:
                return normalize_breaker_id(breaker_id)
        elif entity_type == "current_transformer":
            ct_id = exact.get("ct_id")
            if ct_id:
                return normalize_breaker_id(ct_id)  # такая же логика
        elif entity_type == "room":
            room_no = exact.get("room_no")
            if room_no:
                return str(room_no).strip()
        elif entity_type == "spec_row":
            spec_pos = exact.get("spec_position")
            if spec_pos:
                return normalize_panel_id(spec_pos)

        # Fallback — normalized_label
        return (mention.get("normalized_label") or "").strip().upper()

    def add_mention(self, mention: dict, batch_source: str):
        """Добавить entity_mention в canonical memory."""
        entity_type = mention.get("entity_type")
        if not entity_type:
            return

        key = self._canonical_key(mention)
        if not key:
            return  # без ключа не добавляем

        # Обогащаем mention мета-информацией
        enriched = dict(mention)
        enriched["_canonical_key"] = key
        enriched["_batch_source"] = batch_source

        self.mentions_by_key[entity_type][key].append(enriched)
        self._all_mentions_by_id[mention.get("mention_id")] = enriched

    def add_relation(self, relation: dict):
        self.relations.append(relation)

    def add_uncertainty(self, uncertainty: dict):
        self.uncertainties.append(uncertainty)

    def find_mention_by_id(self, mention_id: str) -> dict | None:
        return self._all_mentions_by_id.get(mention_id)

    def get_entities(self, entity_type: str) -> dict[str, list[dict]]:
        """Вернуть все canonical entities данного типа: {key: [mentions]}."""
        return dict(self.mentions_by_key.get(entity_type, {}))

    def get_relations_by_type(self, relation_type: str) -> list[dict]:
        return [r for r in self.relations if r.get("relation_type") == relation_type]

    def to_dict(self) -> dict:
        """Сериализовать в dict для JSON."""
        out = {
            "mentions_by_key": {
                entity_type: dict(entities)
                for entity_type, entities in self.mentions_by_key.items()
            },
            "relations": self.relations,
            "uncertainties": self.uncertainties,
        }
        return out

    def stats(self) -> dict:
        return {
            "total_lines": len(self.mentions_by_key.get("line", {})),
            "total_panels": len(self.mentions_by_key.get("panel", {})),
            "total_breakers": len(self.mentions_by_key.get("breaker", {})),
            "total_cts": len(self.mentions_by_key.get("current_transformer", {})),
            "total_notes": sum(
                len(mentions)
                for mentions in self.mentions_by_key.get("note", {}).values()
            ),
            "total_spec_rows": len(self.mentions_by_key.get("spec_row", {})),
            "total_rooms": len(self.mentions_by_key.get("room", {})),
            "total_relations": len(self.relations),
            "total_uncertainties": len(self.uncertainties),
            "duplicate_identifier_relations": len(
                self.get_relations_by_type("duplicate_identifier_with")
            ),
        }


def build_from_typed_facts(project_dir: Path) -> CanonicalMemory:
    """Построить canonical memory из typed_facts_batch_*.json."""
    out = project_dir / "_output"
    memory = CanonicalMemory()

    typed_files = sorted(out.glob("typed_facts_batch_*.json"))

    if not typed_files:
        print(f"[ERROR] Нет typed_facts_batch_*.json в {out}", file=sys.stderr)
        print(f"[INFO]  Сначала запусти extraction pipeline", file=sys.stderr)
        return memory

    for f in typed_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Не могу прочитать {f.name}: {e}", file=sys.stderr)
            continue

        batch_source = f.name

        for mention in data.get("entity_mentions", []):
            memory.add_mention(mention, batch_source)

        for relation in data.get("relation_mentions", []):
            memory.add_relation(relation)

        for uncertainty in data.get("uncertainty_events", []):
            memory.add_uncertainty(uncertainty)

    return memory


def main():
    parser = argparse.ArgumentParser(description="Build canonical memory v2 from typed facts")
    parser.add_argument("project", help="Путь к проекту")
    parser.add_argument("--output", help="Куда сохранить")
    args = parser.parse_args()

    project_dir = Path(args.project)
    if not project_dir.exists():
        print(f"ERROR: {project_dir} не найден", file=sys.stderr)
        sys.exit(1)

    memory = build_from_typed_facts(project_dir)
    stats = memory.stats()

    print(f"\n=== Canonical Memory v2 Stats ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    output_path = Path(args.output) if args.output else project_dir / "_output" / "canonical_memory_v2.json"
    output_path.write_text(
        json.dumps(memory.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\nSaved: {output_path}")


if __name__ == "__main__":
    main()
