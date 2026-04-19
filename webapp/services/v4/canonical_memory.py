"""
canonical_memory.py — Stage 2 v4 pipeline.

Строит canonical memory из typed_facts_batch_*.json:
- группирует entity_mentions по entity_type + canonical_key
- нормализует синонимы имён атрибутов (разные модели используют разные термины)
- нормализует entity_type cable→line
- хранит relations и uncertainty_events

Используется generators.py для поиска кандидатов замечаний.
"""
from __future__ import annotations

import json
import re
import logging
from collections import defaultdict
from pathlib import Path

log = logging.getLogger(__name__)


def normalize_line_id(raw: str) -> str:
    """Нормализовать идентификатор кабельной линии."""
    if not raw:
        return ""
    s = str(raw).strip().upper()
    # Латиница M → кириллица М (частая OCR ошибка)
    s = s.replace("M", "М")
    # Добавить дефис если отсутствует (М1.1 → М-1.1)
    s = re.sub(r"^(М)(\d)", r"\1-\2", s)
    return s


def normalize_panel_id(raw) -> str:
    if not raw:
        return ""
    s = str(raw).strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_breaker_id(raw) -> str:
    if not raw:
        return ""
    s = str(raw).strip().upper().replace(" ", "")
    return s


# Алиасы имён атрибутов (fallback — EOM).
# Дисциплинарные алиасы загружаются из v4_config.canonical_memory.attr_aliases
_ATTR_ALIASES_DEFAULT = {
    "model": "breaker_model",
    "nominal_A": "breaker_nominal_a",
    "nominal_a": "breaker_nominal_a",
    "text_verbatim": "note_text",
    "text": "note_text",
    "name": "designation",
}

# Entity type aliases (fallback — EOM)
_ENTITY_TYPE_ALIASES_DEFAULT = {
    "cable": "line",
}


# OCR-нормализация: латинские буквы, визуально идентичные кириллическим,
# приводим к кириллице. Это устраняет ложные mismatches вида
# "ППГнг(A)-HF" vs "ППГнг(А)-HF" (латинская A vs кириллическая А).
_LATIN_TO_CYRILLIC = str.maketrans({
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н",
    "K": "К", "M": "М", "O": "О", "P": "Р", "T": "Т",
    "X": "Х", "Y": "У",
    "a": "а", "c": "с", "e": "е", "o": "о", "p": "р",
    "x": "х", "y": "у",
})


def normalize_ocr_text(value) -> object:
    """Нормализовать текстовое значение для устойчивого сравнения.

    - Латиница → кириллица для визуально идентичных букв (A→А, M→М, ...)
    - Сжимаем whitespace
    - Приводим к .strip()

    Нечтекстовые значения возвращаются без изменений.
    """
    if not isinstance(value, str):
        return value
    s = value.strip().translate(_LATIN_TO_CYRILLIC)
    # Сжимаем пробелы внутри
    s = re.sub(r"\s+", " ", s)
    return s


# Поля для OCR-нормализации (fallback — EOM)
_OCR_NORMALIZED_DEFAULT = {
    "cable_mark",
    "breaker_model",
    "designation",
}


class CanonicalMemory:
    """Группировка упоминаний по каноническим ключам.

    Структура:
      mentions_by_key[entity_type][canonical_key] = [mention1, mention2, ...]
      relations = [relation1, relation2, ...]
      uncertainties = [uncertainty1, ...]
    """

    def __init__(self, config: dict | None = None):
        self.mentions_by_key = defaultdict(lambda: defaultdict(list))
        self.relations: list[dict] = []
        self.uncertainties: list[dict] = []
        self._all_mentions_by_id: dict[str, dict] = {}
        self._config = config or {}

        # Загружаем aliases из config
        cm_cfg = self._config.get("canonical_memory", {})
        self._entity_type_aliases = cm_cfg.get("entity_type_aliases", _ENTITY_TYPE_ALIASES_DEFAULT)
        self._attr_aliases = cm_cfg.get("attr_aliases", _ATTR_ALIASES_DEFAULT)
        self._ocr_fields = set(cm_cfg.get("ocr_normalized_fields", _OCR_NORMALIZED_DEFAULT))

        # exact_key_fields из config.extraction
        self._exact_key_fields = self._config.get("extraction", {}).get("exact_key_fields", {})

    def _canonical_key(self, mention: dict) -> str:
        entity_type = mention.get("entity_type")
        exact = mention.get("exact_keys", {}) or {}

        # Config-driven: если в exact_key_fields указано поле для этого типа
        if self._exact_key_fields and entity_type in self._exact_key_fields:
            key_field = self._exact_key_fields[entity_type]
            val = exact.get(key_field)
            if val:
                return normalize_line_id(val) if "line" in key_field or "id" in key_field else normalize_panel_id(val)

        # Hardcoded EOM fallback
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
                return normalize_breaker_id(ct_id)
        elif entity_type == "room":
            room_no = exact.get("room_no")
            if room_no:
                return str(room_no).strip()
        elif entity_type == "spec_row":
            spec_pos = exact.get("spec_position")
            if spec_pos:
                return normalize_panel_id(spec_pos)

        # Fallback — normalized_label
        return str(mention.get("normalized_label") or "").strip().upper()

    def add_mention(self, mention: dict, batch_source: str):
        entity_type = mention.get("entity_type")
        if not entity_type:
            return

        # Entity type aliases (config-driven, fallback: cable → line)
        alias = self._entity_type_aliases.get(entity_type)
        if alias:
            entity_type = alias
            mention = {**mention, "entity_type": alias}

        # Нормализация имён атрибутов + OCR-нормализация значений (config-driven)
        if mention.get("attributes"):
            new_attrs = []
            for a in mention["attributes"]:
                aname = a.get("name")
                if aname in self._attr_aliases:
                    aname = self._attr_aliases[aname]
                    a = {**a, "name": aname}
                # OCR-нормализация значений для текстовых полей
                if aname in self._ocr_fields:
                    a = {**a}
                    if a.get("value_norm") is not None:
                        a["value_norm"] = normalize_ocr_text(a["value_norm"])
                    if a.get("value_raw") is not None:
                        a["value_raw"] = normalize_ocr_text(a["value_raw"])
                new_attrs.append(a)
            mention = {**mention, "attributes": new_attrs}

        key = self._canonical_key(mention)
        if not key:
            return

        enriched = dict(mention)
        enriched["_canonical_key"] = key
        enriched["_batch_source"] = batch_source

        self.mentions_by_key[entity_type][key].append(enriched)
        mid = mention.get("mention_id")
        if mid:
            self._all_mentions_by_id[mid] = enriched

    def add_relation(self, relation: dict):
        self.relations.append(relation)

    def add_uncertainty(self, uncertainty: dict):
        self.uncertainties.append(uncertainty)

    def find_mention_by_id(self, mention_id: str) -> dict | None:
        return self._all_mentions_by_id.get(mention_id)

    def get_entities(self, entity_type: str) -> dict[str, list[dict]]:
        return dict(self.mentions_by_key.get(entity_type, {}))

    def get_relations_by_type(self, relation_type: str) -> list[dict]:
        return [r for r in self.relations if r.get("relation_type") == relation_type]

    def to_dict(self) -> dict:
        return {
            "mentions_by_key": {
                entity_type: dict(entities)
                for entity_type, entities in self.mentions_by_key.items()
            },
            "relations": self.relations,
            "uncertainties": self.uncertainties,
        }

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


_TEXT_REF_PREFIX = "TEXT_REF:"


def _load_text_block_map(output_dir: Path) -> dict[str, str]:
    """id текстового блока → его полный текст из document_graph.json."""
    graph_path = output_dir / "document_graph.json"
    if not graph_path.exists():
        return {}
    try:
        graph = json.loads(graph_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, str] = {}
    for page in graph.get("pages", []):
        for tb in page.get("text_blocks", []):
            tb_id = tb.get("id")
            text = tb.get("text")
            if tb_id and text:
                out[tb_id] = text
    return out


def _resolve_text_refs(mention: dict, text_blocks: dict[str, str]) -> dict:
    """Развернуть TEXT_REF:<id> в attributes/raw_text_excerpt в полный текст."""
    if not text_blocks:
        return mention

    def _resolve(val):
        if isinstance(val, str) and val.startswith(_TEXT_REF_PREFIX):
            ref = val[len(_TEXT_REF_PREFIX):].strip()
            return text_blocks.get(ref, val)
        return val

    attrs = mention.get("attributes")
    if attrs:
        new_attrs = []
        for a in attrs:
            a2 = a
            if isinstance(a.get("value_raw"), str):
                resolved = _resolve(a["value_raw"])
                if resolved is not a["value_raw"]:
                    a2 = {**a, "value_raw": resolved}
            new_attrs.append(a2)
        mention = {**mention, "attributes": new_attrs}

    excerpt = mention.get("raw_text_excerpt")
    if isinstance(excerpt, str):
        resolved = _resolve(excerpt)
        if resolved is not excerpt:
            mention = {**mention, "raw_text_excerpt": resolved}

    return mention


def build_from_typed_facts(output_dir: Path, config: dict | None = None) -> CanonicalMemory:
    """Построить canonical memory из typed_facts_batch_*.json в output_dir."""
    memory = CanonicalMemory(config=config)

    typed_files = sorted(output_dir.glob("typed_facts_batch_*.json"))
    if not typed_files:
        log.error(f"Нет typed_facts_batch_*.json в {output_dir}")
        return memory

    text_blocks = _load_text_block_map(output_dir)

    for f in typed_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"Не могу прочитать {f.name}: {e}")
            continue

        batch_source = f.name

        for mention in data.get("entity_mentions", []):
            mention = _resolve_text_refs(mention, text_blocks)
            memory.add_mention(mention, batch_source)

        for relation in data.get("relation_mentions", []):
            memory.add_relation(relation)

        for uncertainty in data.get("uncertainty_events", []):
            memory.add_uncertainty(uncertainty)

    return memory


def save_canonical_memory(memory: CanonicalMemory, output_dir: Path) -> Path:
    """Сохранить canonical memory в output_dir/canonical_memory_v2.json."""
    path = output_dir / "canonical_memory_v2.json"
    path.write_text(
        json.dumps(memory.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path
