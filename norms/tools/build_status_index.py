"""Собирает status_index.json — single source of truth по статусам норм.

Приоритет: manual overrides > vault parse.

Вход:
    vault/*.md (кроме MOC-*.md)
    tools/status_overrides.yaml

Выход:
    tools/status_index.json

Правила классификации:
    doc_status      ∈ {active, replaced, cancelled, unknown}
    edition_status  ∈ {current, outdated, unknown, null}
    effective_status = f(doc_status, edition_status):
        replaced        -> replaced
        cancelled       -> cancelled
        active+outdated -> outdated_edition
        active+current/unknown/null -> active
        unknown         -> unknown

Никаких seed/bootstrap из других проектов. Никакого WebSearch.
Если нормы нет ни в vault, ни в overrides — её просто нет в index
(честный сигнал «не покрыто» для intake_missing_norms.py).

Запуск:
    python3 tools/build_status_index.py
    python3 tools/build_status_index.py --quiet
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).parent))
from parse_filename import parse_filename  # noqa: E402

HERE = Path(__file__).resolve().parent
VAULT = HERE.parent / "vault"
OVERRIDES_PATH = HERE / "status_overrides.yaml"
OUTPUT_PATH = HERE / "status_index.json"


# ---------- нормализация / матч-ключи ----------


def _match_key(s: str) -> str:
    """Ключ сравнения: без пробелов, _→., lowercase."""
    if not s:
        return ""
    return re.sub(r"\s+", "", s).replace("_", ".").lower()


def _default_aliases(code: str) -> list[str]:
    aliases: set[str] = set()
    if code:
        aliases.add(code)
        aliases.add(code.replace(".", "_"))
        aliases.add(code.replace("_", "."))
    return sorted(a for a in aliases if a)


# ---------- overrides ----------


def load_overrides_raw() -> dict[str, object]:
    if not OVERRIDES_PATH.exists():
        return {}
    data = yaml.safe_load(OVERRIDES_PATH.read_text(encoding="utf-8")) or {}
    return data.get("overrides") or {}


def _parse_override(raw: object) -> dict[str, Any]:
    """Привести запись overrides-а к унифицированному виду.

    Поддерживает:
      - сокращение "cancelled" (str)
      - сокращение {"replaced_by": "X"}
      - полный dict с полями doc_status/edition_status/aliases/...
    """
    out: dict[str, Any] = {
        "doc_status": "active",
        "edition_status": None,
        "current_version": None,
        "replacement_doc": None,
        "aliases": [],
        "details": None,
        "source_url": None,
        "last_verified": None,
    }
    if raw is None:
        return out
    if isinstance(raw, str):
        val = raw.strip().lower()
        if val == "cancelled":
            out["doc_status"] = "cancelled"
        elif val == "replaced":
            out["doc_status"] = "replaced"
        elif val == "unknown":
            out["doc_status"] = "unknown"
        elif val == "active":
            out["doc_status"] = "active"
        return out
    if not isinstance(raw, dict):
        return out

    # Короткая форма {replaced_by: X}
    if "replaced_by" in raw and raw["replaced_by"]:
        out["doc_status"] = "replaced"
        out["replacement_doc"] = str(raw["replaced_by"]).strip()

    # Длинная форма
    status_value = raw.get("doc_status", raw.get("status"))
    if status_value:
        v = str(status_value).strip().lower()
        if v in {"active", "replaced", "cancelled", "unknown"}:
            out["doc_status"] = v

    if "edition_status" in raw:
        ev = raw["edition_status"]
        if ev is None:
            out["edition_status"] = None
        else:
            es = str(ev).strip().lower()
            if es in {"current", "outdated", "unknown"}:
                out["edition_status"] = es
            else:
                out["edition_status"] = None

    for field in ("current_version", "replacement_doc", "details", "source_url", "last_verified"):
        if field in raw and raw[field] is not None:
            out[field] = str(raw[field]).strip() or None

    if "aliases" in raw and isinstance(raw["aliases"], list):
        out["aliases"] = [str(a).strip() for a in raw["aliases"] if str(a).strip()]

    # Для replaced/cancelled edition_status обычно неприменим
    if out["doc_status"] in {"replaced", "cancelled"} and out["edition_status"] is None:
        pass  # оставляем null — это корректно

    return out


def _effective_status(doc_status: str, edition_status: str | None) -> str:
    if doc_status == "replaced":
        return "replaced"
    if doc_status == "cancelled":
        return "cancelled"
    if doc_status == "active":
        if edition_status == "outdated":
            return "outdated_edition"
        return "active"
    return "unknown"


# ---------- сборка ----------


def build_index() -> dict:
    overrides_raw = load_overrides_raw()
    overrides_by_key: dict[str, tuple[str, dict]] = {}
    for raw_code, raw_val in overrides_raw.items():
        parsed = _parse_override(raw_val)
        overrides_by_key[_match_key(str(raw_code))] = (str(raw_code), parsed)

    entries: list[dict] = []
    parse_failures: list[str] = []
    consumed_override_keys: set[str] = set()

    for md in sorted(VAULT.glob("*.md")):
        if md.name.startswith("MOC - "):
            continue
        parsed = parse_filename(md.name)
        code = parsed["code"]
        if parsed["parse_confidence"] == "low":
            parse_failures.append(md.name)

        key = _match_key(code)
        override = overrides_by_key.get(key)
        if override:
            _, od = override
            consumed_override_keys.add(key)
            doc_status = od["doc_status"]
            edition_status = od["edition_status"]
            replacement_doc = od["replacement_doc"]
            current_version = od["current_version"] or (
                code if doc_status == "active" and edition_status != "outdated"
                else replacement_doc if doc_status == "replaced"
                else None
            )
            details = od["details"]
            source_url = od["source_url"]
            last_verified = od["last_verified"]
            extra_aliases = od["aliases"]
        else:
            doc_status = "active"
            edition_status = None
            replacement_doc = None
            current_version = code
            details = None
            source_url = None
            last_verified = None
            extra_aliases = []

        aliases = sorted(set(_default_aliases(code)) | set(extra_aliases))

        entries.append(
            {
                "code": code,
                "aliases": aliases,
                "type": parsed["type"],
                "year": parsed["year"],
                "title": parsed["title"],
                "file": parsed["file"],
                "doc_status": doc_status,
                "edition_status": edition_status,
                "replacement_doc": replacement_doc,
                "current_version": current_version,
                "details": details,
                "source_url": source_url,
                "last_verified": last_verified,
                "parse_confidence": parsed["parse_confidence"],
                "source": "vault",
                "authoritative": True,
                "has_text": True,
            }
        )

    # Override-only: код есть в overrides.yaml, файла в vault нет.
    override_only_codes: list[str] = []
    for key, (original_code, od) in overrides_by_key.items():
        if key in consumed_override_keys:
            continue
        doc_status = od["doc_status"]
        if doc_status == "active" and od["edition_status"] != "outdated":
            # override-only ACTIVE без файла не имеет смысла — пропускаем.
            continue
        replacement_doc = od["replacement_doc"]
        current_version = od["current_version"] or (
            replacement_doc if doc_status == "replaced" else None
        )
        aliases = sorted(set(_default_aliases(original_code)) | set(od["aliases"]))
        entries.append(
            {
                "code": original_code,
                "aliases": aliases,
                "type": None,
                "year": None,
                "title": None,
                "file": None,
                "doc_status": doc_status,
                "edition_status": od["edition_status"],
                "replacement_doc": replacement_doc,
                "current_version": current_version,
                "details": od["details"],
                "source_url": od["source_url"],
                "last_verified": od["last_verified"],
                "parse_confidence": None,
                "source": "override_only",
                "authoritative": True,
                "has_text": False,
            }
        )
        override_only_codes.append(original_code)

    # ---------- агрегаты ----------
    totals_doc: dict[str, int] = {}
    totals_edition: dict[str, int] = {}
    totals_eff: dict[str, int] = {}
    coverage_by_type: dict[str, dict[str, int]] = {}
    for e in entries:
        ds = e["doc_status"]
        totals_doc[ds] = totals_doc.get(ds, 0) + 1
        es_key = e["edition_status"] if e["edition_status"] is not None else "null"
        totals_edition[es_key] = totals_edition.get(es_key, 0) + 1
        eff = _effective_status(e["doc_status"], e["edition_status"])
        totals_eff[eff] = totals_eff.get(eff, 0) + 1
        tkey = e["type"] or "other"
        bucket = coverage_by_type.setdefault(
            tkey, {"total": 0, "has_text": 0, "override_only": 0}
        )
        bucket["total"] += 1
        if e["has_text"]:
            bucket["has_text"] += 1
        if e["source"] == "override_only":
            bucket["override_only"] += 1

    return {
        "meta": {
            "indexed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "vault_path": str(VAULT),
            "total": len(entries),
            "totals_by_doc_status": totals_doc,
            "totals_by_edition_status": totals_edition,
            "totals_by_effective_status": totals_eff,
            "coverage_by_type": coverage_by_type,
            "override_only": override_only_codes,
            "parse_failures": parse_failures,
        },
        "norms": entries,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--quiet", action="store_true", help="Не печатать сводку")
    args = ap.parse_args()

    if not VAULT.exists():
        print(f"ERROR: vault не найден: {VAULT}", file=sys.stderr)
        return 1

    index = build_index()
    OUTPUT_PATH.write_text(
        json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if not args.quiet:
        m = index["meta"]
        print(f"Сохранено {m['total']} записей → {OUTPUT_PATH.name}", file=sys.stderr)
        print("  totals_by_doc_status:", m["totals_by_doc_status"], file=sys.stderr)
        print("  totals_by_effective_status:", m["totals_by_effective_status"], file=sys.stderr)
        if m["override_only"]:
            print(
                f"  override_only: {len(m['override_only'])} (codes: {m['override_only'][:5]}{'…' if len(m['override_only']) > 5 else ''})",
                file=sys.stderr,
            )
        if m["parse_failures"]:
            print(
                f"  parse_failures: {len(m['parse_failures'])}",
                file=sys.stderr,
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
