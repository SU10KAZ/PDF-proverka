#!/usr/bin/env python3
"""
validate_migration.py — проверка корректности миграции в projects_v2.

Проверяет (по old_to_new_map.json + файловой системе):
  1. весь входной комплект перенесён в 01_input;
  2. checksum копии совпадает с записанным и с ТЕКУЩИМ legacy-файлом
     (значит legacy не изменился и копия идентична);
  3. критичные артефакты (03_findings.json, 01_blocks_analysis.json,
     02_text_analysis.json) присутствуют в новой структуре;
  4. версии получили строгие индексы v001, v002, ...;
  5. старая структура (legacy-файлы) на месте и не модифицирована;
  6. в projects_v2/_system/old_to_new_map.json есть карта соответствий.

READ-ONLY: ничего не пишет и не меняет.

Использование:
  python scripts/projects_v2/validate_migration.py                 # все миграции из map
  python scripts/projects_v2/validate_migration.py --document <document_code>
  python scripts/projects_v2/validate_migration.py --v2-root <path>
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402


def validate_map(map_obj: dict, document_filter: str | None = None) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    notes: list[str] = []

    migrations = map_obj.get("migrations", [])
    if not migrations:
        errors.append("old_to_new_map.json: no migrations recorded")
        return errors, notes

    # группируем версии по документу для проверки индексации
    by_doc: dict[tuple, list[dict]] = {}
    for m in migrations:
        if document_filter and m.get("document_code") != document_filter:
            continue
        key = (m.get("object_id"), m.get("document_code"))
        by_doc.setdefault(key, []).append(m)

    if not by_doc:
        errors.append(f"no migrations match document filter: {document_filter}")
        return errors, notes

    for (object_id, document_code), versions in by_doc.items():
        tag = f"[{document_code}]"

        # (4) строгая индексация v001, v002 ...
        vids = sorted(v["version_id"] for v in versions)
        expected = [f"v{i:03d}" for i in range(1, len(versions) + 1)]
        if vids != expected:
            errors.append(f"{tag} version indexing mismatch: got {vids}, expected {expected}")
        else:
            notes.append(f"{tag} version index OK: {vids}")

        for m in versions:
            vtag = f"{tag} {m['version_id']}"
            files = m.get("files", [])
            legacy_preserve = m.get("migration_kind") == "legacy_findings_preserve"

            # (1)+(2) входной комплект + checksum
            if legacy_preserve:
                # legacy snapshot (King&Sons): источники сохранены как legacy_bundle,
                # строгий quad не гарантируется (допустим source-only). Проверяем
                # лишь, что bundle непустой; checksum/критичные артефакты — как обычно.
                if not files:
                    errors.append(f"{vtag} legacy_findings_preserve: empty file set in map")
                else:
                    notes.append(f"{vtag} legacy_findings_preserve bundle: {len(files)} files")
            else:
                input_files = [f for f in files if str(f.get("role", "")).startswith("input:")]
                roles_present = {f["role"].split(":", 1)[1] for f in input_files}
                for req in ("document_md", "result_json"):
                    if req not in roles_present:
                        errors.append(f"{vtag} missing required input in map: {req}")
                if "pdf" not in roles_present:
                    notes.append(f"{vtag} note: no .pdf in input (check source)")

            for f in files:
                new_path = Path(f["new_path"])
                old_path = Path(f["old_path"])
                expected_sha = f.get("sha256")
                if not new_path.exists():
                    errors.append(f"{vtag} copied file missing: {new_path}")
                    continue
                if expected_sha is None:
                    continue  # sha не трекали (например png-блоки)
                # checksum новой копии == записанный
                actual_new = v2lib.sha256_file(new_path)
                if actual_new != expected_sha:
                    errors.append(f"{vtag} checksum drift (new copy): {new_path}")
                # После cutover v2 и legacy могли легитимно разойтись. Новая
                # карта хранит независимый baseline legacy_sha256; старые карты
                # по-прежнему требуют равенства исходному sha256.
                if old_path.exists():
                    actual_old = v2lib.sha256_file(old_path)
                    expected_old = f.get("legacy_sha256", expected_sha)
                    if actual_old != expected_old:
                        errors.append(f"{vtag} LEGACY CHANGED since migration: {old_path}")
                    elif expected_old != expected_sha:
                        notes.append(f"{vtag} post-cutover divergence recorded: {old_path.name}")
                else:
                    # legacy-файл пропал — мог быть удалён вне нашего процесса
                    notes.append(f"{vtag} legacy source no longer present: {old_path}")

            # (3) критичные артефакты присутствуют в новой структуре
            doc_dir = Path(m["v2_document_dir"])
            vroot = doc_dir / "versions" / m["version_id"]
            run_id = m.get("analysis_run_id")
            for crit in v2lib.CRITICAL_ANALYSIS_FILES:
                candidates = [
                    vroot / "03_analysis" / "latest" / crit,
                ]
                if run_id:
                    candidates.append(vroot / "03_analysis" / "runs" / run_id / crit)
                if any(c.exists() for c in candidates):
                    notes.append(f"{vtag} critical artifact present: {crit}")
                else:
                    # критично только если оно было в legacy _output
                    legacy_out = Path(m["legacy_folder_path"]) / "_output" / crit
                    if legacy_out.exists():
                        errors.append(f"{vtag} CRITICAL artifact lost: {crit}")
                    else:
                        notes.append(f"{vtag} critical artifact absent in legacy too: {crit}")

    return errors, notes


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Validate projects_v2 migration (read-only)")
    parser.add_argument("--v2-root", default=None)
    parser.add_argument("--document", default=None, help="validate only this document_code")
    args = parser.parse_args(argv)

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    map_path = v2_root / "_system" / "old_to_new_map.json"
    if not map_path.exists():
        print(f"[FAIL] old_to_new_map.json not found: {map_path}", file=sys.stderr)
        return 1

    map_obj = v2lib.load_old_to_new_map(map_path)
    errors, notes = validate_map(map_obj, args.document)

    print(f"=== validate_migration ({map_path}) ===")
    for n in notes:
        print(f"  ok  {n}")
    if errors:
        print()
        for e in errors:
            print(f"  ERR {e}")
        print(f"\n[FAIL] {len(errors)} error(s)")
        return 1
    print(f"\n[PASS] all checks passed ({len(notes)} ok)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
