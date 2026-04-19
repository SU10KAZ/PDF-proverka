import json
import shutil
from pathlib import Path

SOURCE_ROOT = Path("/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/AR")
DEST_ROOT = Path("/home/coder/projects/PDF-proverka/Батчи")

PROJECTS = [
    "13АВ-РД-АР0.1-ПА",
    "13АВ-РД-АР0.3-ПА (Изм.1)",
    "13АВ-РД-АР0.4-ПА",
    "13АВ-РД-АР1.1-К2",
    "13АВ-РД-АР1.1-К3-К4",
    "13АВ-РД-АР1.1-К4 (Изм.2)",
    "13АВ-РД-АР1.1-К4-К5",
    "13АВ-РД-АР1.1-К5-К6",
    "13АВ-РД-АР1.2-К3 (2)",
    "13АВ-РД-АР1.2-К3К4",
    "13АВ-РД-АР1.2-К3К4 (1)",
]


def main() -> None:
    total_copied = 0
    total_missing = 0

    for name in PROJECTS:
        src_output = SOURCE_ROOT / name / "_output"
        blocks_dir = src_output / "blocks"
        batches_json = src_output / "block_batches.json"

        with batches_json.open(encoding="utf-8") as f:
            data = json.load(f)

        dest_project = DEST_ROOT / name
        dest_project.mkdir(parents=True, exist_ok=True)
        shutil.copy2(batches_json, dest_project / "_batch_info.json")

        copied = 0
        missing: list[str] = []
        for batch in data["batches"]:
            bid = batch["batch_id"]
            dest_batch = dest_project / f"batch_{bid}"
            dest_batch.mkdir(exist_ok=True)
            for block in batch["blocks"]:
                src_png = blocks_dir / block["file"]
                if src_png.exists():
                    shutil.copy2(src_png, dest_batch / block["file"])
                    copied += 1
                else:
                    missing.append(block["file"])

        total_copied += copied
        total_missing += len(missing)
        print(f"[{name}] batches={data['total_batches']} copied={copied} missing={len(missing)}")
        if missing:
            preview = ", ".join(missing[:3])
            tail = "..." if len(missing) > 3 else ""
            print(f"  MISSING: {preview}{tail}")

    print(f"\nTOTAL: copied={total_copied} missing={total_missing}")


if __name__ == "__main__":
    main()
