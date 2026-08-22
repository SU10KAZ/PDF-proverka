#!/usr/bin/env python3
"""Run the research-only Vision versus VectorDescription+diff comparison."""
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


EXPERIMENT_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = EXPERIMENT_DIR / "artifacts"
AI_DIR = ARTIFACT_DIR / "ai_experiment"
SCHEMA = EXPERIMENT_DIR / "ai_output_schema.json"
MODEL = "gpt-5.6-sol"
PAIR_IDS = (
    "ss_scheme_text_changed",
    "ss_table_graphic",
    "ar_plan",
    "vk_nodes",
    "eom_singleline_changed",
)


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _base_prompt() -> str:
    return """Ты независимо сравниваешь пять пар инженерных графических блоков PDF.
Для каждой пары выбери один classification и перечисли только крупные фактические изменения.
Не придумывай назначение неразборчивых элементов. Если данных недостаточно, явно укажи это
в uncertainties. LEFT — предыдущая версия, RIGHT — следующая. Верни только JSON по схеме.
"""


def _vision_prompt() -> tuple[str, list[Path]]:
    images = []
    rows = [_base_prompt(), "Ниже изображения приложены строго в указанном порядке:"]
    ordinal = 1
    for pair_id in PAIR_IDS:
        left = ARTIFACT_DIR / "diagnostics" / pair_id / "left.png"
        right = ARTIFACT_DIR / "diagnostics" / pair_id / "right.png"
        if not left.is_file() or not right.is_file():
            raise FileNotFoundError(f"Missing diagnostics for {pair_id}")
        rows.append(f"{ordinal}. {pair_id} LEFT; {ordinal + 1}. {pair_id} RIGHT")
        images.extend((left, right))
        ordinal += 2
    rows.append("Используй только приложенные raster images; не открывай файлы и не вызывай инструменты.")
    return "\n".join(rows) + "\n", images


def _vector_prompt() -> str:
    rows = [
        _base_prompt(),
        "Используй только приведённые Level 3 vector descriptions и deterministic diff; картинок нет.",
        "Числа координат и подписи можно повторять только как evidence из входа.",
    ]
    for pair_id in PAIR_IDS:
        left = _load(ARTIFACT_DIR / "descriptions" / pair_id / "left" / "vector_block.json")
        right = _load(ARTIFACT_DIR / "descriptions" / pair_id / "right" / "vector_block.json")
        comparison = _load(ARTIFACT_DIR / "comparisons" / pair_id / "comparison.json")
        payload = {
            "pair_id": pair_id,
            "left_level_3": left["size_metrics"]["compact_payload"],
            "right_level_3": right["size_metrics"]["compact_payload"],
            "deterministic_diff": {
                "status": comparison["status"],
                "geometry": {
                    "similarity": comparison["geometry"]["similarity"],
                    "selected_tolerance": comparison["geometry"]["selected_tolerance"],
                    "left_coverage": comparison["geometry"]["left_coverage"],
                    "right_coverage": comparison["geometry"]["right_coverage"],
                    "encoding_rewrite_suspected": comparison["geometry"][
                        "encoding_rewrite_suspected"
                    ],
                    "tolerance_experiment": [
                        {
                            key: run[key]
                            for key in (
                                "tolerance",
                                "similarity",
                                "left_coverage",
                                "right_coverage",
                                "left_used",
                                "right_used",
                                "capped",
                            )
                        }
                        for run in comparison["geometry"]["tolerance_experiment"]
                    ],
                },
                "text": {
                    key: comparison["text"][key]
                    for key in (
                        "similarity",
                        "character_stream_similarity",
                        "effective_similarity",
                        "reliable",
                        "left_layer_quality",
                        "right_layer_quality",
                        "removed",
                        "added",
                        "value_changes",
                        "truncated",
                    )
                },
                "topology": comparison["topology"],
                "repeated_patterns": comparison["repeated_patterns"],
                "differences": comparison["differences"],
                "caveats": comparison["caveats"],
            },
        }
        rows.append(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return "\n\n".join(rows) + "\n"


def _run(prompt: str, images: list[Path]) -> tuple[dict[str, Any], dict[str, Any]]:
    command = [
        "codex",
        "exec",
        "--ephemeral",
        "--skip-git-repo-check",
        "--ignore-rules",
        "--sandbox",
        "read-only",
        "--model",
        MODEL,
        "--output-schema",
        str(SCHEMA),
        "-C",
        tempfile.gettempdir(),
    ]
    for path in images:
        command.extend(("--image", str(path.resolve())))
    command.append("-")
    completed = subprocess.run(
        command,
        input=prompt,
        text=True,
        capture_output=True,
        timeout=900,
        check=False,
    )
    metadata = {
        "model": MODEL,
        "returncode": completed.returncode,
        "stderr_tail": completed.stderr[-4000:],
        "prompt_characters": len(prompt),
        "image_count": len(images),
    }
    if completed.returncode:
        raise RuntimeError(f"codex exec failed: {metadata}")
    try:
        return json.loads(completed.stdout), metadata
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Model did not return JSON; stdout={completed.stdout[-4000:]}") from error


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=("vision", "vector", "all"), default="all")
    args = parser.parse_args()
    AI_DIR.mkdir(parents=True, exist_ok=True)
    arms = ("vision", "vector") if args.arm == "all" else (args.arm,)
    metadata = _load(AI_DIR / "invocation_metadata.json") if (AI_DIR / "invocation_metadata.json").is_file() else {}
    for arm in arms:
        if arm == "vision":
            prompt, images = _vision_prompt()
        else:
            prompt, images = _vector_prompt(), []
        (AI_DIR / f"{arm}_prompt.txt").write_text(prompt, encoding="utf-8")
        result, row = _run(prompt, images)
        (AI_DIR / f"{arm}_output.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        metadata[arm] = row
    (AI_DIR / "invocation_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
