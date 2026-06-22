from __future__ import annotations

_NAMES = {
    "general_engineering_facts": "Extract general engineering facts from the block.",
    "ocr_strict": "Read only visible OCR text and do not infer hidden content.",
    "stamp_mode": "Extract title block and stamp attributes from drawings.",
    "table_mode": "Extract rows, columns, units, and values from tables.",
    "scheme_mode": "Extract labels, ratings, and connections from engineering schemes.",
    "material_numeric_mode": "Extract materials, dimensions, elevations, and numeric parameters.",
}


def get_single_pass(name: str) -> str:
    if name not in _NAMES:
        raise KeyError(name)
    return (
        f"{_NAMES[name]} Return JSON only. The JSON must include labels, "
        "materials, numeric_parameters, visible_text, confidence, warnings, "
        "and usable_for_diff fields. Do not include markdown fences."
    )


def two_pass_facts(ocr_text: str) -> str:
    return (
        "Use the OCR text below as evidence and return JSON only with extracted "
        "engineering facts, confidence, warnings, and usable_for_diff.\n\n"
        f"OCR_TEXT:\n{ocr_text}"
    )
