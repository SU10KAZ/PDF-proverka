#!/usr/bin/env python3
"""Remove Critic v2 from portal (backend routers + frontend + EV routing)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if len(sys.argv) > 1:
    ROOT = Path(sys.argv[1])


def _delete_line_range(lines: list[str], start: int, end: int) -> list[str]:
    return lines[:start] + lines[end:]


def _find_line(lines: list[str], pattern: str, start: int = 0) -> int:
    for i in range(start, len(lines)):
        if pattern in lines[i]:
            return i
    return -1


def patch_main_py(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"\s*critic_v2_ui,\n", "\n", text)
    text = re.sub(r"\s*critic_v2_assisted_round1,\n", "\n", text)
    text = re.sub(r"\napp\.include_router\(critic_v2_ui\.router\)\n", "\n", text)
    text = re.sub(r"\napp\.include_router\(critic_v2_assisted_round1\.router\)\n", "\n", text)
    path.write_text(text, encoding="utf-8")
    print("patched", path)


def patch_kb_routing(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "    critic_v2_score: Optional[int] = None,\n    critic_skip_threshold: int = 85,\n",
        "",
    )
    text = text.replace(
        "    if kb == \"accept\" and critic_v2_score is not None and critic_v2_score >= critic_skip_threshold:\n"
        "        return False, \"kb_accept_high_critic\"\n",
        "",
    )
    path.write_text(text, encoding="utf-8")
    print("patched", path)


def patch_engine(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    text = text.replace("        critic_v2_score: Optional[int] = None,\n", "")
    text = text.replace(
        "                finding, kb_decision=kb_decision, critic_v2_score=critic_v2_score,\n",
        "                finding, kb_decision=kb_decision,\n",
    )
    text = text.replace("        critic_map: Optional[dict] = None,\n", "")
    text = text.replace("        critic_map = critic_map or {}\n", "")
    text = text.replace("                    critic_v2_score=critic_map.get(fid),\n", "")
    path.write_text(text, encoding="utf-8")
    print("patched", path)


def patch_evidence_service(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    text = re.sub(
        r"\n\ndef _load_critic_score_map\(output_dir: Path\).*?\n    return \{\}\n\n\n",
        "\n\n",
        text,
        flags=re.DOTALL,
    )
    text = text.replace(
        "    critic_map = _load_critic_score_map(output_dir) if respect_kb_routing else {}\n",
        "",
    )
    text = text.replace("        critic_map=critic_map,\n", "")
    path.write_text(text, encoding="utf-8")
    print("patched", path)


def patch_app_js(path: Path) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)

    i0 = _find_line(lines, "// ─── Critic v2 UI Triage View")
    i1 = _find_line(lines, "// Prompts")
    if i0 >= 0 and i1 > i0:
        lines = _delete_line_range(lines, i0, i1)
        print(f"app.js: removed main cv2 block ({i1 - i0} lines)")

    i0 = _find_line(lines, "// ─── Inline Critic v2")
    i1 = _find_line(lines, "// ─── Blocks (OCR)")
    if i0 >= 0 and i1 > i0:
        lines = _delete_line_range(lines, i0, i1)
        print(f"app.js: removed inline cv2 block ({i1 - i0} lines)")

    text = "".join(lines)

    # loadFindings cv2 cleanup
    text = re.sub(
        r"            // Сбрасываем inline-критика при смене проекта\n"
        r"            findingsCv2Map\.value = \{\};\n"
        r"            findingsCv2Available\.value = false;\n"
        r"            findingsCv2Warning\.value = '';\n"
        r"            findingsCv2Loading\.value = false;\n"
        r"            // Manual reload инвалидирует session-cache Critic v2 для этого проекта\n"
        r"            if \(forceRefresh\) \{\n"
        r"                delete _findingsCv2SessionCache\[id\];\n"
        r"            \}\n",
        "",
        text,
    )
    text = re.sub(
        r"                    // Critic v2 — deferred \(idle\), session-cached\n"
        r"                    _scheduleCriticV2Load\(id, \{ forceRefresh: false \}\);\n",
        "",
        text,
    )
    text = re.sub(
        r"                // Critic v2 — deferred \(idle\), session-cached, не блокирует таблицу\n"
        r"                _scheduleCriticV2Load\(id, \{ forceRefresh: forceRefresh \}\);\n",
        "",
        text,
    )

    # _applyFindingsFilter cv2
    text = re.sub(
        r"            const cv2Map = findingsCv2Map\.value \|\| \{\};\n"
        r"            const cv2Has = findingsCv2Available\.value;\n"
        r"            const showHidden = cv2ShowHidden\.value;\n"
        r"            const displayFilter = cv2DisplayFilter\.value;\n",
        "",
        text,
    )
    text = re.sub(
        r"            // Скрытие по Critic v2 — только если данные есть и юзер не открыл их явно\n"
        r"            if \(cv2Has && !showHidden\) \{\n"
        r"                items = items\.filter\(f => \{\n"
        r"                    const cv2 = cv2Map\[f\.id\];\n"
        r"                    return !cv2 \|\| !cv2IsHiddenByDefault\(cv2\);\n"
        r"                \}\);\n"
        r"            \}\n"
        r"            // Фильтр по bucket'у\n"
        r"            if \(cv2Has && displayFilter\) \{\n"
        r"                items = items\.filter\(f => \{\n"
        r"                    const cv2 = cv2Map\[f\.id\];\n"
        r"                    if \(!cv2\) return false;\n"
        r"                    const score = cv2DisplayScore\(cv2\);\n"
        r"                    const b = cv2DisplayBucket\(score\);\n"
        r"                    return b && b\.key === displayFilter;\n"
        r"                \}\);\n"
        r"            \}\n",
        "",
        text,
    )

    # sortedFindings — remove cv2 sort branch
    text = re.sub(
        r"        // Сортировка по столбцу Critic v2: null → 'desc' \(100→0\) → 'asc' \(0→100\) → null\n"
        r"        const cv2SortDir = ref\(null\);\n"
        r"        function toggleCv2Sort\(\) \{\n"
        r"            if \(cv2SortDir\.value === null\) cv2SortDir\.value = 'desc';\n"
        r"            else if \(cv2SortDir\.value === 'desc'\) cv2SortDir\.value = 'asc';\n"
        r"            else cv2SortDir\.value = null;\n"
        r"            findingsPage\.value = 1;\n"
        r"        \}\n\n",
        "",
        text,
    )
    text = re.sub(
        r"        // Сортировка: отклонённые всегда внизу \(если есть решения\)\.\n"
        r"        // Если активна сортировка по Critic v2 — она имеет приоритет, nulls в конец\.\n"
        r"        const sortedFindings = computed\(\(\) => \{\n"
        r"            const items = filteredFindings\.value;\n"
        r"            if \(cv2SortDir\.value\) \{\n"
        r"                const dir = cv2SortDir\.value === 'asc' \? 1 : -1;\n"
        r"                return \[\.\.\.items\]\.sort\(\(a, b\) => \{\n"
        r"                    const sa = findingCv2Score\(a\.id\);\n"
        r"                    const sb = findingCv2Score\(b\.id\);\n"
        r"                    const aNull = sa == null, bNull = sb == null;\n"
        r"                    if \(aNull && bNull\) return 0;\n"
        r"                    if \(aNull\) return 1;\n"
        r"                    if \(bNull\) return -1;\n"
        r"                    return \(sa - sb\) \* dir;\n"
        r"                \}\);\n"
        r"            \}\n"
        r"            if \(!Object\.keys\(expertDecisions\.value\)\.length\) return items;\n",
        "        const sortedFindings = computed(() => {\n"
        "            const items = filteredFindings.value;\n"
        "            if (!Object.keys(expertDecisions.value).length) return items;\n",
        text,
    )

    # hash routes
    text = re.sub(
        r"            \} else if \(hash === '/critic-v2-ui'\) \{\n"
        r"                // Experimental offline view\. Does NOT touch production pipeline\.\n"
        r"                currentView\.value = 'critic-v2-ui';\n"
        r"                connectGlobalWS\(\);\n",
        "            ",
        text,
    )
    text = re.sub(
        r"            \} else if \(hash\.match\(\^\\/project\\/\(\.\+\)\\/critic-v2-disagreements\$\/\)\) \{\n"
        r"[\s\S]*?cv2LoadProject\(id, \{ disagreementsMode: true \}\);\n"
        r"            \} else if \(hash\.match\(\^\\/project\\/\(\.\+\)\\/critic-v2\$\/\)\) \{\n"
        r"[\s\S]*?cv2LoadProject\(id\);\n",
        "            ",
        text,
    )

    text = text.replace("            'document', 'critic-v2-project',\n", "            'document',\n")

    text = re.sub(
        r"        // Inline Critic v2 toggles\n"
        r"        watch\(cv2ShowHidden, \(\) => \{ findingsPage\.value = 1; _applyFindingsFilter\(\); \}\);\n"
        r"        watch\(cv2DisplayFilter, \(\) => \{ findingsPage\.value = 1; _applyFindingsFilter\(\); \}\);\n\n",
        "",
        text,
    )

    # return exports — drop lines with cv2
    out_lines = []
    skip_cv2_block = False
    for line in text.splitlines(keepends=True):
        if "// Inline Critic v2 (experimental" in line:
            skip_cv2_block = True
            continue
        if skip_cv2_block:
            if "kbValidationAvailable" in line or "KB-Validation" in line:
                skip_cv2_block = False
                out_lines.append(line)
            continue
        if re.search(r"\bcv2[A-Z]|\bfindingCv2|CV2_DISPLAY|cv2DebugVisible|cv2ShowHidden|cv2HiddenCount|cv2SortDir|toggleCv2Sort", line):
            if "// Critic v2" in line:
                continue
            continue
        out_lines.append(line)
    text = "".join(out_lines)

    path.write_text(text, encoding="utf-8")
    print("patched", path)


def patch_index_html(path: Path) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)

    # nav global
    i0 = _find_line(lines, "<!-- Critic v2 debug nav:")
    if i0 >= 0:
        i1 = i0
        while i1 < len(lines) and "</div>" not in lines[i1]:
            i1 += 1
        lines = _delete_line_range(lines, i0, i1 + 1)

    # project tab
    i0 = _find_line(lines, "<!-- Единая вкладка Critic v2")
    if i0 >= 0:
        i1 = i0
        while i1 < len(lines) and "</button>" not in lines[i1]:
            i1 += 1
        lines = _delete_line_range(lines, i0, i1 + 1)

    # findings filters
    i0 = _find_line(lines, "<!-- Critic v2 (experimental) — фильтр")
    if i0 >= 0:
        i1 = _find_line(lines, "Critic v2: нет данных", i0)
        if i1 >= 0:
            i1 += 1
            while i1 < len(lines) and lines[i1].strip() and not lines[i1].strip().startswith("<"):
                i1 += 1
            lines = _delete_line_range(lines, i0, i1)

    # table header column
    lines = [ln for ln in lines if "findingsCv2Available" not in ln or "colspan" in ln]
    # fix colspan
    for i, ln in enumerate(lines):
        if "colspan" in ln and "findingsCv2Available" in ln:
            lines[i] = re.sub(
                r"\(expertReviewMode \? 10 : 8\) \+ \(findingsCv2Available \? 1 : 0\)",
                "(expertReviewMode ? 10 : 8)",
                ln,
            )

    # remove th/td with cv2 - already removed header via findingsCv2Available filter - need targeted removal
    new_lines = []
    skip = False
    for ln in lines:
        if 'v-if="findingsCv2Available"' in ln and "<th" in ln:
            skip = True
            continue
        if skip and "</th>" in ln:
            skip = False
            continue
        if 'v-if="findingsCv2Available"' in ln and "<td" in ln:
            skip = True
            continue
        if skip and "</td>" in ln:
            skip = False
            continue
        if "findingCv2" in ln or "cv2-disp" in ln:
            continue
        new_lines.append(ln)
    lines = new_lines

    # large views
    i0 = _find_line(lines, "<!-- ═══ Critic v2 UI Triage View")
    i1 = _find_line(lines, "<!-- ═══ Critic v2 Project-Scoped View")
    if i0 >= 0 and i1 > i0:
        lines = _delete_line_range(lines, i0, i1)

    i0 = _find_line(lines, "<!-- ═══ Critic v2 Project-Scoped View")
    if i0 >= 0:
        i1 = _find_line(lines, "<!-- Batch LLM модалки удалены")
        if i1 < 0:
            i1 = _find_line(lines, "</div><!-- /main-area -->")
        if i1 > i0:
            lines = _delete_line_range(lines, i0, i1)

    path.write_text("".join(lines), encoding="utf-8")
    print("patched", path)


def patch_styles_css(path: Path) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    kept = [ln for ln in lines if ".cv2-" not in ln and "th.cv2-" not in ln]
    path.write_text("".join(kept), encoding="utf-8")
    print("patched", path, f"removed {len(lines) - len(kept)} lines")


def main() -> None:
    base = ROOT
    patch_main_py(base / "backend/app/main.py")
    patch_kb_routing(
        base / "backend/app/pipeline/stages/findings_review/evidence_verifier/kb_routing.py"
    )
    patch_engine(
        base / "backend/app/pipeline/stages/findings_review/evidence_verifier/engine.py"
    )
    patch_evidence_service(base / "backend/app/services/findings/evidence_validation_service.py")
    patch_app_js(base / "frontend/static/js/app.js")
    patch_index_html(base / "frontend/index.html")
    patch_styles_css(base / "frontend/static/css/styles.css")
    print("done")


if __name__ == "__main__":
    main()
