"""Object board state survives upload sessions; removal never deletes source or run files."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from . import paths

SIDES = {"left": "stage_1", "right": "stage_2"}


class BoardConflict(ValueError):
    pass


def path_key(value):
    return str(Path(value).expanduser().resolve()) if value else None


def roots(meta):
    return tuple(path_key(meta.get(key)) for key in ("stage_a_path", "stage_b_path"))


def state_path(meta):
    key = hashlib.sha256(json.dumps(roots(meta)).encode()).hexdigest()
    return paths.comparison_root_path() / "boards" / key / "state.json"


def load(meta):
    from . import store
    state = store._read_json(state_path(meta))
    if isinstance(state, dict):
        return state
    # A newer upload session can carry an older inherited layout. Pick the last
    # actual edit, not the first session in the creation-time index.
    candidates = []
    for entry in store._read_index()["sessions"]:
        if roots(entry) != roots(meta):
            continue
        previous = store._load_session_meta(entry["id"])
        pairing = (previous or {}).get("document_pairing")
        if isinstance(pairing, dict):
            candidates.append((str(pairing.get("updated_at") or ""),
                               not bool(pairing.get("inherited_from")), previous, pairing))
    if candidates:
        _, _, previous, pairing = max(candidates, key=lambda item: item[:2])
    else:
        previous, pairing = meta, {"left_order": [], "right_order": [], "confirmed_pairs": []}
        # Old clients did not always persist the board, even after completing an audit.
        used = {side: set() for side in SIDES}
        for run in store._comparison_runs(meta):
            if any(run[side]["pdf_path"] in used[side] for side in SIDES):
                continue
            for side in SIDES:
                path = run[side]["pdf_path"]
                pairing[side + "_order"].append(path)
                used[side].add(path)
            pairing["confirmed_pairs"].append({"left_pdf": run["left"]["pdf_path"], "right_pdf": run["right"]["pdf_path"]})
    return {"revision": 0, "source_session_id": previous["id"], "pairing": pairing,
            "documents": previous.get("documents") or {},
            "removed_documents": {side: [] for side in SIDES}, "removed_groups": []}


def available(meta, state):
    from . import document_groups
    result = {}
    for side, stage in SIDES.items():
        documents = (meta.get("documents") or {}).get(stage) or []
        hidden = document_groups.hidden_documents(meta.get("stage_a_path" if side == "left" else "stage_b_path"), side, documents)
        hidden = {path_key(p) for p in hidden} | set(state.get("removed_documents", {}).get(side, []))
        result[side] = [d for d in documents if path_key(d.get("pdf_path")) not in hidden]
    return result


def project(meta, state=None):
    state = load(meta) if state is None else state
    saved = state["pairing"]
    documents = available(meta, state)
    length = max(len(saved.get(side + "_order", [])) for side in SIDES)
    orders, used = {}, {side: set() for side in SIDES}
    for side, stage in SIDES.items():
        by_path = {path_key(d["pdf_path"]): d for d in documents[side]}
        by_code = {d["document_code"]: d for d in documents[side] if d.get("document_code") and not d.get("assembly_ref")}
        previous = {path_key(d["pdf_path"]): d for d in state.get("documents", {}).get(stage, [])}
        order = []
        for old in saved.get(side + "_order", []):
            document = by_path.get(path_key(old))
            if not document and old:
                source = previous.get(path_key(old)) or {}
                # Genuine new versions keep their row, but do not inherit approval or audits.
                if not source.get("assembly_ref"):
                    document = by_code.get(source.get("document_code"))
            path = document.get("pdf_path") if document else None
            if path in used[side]:
                path = None
            order.append(path)
            if path:
                used[side].add(path)
        orders[side] = order + [None] * (length - len(order))
    # New documents never fill reserved empty slots, nor silently form new pairs.
    for side in SIDES:
        other = "right" if side == "left" else "left"
        for document in documents[side]:
            path = document["pdf_path"]
            if path not in used[side]:
                orders[side].append(path)
                orders[other].append(None)
                used[side].add(path)
    rows = [(a, b) for a, b in zip(orders["left"], orders["right"]) if a or b]
    paired = {(path_key(a), path_key(b)): {"left_pdf": a, "right_pdf": b} for a, b in rows if a and b}
    confirmed = [paired[key] for p in saved.get("confirmed_pairs", [])
                 if (key := (path_key(p.get("left_pdf")), path_key(p.get("right_pdf")))) in paired]
    result = {**saved, "version": 1, "left_order": [a for a, _ in rows], "right_order": [b for _, b in rows],
              "confirmed_pairs": confirmed, "revision": state["revision"],
              "removed_documents": state.get("removed_documents", {side: [] for side in SIDES}),
              "hidden_documents": {side: [d["pdf_path"] for d in (meta.get("documents") or {}).get(stage, [])
                                           if path_key(d.get("pdf_path")) in state.get("removed_documents", {}).get(side, [])]
                                   for side, stage in SIDES.items()},
              "removed_groups": state.get("removed_groups", []),
              # Не часть раскладки: читаются заново, присланное клиентом не сохраняется.
              "worked_pairs": worked_pairs(meta)}
    result.pop("inherited_from", None)
    if state.get("source_session_id") and state["source_session_id"] != meta["id"]:
        result["inherited_from"] = state["source_session_id"]
    return result


def worked_path(meta):
    """Отметки «Отработано» лежат рядом с раскладкой: своя доска у каждой версии сравнения."""
    return state_path(meta).with_name("worked_pairs.json")


def worked_pairs(meta):
    """Пары, которые инженер отметил «Отработано»: [{left_pdf, right_pdf, by, by_id, at}]."""
    from . import store
    data = store._read_json(worked_path(meta))
    marks = data.get("pairs") if isinstance(data, dict) else None
    return [dict(mark) for mark in marks or [] if isinstance(mark, dict)
            and mark.get("left_pdf") and mark.get("right_pdf")]


def mark_worked(meta, left_pdf, right_pdf, *, by, by_id=None):
    """Отметить пару «Отработано». Отметка привязана к PDF обеих сторон: новая
    версия документа или пересборка группы — это уже другая пара без отметки.
    Первая отметка не перезаписывается."""
    from . import store
    documents = meta.get("documents") or {}
    for side, stage in SIDES.items():
        path = left_pdf if side == "left" else right_pdf
        if not path or path_key(path) not in {path_key(d.get("pdf_path")) for d in documents.get(stage) or []}:
            raise ValueError("Пара больше не найдена в списке. Обновите страницу.")
    marks = worked_pairs(meta)
    key = (path_key(left_pdf), path_key(right_pdf))
    if not any((path_key(m["left_pdf"]), path_key(m["right_pdf"])) == key for m in marks):
        marks.append({"left_pdf": str(left_pdf), "right_pdf": str(right_pdf),
                      "by": str(by or ""), "by_id": str(by_id or "") or None, "at": store._utc_now()})
        store._atomic_write_json(worked_path(meta), {"schema": "stage_comparison_worked_pairs/1", "pairs": marks})
    return marks


def check_revision(state, expected):
    if expected is not None and expected != state["revision"]:
        raise BoardConflict("Список уже изменился. Обновите страницу перед следующим изменением.")


def check_session(meta, state):
    """An old result tab may read the board, but cannot erase newer uploads."""
    from . import store
    source = store._load_session_meta(state.get("source_session_id", ""))
    if not source or source["id"] == meta["id"] or source.get("created_at", "") < meta.get("created_at", ""):
        return
    if source.get("created_at") == meta.get("created_at"):
        ids = [entry["id"] for entry in store._read_index()["sessions"]]
        if meta["id"] not in ids or source["id"] not in ids or ids.index(meta["id"]) < ids.index(source["id"]):
            return
    def inputs(documents):
        return {stage: {path_key(d["pdf_path"]) for d in documents.get(stage, [])} for stage in SIDES.values()}
    if inputs(source.get("documents") or {}) != inputs(meta.get("documents") or {}):
        raise BoardConflict("В список добавлены новые проекты. Обновите страницу перед изменением сопоставления.")


def save(meta, pairing, *, expected_revision=None, state=None):
    from . import store
    state = load(meta) if state is None else state
    check_revision(state, expected_revision)
    check_session(meta, state)
    state = {**state, "revision": state["revision"] + 1, "source_session_id": meta["id"],
             "documents": meta.get("documents") or {}, "pairing": pairing}
    store._atomic_write_json(state_path(meta), state)
    return project(meta, state)


def remove(meta, left_pdf=None, right_pdf=None, group_id=None, expected_revision=None):
    from . import store, document_groups
    state = load(meta)
    check_revision(state, expected_revision)
    check_session(meta, state)
    selected = {"left": [left_pdf] if left_pdf else [], "right": [right_pdf] if right_pdf else []}
    if group_id:
        if left_pdf or right_pdf:
            raise ValueError("Выберите группу или отдельные документы")
        groups = document_groups._groups_for_stage_path(meta["stage_a_path"])
        group = next((g for g in groups if g["group_id"] == group_id), None)
        if not group or group_id in state.get("removed_groups", []):
            raise ValueError("Группа больше не найдена в списке")
        # Область версии сравнения: сборки группы лежат в её рабочей папке.
        object_id = document_groups.assemblies.scope_for_stage_path(meta["stage_a_path"])
        if any(document_groups._assembly_state(object_id, group[side]).get("status")
               in document_groups.BUILDING_STATUSES for side in SIDES):
            raise BoardConflict("Дождитесь завершения сборки перед удалением из списка")
        for side, stage in SIDES.items():
            # An independent comparison shares the left source with its original row.
            if side == "left" and group.get("independent"):
                selected[side] = []
                continue
            shared = {code for other in groups if other["group_id"] != group_id
                      and other["group_id"] not in state.get("removed_groups", []) for code in other[side]["members"]}
            codes = set(group[side]["members"]) - shared
            selected[side] = [d["pdf_path"] for d in meta["documents"][stage]
                              if d.get("document_code") in codes or (d.get("assembly_ref") and
                                 d["assembly_ref"].get("assembly_id") == group[side].get("assembly_id"))]
    else:
        allowed = available(meta, state)
        for side in SIDES:
            if any(path not in {d["pdf_path"] for d in allowed[side]} for path in selected[side]):
                raise ValueError("Документ больше не найден в списке")
            grouped = document_groups.grouped_codes(meta.get("stage_a_path" if side == "left" else "stage_b_path"), side)
            if any(d.get("document_code") in grouped for d in allowed[side] if d["pdf_path"] in selected[side]):
                raise ValueError("Сначала уберите документ из группы кнопкой × или удалите всю пару")
    if not any(selected.values()):
        raise ValueError("Выберите пару или документ")
    if not group_id and left_pdf and right_pdf:
        current = project(meta, state)
        if (left_pdf, right_pdf) not in zip(current["left_order"], current["right_order"]):
            raise BoardConflict("Состав пары изменился. Обновите страницу перед удалением.")
    keys = {side: {path_key(p) for p in selected[side]} for side in SIDES}
    for run in store._comparison_runs(meta):
        if run["state"].get("status") == "RUNNING" and any(path_key(run[side].get("pdf_path")) in keys[side] for side in SIDES):
            raise BoardConflict("Дождитесь завершения анализа перед удалением из списка")
    pairing = project(meta, state)
    state["removed_documents"] = {side: sorted(set(state.get("removed_documents", {}).get(side, [])) | keys[side]) for side in SIDES}
    if group_id:
        state["removed_groups"] = [*state.get("removed_groups", []), group_id]
    pairing["updated_at"] = store._utc_now()
    return save(meta, pairing, state=state)
