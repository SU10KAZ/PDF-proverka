"""Finite structural quota selection, separate from the frozen semantic producer.

SciPy/HiGHS selects up to the declared quotas, exactly six added documents, at
most three cases per document/stratum and one case per page pair. Maximizing
case count precedes document diversity. No predicted or human labels enter it.
"""
from collections import defaultdict
from pathlib import Path

from .dev_packet import ROOT, QUOTAS, load_pool, select_cases
from .run import read, write


def solve(pool, banned=(), require_full=False, added_documents=6, require_diversity=False):
    import numpy as np
    import scipy
    from scipy.optimize import milp, Bounds, LinearConstraint
    from scipy.sparse import coo_matrix

    pool = sorted([d for d in pool if d["document"]["document_version"] not in banned
                   and not d["document"]["document_code"].casefold().startswith("mockup")],
                  key=lambda d: d["document"]["document_version"])
    cases = [(di, c) for di, d in enumerate(pool) for c in d["cases"]]
    n_docs, n_cases = len(pool), len(cases)
    groups = sorted({(di, c["stratum"]) for di, c in cases})
    z = {g: n_docs + n_cases + i for i, g in enumerate(groups)}
    size = n_docs + n_cases + len(groups)
    cost, low, high = np.zeros(size), np.zeros(size), np.ones(size)
    base = sum(d["base"] for d in pool)
    for i, d in enumerate(pool):
        cost[i] = (i + 1) / (n_docs + 1)
        if d["base"]:
            low[i] = high[i] = 1
    for ci in range(n_cases):
        cost[n_docs + ci] = -10000 + (ci + 1) / ((n_cases + 1) * 100)
    for zi in z.values():
        cost[zi] = -1
    row_ids, col_ids, data, lower, upper = [], [], [], [], []
    def constraint(terms, minimum=-np.inf, maximum=np.inf):
        r = len(lower)
        for i, value in terms:
            row_ids.append(r); col_ids.append(i); data.append(value)
        lower.append(minimum); upper.append(maximum)
    constraint([(i, 1) for i in range(n_docs)], base + added_documents, base + added_documents)
    by_code, by_variant, by_page, by_group = defaultdict(list), defaultdict(list), defaultdict(list), defaultdict(list)
    for di, d in enumerate(pool):
        by_code[d["document"]["document_code"]].append(di)
    for ids in by_code.values():
        constraint([(i, 1) for i in ids], maximum=1)
    for ci, (di, c) in enumerate(cases):
        xi = n_docs + ci
        constraint([(xi, 1), (di, -1)], maximum=0)
        by_variant[c["variant"]].append(xi)
        by_page[tuple(c.get("page_content_hashes", [di, *c["page_pair"]]))].append(xi)
        by_group[(di, c["stratum"])].append(xi)
    for variant, quota in QUOTAS.items():
        constraint([(i, 1) for i in by_variant[variant]], minimum=quota if require_full else -np.inf, maximum=quota)
    for ids in by_page.values():
        constraint([(i, 1) for i in ids], maximum=1)
    for group, ids in by_group.items():
        constraint([(i, 1) for i in ids], maximum=3)
        constraint([(z[group], 1)] + [(i, -1) for i in ids], maximum=0)
    # Reward diversity up to the plan's minimum; no benefit from over-diversifying
    # one easy stratum while neglecting another. Small controls cannot span six docs.
    strata = sorted({g[1] for g in groups})
    for s in strata:
        target = min(6, sum(q for v, q in QUOTAS.items() if v.split('_')[0] == s))
        constraint([(zi, 1) for (di, st), zi in z.items() if st == s], minimum=target if require_diversity else -np.inf, maximum=target)
    matrix = coo_matrix((data, (row_ids, col_ids)), shape=(len(lower), size)).tocsc()
    # Explicit integer index width expected by HiGHS' wrapper.
    matrix.indices = matrix.indices.astype(np.int32)
    matrix.indptr = matrix.indptr.astype(np.int32)
    if require_full:
        cost[:] = 0  # Feasibility question, not a time-consuming optimum proof.
    result = milp(cost, integrality=np.ones(size), bounds=Bounds(low, high),
                  constraints=LinearConstraint(matrix, lower, upper), options={"time_limit": 50, "mip_rel_gap": 0})
    if result.x is None:
        raise RuntimeError(f"No feasible DEV selection: {result.message}")
    selected_docs = [d for i, d in enumerate(pool) if result.x[i] > 0.5]
    selected_cases = [c for ci, (di, c) in enumerate(cases) if result.x[n_docs + ci] > 0.5]
    return {"documents": [d["document"]["document_version"] for d in selected_docs],
            "cases": [{"case_id": c["case_id"], "variant": c["variant"]} for c in selected_cases],
            "solver": {"name": "scipy.optimize.milp / HiGHS", "scipy_version": scipy.__version__,
                       "message": result.message, "status": int(result.status), "mip_gap": float(result.mip_gap),
                       "case_count": len(selected_cases), "base_documents": base, "added_documents": added_documents,
                       "require_diversity": require_diversity},
            "selection_rule": "Structural quotas only; max cases, then capped stratum document diversity, deterministic index tie-breaks"}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-selection", type=Path)
    args = parser.parse_args()
    pool = load_pool()
    if args.source_selection:
        selected = set(read(args.source_selection)["documents"])
        pool = [d for d in pool if d["document"]["document_version"] in selected]
        manifest = solve(pool, require_full=True, added_documents=sum(not d["base"] for d in pool), require_diversity=True)
        manifest["source_selection_sha256"] = __import__("hashlib").sha256(args.source_selection.read_bytes()).hexdigest()
    else:
        manifest = solve(pool, require_full=True)
    write(ROOT / "dev_selection.json", manifest)
    print(manifest["solver"])


if __name__ == "__main__":
    main()
