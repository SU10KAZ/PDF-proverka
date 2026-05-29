"""Staging / internal smoke for evidence_first_s2_fallback.

Validates the controlled-enable MACHINERY through the REAL entry points,
deterministically and cheaply (mock provider — no Opus spend). The real Opus
output is already proven by the 831s shadow run; this smoke checks the wiring:

  1. flag OFF → batch preflight КР2 = skip_too_large; run_enriched_comparison =
     too_large (fallback NOT triggered)  → rollback semantics intact.
  2. flag ON  → batch preflight КР2 = run + analysis_strategy; summary
     will_run_fallback=1, skip_too_large=0.
  3. flag ON  → run_enriched_comparison(КР2, mock provider) dispatches into the
     fallback orchestrator → status=done, strategy set, changes>0, diagnostics
     present; global-singleton stamp collapse works (5 chunks → 1 stamp).
  4. NON-DESTRUCTIVE: production comparison_result.json is backed up and restored
     to its original too_large state at the end.

Run:
  STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED=true \
  python experiments/stage_comparison_evidence_first_s2/scripts/staging_smoke.py
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from backend.app.services.stage_comparison import enriched_comparison as ec
from backend.app.services.stage_comparison import unified_analysis_jobs as uaj
from backend.app.services.stage_comparison import paths as paths_mod
from backend.app.services.stage_comparison.text_llm_provider import ProviderResult

SID = "ba413a93c5754f6c"
PID = "p2ef68719"

# Реальные grounded-цитаты из КР2 enriched MD (подтверждены в acceptance harness).
_STAMP_L = "Шифр: АА/БЭ-03-ДС3-КР2"
_MAT_L = "Класс бетона фундаментной плиты по прочности В30, W8, F150"
_MAT_R = "Бетон класса В30, класс морозостойкости F200, класс водонепроницаемости W6"


class _MockProvider:
    """Возвращает per-chunk JSON: штамп (в каждом чанке → тест singleton collapse)
    + одно grounded материальное изменение. Без сети, без Opus."""
    name = "mock"

    def check_availability(self):
        return True, None

    def invoke(self, *, system_prompt, user_prompt, model, timeout_sec, work_dir=None):
        payload = {"status": "done", "summary": "mock chunk", "changes": [
            {"source": "stamp", "type": "stamp_changed", "severity": "high",
             "title": "Штамп изменён (формулировка варьируется по чанку)",
             "summary": "stamp", "confidence": 0.9,
             "evidence_left": {"quote": _STAMP_L}, "evidence_right": {"quote": ""}},
            {"source": "mixed", "type": "material_changed", "severity": "high",
             "title": "Класс бетона фундаментной плиты W8/F150 → W6/F200",
             "summary": "concrete", "confidence": 0.85,
             "evidence_left": {"quote": _MAT_L}, "evidence_right": {"quote": _MAT_R}},
        ], "warnings": []}
        return ProviderResult(status="done", raw_response=json.dumps(payload),
                              provider="mock", model=model)


def _fail(msg):
    print(f"  ✗ FAIL: {msg}")
    sys.exit(1)


def _ok(msg):
    print(f"  ✓ {msg}")


def main():
    res_path = paths_mod.enriched_comparison_result_path(SID, PID)
    backup = res_path.read_text(encoding="utf-8") if res_path.exists() else None
    print(f"backup existing result: {bool(backup)} ({res_path})")

    # Гарантируем, что enriched-compare включён (для preflight too_large расчёта).
    os.environ["STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED"] = "true"
    os.environ["STAGE_COMPARISON_ENRICHED_COMPARE_MODEL"] = "opus"

    try:
        # ── SMOKE 1: flag OFF → skip_too_large + too_large ──────────────────
        print("\n[SMOKE 1] fallback DISABLED → too_large blocks (rollback semantics)")
        os.environ["STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED"] = "false"
        info = uaj._classify_pair_for_batch(SID, PID, force_compare=True)
        if info.get("action") != "skip_too_large":
            _fail(f"expected skip_too_large, got {info.get('action')}")
        if info.get("too_large") is not True:
            _fail("expected too_large=True")
        if "analysis_strategy" in info:
            _fail("analysis_strategy must be absent when fallback disabled")
        _ok(f"preflight action={info['action']} too_large={info['too_large']} (no strategy)")

        r_off = ec.run_enriched_comparison(SID, PID, force=True, provider=_MockProvider())
        if r_off.get("status") != "too_large":
            _fail(f"expected status=too_large, got {r_off.get('status')}")
        if r_off.get("changes"):
            _fail("expected changes=[] under too_large")
        _ok(f"run_enriched_comparison status={r_off['status']} changes={len(r_off.get('changes') or [])}")

        # ── SMOKE 2: flag ON → preflight run + strategy ─────────────────────
        print("\n[SMOKE 2] fallback ENABLED → batch preflight routes to run")
        os.environ["STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED"] = "true"
        info = uaj._classify_pair_for_batch(SID, PID, force_compare=True)
        if info.get("action") != "run":
            _fail(f"expected action=run, got {info.get('action')}")
        if info.get("analysis_strategy") != ec.SYSTEM_PROMPT and \
                info.get("analysis_strategy") != "evidence_first_s2_fallback":
            _fail(f"expected analysis_strategy=evidence_first_s2_fallback, got {info.get('analysis_strategy')}")
        _ok(f"preflight action=run analysis_strategy={info['analysis_strategy']} "
            f"too_large={info['too_large']} fallback_enabled={info['fallback_enabled']}")

        # ── SMOKE 3: flag ON → run_enriched_comparison dispatches to fallback ─
        print("\n[SMOKE 3] fallback ENABLED → run_enriched_comparison → orchestrator (mock provider)")
        r_on = ec.run_enriched_comparison(SID, PID, force=True, provider=_MockProvider())
        if r_on.get("status") != "done":
            _fail(f"expected status=done, got {r_on.get('status')} ({r_on.get('error')})")
        if r_on.get("strategy") != "evidence_first_s2_fallback":
            _fail(f"expected strategy marker, got {r_on.get('strategy')}")
        if not r_on.get("fallback"):
            _fail("expected fallback=true")
        changes = r_on.get("changes") or []
        if not changes:
            _fail("expected changes>0")
        diag = r_on.get("diagnostics") or {}
        for k in ("deterministic_changes", "llm_changes_raw", "final_changes", "chunk_results"):
            if k not in diag:
                _fail(f"missing diagnostics.{k}")
        stamps = [c for c in changes if c.get("type") == "stamp_changed"]
        if len(stamps) != 1:
            _fail(f"global-singleton stamp collapse failed: {len(stamps)} stamp_changed (expected 1)")
        ungrounded = [c for c in changes if c.get("provenance") == "llm_chunk" and not c.get("evidence_verified")]
        if ungrounded:
            _fail(f"{len(ungrounded)} ungrounded llm changes leaked")
        _ok(f"status=done strategy={r_on['strategy']} changes={len(changes)} "
            f"det={diag['deterministic_changes']} llm_raw={diag['llm_changes_raw']} "
            f"final={diag['final_changes']} stamp_collapsed_to={len(stamps)}")
        _ok(f"chunk_results: {[(c.get('chunk_id'), c.get('status')) for c in diag['chunk_results']]}")

        # ── SMOKE 4: monitoring jq-equivalent works on the result ───────────
        print("\n[SMOKE 4] monitoring view (jq-equivalent)")
        mon = {
            "status": r_on["status"], "strategy": r_on["strategy"],
            "n": len(changes), "det": diag["deterministic_changes"],
            "llm_raw": diag["llm_changes_raw"], "dropped": diag.get("llm_changes_dropped_ungrounded"),
            "dups": diag.get("duplicates_removed"), "final": diag["final_changes"],
        }
        print("  " + json.dumps(mon, ensure_ascii=False))
        _ok("monitoring fields present")

        print("\n[SMOKE RESULT] ✅ ALL PASS — enable machinery validated end-to-end")
    finally:
        # ── NON-DESTRUCTIVE restore ─────────────────────────────────────────
        if backup is not None:
            res_path.write_text(backup, encoding="utf-8")
            restored = json.loads(backup).get("status")
            print(f"\n[restore] production result restored to status={restored} (non-destructive)")
        else:
            print("\n[restore] no prior result to restore")


if __name__ == "__main__":
    main()
