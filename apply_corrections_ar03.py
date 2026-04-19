import json, shutil
from pathlib import Path

base = Path("projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР0.3-ПА (Изм.1)/_output")

with open(base / "03_findings.json", "r", encoding="utf-8") as f:
    findings_data = json.load(f)

with open(base / "03_findings_review.json", "r", encoding="utf-8") as f:
    review_data = json.load(f)

verdicts = {r["finding_id"]: r for r in review_data["reviews"]}
to_remove = {fid for fid, r in verdicts.items() if r["verdict"] == "not_practical"}
print("To remove:", to_remove)

removed_findings = []
corrected_findings = []

for f in findings_data["findings"]:
    fid = f["id"]
    if fid in to_remove:
        removed_findings.append({
            "id": fid,
            "reason": "формальное замечание, не влияет на строительство",
            "critic_details": verdicts[fid]["details"]
        })
    else:
        corrected_findings.append(f)

print(f"Original: {len(findings_data['findings'])} findings")
print(f"Removed: {len(removed_findings)}")
print(f"Remaining: {len(corrected_findings)}")

by_severity = {}
for f in corrected_findings:
    sev = f.get("severity", "")
    by_severity[sev] = by_severity.get(sev, 0) + 1

print("By severity:", by_severity)

passed = sum(1 for r in review_data["reviews"] if r["verdict"] == "pass")

result = {
    "meta": {
        **findings_data["meta"],
        "total_findings": len(corrected_findings),
        "by_severity": by_severity,
        "review_applied": True,
        "review_stats": {
            "total_reviewed": len(review_data["reviews"]),
            "passed": passed,
            "fixed": 0,
            "removed": len(removed_findings),
            "downgraded": 0
        }
    },
    "findings": corrected_findings,
    "removed_findings": removed_findings
}

shutil.copy(base / "03_findings.json", base / "03_findings_pre_review.json")
print("Backup created.")

with open(base / "03_findings.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print("Written: 03_findings.json")
