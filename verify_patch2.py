import json
path = "/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЅ6.17-23.2-К2 (Изм.1).pdf/_output/03_findings.json"
with open(path) as f:
    d = json.load(f)

check_ids = ['F-079', 'F-044', 'F-061', 'F-020', 'F-037', 'F-035']
for fid in check_ids:
    for finding in d['findings']:
        if finding['id'] == fid:
            print(f"=== {fid} ===")
            print(f"  norm: {finding.get('norm')}")
            print(f"  norm_status: {finding.get('norm_status')}")
            rev = finding.get('norm_revision')
            if rev:
                print(f"  revised_norm: {rev.get('revised_norm')}")
                reason = rev.get('revision_reason', '')
                print(f"  reason: {reason[:100]}...")
            print()
            break

print("File is valid JSON with", len(d['findings']), "findings")
print("Removed findings:", len(d.get('removed_findings', [])))
