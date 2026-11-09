"""Offline CLI feature probe only; never starts an inference session."""
import json
import shutil
import subprocess
from pathlib import Path


FEATURES = ('shell_tool', 'unified_exec', 'apps', 'plugins', 'memories',
            'multi_agent', 'multi_agent_v2', 'view_image', 'image_generation',
            'request_permissions_tool', 'shell_snapshot')


def probe():
    cli = shutil.which('codex')
    if not cli:
        raise RuntimeError('Codex CLI unavailable')
    args = [cli]
    for feature in FEATURES:
        args.extend(['-c', f'features.{feature}=false'])
    args.extend(['features', 'list'])
    result = subprocess.run(args, capture_output=True, text=True, timeout=30, check=True)
    observed = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if fields and fields[0] in FEATURES and fields[-1] in ('true', 'false'):
            observed[fields[0]] = fields[-1] == 'true'
    unmet = [name for name in FEATURES if observed.get(name) is not False]
    return {'cli': cli, 'version': subprocess.check_output([cli,'--version'], text=True).strip(),
            'requested_disabled': list(FEATURES), 'observed_features': observed,
            'unmet_disabled_features': unmet,
            'status': 'not_verified' if unmet else 'configuration_probe_passed_only',
            'model_calls': 0, 'source_data_sent': False,
            'boundary': 'Local feature listing is not an inference tool inventory. Do not override managed settings or weaken sandbox/approvals. Neither outcome certifies historical-answer isolation.',
            'official_reference': 'https://learn.chatgpt.com/docs/agent-configuration/subagents#custom-agent-file-schema'}


if __name__ == '__main__':
    result = probe()
    Path(__file__).with_name('codex_restricted_features_probe.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print(result['status'], result['unmet_disabled_features'])
