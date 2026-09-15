"""Fresh Codex CLI processes with a filesystem allowlist and receipted resume.

No OpenRouter imports or alternative credentials. The existing ChatGPT login is
mounted solely for Codex authentication; user config, sessions and corpus are not.
"""
import asyncio
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

from experiments.project_change_272.inventory import immutable, now, read, sha
from experiments.project_change_semantic_272.packets import digest
from .schema import validate
from .serialization import prepare_request_bytes, request_sha256

CONFIG = dict(provider='codex_chatgpt', model='gpt-6-astra', reasoning='xhigh',
              service_tier='priority', mechanism='codex exec ephemeral + bubblewrap',
              max_parallelism=2, timeout_seconds=900, retries=0)
DISABLED = ['shell_tool', 'unified_exec', 'apps', 'plugins', 'remote_plugin',
            'memories', 'multi_agent', 'browser_use', 'browser_use_external',
            'computer_use', 'image_generation', 'hooks', 'shell_snapshot',
            'skill_search', 'code_mode_host', 'unbounded_connection_retries']


class AuthorizationBlocked(RuntimeError):
    pass


class InferenceFailed(RuntimeError):
    pass


def classify_error(text):
    text = text.casefold()
    if any(s in text for s in ['usage limit', 'quota', 'insufficient_quota', 'rate_limit_exceeded',
            'credit balance', 'limit exceeded', 'not authenticated', 'unauthorized',
            '401 unauthorized', '403 forbidden', 'not supported when using codex with a chatgpt']):
        return 'AUTHORIZATION_BLOCKED'
    return 'INFERENCE_FAILED'


def safe_env():
    # Do not pass provider keys, endpoint overrides, parent thread IDs or shell hooks.
    names = ['PATH', 'LANG', 'LC_ALL', 'TZ']
    return {k: os.environ[k] for k in names if k in os.environ}


def sandbox_command(work, command):
    cli = Path(shutil.which('codex') or '').resolve()
    auth = Path('/home/coder/.codex/auth.json')
    if not cli.is_file() or not auth.is_file() or not shutil.which('bwrap'):
        raise AuthorizationBlocked('Existing Codex CLI/ChatGPT authentication or required isolation unavailable')
    args = ['bwrap', '--unshare-all', '--share-net', '--die-with-parent', '--new-session',
            '--ro-bind', '/usr', '/usr', '--symlink', 'usr/bin', '/bin',
            '--symlink', 'usr/lib', '/lib', '--symlink', 'usr/lib64', '/lib64',
            '--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp',
            '--dir', '/home/coder/.codex', '--ro-bind', str(auth), '/home/coder/.codex/auth.json',
            '--ro-bind', str(cli), '/opt/codex', '--bind', str(Path(work).resolve()), '/work',
            '--chdir', '/work', '--setenv', 'HOME', '/home/coder',
            '--setenv', 'CODEX_HOME', '/home/coder/.codex']
    for path in ['/etc/ssl', '/etc/resolv.conf', '/etc/hosts', '/etc/nsswitch.conf', '/etc/passwd']:
        if Path(path).exists():
            args += ['--ro-bind', path, path]
    return args + command


def cli_command(images):
    cmd = ['/opt/codex', 'exec', '--ephemeral', '--ignore-user-config', '--ignore-rules',
           '--skip-git-repo-check', '--sandbox', 'read-only', '--json', '--color', 'never',
           '--model', CONFIG['model'], '-c', 'model_provider="openai"',
           '-c', 'model_reasoning_effort="' + CONFIG['reasoning'] + '"',
           '-c', 'service_tier="' + CONFIG['service_tier'] + '"',
           '-c', 'web_search="disabled"', '-c', 'mcp_servers={}',
           '-c', 'project_doc_max_bytes=0', '-c', 'approval_policy="never"',
           '--output-schema', '/work/schema.json', '--output-last-message', '/work/final.txt']
    for feature in DISABLED:
        cmd += ['--disable', feature]
    for name in images:
        cmd += ['--image', '/work/' + name]
    return cmd + ['-']


def runtime_identity():
    import importlib.metadata
    cli = Path(shutil.which('codex')).resolve()
    return dict(cli_version=subprocess.check_output([str(cli), '--version'], text=True).strip(),
                cli_sha256=sha(cli), wrapper_sha256=sha(__file__), disabled_features=DISABLED,
                jsonschema_version=importlib.metadata.version('jsonschema'),
                isolation='bubblewrap mount allowlist + PID namespace; no corpus/repo mounts', **CONFIG)


class CodexProvider:
    def __init__(self, out, config=None):
        self.out = Path(out)
        self.config = config or runtime_identity()
        self.stopped = False
        self.invocations = 0
        self.cache_hits = 0

    async def call(self, key, system, data, schema, packet=None, *, expected_request_sha256=None):
        if self.stopped:
            raise AuthorizationBlocked('Queue already stopped')
        if not key or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in key):
            raise ValueError('Invalid call key')
        payload, images = prepare_request_bytes(system, data, packet)
        exact_request_sha256 = request_sha256(payload)
        if expected_request_sha256 is not None and exact_request_sha256 != expected_request_sha256:
            raise ValueError('Frozen request bytes/hash mismatch before invocation')
        request = dict(key=key, system=system, data=data, images=images, schema=schema, config=self.config)
        request_hash = digest(request)
        target = self.out / 'calls' / key
        target.mkdir(parents=True, exist_ok=True)
        cached = target / 'SUCCESS.json'
        if cached.exists():
            receipt = read(cached)
            if receipt['request_hash'] != request_hash:
                raise ValueError('Resume request/config drift')
            parsed = Path(receipt['normalized_path'])
            if sha(parsed) != receipt['normalized_sha256']:
                raise ValueError('Cached response drift')
            value = read(parsed)
            validate(value, schema)
            self.cache_hits += 1
            return value
        if (target / 'REQUEST.json').exists():
            if read(target / 'REQUEST.json') != request:
                raise ValueError('Failed request cannot resume under different configuration')
        else:
            immutable(target / 'REQUEST.json', request)
        attempt = target / ('attempt_%03d' % (len(list(target.glob('attempt_*'))) + 1))
        attempt.mkdir()
        immutable(attempt / 'schema.json', schema)
        immutable(attempt / 'packet_view.json', data)
        names = []
        for index, r in enumerate(images):
            name = 'image_%02d.png' % index
            shutil.copyfile(r['path'], attempt / name)
            if sha(attempt / name) != r['sha256']:
                raise ValueError('Copied raster drift')
            names.append(name)
        (attempt / 'prompt.txt').write_bytes(payload)
        input_hashes = {p.name: sha(p) for p in attempt.iterdir() if p.is_file()}
        command = sandbox_command(attempt, cli_command(names))
        if sha(attempt / 'prompt.txt') != exact_request_sha256:
            raise ValueError('Saved request bytes drift before invocation')
        immutable(attempt / 'INVOCATION.json', dict(at=now(), request_hash=request_hash,
                  exact_request_sha256=exact_request_sha256,
                  expected_request_sha256=expected_request_sha256,
                  command=command, input_hashes=input_hashes, environment_keys=list(safe_env())))
        self.invocations += 1
        started = time.monotonic()
        process = None
        timed_out = False
        try:
            with (attempt / 'raw.jsonl').open('wb') as raw, (attempt / 'stderr.txt').open('wb') as err:
                process = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
                    stdout=raw, stderr=err, env=safe_env(), start_new_session=True)
                try:
                    await asyncio.wait_for(process.communicate(payload), CONFIG['timeout_seconds'])
                except asyncio.TimeoutError:
                    timed_out = True
                    os.killpg(process.pid, signal.SIGKILL)
                    await process.wait()
        except BaseException:
            if process is not None and process.returncode is None:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
            raise
        records = []
        for line in (attempt / 'raw.jsonl').read_text().splitlines():
            try:
                records.append(json.loads(line))
            except ValueError:
                pass
        usage = [r['usage'] for r in records if r.get('type') == 'turn.completed' and r.get('usage')]
        # Tool execution is forbidden even inside the allowlist. Fail closed if it happens.
        tool_items = [r for r in records if r.get('item', {}).get('type') not in
                      {None, 'agent_message', 'reasoning', 'error'} and r.get('type', '').startswith('item.')]
        raw_text = (attempt / 'raw.jsonl').read_text() + (attempt / 'stderr.txt').read_text()
        receipt = dict(at=now(), request_hash=request_hash, provider='codex_chatgpt',
            exact_request_sha256=exact_request_sha256,
            expected_request_sha256=expected_request_sha256,
            seconds=time.monotonic() - started, exit_code=process.returncode, timed_out=timed_out,
            usage=usage, input_characters=len(payload.decode('utf-8')), source_images=len(images),
            raw_sha256=sha(attempt / 'raw.jsonl'), provider_cost_usd=None,
            cli_diagnostics=[r['item'] for r in records if r.get('item', {}).get('type') == 'error'],
            output_bytes=(attempt / 'final.txt').stat().st_size if (attempt / 'final.txt').exists() else 0,
            config=self.config, tool_items=len(tool_items))
        try:
            if process.returncode != 0 or timed_out:
                status = classify_error(raw_text)
                if status == 'AUTHORIZATION_BLOCKED':
                    self.stopped = True
                    raise AuthorizationBlocked(status)
                raise InferenceFailed('Codex process failed; see raw attempt artifacts')
            if not usage or tool_items:
                raise InferenceFailed('No completed turn or unexpected tool execution')
            if any(sha(attempt / name) != h for name, h in input_hashes.items()):
                raise InferenceFailed('Inference input mutated')
            value = json.loads((attempt / 'final.txt').read_text())
            validate(value, schema)
        except Exception as exc:
            receipt.update(status='AUTHORIZATION_BLOCKED' if isinstance(exc, AuthorizationBlocked)
                           else 'FAILED_CLOSED', error_type=type(exc).__name__)
            immutable(attempt / 'VALIDATION.json', receipt)
            raise
        immutable(attempt / 'normalized.json', value)
        receipt.update(status='SUCCESS', normalized_path=str(attempt / 'normalized.json'),
                       normalized_sha256=sha(attempt / 'normalized.json'))
        immutable(attempt / 'VALIDATION.json', receipt)
        immutable(cached, receipt)
        return value
