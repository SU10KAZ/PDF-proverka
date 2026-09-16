"""Isolated one-shot Codex calls for the new broad-context research stage.

Uses the existing mount allowlist/CLI settings verbatim. Does not change the
83k-character serializer or any inference/admission code used by comparison.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import time

from experiments.project_change_semantic_codex_272.provider import (
    cli_command, sandbox_command, safe_env, classify_error,
)
from experiments.project_change_semantic_codex_272.schema import validate
from .preflight import sha, read, write


async def call(key, input_dir, output_dir, schema, expected):
    target = Path(output_dir) / key
    if target.exists():
        raise FileExistsError('No automatic retry/resume of a model request: ' + key)
    target.mkdir(parents=True)
    payload = (input_dir / 'EXACT_PROMPT.txt').read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected:
        raise ValueError('Frozen request drift')
    images = read(input_dir / 'IMAGES.json')
    shutil.copyfile(input_dir / 'EXACT_PROMPT.txt', target / 'prompt.txt')
    write(target / 'schema.json', schema)
    names = []
    for i, raster in enumerate(images):
        name = f'image_{i:02d}.png'
        shutil.copyfile(raster['path'], target / name)
        if sha(target / name) != raster['sha256']:
            raise ValueError('Raster drift')
        names.append(name)
    input_hashes = {p.name: sha(p) for p in target.iterdir() if p.is_file()}
    command = sandbox_command(target, cli_command(names))
    write(target / 'INVOCATION.json', dict(model='gpt-6-astra', reasoning='xhigh',
          command=command, input_hashes=input_hashes, exact_request_sha256=expected,
          provider='codex_chatgpt', tools_disabled=True, fresh_context=True, retries=0))
    started = time.monotonic()
    process = None
    records, usage = [], []
    try:
        with (target / 'raw.jsonl').open('wb') as stdout, (target / 'stderr.txt').open('wb') as stderr:
            process = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
                stdout=stdout, stderr=stderr, env=safe_env(), start_new_session=True)
            await asyncio.wait_for(process.communicate(payload), timeout=900)
        for line in (target / 'raw.jsonl').read_text().splitlines():
            try:
                records.append(json.loads(line))
            except ValueError:
                continue
        usage = [r['usage'] for r in records if r.get('type') == 'turn.completed' and r.get('usage')]
        tools = [r for r in records if r.get('item', {}).get('type') not in
                 {None, 'agent_message', 'reasoning', 'error'} and r.get('type', '').startswith('item.')]
        if process.returncode or not usage or tools:
            raise RuntimeError(classify_error((target / 'stderr.txt').read_text() +
                                             (target / 'raw.jsonl').read_text()))
        if any(sha(target / name) != digest for name, digest in input_hashes.items()):
            raise ValueError('Model input mutated')
        value = read(target / 'final.txt')
        validate(value, schema)
        if value['bundle_id'] != key:
            raise ValueError('Response bundle ID mismatch')
        ids = [c['candidate_id'] for c in value['candidates']]
        if len(set(ids)) != len(ids):
            raise ValueError('Duplicate local candidate IDs inside response')
        write(target / 'parsed.json', value)
        write(target / 'SUCCESS.json', dict(status='SUCCESS', usage=usage,
              seconds=time.monotonic() - started, output_sha256=sha(target / 'parsed.json'),
              raw_sha256=sha(target / 'raw.jsonl'), exact_request_sha256=expected,
              images=len(images), tool_items=0, model='gpt-6-astra', reasoning='xhigh'))
        return value
    except BaseException as exc:
        if process is not None and process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        write(target / 'FAILURE.json', dict(status='FAILED_CLOSED', error_type=type(exc).__name__,
              error=str(exc), usage=usage, seconds=time.monotonic() - started, retries=0))
        raise
