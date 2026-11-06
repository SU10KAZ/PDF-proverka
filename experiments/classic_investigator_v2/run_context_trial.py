"""Replay prepared source-context intervention without changing the running control."""
import argparse
import asyncio
from pathlib import Path
import time
from source_contract import read, write, sha
from investigate import configure
from quota_policy import assert_allowed
from subscription_retry import retry_subscription_call


async def run(work, trial):
    protocol = read(trial / 'protocol.json')
    if (trial / 'status.json').exists():
        raise ValueError('Existing trial requires explicit recovery')
    if sha(protocol['graph_file']) != protocol['graph_sha256']:
        raise ValueError('Graph changed')
    for item in protocol['requests']:
        if sha(item['path']) != item['sha256']:
            raise ValueError('Prepared request changed')
        request = read(item['path'])
        if request['options']['model'] not in {'codex/gpt-6-astra', 'codex/gpt-6-sol'}:
            raise ValueError('Codex subscription only')
        for path, digest in request['image_hashes'].items():
            if sha(path) != digest:
                raise ValueError('Image changed')
    configure(trial)
    from backend.app.services.llm import codex_runner
    state = {'status': 'running', 'started': time.time(), 'code_sha256': sha(__file__),
             'protocol_sha256': sha(trial / 'protocol.json'), 'results': []}
    write(trial / 'status.json', state)
    try:
        for index, item in enumerate(protocol['requests']):
            if (work / 'STOP_REQUESTED.json').exists():
                raise RuntimeError('Stop requested')
            request = read(item['path'])
            attempts = []
            def record(attempt):
                attempts.append(attempt)
                write(trial / f'{index:02d}_attempts.json', attempts)
            async def guarded(*args, **kwargs):
                assert_allowed(work, kwargs['model'])
                return await codex_runner.run_codex_json_messages(*args, **kwargs)
            result = await retry_subscription_call(
                guarded, request['messages'], **request['options'],
                stop_requested=lambda: (work / 'STOP_REQUESTED.json').exists(), on_attempt=record)
            write(trial / f'{index:02d}_response.json', {
                'json': result.json_data, 'text': result.text, 'error': result.error_message,
                'usage': {k: getattr(result, k, None) for k in
                          ('model', 'input_tokens', 'output_tokens', 'duration_ms', 'cost_usd')}})
            if result.is_error:
                raise RuntimeError(result.error_message)
            state['results'].append({'model': item['model'], 'response': f'{index:02d}_response.json',
                                     'status': 'completed_unadjudicated'})
            write(trial / 'status.json', state)
            print(item['model'], 'completed', flush=True)
        state['status'] = 'completed_unadjudicated'
    except Exception as error:
        state.update(status='failed', error=f'{type(error).__name__}: {error}')
        raise
    finally:
        state['finished'] = time.time()
        write(trial / 'status.json', state)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--trial', type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(run(args.work, args.trial))
