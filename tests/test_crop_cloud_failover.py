"""Offline crop incident regression; all HTTP calls are intercepted."""
import json
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import socket
import ssl
import urllib.error
from uuid import uuid4

import pytest

from backend.app.pipeline.stages.crop_blocks import blocks
from backend.app.pipeline.stages.crop_blocks.cloud_failover import (
    CropFailover, LocalSource, SourceIdentityError, availability_failure,
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*a, **kw):
        raise AssertionError('Unexpected real cloud access')
    monkeypatch.setattr(blocks.urllib.request, 'urlopen', forbidden)


def document(tmp_path, n=100):
    doc = blocks.fitz.open()
    pg = doc.new_page(width=120, height=160)
    pg.insert_text((10, 35), 'crop geometry test')
    pg.set_rotation(90)
    doc.save(tmp_path / 'sample.pdf')
    doc.close()
    data = {'pages': [{'page_number': 1, 'width': 160, 'height': 120,
                      'blocks': [{'id': f'b{i}', 'block_type': 'image',
                                  'coords_px': [0, 0, 140, 90],
                                  'crop_url': f'https://crop.invalid/{i}.pdf'}
                                 for i in range(n)]}]}
    (tmp_path / 'sample_result.json').write_text(json.dumps(data))
    return LocalSource(tmp_path / 'sample.pdf', tmp_path / 'sample_result.json')


def run_crop(tmp_path):
    return blocks.crop_blocks(str(tmp_path), skip_small=False, dpi=100,
                             output_dir_name=str(tmp_path / 'out'), force=True)


def test_100_blocks_one_actual_http_call_and_geometry(monkeypatch, tmp_path, capsys):
    src = document(tmp_path)
    calls, sleeps = [], []
    def timeout(*a, **kw):
        calls.append(kw['timeout'])
        raise urllib.error.URLError(socket.timeout('simulated'))
    monkeypatch.setattr(blocks.urllib.request, 'urlopen', timeout)
    monkeypatch.setattr(blocks.time, 'sleep', sleeps.append)
    result = run_crop(tmp_path)
    m = result['crop_failover']
    assert m['cloud_crop_attempts'] == len(calls) == 1
    assert m['cloud_crop_failures'] == 1
    assert m['cloud_crop_skipped_after_breaker'] == 99
    assert m['local_crop_attempts'] == m['local_crop_successes'] == 100
    assert m['local_crop_failures'] == result['errors'] == 0
    assert sleeps == []
    assert capsys.readouterr().out.count('CLOUD_CROP_UNAVAILABLE') == 1
    # Exact bytes vs the existing rotated local PDF fallback, same arguments.
    expected = tmp_path / 'expected.png'
    blocks.crop_from_pdf(src.pdf, 1, [0, 0, 140, 90], 160, 120, expected,
                         dpi=100, min_long_side=blocks.MIN_LONG_SIDE_PX)
    assert (tmp_path / 'out/block_b0.png').read_bytes() == expected.read_bytes()
    assert (tmp_path / 'out/block_b99.png').read_bytes() == expected.read_bytes()
    index = json.loads((tmp_path / 'out/index.json').read_text())
    assert index['crop_failover'] == m
    assert index['blocks'][0]['source'] == 'pdf_fallback'


def test_cloud_success_unchanged(monkeypatch, tmp_path):
    src = document(tmp_path, 3)
    class Response:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self): return src.pdf.read_bytes()
    calls = []
    monkeypatch.setattr(blocks.urllib.request, 'urlopen', lambda *a, **kw: calls.append(1) or Response())
    result = run_crop(tmp_path)
    assert len(calls) == 3
    assert result['crop_failover']['local_crop_attempts'] == 0
    assert {b['source'] for b in result['blocks']} == {'cloud'}


def test_missing_local_is_failure(monkeypatch, tmp_path):
    src = document(tmp_path, 2)
    src.pdf.unlink()
    calls = []
    def timeout(*a, **kw):
        calls.append(1)
        raise TimeoutError('simulated')
    monkeypatch.setattr(blocks.urllib.request, 'urlopen', timeout)
    monkeypatch.setattr(blocks.time, 'sleep', lambda _: None)
    result = run_crop(tmp_path)
    assert result['errors'] == 2
    assert result['total_blocks'] == 0
    # No verified local source: preserve the old HTTP retry policy.
    assert len(calls) == 6
    assert result['crop_failover']['cloud_crop_skipped_after_breaker'] == 0


def test_corrupt_cloud_content_stays_visible(monkeypatch, tmp_path):
    document(tmp_path, 2)
    def corrupt(*a, **kw): raise ValueError('corrupted content')
    monkeypatch.setattr(blocks, 'download_and_convert', corrupt)
    result = run_crop(tmp_path)
    assert result['errors'] == 2
    assert result['crop_failover']['cloud_crop_attempts'] == 2
    assert result['crop_failover']['local_crop_attempts'] == 0


def test_failed_local_does_not_open(tmp_path):
    src = document(tmp_path, 1)
    breaker = CropFailover(state_root=tmp_path / 'state')
    def cloud(): raise TimeoutError()
    def local(): raise ValueError('bad bbox')
    for _ in range(2):
        with pytest.raises(ValueError):
            breaker.crop(source=src, provider='x', cloud=cloud, local=local)
    assert breaker.metrics['cloud_crop_attempts'] == 2
    assert breaker.metrics['local_crop_failures'] == 2


def test_source_mutation_rejected(tmp_path):
    src = document(tmp_path, 1)
    src.pdf.write_bytes(b'wrong PDF')
    with pytest.raises(SourceIdentityError):
        CropFailover(state_root=tmp_path / 'state').crop(
            source=src, provider='x', cloud=lambda: None, local=lambda: None)


def test_new_job_probes_again_and_different_source_isolated(tmp_path):
    src = document(tmp_path, 1)
    calls = []
    def cloud():
        calls.append(1)
        raise TimeoutError()
    for job in ('a', 'b'):
        breaker = CropFailover(job, tmp_path / 'state')
        for provider in ('x', 'y'):
            for _ in range(2):
                breaker.crop(source=src, provider=provider, cloud=cloud, local=lambda: (1, 1))
    assert len(calls) == 4


def _parallel_task(args):
    root, job = args
    src = LocalSource(root / 'sample.pdf', root / 'sample_result.json')
    breaker = CropFailover(job, root / 'state')
    def cloud():
        with (root / 'attempts').open('a') as f: f.write('attempt\n')
        raise TimeoutError('simulated')
    result = breaker.crop(source=src, provider='x', cloud=cloud, local=lambda: (1, 1))
    return result, breaker.metrics


@pytest.mark.parametrize('processes', [False, True])
def test_parallel_probe_is_serialized(tmp_path, processes):
    document(tmp_path, 1)
    args = [(tmp_path, uuid4().hex)] * 20
    if processes:
        with multiprocessing.get_context('fork').Pool(8) as pool:
            results = pool.map(_parallel_task, args)
    else:
        with ThreadPoolExecutor(20) as pool:
            results = list(pool.map(_parallel_task, args))
    assert (tmp_path / 'attempts').read_text().splitlines() == ['attempt']
    assert sum(m['local_crop_successes'] for _, m in results) == 20
    assert sum(m['cloud_crop_skipped_after_breaker'] for _, m in results) == 19


@pytest.mark.parametrize('error,expected', [
    (TimeoutError(), True), (ConnectionRefusedError(), True),
    (socket.gaierror(), True), (ssl.SSLCertVerificationError(), False),
    (ValueError('wrong source'), False), (OSError('disk'), False),
    *[(urllib.error.HTTPError('x', c, '', {}, None), c in (408, 502, 503, 504))
      for c in (400, 403, 404, 408, 429, 500, 502, 503, 504)],
])
def test_failure_classification(error, expected):
    assert availability_failure(error) is expected


def test_wrong_named_pdf_not_substituted(tmp_path):
    src = document(tmp_path, 1)
    sources = blocks._source_files(tmp_path, {})
    assert blocks._select_source_pdf(tmp_path, tmp_path / 'other_result.json', [], sources) is None
    assert blocks._select_source_pdf(tmp_path, src.ocr, [], sources) == src.pdf


def test_declared_hash_and_normalized_copy_mismatch_fail_closed(tmp_path):
    src = document(tmp_path, 1)
    with pytest.raises(SourceIdentityError, match='SHA256'):
        LocalSource(src.pdf, src.ocr, expected_sha256='0' * 64)
    wrong = tmp_path / 'wrong.pdf'
    wrong.write_bytes(b'other document')
    with pytest.raises(SourceIdentityError, match='declared source'):
        LocalSource(src.pdf, src.ocr, expected_pdf=wrong)


def test_normalized_source_requires_same_bytes(tmp_path):
    src = document(tmp_path, 1)
    normalized = tmp_path / 'document.pdf'
    normalized.write_bytes(src.pdf.read_bytes())
    sources = blocks._source_files(tmp_path, {})
    verified = blocks._verified_crop_source(normalized, src.ocr, {'pdf_path': 'sample.pdf'}, sources)
    assert verified.pdf == normalized
    with pytest.raises(SourceIdentityError):
        blocks._verified_crop_source(normalized, src.ocr, {'pdf_path': 'unknown.pdf'}, sources)
    normalized.write_bytes(b'wrong document')
    with pytest.raises(SourceIdentityError):
        blocks._verified_crop_source(normalized, src.ocr, {'pdf_path': 'sample.pdf'}, sources)


def test_generic_multi_pdf_source_is_ambiguous(tmp_path):
    document(tmp_path, 1)
    (tmp_path / 'other.pdf').write_bytes(b'other')
    sources = blocks._source_files(tmp_path, {})
    assert blocks._select_source_pdf(tmp_path, tmp_path / 'result.json',
                                     ['sample.pdf', 'other.pdf'], sources) is None


def test_different_pdf_same_provider_not_disabled(tmp_path):
    a, b = tmp_path / 'a', tmp_path / 'b'
    a.mkdir(); b.mkdir()
    srcs = [document(a, 1), document(b, 1)]
    breaker = CropFailover('shared-job', tmp_path / 'state')
    calls = []
    def cloud():
        calls.append(1)
        raise TimeoutError()
    for src in srcs:
        for _ in range(2):
            breaker.crop(source=src, provider='x', cloud=cloud, local=lambda: (1, 1))
    assert len(calls) == 2


def test_env_job_id_shared_but_independent_default_not_shared(monkeypatch, tmp_path):
    src = document(tmp_path, 1)
    calls = []
    def cloud():
        calls.append(1)
        raise TimeoutError()
    monkeypatch.setenv('AUDIT_CROP_JOB_ID', 'precrop-and-foreground')
    for _ in range(2):
        CropFailover(state_root=tmp_path / 'state').crop(
            source=src, provider='x', cloud=cloud, local=lambda: (1, 1))
    assert len(calls) == 1
    monkeypatch.delenv('AUDIT_CROP_JOB_ID')
    for _ in range(2):
        CropFailover(state_root=tmp_path / 'state').crop(
            source=src, provider='x', cloud=cloud, local=lambda: (1, 1))
    assert len(calls) == 3
