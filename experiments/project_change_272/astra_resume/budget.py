"""Offline conservative reserve estimate; no evidence mutation or network access."""
import hashlib
from pathlib import Path
import sys


def reserve(payload, schema_text, image_count):
    cache = Path('/tmp/data-gym-cache/fb374d419588a4632f3f557e76b4b70aebbca790')
    assert hashlib.sha256(cache.read_bytes()).hexdigest() == '446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d'
    # Load verified ranks directly: get_encoding must never fetch a missing cache.
    sys.path.insert(0, '/tmp/projectchange-capacity-deps')
    import tiktoken
    from tiktoken.load import load_tiktoken_bpe
    from tiktoken_ext.openai_public import o200k_base
    # The constructor's cache URL maps to the verified file above. Use its local
    # parameters while preventing network cache loading altogether.
    import tiktoken_ext.openai_public as public
    original = public.load_tiktoken_bpe
    public.load_tiktoken_bpe = lambda *a, **kw: load_tiktoken_bpe(str(cache))
    try:
        params = o200k_base()
    finally:
        public.load_tiktoken_bpe = original
    enc = tiktoken.Encoding(**params)
    text_tokens = len(enc.encode(payload, disallowed_special=())) + len(enc.encode(schema_text, disallowed_special=()))
    return {'estimated_text_tokens': text_tokens, 'text_with_25_percent_margin': (text_tokens*5+3)//4,
            'image_reserve': image_count*20000, 'output_reserve': 150000, 'envelope_reserve': 50000,
            'total': (text_tokens*5+3)//4 + image_count*20000 + 200000,
            'method': 'local verified o200k_base estimate + 25% text margin + 20k/image + 50k envelope + 150k output'}
