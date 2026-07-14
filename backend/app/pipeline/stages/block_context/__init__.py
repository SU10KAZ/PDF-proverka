"""Этап «Векторные графы блоков» перед Stage 01.

Импорты ленивые: реестр профильных графов использует встроенный каталог этого
пакета и не должен циклически загружать builder при старте.
"""

__all__ = [
    "build_block_context",
    "load_block_context_summary",
    "validate_block_context_summary",
]


def __getattr__(name):
    if name == "build_block_context":
        from .builder import build_block_context
        return build_block_context
    if name in {"load_block_context_summary", "validate_block_context_summary"}:
        from .contract import load_block_context_summary, validate_block_context_summary
        return {
            "load_block_context_summary": load_block_context_summary,
            "validate_block_context_summary": validate_block_context_summary,
        }[name]
    raise AttributeError(name)
