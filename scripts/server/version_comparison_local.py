"""Local portal entrypoint: only deterministic comparison recovery on startup.

The ordinary application startup starts unrelated pipelines. This replaces the
previous --lifespan off launcher while keeping those pipelines inactive.
"""
from backend.app.main import app
from backend.app.services.version_comparison.recovery import lifespan

app.router.lifespan_context = lifespan
