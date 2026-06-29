#!/usr/bin/env python3
from backend.app.main import app

paths = [getattr(r, "path", "") for r in app.routes]
print([p for p in paths if "critic" in p])
