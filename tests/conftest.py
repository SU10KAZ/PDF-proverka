"""Общие фикстуры для тестов."""
import os
from pathlib import Path

# Гарантируем, что AUDIT_BASE_DIR указывает на корень проекта,
# чтобы config.py работал корректно при pytest на любой машине.
os.environ.setdefault(
    "AUDIT_BASE_DIR",
    str(Path(__file__).resolve().parent.parent),
)
