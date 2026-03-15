"""Тесты для TTL-кеша iter_project_dirs."""
import time
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

import webapp.services.project_service as ps


def test_cache_returns_same_result():
    """Второй вызов использует кеш (не сканирует файловую систему)."""
    # Сбросить кеш
    ps._PROJECT_DIRS_CACHE.clear()
    ps._PROJECT_DIRS_CACHE_TIME = 0.0

    with patch.object(Path, "exists", return_value=True), \
         patch.object(Path, "iterdir", return_value=[]):
        result1 = ps.iter_project_dirs(force=True)
        result2 = ps.iter_project_dirs()
        assert result1 == result2


def test_cache_force_refresh():
    """force=True обновляет кеш."""
    ps._PROJECT_DIRS_CACHE.clear()
    ps._PROJECT_DIRS_CACHE_TIME = time.time()

    with patch.object(Path, "exists", return_value=True), \
         patch.object(Path, "iterdir", return_value=[]) as mock_iter:
        ps.iter_project_dirs(force=True)
        mock_iter.assert_called()


def test_cache_ttl_expiry():
    """Кеш истекает через TTL."""
    ps._PROJECT_DIRS_CACHE = [("test", Path("/test"))]
    ps._PROJECT_DIRS_CACHE_TIME = time.time() - ps._PROJECT_DIRS_TTL - 1

    with patch.object(Path, "exists", return_value=True), \
         patch.object(Path, "iterdir", return_value=[]) as mock_iter:
        ps.iter_project_dirs()
        mock_iter.assert_called()  # Cache expired → filesystem scan
