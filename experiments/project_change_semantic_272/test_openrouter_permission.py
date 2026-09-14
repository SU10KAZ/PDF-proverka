import asyncio
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from .run import run
from .model_io import LocalCalls
from .openrouter_permission import OpenRouterPermissionRequired,require_openrouter_permission


class OpenRouterUserPermission(unittest.TestCase):
    def test_bulk_run_stops_before_client_or_source_access(self):
        with patch('openai.AsyncOpenAI') as client, patch('experiments.project_change_semantic_272.run.authorize') as sources:
            with self.assertRaises(OpenRouterPermissionRequired):
                asyncio.run(run('not-authorized',Path('/does-not-exist')))
            client.assert_not_called()
            sources.assert_not_called()

    def test_ownership_stops_before_client_creation(self):
        with patch('openai.AsyncOpenAI') as client:
            with self.assertRaises(OpenRouterPermissionRequired):LocalCalls(Path('/does-not-exist'))
            client.assert_not_called()

    def test_enabled_paid_api_and_available_key_do_not_grant_user_permission(self):
        with patch.dict(os.environ,{'PAID_API_ENABLED':'true','OPENROUTER_API_KEY':'synthetic-test-key'}):
            with self.assertRaises(OpenRouterPermissionRequired):require_openrouter_permission()


if __name__=='__main__':unittest.main()
