from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException, WebSocketDisconnect

from infra.s3 import clear_s3_objects, get_s3_state
from infra_test_utils import load_infra_api


api = load_infra_api()


class S3Tests(unittest.TestCase):
    def setUp(self):
        self.ctx = SimpleNamespace(
            aws_access_key_id="test-key", aws_secret_access_key="test-secret",
            aws_bucket_name="test-bucket",
            get_s3_config=lambda: {
                "aws_access_key_id": "test-key", "aws_secret_access_key": "test-secret",
                "endpoint_url": "http://storage.example:9000", "region_name": "us-east-1",
            },
        )
        self.client = Mock()
        self.sdk = ModuleType("boto3")
        self.sdk.session = SimpleNamespace(Session=Mock())
        self.sdk.session.Session.return_value.client.return_value = self.client
        config = ModuleType("botocore.config")
        config.Config = Mock()
        self.config = config.Config
        dependencies = patch.dict("sys.modules", {"boto3": self.sdk, "botocore.config": config})
        dependencies.start()
        self.addCleanup(dependencies.stop)

    def test_filled_bucket_uses_context_and_requests_only_one_object(self):
        self.client.list_objects_v2.return_value = {"Contents": [{"Key": "image.jpg"}]}
        self.assertEqual(get_s3_state(self.ctx), "filled")
        self.client.list_objects_v2.assert_called_once_with(Bucket="test-bucket", MaxKeys=1)
        options = self.sdk.session.Session.return_value.client.call_args.kwargs
        self.assertEqual(options["endpoint_url"], "http://storage.example:9000")
        self.assertEqual(options["aws_access_key_id"], "test-key")
        self.assertEqual(options["aws_secret_access_key"], "test-secret")
        self.assertEqual(options["region_name"], "us-east-1")
        self.client.close.assert_called_once()

    def test_empty_bucket(self):
        self.client.list_objects_v2.return_value = {"KeyCount": 0}
        self.assertEqual(get_s3_state(self.ctx), "empty")

    def test_access_failure_is_not_empty_and_client_is_closed(self):
        self.client.list_objects_v2.side_effect = OSError("access denied")
        with self.assertRaises(OSError):
            get_s3_state(self.ctx)
        self.client.close.assert_called_once()

    def test_missing_settings_do_not_fall_back_to_environment_credentials(self):
        self.ctx.aws_secret_access_key = None
        with self.assertRaises(ValueError):
            get_s3_state(self.ctx)
        self.sdk.session.Session.assert_not_called()

    def test_clear_bucket_paginates_without_deleting_bucket(self):
        first_page = [{"Key": f"image_{i}.jpg"} for i in range(1000)]
        self.client.get_paginator.return_value.paginate.return_value = [
            {"Contents": first_page}, {"Contents": [{"Key": "last.jpg"}]},
        ]
        self.client.delete_objects.return_value = {}
        self.assertEqual(clear_s3_objects(self.ctx), 1001)
        batches = self.client.delete_objects.call_args_list
        self.assertEqual([len(call.kwargs["Delete"]["Objects"]) for call in batches], [1000, 1])
        self.assertTrue(all(call.kwargs["Bucket"] == "test-bucket" for call in batches))
        self.client.delete_bucket.assert_not_called()
        self.client.close.assert_called_once()

    def test_clear_empty_bucket(self):
        self.client.get_paginator.return_value.paginate.return_value = [{}]
        self.assertEqual(clear_s3_objects(self.ctx), 0)
        self.client.delete_objects.assert_not_called()

    def test_partial_delete_errors_are_not_success(self):
        self.client.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": "locked"}]}]
        self.client.delete_objects.return_value = {"Errors": [{"Key": "locked", "Code": "AccessDenied"}]}
        with self.assertRaises(RuntimeError):
            clear_s3_objects(self.ctx)
        self.client.close.assert_called_once()


class S3ApiTests(unittest.IsolatedAsyncioTestCase):
    async def websocket_state(self, state=None, error=None):
        messages = []

        async def send_json(message):
            messages.append(message)
            raise WebSocketDisconnect()

        ws = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(current_context="sample")),
            accept=AsyncMock(), send_json=send_json,
        )
        with patch.object(api, "get_runtime_context", return_value="sample-settings") as get_context:
            with patch.object(api.asyncio, "to_thread", new=AsyncMock(return_value=state, side_effect=error)) as worker:
                await api.watch_s3_health(ws)
                worker.assert_awaited_once_with(api.get_s3_state, "sample-settings")
            get_context.assert_called_once_with("sample")
        return messages[0]

    async def test_websocket_reports_bucket_states(self):
        for state in ("empty", "filled"):
            self.assertEqual(await self.websocket_state(state), {"state": state})

    async def test_websocket_reports_missing_bucket_or_failed_access_as_disconnected(self):
        for error in (ValueError("missing configuration"), OSError("unreachable"), RuntimeError("NoSuchBucket")):
            self.assertEqual(await self.websocket_state(error=error), {"state": "disconnected"})

    async def test_clear_uses_current_context_and_runs_off_event_loop(self):
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(current_context="test")))
        with patch.object(api, "get_runtime_context", return_value="test-settings") as get_context:
            with patch.object(api.asyncio, "to_thread", new=AsyncMock(return_value=12)) as worker:
                self.assertEqual(await api.truncate_s3(request), {"deleted_objects": 12})
                worker.assert_awaited_once_with(api.clear_s3_objects, "test-settings")
            get_context.assert_called_once_with("test")

    async def test_clear_failure_does_not_leak_credentials(self):
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(current_context="test")))
        with patch.object(api.asyncio, "to_thread", new=AsyncMock(side_effect=RuntimeError("test-secret"))):
            with self.assertRaises(HTTPException) as caught:
                await api.truncate_s3(request)
        self.assertEqual(caught.exception.status_code, 502)
        self.assertNotIn("test-secret", caught.exception.detail)


if __name__ == "__main__":
    unittest.main()
