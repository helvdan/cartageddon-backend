import json
import unittest
from types import SimpleNamespace

from fastapi import FastAPI

from pipeline_api import create_pipeline_router


def stage(postfix):
    return SimpleNamespace(postfix=postfix)


class PipelineStagesTests(unittest.IsolatedAsyncioTestCase):
    async def get_stages(self, order):
        pipeline = SimpleNamespace(STAGES_ORDER=order)
        app = FastAPI()
        app.include_router(create_pipeline_router(pipeline))
        messages = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            messages.append(message)

        await app({
            "type": "http", "http_version": "1.1", "method": "GET",
            "scheme": "http", "path": "/pipeline/stages", "raw_path": b"/pipeline/stages",
            "query_string": b"", "root_path": "", "headers": [],
            "server": ("test", 80), "client": ("test", 1234),
        }, receive, send)
        self.assertEqual(messages[0]["status"], 200)
        return json.loads(b"".join(message.get("body", b"") for message in messages))

    async def test_preserves_pipeline_order_without_inventing_stages(self):
        data = await self.get_stages((stage("custom_first"), stage("reduced"), stage("loading_db")))
        self.assertEqual(data, {"stages": [
            [{"id": "custom_first", "target": None}],
            [{"id": "reduced", "target": None}],
            [{"id": "loading_db", "target": "Postgres"}],
        ]})

    async def test_preserves_parallel_groups_and_storage_targets(self):
        data = await self.get_stages((stage("joined"), (stage("loading_db"), stage("loading_s3"))))
        self.assertEqual(data["stages"][1], [
            {"id": "loading_db", "target": "Postgres"},
            {"id": "loading_s3", "target": "MinIO S3"},
        ])

    async def test_supports_empty_pipeline(self):
        self.assertEqual(await self.get_stages(()), {"stages": []})


if __name__ == "__main__":
    unittest.main()
