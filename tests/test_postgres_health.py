from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import WebSocketDisconnect

from infra_test_utils import load_infra_api


api = load_infra_api()


def table_result(names):
    return SimpleNamespace(scalars=lambda: names)


def exists_result(has_rows):
    return SimpleNamespace(scalar_one=lambda: has_rows)


class PostgresHealthTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.schema = patch.object(api, "OpenCartSchema")
        self.schema.start().return_value.get_pg_tables.return_value = ["catalog_product", "catalog_category"]
        self.addCleanup(self.schema.stop)

    async def test_real_rows_mark_database_filled_without_statistics(self):
        db = SimpleNamespace(execute=AsyncMock(side_effect=[
            table_result(["catalog_product"]), exists_result(True),
        ]))
        self.assertEqual(await api.get_postgres_state(db), "filled")
        query = str(db.execute.call_args.args[0])
        self.assertIn("EXISTS", query)
        self.assertIn("public.catalog_product", query)
        self.assertNotIn("pg_stat", query)

    async def test_partial_load_counts_data_outside_product_table(self):
        db = SimpleNamespace(execute=AsyncMock(side_effect=[
            table_result(["catalog_product", "catalog_category"]), exists_result(True),
        ]))
        self.assertEqual(await api.get_postgres_state(db), "filled")
        self.assertIn("public.catalog_category", str(db.execute.call_args.args[0]))
        self.assertEqual(db.execute.await_count, 2)

    async def test_all_existing_etl_tables_empty(self):
        db = SimpleNamespace(execute=AsyncMock(side_effect=[
            table_result(["catalog_product", "catalog_category"]),
            exists_result(False), exists_result(False),
        ]))
        self.assertEqual(await api.get_postgres_state(db), "empty")

    async def test_absent_etl_tables_do_not_query_missing_relations(self):
        db = SimpleNamespace(execute=AsyncMock(return_value=table_result(["django_migrations"])))
        self.assertEqual(await api.get_postgres_state(db), "empty")
        self.assertEqual(db.execute.await_count, 1)

    async def test_query_failure_is_not_reported_as_empty(self):
        db = SimpleNamespace(execute=AsyncMock(side_effect=OSError("connection refused")))
        with self.assertRaises(OSError):
            await api.get_postgres_state(db)

    async def websocket_state(self, db):
        messages = []

        async def sessions(ctx):
            yield db

        async def send_json(message):
            messages.append(message)
            raise WebSocketDisconnect()

        ws = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(current_context="zoovostorg (test)")),
            accept=AsyncMock(), send_json=send_json,
        )
        with patch.object(api, "create_dynamic_session", sessions):
            await api.watch_postgres_health(ws)
        return messages[0]

    async def test_websocket_reports_filled_for_real_rows(self):
        db = SimpleNamespace(execute=AsyncMock(side_effect=[
            table_result(["catalog_product"]), exists_result(True),
        ]))
        self.assertEqual(await self.websocket_state(db), {"state": "filled"})

    async def test_websocket_reports_disconnected_for_connection_failure(self):
        db = SimpleNamespace(execute=AsyncMock(side_effect=OSError("connection refused")))
        self.assertEqual(await self.websocket_state(db), {"state": "disconnected"})


if __name__ == "__main__":
    unittest.main()
