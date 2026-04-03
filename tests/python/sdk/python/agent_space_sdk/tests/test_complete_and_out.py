"""Tests for launch-supported SDK exports and complete_and_out helpers."""

import unittest
from uuid import uuid4

from agent_space_sdk import (
    AsyncAgentSpaceClient,
    AuthenticationError,
    CompleteAndOutRequest,
    CreateAgentRequest,
    RateLimitError,
    AgentSpaceClient,
)


def _agent_payload(kind: str, status: str) -> dict:
    return {
        "id": str(uuid4()),
        "kind": kind,
        "status": status,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "payload": {"kind": kind},
        "tags": [],
        "metadata": {},
    }


class SDKExportTests(unittest.TestCase):
    def test_public_root_exports_launch_errors(self):
        self.assertTrue(AuthenticationError)
        self.assertTrue(RateLimitError)

    def test_complete_and_out_request_serializes_outputs(self):
        request = CompleteAndOutRequest(
            result={"ok": True},
            outputs=[
                CreateAgentRequest(kind="child", payload={"n": 1}),
            ],
        )

        self.assertEqual(
            request.to_dict(),
            {
                "result": {"ok": True},
                "outputs": [{"kind": "child", "payload": {"n": 1}}],
            },
        )


class SyncCompleteAndOutTests(unittest.TestCase):
    def test_complete_and_out_maps_response(self):
        client = AgentSpaceClient("http://localhost:8080/api/v1")
        client.session.post = lambda *args, **kwargs: object()  # type: ignore[method-assign]
        client._handle_response = lambda response: {  # type: ignore[method-assign]
            "completed": _agent_payload("parent", "COMPLETED"),
            "created": [_agent_payload("child", "NEW")],
        }

        completed, created = client.complete_and_out(
            tuple_id=str(uuid4()),
            request=CompleteAndOutRequest(
                result={"ok": True},
                outputs=[CreateAgentRequest(kind="child", payload={"n": 1})],
            ),
            agent_id="agent-1",
            lease_token="lease-1",
        )

        self.assertEqual(completed.status.value, "COMPLETED")
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0].kind, "child")


class AsyncCompleteAndOutTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_and_out_maps_response(self):
        client = AsyncAgentSpaceClient("http://localhost:8080/api/v1")

        async def fake_make_request(*args, **kwargs):
            return {
                "completed": _agent_payload("parent", "COMPLETED"),
                "created": [_agent_payload("child", "NEW")],
            }

        client._make_request = fake_make_request  # type: ignore[method-assign]

        completed, created = await client.complete_and_out(
            tuple_id=uuid4(),
            request=CompleteAndOutRequest(
                result={"ok": True},
                outputs=[CreateAgentRequest(kind="child", payload={"n": 1})],
            ),
            agent_id="agent-1",
            lease_token="lease-1",
        )

        self.assertEqual(completed.status.value, "COMPLETED")
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0].kind, "child")


if __name__ == "__main__":
    unittest.main()
