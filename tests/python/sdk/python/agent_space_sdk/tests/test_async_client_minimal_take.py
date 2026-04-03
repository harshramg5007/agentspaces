"""Tests for async client minimal take handling."""

import unittest
from uuid import uuid4

from agent_space_sdk.async_client import AsyncAgentSpaceClient
from agent_space_sdk.models import Query, TakeRequest, AgentClaim


class AsyncClientMinimalTakeTests(unittest.IsolatedAsyncioTestCase):
    async def test_take_agent_returns_agent_claim_for_minimal_response(self):
        client = AsyncAgentSpaceClient("http://localhost:8080/api/v1")

        async def fake_make_request(*args, **kwargs):
            return {"id": str(uuid4()), "lease_token": "lease-token-1"}

        client._make_request = fake_make_request  # type: ignore[method-assign]

        claim = await client.take_agent(
            TakeRequest(query=Query(kind="bench_scaling")),
            agent_id="agent-1",
            prefer_minimal=True,
        )

        self.assertIsInstance(claim, AgentClaim)
        self.assertEqual(claim.lease_token, "lease-token-1")


if __name__ == "__main__":
    unittest.main()
