"""Tests for async client error mapping."""

import unittest

from agent_space_sdk.async_client import AsyncAgentSpaceClient
from agent_space_sdk.exceptions import RateLimitError, AgentSpaceError


class _FakeResponse:
    def __init__(self, status: int):
        self.status = status


class AsyncClientErrorMappingTests(unittest.IsolatedAsyncioTestCase):
    async def test_check_response_maps_429_to_rate_limit(self):
        client = AsyncAgentSpaceClient("http://localhost:8080/api/v1")
        response = _FakeResponse(429)

        with self.assertRaises(RateLimitError):
            await client._check_response(  # noqa: SLF001 - testing internal mapping
                response,
                {"message": "Too Many Requests", "error": "RATE_LIMIT"},
            )

    async def test_check_response_uses_base_error_for_unknown_status(self):
        client = AsyncAgentSpaceClient("http://localhost:8080/api/v1")
        response = _FakeResponse(418)

        with self.assertRaises(AgentSpaceError):
            await client._check_response(  # noqa: SLF001 - testing internal mapping
                response,
                {"message": "I'm a teapot"},
            )


if __name__ == "__main__":
    unittest.main()
