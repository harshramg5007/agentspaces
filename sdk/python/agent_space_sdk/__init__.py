"""
Agent Space SDK for Python

A Python client library for interacting with the Agent Space API.
Provides synchronous and asynchronous clients for the launch-supported lifecycle API.
"""

__version__ = "1.0.0"

from .client import AgentSpaceClient
try:
    from .async_client import AsyncAgentSpaceClient
except ImportError as exc:
    class AsyncAgentSpaceClient:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "aiohttp is required for AsyncAgentSpaceClient. "
                "Install with: pip install agent-spaces[async]"
            ) from exc
from .models import (
    AgentClaim,
    Agent,
    Query,
    Event,
    DAG,
    DAGNode,
    Edge,
    AgentStatus,
    EventType,
    EdgeType,
    CreateAgentRequest,
    UpdateAgentRequest,
    TakeRequest,
    InRequest,
    ReadRequest,
    CompleteAgentRequest,
    CompleteAndOutRequest,
    RenewLeaseRequest,
)
from .exceptions import (
    AgentSpaceError,
    NotFoundError,
    BadRequestError,
    TimeoutError,
    InternalError,
    AuthenticationError,
    RateLimitError,
    ServiceUnavailableError,
    GatewayTimeoutError,
)

__all__ = [
    "AgentSpaceClient",
    "AsyncAgentSpaceClient",
    "AgentClaim",
    "Agent",
    "Query",
    "Event",
    "DAG",
    "DAGNode",
    "Edge",
    "AgentStatus",
    "EventType",
    "EdgeType",
    "CreateAgentRequest",
    "UpdateAgentRequest",
    "TakeRequest",
    "InRequest",
    "ReadRequest",
    "CompleteAgentRequest",
    "CompleteAndOutRequest",
    "RenewLeaseRequest",
    "AgentSpaceError",
    "NotFoundError",
    "BadRequestError",
    "TimeoutError",
    "InternalError",
    "AuthenticationError",
    "RateLimitError",
    "ServiceUnavailableError",
    "GatewayTimeoutError",
]
