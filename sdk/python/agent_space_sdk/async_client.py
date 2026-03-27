"""Asynchronous client for the Agent Space API."""

import asyncio
import json
from typing import Optional, List, Dict, Any, Tuple as TupleType, Union
from uuid import UUID
from urllib.parse import urljoin

try:
    import aiohttp
except ImportError:
    raise ImportError(
        "aiohttp is required for async client. "
        "Install it with: pip install agent-space-sdk[async]"
    )

from .models.agent import Agent, AgentClaim, Query, Event, EventType
from .models.dag import DAG
from .models.requests import (
    CreateAgentRequest,
    UpdateAgentRequest,
    TakeRequest,
    InRequest,
    ReadRequest,
    CompleteAgentRequest,
    CompleteAndOutRequest,
    RenewLeaseRequest,
)
from .exceptions.errors import (
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
from .utils.retry import RetryConfig, calculate_delay, should_retry
from .utils.auth import AuthConfig


class AsyncAgentSpaceClient:
    """
    Asynchronous client for interacting with the Agent Space API.
    
    This client provides async/await methods for all agent space operations.
    It's ideal for high-concurrency applications and integration with async
    frameworks like FastAPI or aiohttp.
    
    Example:
        ```python
        import asyncio
        
        async def main():
            # Create a client
            async with AsyncAgentSpaceClient("http://localhost:8080/api/v1") as client:
                # Create a agent
                request = CreateAgentRequest(
                    kind="task",
                    payload={"action": "process", "data": "example"}
                )
                agent = await client.create_agent(request)
                
                # Query agents
                query = Query(kind="task", status=AgentStatus.NEW)
                results, total = await client.query_agents(query)
        
        asyncio.run(main())
        ```
    """
    
    def __init__(
        self,
        base_url: str = "http://localhost:8080/api/v1",
        api_key: Optional[str] = None,
        retry_config: Optional[RetryConfig] = None,
        timeout: float = 30.0,
        connector: Optional[aiohttp.BaseConnector] = None
    ):
        """
        Initialize the async Agent Space client.
        
        Args:
            base_url: Base URL for the Agent Space API
            api_key: Optional API key for authentication
            retry_config: Configuration for retry behavior
            timeout: Default timeout for requests in seconds
            connector: Optional aiohttp connector for connection pooling
        """
        self.base_url = base_url.rstrip('/') + '/'
        self.api_key = api_key
        self.retry_config = retry_config or RetryConfig()
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._connector = connector
        self._session: Optional[aiohttp.ClientSession] = None
        self._owns_session = False
    
    async def __aenter__(self):
        """Async context manager entry."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=self.timeout
            )
            self._owns_session = True
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def close(self):
        """Close the underlying session."""
        if self._session and self._owns_session:
            await self._session.close()
            self._session = None
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication if configured."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        extra_headers: Optional[Dict[str, str]] = None
    ) -> aiohttp.ClientResponse:
        """
        Make an async HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, PUT, etc.)
            endpoint: API endpoint path
            json_data: JSON data for request body
            params: Query parameters
            timeout: Request timeout (uses default if not specified)
            extra_headers: Additional headers to include

        Returns:
            Response object

        Raises:
            Various AgentSpaceError subclasses based on response
        """
        if self._session is None:
            raise RuntimeError("Client session not initialized. Use 'async with' or call __aenter__")

        # Strip leading slash from endpoint to avoid urljoin issues
        url = urljoin(self.base_url, endpoint.lstrip('/'))
        headers = self._get_headers()
        if extra_headers:
            headers.update(extra_headers)
        
        # Apply custom timeout if specified
        request_timeout = self.timeout
        if timeout:
            request_timeout = aiohttp.ClientTimeout(total=timeout)
        
        # Retry logic
        last_exception = None
        for attempt in range(self.retry_config.max_retries + 1):
            try:
                async with self._session.request(
                    method=method,
                    url=url,
                    json=json_data,
                    params=params,
                    headers=headers,
                    timeout=request_timeout
                ) as response:
                    if response.status == 204:
                        return None
                    # Check if we should retry based on status code
                    if response.status in self.retry_config.retry_status_codes:
                        if attempt < self.retry_config.max_retries:
                            delay = calculate_delay(attempt, self.retry_config)
                            await asyncio.sleep(delay)
                            continue
                    
                    # Read response body
                    try:
                        response_data = await response.json()
                    except (json.JSONDecodeError, aiohttp.ContentTypeError):
                        response_data = await response.text()
                    
                    # Check for errors
                    await self._check_response(response, response_data)
                    
                    return response_data
                    
            except asyncio.TimeoutError:
                raise TimeoutError("Request timed out")
            except aiohttp.ClientError as e:
                last_exception = e

                # Retry on connection errors
                if attempt < self.retry_config.max_retries:
                    delay = calculate_delay(attempt, self.retry_config)
                    await asyncio.sleep(delay)
                    continue

                raise AgentSpaceError(f"Request failed: {str(e)}")
        
        # If we've exhausted retries, raise the last exception
        if last_exception:
            raise AgentSpaceError(f"Request failed after {self.retry_config.max_retries} retries: {str(last_exception)}")
    
    async def _check_response(self, response: aiohttp.ClientResponse, response_data: Any) -> None:
        """
        Check response status and raise appropriate exceptions.
        
        Args:
            response: Response object to check
            response_data: Parsed response data
            
        Raises:
            Various AgentSpaceError subclasses based on status code
        """
        if response.status < 400:
            return
        
        # Parse error response
        if isinstance(response_data, dict):
            message = response_data.get("message", "Unknown error")
            error_code = response_data.get("error")
            details = response_data.get("details", {})
        else:
            message = f"HTTP {response.status}: {response_data}"
            error_code = None
            details = {}
        
        # Map status codes to exceptions
        if response.status == 400:
            raise BadRequestError(message, error_code, details)
        elif response.status == 401:
            raise AuthenticationError(message, error_code, details)
        elif response.status == 404:
            raise NotFoundError(message, error_code, details)
        elif response.status == 408:
            raise TimeoutError(message, error_code, details)
        elif response.status == 429:
            raise RateLimitError(message, error_code, details)
        elif response.status == 503:
            raise ServiceUnavailableError(message, error_code, details)
        elif response.status == 504:
            raise GatewayTimeoutError(message, error_code, details)
        elif response.status >= 500:
            raise InternalError(message, error_code, details)
        else:
            raise AgentSpaceError(message, error_code, details)
    
    # Agent operations
    
    async def create_agent(self, request: CreateAgentRequest) -> Agent:
        """
        Create a new agent in the agent space.
        
        Args:
            request: Agent creation request
            
        Returns:
            Created agent
            
        Raises:
            BadRequestError: If request is invalid
            InternalError: If server error occurs
        """
        response_data = await self._make_request(
            "POST",
            "/agents",
            json_data=request.to_dict()
        )
        return Agent.from_dict(response_data)
    
    async def get_agent(self, tuple_id: UUID) -> Agent:
        """
        Get a agent by its ID.
        
        Args:
            tuple_id: UUID of the agent
            
        Returns:
            The requested agent
            
        Raises:
            NotFoundError: If agent doesn't exist
        """
        response_data = await self._make_request(
            "GET",
            f"/agents/{tuple_id}"
        )
        return Agent.from_dict(response_data)
    
    async def update_agent(self, tuple_id: UUID, request: UpdateAgentRequest) -> Agent:
        """
        Update an existing agent.
        
        Args:
            tuple_id: UUID of the agent to update
            request: Update request with new values
            
        Returns:
            Updated agent
            
        Raises:
            NotFoundError: If agent doesn't exist
            BadRequestError: If update is invalid
        """
        response_data = await self._make_request(
            "PUT",
            f"/agents/{tuple_id}",
            json_data=request.to_dict()
        )
        return Agent.from_dict(response_data)
    
    async def query_agents(self, query: Query, timeout: Optional[float] = None) -> TupleType[List[Agent], int]:
        """
        Query agents without removing them from the space.

        Args:
            query: Query parameters
            timeout: Optional request timeout override (seconds)

        Returns:
            Agent of (list of matching agents, total count)
        """
        response_data = await self._make_request(
            "POST",
            "agents/query",
            json_data=query.to_dict(),
            timeout=timeout
        )
        if response_data is None:
            return [], 0
        agents_raw = response_data.get("agents") or []
        agents = [Agent.from_dict(t) for t in agents_raw]
        return agents, response_data.get("total", 0)

    async def agent_stats(self, query: Query, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Get agent counts by status for a query.

        Args:
            query: Query parameters
            timeout: Optional request timeout override (seconds)

        Returns:
            Dict with counts and total
        """
        response_data = await self._make_request(
            "POST",
            "agents/stats",
            json_data=query.to_dict(),
            timeout=timeout
        )
        return response_data or {"counts": {}, "total": 0}

    async def read_agent(self, request: ReadRequest) -> Optional[Agent]:
        """
        Read a agent from the space (non-destructive).

        Args:
            request: Read request with query and optional blocking settings

        Returns:
            Matching agent if found, None otherwise

        Raises:
            TimeoutError: If request times out (408 status)
        """
        try:
            response_data = await self._make_request(
                "POST",
                "agents/read",
                json_data=request.to_dict()
            )
            if response_data is None:
                return None
            return Agent.from_dict(response_data)
        except NotFoundError:
            # 204 No Content is translated to NotFoundError
            return None
    
    async def take_agent(
        self,
        request: TakeRequest,
        agent_id: Optional[str] = None,
        prefer_minimal: bool = False,
    ) -> Optional[Union[Agent, AgentClaim]]:
        """
        Take a agent from the space (destructive read).

        Args:
            request: Take request with query
            agent_id: Optional agent ID for ownership (sent via X-Agent-ID header)
            prefer_minimal: Request minimal response (only id and lease_token)

        Returns:
            Taken agent if found, or AgentClaim for minimal responses, else None
        """
        try:
            extra_headers = {}
            if agent_id:
                extra_headers["X-Agent-ID"] = agent_id
            if prefer_minimal:
                extra_headers["Prefer"] = "return=minimal"
            response_data = await self._make_request(
                "POST",
                "agents/take",
                json_data=request.to_dict(),
                extra_headers=extra_headers
            )
            if response_data is None:
                return None
            if (
                prefer_minimal
                and isinstance(response_data, dict)
                and "id" in response_data
                and "lease_token" in response_data
                and "kind" not in response_data
            ):
                return AgentClaim.from_dict(response_data)
            return Agent.from_dict(response_data)
        except NotFoundError:
            # 204 No Content is translated to NotFoundError
            return None
    
    async def in_agent(self, request: InRequest, agent_id: Optional[str] = None) -> Optional[Agent]:
        """
        Blocking take operation - wait for a matching agent.

        Args:
            request: In request with query and timeout
            agent_id: Optional agent ID for ownership (sent via X-Agent-ID header)

        Returns:
            Taken agent if found within timeout, None otherwise

        Raises:
            TimeoutError: If request times out (408 status)
        """
        # Use a longer timeout for the HTTP request than the in operation
        http_timeout = request.timeout + 5

        extra_headers = {}
        if agent_id:
            extra_headers["X-Agent-ID"] = agent_id
        response_data = await self._make_request(
            "POST",
            "agents/in",
            json_data=request.to_dict(),
            timeout=http_timeout,
            extra_headers=extra_headers
        )

        if response_data is None:
            return None
        return Agent.from_dict(response_data)
    
    async def complete_agent(
        self,
        tuple_id: UUID,
        request: CompleteAgentRequest,
        agent_id: str,
        lease_token: str,
        prefer_minimal: bool = False,
    ) -> Optional[Agent]:
        """
        Mark a agent as completed with a result.

        Args:
            tuple_id: UUID of the agent to complete
            request: Completion request with result data
            agent_id: Agent ID completing the agent
            lease_token: Lease token from the claim
            prefer_minimal: Request 204 minimal response body from the server

        Returns:
            Completed agent when representation is returned; otherwise None

        Raises:
            NotFoundError: If agent doesn't exist
            BadRequestError: If agent cannot be completed
        """
        if not agent_id:
            raise ValueError("agent_id is required")
        if not lease_token:
            raise ValueError("lease_token is required")
        headers = {
            "X-Agent-ID": agent_id,
            "X-Lease-Token": lease_token,
        }
        if prefer_minimal:
            headers["Prefer"] = "return=minimal"
        response_data = await self._make_request(
            "POST",
            f"agents/{tuple_id}/complete",
            json_data=request.to_dict(),
            extra_headers=headers,
        )
        if response_data is None:
            if prefer_minimal:
                return None
            # API returned empty body, try to fetch the agent
            try:
                return await self.get_agent(tuple_id)
            except NotFoundError:
                # Agent was completed but can't be fetched back - this is OK
                return None
        return Agent.from_dict(response_data)

    async def complete_and_out(
        self,
        tuple_id: UUID,
        request: CompleteAndOutRequest,
        agent_id: str,
        lease_token: str,
    ) -> TupleType[Agent, List[Agent]]:
        """
        Atomically complete a agent and create output agents.
        """
        if not agent_id:
            raise ValueError("agent_id is required")
        if not lease_token:
            raise ValueError("lease_token is required")

        response_data = await self._make_request(
            "POST",
            f"agents/{tuple_id}/complete-and-out",
            json_data=request.to_dict(),
            extra_headers={
                "X-Agent-ID": agent_id,
                "X-Lease-Token": lease_token,
            },
        )
        completed = Agent.from_dict(response_data["completed"])
        created = [Agent.from_dict(item) for item in response_data.get("created", [])]
        return completed, created
    
    async def release_agent(
        self,
        tuple_id: UUID,
        agent_id: str,
        lease_token: str,
        reason: str = "",
        obs: Optional[Dict[str, Any]] = None,
    ) -> Agent:
        """
        Release ownership of a agent.

        Args:
            tuple_id: UUID of the agent to release
            agent_id: ID of the agent releasing the agent (must match current owner)
            lease_token: Lease token from the claim
            reason: Optional reason for release (e.g., "lease_expired" for reaper)
            obs: Optional telemetry observation payload

        Returns:
            Released agent

        Raises:
            NotFoundError: If agent doesn't exist
            BadRequestError: If agent_id header is missing
        """
        if not agent_id:
            raise ValueError("agent_id is required")
        if not lease_token:
            raise ValueError("lease_token is required")

        # Build request body if reason or obs provided
        json_body = None
        if reason or obs is not None:
            json_body = {}
            if reason:
                json_body["reason"] = reason
            if obs is not None:
                json_body["obs"] = obs

        response_data = await self._make_request(
            "POST",
            f"agents/{tuple_id}/release",
            extra_headers={
                "X-Agent-ID": agent_id,
                "X-Lease-Token": lease_token,
            },
            json_data=json_body,
        )
        if response_data is None:
            # API returned empty body, fetch the agent
            return await self.get_agent(tuple_id)
        return Agent.from_dict(response_data)
    
    # Event operations
    
    async def get_events(
        self,
        tuple_id: Optional[UUID] = None,
        agent_id: Optional[str] = None,
        event_type: Optional[EventType] = None,
        trace_id: Optional[UUID] = None,
        limit: int = 100,
        offset: int = 0,
        cursor: Optional[str] = None,
        return_cursor: bool = False,
    ) -> Union[TupleType[List[Event], int], TupleType[List[Event], int, Optional[str]]]:
        """
        Get events from the agent space.
        
        Args:
            tuple_id: Filter by agent ID
            agent_id: Filter by agent ID
            event_type: Filter by event type
            trace_id: Filter by trace ID
            limit: Maximum number of events to return
            offset: Number of events to skip
            cursor: Optional cursor for cursor-based pagination
            return_cursor: When true, include next_cursor in response agent

        Returns:
            Agent of (list of events, total count)
            If return_cursor=True, returns (list of events, total count, next_cursor)
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if tuple_id:
            params["tuple_id"] = str(tuple_id)
        if agent_id:
            params["agent_id"] = agent_id
        if event_type:
            params["type"] = event_type.value
        if trace_id:
            params["trace_id"] = str(trace_id)
        if cursor:
            params["cursor"] = cursor
        
        response_data = await self._make_request(
            "GET",
            "/events",
            params=params
        )
        
        events_raw = response_data.get("events") or []
        events = [Event.from_dict(e) for e in events_raw]
        total = response_data.get("total", 0)
        next_cursor = response_data.get("next_cursor")
        if return_cursor:
            return events, total, next_cursor
        return events, total

    # Telemetry operations

    async def ingest_telemetry_span(self, span: Dict[str, Any]) -> int:
        """
        Ingest a single telemetry span.

        Args:
            span: Telemetry span payload

        Returns:
            Number of spans ingested
        """
        response_data = await self._make_request(
            "POST",
            "/telemetry/spans",
            json_data=span,
        )
        return int(response_data.get("ingested", 0))

    async def ingest_telemetry_spans(self, spans: List[Dict[str, Any]]) -> int:
        """
        Ingest telemetry spans in batch.

        Args:
            spans: List of telemetry span payloads

        Returns:
            Number of spans ingested
        """
        response_data = await self._make_request(
            "POST",
            "/telemetry/spans:batch",
            json_data={"spans": spans},
        )
        return int(response_data.get("ingested", 0))

    async def list_telemetry_spans(
        self,
        trace_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Query telemetry spans by trace ID.

        Args:
            trace_id: Trace ID to filter on
            limit: Maximum number of results
            offset: Offset for pagination

        Returns:
            List of telemetry spans (dicts)
        """
        response_data = await self._make_request(
            "GET",
            "/telemetry/spans",
            params={
                "trace_id": str(trace_id),
                "limit": limit,
                "offset": offset,
            },
        )
        return response_data.get("spans") or []

    async def renew_lease(
        self,
        tuple_id: UUID,
        agent_id: str,
        lease_token: str,
        request: Optional[RenewLeaseRequest] = None,
    ) -> Agent:
        """
        Renew a agent lease.

        Args:
            tuple_id: Agent ID
            agent_id: Agent ID renewing the lease
            lease_token: Current lease token
            request: Optional renew request (e.g., lease_sec override)

        Returns:
            Updated agent
        """
        json_body = request.to_dict() if request is not None else None
        response_data = await self._make_request(
            "POST",
            f"/agents/{tuple_id}/renew",
            extra_headers={
                "X-Agent-ID": agent_id,
                "X-Lease-Token": lease_token,
            },
            json_data=json_body,
        )
        if response_data is None:
            return await self.get_agent(tuple_id)
        return Agent.from_dict(response_data)
    
    # DAG operations
    
    async def get_dag(self, root_id: UUID) -> DAG:
        """
        Get the DAG (Directed Acyclic Graph) for a agent and its children.
        
        Args:
            root_id: UUID of the root agent
            
        Returns:
            DAG structure
            
        Raises:
            NotFoundError: If root agent doesn't exist
        """
        response_data = await self._make_request(
            "GET",
            f"/dag/{root_id}"
        )
        return DAG.from_dict(response_data)
    
    # System operations
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check the health of the agent space service.
        
        Returns:
            Health status information
        """
        response_data = await self._make_request(
            "GET",
            "/health"
        )
        return response_data
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Get system statistics.
        
        Returns:
            System statistics including agent counts, events, etc.
        """
        response_data = await self._make_request(
            "GET",
            "/stats"
        )
        return response_data
    
    # Helper methods
    
    async def wait_for_completion(
        self,
        tuple_id: UUID,
        timeout: float = 300.0,
        poll_interval: float = 1.0
    ) -> Agent:
        """
        Wait for a agent to reach completion status.
        
        Args:
            tuple_id: UUID of the agent to monitor
            timeout: Maximum time to wait in seconds
            poll_interval: Time between status checks in seconds
            
        Returns:
            The completed agent
            
        Raises:
            TimeoutError: If agent doesn't complete within timeout
            NotFoundError: If agent doesn't exist
        """
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            agent_obj = await self.get_agent(tuple_id)
            
            if agent_obj.status.value in ["COMPLETED", "FAILED", "CANCELLED"]:
                return agent_obj
            
            await asyncio.sleep(poll_interval)
        
        raise TimeoutError(f"Agent {tuple_id} did not complete within {timeout} seconds")
    
    async def batch_create_agents(
        self,
        requests: List[CreateAgentRequest]
    ) -> List[Agent]:
        """
        Create multiple agents concurrently.
        
        This method creates agents in parallel for better performance
        compared to sequential creation.
        
        Args:
            requests: List of agent creation requests
            
        Returns:
            List of created agents
        """
        tasks = [self.create_agent(request) for request in requests]
        return await asyncio.gather(*tasks)
