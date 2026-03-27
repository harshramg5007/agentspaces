"""Synchronous client for the Agent Space API."""

import json
from typing import Any, Dict, List, Optional, Tuple as TupleType, Union
from urllib.parse import urljoin
from uuid import UUID

import requests

from .exceptions import (
    AuthenticationError,
    BadRequestError,
    GatewayTimeoutError,
    InternalError,
    NotFoundError,
    RateLimitError,
    ServiceUnavailableError,
    TimeoutError,
    AgentSpaceError,
)
from .models import (
    CompleteAgentRequest,
    CompleteAndOutRequest,
    CreateAgentRequest,
    DAG,
    Event,
    InRequest,
    ReadRequest,
    Query,
    RenewLeaseRequest,
    TakeRequest,
    Agent,
    UpdateAgentRequest,
)
from .utils import AuthConfig, RetryConfig, retry_with_backoff


class AgentSpaceClient:
    """
    Synchronous client for interacting with the Agent Space API.
    
    Args:
        base_url: Base URL of the Agent Space API (e.g., "http://localhost:8080/api/v1")
        auth_config: Authentication configuration
        retry_config: Retry configuration for failed requests
        timeout: Default timeout for requests in seconds
        verify_ssl: Whether to verify SSL certificates
    
    Example:
        client = AgentSpaceClient("http://localhost:8080/api/v1")
        
        # Create a agent
        agent_data = CreateAgentRequest(
            kind="task",
            payload={"action": "process", "data": "example"}
        )
        agent = client.create_agent(agent_data)
        
        # Query agents
        query = Query(kind="task", status=AgentStatus.NEW)
        results = client.query_agents(query)
    """
    
    def __init__(
        self,
        base_url: str,
        auth_config: Optional[AuthConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        timeout: float = 30.0,
        verify_ssl: bool = True,
    ):
        self.base_url = base_url.rstrip('/') + '/'
        self.auth_config = auth_config or AuthConfig()
        self.retry_config = retry_config or RetryConfig()
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self.session.headers.update(self.auth_config.get_headers())
    
    def _build_url(self, path: str) -> str:
        """Build full URL from path."""
        return urljoin(self.base_url, path.lstrip('/'))
    
    def _handle_response(self, response: requests.Response) -> Any:
        """Handle API response and raise appropriate exceptions."""
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Parse error response
            try:
                error_data = response.json()
                message = error_data.get("message", str(e))
                error_code = error_data.get("error", None)
                details = error_data.get("details", {})
            except (json.JSONDecodeError, KeyError):
                message = str(e)
                error_code = None
                details = {}
            
            # Raise appropriate exception based on status code
            if response.status_code == 404:
                raise NotFoundError(message, error_code, details)
            elif response.status_code == 400:
                raise BadRequestError(message, error_code, details)
            elif response.status_code == 401:
                raise AuthenticationError(message, error_code, details)
            elif response.status_code == 429:
                raise RateLimitError(message, error_code, details)
            elif response.status_code == 408:
                raise TimeoutError(message, error_code, details)
            elif response.status_code == 503:
                raise ServiceUnavailableError(message, error_code, details)
            elif response.status_code == 504:
                raise GatewayTimeoutError(message, error_code, details)
            elif response.status_code >= 500:
                raise InternalError(message, error_code, details)
            else:
                raise AgentSpaceError(message, error_code, details)
        
        # Return JSON response if available
        if response.content:
            return response.json()
        return None
    
    @retry_with_backoff()
    def create_agent(self, request: CreateAgentRequest) -> Agent:
        """
        Create a new agent in the space.
        
        Args:
            request: Agent creation request
        
        Returns:
            The created Agent
        
        Raises:
            BadRequestError: If the request is invalid
            InternalError: If the server encounters an error
        """
        url = self._build_url("/agents")
        response = self.session.post(
            url,
            json=request.to_dict(),
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        return Agent.from_dict(data)
    
    @retry_with_backoff()
    def get_agent(self, tuple_id: Union[str, UUID]) -> Agent:
        """
        Get a agent by ID.
        
        Args:
            tuple_id: ID of the agent to retrieve
        
        Returns:
            The requested Agent
        
        Raises:
            NotFoundError: If the agent is not found
            InternalError: If the server encounters an error
        """
        url = self._build_url(f"/agents/{tuple_id}")
        response = self.session.get(
            url,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        return Agent.from_dict(data)
    
    @retry_with_backoff()
    def update_agent(self, tuple_id: Union[str, UUID], request: UpdateAgentRequest) -> Agent:
        """
        Update a agent.
        
        Args:
            tuple_id: ID of the agent to update
            request: Update request
        
        Returns:
            The updated Agent
        
        Raises:
            NotFoundError: If the agent is not found
            BadRequestError: If the request is invalid
            InternalError: If the server encounters an error
        """
        url = self._build_url(f"/agents/{tuple_id}")
        response = self.session.put(
            url,
            json=request.to_dict(),
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        return Agent.from_dict(data)
    
    @retry_with_backoff()
    def query_agents(self, query: Query, timeout: Optional[float] = None) -> List[Agent]:
        """
        Query agents (non-destructive read).
        
        Args:
            query: Query parameters
            timeout: Optional request timeout override (seconds)
        
        Returns:
            List of matching Agents
        
        Raises:
            BadRequestError: If the query is invalid
            InternalError: If the server encounters an error
        """
        url = self._build_url("agents/query")
        response = self.session.post(
            url,
            json=query.to_dict(),
            timeout=timeout or self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        if data is None:
            return []
        agents = data.get("agents") or []
        return [Agent.from_dict(t) for t in agents]

    @retry_with_backoff()
    def agent_stats(self, query: Query, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Get agent counts by status for a query.

        Args:
            query: Query parameters
            timeout: Optional request timeout override (seconds)

        Returns:
            Dict with counts and total
        """
        url = self._build_url("agents/stats")
        response = self.session.post(
            url,
            json=query.to_dict(),
            timeout=timeout or self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        return data or {"counts": {}, "total": 0}

    @retry_with_backoff()
    def read_agent(self, request: ReadRequest) -> Optional[Agent]:
        """
        Read a agent (non-destructive).

        Args:
            request: Read request with query and optional blocking settings

        Returns:
            The matching Agent, or None if not found

        Raises:
            TimeoutError: If blocking read times out
            BadRequestError: If the request is invalid
            InternalError: If the server encounters an error
        """
        url = self._build_url("/agents/read")
        response = self.session.post(
            url,
            json=request.to_dict(),
            timeout=self.timeout,
            verify=self.verify_ssl,
        )

        if response.status_code == 204:
            return None
        if response.status_code == 408:
            raise TimeoutError("Operation timed out", "TIMEOUT", {})

        data = self._handle_response(response)
        if data is None:
            return None
        return Agent.from_dict(data)
    
    @retry_with_backoff()
    def take_agent(self, request: TakeRequest, agent_id: Optional[str] = None) -> Optional[Agent]:
        """
        Take a agent (destructive read).

        Args:
            request: Take request with query
            agent_id: Optional agent ID for ownership (sent via X-Agent-ID header)

        Returns:
            The taken Agent, or None if no match found

        Raises:
            BadRequestError: If the request is invalid
            InternalError: If the server encounters an error
        """
        url = self._build_url("/agents/take")
        headers = {}
        if agent_id:
            headers["X-Agent-ID"] = agent_id
        response = self.session.post(
            url,
            json=request.to_dict(),
            headers=headers,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        
        if response.status_code == 204:
            return None

        data = self._handle_response(response)
        if data is None:
            return None
        return Agent.from_dict(data)

    def in_agent(self, request: InRequest, agent_id: Optional[str] = None) -> Optional[Agent]:
        """
        Blocking take operation.
        
        Note: This method does not use retry logic as it's a blocking operation.
        
        Args:
            request: In request with query and timeout
            agent_id: Optional agent ID for ownership (sent via X-Agent-ID header)
        
        Returns:
            The taken Agent, or None if timeout
        
        Raises:
            TimeoutError: If the operation times out
            BadRequestError: If the request is invalid
            InternalError: If the server encounters an error
        """
        url = self._build_url("/agents/in")
        # Extend timeout to account for server-side blocking
        client_timeout = request.timeout + 5
        
        headers = {}
        if agent_id:
            headers["X-Agent-ID"] = agent_id
        response = self.session.post(
            url,
            json=request.to_dict(),
            headers=headers,
            timeout=client_timeout,
            verify=self.verify_ssl,
        )
        
        if response.status_code == 408:
            raise TimeoutError("Operation timed out", "TIMEOUT", {})

        data = self._handle_response(response)
        if data is None:
            return None
        return Agent.from_dict(data)

    @retry_with_backoff()
    def complete_agent(
        self,
        tuple_id: Union[str, UUID],
        request: CompleteAgentRequest,
        agent_id: str,
        lease_token: str,
        prefer_minimal: bool = False,
    ) -> Optional[Agent]:
        """
        Mark a agent as completed.
        
        Args:
            tuple_id: ID of the agent to complete
            request: Completion request with result data
            agent_id: Agent ID completing the agent
            lease_token: Lease token from the claim
            prefer_minimal: Request 204 minimal response body from the server

        Returns:
            The completed Agent when representation is returned; otherwise None
        
        Raises:
            NotFoundError: If the agent is not found
            BadRequestError: If the request is invalid
            InternalError: If the server encounters an error
        """
        if not agent_id:
            raise ValueError("agent_id is required")
        if not lease_token:
            raise ValueError("lease_token is required")
        url = self._build_url(f"agents/{tuple_id}/complete")
        headers = {
            "X-Agent-ID": agent_id,
            "X-Lease-Token": lease_token,
        }
        if prefer_minimal:
            headers["Prefer"] = "return=minimal"
        response = self.session.post(
            url,
            json=request.to_dict(),
            headers=headers,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        if data is None:
            if prefer_minimal:
                return None
            # API returned empty body, try to fetch the agent
            try:
                return self.get_agent(tuple_id)
            except NotFoundError:
                # Agent was completed but can't be fetched back - this is OK
                return None
        return Agent.from_dict(data)

    @retry_with_backoff()
    def complete_and_out(
        self,
        tuple_id: Union[str, UUID],
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

        url = self._build_url(f"agents/{tuple_id}/complete-and-out")
        response = self.session.post(
            url,
            json=request.to_dict(),
            headers={
                "X-Agent-ID": agent_id,
                "X-Lease-Token": lease_token,
            },
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response) or {}
        completed = Agent.from_dict(data["completed"])
        created = [Agent.from_dict(item) for item in data.get("created", [])]
        return completed, created

    @retry_with_backoff()
    def release_agent(
        self,
        tuple_id: Union[str, UUID],
        agent_id: str,
        lease_token: str,
        reason: str = "",
        obs: Optional[Dict[str, Any]] = None,
    ) -> Agent:
        """
        Release ownership of a agent.

        Args:
            tuple_id: ID of the agent to release
            agent_id: ID of the agent releasing the agent (must match current owner)
            lease_token: Lease token from the claim
            reason: Optional reason for release (e.g., "lease_expired" for reaper)
            obs: Optional telemetry observation payload

        Returns:
            The released Agent

        Raises:
            NotFoundError: If the agent is not found
            BadRequestError: If agent_id header is missing
            AgentSpaceError: If the agent doesn't own the agent (403)
            InternalError: If the server encounters an error
        """
        if not agent_id:
            raise ValueError("agent_id is required")
        if not lease_token:
            raise ValueError("lease_token is required")
        url = self._build_url(f"agents/{tuple_id}/release")

        # Build request body if reason provided
        json_body = None
        if reason or obs is not None:
            json_body = {}
            if reason:
                json_body["reason"] = reason
            if obs is not None:
                json_body["obs"] = obs

        response = self.session.post(
            url,
            headers={
                "X-Agent-ID": agent_id,
                "X-Lease-Token": lease_token,
            },
            json=json_body,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        if data is None:
            # API returned empty body, fetch the agent
            return self.get_agent(tuple_id)
        return Agent.from_dict(data)
    
    @retry_with_backoff()
    def get_events(
        self,
        tuple_id: Optional[Union[str, UUID]] = None,
        agent_id: Optional[str] = None,
        event_type: Optional[str] = None,
        trace_id: Optional[Union[str, UUID]] = None,
        limit: int = 100,
        offset: int = 0,
        cursor: Optional[str] = None,
        return_cursor: bool = False,
    ) -> Union[List[Event], tuple]:
        """
        Get events.
        
        Args:
            tuple_id: Filter by agent ID
            agent_id: Filter by agent ID
            event_type: Filter by event type
            trace_id: Filter by trace ID
            limit: Maximum number of results
            offset: Offset for pagination
            cursor: Optional cursor for cursor-based pagination
            return_cursor: When true, include next_cursor in response agent

        Returns:
            List of Events
            If return_cursor=True, returns (events, total, next_cursor)
        
        Raises:
            BadRequestError: If the request is invalid
            InternalError: If the server encounters an error
        """
        url = self._build_url("/events")
        params = {
            "limit": limit,
            "offset": offset,
        }
        
        if tuple_id:
            params["tuple_id"] = str(tuple_id)
        if agent_id:
            params["agent_id"] = agent_id
        if event_type:
            params["type"] = event_type
        if trace_id:
            params["trace_id"] = str(trace_id)
        if cursor:
            params["cursor"] = cursor

        response = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        events = data.get("events") or []
        parsed = [Event.from_dict(e) for e in events]
        if return_cursor:
            return parsed, data.get("total", len(parsed)), data.get("next_cursor")
        return parsed

    @retry_with_backoff()
    def ingest_telemetry_span(self, span: Dict[str, Any]) -> int:
        """
        Ingest a single telemetry span.

        Args:
            span: Telemetry span payload

        Returns:
            Number of spans ingested
        """
        url = self._build_url("/telemetry/spans")
        response = self.session.post(
            url,
            json=span,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        return int(data.get("ingested", 0))

    @retry_with_backoff()
    def ingest_telemetry_spans(self, spans: List[Dict[str, Any]]) -> int:
        """
        Ingest telemetry spans in batch.

        Args:
            spans: List of telemetry span payloads

        Returns:
            Number of spans ingested
        """
        url = self._build_url("/telemetry/spans:batch")
        response = self.session.post(
            url,
            json={"spans": spans},
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        return int(data.get("ingested", 0))

    @retry_with_backoff()
    def list_telemetry_spans(
        self,
        trace_id: Union[str, UUID],
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
        url = self._build_url("/telemetry/spans")
        params = {
            "trace_id": str(trace_id),
            "limit": limit,
            "offset": offset,
        }
        response = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        return data.get("spans") or []

    @retry_with_backoff()
    def renew_lease(
        self,
        tuple_id: Union[str, UUID],
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
        url = self._build_url(f"/agents/{tuple_id}/renew")
        headers = {
            "X-Agent-ID": agent_id,
            "X-Lease-Token": lease_token,
        }
        json_body = request.to_dict() if request is not None else None

        response = self.session.post(
            url,
            headers=headers,
            json=json_body,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        if data is None:
            return self.get_agent(tuple_id)
        return Agent.from_dict(data)
    
    @retry_with_backoff()
    def get_dag(self, root_id: Union[str, UUID]) -> DAG:
        """
        Get DAG for a agent.
        
        Args:
            root_id: Root agent ID
        
        Returns:
            DAG structure
        
        Raises:
            NotFoundError: If the agent is not found
            InternalError: If the server encounters an error
        """
        url = self._build_url(f"/dag/{root_id}")
        response = self.session.get(
            url,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        data = self._handle_response(response)
        return DAG.from_dict(data)
    
    @retry_with_backoff()
    def health_check(self) -> Dict[str, str]:
        """
        Check service health.
        
        Returns:
            Health status information
        
        Raises:
            InternalError: If the server is unhealthy
        """
        url = self._build_url("/health")
        response = self.session.get(
            url,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        return self._handle_response(response)
    
    @retry_with_backoff()
    def get_stats(self) -> Dict[str, Any]:
        """
        Get system statistics.
        
        Returns:
            System statistics
        
        Raises:
            InternalError: If the server encounters an error
        """
        url = self._build_url("/stats")
        response = self.session.get(
            url,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        return self._handle_response(response)
    
    def close(self):
        """Close the client session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
