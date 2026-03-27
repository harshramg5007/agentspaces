"""Agent and related models for the Agent Space SDK."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

_MAX_SAFE_TTL_SECONDS = (2**63 - 1) // 1_000_000_000
_TTL_DURATION_SCALES = (1_000_000_000, 1_000_000, 1_000)


def _parse_timestamp(ts: str) -> datetime:
    """Parse timestamp, normalizing fractional seconds for Python.

    Go returns RFC3339Nano with 1-9 fractional digits, while Python only supports
    microseconds (6 digits). We truncate or pad the fractional part as needed.
    """
    ts = ts.strip().replace("Z", "+00:00")

    tz_match = re.search(r"([+-]\d{2}:\d{2})$", ts)
    tz = tz_match.group(1) if tz_match else ""
    core = ts[:-len(tz)] if tz else ts

    if "." in core:
        base, frac = core.split(".", 1)
        frac_digits = re.sub(r"\D", "", frac)
        if len(frac_digits) > 6:
            frac_digits = frac_digits[:6]
        else:
            frac_digits = frac_digits.ljust(6, "0")
        if frac_digits:
            core = f"{base}.{frac_digits}"
        else:
            core = base

    return datetime.fromisoformat(core + tz)


def _normalize_ttl_seconds(raw_ttl: Any) -> Optional[int]:
    """Normalize TTL values to seconds.

    Server responses can carry Go `time.Duration` shaped values (nanoseconds).
    Normalize those back to seconds and clamp into int64-duration-safe range.
    """
    if raw_ttl is None:
        return None

    try:
        if hasattr(raw_ttl, "total_seconds"):
            ttl_value = int(raw_ttl.total_seconds())
        elif isinstance(raw_ttl, str):
            raw_piece = raw_ttl.strip()
            if raw_piece == "":
                return None
            ttl_value = int(float(raw_piece))
        else:
            ttl_value = int(raw_ttl)
    except (TypeError, ValueError, OverflowError):
        return None

    if ttl_value <= 0:
        return 0
    if ttl_value <= _MAX_SAFE_TTL_SECONDS:
        return ttl_value

    for scale in _TTL_DURATION_SCALES:
        if ttl_value % scale != 0:
            continue
        normalized = ttl_value // scale
        if 0 < normalized <= _MAX_SAFE_TTL_SECONDS:
            return int(normalized)

    return int(_MAX_SAFE_TTL_SECONDS)


class AgentStatus(str, Enum):
    """Status of a agent in the space."""
    NEW = "NEW"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class EventType(str, Enum):
    """Types of events that can occur on agents."""
    CREATED = "CREATED"
    CLAIMED = "CLAIMED"
    UPDATED = "UPDATED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RELEASED = "RELEASED"


@dataclass
class AgentClaim:
    """Minimal take response containing only claim identity and fencing token."""

    id: UUID
    lease_token: str
    shard_id: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentClaim":
        """Create a AgentClaim from a minimal take response."""
        return cls(
            id=UUID(data["id"]),
            lease_token=data["lease_token"],
            shard_id=data.get("shard_id"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert the claim to a dictionary."""
        return {
            "id": str(self.id),
            "lease_token": self.lease_token,
            "shard_id": self.shard_id,
        }


@dataclass
class Agent:
    """
    Represents a agent in the agent space.
    
    Attributes:
        id: Unique identifier
        kind: Type/category of the agent
        status: Current status
        created_at: Creation timestamp
        updated_at: Last update timestamp
        payload: Actual data payload
        tags: Tags for filtering
        owner: Agent that owns this agent
        parent_id: Parent agent ID for DAG relationships
        trace_id: Distributed tracing ID
        ttl: Time to live in seconds
        owner_time: When ownership was taken
        lease_token: Lease token for the current claim
        lease_until: Lease expiry timestamp
        version: Monotonic agent version
        metadata: Additional metadata
    """
    id: UUID
    kind: str
    status: AgentStatus
    created_at: datetime
    updated_at: datetime
    shard_id: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    owner: Optional[str] = None
    parent_id: Optional[UUID] = None
    trace_id: Optional[UUID] = None
    namespace_id: Optional[str] = None
    ttl: Optional[int] = None
    owner_time: Optional[datetime] = None
    lease_token: Optional[str] = None
    lease_until: Optional[datetime] = None
    version: Optional[int] = None
    metadata: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Agent":
        """Create a Agent from a dictionary."""
        return cls(
            id=UUID(data["id"]),
            kind=data["kind"],
            status=AgentStatus(data["status"]),
            created_at=_parse_timestamp(data["created_at"]),
            updated_at=_parse_timestamp(data["updated_at"]),
            shard_id=data.get("shard_id"),
            payload=data.get("payload", {}),
            tags=data.get("tags", []),
            owner=data.get("owner"),
            parent_id=UUID(data["parent_id"]) if data.get("parent_id") else None,
            trace_id=UUID(data["trace_id"]) if data.get("trace_id") else None,
            namespace_id=data.get("namespace_id"),
            ttl=_normalize_ttl_seconds(data.get("ttl")),
            owner_time=_parse_timestamp(data["owner_time"]) if data.get("owner_time") else None,
            lease_token=data.get("lease_token"),
            lease_until=_parse_timestamp(data["lease_until"]) if data.get("lease_until") else None,
            version=data.get("version"),
            metadata=data.get("metadata", {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Agent to a dictionary."""
        result = {
            "id": str(self.id),
            "kind": self.kind,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "payload": self.payload,
            "tags": self.tags,
            "metadata": self.metadata,
        }
        if self.shard_id:
            result["shard_id"] = self.shard_id
        
        if self.owner:
            result["owner"] = self.owner
        if self.parent_id:
            result["parent_id"] = str(self.parent_id)
        if self.trace_id:
            result["trace_id"] = str(self.trace_id)
        if self.namespace_id:
            result["namespace_id"] = self.namespace_id
        if self.ttl is not None:
            result["ttl"] = self.ttl
        if self.owner_time:
            result["owner_time"] = self.owner_time.isoformat()
        if self.lease_token:
            result["lease_token"] = self.lease_token
        if self.lease_until:
            result["lease_until"] = self.lease_until.isoformat()
        if self.version is not None:
            result["version"] = self.version
            
        return result


@dataclass
class Query:
    """
    Query parameters for searching agents.
    
    Attributes:
        kind: Filter by agent kind
        status: Filter by status
        tags: Filter by tags (AND condition)
        owner: Filter by owner
        parent_id: Filter by parent ID
        trace_id: Filter by trace ID
        metadata: Filter by metadata (exact match)
        agent_id: Agent ID used for ownership on take/in
        limit: Maximum number of results
        offset: Offset for pagination
    """
    kind: Optional[str] = None
    status: Optional[AgentStatus] = None
    tags: Optional[List[str]] = None
    owner: Optional[str] = None
    parent_id: Optional[UUID] = None
    trace_id: Optional[UUID] = None
    namespace_id: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    agent_id: Optional[str] = None
    limit: int = 100
    offset: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Query to a dictionary for API calls."""
        result = {}
        
        if self.kind:
            result["kind"] = self.kind
        if self.status:
            result["status"] = self.status.value
        if self.tags:
            result["tags"] = self.tags
        if self.owner:
            result["owner"] = self.owner
        if self.parent_id:
            result["parent_id"] = str(self.parent_id)
        if self.trace_id:
            result["trace_id"] = str(self.trace_id)
        if self.namespace_id:
            result["namespace_id"] = self.namespace_id
        if self.metadata:
            result["metadata"] = self.metadata
        if self.agent_id:
            result["agent_id"] = self.agent_id
        if self.limit != 100:
            result["limit"] = self.limit
        if self.offset != 0:
            result["offset"] = self.offset
            
        return result


@dataclass
class Event:
    """
    Represents an event that occurred on a agent.
    
    Attributes:
        id: Event ID
        tuple_id: ID of the agent this event relates to
        type: Type of event
        timestamp: When the event occurred
        agent_id: Agent that triggered the event
        data: Additional event data
        trace_id: Distributed tracing ID
        version: Monotonic agent version
    """
    id: UUID
    tuple_id: UUID
    type: EventType
    timestamp: datetime
    agent_id: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    trace_id: Optional[UUID] = None
    version: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        """Create an Event from a dictionary."""
        return cls(
            id=UUID(data["id"]),
            tuple_id=UUID(data["tuple_id"]),
            type=EventType(data["type"]),
            timestamp=_parse_timestamp(data["timestamp"]),
            agent_id=data.get("agent_id"),
            data=data.get("data"),
            trace_id=UUID(data["trace_id"]) if data.get("trace_id") else None,
            version=data.get("version"),
        )


@dataclass
class CreateAgentRequest:
    """Request for creating a new agent."""
    kind: str
    payload: Dict[str, Any]
    tags: Optional[List[str]] = None
    parent_id: Optional[UUID] = None
    trace_id: Optional[UUID] = None
    namespace_id: Optional[str] = None
    ttl: Optional[int] = None
    metadata: Optional[Dict[str, str]] = None
    obs: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API call."""
        result = {
            "kind": self.kind,
            "payload": self.payload,
        }
        
        if self.tags:
            result["tags"] = self.tags
        if self.parent_id:
            result["parent_id"] = str(self.parent_id)
        if self.trace_id:
            result["trace_id"] = str(self.trace_id)
        if self.namespace_id:
            result["namespace_id"] = self.namespace_id
        if self.ttl is not None:
            result["ttl"] = self.ttl
        if self.metadata:
            result["metadata"] = self.metadata
        if self.obs is not None:
            result["obs"] = self.obs
            
        return result


@dataclass
class UpdateAgentRequest:
    """Request for updating a agent."""
    payload: Optional[Dict[str, Any]] = None
    status: Optional[AgentStatus] = None
    tags: Optional[List[str]] = None
    owner: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    obs: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API call."""
        result = {}
        
        if self.payload is not None:
            result["payload"] = self.payload
        if self.status:
            result["status"] = self.status.value
        if self.tags is not None:
            result["tags"] = self.tags
        if self.owner is not None:
            result["owner"] = self.owner
        if self.metadata is not None:
            result["metadata"] = self.metadata
        if self.obs is not None:
            result["obs"] = self.obs
            
        return result


@dataclass
class TakeRequest:
    """Request for taking a agent."""
    query: Query
    obs: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API call."""
        result = {"query": self.query.to_dict()}
        if self.obs is not None:
            result["obs"] = self.obs
        return result


@dataclass
class InRequest:
    """Request for blocking take operation."""
    query: Query
    timeout: int = 30  # seconds
    obs: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API call."""
        result = {
            "query": self.query.to_dict(),
            "timeout": self.timeout,
        }
        if self.obs is not None:
            result["obs"] = self.obs
        return result


@dataclass
class ReadRequest:
    """Request for non-destructive read."""
    query: Query
    timeout: int = 30  # seconds (blocking only)
    blocking: bool = False

    def __post_init__(self):
        if self.blocking and (self.timeout < 1 or self.timeout > 300):
            raise ValueError("Timeout must be between 1 and 300 seconds")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API call."""
        result = {"query": self.query.to_dict()}
        if self.blocking:
            result["blocking"] = True
            result["timeout"] = self.timeout
        return result


@dataclass
class CompleteAgentRequest:
    """Request for completing a agent."""
    result: Dict[str, Any]
    shard_id: Optional[str] = None
    obs: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API call."""
        result = {"result": self.result}
        if self.shard_id is not None:
            result["shard_id"] = self.shard_id
        if self.obs is not None:
            result["obs"] = self.obs
        return result


@dataclass
class CompleteAndOutRequest:
    """Request for atomically completing a agent and creating outputs."""

    result: Dict[str, Any]
    shard_id: Optional[str] = None
    outputs: List[CreateAgentRequest] = field(default_factory=list)
    obs: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API call."""
        result = {
            "result": self.result,
            "outputs": [output.to_dict() for output in self.outputs],
        }
        if self.shard_id is not None:
            result["shard_id"] = self.shard_id
        if self.obs is not None:
            result["obs"] = self.obs
        return result


@dataclass
class RenewLeaseRequest:
    """Request for renewing a lease."""
    lease_sec: Optional[int] = None
    shard_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API call."""
        result = {}
        if self.lease_sec is not None:
            result["lease_sec"] = self.lease_sec
        if self.shard_id is not None:
            result["shard_id"] = self.shard_id
        return result
