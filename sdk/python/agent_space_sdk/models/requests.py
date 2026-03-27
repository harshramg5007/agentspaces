"""Request model classes for API operations."""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from uuid import UUID

from .agent import Query


@dataclass
class CreateAgentRequest:
    """
    Request parameters for creating a new agent.
    
    Required fields:
    - kind: Type/category of the agent
    - payload: Actual data payload
    """
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
        """
        Convert to dictionary for API request.
        
        Returns:
            Dictionary representation of the request
        """
        result = {
            "kind": self.kind,
            "payload": self.payload
        }
        
        if self.tags is not None:
            result["tags"] = self.tags
        if self.parent_id is not None:
            result["parent_id"] = str(self.parent_id)
        if self.trace_id is not None:
            result["trace_id"] = str(self.trace_id)
        if self.namespace_id is not None:
            result["namespace_id"] = self.namespace_id
        if self.ttl is not None:
            result["ttl"] = self.ttl
        if self.metadata is not None:
            result["metadata"] = self.metadata
        if self.obs is not None:
            result["obs"] = self.obs
            
        return result


@dataclass
class UpdateAgentRequest:
    """
    Request parameters for updating an existing agent.
    
    All fields are optional. Lease-sensitive fields such as status and owner
    must be changed through take/complete/release/renew operations instead of
    the generic update endpoint.
    """
    payload: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, str]] = None
    obs: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API request.
        
        Returns:
            Dictionary representation of the request
        """
        result = {}
        
        if self.payload is not None:
            result["payload"] = self.payload
        if self.tags is not None:
            result["tags"] = self.tags
        if self.metadata is not None:
            result["metadata"] = self.metadata
        if self.obs is not None:
            result["obs"] = self.obs
            
        return result


@dataclass
class TakeRequest:
    """
    Request parameters for taking a agent (destructive read).
    
    The query parameter specifies which agent to take.
    """
    query: Query
    obs: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API request.
        
        Returns:
            Dictionary representation of the request
        """
        result = {"query": self.query.to_dict()}
        if self.obs is not None:
            result["obs"] = self.obs
        return result


@dataclass
class InRequest:
    """
    Request parameters for blocking take operation.
    
    Required fields:
    - query: Specifies which agent to wait for
    - timeout: Maximum time to wait in seconds (1-300)
    """
    query: Query
    timeout: int = 30
    obs: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate timeout is within allowed range."""
        if self.timeout < 1 or self.timeout > 300:
            raise ValueError("Timeout must be between 1 and 300 seconds")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API request.
        
        Returns:
            Dictionary representation of the request
        """
        result = {
            "query": self.query.to_dict(),
            "timeout": self.timeout
        }
        if self.obs is not None:
            result["obs"] = self.obs
        return result


@dataclass
class ReadRequest:
    """
    Request parameters for non-destructive read.

    Required fields:
    - query: Specifies which agent to read
    """
    query: Query
    timeout: int = 30
    blocking: bool = False

    def __post_init__(self):
        """Validate timeout is within allowed range when blocking."""
        if self.blocking and (self.timeout < 1 or self.timeout > 300):
            raise ValueError("Timeout must be between 1 and 300 seconds")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API request.

        Returns:
            Dictionary representation of the request
        """
        result = {"query": self.query.to_dict()}
        if self.blocking:
            result["blocking"] = True
            result["timeout"] = self.timeout
        return result


@dataclass
class CompleteAgentRequest:
    """
    Request parameters for marking a agent as completed.
    
    Required fields:
    - result: Completion result data
    """
    result: Dict[str, Any]
    shard_id: Optional[str] = None
    obs: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API request.
        
        Returns:
            Dictionary representation of the request
        """
        result = {"result": self.result}
        if self.shard_id is not None:
            result["shard_id"] = self.shard_id
        if self.obs is not None:
            result["obs"] = self.obs
        return result


@dataclass
class CompleteAndOutRequest:
    """
    Request parameters for atomically completing a agent and creating outputs.
    """

    result: Dict[str, Any]
    shard_id: Optional[str] = None
    outputs: List[CreateAgentRequest] = field(default_factory=list)
    obs: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API request."""
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
    """
    Request parameters for renewing a lease.
    """
    lease_sec: Optional[int] = None
    shard_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API request.

        Returns:
            Dictionary representation of the request
        """
        result = {}
        if self.lease_sec is not None:
            result["lease_sec"] = self.lease_sec
        if self.shard_id is not None:
            result["shard_id"] = self.shard_id
        return result
